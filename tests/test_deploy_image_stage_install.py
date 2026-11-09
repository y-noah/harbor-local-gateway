import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import patch
from test_postgres_runtime_upgrade import ticket, active, CONFIG, MATERIALS, IMAGE, upgrade, controller

spec = importlib.util.spec_from_file_location('image_stage_install', Path(__file__).parents[1] / 'ci/install-image-stage.py')
i = importlib.util.module_from_spec(spec);spec.loader.exec_module(i)


class ImageStageInstallTests(unittest.TestCase):
    def fixture(self, directory, stack):
        root = Path(directory)
        paths = {name: root / name for name in ['wrapper.py', 'helper.py', 'manifest.json', 'ticket.json', 'active.json', 'candidate.json']}
        tools = (str(paths['wrapper.py']), *(str(root / ('host-' + str(n) + '.py')) for n in range(3)))
        for name in tools:
            Path(name).write_bytes(('synthetic old file ' + name).encode());Path(name).chmod(0o600)
        t = ticket();t['hostToolsRevision'] = '9' * 40
        manifest = {'version': 1, 'contract': 'harbor-postgres-runtime-tools-v1', 'revision': t['hostToolsRevision'], 'files': {name: hashlib.sha256(Path(name).read_bytes()).hexdigest() for name in tools}}
        a = active();a['image'] = t['to']['image'];a['candidate']['containerId'] = 'c' * 64;a['candidate']['imageId'] = t['to']['configDigest'];a['approval'] = upgrade.approval_binding(upgrade.approval(t));a['configDigest'] = controller.fingerprint({})
        paths['manifest.json'].write_bytes(i.body(manifest));paths['ticket.json'].write_bytes(i.body(t));paths['active.json'].write_bytes(i.body(a));paths['candidate.json'].write_bytes(i.body({'socketPath': '/run/harbor-entry/candidate.sock'}))
        releases = root / 'releases';release = releases / t['deploymentId'];release.mkdir(parents=True)
        record = {'contract': upgrade.CONTRACT, 'ticketDigest': upgrade.digest(t), 'ticket': t, 'phase': 'complete', 'candidate': {'containerId': a['candidate']['containerId']}}
        history_path = release / 'release.json';history_path.write_bytes(i.body(record));history_before = history_path.read_bytes()
        profile, marker = root / 'profile.json', root / 'marker.json';profile.write_text('{}');marker.write_text(json.dumps({'contract': 'postgres-conversation-v2', 'database_digest': '3' * 64}))
        def json_file(path):
            path = Path(path)
            if str(path) == MATERIALS + '/postgres-storage.json':
                path = marker
            return json.loads(path.read_bytes())
        # Actual source verification and startup authorization read real JSON and
        # code files; only platform ownership and unrelated Docker/DB I/O are substituted.
        def read_file(path, mode=None):
            path = Path(path);self.assertTrue(path.is_file() and not path.is_symlink())
            if mode is not None:
                self.assertEqual(path.stat().st_mode & 0o777, mode)
            return path.read_bytes()
        for path in [paths['manifest.json'], paths['ticket.json'], paths['active.json'], paths['candidate.json']]:
            path.chmod(0o600)
        replacements = [('WRAPPER', paths['wrapper.py']), ('HELPER', paths['helper.py']), ('MANIFEST', paths['manifest.json']), ('TICKET', paths['ticket.json']), ('HISTORY', root / 'installs')]
        for name, value in replacements:
            stack.enter_context(patch.object(i, name, value))
        lock_paths = [str(root / 'deploy.lock'), str(root / 'controller.lock')]
        for name in lock_paths:
            Path(name).touch(mode=0o600)
        stack.enter_context(patch.object(i, 'LOCKS', lock_paths));stack.enter_context(patch.object(i, 'parents'))
        stack.enter_context(patch.object(i, 'read', side_effect=read_file));stack.enter_context(patch.object(i.os, 'geteuid', return_value=0))
        stack.enter_context(patch.object(i, 'module', side_effect=lambda path, name: upgrade if path == i.UPGRADE else controller))
        stack.enter_context(patch.object(upgrade, 'HOST_FILES', tools));stack.enter_context(patch.object(upgrade, 'TOOLS_MANIFEST', paths['manifest.json']));stack.enter_context(patch.object(upgrade, 'RELEASES', releases))
        stack.enter_context(patch.object(upgrade, 'private_json', side_effect=json_file));stack.enter_context(patch.object(upgrade, 'trusted_code_bytes', side_effect=lambda path: Path(path).read_bytes()))
        stack.enter_context(patch.object(upgrade, 'load', return_value=controller));stack.enter_context(patch.object(upgrade, 'check_storage', side_effect=lambda value: self.assertEqual(value['database'], t['database'])))
        stack.enter_context(patch.object(controller, 'ACTIVE', paths['active.json']));stack.enter_context(patch.object(controller, 'PROFILE', profile));stack.enter_context(patch.object(controller, 'CANDIDATE_PROFILE', paths['candidate.json']))
        stack.enter_context(patch.object(controller, 'private_json', side_effect=json_file));stack.enter_context(patch.object(controller, 'save_json', side_effect=lambda path, value: Path(path).write_bytes(i.body(value))))
        stack.enter_context(patch.object(controller, 'validate', side_effect=lambda value: value));stack.enter_context(patch.object(controller, 'verify_candidate_bridge'))
        stack.enter_context(patch.object(controller, 'runtime_upgrade_module', return_value=upgrade));stack.enter_context(patch.object(controller, 'entry_state', return_value={'phase': 'active'}))
        stack.enter_context(patch.object(controller, 'inspect_container', return_value={'Id': a['candidate']['containerId'], 'Image': a['candidate']['imageId']}))
        stack.enter_context(patch.object(controller.EntryDeployment, 'verify_candidate_row', side_effect=lambda row: self.assertEqual(row['Image'], a['candidate']['imageId'])))
        stack.enter_context(patch.object(controller.EntryDeployment, 'endpoint', return_value='192.0.2.3'));stack.enter_context(patch.object(controller, 'candidate_mounts', return_value=a['mounts']))
        source = root / 'source';source.mkdir(mode=0o700)
        (source / 'deploy-wrapper.py').write_bytes(b'NEW_WRAPPER = True\n');(source / 'release-image.py').write_bytes(b'NEW_STAGE_HELPER = True\n')
        arguments = (source, '8' * 40, i.sha(paths['manifest.json'].read_bytes()), i.sha(paths['ticket.json'].read_bytes()), i.sha((source / 'deploy-wrapper.py').read_bytes()), i.sha((source / 'release-image.py').read_bytes()))
        return paths, t, manifest, history_path, history_before, arguments

    def test_old_active_release_and_startup_gate_remain_valid_after_fixed_ticket_binding_update_and_rollback(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            paths, old_ticket, old_manifest, historical, historical_before, arguments = self.fixture(directory, stack)
            old_wrapper = paths['wrapper.py'].read_bytes()
            original_authorization = controller.read_active_authorization(IMAGE)
            controller.active_record()
            result = i.install(*arguments)
            self.assertTrue(result['installed']);self.assertFalse(result['businessContainersTouched']);self.assertFalse(result['rootStagingSwitchChanged'])
            current_ticket = json.loads(paths['ticket.json'].read_bytes());current_manifest = json.loads(paths['manifest.json'].read_bytes())
            self.assertEqual(current_ticket['hostToolsRevision'], '8' * 40)
            self.assertEqual({k: v for k, v in current_ticket.items() if k != 'hostToolsRevision'}, {k: v for k, v in old_ticket.items() if k != 'hostToolsRevision'})
            self.assertEqual(upgrade.verify_installed_tools(current_ticket), current_manifest)
            self.assertEqual(controller.read_active_authorization(IMAGE), original_authorization)
            controller.active_record()  # Actual startup/active entry path still consumes the old approval.
            self.assertEqual(historical.read_bytes(), historical_before)
            self.assertEqual(upgrade.record_for(original_authorization)['ticket']['hostToolsRevision'], '9' * 40)
            self.assertTrue(i.install(*arguments)['reused'])
            reverted = i.rollback('8' * 40)
            self.assertTrue(reverted['rolledBack']);self.assertEqual(paths['wrapper.py'].read_bytes(), old_wrapper);self.assertFalse(paths['helper.py'].exists())
            self.assertEqual(json.loads(paths['ticket.json'].read_bytes()), old_ticket);self.assertEqual(upgrade.verify_installed_tools(old_ticket), old_manifest)
            self.assertEqual(controller.read_active_authorization(IMAGE), original_authorization);self.assertEqual(historical.read_bytes(), historical_before)
            self.assertTrue(i.install(*arguments)['installed'])
            self.assertEqual(json.loads((Path(directory) / 'installs' / ('image-stage-' + '8' * 40) / 'journal.json').read_bytes())['attempts'], 2)

    def test_failed_post_install_check_restores_original_fixed_files_without_changing_the_release_record(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            paths, old_ticket, old_manifest, historical, historical_before, arguments = self.fixture(directory, stack)
            before = {name: paths[name].read_bytes() for name in ['wrapper.py', 'manifest.json', 'ticket.json']}
            original = i.current_binding;calls = 0
            def fail_after_install(*args):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise RuntimeError('synthetic postcheck failed')
                return original(*args)
            with patch.object(i, 'current_binding', side_effect=fail_after_install), self.assertRaisesRegex(RuntimeError, 'synthetic postcheck'):
                i.install(*arguments)
            for name, raw in before.items():
                self.assertEqual(paths[name].read_bytes(), raw)
            self.assertFalse(paths['helper.py'].exists());self.assertEqual(historical.read_bytes(), historical_before);upgrade.verify_installed_tools(old_ticket)
            result = i.install(*arguments)
            self.assertTrue(result['installed']);self.assertEqual(historical.read_bytes(), historical_before)

    def test_unknown_current_bytes_or_a_pending_ticket_are_rejected_before_installing_anything(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            paths, old_ticket, old_manifest, historical, historical_before, arguments = self.fixture(directory, stack)
            original = paths['wrapper.py'].read_bytes();paths['wrapper.py'].write_bytes(b'foreign wrapper')
            with self.assertRaisesRegex(RuntimeError, 'bytes differ'):
                i.install(*arguments)
            self.assertFalse(paths['helper.py'].exists());paths['wrapper.py'].write_bytes(original)
            record = json.loads(historical_before);record['phase'] = 'holding';historical.write_bytes(i.body(record))
            with self.assertRaisesRegex((ValueError, RuntimeError), 'current_release|current durable PG release'):
                i.install(*arguments)
            self.assertFalse(paths['helper.py'].exists());self.assertEqual(json.loads(paths['manifest.json'].read_bytes()), old_manifest)


if __name__ == '__main__':
    unittest.main()
