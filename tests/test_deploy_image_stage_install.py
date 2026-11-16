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
from unittest.mock import Mock
from types import SimpleNamespace
from test_postgres_runtime_upgrade import ticket, active, target, CONFIG, MATERIALS, IMAGE, upgrade, controller, pg

spec = importlib.util.spec_from_file_location('image_stage_install', Path(__file__).parents[1] / 'ci/install-image-stage.py')
i = importlib.util.module_from_spec(spec);spec.loader.exec_module(i)


class ImageStageInstallTests(unittest.TestCase):
    def bundle_fixture(self, directory, stack, guard=None):
        values, config_path = self.qualification_fixture(directory, stack, guard=guard or Mock(return_value='no'))
        paths, old_ticket, _, history, history_before, arguments = values
        root = Path(directory)
        for field in ['COMPOSE', 'UPGRADE', 'ENTRY', 'MATERIAL', 'USAGE', 'DEPARTMENT']:
            path = root / (field.lower() + '.py');path.write_bytes(('OLD_' + field + ' = True\n').encode());path.chmod(0o644)
            stack.enter_context(patch.object(i, field, path))
        stack.enter_context(patch.object(upgrade, 'ENTRY', i.ENTRY))
        tools = tuple(i.host_sources())
        stack.enter_context(patch.object(upgrade, 'HOST_FILES', tools))
        manifest = json.loads(paths['manifest.json'].read_bytes());manifest['files'] = {name: i.sha(Path(name).read_bytes()) for name in tools};paths['manifest.json'].write_bytes(i.body(manifest))
        paths['helper.py'].write_bytes(b'APPROVED_HELPER = True\n');paths['helper.py'].chmod(0o644)
        source = arguments[0]
        for path, name in i.host_sources().items():
            (source / name).write_bytes(('NEW_' + Path(name).stem.replace('-', '_').upper() + ' = True\n').encode())
        replacements = {path: (source / name).read_bytes() for path, name in i.host_sources().items()}
        arguments = (source, arguments[1], i.sha(paths['manifest.json'].read_bytes()), arguments[3], i.sha(replacements[str(i.WRAPPER)]), i.sha(paths['helper.py'].read_bytes()))
        options = {'runtime': True, 'expected_compose': i.sha(replacements[str(i.COMPOSE)]), 'expected_upgrade': i.sha(replacements[str(i.UPGRADE)]), 'expected_host_bundle': i.sha(i.body(i.host_bundle(arguments[1], replacements)))}
        original_module_bytes = i.module_bytes
        stack.enter_context(patch.object(i, 'module_bytes', side_effect=lambda raw, path, name: upgrade if path == i.UPGRADE else controller if path == i.ENTRY else original_module_bytes(raw, path, name)))
        return paths, old_ticket, history, history_before, arguments, options, replacements, config_path

    def test_bundle_replaces_all_seven_modules_and_qualification_requires_same_ci_source_pin(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            paths, old_ticket, history, old_history, args, options, replacements, _ = self.bundle_fixture(directory, stack)
            before = {name: Path(name).read_bytes() for name in replacements}
            installed = i.install(*args, **options)
            self.assertTrue(installed['installed']);self.assertEqual(installed['installationScope'], 'fixed-seven-host-modules');self.assertEqual(installed['verifiedHostBundleSha256'], options['expected_host_bundle'])
            for name, raw in replacements.items():self.assertEqual(Path(name).read_bytes(), raw)
            self.assertNotEqual(before[str(i.USAGE)], Path(i.USAGE).read_bytes());self.assertNotEqual(before[str(i.DEPARTMENT)], Path(i.DEPARTMENT).read_bytes())
            journal = json.loads((i.HISTORY / ('runtime-tools-' + args[1]) / 'journal.json').read_bytes())
            self.assertEqual(journal['version'], 2);self.assertEqual(set(journal['files']), set(i.host_sources()) | {str(i.HELPER), str(i.MANIFEST), str(i.TICKET)})
            self.assertEqual(journal['hostBundleSha256'], options['expected_host_bundle'])
            with self.assertRaisesRegex(ValueError, 'bundle_pin_required'):i.qualify(args[1], runtime=True)
            with self.assertRaisesRegex(ValueError, 'bundle_source'):i.qualify(args[1], runtime=True, expected_host_bundle='0' * 64)
            ready = i.qualify(args[1], runtime=True, expected_host_bundle=options['expected_host_bundle'])
            self.assertTrue(ready['hostToolsReady']);self.assertFalse(ready['publicationTriggered']);self.assertEqual(ready['installationScope'], 'fixed-seven-host-modules')
            self.assertEqual(ready['verifiedHostBundleSha256'], options['expected_host_bundle']);self.assertEqual(history.read_bytes(), old_history)
            self.assertTrue(i.install(*args, **options)['reused'])
            original_module = i.module
            def restored_entry_only(path, name):
                if path == i.ENTRY:self.assertEqual(Path(path).read_bytes(), before[str(i.ENTRY)])
                return original_module(path, name)
            with patch.object(i, 'module', side_effect=restored_entry_only):restored = i.rollback(args[1], runtime=True)
            for name, raw in before.items():self.assertEqual(Path(name).read_bytes(), raw)
            self.assertEqual(json.loads(paths['ticket.json'].read_bytes()), old_ticket);self.assertEqual(history.read_bytes(), old_history)
            restored_pin = i.sha(i.body(i.host_bundle(old_ticket['hostToolsRevision'], before)))
            self.assertEqual(restored['restoredHostBundleSha256'], restored_pin)
            self.assertTrue(i.qualify(args[1], runtime=True, expected_host_bundle=restored_pin)['hostToolsReady'])
            self.assertTrue(i.install(*args, **options)['installed'])

    def test_bundle_normalizes_unchanged_code_mode_and_restores_its_original_mode(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            _, _, _, _, args, options, replacements, _ = self.bundle_fixture(directory, stack)
            Path(i.USAGE).write_bytes(replacements[str(i.USAGE)]);Path(i.USAGE).chmod(0o600)
            manifest = json.loads(i.MANIFEST.read_bytes());manifest['files'][str(i.USAGE)] = i.sha(Path(i.USAGE).read_bytes());i.MANIFEST.write_bytes(i.body(manifest))
            args = (*args[:2], i.sha(i.MANIFEST.read_bytes()), *args[3:])
            i.install(*args, **options);self.assertEqual(Path(i.USAGE).stat().st_mode & 0o777, 0o644)
            i.rollback(args[1], runtime=True);self.assertEqual(Path(i.USAGE).stat().st_mode & 0o777, 0o600)

    def test_bundle_rollback_authorization_never_indirectly_imports_broken_new_entry(self):
        real_module_bytes = i.module_bytes
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            _, old_ticket, history, history_before, args, options, replacements, _ = self.bundle_fixture(directory, stack)
            before = {name: (Path(name).read_bytes(), Path(name).stat().st_mode & 0o777) for name in replacements}
            replacements[str(i.ENTRY)] = b'raise RuntimeError("new_entry_must_not_execute_during_recovery")\n'
            (args[0] / 'gateway-entry-controller.py').write_bytes(replacements[str(i.ENTRY)])
            options['expected_host_bundle'] = i.sha(i.body(i.host_bundle(args[1], replacements)))
            i.install(*args, **options)
            journal_path = i.HISTORY / ('runtime-tools-' + args[1]) / 'journal.json'
            journal = json.loads(journal_path.read_bytes());journal['state'] = 'prepared';journal_path.write_bytes(i.body(journal))
            def real_entry_loader(path, name):
                self.assertEqual(Path(path), i.ENTRY)
                return real_module_bytes(i.read(Path(path)), Path(path), name)
            # Keep the real active_record -> read_active_authorization ->
            # upgrade.active_authorization chain. If any indirect loader reads
            # the installed new ENTRY, its actual module body raises above.
            with patch.object(upgrade, 'load', side_effect=real_entry_loader), patch.object(controller, 'read_active_authorization', wraps=controller.read_active_authorization) as authorization, patch.object(upgrade, 'active_authorization', wraps=upgrade.active_authorization) as upgrade_authorization:
                result = i.rollback(args[1], runtime=True)
                self.assertTrue(result['rolledBack']);self.assertGreater(authorization.call_count, 0);self.assertGreater(upgrade_authorization.call_count, 0)
            self.assertEqual(before, {name: (Path(name).read_bytes(), Path(name).stat().st_mode & 0o777) for name in before})
            self.assertEqual(history.read_bytes(), history_before);upgrade.verify_installed_tools(old_ticket)

    def test_bundle_source_pin_rejects_changed_missing_and_wrong_mode_before_writing(self):
        for change in ['usage', 'department', 'entry', 'material', 'missing', 'wrong-pin', 'non-runtime']:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                paths, _, _, _, args, options, replacements, _ = self.bundle_fixture(directory, stack)
                before = {name: Path(name).read_bytes() for name in replacements}
                target = {'usage': i.USAGE, 'department': i.DEPARTMENT, 'entry': i.ENTRY, 'material': i.MATERIAL}.get(change)
                if target:(args[0] / i.host_sources()[str(target)]).write_bytes(b'FOREIGN = True\n')
                elif change == 'missing':(args[0] / 'postgres-usage-cascade.py').unlink()
                elif change == 'wrong-pin':options['expected_host_bundle'] = '0' * 64
                else:options['runtime'] = False
                with self.assertRaises((ValueError, FileNotFoundError)):i.install(*args, **options)
                self.assertEqual(before, {name: Path(name).read_bytes() for name in before});self.assertFalse(i.HISTORY.exists())

    def test_bundle_requires_new_revision_and_preserves_complete_or_rolled_back_v1_history(self):
        for rollback_first in [False, True]:
            with self.subTest(rollback_first=rollback_first), tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                _, _, _, _, args, options, _, _ = self.bundle_fixture(directory, stack)
                legacy_options = {key: value for key, value in options.items() if key != 'expected_host_bundle'}
                i.install(*args, **legacy_options)
                if rollback_first:i.rollback(args[1], runtime=True)
                before = {path: path.read_bytes() for path in Path(directory).rglob('*') if path.is_file()}
                current_args = (*args[:2], i.sha(i.MANIFEST.read_bytes()), i.sha(i.TICKET.read_bytes()), *args[4:])
                with self.assertRaisesRegex(ValueError, 'existing_record'):i.install(*current_args, **options)
                self.assertEqual(before, {path: path.read_bytes() for path in before})
                journal = json.loads((i.HISTORY / ('runtime-tools-' + args[1]) / 'journal.json').read_bytes())
                self.assertEqual(journal['version'], 1);self.assertEqual(journal['state'], 'rolled-back' if rollback_first else 'complete')
                self.assertEqual(i.qualify(args[1], runtime=True)['installationScope'], 'legacy-three-runtime-files')

    def test_bundle_postcheck_failure_and_explicit_rollback_restore_each_module_byte_and_mode(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            paths, old_ticket, history, old_history, args, options, replacements, _ = self.bundle_fixture(directory, stack)
            before = {name: (Path(name).read_bytes(), Path(name).stat().st_mode & 0o777) for name in replacements}
            original = i.current_binding;count = 0
            def postcheck(*arguments, **kwargs):
                nonlocal count
                count += 1
                if count == 2:raise RuntimeError('synthetic bundle postcheck')
                return original(*arguments, **kwargs)
            with patch.object(i, 'current_binding', side_effect=postcheck), self.assertRaisesRegex(RuntimeError, 'bundle postcheck'):i.install(*args, **options)
            self.assertEqual(before, {name: (Path(name).read_bytes(), Path(name).stat().st_mode & 0o777) for name in before})
            self.assertEqual(history.read_bytes(), old_history);upgrade.verify_installed_tools(old_ticket)
            i.install(*args, **options)
            Path(i.ENTRY).write_bytes(b'BROKEN =\n');Path(i.UPGRADE).write_bytes(b'BROKEN =\n')
            with self.assertRaisesRegex(ValueError, 'rollback_changed'):i.rollback(args[1], runtime=True)
            Path(i.ENTRY).write_bytes(replacements[str(i.ENTRY)]);Path(i.UPGRADE).write_bytes(replacements[str(i.UPGRADE)])
            i.rollback(args[1], runtime=True)
            self.assertEqual(before, {name: (Path(name).read_bytes(), Path(name).stat().st_mode & 0o777) for name in before})

    def test_bundle_qualification_rejects_revision_only_relabel_and_changes_during_guard(self):
        for target in ['USAGE', 'DEPARTMENT', 'ENTRY', 'MATERIAL']:
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                guard = Mock(return_value='no')
                _, _, _, _, args, options, _, _ = self.bundle_fixture(directory, stack, guard)
                i.install(*args, **options)
                path = getattr(i, target);original = path.read_bytes();path.write_bytes(b'OLD_MAINTENANCE = True\n')
                with self.assertRaisesRegex(ValueError, 'qualification_bytes'):i.qualify(args[1], runtime=True, expected_host_bundle=options['expected_host_bundle'])
                path.write_bytes(original)
                guard.side_effect = lambda config: (path.write_bytes(b'CHANGED_DURING_GUARD = True\n'), 'no')[1]
                with self.assertRaisesRegex((ValueError, RuntimeError), 'qualification_bytes_changed|installed host tool bytes differ'):i.qualify(args[1], runtime=True, expected_host_bundle=options['expected_host_bundle'])

    def fixture(self, directory, stack):
        root = Path(directory)
        paths = {name: root / name for name in ['wrapper.py', 'helper.py', 'manifest.json', 'ticket.json', 'active.json', 'candidate.json']}
        tools = (str(paths['wrapper.py']), *(str(root / ('host-' + str(n) + '.py')) for n in range(3)))
        for name in tools:
            Path(name).write_bytes(('synthetic old file ' + name).encode());Path(name).chmod(0o600)
        paths['wrapper.py'].chmod(0o755)
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
            path = Path(path)
            if not path.exists():raise FileNotFoundError(path)
            self.assertTrue(path.is_file() and not path.is_symlink())
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
            self.assertFalse(result['hostToolsReady']);self.assertTrue(result['qualificationRequired'])
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

    def qualification_fixture(self, directory, stack, config=None, guard=None):
        values = self.fixture(directory, stack)
        config_path = Path(directory) / 'root-config.json'
        config_path.write_bytes(i.body(config or CONFIG));config_path.chmod(0o600)
        stack.enter_context(patch.object(i, 'CONFIG', config_path))
        composition = SimpleNamespace(storage_restart_policy=guard or pg.storage_restart_policy)
        stack.enter_context(patch.object(i, 'module', side_effect=lambda path, name: upgrade if path == i.UPGRADE else composition if path == i.COMPOSE else controller))
        return values, config_path

    def test_guard_refresh_is_required_after_install_and_qualification_is_readonly(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            guard = Mock(side_effect=RuntimeError('host_approved_tools_refresh_required'))
            values, _ = self.qualification_fixture(directory, stack, {**CONFIG, 'postgres_storage_startup_guard': True}, guard)
            paths, _, _, historical, history_before, arguments = values
            result = i.install(*arguments);self.assertFalse(result['hostToolsReady'])
            snapshot = {p: (p.read_bytes(),p.stat().st_mtime_ns,p.stat().st_ctime_ns,p.stat().st_ino) for p in Path(directory).rglob('*') if p.is_file()}
            controller.entry_state.reset_mock()
            with patch.object(controller,'save_json',side_effect=AssertionError('qualification must not save any state')) as save,patch.object(controller,'active_record',side_effect=AssertionError('qualification must not refresh active')):
                with self.assertRaisesRegex(RuntimeError, 'refresh_required'):
                    i.qualify('8' * 40)
                self.assertEqual(snapshot, {p: (p.read_bytes(),p.stat().st_mtime_ns,p.stat().st_ctime_ns,p.stat().st_ino) for p in snapshot})
                guard.side_effect = None;guard.return_value = 'no'
                ready = i.qualify('8' * 40)
                self.assertTrue(ready['hostToolsReady']);self.assertTrue(ready['storageStartupGuardEnabled']);self.assertFalse(ready['publicationTriggered'])
                self.assertEqual(snapshot, {p: (p.read_bytes(),p.stat().st_mtime_ns,p.stat().st_ctime_ns,p.stat().st_ino) for p in snapshot});self.assertEqual(historical.read_bytes(), history_before);save.assert_not_called()
                self.assertTrue(controller.entry_state.call_args_list)
                self.assertTrue(all(not call.args and not call.kwargs for call in controller.entry_state.call_args_list))
            self.assertFalse(i.install(*arguments)['hostToolsReady'])  # Never cache live supervisor readiness.

    def test_readonly_qualification_rejects_address_drift_without_persisting_it(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            values, _ = self.qualification_fixture(directory, stack)
            paths = values[0];i.install(*values[-1])
            before = {p: (p.read_bytes(),p.stat().st_mtime_ns,p.stat().st_ctime_ns,p.stat().st_ino) for p in paths.values() if p.exists()}
            with patch.object(controller.EntryDeployment,'endpoint',return_value='192.0.2.99') as endpoint,patch.object(controller,'save_json',side_effect=AssertionError('read-only qualification saved state')) as save:
                with self.assertRaisesRegex(ValueError,'active_address_changed'):i.qualify('8' * 40)
            self.assertEqual(endpoint.call_count,1);save.assert_not_called()
            self.assertEqual(before,{p: (p.read_bytes(),p.stat().st_mtime_ns,p.stat().st_ctime_ns,p.stat().st_ino) for p in before})

    def test_readonly_qualification_rejects_changed_slot_or_database_approval(self):
        for change in ['slot','authorization']:
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                values, _ = self.qualification_fixture(directory, stack)
                paths = values[0];i.install(*values[-1])
                if change=='slot':paths['candidate.json'].write_bytes(i.body({'socketPath':'/run/other.sock'}))
                with patch.object(controller,'save_json',side_effect=AssertionError('failed qualification saved state')) as save:
                    with ExitStack() as checks:
                        if change=='authorization':checks.enter_context(patch.object(controller,'read_active_authorization',return_value={'state':'revoked'}))
                        with self.assertRaises((ValueError,RuntimeError)):i.qualify('8' * 40)
                    save.assert_not_called()

    def test_unmigrated_configuration_keeps_legacy_restart_policy(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            values, _ = self.qualification_fixture(directory, stack)
            i.install(*values[-1]);ready = i.qualify('8' * 40)
            self.assertTrue(ready['hostToolsReady']);self.assertFalse(ready['storageStartupGuardEnabled'])
            i.rollback('8' * 40)
            self.assertTrue(i.qualify('8' * 40)['hostToolsReady'])

    def test_equivalent_recovered_ticket_encoding_is_not_an_approval_change(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            values, _ = self.qualification_fixture(directory, stack)
            paths = values[0];i.install(*values[-1])
            for name in ['ticket.json','manifest.json']:
                path=paths[name];path.write_text(json.dumps(json.loads(path.read_bytes()),indent=2)+'\n')
            before={paths[name]:paths[name].read_bytes() for name in ['ticket.json','manifest.json']}
            with patch.object(controller,'save_json',side_effect=AssertionError('qualification rewrote encoding')):
                self.assertTrue(i.qualify('8' * 40)['hostToolsReady'])
            self.assertEqual(before,{path:path.read_bytes() for path in before})
            i.rollback('8' * 40)
            path=paths['ticket.json'];path.write_text(json.dumps(json.loads(path.read_bytes()),indent=2)+'\n')
            self.assertTrue(i.qualify('8' * 40)['hostToolsReady'])

    def test_guard_observation_cannot_reencode_ticket_during_qualification(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            guard=Mock(return_value='no')
            values, _ = self.qualification_fixture(directory, stack,{**CONFIG,'postgres_storage_startup_guard':True},guard)
            paths=values[0];i.install(*values[-1])
            def rewrite(config):
                path=paths['ticket.json'];path.write_text(json.dumps(json.loads(path.read_bytes()),indent=2)+'\n');return 'no'
            guard.side_effect=rewrite
            with self.assertRaisesRegex(ValueError,'qualification_bytes_changed'):i.qualify('8' * 40)

    def test_equivalent_json_exception_does_not_allow_executable_or_approval_change(self):
        for change in ['wrapper','ticket']:
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                values, _ = self.qualification_fixture(directory, stack)
                paths=values[0];i.install(*values[-1])
                path=paths['wrapper.py'] if change=='wrapper' else paths['ticket.json']
                value=b'NEW_WRAPPER = True\n\n' if change=='wrapper' else i.body({**json.loads(path.read_bytes()),'approved':False})
                path.write_bytes(value)
                with self.assertRaises((ValueError,RuntimeError)):i.qualify('8' * 40)
                with self.assertRaisesRegex(ValueError,'rollback_changed'):i.rollback('8' * 40)
                self.assertEqual(path.read_bytes(),value)

    def test_qualification_rejects_lost_wrapper_execution_permission(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            values, _ = self.qualification_fixture(directory, stack)
            paths=values[0];i.install(*values[-1]);before=paths['wrapper.py'].read_bytes();paths['wrapper.py'].chmod(0o644)
            with self.assertRaisesRegex(ValueError,'qualification_mode'):i.qualify('8' * 40)
            self.assertEqual(paths['wrapper.py'].read_bytes(),before);self.assertEqual(paths['wrapper.py'].stat().st_mode&0o777,0o644)

    def test_qualification_rejects_mode_change_during_guard_observation(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            guard=Mock(return_value='no')
            values, _ = self.qualification_fixture(directory, stack,{**CONFIG,'postgres_storage_startup_guard':True},guard)
            paths=values[0];i.install(*values[-1])
            def lose_execute(config):paths['wrapper.py'].chmod(0o644);return 'no'
            guard.side_effect=lose_execute
            with self.assertRaisesRegex(ValueError,'qualification_mode_changed'):i.qualify('8' * 40)

    def pending_fixture(self, directory, stack, guard=None, bundled=False):
        configuration={**CONFIG,'storage_engine':'postgres','local_assurance_release':True,'entry_installed_release':True,'image_prefix':IMAGE.rsplit(':',1)[0],'postgres_network':'owned-pg-net','postgres_storage_startup_guard':True}
        guard=guard or Mock(return_value='no')
        values, config_path=self.qualification_fixture(directory,stack,configuration,guard)
        paths, old_ticket, _, history, _, arguments=values;root=Path(directory)
        compose, next_upgrade=root/'composition.py',root/'upgrade.py'
        for path in [compose,next_upgrade,root/'host-3.py']:
            path.write_bytes(b'SYNTHETIC_TOOL = True\n');path.chmod(0o600)
        stack.enter_context(patch.object(i,'COMPOSE',compose));stack.enter_context(patch.object(i,'UPGRADE',next_upgrade))
        tools=(*upgrade.HOST_FILES,str(compose),str(next_upgrade),str(root/'host-3.py'))
        stack.enter_context(patch.object(upgrade,'HOST_FILES',tools))
        manifest=json.loads(paths['manifest.json'].read_bytes());manifest['files']={name:i.sha(Path(name).read_bytes()) for name in tools};paths['manifest.json'].write_bytes(i.body(manifest))
        paths['helper.py'].write_bytes(b'APPROVED_HELPER = True\n');paths['helper.py'].chmod(0o644)
        source=arguments[0]
        (source/'postgres-deploy.py').write_bytes(b'APPROVED_COMPOSE = True\n');(source/'postgres-upgrade-deploy.py').write_bytes(b'APPROVED_UPGRADE = True\n')
        arguments=(*arguments[:2],i.sha(paths['manifest.json'].read_bytes()),*arguments[3:5],i.sha(paths['helper.py'].read_bytes()))
        historical=json.loads(history.read_bytes());historical['workerId']='d'*64;historical['candidate']['imageId']=old_ticket['to']['configDigest'];history.write_bytes(i.body(historical))
        bundle_pin = None
        if bundled:
            for field, suffix in [('ENTRY', 0), ('MATERIAL', 1), ('USAGE', 2), ('DEPARTMENT', 3)]:stack.enter_context(patch.object(i, field, root / ('host-' + str(suffix) + '.py')))
            for name in ['gateway-entry-controller.py', 'harbor-material-storage.py', 'postgres-usage-cascade.py', 'postgres-department-release.py']:(source / name).write_bytes(b'APPROVED_BUNDLE_MODULE = True\n')
            bundle_pin = i.sha(i.body(i.host_bundle(arguments[1], {path: (source / name).read_bytes() for path, name in i.host_sources().items()})))
        i.install(*arguments,runtime=True,expected_compose=i.sha((source/'postgres-deploy.py').read_bytes()),expected_upgrade=i.sha((source/'postgres-upgrade-deploy.py').read_bytes()),expected_host_bundle=bundle_pin)
        current=json.loads(paths['active.json'].read_bytes());source_ticket=json.loads(paths['ticket.json'].read_bytes());pending=copy.deepcopy(source_ticket)
        pending['deploymentId']='00000000-0000-0000-0000-000000000003'
        pending['to']={'image':configuration['image_prefix']+':'+'e'*40,'revision':'e'*40,'configDigest':'sha256:'+'e'*64,'manifestDigest':'sha256:'+'f'*64}
        pending['from']={'containerId':current['candidate']['containerId'],'workerId':'d'*64,'imageId':current['candidate']['imageId'],'image':current['image'],'activeIdentityDigest':upgrade.digest(upgrade.active_identity(current))}
        paths['ticket.json'].write_bytes(i.body(pending))
        rows={}
        for role in ['gateway','worker']:
            row=target(source_ticket,role);row['Config']['Labels'].update({'com.harbor.activation-deployment-id':current['approval']['deploymentId'],'com.harbor.activation-receipt':current['approval']['receiptDigest']});rows[row['Id']]=row
        stack.enter_context(patch.object(controller,'inspect_container',side_effect=lambda identifier:rows[identifier]))
        controller.entry_state.return_value={'phase':'active','slot':'candidate'}
        network={'Id':pending['network']['id'],'Driver':'bridge','Internal':False,'Containers':{identifier:{} for identifier in [*pending['network']['infrastructureContainerIds'],*rows]}}
        def execute(args):
            if args[:3]==['/usr/bin/docker','network','inspect']:return json.dumps([network])
            if args[:6]==['/usr/sbin/ip','-j','link','show','dev',pending['network']['bridge']]:return json.dumps([{'ifindex':pending['network']['bridgeIfindex'],'address':pending['network']['bridgeMac']}])
            if args[:6]==['/usr/sbin/ip','-j','link','show','master',pending['network']['bridge']]:return '[]'
            raise AssertionError('qualification attempted non-readonly command')
        stack.enter_context(patch.object(controller,'execute',side_effect=execute))
        return paths,pending,rows,network,guard,source_ticket,history,config_path

    def test_full_bundle_fresh_pending_qualification_pins_ci_modules_without_writes(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            paths, pending, _, _, _, _, _, _ = self.pending_fixture(directory, stack, bundled=True)
            journal = json.loads((i.HISTORY / ('runtime-tools-' + '8' * 40) / 'journal.json').read_bytes())
            before = {p: (p.read_bytes(), p.stat().st_mtime_ns, p.stat().st_ctime_ns, p.stat().st_ino) for p in Path(directory).rglob('*') if p.is_file()}
            with patch.object(controller, 'save_json', side_effect=AssertionError('read-only bundle qualification wrote state')):
                ready = i.qualify('8' * 40, runtime=True, expected_host_bundle=journal['hostBundleSha256'], pending_deployment_id=pending['deploymentId'], pending_ticket_digest=upgrade.digest(pending))
            self.assertTrue(ready['hostToolsReady']);self.assertFalse(ready['publicationTriggered']);self.assertEqual(ready['verifiedHostBundleSha256'], journal['hostBundleSha256'])
            self.assertEqual(before, {p: (p.read_bytes(), p.stat().st_mtime_ns, p.stat().st_ctime_ns, p.stat().st_ino) for p in before})

    def test_legacy_runtime_qualification_cannot_claim_a_full_ci_bundle(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            _, pending, _, _, _, _, _, _ = self.pending_fixture(directory, stack)
            with self.assertRaisesRegex(ValueError, 'bundle_pin_required'):
                i.qualify('8' * 40, runtime=True, expected_host_bundle='0' * 64, pending_deployment_id=pending['deploymentId'], pending_ticket_digest=upgrade.digest(pending))

    def test_explicit_fresh_pending_is_readonly_and_bound_to_original_source(self):
        with tempfile.TemporaryDirectory() as directory,ExitStack() as stack:
            paths,pending,rows,network,guard,_,_,_=self.pending_fixture(directory,stack)
            snapshot={p:(p.read_bytes(),p.stat().st_mtime_ns,p.stat().st_ctime_ns) for p in Path(directory).rglob('*') if p.is_file()}
            controller.entry_state.reset_mock()
            with patch.object(controller,'save_json',side_effect=AssertionError('pending qualification wrote state')) as save:
                ready=i.qualify('8'*40,runtime=True,pending_deployment_id=pending['deploymentId'],pending_ticket_digest=upgrade.digest(pending))
            self.assertTrue(ready['hostToolsReady']);self.assertFalse(ready['publicationTriggered']);self.assertEqual(ready['pendingTicketDigest'],upgrade.digest(pending));save.assert_not_called()
            self.assertTrue(all(not call.args and not call.kwargs for call in controller.entry_state.call_args_list));self.assertEqual(snapshot,{p:(p.read_bytes(),p.stat().st_mtime_ns,p.stat().st_ctime_ns) for p in snapshot})
            with self.assertRaises((ValueError,RuntimeError)):i.qualify('8'*40,runtime=True)  # No implicit broadening of the completed-source path.

    def test_pending_parameters_must_be_paired_and_exact(self):
        for change in ['missing-id','missing-digest','wrong-id','wrong-digest','non-runtime']:
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory,ExitStack() as stack:
                paths,pending,_,_,guard,_,_,_=self.pending_fixture(directory,stack)
                options={'runtime':True,'pending_deployment_id':pending['deploymentId'],'pending_ticket_digest':upgrade.digest(pending)}
                if change=='missing-id':options['pending_deployment_id']=None
                elif change=='missing-digest':options['pending_ticket_digest']=None
                elif change=='wrong-id':options['pending_deployment_id']='00000000-0000-0000-0000-000000000004'
                elif change=='wrong-digest':options['pending_ticket_digest']='0'*64
                else:options['runtime']=False
                with self.assertRaises((ValueError,RuntimeError)):i.qualify('8'*40,**options)
                guard.assert_not_called()

    def test_pending_rejects_started_or_consumed_release_including_failed_old_ticket(self):
        for kind in ['rolled-back-durable','runner','preflights','dangling-preflight']:
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as directory,ExitStack() as stack:
                _,pending,_,_,guard,_,_,_=self.pending_fixture(directory,stack);identifier=pending['deploymentId']
                if kind=='rolled-back-durable':
                    path=upgrade.release_path(identifier);path.mkdir();(path/'release.json').write_bytes(i.body({'phase':'rolled-back-pg','ticket':pending}))
                else:
                    path=upgrade.RELEASES/'runners'/(identifier if kind=='runner' else identifier+'-preflights');path.parent.mkdir(exist_ok=True)
                    if kind=='dangling-preflight':path.symlink_to(Path(directory)/'missing')
                    else:path.mkdir()
                with self.assertRaisesRegex(ValueError,'pending_(consumed|started)'):i.qualify('8'*40,runtime=True,pending_deployment_id=identifier,pending_ticket_digest=upgrade.digest(pending))
                guard.assert_not_called()

    def test_pending_requires_actual_source_materials_database_network_and_health(self):
        for change in ['database','materials','source','network','health']:
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory,ExitStack() as stack:
                paths,pending,rows,network,guard,_,_,_=self.pending_fixture(directory,stack)
                if change=='database':pending['database']['name']='other_owned_database'
                elif change=='materials':pending['materialsDirectory']=MATERIALS.rsplit('/',1)[0]+'/00000000-0000-0000-0000-000000000004'
                elif change=='source':rows[pending['from']['containerId']]['Id']='7'*64
                elif change=='network':network['Id']='7'*64
                else:rows[pending['from']['workerId']]['State']['Health']['Status']='unhealthy'
                paths['ticket.json'].write_bytes(i.body(pending))
                with self.assertRaises((ValueError,RuntimeError)):i.qualify('8'*40,runtime=True,pending_deployment_id=pending['deploymentId'],pending_ticket_digest=upgrade.digest(pending))
                guard.assert_not_called()

    def test_pending_rechecks_consumption_and_raw_ticket_after_guard(self):
        for change in ['durable','runner','ticket']:
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory,ExitStack() as stack:
                paths,pending,_,_,guard,_,_,_=self.pending_fixture(directory,stack)
                def consume(config):
                    if change=='durable':upgrade.release_path(pending['deploymentId']).mkdir()
                    elif change=='runner':(upgrade.RELEASES/'runners'/pending['deploymentId']).mkdir(parents=True)
                    else:paths['ticket.json'].write_text(json.dumps(pending,indent=2)+'\n')
                    return 'no'
                guard.side_effect=consume
                with self.assertRaises((ValueError,RuntimeError)):i.qualify('8'*40,runtime=True,pending_deployment_id=pending['deploymentId'],pending_ticket_digest=upgrade.digest(pending))

    def test_existing_host_contract_accepts_pending_without_rebinding_history(self):
        with tempfile.TemporaryDirectory() as directory,ExitStack() as stack:
            guard=Mock();paths,pending,rows,_,_,source_ticket,history,_=self.pending_fixture(directory,stack,guard)
            spec=importlib.util.spec_from_file_location('unchanged_storage_host_contract',Path(__file__).parents[1]/'ci/harbor-pg-storage-host.py')
            host=importlib.util.module_from_spec(spec);spec.loader.exec_module(host)
            manifest=json.loads(paths['manifest.json'].read_bytes());material=Path(directory)/'host-0.py'
            mutable=frozenset([str(paths['wrapper.py']),str(i.COMPOSE),str(i.UPGRADE),str(Path(directory)/'host-3.py')])
            plan={'scope':'harbor-pg-storage-move-v1','runId':'a'*32,'systemId':pending['database']['cluster'],'materialsDirectory':MATERIALS,'toolSha256':{str(material):manifest['files'][str(material)]}}
            journal={'scope':plan['scope'],'runId':plan['runId'],'phase':'complete','oldDataRollbackForbidden':True}
            current=json.loads(paths['active.json'].read_bytes())
            mapping={str(paths['manifest.json']):paths['manifest.json'],str(paths['ticket.json']):paths['ticket.json'],'/var/lib/harbor-entry/active.json':paths['active.json'],'/var/lib/harbor-postgres-upgrades/'+current['approval']['deploymentId']+'/release.json':history}
            def docker(args):
                self.assertEqual(args[:3],['/usr/bin/docker','container','inspect'])
                identifier=pending['from']['containerId'] if args[-1]==host.GW else pending['from']['workerId']
                return json.dumps([rows[identifier]])
            for name,value in [('RUNTIME_MANIFEST',paths['manifest.json']),('RUNTIME_TICKET',paths['ticket.json']),('MATERIAL',material),('MUTABLE_TOOLS',mutable),('RUNTIME_TOOLS',frozenset(manifest['files']))]:stack.enter_context(patch.object(host,name,value))
            stack.enter_context(patch.object(host,'private',side_effect=lambda path,*args:mapping[str(path)].read_bytes()))
            stack.enter_context(patch.object(host,'code_bytes',side_effect=lambda path:Path(path).read_bytes()))
            stack.enter_context(patch.object(host,'run',side_effect=docker))
            original_history=history.read_bytes();paths['ticket.json'].write_bytes(i.body(source_ticket))
            with self.assertRaisesRegex(RuntimeError,'host_runtime_ticket_not_current'):host.verified_runtime_tools(plan,journal)
            paths['ticket.json'].write_bytes(i.body(pending))
            def real_guard(config):
                self.assertEqual(host.verified_runtime_tools(plan,journal),{name:manifest['files'][name] for name in mutable});return 'no'
            guard.side_effect=real_guard
            ready=i.qualify('8'*40,runtime=True,pending_deployment_id=pending['deploymentId'],pending_ticket_digest=upgrade.digest(pending))
            self.assertTrue(ready['hostToolsReady']);self.assertEqual(history.read_bytes(),original_history)

    def test_qualification_rejects_configuration_or_tool_changes_during_guard(self):
        for change in ['config', 'wrapper']:
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                guard = Mock(return_value='no')
                values, config_path = self.qualification_fixture(directory, stack, {**CONFIG, 'postgres_storage_startup_guard': True}, guard)
                paths = values[0];i.install(*values[-1])
                def mutate(config):
                    (config_path if change == 'config' else paths['wrapper.py']).write_bytes(i.body({'changed': True}) if change == 'config' else b'changed wrapper')
                    return 'no'
                guard.side_effect = mutate
                with self.assertRaises((ValueError, RuntimeError)):
                    i.qualify('8' * 40)

    def test_qualification_refuses_pending_installation_journal_or_pending_release_ticket(self):
        for change in ['journal', 'ticket']:
            with self.subTest(change=change),tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                values, _ = self.qualification_fixture(directory, stack)
                paths = values[0];i.install(*values[-1])
                if change == 'journal':
                    journal = Path(directory) / 'installs' / ('image-stage-' + '8' * 40) / 'journal.json'
                    value = json.loads(journal.read_bytes());value['state'] = 'prepared';journal.write_bytes(i.body(value))
                else:
                    value = json.loads(paths['ticket.json'].read_bytes());value['deploymentId'] = '00000000-0000-0000-0000-000000000099';paths['ticket.json'].write_bytes(i.body(value))
                with self.assertRaises((ValueError, RuntimeError)):
                    i.qualify('8' * 40)


if __name__ == '__main__':
    unittest.main()
