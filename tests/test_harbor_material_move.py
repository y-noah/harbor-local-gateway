import copy
import contextlib
import json
import importlib.util
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

PATH = Path(__file__).resolve().parents[1] / 'ci/harbor-material-move.py'
spec = importlib.util.spec_from_file_location('material_move', PATH)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class Tests(unittest.TestCase):
    def test_real_cold_copy_preserves_bytes_modes_and_original_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory) / 'source', Path(directory) / 'target'
            source.mkdir(mode=0o700)
            target.mkdir(mode=0o700)
            (source / 'nested').mkdir(mode=0o700)
            (source / 'nested' / 'large.bin').write_bytes(bytes(range(256)) * 8192)
            (source / 'nested' / 'large.bin').chmod(0o600)
            (source / 'metadata.json').write_text('{"synthetic":true}')
            (source / 'metadata.json').chmod(0o644)
            before = m.scan(source, os.getuid(), digest=True)
            checks = []
            proof = m.copy_cold(source, target, os.getuid(), lambda: checks.append(True))
            self.assertEqual(m.scan(source, os.getuid(), digest=True), before)
            self.assertEqual(m.semantic(proof['sourceTree']), m.semantic(proof['targetTree']))
            self.assertEqual((source / 'nested/large.bin').read_bytes(), (target / 'nested/large.bin').read_bytes())
            self.assertEqual(len(checks), 2)

    def test_cold_copy_refuses_before_write_when_writers_not_fenced(self):
        with patch.object(m, 'scan', side_effect=AssertionError('must not read')):
            with self.assertRaisesRegex(ValueError, 'busy'):
                m.copy_cold('/source', '/target', 0, lambda: (_ for _ in ()).throw(ValueError('busy')))

    def test_source_change_during_copy_is_rejected_and_both_trees_remain(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target = Path(directory) / 'source', Path(directory) / 'target'
            source.mkdir(mode=0o700)
            target.mkdir(mode=0o700)
            file = source / 'synthetic'
            file.write_bytes(b'before')
            file.chmod(0o600)
            calls = []
            def check():
                calls.append(True)
                if len(calls) == 2:
                    file.write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'cold_copy_mismatch'):
                m.copy_cold(source, target, os.getuid(), check)
            self.assertTrue(file.exists())
            self.assertTrue((target / file.name).exists())

    def test_inventory_rejects_links_writable_entries_and_special_files(self):
        for case in ['symlink', 'hardlink', 'writable', 'fifo']:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                file = root / 'file'
                file.write_bytes(b'synthetic')
                file.chmod(0o600)
                if case == 'symlink':
                    (root / 'link').symlink_to(file)
                elif case == 'hardlink':
                    os.link(file, root / 'link')
                elif case == 'writable':
                    file.chmod(0o666)
                else:
                    os.mkfifo(root / 'fifo', 0o600)
                with self.assertRaisesRegex(ValueError, 'tree_untrusted'):
                    m.scan(root, os.getuid(), digest=True)

    def test_rollback_forbidden_after_completed_activation_before_any_mutation(self):
        with patch.object(m.g, 'disk_mount', side_effect=AssertionError('must not observe')):
            with self.assertRaisesRegex(ValueError, 'rollback_after_activation_forbidden'):
                m.rollback({}, {'phase': 'complete'})

    def test_rollback_refuses_changed_target_before_unmount_or_rename(self):
        state = {'phase': 'switching', 'rows': [{'logical': '/source', 'target': '/target', 'uid': 0,
                 'purpose': 'application-data', 'proof': {'sourceTree': {'file': {'identity': [1], 'sha256': 'before'}}, 'targetTree': {'file': {'identity': [1], 'sha256': 'before'}}}}]}
        with patch.object(m.g, 'disk_mount'), patch.object(m, 'quiescent'), patch.object(m, 'scan', return_value={'file': {'identity': [2], 'sha256': 'after'}}), patch.object(m.os, 'rename', side_effect=AssertionError('must not mutate')):
            with self.assertRaisesRegex(ValueError, 'copied_target_changed'):
                m.rollback({}, state)

    def test_complete_switch_reentry_only_checks_guards_and_does_not_start_apps(self):
        with patch.object(m.g, 'guard', return_value={'enabled': True}) as guard, patch.object(m, 'quiescent', side_effect=AssertionError('already resumed apps are allowed')):
            value = m.switch({}, {'phase': 'complete'})
            self.assertTrue(value['alreadyComplete'])
            self.assertFalse(value['applicationsStarted'])
            guard.assert_called_once()

class CutoverTests(unittest.TestCase):
    def fixture(self, directory):
        root = Path(directory)
        source, target = root / 'logical', root / 'physical'
        source.mkdir(mode=0o700)
        target.mkdir(mode=0o700)
        (source / 'synthetic').write_bytes(b'preserved')
        (source / 'synthetic').chmod(0o600)
        proof = m.copy_cold(source, target, os.getuid(), lambda: None)
        row = {'purpose': 'application-data', 'logical': str(source), 'target': str(target),
               'uid': os.getuid(), 'sourceId': m.g.identity(source)[:2], 'targetId': m.g.identity(target)[:2],
               'phase': 'copied', 'temporary': False, 'proof': proof}
        return root, row, {'phase': 'copied', 'rows': [row]}, {'nonce': 'a' * 8 + '-aaaa-aaaa-aaaa-' + 'a' * 12, 'diskUuid': '12345678-abcd'}

    @contextlib.contextmanager
    def harness(self, root, row, failure=None):
        mounted_paths = set()
        calls = []
        real_identity = m.g.identity
        unit_root = root / 'units'
        unit_root.mkdir(mode=0o700)
        def identity(path):
            return real_identity(row['target'] if str(path) in mounted_paths else path)
        def run(args, **kwargs):
            calls.append(args)
            if args[0] == '/usr/bin/systemd-escape':
                return b'owned.mount\n'
            if args[1] == 'start':
                if failure and failure[0]:
                    failure[0] = False
                    raise ValueError('synthetic_start_failure')
                mounted_paths.add(row['logical'])
            elif args[1] == 'stop':
                mounted_paths.discard(row['logical'])
            return b''
        with contextlib.ExitStack() as stack:
            for obj, name, val in [(m, 'STATE', root / 'state.json'), (m, 'UNIT_ROOT', unit_root),
                                   (m.g, 'CONFIG', root / 'config.json'), (m.g, 'TARGETS', {'application-data': 'data'}),
                                   (m.g, 'identity', identity), (m.g, 'directory', lambda *a, **k: None),
                                   (m.g, 'disk_mount', lambda *a: 9), (m.g, 'boot_unit', lambda *a: 'disk.mount'),
                                   (m.g, 'validate', lambda value: value), (m.g, 'binding', lambda *a: None),
                                   (m.g, 'guard', lambda *a, **k: {'enabled': True}),
                                   (m.g, 'private_json', lambda path, *a: json.loads(Path(path).read_bytes())),
                                   (m, 'quiescent', lambda *a: None), (m, 'empty_mountpoint', lambda path: str(path) == row['logical']), (m, 'clear_maintenance', lambda *a: None),
                                   (m, 'mounted', lambda path: str(path) in mounted_paths), (m, 'run', run)]:
                stack.enter_context(patch.object(obj, name, val))
            yield calls, mounted_paths

    def test_complete_cutover_retains_original_and_idempotent_reentry(self):
        with tempfile.TemporaryDirectory() as directory:
            root, row, state, value = self.fixture(directory)
            with self.harness(root, row) as (calls, mounted_paths):
                result = m.switch(value, state)
                self.assertEqual(result['phase'], 'complete')
                self.assertEqual(Path(row['oldRoot']).joinpath('synthetic').read_bytes(), b'preserved')
                self.assertEqual(Path(row['target']).joinpath('synthetic').read_bytes(), b'preserved')
                before = len(calls)
                self.assertTrue(m.switch(value, state)['alreadyComplete'])
                self.assertEqual(len(calls), before)
                self.assertIn(row['logical'], mounted_paths)

    def test_failed_mount_keeps_receipt_and_explicit_rollback_restores_same_source_inode(self):
        with tempfile.TemporaryDirectory() as directory:
            root, row, state, value = self.fixture(directory)
            with self.harness(root, row, [True]) as (calls, mounted_paths):
                with self.assertRaisesRegex(ValueError, 'synthetic_start_failure'):
                    m.switch(value, state)
                self.assertEqual(row['phase'], 'renamed')
                result = m.rollback(value, state)
                self.assertEqual(result['phase'], 'rolled-back')
                self.assertEqual(m.g.identity(row['logical'])[:2], row['sourceId'])
                self.assertEqual(Path(row['logical']).joinpath('synthetic').read_bytes(), b'preserved')
                self.assertTrue(Path(row['target']).joinpath('synthetic').exists())
                self.assertFalse(mounted_paths)

    def test_failed_mount_can_resume_with_same_unit_and_copy_proof(self):
        with tempfile.TemporaryDirectory() as directory:
            root, row, state, value = self.fixture(directory)
            with self.harness(root, row, [True]):
                with self.assertRaises(ValueError):
                    m.switch(value, state)
                unit = m.UNIT_ROOT / row['unit']
                before = unit.stat().st_ino
                proof = copy.deepcopy(row['proof'])
                self.assertEqual(m.switch(value, state)['phase'], 'complete')
                self.assertEqual(unit.stat().st_ino, before)
                self.assertEqual(row['proof'], proof)



class RecoveryRegressionTests(unittest.TestCase):
    @contextlib.contextmanager
    def prepare_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, physical = root / 'source', root / 'disk'
            source.mkdir(mode=0o700)
            physical.mkdir(mode=0o700)
            (source / 'nested').mkdir(mode=0o700)
            (source / 'nested/data').write_bytes(b'synthetic durable content')
            (source / 'nested/data').chmod(0o600)
            approval_path = root / 'approval.json'
            approval_path.write_text('{}')
            approval_path.chmod(0o600)
            row = {'purpose': 'application-data', 'logical': str(source), 'target': str(physical / 'data'), 'uid': os.getuid(), 'sourceId': m.g.identity(source)[:2], 'temporary': False}
            value = {'nonce': 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'contract': 'harbor-material-move-v1', 'releaseRoots': {}}
            def mkdir(path, uid):
                Path(path).mkdir(mode=0o700)
            with patch.object(m.g, 'ROOT', physical), patch.object(m, 'STATE', physical / 'state.json'), patch.object(m, 'APPROVAL', approval_path), patch.object(m.g, 'CONFIG', root / 'absent.json'), patch.object(m.g, 'directory'), patch.object(m.g, 'private_json', side_effect=lambda path, *args: json.loads(Path(path).read_text())), patch.object(m.g, 'disk_mount'), patch.object(m, 'quiescent'), patch.object(m, 'target_directory', side_effect=mkdir):
                yield root, source, row, value

    def test_approval_checks_real_schema_without_fabricating_target_inodes(self):
        import hashlib
        root = m.g.STACK / 'local-assurance/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
        value = {'contract': 'harbor-material-move-v1', 'approved': True, 'nonce': 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'diskUuid': '12345678-abcd', 'materialsDirectory': str(root), 'sourceRoots': {}, 'legacyMaterials': [], 'legacyTemporary': [{'logical': '/tmp/harbor-release-123-owned', 'sourceId': [9, 99], 'uid': 1002}], 'containers': {name: 'a' * 64 for name in m.APPS}, 'tools': {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest() for path in [PATH, Path(m.g.__file__)]}, 'releaseRoots': {'release-materials': [9, 31], 'release-tmp': [9, 32]}, 'guards': {name: hashlib.sha256(b'synthetic-code').hexdigest() for name in m.GUARD_FILES}, 'runtimeRebuildableCaches': ['codex-launcher-aliases']}
        for index, purpose in enumerate(m.g.TARGETS):
            logical = m.g.FIXED_LOGICAL.get(purpose, str(root / ('pg-data' if purpose == 'application-data' else 'runtime')))
            value['sourceRoots'][purpose] = {'logical': logical, 'sourceId': [9, index + 1]}
        with patch.object(m.g, 'private_json', return_value=value), patch.object(m, 'guard_code', return_value=b'synthetic-code'):
            approved, rows = m.approval()
            self.assertEqual(rows[-1]['uid'], 1002)
            self.assertEqual(next(row for row in rows if row['purpose'] == 'application-runtime')['runtimeRebuildableCaches'], ['codex-launcher-aliases'])
            bad = copy.deepcopy(value)
            bad['sourceRoots']['restore']['logical'] = '/foreign/restore'
            with patch.object(m.g, 'private_json', return_value=bad), self.assertRaisesRegex(ValueError, 'logical_scope_invalid'):
                m.approval()

    def test_recursive_durability_precedes_copied_journal_and_failure_does_not_publish(self):
        with self.prepare_fixture() as (root, source, row, value):
            events = []
            fsync, save = m.os.fsync, m.save
            def record_sync(fd):
                events.append(('fsync', os.readlink('/proc/self/fd/' + str(fd))))
                return fsync(fd)
            def record_save(path, state):
                events.append(('save', state['phase'], [entry['phase'] for entry in state['rows']]))
                return save(path, state)
            with patch.object(m.os, 'fsync', side_effect=record_sync), patch.object(m, 'save', side_effect=record_save):
                m.prepare(value, [row])
            first_copied = next(index for index, event in enumerate(events) if event[0] == 'save' and 'copied' in event[2])
            synced = {event[1] for event in events[:first_copied] if event[0] == 'fsync'}
            for suffix in ['nested/data', 'nested', '.']:
                self.assertIn(str(Path(row['target']) / suffix), synced)
        with self.prepare_fixture() as (root, source, row, value):
            with patch.object(m, 'durable_tree', side_effect=OSError('synthetic fsync failure')), self.assertRaisesRegex(OSError, 'fsync failure'):
                m.prepare(value, [row])
            state = json.loads(m.STATE.read_text())
            self.assertEqual(state['phase'], 'copying')
            self.assertEqual(state['rows'][0]['phase'], 'copying')
            self.assertNotIn('proof', state['rows'][0])

    def test_real_killed_rsync_retries_only_owned_scratch_and_preserves_source(self):
        import signal
        import subprocess
        import time
        with self.prepare_fixture() as (root, source, row, value):
            payload = source / 'nested/large'
            payload.write_bytes(bytes(range(256)) * 65536)
            payload.chmod(0o600)
            before = m.scan(source, os.getuid(), digest=True)
            def interrupted(args, **kwargs):
                self.assertEqual(args[0], '/usr/bin/rsync')
                scratch = Path(next(arg.split('=', 1)[1] for arg in args if arg.startswith('--temp-dir=')))
                process = subprocess.Popen([*args[:1], '--bwlimit=128', *args[1:]], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
                try:
                    deadline = time.monotonic() + 5
                    while time.monotonic() < deadline and not any(path.stat().st_size > 0 for path in scratch.iterdir()):
                        time.sleep(0.01)
                    self.assertTrue(any(scratch.iterdir()), 'rsync must actually create its transfer file')
                finally:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
                raise ValueError('synthetic killed rsync')
            with patch.object(m, 'run', side_effect=interrupted), self.assertRaisesRegex(ValueError, 'killed rsync'):
                m.prepare(value, [row])
            state = json.loads(m.STATE.read_text())
            scratch = Path(state['rows'][0]['scratch']['path'])
            self.assertTrue(any(scratch.iterdir()))
            self.assertEqual(m.prepare(value, [row])['phase'], 'copied')
            self.assertFalse(any(scratch.iterdir()))
            self.assertEqual(m.scan(source, os.getuid(), digest=True), before)
            self.assertEqual((Path(row['target']) / 'nested/large').read_bytes(), payload.read_bytes())

    def test_unknown_target_file_and_foreign_scratch_link_are_not_deleted(self):
        with tempfile.TemporaryDirectory() as directory:
            source, target, scratch = (Path(directory) / name for name in ['source', 'target', 'scratch'])
            for path in [source, target, scratch]:
                path.mkdir(mode=0o700)
            (source / 'data').write_bytes(b'synthetic')
            (source / 'data').chmod(0o600)
            foreign = target / 'foreign'
            foreign.write_bytes(b'preserve unknown')
            foreign.chmod(0o600)
            entry = {'path': str(scratch), 'id': m.g.identity(scratch)[:2]}
            with self.assertRaisesRegex(ValueError, 'cold_copy_mismatch'):
                m.copy_cold(source, target, os.getuid(), lambda: None, scratch=entry)
            self.assertEqual(foreign.read_bytes(), b'preserve unknown')
            (scratch / 'link').symlink_to(foreign)
            with self.assertRaisesRegex(ValueError, 'copy_scratch_untrusted'):
                m.clean_scratch(entry, os.getuid())
            self.assertEqual(foreign.read_bytes(), b'preserve unknown')

    def test_partial_registered_release_child_can_rollback_but_foreign_sibling_cannot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, target = root / 'source', root / 'target'
            source.mkdir(mode=0o700)
            target.mkdir(mode=0o700)
            proof = m.copy_cold(source, target, os.getuid(), lambda: None)
            child = target / 'release-owned'
            child.mkdir(mode=0o700)
            (child / 'partial').write_bytes(b'partial preserved')
            parent = {'purpose': 'release-tmp', 'target': str(target), 'logical': str(source), 'uid': os.getuid(), 'phase': 'copied', 'proof': proof}
            entry = {'purpose': 'legacyTemporary-release-owned', 'target': str(child), 'uid': os.getuid(), 'targetId': m.g.identity(child)[:2], 'phase': 'copying'}
            state = {'phase': 'copying', 'rows': [parent, entry]}
            with patch.object(m.g, 'disk_mount'), patch.object(m, 'quiescent'), patch.object(m.g, 'CONFIG', root / 'absent'), patch.object(m, 'clear_maintenance'), patch.object(m, 'save'):
                self.assertEqual(m.rollback({}, state)['phase'], 'rolled-back')
                self.assertEqual((child / 'partial').read_bytes(), b'partial preserved')
                (target / 'foreign').write_bytes(b'foreign')
                with self.assertRaisesRegex(ValueError, 'copied_target_changed'):
                    m.target_unchanged(parent, state, allow_incomplete=True)

    def test_exact_opt_in_launcher_alias_exclusion_never_follows_external_link(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, target = root / 'source', root / 'target'
            source.mkdir(mode=0o700)
            target.mkdir(mode=0o700)
            account = source / 'accounts/synthetic'
            aliases = account / 'tmp/arg0'
            aliases.mkdir(mode=0o700, parents=True)
            (aliases / 'codex').symlink_to('/outside/synthetic-tool')
            auth = account / 'auth.json'
            auth.write_text('synthetic-auth-must-preserve')
            auth.chmod(0o600)
            with self.assertRaisesRegex(ValueError, 'tree_untrusted'):
                m.scan(source, os.getuid())
            proof = m.copy_cold(source, target, os.getuid(), lambda: None, rebuildable_caches=['codex-launcher-aliases'])
            self.assertEqual((target / 'accounts/synthetic/auth.json').read_text(), auth.read_text())
            self.assertFalse((target / 'accounts/synthetic/tmp/arg0').exists())
            self.assertTrue((aliases / 'codex').is_symlink())
            (account / 'unknown').symlink_to('/outside/unknown')
            with self.assertRaisesRegex(ValueError, 'tree_untrusted'):
                m.scan(source, os.getuid(), rebuildable_caches=['codex-launcher-aliases'])


    def test_cache_selection_preserves_same_named_file_and_rejects_alias_root_link(self):
        for case in ['file', 'link']:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                source, target = Path(directory) / 'source', Path(directory) / 'target'
                source.mkdir(mode=0o700)
                target.mkdir(mode=0o700)
                parent = source / 'accounts/synthetic/tmp'
                parent.mkdir(mode=0o700, parents=True)
                arg0 = parent / 'arg0'
                if case == 'link':
                    arg0.symlink_to('/outside/not-allowed')
                    with self.assertRaisesRegex(ValueError, 'tree_untrusted'):
                        m.scan(source, os.getuid(), rebuildable_caches=['codex-launcher-aliases'])
                else:
                    arg0.write_bytes(b'not-a-cache-directory')
                    arg0.chmod(0o600)
                    m.copy_cold(source, target, os.getuid(), lambda: None, rebuildable_caches=['codex-launcher-aliases'])
                    self.assertEqual((target / 'accounts/synthetic/tmp/arg0').read_bytes(), arg0.read_bytes())


    def test_scratch_creation_interruptions_preserve_unproved_directory_and_resume_fresh(self):
        for fault in ['after-mkdir', 'parent-fsync', 'before-identity-journal']:
            with self.subTest(fault=fault), self.prepare_fixture() as (root, source, row, value):
                mkdir, sync, save = Path.mkdir, m.sync, m.save
                failed = False
                def interrupted_mkdir(path, *args, **kwargs):
                    nonlocal failed
                    mkdir(path, *args, **kwargs)
                    if fault == 'after-mkdir' and path.name.startswith('.material-copy-') and not failed:
                        failed = True
                        raise OSError('synthetic after scratch mkdir')
                def interrupted_sync(path):
                    nonlocal failed
                    state = json.loads(m.STATE.read_text()) if m.STATE.exists() else {}
                    entry = state.get('rows', [{}])[-1] if state.get('rows') else {}
                    candidate = entry.get('scratchIntent', {}).get('path')
                    if fault == 'parent-fsync' and candidate and Path(candidate).exists() and not failed:
                        failed = True
                        raise OSError('synthetic scratch fsync failure')
                    return sync(path)
                def interrupted_save(path, state):
                    nonlocal failed
                    if fault == 'before-identity-journal' and state['rows'] and 'scratch' in state['rows'][-1] and not failed:
                        failed = True
                        raise OSError('synthetic scratch journal failure')
                    return save(path, state)
                with patch.object(Path, 'mkdir', new=interrupted_mkdir), patch.object(m, 'sync', side_effect=interrupted_sync), patch.object(m, 'save', side_effect=interrupted_save), self.assertRaisesRegex(OSError, 'synthetic'):
                    m.prepare(value, [row])
                persisted = json.loads(m.STATE.read_text())
                old = Path(persisted['rows'][0]['scratchIntent']['path'])
                self.assertTrue(old.is_dir())
                self.assertNotIn('scratch', persisted['rows'][0])
                marker = old / 'unproved-must-preserve'
                marker.write_bytes(b'unknown directory contents')
                marker.chmod(0o600)
                old_identity = m.g.identity(old)[:2]
                self.assertEqual(m.prepare(value, [row])['phase'], 'copied')
                resumed = json.loads(m.STATE.read_text())['rows'][0]
                self.assertNotEqual(resumed['scratch']['path'], str(old))
                self.assertIn(str(old), resumed['unprovedScratchPaths'])
                self.assertEqual(m.g.identity(old)[:2], old_identity)
                self.assertEqual(marker.read_bytes(), b'unknown directory contents')
                self.assertEqual((Path(row['target']) / 'nested/data').read_bytes(), (source / 'nested/data').read_bytes())


if __name__ == '__main__':
    unittest.main()
