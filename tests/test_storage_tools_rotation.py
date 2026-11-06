"""Minimal host tools rotation tests; no daemon, external host or real install."""
import importlib.util
import json
import os
import socket
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('rotate_tools', ROOT / 'ci/storage-recovery/rotate-tools.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
b = r.b


class RotationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='harbor-tools-rotation-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.patches = []
        def patched(obj, name, value):
            p = patch.object(obj, name, value); p.start(); self.patches.append(p)
        self.addCleanup(lambda: [p.stop() for p in reversed(self.patches)])
        patched(r, 'UID', os.getuid())
        patched(r, 'APPROVAL', self.base / 'approval.json')
        patched(r, 'HISTORY', self.base / 'history')
        patched(r, 'PROC', self.base / 'proc')
        patched(r, 'DOCKER_SOCKET', self.base / 'docker.sock')
        self.docker_socket = socket.socket(socket.AF_UNIX)
        self.docker_socket.bind(str(r.DOCKER_SOCKET)); r.DOCKER_SOCKET.chmod(0o660)
        self.addCleanup(self.docker_socket.close)
        self.docker_root = self.base / 'docker-data'; self.docker_root.mkdir()
        self.container_directory = self.docker_root / 'containers'; self.container_directory.mkdir()
        self.containers = {}
        self.docker_calls = []
        r.PROC.mkdir()
        patched(b, 'DESTINATION', self.base / 'tools')
        patched(b, 'PROFILE', self.base / 'tools.json')
        patched(r.os, 'getuid', lambda: 0)
        patched(r.os, 'geteuid', lambda: 0)
        def trusted_directory(path):
            info = path.lstat()
            self.assertTrue(path.is_dir()); self.assertFalse(path.is_symlink()); self.assertFalse(info.st_mode & 0o022)
            return os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        patched(b, 'trusted_directory', trusted_directory)
        def verify_tool_tree(path):
            for item in [path, *path.rglob('*')]:
                self.assertFalse(item.is_symlink()); self.assertFalse(item.stat().st_mode & 0o022)
        patched(b, 'verify_tool_tree', verify_tool_tree)
        self.old = {'version': 1, 'businessImage': 'registry.example/b@sha256:' + '1' * 64, 'infrastructureImage': 'registry.example/i@sha256:' + '2' * 64, 'revision': '3' * 40, 'nodeVersion': 'v24.21.0'}
        self.new = {**self.old, 'businessImage': 'registry.example/b@sha256:' + '4' * 64, 'infrastructureImage': 'registry.example/i@sha256:' + '5' * 64, 'revision': '6' * 40}
        self.make_tree(self.old)
        b.PROFILE.write_bytes(r.body(self.old)); b.PROFILE.chmod(0o600)
        self.raw_profile = b.PROFILE.read_bytes()
        self.old_inode = r.identity(b.DESTINATION)
        self.profile_inode = r.identity(b.PROFILE)
        self.approved = {'version': 1, 'id': str(uuid.uuid4()), 'expected': {'profileSha256': r.sha(self.raw_profile), 'receiptSha256': r.sha((b.DESTINATION / 'installation.json').read_bytes()), 'treeIdentity': self.old_inode}, 'nextTools': self.new}
        self.save_approval()
        patched(b, 'configuration', lambda: (r.read_control(b.PROFILE), r.next_config(json.loads(r.read_control(b.PROFILE)))))
        self.image_checks = []
        patched(b, 'image_identity', lambda config: self.image_checks.append(config))
        self.runtime_checks = []
        def verify_copy(path, config):
            self.runtime_checks.append((path, config))
            self.assertEqual((path / 'code.mjs').read_text(), config['revision'])
            return {'revision': config['revision'], 'sourceDigest': '7' * 64, 'nodeVersion': config['nodeVersion'], 'nodeDigest': '8' * 64}
        patched(b, 'verify_copy', verify_copy)
        patched(b, 'command', self.docker_command)

    def docker_command(self, args, **kwargs):
        self.docker_calls.append(args)
        self.assertEqual(args[:3], ['/usr/bin/docker', '--host', 'unix://' + str(r.DOCKER_SOCKET)])
        if args[3:] == ['info', '--format', '{{json .DockerRootDir}}']:
            return json.dumps(str(self.docker_root)).encode()
        if args[3:5] == ['container', 'inspect'] and len(args) == 6:
            return json.dumps([self.containers[args[5]]]).encode()
        self.fail('unexpected command: ' + repr(args))

    def add_container(self, state, mounts, cid='9' * 64):
        if cid not in self.containers:
            (self.container_directory / cid).mkdir(mode=0o700)
        self.containers[cid] = {'Id': cid, 'State': state, 'Mounts': mounts}
        return cid

    def make_tree(self, config):
        b.DESTINATION.mkdir(mode=0o755)
        (b.DESTINATION / 'code.mjs').write_text(config['revision'])
        receipt = {**config, 'sourceDigest': '7' * 64, 'nodeDigest': '8' * 64, 'postgresHostToolsInstalled': False}
        (b.DESTINATION / 'installation.json').write_bytes(r.body(receipt)); (b.DESTINATION / 'installation.json').chmod(0o600)

    def save_approval(self):
        r.APPROVAL.write_bytes(r.body(self.approved)); r.APPROVAL.chmod(0o600)

    @property
    def directory(self):
        return r.HISTORY / self.approved['id']

    def assert_old(self):
        self.assertEqual(r.identity(b.DESTINATION), self.old_inode)
        self.assertEqual(r.identity(b.PROFILE), self.profile_inode)
        self.assertEqual(b.PROFILE.read_bytes(), self.raw_profile)
        self.assertEqual((b.DESTINATION / 'code.mjs').read_text(), self.old['revision'])

    def test_prepare_preserves_old_tree_then_accept_and_rollback_preserves_both(self):
        result = r.run('prepare')
        self.assertEqual(result['status'], 'prepared')
        self.assertFalse(b.DESTINATION.exists())
        self.assertEqual(r.identity(self.directory / 'previous'), self.old_inode)
        self.assertEqual(json.loads(b.PROFILE.read_text()), self.new)
        self.make_tree(self.new)
        new_inode = r.identity(b.DESTINATION)
        self.assertEqual(r.run('accept')['status'], 'accepted')
        self.assertTrue(self.runtime_checks)
        self.assertEqual(r.run('rollback')['status'], 'rolled-back')
        self.assert_old()
        self.assertEqual(r.identity(self.directory / 'replaced'), new_inode)
        journal = json.loads((self.directory / 'journal.json').read_text())
        self.assertEqual(r.profile_bytes(journal)['old'], self.raw_profile)
        r.run('rollback'); self.assert_old()
        with self.assertRaisesRegex(b.Refused, 'direction_conflict'):
            r.run('prepare')

    def test_prepare_can_resume_after_tree_rename_and_profile_write(self):
        for point in ['old-tree-moved', 'profile-new']:
            with self.subTest(point=point):
                if not b.DESTINATION.exists():
                    r.run('rollback')
                self.approved['id'] = str(uuid.uuid4()); self.save_approval()
                def crash(phase):
                    if phase == point: raise InterruptedError('power loss')
                with self.assertRaises(InterruptedError): r.run('prepare', hook=crash)
                r.run('prepare')
                self.assertFalse(b.DESTINATION.exists())
                r.run('prepare')
                r.run('rollback'); self.assert_old()

    def test_rollback_can_resume_after_each_move_and_profile_write(self):
        for point in ['new-tree-moved', 'profile-old', 'old-tree-restored']:
            with self.subTest(point=point):
                self.approved['id'] = str(uuid.uuid4()); self.save_approval()
                r.run('prepare'); self.make_tree(self.new); r.run('accept')
                def crash(phase):
                    if phase == point: raise InterruptedError('power loss')
                with self.assertRaises(InterruptedError): r.run('rollback', hook=crash)
                r.run('rollback'); self.assert_old()

    def test_partial_profile_write_can_finish_or_rollback(self):
        save = r.save
        fired = [False]
        def tear(directory, journal):
            save(directory, journal)
            if journal['profileWriting'] and not fired[0]:
                fired[0] = True
                with b.PROFILE.open('r+b') as handle: handle.write(b'{part'); handle.truncate(); handle.flush(); os.fsync(handle.fileno())
                raise InterruptedError('power loss while writing profile')
        with patch.object(r, 'save', tear), self.assertRaises(InterruptedError): r.run('prepare')
        r.run('prepare')
        self.assertEqual(json.loads(b.PROFILE.read_text()), self.new)
        r.run('rollback'); self.assert_old()

    def test_original_installer_lock_blocks_rotation(self):
        import fcntl
        lock = self.base / '.harbor-storage-tools.lock'
        fd = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError): r.run('prepare')
        finally: os.close(fd)
        self.assert_old()

    def test_pg_installer_lock_blocks_rotation_without_moving_tree(self):
        import fcntl
        fd = os.open(b.DESTINATION / '.postgres-tools.lock', os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError): r.run('prepare')
        finally: os.close(fd)
        self.assert_old()

    def test_foreign_canonical_tree_or_profile_change_is_never_overwritten(self):
        r.run('prepare'); self.make_tree(self.old)
        with self.assertRaisesRegex(b.Refused, 'receipt_binding'): r.run('rollback')
        self.assertEqual((b.DESTINATION / 'code.mjs').read_text(), self.old['revision'])
        self.assertEqual(r.identity(self.directory / 'previous'), self.old_inode)

    def test_replaced_profile_inode_and_changed_approval_refuse_resume(self):
        r.run('prepare')
        foreign = self.base / 'foreign.json'; foreign.write_bytes(b.PROFILE.read_bytes()); foreign.chmod(0o600)
        os.replace(foreign, b.PROFILE)
        with self.assertRaisesRegex(b.Refused, 'profile_identity'): r.run('rollback')
        self.assertEqual(json.loads(b.PROFILE.read_text()), self.new)

    def test_old_hash_owner_and_wrong_next_profile_refuse_initial_move(self):
        original = self.approved['expected']['receiptSha256']
        self.approved['expected']['receiptSha256'] = 'f' * 64; self.save_approval()
        with self.assertRaisesRegex(b.Refused, 'receipt_changed'): r.run('prepare')
        self.assert_old()
        self.approved['expected']['receiptSha256'] = original
        self.approved['nextTools']['version'] = True; self.save_approval()
        with self.assertRaisesRegex(b.Refused, 'approval'): r.run('prepare')
        self.assert_old()

    def fake_process(self, pid, command, exe='/usr/bin/python3', cwd=None, maps=b'', fds=None):
        path = r.PROC / str(pid); path.mkdir()
        (path / 'cmdline').write_bytes(b'\0'.join(os.fsencode(arg) for arg in command) + b'\0')
        (path / 'exe').symlink_to(exe)
        (path / 'cwd').symlink_to(cwd or self.base)
        (path / 'maps').write_bytes(maps)
        (path / 'fd').mkdir()
        for name, target in (fds or {}).items(): (path / 'fd' / str(name)).symlink_to(target)
        return path

    def test_python_c_source_substring_is_not_a_host_job(self):
        self.fake_process(10, ['/usr/bin/python3', '-c', "from pathlib import Path; root=Path('/opt/harbor-storage-tools'); print('storage-backup.mjs')"])
        self.fake_process(11, ['/usr/bin/rg', 'storage-backup.mjs'], exe='/usr/bin/rg')
        r.run('prepare')
        self.assertFalse(b.DESTINATION.exists())

    def test_actual_node_executable_and_standalone_storage_entry_block(self):
        self.fake_process(10, ['/usr/bin/python3', '/tmp/storage-backup.mjs'])
        with self.assertRaisesRegex(b.Refused, 'host_jobs_active'): r.run('prepare')
        self.assert_old()

    def test_open_tool_inode_and_mapped_tool_paths_block_rotation(self):
        self.fake_process(10, ['/usr/bin/python3', '-c', 'pass'], fds={4: b.DESTINATION / 'code.mjs'})
        with self.assertRaisesRegex(b.Refused, 'host_jobs_active'): r.run('prepare')
        self.assert_old()

    def test_real_executable_cwd_and_maps_are_observed(self):
        code = b.DESTINATION / 'code.mjs'
        process = self.fake_process(10, ['/usr/bin/node', '-e', 'pass'], exe=code)
        with self.assertRaisesRegex(b.Refused, 'host_jobs_active'): r.run('prepare')
        (process / 'exe').unlink(); (process / 'exe').symlink_to('/usr/bin/python3')
        (process / 'cwd').unlink(); (process / 'cwd').symlink_to(b.DESTINATION)
        with self.assertRaisesRegex(b.Refused, 'host_jobs_active'): r.run('prepare')
        (process / 'cwd').unlink(); (process / 'cwd').symlink_to(self.base)
        (process / 'maps').write_text('00400000-00401000 r-xp 00000000 08:01 42 ' + str(code) + '\n')
        with self.assertRaisesRegex(b.Refused, 'host_jobs_active'): r.run('prepare')
        self.assert_old()

    def test_running_container_mount_prevents_rotation(self):
        self.add_container({'Status': 'running', 'Running': True, 'Paused': False, 'Restarting': False, 'Pid': 42}, [{'Source': str(b.DESTINATION)}])
        with self.assertRaisesRegex(b.Refused, 'mounted_tools_active'): r.run('prepare')
        self.assert_old()

    def test_paused_restarting_removing_and_dead_alias_mounts_block_without_writes(self):
        for state in [{'Status': 'paused', 'Running': True, 'Paused': True, 'Restarting': False, 'Pid': 42}, {'Status': 'restarting', 'Running': False, 'Paused': False, 'Restarting': True, 'Pid': 0}, {'Status': 'removing', 'Running': False, 'Paused': False, 'Restarting': False, 'Pid': 0}, {'Status': 'dead', 'Running': False, 'Paused': False, 'Restarting': False, 'Pid': 0}]:
            with self.subTest(state=state):
                self.add_container(state, [{'Type': 'bind', 'Source': str(b.DESTINATION), 'Destination': '/unrelated/container/code', 'RW': False}])
                with self.assertRaisesRegex(b.Refused, 'mounted_tools_active'): r.run('prepare')
                self.assert_old(); self.assertFalse((self.directory / 'previous').exists())

    def test_stopped_container_without_resources_does_not_block_rotation(self):
        self.add_container({'Status': 'exited', 'Running': False, 'Paused': False, 'Restarting': False, 'Pid': 0}, [{'Source': str(b.DESTINATION), 'Destination': '/container/code', 'RW': False}])
        r.run('prepare')
        self.assertFalse(b.DESTINATION.exists()); self.assertEqual(r.identity(self.directory / 'previous'), self.old_inode)

    def test_inventory_uses_local_socket_and_every_directory_id_without_ls(self):
        for cid in ['7' * 64, '8' * 64]:
            self.add_container({'Status': 'exited', 'Running': False, 'Paused': False, 'Restarting': False, 'Pid': 0}, [], cid)
        r.run('prepare')
        inspected = {call[-1] for call in self.docker_calls if call[3:5] == ['container', 'inspect']}
        self.assertEqual(inspected, set(self.containers))
        self.assertFalse(any('ls' in call for call in self.docker_calls))

    def test_missing_or_writable_data_root_and_unknown_entry_are_rejected(self):
        with patch.object(b, 'command', return_value=json.dumps(str(self.base / 'missing')).encode()):
            with self.assertRaises(FileNotFoundError): r.run('prepare')
        self.docker_root.chmod(0o777)
        with self.assertRaises(AssertionError): r.run('prepare')
        self.docker_root.chmod(0o755)
        (self.container_directory / 'unknown').mkdir()
        with self.assertRaisesRegex(b.Refused, 'docker_directory_entry'): r.run('prepare')
        self.assert_old()

    def test_remote_context_is_never_selected_and_untrusted_socket_is_rejected(self):
        with patch.dict(os.environ, {'DOCKER_HOST': 'tcp://untrusted.invalid:2375'}):
            r.run('prepare')
        self.assertTrue(all(call[1:3] == ['--host', 'unix://' + str(r.DOCKER_SOCKET)] for call in self.docker_calls))
        r.run('rollback')
        r.DOCKER_SOCKET.chmod(0o666)
        with self.assertRaisesRegex(b.Refused, 'docker_socket_untrusted'): r.run('prepare')
        self.assert_old()

    def test_concurrent_id_addition_or_same_name_replacement_is_rejected(self):
        cid = self.add_container({'Status': 'exited', 'Running': False, 'Paused': False, 'Restarting': False, 'Pid': 0}, [])
        original = self.docker_command
        def changed(args, **kwargs):
            value = original(args, **kwargs)
            if args[3:5] == ['container', 'inspect']:
                (self.container_directory / ('8' * 64)).mkdir()
            return value
        with patch.object(b, 'command', changed):
            with self.assertRaisesRegex(b.Refused, 'docker_directory_changed'): r.run('prepare')
        (self.container_directory / ('8' * 64)).rmdir()
        def replaced(args, **kwargs):
            value = original(args, **kwargs)
            if args[3:5] == ['container', 'inspect']:
                target = self.container_directory / cid; target.rename(self.container_directory / 'saved-directory'); target.mkdir()
            return value
        with patch.object(b, 'command', replaced):
            with self.assertRaisesRegex(b.Refused, 'docker_directory_entry|docker_directory_changed'): r.run('prepare')
        self.assert_old()

    def test_inspect_missing_identity_timeout_or_unknown_state_is_not_absence(self):
        self.add_container({'Status': 'unknown', 'Running': False, 'Paused': False, 'Restarting': False, 'Pid': 0}, [])
        with self.assertRaisesRegex(b.Refused, 'container_inventory'): r.run('prepare')
        original = self.docker_command
        def missing(args, **kwargs):
            if args[3:5] == ['container', 'inspect']: return b'[]'
            return original(args, **kwargs)
        with patch.object(b, 'command', missing):
            with self.assertRaisesRegex(b.Refused, 'container_inventory'): r.run('prepare')
        def timeout(args, **kwargs):
            if args[3:5] == ['container', 'inspect']: raise subprocess.TimeoutExpired('docker', 15)
            return original(args, **kwargs)
        with patch.object(b, 'command', timeout):
            with self.assertRaises(subprocess.TimeoutExpired): r.run('prepare')
        self.assert_old()

    def test_rollback_does_not_require_deleted_new_images(self):
        r.run('prepare'); self.make_tree(self.new); r.run('accept')
        with patch.object(b, 'image_identity', side_effect=b.Refused('image_missing')):
            r.run('rollback')
        self.assert_old()


if __name__ == '__main__': unittest.main()
