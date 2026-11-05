"""本机真实文件机制；root/Docker 信任边界使用替身，不声称宿主安装验收。"""
import concurrent.futures
import errno
import importlib.util
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'ci/storage-recovery/prepare-recording-key.py'
spec = importlib.util.spec_from_file_location('recording_key_prepare', SCRIPT)
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)


class RecordingKeyTests(unittest.TestCase):
    def setUp(self):
        # Docker 的 /tmp 可能位于不支持 O_TMPFILE 的 overlay；Linux tmpfs
        # 用于执行同一匿名 inode/no-replace 机制，不给产品增加具名临时文件降级。
        temporary_root = '/dev/shm' if Path('/dev/shm').is_dir() else None
        self.temp = tempfile.TemporaryDirectory(prefix='harbor-recording-material-', dir=temporary_root)
        self.path = Path(self.temp.name)
        self.fd = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY)
        self.uid = os.getuid()
        self.forbidden = [b'm' * 32, b'r' * 32]
        b.CANCELLED = False

    def tearDown(self):
        b.CANCELLED = False
        os.close(self.fd)
        self.temp.cleanup()

    def write(self, name, body=b'x' * 32, mode=0o600):
        (self.path / name).write_bytes(body)
        os.chmod(self.path / name, mode)

    def prepare(self, validate=lambda: None):
        return b.prepare(self.fd, self.forbidden, validate, self.uid)

    def test_real_anonymous_atomic_publish_and_existing_bytes_preserved(self):
        self.assertEqual(self.prepare(), 'created')
        before = (self.path / b.KEY).read_bytes()
        info = (self.path / b.KEY).stat()
        self.assertEqual(len(before), 32)
        self.assertNotIn(before, self.forbidden)
        self.assertEqual(info.st_uid, self.uid)
        self.assertEqual(stat.S_IMODE(info.st_mode), 0o600)
        self.assertEqual(info.st_nlink, 1)
        self.assertEqual(self.prepare(), 'existing_preserved')
        self.assertEqual(before, (self.path / b.KEY).read_bytes())
        self.assertEqual(info.st_ino, (self.path / b.KEY).stat().st_ino)
        self.assertEqual(os.listdir(self.path), [b.KEY])

    def test_real_parallel_publish_never_overwrites(self):
        def perform(_):
            return self.prepare()
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(perform, range(30)))
        self.assertEqual(results.count('created'), 1)
        self.assertTrue(set(results) <= {'created', 'existing_preserved', 'concurrent_existing_preserved'})
        self.assertEqual(len((self.path / b.KEY).read_bytes()), 32)
        self.assertEqual(os.listdir(self.path), [b.KEY])

    def test_missing_key_with_any_spool_entry_is_refused(self):
        for kind in ['dir', 'file', 'symlink']:
            with self.subTest(kind=kind):
                p = self.path / 'recording-spool'
                if kind == 'dir':
                    p.mkdir()
                elif kind == 'file':
                    p.touch()
                else:
                    p.symlink_to('/nonexistent')
                with self.assertRaisesRegex(b.Refused, 'missing_key_with_spool'):
                    self.prepare()
                self.assertFalse((self.path / b.KEY).exists())
                p.rmdir() if kind == 'dir' else p.unlink()

    def test_existing_key_with_spool_is_preserved(self):
        self.write(b.KEY)
        (self.path / 'recording-spool').mkdir()
        self.assertEqual(self.prepare(), 'existing_preserved')

    def test_reused_master_or_report_key_is_refused(self):
        for value in self.forbidden:
            self.write(b.KEY, value)
            with self.assertRaisesRegex(b.Refused, 'material_reused'):
                self.prepare()
            self.assertEqual((self.path / b.KEY).read_bytes(), value)

    def test_generated_collision_is_refused_without_file(self):
        with patch.object(b.os, 'getrandom', return_value=self.forbidden[0]):
            with self.assertRaisesRegex(b.Refused, 'material_reused'):
                self.prepare()
        self.assertEqual(os.listdir(self.path), [])

    def test_wrong_length_mode_and_hardlink_are_not_repaired(self):
        for body, mode in [(b'bad', 0o600), (b'x'*33, 0o600), (b'x'*32, 0o644), (b'x'*32, 0o400)]:
            with self.subTest(size=len(body), mode=mode):
                self.write(b.KEY, body, mode)
                with self.assertRaisesRegex(b.Refused, 'material_invalid'):
                    self.prepare()
                self.assertEqual((self.path / b.KEY).read_bytes(), body)
        os.chmod(self.path / b.KEY, 0o600)
        os.link(self.path / b.KEY, self.path / 'alias')
        with self.assertRaisesRegex(b.Refused, 'material_invalid'):
            self.prepare()

    def test_symlink_and_special_file_are_refused_without_following(self):
        self.write('other')
        (self.path / b.KEY).symlink_to('other')
        with self.assertRaises(OSError):
            self.prepare()
        (self.path / b.KEY).unlink()
        os.mkfifo(self.path / b.KEY, 0o600)
        with self.assertRaisesRegex(b.Refused, 'material_invalid'):
            self.prepare()
        self.assertEqual((self.path / 'other').read_bytes(), b'x'*32)

    def test_wrong_owner_and_replaced_existing_entry_refused(self):
        self.write(b.KEY)
        with self.assertRaisesRegex(b.Refused, 'material_invalid'):
            b.material(self.fd, b.KEY, self.uid + 1)
        def replace():
            (self.path / b.KEY).unlink()
            self.write(b.KEY, b'changed-'*4)
        with self.assertRaisesRegex(b.Refused, 'material_changed'):
            self.prepare(replace)

    def test_concurrent_symlink_winner_is_not_followed_or_overwritten(self):
        self.write('other')
        def replace():
            (self.path / b.KEY).symlink_to('other')
        with self.assertRaises(OSError):
            self.prepare(replace)
        self.assertTrue((self.path / b.KEY).is_symlink())
        self.assertEqual((self.path / 'other').read_bytes(), b'x'*32)

    def test_concurrent_winner_replaced_during_source_revalidation_refused(self):
        validations = 0
        def validate():
            nonlocal validations
            validations += 1
            if validations == 1:
                self.write(b.KEY, b'w'*32)
            else:
                # winner 已读取后源 Docker 取证期间被替换。
                (self.path / b.KEY).unlink()
                self.write(b.KEY, self.forbidden[0])
        with self.assertRaisesRegex(b.Refused, 'material_changed'):
            self.prepare(validate)
        self.assertEqual((self.path / b.KEY).read_bytes(), self.forbidden[0])

    def test_material_changed_during_read_is_refused(self):
        self.write(b.KEY)
        read = os.read
        def changed(fd, count):
            value = read(fd, count)
            (self.path / b.KEY).write_bytes(b'changed-'*4)
            return value
        with patch.object(b.os, 'read', side_effect=changed):
            with self.assertRaisesRegex(b.Refused, 'material_changed'):
                b.material(self.fd, b.KEY, self.uid)

    def test_cancel_before_publish_leaves_no_material(self):
        b.CANCELLED = True
        with self.assertRaisesRegex(b.Refused, 'cancelled'):
            self.prepare()
        self.assertEqual(os.listdir(self.path), [])

    def test_revalidation_failure_before_publish_leaves_no_temp(self):
        def fail():
            raise b.Refused('source_changed')
        with self.assertRaisesRegex(b.Refused, 'source_changed'):
            self.prepare(fail)
        self.assertEqual(os.listdir(self.path), [])

    def test_source_changed_after_publish_preserves_key_for_recheck(self):
        calls = 0
        def validate():
            nonlocal calls
            calls += 1
            if calls == 2:
                raise b.Refused('source_changed')
        with self.assertRaisesRegex(b.Refused, 'published_recheck_required'):
            self.prepare(validate)
        value = (self.path / b.KEY).read_bytes()
        self.assertEqual(self.prepare(), 'existing_preserved')
        self.assertEqual((self.path / b.KEY).read_bytes(), value)

    def test_fsync_failure_after_publish_keeps_exact_material(self):
        fsync = os.fsync
        def fail(fd):
            if fd == self.fd:
                raise OSError('private-path-must-not-leak')
            fsync(fd)
        with patch.object(b.os, 'fsync', side_effect=fail):
            with self.assertRaisesRegex(b.Refused, 'published_recheck_required'):
                self.prepare()
        self.assertEqual(os.listdir(self.path), [b.KEY])
        self.assertEqual(len((self.path / b.KEY).read_bytes()), 32)

    def test_cancel_after_link_keeps_material_and_reports_uncertain(self):
        link = os.link
        def cancel(*args, **kwargs):
            link(*args, **kwargs)
            b.CANCELLED = True
        with patch.object(b.os, 'link', side_effect=cancel):
            with self.assertRaisesRegex(b.Refused, 'published_recheck_required'):
                self.prepare()
        self.assertEqual(len((self.path / b.KEY).read_bytes()), 32)

    def test_spool_appearing_at_final_validation_refuses_publish(self):
        def validate():
            (self.path / 'recording-spool').mkdir()
        with self.assertRaisesRegex(b.Refused, 'missing_key_with_spool'):
            self.prepare(validate)
        self.assertFalse((self.path / b.KEY).exists())

    def test_no_unsafe_fallback_if_anonymous_files_unsupported(self):
        original = os.open
        for code in (errno.EOPNOTSUPP, errno.EINVAL):
            with self.subTest(errno=code):
                def fail(path, flags, *args, **kwargs):
                    if flags & os.O_TMPFILE == os.O_TMPFILE:
                        raise OSError(code, 'anonymous inode unsupported')
                    return original(path, flags, *args, **kwargs)
                with patch.object(b.os, 'open', side_effect=fail):
                    with self.assertRaises(OSError) as raised:
                        self.prepare()
                self.assertEqual(raised.exception.errno, code)
                self.assertFalse(os.path.lexists(self.path / b.KEY))
                self.assertEqual(os.listdir(self.path), [])

    def test_directory_and_source_identity_boundaries_with_substitutes(self):
        # 临时目录祖先可被其他用户写，真实生产路径拒绝此入口。
        with self.assertRaisesRegex(b.Refused, 'directory_untrusted'):
            b.directory(str(self.path), self.uid)
        for path in ['relative', '/', '/etc/../tmp', '/etc//tmp']:
            with self.assertRaisesRegex(b.Refused, 'directory_invalid'):
                b.directory(path, self.uid)
        source = {'container': 'harbor', 'imageId': 'sha256:'+'a'*64, 'user': 'node', 'project': 'harbor', 'dataDirectory': '/approved/data'}
        row = {'Image': source['imageId'], 'Config': {'User': 'node', 'Labels': {'com.aistrong.project': 'harbor'}}, 'Mounts': [{'Type': 'bind', 'Destination': '/app/data', 'Source': '/approved/data'}]}
        def invoke(value):
            result = subprocess.CompletedProcess([], 0, json.dumps([value]).encode(), b'')
            with patch.object(b.subprocess, 'run', return_value=result), patch.object(b, 'directory', side_effect=lambda *_: os.dup(self.fd)):
                b.inspect_source(source, self.fd)
        invoke(row)
        for field, value in [('Image', 'other'), ('Mounts', []), ('Config', {'User': 'root'})]:
            with self.subTest(field=field), self.assertRaisesRegex(b.Refused, 'source_mismatch'):
                invoke({**row, field: value})

    @unittest.skipUnless(os.getuid() in (0, 1000), 'source UID1000 fixture requires UID1000 or isolated root')
    def test_fixed_run_with_real_materials_and_substituted_root_trust_boundary(self):
        # 不安装 /etc 配置、不连接 Docker；只替代 root 信任取证，实际材料读写不替代。
        self.write('key-encryption.key', b'm'*32)
        if os.getuid() == 0:
            os.chown(self.path / 'key-encryption.key', 1000, 1000)
        secrets_path = self.path / 'approved-secrets'
        secrets_path.mkdir()
        report = secrets_path / 'report-app.key'
        report.write_bytes(b'r'*32)
        report.chmod(0o600)
        if os.getuid() == 0:
            os.chown(report, 1000, 1000)
        secrets_fd = os.open(secrets_path, os.O_RDONLY | os.O_DIRECTORY)
        config = {'version': 1, 'source': {'uid': 1000, 'container': 'harbor', 'imageId': 'sha256:'+'a'*64, 'project': 'harbor', 'user': 'node', 'dataDirectory': '/approved/data'}, 'candidate': {'secretsDirectory': '/approved/secrets'}, 'fileRoots': [{'id': 'secrets', 'source': '/approved/secrets'}]}
        def directory(path, uid):
            if (path, uid) == ('/approved/data', 1000):
                return os.dup(self.fd)
            if (path, uid) == ('/approved/secrets', 0):
                return os.dup(secrets_fd)
            raise AssertionError('unapproved path')
        try:
            with patch.object(b.os, 'getuid', return_value=0), patch.object(b.os, 'geteuid', return_value=0), patch.object(b, 'profile', return_value=(b'fixed-profile', config)), patch.object(b, 'directory', side_effect=directory), patch.object(b, 'inspect_source'):
                self.assertEqual(b.run(), 'created')
                self.assertEqual(b.run(), 'existing_preserved')
                existing = (self.path / b.KEY).read_bytes()
                self.assertNotIn(existing, [b'm'*32, b'r'*32])
                (self.path / b.KEY).write_bytes(b'r'*32)
                with self.assertRaisesRegex(b.Refused, 'material_reused'):
                    b.run()
                self.assertEqual((self.path / b.KEY).read_bytes(), b'r'*32)
        finally:
            os.close(secrets_fd)

    def test_fixed_root_entry_rejects_nonroot_and_arguments(self):
        with patch.object(b.os, 'geteuid', return_value=1000):
            with self.assertRaisesRegex(b.Refused, 'root_required'):
                b.run()
        result = subprocess.run([sys.executable, str(SCRIPT), '--arbitrary-config'], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout), {'ok': False, 'status': 'arguments_forbidden'})
        self.assertEqual(result.stderr, '')
        if os.getuid() != 0:
            result = subprocess.run([sys.executable, str(SCRIPT)], capture_output=True, text=True, timeout=10)
            self.assertEqual(json.loads(result.stdout), {'ok': False, 'status': 'root_required'})


if __name__ == '__main__':
    unittest.main()
