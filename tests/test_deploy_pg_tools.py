"""PG 工具提取边界；显式 root 标记时实测降 UID65534 的私有 PG18 集群。"""
import importlib.util
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import signal
import shlex
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'ci/storage-recovery/install-pg-tools.py'
spec = importlib.util.spec_from_file_location('pg_tools_install', SCRIPT)
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)
LOCAL = Path('/tmp/harbor-scale-pg')


class PgToolsTests(unittest.TestCase):
    def setUp(self):
        self.path = Path(tempfile.mkdtemp(prefix='harbor-pg-tools-test-'))
        self.retain_for_process_check = False
        self.original_lstat = Path.lstat
        p.b.CANCELLED = False

    def tearDown(self):
        p.b.CANCELLED = False
        if not self.retain_for_process_check:
            shutil.rmtree(self.path)

    def root_metadata(self):
        original = self.original_lstat
        def root_stat(path):
            values = list(original(path));values[4] = 0
            return os.stat_result(values)
        return patch.object(Path, 'lstat', root_stat)

    def test_glibc_family_excluded_and_actual_non_glibc_links_preserved(self):
        source = self.path / 'image-libs'
        source.mkdir()
        for name in ['libc.so.6', 'libm.so.6', 'libmvec.so.1', 'libpthread.so.0', 'libresolv.so.2', 'libdl.so.2', 'librt.so.1', 'libnss_files.so.2', 'ld-linux-x86-64.so.2', 'libc_malloc_debug.so.0', 'libmemusage.so', 'libpcprofile.so', 'libnsl.so.1', 'libanl.so.1', 'libutil.so.1', 'libcrypto.so.3', 'libldap-2.5.so.0.1', 'libicudata.so.72.1']:
            (source / name).write_bytes(b'synthetic')
        (source / 'libldap-2.5.so.0').symlink_to('libldap-2.5.so.0.1')
        (source / 'libicudata.so.72').symlink_to('/usr/lib/x86_64-linux-gnu/libicudata.so.72.1')
        (source / 'libescape.so.1').symlink_to('/etc/alternatives/private')
        (source / 'gconv').mkdir()
        (source / 'gconv/plugin.so').write_bytes(b'not copied')
        target = self.path / 'private-libs'
        with self.root_metadata():
            p.libraries(source, target)
        self.assertEqual(set(x.name for x in target.iterdir()), {'libcrypto.so.3', 'libldap-2.5.so.0.1', 'libldap-2.5.so.0', 'libicudata.so.72.1', 'libicudata.so.72'})
        self.assertEqual(os.readlink(target / 'libicudata.so.72'), 'libicudata.so.72.1')
        self.assertFalse(any(p.GLIBC.match(x.name) for x in target.iterdir()))
        self.assertEqual(stat.S_IMODE((target / 'libcrypto.so.3').stat().st_mode), 0o644)

    def test_glibc_names_never_match_libcrypto_and_libcrypt(self):
        for name in ['libcrypto.so.3', 'libcrypt.so.1', 'libmount.so.1', 'libcap.so.2', 'libmd.so.0']:
            self.assertIsNone(p.GLIBC.match(name), name)
        for name in ['libc.so.6', 'libc-2.36.so', 'libm-2.36.so', 'ld-2.36.so', 'libpthread.so.0']:
            self.assertIsNotNone(p.GLIBC.match(name), name)

    def test_library_special_inode_and_hardlink_refused(self):
        source = self.path / 'source'
        source.mkdir()
        os.mkfifo(source / 'libbad.so.1')
        with self.root_metadata(), self.assertRaisesRegex(p.b.Refused, 'library_inode_forbidden'):
            p.libraries(source, self.path / 'target1')
        (source / 'libbad.so.1').unlink()
        (source / 'libbad.so.1').write_bytes(b'synthetic')
        os.link(source / 'libbad.so.1', source / 'libalias.so.1')
        with self.root_metadata(), self.assertRaisesRegex(p.b.Refused, 'library_untrusted'):
            p.libraries(source, self.path / 'target2')

    def test_stopped_infra_container_only_has_fixed_tmpfs(self):
        row = {'Image': 'image', 'Config': {'Labels': {'com.harbor.storage-tools-install': 'token'}}, 'State': {'Running': False, 'Status': 'created'}, 'HostConfig': {'NetworkMode': 'none', 'Tmpfs': {'/var/lib/postgresql': 'rw,noexec,nosuid,nodev,mode=0700'}}, 'Mounts': []}
        with patch.object(p.b, 'command', side_effect=lambda *_: json.dumps([row]).encode()):
            p.pg_container('owned', 'token', 'image')
            row['Mounts'] = [{'Type': 'tmpfs', 'Destination': '/var/lib/postgresql'}]
            p.pg_container('owned', 'token', 'image')
            for mount in [{'Type': 'bind', 'Destination': '/var/lib/postgresql'}, {'Type': 'volume', 'Destination': '/app/data'}, {'Type': 'bind', 'Destination': '/var/run/docker.sock'}]:
                row['Mounts'] = [mount]
                with self.assertRaisesRegex(p.b.Refused, 'container_identity'):
                    p.pg_container('owned', 'token', 'image')
            row['Mounts'] = []
            row['State']['Running'] = True
            with self.assertRaisesRegex(p.b.Refused, 'container_identity'):
                p.pg_container('owned', 'token', 'image')

    def test_probe_refuses_nonroot_without_matching_test_identity(self):
        with patch.object(p.os, 'geteuid', return_value=1000), patch.object(p.os, 'getegid', return_value=1000):
            with self.assertRaisesRegex(p.b.Refused, 'probe_identity'):
                p.probe(self.path, self.path / 'work', uid=65534, gid=65534)

    def test_fixed_root_entry_and_no_arbitrary_arguments(self):
        result = subprocess.run([sys.executable, str(SCRIPT), '--url'], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout), {'ok': False, 'status': 'arguments_forbidden'})
        if os.getuid() != 0:
            result = subprocess.run([sys.executable, str(SCRIPT)], capture_output=True, text=True, timeout=10)
            self.assertEqual(json.loads(result.stdout), {'ok': False, 'status': 'root_required'})
        self.assertEqual(result.stderr, '')

    def test_library_root_symlink_is_never_followed(self):
        real = self.path / 'real-libs'
        real.mkdir()
        (real / 'libcrypto.so.3').write_bytes(b'synthetic')
        source = self.path / 'image-libs'
        source.symlink_to(real, target_is_directory=True)
        with self.root_metadata(), self.assertRaisesRegex(p.b.Refused, 'library_root_untrusted'):
            p.libraries(source, self.path / 'target')

    def test_missing_probe_passwd_entry_refused_without_creating_work(self):
        with patch.object(p.pwd, 'getpwuid', side_effect=KeyError):
            with self.assertRaisesRegex(p.b.Refused, 'probe_identity_unavailable'):
                p.probe(self.path, self.path / 'work')
        self.assertFalse((self.path / 'work').exists())

    def test_signalled_initdb_is_uncertain_and_never_reported_stopped(self):
        stage = self.path / 'stage'
        binary = stage / 'usr/lib/postgresql/18/bin'
        binary.mkdir(parents=True)
        for name in p.TOOLS:
            (binary / name).write_bytes(b'synthetic')
            (binary / name).chmod(0o755)
        def run(args, **_):
            if '--version' in args:
                return subprocess.CompletedProcess(args, 0, stdout=b'tool (PostgreSQL) 18.6', stderr=b'')
            raise subprocess.CalledProcessError(-signal.SIGKILL, args)
        with self.root_metadata(), patch.object(p.os, 'geteuid', return_value=0), patch.object(p.os, 'chown'), patch.object(p.subprocess, 'run', side_effect=run):
            with self.assertRaisesRegex(p.b.Refused, 'pg_probe_cleanup_required'):
                p.probe(stage, self.path / 'work')
        self.assertTrue((self.path / 'work').is_dir())

    @contextmanager
    def installer_boundaries(self, result='success', changed=False, final_fsync_failure=False):
        config = {'version': 1, 'businessImage': 'registry.invalid/business@sha256:'+'a'*64, 'infrastructureImage': 'registry.invalid/infra@sha256:'+'b'*64, 'revision': 'c'*40, 'nodeVersion': 'v24.21.0'}
        calls = []
        original_fstat = os.fstat
        original_fsync = os.fsync
        def root_fstat(fd):
            values = list(original_fstat(fd));values[4] = 0
            return os.stat_result(values)
        def sync(fd):
            if final_fsync_failure and os.readlink('/proc/self/fd/'+str(fd)) == str(self.path):
                raise OSError('synthetic final directory fsync failure')
            original_fsync(fd)
        def runner(args, **_):
            calls.append(args)
            if args[1] == 'cp':
                target = Path(args[-1])
                target.mkdir()
                if target.name == '_image-libraries':
                    (target / 'libpq.so.5.18').write_bytes(b'synthetic')
                    (target / 'libpq.so.5').symlink_to('libpq.so.5.18')
                else:
                    (target / 'synthetic').write_bytes(b'code')
            return b'owned-container'
        def probe(stage, work):
            work.mkdir(mode=0o700)
            (work / 'synthetic').write_bytes(b'private-test-data')
            if result == 'uncertain':
                raise p.b.Refused('pg_probe_cleanup_required')
            if result == 'failure':
                raise p.b.Refused('pg_tool_version')
            if result == 'cancel':
                p.b.CANCELLED = True
            return {'privateClusterBackupVerified': True, 'hostGlibcUsed': True, 'versions': {}, 'tools': {}}
        with self.root_metadata(), patch.object(p.os, 'fstat', side_effect=root_fstat), patch.object(p.os, 'fsync', side_effect=sync), patch.object(p, 'ROOT', self.path), patch.object(p, 'DESTINATION', self.path / 'postgres'), patch.object(p.b, 'trusted_directory', side_effect=lambda path: os.open(path, os.O_RDONLY|os.O_DIRECTORY)), patch.object(p, 'node_receipt', return_value={'synthetic': 'fixed'}), patch.object(p.b, 'image_identity', return_value=('business', 'infra')), patch.object(p.b, 'configuration', return_value=(b'changed' if changed else b'approved', config)), patch.object(p.b, 'command', side_effect=runner), patch.object(p, 'pg_container'), patch.object(p, 'probe', side_effect=probe):
            yield config, calls

    def test_runtime_share_parent_remains_inside_complete_copy_and_escape_is_refused(self):
        shared = self.path / 'image-share'
        (shared / '18').mkdir(parents=True)
        (shared / 'postgresql.conf.sample').write_text('# approved image sample')
        (shared / '18/postgresql.conf.sample').symlink_to('../postgresql.conf.sample')
        stage = self.path / 'stage'
        (stage / 'usr/share/postgresql').mkdir(parents=True)
        shutil.copytree(shared / '18', stage / 'usr/share/postgresql/18', symlinks=True)
        with self.root_metadata(), self.assertRaisesRegex(p.b.Refused, 'code_link_forbidden'):
            p.b.normalize_tree(stage)
        shutil.rmtree(stage)
        (stage / 'usr/share').mkdir(parents=True)
        shutil.copytree(shared, stage / 'usr/share/postgresql', symlinks=True)
        with self.root_metadata():
            p.b.normalize_tree(stage)
            p.b.verify_tool_tree(stage)
        self.assertEqual((stage / 'usr/share/postgresql/18/postgresql.conf.sample').read_text(), '# approved image sample')
        self.assertTrue((stage / 'usr/share/postgresql/18/postgresql.conf.sample').is_symlink())
        (self.path / 'outside').write_text('outside must stay unchanged')
        (stage / 'usr/share/postgresql/18/outside').symlink_to('../../../../../outside')
        with self.root_metadata(), self.assertRaisesRegex(p.b.Refused, 'code_link_forbidden'):
            p.b.normalize_tree(stage)
        self.assertEqual((self.path / 'outside').read_text(), 'outside must stay unchanged')

    def test_install_publishes_only_after_probe_and_never_replaces_existing_tree(self):
        with self.installer_boundaries() as (config, calls):
            receipt = p.install(b'approved', config)
            copies = [args[2].split(':', 1)[1] for args in calls if args[1] == 'cp']
            self.assertIn('/usr/share/postgresql', copies)
            self.assertNotIn('/usr/share/postgresql/18', copies)
            self.assertTrue(receipt['postgresHostToolsInstalled'])
            saved = json.loads((self.path / 'postgres/installation.json').read_text())
            self.assertEqual(saved['infrastructureImage'], config['infrastructureImage'])
            with self.assertRaisesRegex(p.b.Refused, 'destination_not_empty'):
                p.install(b'approved', config)
            self.assertTrue(any(args[1:3] == ['rm', '-v'] for args in calls))
        self.assertEqual(list(self.path.glob('.postgres-tools-stage-*')), [])
        self.assertEqual(list(self.path.glob('.postgres-tools-probe-*')), [])

    def test_probe_failure_cancel_and_approval_change_refuse_publish_and_clean(self):
        for result, changed, expected in [('failure', False, 'pg_tool_version'), ('cancel', False, 'cancelled'), ('success', True, 'approval_changed')]:
            p.b.CANCELLED = False
            with self.subTest(result=result, changed=changed), self.installer_boundaries(result=result, changed=changed) as (config, _):
                with self.assertRaisesRegex(p.b.Refused, expected):
                    p.install(b'approved', config)
            self.assertFalse((self.path / 'postgres').exists())
            self.assertEqual(list(self.path.glob('.postgres-tools-stage-*')), [])
            self.assertEqual(list(self.path.glob('.postgres-tools-probe-*')), [])

    def test_uncertain_probe_cleanup_keeps_files_until_operator_checks_processes(self):
        with self.installer_boundaries(result='uncertain') as (config, _):
            with self.assertRaisesRegex(p.b.Refused, 'pg_tools_cleanup_recheck_required'):
                p.install(b'approved', config)
        self.assertFalse((self.path / 'postgres').exists())
        self.assertEqual(len(list(self.path.glob('.postgres-tools-stage-*'))), 1)
        self.assertEqual(len(list(self.path.glob('.postgres-tools-probe-*'))), 1)

    def test_post_publish_fsync_failure_preserves_tools_for_recheck(self):
        with self.installer_boundaries(final_fsync_failure=True) as (config, _):
            with self.assertRaisesRegex(p.b.Refused, 'published_recheck_required'):
                p.install(b'approved', config)
        self.assertTrue((self.path / 'postgres/installation.json').is_file())
        self.assertEqual(list(self.path.glob('.postgres-tools-stage-*')), [])

    @unittest.skipUnless(os.getuid() == 0 and os.environ.get('HARBOR_PG_TOOLS_ROOT_TEST') == '1' and (LOCAL / 'usr/lib/postgresql/18/bin/postgres').is_file(), 'requires explicit isolated root and existing PG18 laboratory toolchain')
    def test_actual_root_owned_tools_uid65534_initdb_server_basebackup_and_verify(self):
        # 本机 Ubuntu22 PG 工具只证明该宿主组合；不冒充批准 Bookworm 镜像/173 Ubuntu24。
        self.path.chmod(0o755)
        stage = self.path / 'stage'
        (stage / 'usr/lib/postgresql').mkdir(parents=True)
        (stage / 'usr/share/postgresql').mkdir(parents=True)
        shutil.copytree(LOCAL / 'usr/lib/postgresql/18', stage / 'usr/lib/postgresql/18')
        shutil.copytree(LOCAL / 'usr/share/postgresql/18', stage / 'usr/share/postgresql/18')
        source = self.path / 'image-libs'
        shutil.copytree(LOCAL / 'usr/lib/x86_64-linux-gnu', source, symlinks=True)
        p.libraries(source, stage / 'usr/lib/x86_64-linux-gnu')
        p.b.normalize_tree(stage)
        p.b.verify_tool_tree(stage)
        work = self.path / 'probe'
        original_popen = subprocess.Popen
        def poison_profile(args, *more, **options):
            if str(args[0]).endswith('/postgres') and '-D' in args:
                (work / '.psqlrc').write_text('\\! /usr/bin/touch ' + shlex.quote(str(work / 'psqlrc-executed')) + '\n')
            if str(args[0]).endswith('/psql'):
                self.assertIn('-X', args)
                self.assertEqual(options['env']['HOME'], str(work))
                self.assertEqual(options['env']['PGPASSFILE'], str(work / '.unused-pgpass'))
            return original_popen(args, *more, **options)
        try:
            with patch.object(p.subprocess, 'Popen', side_effect=poison_profile):
                proof = p.probe(stage, work)
        except p.b.Refused as error:
            if str(error) == 'pg_probe_cleanup_required':
                self.retain_for_process_check = True
            raise
        except subprocess.CalledProcessError as error:
            self.fail('synthetic PG probe failed: ' + error.stderr.decode(errors='replace'))
        self.assertTrue(proof['privateClusterBackupVerified'])
        self.assertTrue(proof['hostGlibcUsed'])
        self.assertEqual(set(proof['versions']), set(p.TOOLS))
        self.assertEqual(work.stat().st_uid, 65534)
        self.assertEqual(stat.S_IMODE(work.stat().st_mode), 0o700)
        self.assertTrue((work / 'backup/backup_manifest').is_file())
        self.assertFalse((work / 'data/postmaster.pid').exists())
        self.assertFalse((work / '.s.PGSQL.5432').exists())
        self.assertFalse((work / 'psqlrc-executed').exists())
        self.assertFalse(any(p.GLIBC.match(x.name) for x in (stage / 'usr/lib/x86_64-linux-gnu').iterdir()))
        for directory, _, files in os.walk(stage):
            self.assertEqual(Path(directory).stat().st_uid, 0)
            self.assertEqual(Path(directory).stat().st_mode & 0o022, 0)
            for name in files:
                path = Path(directory) / name
                self.assertEqual(path.lstat().st_uid, 0)
                if not path.is_symlink():
                    self.assertEqual(path.stat().st_mode & 0o022, 0)

        shutil.rmtree(work)
        target = self.path / 'root-file-preserved'
        target.write_bytes(b'must-remain-unchanged')
        original_run = subprocess.run
        def insert_log_link(args, **options):
            result = original_run(args, **options)
            if str(args[0]).endswith('/initdb') and '-D' in args:
                (work / 'postgres.log').symlink_to(target)
            return result
        with patch.object(p.subprocess, 'run', side_effect=insert_log_link):
            with self.assertRaises(FileExistsError):
                p.probe(stage, work)
        self.assertEqual(target.read_bytes(), b'must-remain-unchanged')
        self.assertFalse((work / 'data/postmaster.pid').exists())


if __name__ == '__main__':
    unittest.main()
