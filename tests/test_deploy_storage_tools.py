"""真实私有文件/原子发布与 Node manifest；root/Docker 取证使用明确替身。"""
import concurrent.futures
from contextlib import contextmanager
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'ci/storage-recovery/install-tools.py'
spec = importlib.util.spec_from_file_location('storage_tools_install', SCRIPT)
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)
NODE = Path(os.environ.get('HARBOR_TEST_NODE') or ('/tmp/harbor-scale-tools/node-v24.21.0-linux-x64/bin/node' if Path('/tmp/harbor-scale-tools/node-v24.21.0-linux-x64/bin/node').is_file() else (shutil.which('node') or '/nonexistent-node')))


@contextmanager
def permissive_umask():
    previous = os.umask(0)
    try:
        yield
    finally:
        os.umask(previous)


def config():
    return {'version': 1, 'businessImage': 'registry.invalid/team/business@sha256:'+'a'*64,
            'infrastructureImage': 'registry.invalid/team/infra@sha256:'+'b'*64,
            'revision': 'c'*40, 'nodeVersion': 'v24.21.0'}


def image_rows(value):
    business = {'Id': 'sha256:'+'1'*64, 'RepoDigests': [value['businessImage']], 'Config': {'Labels': {
        'com.harbor.storage.postgres': 'postgres-conversation-v2', 'com.harbor.postgres-preflight': 'v1',
        'com.harbor.extra-usage-dispatch': 'v1', 'org.opencontainers.image.revision': value['revision']}}}
    infra = {'Id': 'sha256:'+'2'*64, 'RepoDigests': [value['infrastructureImage']], 'Config': {'Labels': {
        'com.harbor.infrastructure': 'postgres18-report-v1', 'com.harbor.publisher-business-image': value['businessImage']}}}
    return {value['businessImage']: business, value['infrastructureImage']: infra}


class StorageToolsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='harbor-storage-tools-test-')
        self.path = Path(self.temp.name)
        self.destination = self.path / 'tools'
        self.original_lstat = Path.lstat
        b.CANCELLED = False

    def tearDown(self):
        b.CANCELLED = False
        self.temp.cleanup()

    def root_metadata(self):
        # 仅替代 root 所有权信任边界，保留真实 inode、类型、权限、硬链接、文件读写。
        original = self.original_lstat
        def root_stat(path):
            result = original(path)
            values = list(result)
            values[4] = 0
            return os.stat_result(values)
        return patch.object(Path, 'lstat', root_stat)

    def test_real_atomic_no_replace_and_empty_destination(self):
        stage = self.path / 'stage'
        stage.mkdir()
        (stage / 'code').write_text('new')
        self.destination.mkdir()
        with self.root_metadata():
            b.publish(stage, self.destination)
        self.assertEqual((self.destination / 'code').read_text(), 'new')
        second = self.path / 'second'
        second.mkdir()
        (second / 'code').write_text('bad')
        with self.root_metadata(), self.assertRaisesRegex(b.Refused, 'destination_not_empty'):
            b.publish(second, self.destination)
        self.assertEqual((self.destination / 'code').read_text(), 'new')

    def test_real_concurrent_atomic_publish_has_one_winner(self):
        stages = []
        for index in range(8):
            stage = self.path / ('stage'+str(index))
            stage.mkdir()
            (stage / 'version').write_text(str(index))
            stages.append(stage)
        def run(stage):
            try:
                b.publish(stage, self.destination)
                return True
            except (b.Refused, OSError):
                return False
        with self.root_metadata(), concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(run, stages))
        self.assertEqual(sum(results), 1)
        self.assertIn((self.destination / 'version').read_text(), [str(i) for i in range(8)])

    def test_destination_symlink_and_writable_directory_refused(self):
        stage = self.path / 'stage'
        stage.mkdir()
        self.destination.symlink_to(stage, target_is_directory=True)
        with self.root_metadata(), self.assertRaisesRegex(b.Refused, 'destination_untrusted'):
            b.publish(stage, self.destination)
        self.destination.unlink()
        self.destination.mkdir(mode=0o777)
        self.destination.chmod(0o777)
        with self.root_metadata(), self.assertRaisesRegex(b.Refused, 'destination_untrusted'):
            b.publish(stage, self.destination)

    def test_code_tree_internal_link_allowed_and_no_writable_files(self):
        app = self.path / 'app'
        app.mkdir()
        (app / 'code.js').write_text('code')
        (app / '.env.example').write_text('EXAMPLE=placeholder')
        (app / 'local').symlink_to('code.js')
        (app / 'code.js').chmod(0o666)
        with self.root_metadata():
            b.normalize_tree(app)
        self.assertEqual(stat.S_IMODE((app / 'code.js').stat().st_mode), 0o644)
        self.assertTrue((app / 'local').is_symlink())

    def test_escape_dangling_absolute_and_root_links_refused(self):
        for target in ['/etc/passwd', '../outside', 'missing']:
            app = self.path / 'app'
            app.mkdir()
            (app / 'bad').symlink_to(target)
            with self.root_metadata(), self.assertRaisesRegex(b.Refused, 'code_link_forbidden'):
                b.normalize_tree(app)
            shutil.rmtree(app)
        (self.path / 'real').mkdir()
        (self.path / 'app').symlink_to('real', target_is_directory=True)
        with self.root_metadata(), self.assertRaisesRegex(b.Refused, 'code_root_invalid'):
            b.normalize_tree(self.path / 'app')

    def test_credentials_data_special_hardlinks_and_suid_refused(self):
        for name in ['.env', '.env.production', '.env.test', 'private.key', 'private.p12', 'docker.sock', 'data', 'runtime', '.git', 'secrets', 'credentials', '.ssh', '.codex']:
            app = self.path / 'app'
            app.mkdir()
            (app / name).write_text('synthetic')
            with self.root_metadata(), self.assertRaisesRegex(b.Refused, 'code_material_forbidden'):
                b.normalize_tree(app)
            shutil.rmtree(app)
        app.mkdir()
        os.mkfifo(app / 'fifo')
        with self.root_metadata(), self.assertRaisesRegex(b.Refused, 'code_inode_forbidden'):
            b.normalize_tree(app)
        (app / 'fifo').unlink()
        (app / 'code').write_text('synthetic')
        os.link(app / 'code', app / 'alias')
        with self.root_metadata(), self.assertRaisesRegex(b.Refused, 'code_owner_invalid'):
            b.normalize_tree(app)
        (app / 'alias').unlink()
        (app / 'code').chmod(0o4755)
        with self.root_metadata(), self.assertRaisesRegex(b.Refused, 'code_material_forbidden'):
            b.normalize_tree(app)

    def test_local_image_digest_contract_and_validation_images(self):
        value = config()
        rows = image_rows(value)
        def inspect(args, **_):
            self.assertEqual(args[:4], ['/usr/bin/docker', 'image', 'inspect', args[3]])
            return json.dumps([rows[args[-1]]]).encode()
        with patch.object(b, 'command', side_effect=inspect):
            self.assertEqual(b.image_identity(value), ('sha256:'+'1'*64, 'sha256:'+'2'*64))
            for key in ['businessImage', 'infrastructureImage']:
                labels = rows[value[key]]['Config']['Labels']
                labels['com.harbor.ci-validation-only'] = 'true'
                with self.assertRaisesRegex(b.Refused, 'image_identity'):
                    b.image_identity(value)
                del labels['com.harbor.ci-validation-only']
            rows[value['businessImage']]['RepoDigests'] = []
            with self.assertRaisesRegex(b.Refused, 'image_identity'):
                b.image_identity(value)

    def test_wrong_revision_and_infrastructure_business_binding_rejected(self):
        value = config()
        rows = image_rows(value)
        def inspect(args, **_):
            return json.dumps([rows[args[-1]]]).encode()
        with patch.object(b, 'command', side_effect=inspect):
            rows[value['businessImage']]['Config']['Labels']['org.opencontainers.image.revision'] = 'd'*40
            with self.assertRaisesRegex(b.Refused, 'business_contract'):
                b.image_identity(value)
            rows[value['businessImage']]['Config']['Labels']['org.opencontainers.image.revision'] = value['revision']
            rows[value['infrastructureImage']]['Config']['Labels']['com.harbor.publisher-business-image'] = 'wrong'
            with self.assertRaisesRegex(b.Refused, 'infrastructure_contract'):
                b.image_identity(value)

    def test_stopped_container_must_be_owned_unstarted_and_unmounted(self):
        row = {'Image': 'sha256:'+'1'*64, 'Config': {'Labels': {'com.harbor.storage-tools-install': 'token'}}, 'State': {'Running': False, 'Status': 'created'}, 'Mounts': [], 'HostConfig': {'NetworkMode': 'none'}}
        def inspect(*_, **__):
            return json.dumps([row]).encode()
        with patch.object(b, 'command', side_effect=inspect):
            b.stopped_container('owned', 'token', row['Image'])
            for key, changed in [('Image', 'wrong'), ('Config', {'Labels': {}}), ('State', {'Running': True, 'Status': 'running'}), ('Mounts', [{'Destination': '/var/run/docker.sock'}]), ('HostConfig', {'NetworkMode': 'host'})]:
                original = row[key]
                row[key] = changed
                with self.assertRaisesRegex(b.Refused, 'container_identity'):
                    b.stopped_container('owned', 'token', 'sha256:'+'1'*64)
                row[key] = original

    def synthetic_app(self):
        app = self.path / 'source-app'
        app.mkdir()
        names = ['storage-receiver-candidate-signal.py', 'storage-operator-context.mjs', 'storage-activation-issuer.mjs', 'storage-operator-backup.mjs', 'storage-operator.mjs', 'storage-backup.mjs', 'storage-maintenance.mjs', 'ci/storage-operator-tools/owned-runtime.mjs', 'package.json', 'package-lock.json', 'public/index.html', 'public/app.js', 'public/style.css', 'clients/image-mcp.mjs', 'clients/harbor.example.toml', 'ci/postgres-deploy.py', 'ci/deploy-wrapper.py', 'ci/postgres-usage-cascade.py', 'ci/postgres-department-release.py', 'ci/harbor-material-storage.py', 'migrations/001-synthetic.sql']
        for name in names:
            target = app / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text('{}' if name.endswith('.json') else ('export {};' if name.endswith('.mjs') else 'synthetic'))
        shutil.copyfile(ROOT / 'ci/postgres-image-manifest.mjs', app / 'ci/postgres-image-manifest.mjs')
        result = subprocess.run([str(NODE), str(app / 'ci/postgres-image-manifest.mjs'), '--create'], env={**b.ENV, 'HARBOR_IMAGE_REVISION': config()['revision']}, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        return app

    def projection_app(self):
        app = self.path / 'app'
        source = app / 'ci/storage-operator-tools'
        source.mkdir(parents=True)
        (source / 'owned-runtime.mjs').write_text('export const proof=1;')
        (source / 'process-signal.py').write_text('# synthetic inert source')
        files = {'ci/storage-operator-tools/'+p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in source.iterdir()}
        (app / 'ci/postgres-image-manifest.json').write_text(json.dumps({'version': 1, 'files': files}))
        return app, source

    def test_projection_keeps_exact_package_and_refuses_overwrite_or_tampering(self):
        app, source = self.projection_app()
        b.project_operator_tools(app)
        destination = app / 'tools/capacity'
        self.assertEqual({p.name: p.read_bytes() for p in source.iterdir()}, {p.name: p.read_bytes() for p in destination.iterdir()})
        with self.assertRaisesRegex(b.Refused, 'operator_tools_destination'):
            b.project_operator_tools(app)
        shutil.rmtree(app / 'tools')
        (source / 'owned-runtime.mjs').write_text('changed')
        with self.assertRaisesRegex(b.Refused, 'operator_tools_package'):
            b.project_operator_tools(app)
        self.assertFalse((app / 'tools').exists())

    def test_projection_rejects_links_extra_files_and_traversal_manifest(self):
        app, source = self.projection_app()
        original = (source / 'owned-runtime.mjs').read_text()
        (source / 'extra.mjs').write_text('extra')
        with self.assertRaisesRegex(b.Refused, 'operator_tools_package'):
            b.project_operator_tools(app)
        (source / 'extra.mjs').unlink()
        (source / 'owned-runtime.mjs').unlink()
        (app / 'outside').write_text(original)
        (source / 'owned-runtime.mjs').symlink_to('../../outside')
        with self.assertRaisesRegex(b.Refused, 'operator_tools_package'):
            b.project_operator_tools(app)
        manifest = app / 'ci/postgres-image-manifest.json'
        manifest.write_text(json.dumps({'files': {'ci/storage-operator-tools/../../escape.mjs': 'a'*64}}))
        with self.assertRaisesRegex(b.Refused, 'operator_tools_manifest'):
            b.project_operator_tools(app)

    @unittest.skipUnless(NODE.is_file(), 'actual Node24 toolchain absent; must rerun qualified CI')
    def test_real_business_module_closure_after_projection(self):
        if subprocess.check_output([str(NODE), '--version'], timeout=10).decode().strip() != config()['nodeVersion']:
            self.skipTest('actual approved Node24.21.0 required')
        stage = self.path / 'stage'
        app = stage / 'app'
        app.mkdir(parents=True)
        for name in ['public', 'clients', 'migrations', 'node_modules']:
            shutil.copytree(ROOT / name, app / name, symlinks=False)
        for path in ROOT.glob('*.mjs'):
            shutil.copyfile(path, app / path.name)
        for name in ['package.json', 'package-lock.json', 'storage-receiver-candidate-signal.py']:
            shutil.copyfile(ROOT / name, app / name)
        (app / 'ci').mkdir()
        for pattern in ['postgres-*.mjs', 'postgres-deploy.py', 'deploy-wrapper.py', 'postgres-usage-cascade.py', 'postgres-department-release.py', 'harbor-material-storage.py']:
            for path in (ROOT / 'ci').glob(pattern):
                shutil.copyfile(path, app / 'ci' / path.name)
        package = app / 'ci/storage-operator-tools'
        package.mkdir()
        for pattern in ['*.mjs', '*.py', '*.c']:
            for path in (ROOT / 'tools/capacity').glob(pattern):
                shutil.copyfile(path, package / path.name)
        subprocess.run([str(NODE), str(app / 'ci/postgres-image-manifest.mjs'), '--create'], env={**b.ENV, 'HARBOR_IMAGE_REVISION': config()['revision']}, check=True, capture_output=True, timeout=15)
        (stage / 'node/bin').mkdir(parents=True)
        shutil.copyfile(NODE, stage / 'node/bin/node')
        with self.root_metadata():
            b.normalize_tree(app)
            b.project_operator_tools(app)
            proof = b.verify_copy(stage, config())
        self.assertEqual(proof['revision'], config()['revision'])
        # 同时验证投影被篡改后入口会拒绝，不能只检查原镜像包。
        (app / 'tools/capacity/owned-runtime.mjs').write_text('export {};')
        with self.root_metadata(), self.assertRaises(subprocess.CalledProcessError):
            b.verify_copy(stage, config())

    @unittest.skipUnless(NODE.is_file(), 'actual Node24 toolchain absent; must rerun qualified CI')
    def test_actual_copied_node_and_manifest_through_install_with_trust_substitutes(self):
        if subprocess.check_output([str(NODE), '--version'], timeout=10).decode().strip() != config()['nodeVersion']:
            self.skipTest('actual approved Node24.21.0 required')
        source_app = self.synthetic_app()
        value = config()
        rows = image_rows(value)
        calls = []
        container = None
        real_command = b.command
        def runner(args, **kwargs):
            nonlocal container
            calls.append(args)
            if args[0] != '/usr/bin/docker':
                return real_command(args, **kwargs)
            action = args[1]
            if action == 'image':
                return json.dumps([rows[args[-1]]]).encode()
            if action == 'create':
                self.assertIn('--pull=never', args)
                self.assertIn('--network=none', args)
                self.assertIn('--read-only', args)
                token = args[args.index('--label')+1].split('=', 1)[1]
                container = {'Image': rows[value['businessImage']]['Id'], 'Config': {'Labels': {'com.harbor.storage-tools-install': token}}, 'State': {'Running': False, 'Status': 'created'}, 'Mounts': [], 'HostConfig': {'NetworkMode': 'none'}}
                return b'container-id'
            if action == 'inspect':
                self.assertIsNotNone(container)
                return json.dumps([container]).encode()
            if action == 'cp':
                if args[2].endswith(':/app'):
                    shutil.copytree(source_app, args[3])
                else:
                    shutil.copyfile(NODE, args[3])
                return b''
            if action == 'rm':
                container = None
                return b''
            raise AssertionError('unexpected Docker operation')
        original_fstat = os.fstat
        def root_fstat(fd):
            info = list(original_fstat(fd));info[4] = 0
            return os.stat_result(info)
        with permissive_umask(), self.root_metadata(), patch.object(b.os, 'fstat', side_effect=root_fstat), patch.object(b, 'DESTINATION', self.destination), patch.object(b, 'trusted_directory', side_effect=lambda path: os.open(path, os.O_RDONLY|os.O_DIRECTORY)), patch.object(b, 'configuration', return_value=(b'approved', value)), patch.object(b, 'command', side_effect=runner):
            result = b.install(b'approved', value)
            self.assertEqual(result['revision'], value['revision'])
            self.assertFalse(result['postgresHostToolsInstalled'])
            saved = json.loads((self.destination / 'installation.json').read_text())
            self.assertEqual(saved['sourceDigest'], result['sourceDigest'])
            self.assertEqual((self.destination / 'node/bin/node').read_bytes(), NODE.read_bytes())
            with self.assertRaisesRegex(b.Refused, 'destination_not_empty'):
                b.install(b'approved', value)
            for directory, _, files in os.walk(self.destination):
                self.assertEqual(Path(directory).stat().st_mode & 0o022, 0)
                for name in files:
                    if not (Path(directory) / name).is_symlink():
                        self.assertEqual((Path(directory) / name).stat().st_mode & 0o022, 0)
            # manifest 不覆盖额外凭据；安装器自身必须拒绝，且不发布任何树。
            shutil.rmtree(self.destination)
            (source_app / 'secrets').mkdir()
            (source_app / 'secrets/database-password').write_text('synthetic-not-a-real-secret')
            with self.assertRaisesRegex(b.Refused, 'code_material_forbidden'):
                b.install(b'approved', value)
            self.assertFalse(self.destination.exists())
            shutil.rmtree(source_app / 'secrets')
            (source_app / '.env.production').write_text('SYNTHETIC=value')
            with self.assertRaisesRegex(b.Refused, 'code_material_forbidden'):
                b.install(b'approved', value)
            self.assertFalse(self.destination.exists())
        self.assertIsNone(container)
        self.assertFalse(any(args[1] in ['start', 'run', 'pull', 'restart'] for args in calls if args[0] == '/usr/bin/docker'))
        self.assertEqual(list(self.path.glob('.harbor-storage-tools-stage-*')), [])

    def test_cancel_before_container_creation_does_not_publish(self):
        original_fstat = os.fstat
        def root_fstat(fd):
            info = list(original_fstat(fd));info[4] = 0
            return os.stat_result(info)
        b.CANCELLED = True
        with self.root_metadata(), patch.object(b.os, 'fstat', side_effect=root_fstat), patch.object(b, 'DESTINATION', self.destination), patch.object(b, 'trusted_directory', side_effect=lambda path: os.open(path, os.O_RDONLY|os.O_DIRECTORY)), patch.object(b, 'image_identity', return_value=('business', 'infra')), patch.object(b, 'command') as cmd:
            with self.assertRaisesRegex(b.Refused, 'cancelled'):
                b.install(b'approved', config())
            cmd.assert_not_called()
        self.assertFalse(self.destination.exists())
        self.assertEqual(list(self.path.glob('.harbor-storage-tools-stage-*')), [])

    def test_failed_copy_cleans_owned_container_and_private_stage(self):
        original_fstat = os.fstat
        def root_fstat(fd):
            info = list(original_fstat(fd));info[4] = 0
            return os.stat_result(info)
        calls = []
        def runner(args, **_):
            calls.append(args)
            if args[1] == 'cp':
                raise subprocess.CalledProcessError(1, args, stderr=b'private-content')
            return b'created'
        with self.root_metadata(), patch.object(b.os, 'fstat', side_effect=root_fstat), patch.object(b, 'DESTINATION', self.destination), patch.object(b, 'trusted_directory', side_effect=lambda path: os.open(path, os.O_RDONLY|os.O_DIRECTORY)), patch.object(b, 'image_identity', return_value=('business', 'infra')), patch.object(b, 'stopped_container'), patch.object(b, 'command', side_effect=runner):
            with self.assertRaises(subprocess.CalledProcessError):
                b.install(b'approved', config())
        self.assertFalse(self.destination.exists())
        self.assertTrue(any(args[1:3] == ['rm', '-v'] for args in calls))
        self.assertEqual(list(self.path.glob('.harbor-storage-tools-stage-*')), [])

    def test_copy_manifest_version_and_source_digest_must_be_valid(self):
        stage = self.path / 'stage'
        (stage / 'node/bin').mkdir(parents=True)
        (stage / 'app').mkdir()
        (stage / 'node/bin/node').write_bytes(b'synthetic executable')
        for version, proof, expected in [('v22.1.0', {}, 'node_version'), ('v24.21.0', {'revision': 'wrong', 'source_digest': 'a'*64}, 'manifest_identity'), ('v24.21.0', {'revision': config()['revision'], 'source_digest': 'invalid'}, 'manifest_identity')]:
            with self.subTest(version=version, proof=proof), self.root_metadata(), patch.object(b, 'command', side_effect=[version.encode(), json.dumps(proof).encode()]):
                with self.assertRaisesRegex(b.Refused, expected):
                    b.verify_copy(stage, config())
        (stage / 'node/bin/node').unlink()
        (stage / 'node/bin/node').symlink_to('/bin/true')
        with self.root_metadata(), self.assertRaisesRegex(b.Refused, 'node_identity'):
            b.verify_copy(stage, config())

    def test_fixed_entry_root_and_arguments(self):
        result = subprocess.run([sys.executable, str(SCRIPT), '--url'], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(json.loads(result.stdout), {'ok': False, 'status': 'arguments_forbidden'})
        if os.getuid() != 0:
            result = subprocess.run([sys.executable, str(SCRIPT)], capture_output=True, text=True, timeout=10)
            self.assertEqual(json.loads(result.stdout), {'ok': False, 'status': 'root_required'})
        self.assertEqual(result.stderr, '')


if __name__ == '__main__':
    unittest.main()
