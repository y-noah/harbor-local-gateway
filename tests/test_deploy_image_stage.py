import copy
import gzip
import hashlib
import importlib.util
import io
import json
import os
import signal
from pathlib import Path
import stat
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).parents[1]
spec = importlib.util.spec_from_file_location('image_stage', ROOT / 'ci/release-image.py')
s = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s)
spec = importlib.util.spec_from_file_location('image_wrapper', ROOT / 'ci/deploy-wrapper.py')
w = importlib.util.module_from_spec(spec)
spec.loader.exec_module(w)
IMAGE = 'registry.test/harbor/harbor-local-gateway:' + 'a' * 40
CONFIG = {'image_prefix': IMAGE.rsplit(':', 1)[0], 'image_staging_enabled': True}


def digest(raw):
    return 'sha256:' + hashlib.sha256(raw).hexdigest()


def fixture(directory, role='gateway', revision='a' * 40, plain=b'synthetic OCI layer'):
    source = Path(directory) / 'source'
    blobs = source / 'blobs/sha256'
    blobs.mkdir(parents=True)
    layer = gzip.compress(plain, mtime=0)
    labels = {'org.opencontainers.image.revision': revision, 'com.harbor.storage.postgres': 'postgres-conversation-v2', 'com.harbor.postgres-preflight': 'v1', 'com.harbor.extra-usage-dispatch': 'v1'}
    image = IMAGE.rsplit(':', 1)[0] + ':' + revision
    parent = 'none'
    if role != 'gateway':
        parent = 'sha256:' + 'f' * 64
        image = IMAGE.rsplit(':', 1)[0] + '-pg-infra:' + revision
        labels = {'com.harbor.infrastructure': 'postgres18-report-v1', 'com.harbor.publisher-business-image': IMAGE.rsplit(':', 1)[0] + '@' + parent}
    config = {'os': 'linux', 'architecture': 'amd64', 'rootfs': {'type': 'layers', 'diff_ids': [digest(plain)]}, 'config': {'Labels': labels, 'Cmd': ['node', 'server.mjs']}}
    config_raw = s.canonical(config)
    manifest = {'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.manifest.v1+json', 'config': {'mediaType': 'application/vnd.oci.image.config.v1+json', 'digest': digest(config_raw), 'size': len(config_raw)}, 'layers': [{'mediaType': 'application/vnd.oci.image.layer.v1.tar+gzip', 'digest': digest(layer), 'size': len(layer)}]}
    manifest_raw = s.canonical(manifest)
    for raw in [layer, config_raw, manifest_raw]:
        (blobs / digest(raw)[7:]).write_bytes(raw)
    index = {'schemaVersion': 2, 'manifests': [{'mediaType': manifest['mediaType'], 'digest': digest(manifest_raw), 'size': len(manifest_raw), 'annotations': {'org.opencontainers.image.ref.name': 'temporary-skopeo-name'}}]}
    (source / 'index.json').write_bytes(s.canonical(index))
    (source / 'oci-layout').write_bytes(b'{"imageLayoutVersion":"1.0.0"}')
    target = Path(directory) / 'transport.oci.tar'
    request = s.prepare(source, target, image, digest(manifest_raw), role, parent, {'project': '12', 'pipeline': '34', 'job': '56'})
    return request, target, config


class ImageStageTests(unittest.TestCase):
    def test_complete_closure_size_is_checked_before_copying_cached_layers(self):
        with tempfile.TemporaryDirectory() as directory:
            request, archive, _ = fixture(directory, plain=os.urandom(100000))
            metadata = Path(directory) / 'metadata.tar';s.delta_archive(archive, metadata, [])
            understated = {**request, 'archiveBytes': metadata.stat().st_size}
            target = Path(directory) / 'too-large.tar'
            with patch.object(s, 'reusable_layers') as reused, self.assertRaisesRegex(s.Refused, 'complete_archive_size'):
                s.assemble_delta(metadata, target, understated)
            reused.assert_not_called();self.assertFalse(target.exists())

    def test_new_failed_receipts_do_not_evict_older_complete_cache_candidates(self):
        with tempfile.TemporaryDirectory() as directory:
            request, archive, _ = fixture(directory)
            value = s.identity(request, CONFIG);root = Path(directory) / 'stages';root.mkdir(mode=0o700)
            completed = root / s.stage_id(value);completed.mkdir(mode=0o700);(completed / 'image.oci.tar').write_bytes(archive.read_bytes());(completed / 'image.oci.tar').chmod(0o600)
            s.save(completed / 'receipt.json', s.receipt(completed, value, 'complete', loadCompleted=True, loadClientExitCode=0, nativeImageImported=True, allConfigAndLayersVerified=True))
            os.utime(completed / 'receipt.json', (1, 1))
            for index in range(3):
                failed = root / hashlib.sha256(str(index).encode()).hexdigest();failed.mkdir(mode=0o700);s.save(failed / 'receipt.json', s.receipt(failed, value, 'failed'));os.utime(failed / 'receipt.json', (index + 2, index + 2))
            metadata = Path(directory) / 'metadata.tar';meta = s.delta_archive(archive, metadata, [])
            with patch.object(s, 'ROOT', root), patch.object(s, 'trusted_directory'), patch.object(s, 'private_json', side_effect=lambda p: json.loads(p.read_bytes())):
                plan = s.plan_layers(io.BytesIO(metadata.read_bytes()), {**request, **meta, 'action': 'image-stage-plan'}, value)
                self.assertEqual(plan['missingBlobs'], []);self.assertGreater(plan['reuseBytes'], 0)

    def test_cancelled_receiver_removes_only_its_partial_upload_and_restores_signal_handlers(self):
        with tempfile.TemporaryDirectory() as directory:
            request, archive, _ = fixture(directory);staging = Path(directory) / 'stages'
            previous = signal.getsignal(signal.SIGHUP)
            class Interrupted:
                def __init__(self):
                    self.first = True
                def read(self, limit):
                    if self.first:
                        self.first = False
                        return archive.read_bytes()[:100]
                    os.kill(os.getpid(), signal.SIGHUP)
            with patch.object(s, 'ROOT', staging), patch.object(s, 'trusted_directory'), patch.object(s, 'start_import') as start, patch.object(s.shutil, 'disk_usage', return_value=Mock(free=10 * 1024**3)):
                with self.assertRaisesRegex(s.Refused, 'cancelled'):
                    s.handle(request, Interrupted(), CONFIG)
                self.assertEqual(list(staging.iterdir()), []);start.assert_not_called()
            self.assertEqual(signal.getsignal(signal.SIGHUP), previous)

    def test_load_timeout_waits_for_actual_client_close_and_never_uses_metadata_as_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            request, archive, _ = fixture(directory);value = s.identity(request, CONFIG);stage = Path(directory) / 'stage';stage.mkdir(mode=0o700);target = stage / 'image.oci.tar';target.write_bytes(archive.read_bytes());target.chmod(0o600)
            child = Mock(returncode=None);child.poll.return_value = None
            with (stage / 'lock').open('w') as lock, patch.object(s.subprocess, 'Popen', return_value=child), patch.object(s, 'native_proof') as proof, patch.object(s.time, 'monotonic', side_effect=[0, 0, 0, s.IMPORT_SECONDS + 1]):
                with self.assertRaisesRegex(s.Refused, 'load_timeout'):
                    s._import_locked(stage, value, lock)
                child.terminate.assert_called_once();child.wait.assert_called_once_with(timeout=5);proof.assert_not_called()

    def test_delta_reuses_only_prior_completed_archive_and_reconstructs_the_exact_new_image(self):
        with tempfile.TemporaryDirectory() as directory:
            plain = os.urandom(100000)
            first, first_tar, _ = fixture(Path(directory) / 'first', plain=plain)
            second, second_tar, _ = fixture(Path(directory) / 'second', revision='b' * 40, plain=plain)
            staging = Path(directory) / 'stages';staging.mkdir(mode=0o700)
            first_value = s.identity(first, CONFIG);second_value = s.identity(second, CONFIG)
            old = staging / s.stage_id(first_value);old.mkdir(mode=0o700)
            (old / 'image.oci.tar').write_bytes(first_tar.read_bytes());(old / 'image.oci.tar').chmod(0o600)
            complete = s.receipt(old, first_value, 'complete', loadCompleted=True, loadClientExitCode=0, nativeImageImported=True, allConfigAndLayersVerified=True)
            s.save(old / 'receipt.json', complete)
            metadata = Path(directory) / 'metadata.tar';meta = s.delta_archive(second_tar, metadata, [])
            with patch.object(s, 'ROOT', staging), patch.object(s, 'trusted_directory'), patch.object(s, 'private_json', side_effect=lambda p: json.loads(p.read_bytes())), patch.object(s, 'start_import') as start, patch.object(s.shutil, 'disk_usage', return_value=Mock(free=10 * 1024**3)):
                plan = s.plan_layers(io.BytesIO(metadata.read_bytes()), {**second, **meta, 'action': 'image-stage-plan'}, second_value)
                self.assertEqual(plan['missingBlobs'], []);self.assertGreater(plan['reuseBytes'], 100000)
                self.assertLess(meta['transportBytes'], second['archiveBytes'] // 2)
                received = s.receive(io.BytesIO(metadata.read_bytes()), second_value, {**second, **meta})
                restored = staging / received['stageId'] / 'image.oci.tar'
                self.assertEqual(restored.read_bytes(), second_tar.read_bytes());self.assertEqual(start.call_count, 1)
                self.assertEqual(s.validate_archive(restored, second_value)['config']['Labels']['org.opencontainers.image.revision'], 'b' * 40)
                # An image name/receipt by itself is insufficient after its archive is damaged.
                old_tar = old / 'image.oci.tar';raw = old_tar.read_bytes();old_tar.write_bytes(raw[:-1] + bytes([raw[-1] ^ 1]))
                with self.assertRaisesRegex(s.Refused, 'archive_digest'):
                    s.plan_layers(io.BytesIO(metadata.read_bytes()), {**second, **meta, 'action': 'image-stage-plan'}, second_value)

    def test_missing_or_unfinished_source_cannot_be_used_as_delta_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            request, archive, _ = fixture(directory)
            staging = Path(directory) / 'stages';staging.mkdir(mode=0o700);value = s.identity(request, CONFIG)
            metadata = Path(directory) / 'metadata.tar';meta = s.delta_archive(archive, metadata, [])
            with patch.object(s, 'ROOT', staging), patch.object(s, 'trusted_directory'), patch.object(s, 'private_json', side_effect=lambda p: json.loads(p.read_bytes())):
                plan = s.plan_layers(io.BytesIO(metadata.read_bytes()), {**request, **meta, 'action': 'image-stage-plan'}, value)
                self.assertEqual(len(plan['missingBlobs']), 1)
                old = staging / s.stage_id(value);old.mkdir(mode=0o700);(old / 'image.oci.tar').write_bytes(archive.read_bytes());(old / 'image.oci.tar').chmod(0o600)
                s.save(old / 'receipt.json', s.receipt(old, value, 'importing'))
                again = s.plan_layers(io.BytesIO(metadata.read_bytes()), {**request, **meta, 'action': 'image-stage-plan'}, value)
                self.assertEqual(again['missingBlobs'], plan['missingBlobs'])
                with self.assertRaisesRegex(s.Refused, 'reuse_missing'):
                    s.assemble_delta(metadata, Path(directory) / 'incomplete.tar', value)

    def test_prepare_preserves_manifest_and_layers_and_normalizes_only_index_name(self):
        for role in ['gateway', 'postgres-infrastructure']:
            with self.subTest(role=role), tempfile.TemporaryDirectory() as directory:
                request, archive, config = fixture(directory, role)
                self.assertEqual(s.validate_archive(archive, request), config)
                self.assertEqual(request['archiveBytes'], archive.stat().st_size)
                self.assertEqual(request['archiveSha256'], hashlib.sha256(archive.read_bytes()).hexdigest())
                self.assertEqual(s.identity(request, CONFIG)['manifestDigest'], request['manifestDigest'])
                with tarfile.open(archive) as tar:
                    self.assertTrue(all(row.isfile() and row.mtime == 0 for row in tar))
                second = Path(directory) / 'same.tar'
                duplicate = s.prepare(Path(directory) / 'source', second, request['image'], request['manifestDigest'], role, request['parentDigest'], request['producer'])
                self.assertEqual(duplicate, request)
                self.assertEqual(second.read_bytes(), archive.read_bytes())

    def test_root_switch_and_exact_repository_are_not_controlled_by_request(self):
        with tempfile.TemporaryDirectory() as directory:
            request, _, _ = fixture(directory)
            for enabled in [None, False, 0, 1, 'true', [], {}]:
                with self.subTest(enabled=enabled), self.assertRaises(s.Refused):
                    s.identity(request, {**CONFIG, 'image_staging_enabled': enabled})
            with self.assertRaises(s.Refused):
                s.identity(request, {'image_prefix': CONFIG['image_prefix']})
            for change in [{'image': IMAGE.replace('harbor/', 'other/')}, {'image': IMAGE.rsplit(':', 1)[0] + ':latest'}, {'url': 'http://other.invalid'}, {'mount': '/'}, {'image_staging_enabled': True}, {'archiveBytes': True}, {'archiveBytes': s.MAX_BYTES + 1}, {'parentDigest': 'sha256:' + 'b' * 64}, {'producer': {'project': '12', 'pipeline': '34', 'job': '56', 'approved': True}}]:
                with self.subTest(change=change), self.assertRaises(s.Refused):
                    s.identity({**request, **change}, CONFIG)

    def test_archive_rejects_extra_entries_symlinks_traversal_bad_hash_and_wrong_config(self):
        with tempfile.TemporaryDirectory() as directory:
            request, archive, _ = fixture(directory)
            for name, kind in [('extra.json', tarfile.REGTYPE), ('../outside', tarfile.REGTYPE), ('blobs/sha256/' + '0' * 64, tarfile.SYMTYPE)]:
                target = Path(directory) / 'bad.tar'
                target.write_bytes(archive.read_bytes())
                with tarfile.open(target, 'a') as tar:
                    row = tarfile.TarInfo(name)
                    row.type = kind
                    row.size = 1 if kind == tarfile.REGTYPE else 0
                    row.linkname = '/etc/shadow' if kind == tarfile.SYMTYPE else ''
                    tar.addfile(row, io.BytesIO(b'x'))
                with self.subTest(name=name), self.assertRaises(s.Refused):
                    s.validate_archive(target, request)
            for field in ['manifestDigest', 'configDigest']:
                with self.subTest(field=field), self.assertRaises(s.Refused):
                    s.validate_archive(archive, {**request, field: 'sha256:' + '0' * 64})
            with patch.object(s, 'MAX_BYTES', 1), self.assertRaises(s.Refused):
                s.validate_archive(archive, request)

    def test_changed_compressed_layer_or_uncompressed_diff_id_cannot_be_imported(self):
        with tempfile.TemporaryDirectory() as directory:
            request, archive, _ = fixture(directory)
            corrupted = Path(directory) / 'corrupted.tar'
            with tarfile.open(archive) as src, tarfile.open(corrupted, 'w') as dst:
                for member in src:
                    raw = src.extractfile(member).read()
                    if member.name.startswith('blobs/') and raw[:2] == b'\x1f\x8b':
                        raw = raw[:-1] + bytes([raw[-1] ^ 1])
                    dst.addfile(member, io.BytesIO(raw))
            with self.assertRaises(s.Refused):
                s.validate_archive(corrupted, request)
            with patch.object(s.gzip, 'GzipFile', return_value=io.BytesIO(b'wrong expansion')):
                with self.assertRaisesRegex(s.Refused, 'layer_diff_id'):
                    s.validate_archive(archive, request)

    def test_receive_is_bounded_idempotent_and_never_runs_a_container(self):
        with tempfile.TemporaryDirectory() as directory:
            request, archive, _ = fixture(directory)
            staging = Path(directory) / 'stages'
            value = s.identity(request, CONFIG)
            with patch.object(s, 'ROOT', staging), patch.object(s, 'trusted_directory'), patch.object(s, 'start_import') as start, patch.object(s, 'private_json', side_effect=lambda p: json.loads(p.read_bytes())), patch.object(s.shutil, 'disk_usage', return_value=Mock(free=10 * 1024**3)):
                result = s.receive(io.BytesIO(archive.read_bytes()), value)
                self.assertEqual(result['phase'], 'queued')
                self.assertFalse(result['deploymentAllowed'])
                self.assertFalse(result['ciVerified'])
                self.assertEqual(start.call_count, 1)
                again = s.receive(io.BytesIO(archive.read_bytes()), value)
                self.assertEqual(again['phase'], 'queued')
                self.assertEqual(start.call_count, 1)
                with self.assertRaises(s.Refused):
                    s.receive(io.BytesIO(archive.read_bytes()[:-1]), value)
                with self.assertRaises(s.Refused):
                    s.receive(io.BytesIO(archive.read_bytes() + b'extra'), value)
                self.assertEqual([p.name for p in staging.iterdir()], [result['stageId']])
                saved = staging / result['stageId'] / 'receipt.json'
                row = json.loads(saved.read_bytes());row['phase'] = 'complete';row['nativeImageImported'] = True;saved.write_text(json.dumps(row))
                with self.assertRaisesRegex(s.Refused, 'completion_receipt'):
                    s.status(value)

    def test_failed_start_retains_authenticated_package_and_failure_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            request, archive, _ = fixture(directory);value = s.identity(request, CONFIG);staging = Path(directory) / 'stages'
            with patch.object(s, 'ROOT', staging), patch.object(s, 'trusted_directory'), patch.object(s, 'start_import', side_effect=RuntimeError('start failed')), patch.object(s.shutil, 'disk_usage', return_value=Mock(free=10 * 1024**3)):
                with self.assertRaises(RuntimeError):
                    s.receive(io.BytesIO(archive.read_bytes()), value)
                saved = staging / s.stage_id(value)
                self.assertEqual(json.loads((saved / 'receipt.json').read_bytes())['phase'], 'failed')
                self.assertEqual((saved / 'image.oci.tar').read_bytes(), archive.read_bytes())

    def test_native_metadata_cannot_replace_the_real_successful_load(self):
        with tempfile.TemporaryDirectory() as directory:
            request, archive, config = fixture(directory);value = s.identity(request, CONFIG);stage = Path(directory) / 'stage';stage.mkdir(mode=0o700);path = stage / 'image.oci.tar';path.write_bytes(archive.read_bytes());path.chmod(0o600)
            fake = Mock(returncode=3);fake.poll.return_value = 3
            with (stage / 'lock').open('w') as lock, patch.object(s.subprocess, 'Popen', return_value=fake) as start, patch.object(s, 'native_proof') as proof:
                with self.assertRaisesRegex(s.Refused, 'load_failed'):
                    s._import_locked(stage, value, lock)
                proof.assert_not_called()
                self.assertEqual(start.call_args.args[0][:2], ['/usr/bin/docker', 'load'])
            self.assertEqual(json.loads((stage / 'receipt.json').read_bytes())['phase'], 'importing')

    def test_real_load_exit_and_both_native_names_config_and_layers_are_required(self):
        with tempfile.TemporaryDirectory() as directory:
            request, archive, config = fixture(directory);value = s.identity(request, CONFIG);stage = Path(directory) / 'stage';stage.mkdir(mode=0o700);path = stage / 'image.oci.tar';path.write_bytes(archive.read_bytes());path.chmod(0o600)
            fake = Mock(returncode=0);fake.poll.return_value = 0
            pinned = IMAGE.rsplit(':', 1)[0] + '@' + value['manifestDigest']
            row = {'Id': value['configDigest'], 'RepoDigests': [pinned], 'Descriptor': {'digest': value['manifestDigest']}, 'Os': 'linux', 'Architecture': 'amd64', 'RootFS': {'Layers': config['rootfs']['diff_ids']}, 'Config': config['config']}
            with (stage / 'lock').open('w') as lock, patch.object(s.subprocess, 'Popen', return_value=fake), patch.object(s.subprocess, 'run', return_value=Mock(returncode=0, stdout=json.dumps([row, row]).encode())):
                s._import_locked(stage, value, lock)
            result = json.loads((stage / 'receipt.json').read_bytes())
            self.assertEqual(result['phase'], 'complete');self.assertTrue(result['loadCompleted']);self.assertEqual(result['loadClientExitCode'], 0);self.assertTrue(result['allConfigAndLayersVerified']);self.assertFalse(result['deploymentAllowed'])
            for field, value in [('Descriptor', {}), ('RepoDigests', []), ('RootFS', {'Layers': []}), ('Config', {})]:
                broken = {**row, field: value}
                with self.subTest(field=field), patch.object(s.subprocess, 'run', return_value=Mock(returncode=0, stdout=json.dumps([row, broken]).encode())), self.assertRaises(s.Refused):
                    s.native_proof(stage, s.identity(request, CONFIG), config)

    def test_wrapper_status_does_not_acquire_the_import_or_deployment_lock(self):
        module = Mock();module.handle.return_value = {'phase': 'importing'}
        payload = {'action': 'image-stage-status'}
        stream = io.BytesIO(json.dumps(payload).encode() + b'\n')
        module_source = Mock(loader=Mock())
        trusted = Mock(st_mode=stat.S_IFREG | 0o600, st_uid=0)
        config = json.dumps(CONFIG)
        with patch.object(w.sys, 'stdin', Mock(buffer=stream)), patch.object(w.Path, 'lstat', return_value=trusted), patch.object(w.Path, 'read_text', return_value=config), patch.object(w.importlib.util, 'spec_from_file_location', return_value=module_source), patch.object(w.importlib.util, 'module_from_spec', return_value=module), patch('builtins.open') as open_file, patch.object(w, 'deploy_entry_postgres') as deploy, patch.object(w.sys, 'stdout', io.StringIO()):
            w.main()
            open_file.assert_not_called();deploy.assert_not_called();module.handle.assert_called_once()


if __name__ == '__main__':
    unittest.main()
