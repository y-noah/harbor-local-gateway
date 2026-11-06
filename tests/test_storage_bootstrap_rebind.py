"""Small file/journal tests; no PostgreSQL, external host or Docker is started."""
import copy
import importlib.util
import json
import os
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('rebind_bootstrap', ROOT / 'ci/storage-recovery/rebind-bootstrap.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
b = r.b


class RebindTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='harbor-rebind-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.root = self.base / 'infra'
        self.root.mkdir(mode=0o711)
        self.root.chmod(0o711)
        self.uid = os.getuid()
        self.patches = []
        def patched(obj, name, value):
            p = patch.object(obj, name, value)
            self.patches.append(p)
            p.start()
        self.addCleanup(lambda: [p.stop() for p in reversed(self.patches)])
        patched(b, 'ROOT', self.root)
        patched(b, 'CONFIG', self.base / 'bootstrap.json')
        patched(b, 'ARCHIVE_PROFILE', self.base / 'archive.json')
        patched(r, 'APPROVAL', self.base / 'rebind.json')
        patched(r, 'ROOT_UID', self.uid)
        patched(r, 'PG_UID', self.uid)
        patched(r.os, 'geteuid', lambda: 0)
        protected = b.protected
        def private_protected(path, uid=0, mode=None, directory=False):
            if Path(path) in self.base.parents:
                return Path(path).lstat()
            return protected(path, self.uid, mode, directory)
        patched(b, 'protected', private_protected)
        write_new = b.write_new
        patched(b, 'write_new', lambda path, data, mode=0o600, uid=0: write_new(path, data, mode, self.uid))
        self.old = json.loads((ROOT / 'ci/storage-recovery/bootstrap.example.json').read_text())
        self.old['publisherDatabase'] = 'retained_old_database'
        self.new = {**self.old, 'businessImage': 'registry.example/business@sha256:' + '1' * 64, 'infrastructureImage': 'registry.example/infra@sha256:' + '2' * 64, 'sourceImageId': 'sha256:' + '3' * 64, 'publisherDatabase': None}
        self.state = {'version': 1, 'owner': 'a' * 32, 'configuration': b.fingerprint(self.old), 'archiveProfileIdentity': [1, 2]}
        for name, mode in [('db', 0o700), ('db/18', 0o700), ('db/18/docker', 0o700), ('db/18/docker/global', 0o700), ('control', 0o755), ('pg-tls', 0o700), ('app-secrets', 0o700), ('app-secrets/device', 0o700), ('archive', 0o700), ('image-volume', 0o555)]:
            path = self.root / name
            path.mkdir(mode=mode)
            path.chmod(mode)
        b.write_new(b.CONFIG, json.dumps(self.old) + '\n')
        b.write_new(self.root / 'state.json', json.dumps(self.state) + '\n')
        b.write_new(self.root / 'control/publisher.json', json.dumps(b.publisher_configuration(self.old)) + '\n', mode=0o644)
        b.write_new(b.ARCHIVE_PROFILE, json.dumps(b.archive_configuration('123456')), mode=0o644)
        for name in ['endpoint.json', 'control/pg_hba.conf', 'postgres-password', 'report-pg.key', 'app-secrets/postgres-report.key', 'pg-ca.key', 'archive.key', 'app-secrets/postgres-ca.crt', 'pg-tls/ca.pem', 'pg-tls/server.key', 'pg-tls/server.pem', 'app-secrets/device/ca.key', 'app-secrets/device/ca.pem', 'app-secrets/device/server.key', 'app-secrets/device/server.pem']:
            b.write_new(self.root / name, ('private-fixture:' + name).encode())
        b.write_new(self.root / 'db/18/docker/PG_VERSION', b'18\n')
        b.write_new(self.root / 'db/18/docker/global/pg_control', (123456).to_bytes(8, __import__('sys').byteorder) + bytes(8184))
        self.pending = self.base / 'storage-transition'
        self.pending.mkdir(mode=0o700)
        (self.pending / 'old-pending.enc').write_bytes(b'old-pending-not-for-this-run')
        self.original = {name: path.read_bytes() for name, (path, _) in r.paths().items()}
        self.source = {'Id': '4' * 64, 'Image': self.new['sourceImageId'], 'Config': {'Labels': {'com.aistrong.project': b.PROJECT}}, 'State': {'Running': True, 'Paused': False, 'Restarting': False, 'Dead': False, 'Pid': 1001, 'Status': 'running', 'StartedAt': '2026-11-06T00:00:00Z'}, 'HostConfig': {'RestartPolicy': {'Name': 'unless-stopped'}}, 'Mounts': [{'Type': 'bind', 'Source': str(self.base / 'data'), 'Destination': '/app/data', 'RW': True}, {'Type': 'bind', 'Source': str(self.base / 'runtime'), 'Destination': '/app/runtime', 'RW': True}]}
        self.infra = None
        self.running = [self.source]
        patched(r.inventory, 'local_containers', lambda: copy.deepcopy(self.running))
        self.approval = {'version': 1, 'id': str(uuid.uuid4()), 'expected': {**{name + 'Sha256': r.digest(raw) for name, raw in self.original.items()}, 'owner': self.state['owner'], 'cluster': '123456'}, 'source': {'containerId': self.source['Id'], 'startedAt': self.source['State']['StartedAt'], 'dataSource': str(self.base / 'data'), 'runtimeSource': str(self.base / 'runtime')}, 'nextBootstrap': self.new}
        self.save_approval()
        patched(b, 'image_checks', lambda config: None)
        patched(b, 'verify_materials', lambda config: b.load_state(config))
        patched(b, 'inspect', lambda kind, name: copy.deepcopy(self.source if name == b.PROJECT else self.infra))
        def docker(*args):
            if args[:2] == ('container', 'ls'):
                return '\n'.join(item['Id'] for item in self.running)
            if args[:2] == ('container', 'inspect'):
                return json.dumps(self.running)
            self.fail('unexpected command: ' + repr(args))
        patched(b, 'docker', docker)

    def save_approval(self):
        if r.APPROVAL.exists():
            r.APPROVAL.write_text(json.dumps(self.approval))
        else:
            b.write_new(r.APPROVAL, json.dumps(self.approval))

    def test_all_resource_holding_states_block_writable_material_mounts(self):
        for status in ['paused', 'restarting', 'removing', 'dead']:
            state = {**self.source['State'], 'Status': status, 'Running': status == 'paused', 'Paused': status == 'paused', 'Restarting': status == 'restarting', 'Dead': status == 'dead', 'Pid': 1002 if status == 'paused' else 0}
            row = {'Id': '7' * 64, 'State': state, 'Mounts': [{'Source': str(self.root / 'db'), 'RW': True}]}
            self.running = [self.source, row]
            with self.subTest(status=status), self.assertRaisesRegex(ValueError, 'running_writer'):
                r.run('apply')
            self.assert_original()
            self.assertFalse((self.root / 'rebind').exists())
            row['Mounts'][0]['RW'] = False
            self.assertFalse(r.run('check')['metadataWritten'])

    def test_unknown_inventory_or_state_cannot_write_metadata(self):
        with patch.object(r.inventory, 'local_containers', side_effect=subprocess.TimeoutExpired('docker', 1)):
            with self.assertRaisesRegex(ValueError, 'running_inventory'):
                r.run('apply')
        self.running.append({'Id': '8' * 64, 'State': {'Running': True}, 'Mounts': []})
        with self.assertRaisesRegex(ValueError, 'running_inventory'):
            r.run('apply')
        self.assert_original()
        self.assertFalse((self.root / 'rebind').exists())

    def assert_original(self):
        for name, (path, _) in r.paths().items():
            self.assertEqual(path.read_bytes(), self.original[name])

    def assert_new(self):
        self.assertEqual(json.loads(b.CONFIG.read_text()), self.new)
        self.assertEqual(json.loads((self.root / 'state.json').read_text()), {**self.state, 'configuration': b.fingerprint(self.new)})
        self.assertIsNone(json.loads((self.root / 'control/publisher.json').read_text())['database'])

    def test_check_has_no_writes_apply_preserves_materials_and_old_snapshots(self):
        witness = r.material_witness()
        self.assertFalse(r.run('check')['metadataWritten'])
        self.assertFalse((self.root / 'rebind').exists())
        self.assert_original()
        before = {name: path.stat().st_ino for name, (path, _) in r.paths().items()}
        self.assertFalse(r.run('apply')['deploymentAllowed'])
        self.assert_new()
        self.assertEqual(r.material_witness(), witness)
        self.assertEqual((self.pending / 'old-pending.enc').read_bytes(), b'old-pending-not-for-this-run')
        journal = r.load_journal(self.root / 'rebind' / self.approval['id'])
        self.assertTrue(journal['complete'])
        for name, (path, _) in r.paths().items():
            self.assertEqual(r.decoded(journal['entries'][name]['old']), self.original[name])
            self.assertEqual(path.stat().st_ino, before[name])
        r.run('apply')
        self.assert_new()

    def test_each_written_phase_can_resume_and_rollback_original_bytes(self):
        for index, name in enumerate(r.paths()):
            with self.subTest(name=name):
                if index:
                    # Separate explicit approvals retain previous rollback history.
                    self.approval['id'] = str(uuid.uuid4())
                    self.save_approval()
                def interrupt(written):
                    if written == name:
                        raise InterruptedError('injected crash after fsync')
                with self.assertRaises(InterruptedError):
                    r.run('apply', after_write=interrupt)
                r.run('apply')
                self.assert_new()
                r.run('rollback')
                self.assert_original()
                self.assertTrue(r.run('rollback')['ready'])
                with self.assertRaisesRegex(ValueError, 'direction_conflict'):
                    r.run('apply')

    def test_partial_in_place_write_can_resume(self):
        write = r.write_control
        def torn(fd, raw):
            os.lseek(fd, 0, os.SEEK_SET)
            os.write(fd, raw[:7])
            os.ftruncate(fd, 7)
            os.fsync(fd)
            raise InterruptedError('power loss')
        with patch.object(r, 'write_control', torn), self.assertRaises(InterruptedError):
            r.run('apply')
        self.assertEqual(len(b.CONFIG.read_bytes()), 7)
        r.run('apply')
        self.assert_new()

    def test_partial_apply_can_rollback_without_finishing_new_version(self):
        def interrupt(name):
            raise InterruptedError('injected')
        with self.assertRaises(InterruptedError):
            r.run('apply', after_write=interrupt)
        r.run('rollback')
        self.assert_original()

    def test_expected_hash_owner_and_fingerprint_fail_before_any_writes(self):
        variants = [('bootstrapSha256', 'f' * 64), ('owner', 'b' * 32)]
        for key, value in variants:
            with self.subTest(key=key):
                saved = self.approval['expected'][key]
                self.approval['expected'][key] = value
                self.save_approval()
                with self.assertRaises(ValueError):
                    r.run('apply')
                self.assert_original()
                self.assertFalse((self.root / 'rebind').exists())
                self.approval['expected'][key] = saved
        self.state['configuration'] = 'f' * 64
        (self.root / 'state.json').write_text(json.dumps(self.state))
        self.approval['expected']['stateSha256'] = r.digest((self.root / 'state.json').read_bytes())
        self.save_approval()
        with self.assertRaisesRegex(ValueError, 'old_identity'):
            r.run('apply')
        self.assertFalse((self.root / 'rebind').exists())

    def test_old_operator_publisher_binding_may_differ_from_bootstrap(self):
        # The operator can bind its old target while bootstrap remains null.
        self.old['publisherDatabase'] = None
        b.CONFIG.write_text(json.dumps(self.old) + '\n')
        self.original['bootstrap'] = b.CONFIG.read_bytes()
        self.approval['expected']['bootstrapSha256'] = r.digest(self.original['bootstrap'])
        self.save_approval()
        r.run('apply')
        self.assert_new()
        r.run('rollback')
        self.assert_original()

    def test_private_parent_modes_are_observed_without_assuming_initdb_parent_mode(self):
        (self.root / 'db/18').chmod(0o755)
        r.run('apply')
        self.assertEqual((self.root / 'db/18').stat().st_mode & 0o777, 0o755)
        r.run('rollback')
        self.assert_original()

    def test_unknown_journal_directory_file_is_preserved_and_refused(self):
        directory = self.root / 'rebind' / self.approval['id']
        directory.mkdir(parents=True, mode=0o700)
        directory.parent.chmod(0o700)
        foreign = directory / 'foreign'
        foreign.write_text('preserve')
        with self.assertRaisesRegex(ValueError, 'journal_directory'):
            r.run('apply')
        self.assertEqual(foreign.read_text(), 'preserve')
        self.assert_original()

    def test_known_interrupted_journal_stage_is_rebuilt_after_validation(self):
        r.run('apply')
        directory = self.root / 'rebind' / self.approval['id']
        b.write_new(directory / '.journal.next', b'{partial')
        r.run('apply')
        self.assertFalse((directory / '.journal.next').exists())
        self.assert_new()

    def test_configuration_scope_and_generation_refuse_changes(self):
        self.new['databasePort'] += 1
        self.save_approval()
        with self.assertRaisesRegex(ValueError, 'unsupported_change'):
            r.run('apply')
        self.new['databasePort'] -= 1
        self.save_approval()
        self.source['State']['StartedAt'] = 'changed'
        with self.assertRaisesRegex(ValueError, 'source_generation'):
            r.run('apply')
        self.assert_original()

    def test_running_infrastructure_foreign_owner_and_writer_refused(self):
        self.infra = {'State': {'Running': True}}
        with self.assertRaisesRegex(ValueError, 'infrastructure_running'):
            r.run('apply')
        self.infra = {'State': {'Running': False, 'Restarting': False, 'Paused': False, 'Pid': 0}, 'Config': {'Labels': {'com.harbor.bootstrap-owner': 'foreign'}}}
        with self.assertRaisesRegex(ValueError, 'infrastructure_owner'):
            r.run('apply')
        self.infra = None
        self.running.append({'Id': '5' * 64, 'State': {**self.source['State'], 'Pid': 1002}, 'Mounts': [{'Source': str(self.root / 'db'), 'RW': True}]})
        with self.assertRaisesRegex(ValueError, 'running_writer'):
            r.run('apply')
        self.assert_original()

    def test_exact_stopped_owned_infrastructure_is_accepted_without_start(self):
        name = b.PROJECT + '-postgres-infra'
        specs = [('db', str(self.root / 'db'), True), ('image-volume', '/var/lib/postgresql', False), ('pg-tls', '/run/harbor-pg-tls', False), ('control', '/run/harbor-infra-control', False), ('report-pg.key', '/run/harbor-report.key', False), ('postgres-password', '/run/harbor-postgres-password', False), ('archive', str(self.root / 'archive'), True), ('archive.key', str(self.root / 'archive.key'), False), (b.ARCHIVE_PROFILE, str(b.ARCHIVE_PROFILE), False)]
        self.infra = {'Id': '6' * 64, 'State': {'Running': False, 'Restarting': False, 'Paused': False, 'Pid': 0}, 'Config': {'Image': self.old['infrastructureImage'], 'Entrypoint': ['/usr/local/bin/harbor-postgres-infra'], 'Cmd': [], 'Labels': {'com.harbor.bootstrap-owner': self.state['owner'], 'com.aistrong.project': b.PROJECT + '-infra'}}, 'HostConfig': {'NetworkMode': name, 'PortBindings': {'5432/tcp': [{'HostIp': '127.0.0.1', 'HostPort': str(self.old['databasePort'])}]}}, 'NetworkSettings': {'Networks': {name: {}}}, 'Mounts': [{'Type': 'bind', 'Source': str(self.root / source), 'Destination': destination, 'RW': rw} for source, destination, rw in specs]}
        self.infra['State'].update(Status='exited', Dead=False)
        self.running.append(self.infra)
        r.run('apply')
        self.assert_new()
        self.assertFalse(self.infra['State']['Running'])
        self.infra['Mounts'][0]['Source'] = str(self.base / 'foreign-db')
        with self.assertRaisesRegex(ValueError, 'container_configuration'):
            r.run('rollback')

    def test_writable_approval_and_linked_controls_refuse_changes(self):
        r.APPROVAL.chmod(0o644)
        with self.assertRaisesRegex(ValueError, 'control_identity'):
            r.run('apply')
        r.APPROVAL.chmod(0o600)
        os.link(b.CONFIG, self.base / 'bootstrap-alias')
        with self.assertRaisesRegex(ValueError, 'control_identity'):
            r.run('apply')
        self.assert_original()

    def test_container_inventory_errors_are_not_absence(self):
        with patch.object(b, 'inspect', side_effect=ValueError('bootstrap_command_failed')):
            with self.assertRaisesRegex(ValueError, 'command_failed'):
                r.run('apply')
        self.assert_original()

    def test_cold_cluster_mismatch_and_postmaster_pid_refuse_writes(self):
        self.approval['expected']['cluster'] = '999'
        self.save_approval()
        with self.assertRaisesRegex(ValueError, 'cluster_mismatch'):
            r.run('apply')
        self.approval['expected']['cluster'] = '123456'
        self.save_approval()
        (self.root / 'db/18/docker/postmaster.pid').write_text('stale is not assumed safe')
        with self.assertRaisesRegex(ValueError, 'cluster_not_cold'):
            r.run('apply')
        self.assert_original()

    def test_replaced_inode_or_completed_external_change_is_preserved(self):
        r.run('apply')
        path = self.root / 'control/publisher.json'
        foreign = self.root / 'control/foreign.json'
        foreign.write_bytes(path.read_bytes())
        foreign.chmod(0o644)
        os.replace(foreign, path)
        with self.assertRaisesRegex(ValueError, 'inode_changed'):
            r.run('rollback')
        self.assertIsNone(json.loads(path.read_text())['database'])

    def test_external_edit_to_completed_metadata_is_not_overwritten(self):
        r.run('apply')
        b.CONFIG.write_text('foreign-owner-edit')
        with self.assertRaisesRegex(ValueError, 'external_change'):
            r.run('rollback')
        self.assertEqual(b.CONFIG.read_text(), 'foreign-owner-edit')

    def test_changed_approval_or_key_blocks_resume(self):
        def interrupt(name):
            raise InterruptedError('injected')
        with self.assertRaises(InterruptedError):
            r.run('apply', after_write=interrupt)
        original_approval = r.APPROVAL.read_bytes()
        self.approval['source']['startedAt'] = 'another-generation'
        self.save_approval()
        with self.assertRaisesRegex(ValueError, 'approval_changed'):
            r.run('apply')
        r.APPROVAL.write_bytes(original_approval)
        key = self.root / 'archive.key'
        key.write_bytes(b'changed-key-must-be-preserved')
        with self.assertRaisesRegex(ValueError, 'material_changed'):
            r.run('apply')
        self.assertEqual(key.read_bytes(), b'changed-key-must-be-preserved')

    def test_rollback_itself_can_resume_after_interruption(self):
        r.run('apply')
        def interrupt(name):
            raise InterruptedError('rollback interrupted')
        with self.assertRaises(InterruptedError):
            r.run('rollback', after_write=interrupt)
        r.run('rollback')
        self.assert_original()


if __name__ == '__main__':
    unittest.main()
