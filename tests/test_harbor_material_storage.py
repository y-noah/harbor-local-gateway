import copy
import importlib.util
import json
import os
from pathlib import Path
import stat
import types
import unittest
from unittest.mock import patch

PATH = Path(__file__).resolve().parents[1] / 'ci/harbor-material-storage.py'
spec = importlib.util.spec_from_file_location('material', PATH)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def profile():
    roots = {}
    parent = str(m.STACK / ('local-assurance/' + 'a' * 8 + '-aaaa-aaaa-aaaa-' + 'a' * 12))
    for index, (purpose, target) in enumerate(m.TARGETS.items()):
        logical = m.FIXED_LOGICAL.get(purpose, parent + ('/pg-data' if purpose == 'application-data' else '/runtime'))
        roots[purpose] = {'logical': logical, 'target': str(m.ROOT / target),
                          'uid': 1000 if purpose.startswith('application-') else 999 if purpose == 'restore' else 0,
                          'targetInode': 25 + index}
    return {'version': 1, 'contract': m.CONTRACT, 'diskUuid': '12345678-abcd', 'roots': roots}


class Tests(unittest.TestCase):
    def test_schema_rejects_other_project_paths_incomplete_roots_or_mismatched_pair(self):
        good = profile()
        m.validate(good)
        for mutate in [lambda p: p.update(version=True),
                       lambda p: p['roots'].pop('restore'),
                       lambda p: p['roots']['application-runtime'].update(logical='/opt/other/runtime'),
                       lambda p: p['roots']['application-data'].update(target='/var/lib/other'),
                       lambda p: p['roots']['application-runtime'].update(logical=p['roots']['application-runtime']['logical'].replace('a' * 8, 'b' * 8)),
                       lambda p: p['roots']['restore'].update(uid=0)]:
            value = copy.deepcopy(good)
            mutate(value)
            with self.assertRaises(ValueError):
                m.validate(value)

    def test_missing_marker_is_legacy_only_and_allocate_cannot_fall_back(self):
        with patch.object(m, 'configuration', return_value=None):
            self.assertEqual(m.guard(['restore']), {'enabled': False})
            with self.assertRaisesRegex(ValueError, 'configuration_required'):
                m.allocate('release-tmp', 'release-123')

    def test_installed_marker_checks_expected_application_paths_before_binding(self):
        value = profile()
        calls = []
        with patch.object(m, 'configuration', return_value=value), patch.object(m, 'disk_mount', return_value=9), patch.object(m, 'binding', side_effect=lambda *args: calls.append(args)):
            with self.assertRaisesRegex(ValueError, 'caller_path_mismatch'):
                m.guard(['application-data'], expected={'application-data': '/other'}, required=True)
            self.assertFalse(calls)
            result = m.guard(['application-data'], expected={'application-data': value['roots']['application-data']['logical']})
            self.assertTrue(result['mountsVerified'])
            self.assertEqual(len(calls), 1)

    def test_disk_uuid_or_missing_exact_mount_rejects_before_binding(self):
        for rows in [[], [{'target': '/sdc-disk', 'uuid': 'foreign'}], [{'target': '/', 'uuid': '12345678-abcd'}]]:
            with patch.object(m, 'command', return_value=json.dumps({'filesystems': rows}).encode()), patch.object(m, 'directory', side_effect=AssertionError('must not continue')):
                with self.assertRaisesRegex(ValueError, 'disk_unavailable'):
                    m.disk_mount(profile())

    def test_bind_requires_exact_mount_uuid_device_and_inode(self):
        value = profile()
        row = value['roots']['restore']
        target = types.SimpleNamespace(st_dev=9, st_ino=25)
        for case in ['good', 'logical-inode', 'target-inode', 'target-device', 'uuid', 'not-exact']:
            logical = types.SimpleNamespace(st_dev=9, st_ino=26 if case == 'logical-inode' else 25)
            actual_target = types.SimpleNamespace(st_dev=8 if case == 'target-device' else 9, st_ino=26 if case == 'target-inode' else 25)
            mount = {'target': '/var/lib' if case == 'not-exact' else row['logical'], 'uuid': 'foreign' if case == 'uuid' else value['diskUuid']}
            with patch.object(m, 'directory', side_effect=[actual_target, logical]), patch.object(m, 'command', return_value=json.dumps({'filesystems': [mount]}).encode()):
                if case == 'good':
                    self.assertEqual(m.binding(value, 'restore', 9), Path(row['logical']))
                else:
                    with self.assertRaisesRegex(ValueError, 'bind_unavailable'):
                        m.binding(value, 'restore', 9)

    def test_allocation_refuses_unknown_scope_traversal_or_missing_mount_before_mkdir(self):
        for purpose, name in [('restore', 'release'), ('release-tmp', '../other'), ('release-tmp', '/other'), ('release-tmp', 'a\nb')]:
            with patch.object(m.os, 'mkdir', side_effect=AssertionError('must not create')):
                with self.assertRaises(ValueError):
                    m.allocate(purpose, name)
        with patch.object(m, 'guard', side_effect=ValueError('disk_missing')), patch.object(m.os, 'mkdir', side_effect=AssertionError('must not create')):
            with self.assertRaisesRegex(ValueError, 'disk_missing'):
                m.allocate('release-materials', 'release-1')

    def test_generated_mount_has_explicit_disk_order_and_target_path(self):
        unit = m.unit_text('/var/lib/harbor-restore', '/sdc-disk/harbor-data/restore', r'sdc\x2ddisk.mount')
        for text in [b'Requires=sdc\\x2ddisk.mount\n', b'After=sdc\\x2ddisk.mount\n', b'RequiresMountsFor=/sdc-disk/harbor-data/restore\n', b'Where=/var/lib/harbor-restore\n']:
            self.assertIn(text, unit)
        with self.assertRaises(ValueError):
            m.unit_text('/var/lib/harbor-restore\nWhat=/other', '/sdc-disk/harbor-data/restore', 'sdc.mount')

    def test_uuid_boot_dependency_requires_persistent_generated_fstab_entry(self):
        name = r'sdc\x2ddisk.mount'
        props = '\n'.join(['LoadState=loaded', 'ActiveState=active', 'Where=/sdc-disk', 'UnitFileState=generated', 'SourcePath=/etc/fstab', 'FragmentPath=/run/systemd/generator/' + name]).encode()
        for source, accepted in [('UUID=12345678-abcd', True), ('/dev/disk/by-uuid/12345678-abcd', True), ('/dev/sdc2', False)]:
            replies = [name.encode() + b'\n', props, json.dumps({'filesystems': [{'target': '/sdc-disk', 'source': source}]}).encode()]
            with patch.object(m, 'disk_mount'), patch.object(m, 'command', side_effect=replies):
                if accepted:
                    self.assertEqual(m.boot_unit(profile()), name)
                else:
                    with self.assertRaisesRegex(ValueError, 'boot_disk_not_uuid_bound'):
                        m.boot_unit(profile())

    def test_maintenance_blocks_even_before_final_manifest_exists(self):
        with patch.object(m.os.path, 'lexists', return_value=True), patch.object(m, 'configuration', side_effect=AssertionError('must not fall back')):
            with self.assertRaisesRegex(ValueError, 'maintenance_in_progress'):
                m.guard(['restore'])

    def release_approval(self):
        return {'contract': 'harbor-release-storage-allocation-v1', 'diskUuid': '12345678-abcd',
                'roots': {'release-materials': [9, 25], 'release-tmp': [9, 26]}}

    def test_release_only_allocation_is_explicit_new_disk_proof_not_full_migration(self):
        approval = self.release_approval()
        fake = types.SimpleNamespace(st_dev=9, st_ino=25)
        with patch.object(m, 'configuration', return_value=None), patch.object(m, 'disk_mount', return_value=9), patch.object(m, 'directory', return_value=fake), patch.object(m.os, 'open', return_value=123), patch.object(m.os, 'fstat', return_value=fake), patch.object(m.os, 'mkdir') as mkdir, patch.object(m.os, 'fsync'), patch.object(m.os, 'close'), patch.object(m, 'identity', return_value=[9, 25]):
            result = m.allocate_approved('release-materials', 'pipeline-1', approval)
            self.assertEqual(result['directory'], '/sdc-disk/harbor-data/release-materials/pipeline-1')
            self.assertFalse(result['fullMaterialMigrationVerified'])
            mkdir.assert_called_once_with('pipeline-1', 0o700, dir_fd=123)

    def test_release_only_allocation_rejects_wrong_disk_or_replaced_root_without_writes(self):
        for kind in ['disk', 'inode', 'shared']:
            approval = self.release_approval()
            if kind == 'shared':
                approval['roots']['release-tmp'] = [9, 25]
            fake = types.SimpleNamespace(st_dev=9, st_ino=27)
            with patch.object(m, 'configuration', return_value=None), patch.object(m, 'disk_mount', side_effect=ValueError('disk_unavailable') if kind == 'disk' else None, return_value=9), patch.object(m, 'directory', return_value=fake), patch.object(m.os, 'mkdir', side_effect=AssertionError('must not create')):
                with self.assertRaises(ValueError):
                    m.allocate_approved('release-materials', 'pipeline-1', approval)


    def test_plan_shape_has_no_physical_inode_and_manifest_requires_actual_unique_inodes(self):
        physical = profile()
        planned = copy.deepcopy(physical)
        for row in planned['roots'].values():
            del row['targetInode']
        self.assertEqual(m.validate_plan(planned), planned)
        with self.assertRaisesRegex(ValueError, 'root_invalid'):
            m.validate(planned)
        with self.assertRaisesRegex(ValueError, 'root_invalid'):
            m.validate_plan(physical)
        duplicate = copy.deepcopy(physical)
        for row in duplicate['roots'].values():
            row['targetInode'] = 1
        with self.assertRaisesRegex(ValueError, 'overlapping_target_roots'):
            m.validate(duplicate)
        self.assertEqual(m.validate(physical), physical)


if __name__ == '__main__':
    unittest.main()
