import gzip
import hashlib
import importlib.util
import io
import os
from pathlib import Path
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    'capacity_runtime_extract', Path(__file__).parents[1] / 'tools/capacity/runtime-extract.py')
extractor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(extractor)


def archive(rows):
    result = io.BytesIO()
    with tarfile.open(fileobj=result, mode='w') as stream:
        for name, kind, value in rows:
            item = tarfile.TarInfo(name)
            item.mode = 0o755 if kind == 'dir' else 0o644
            if kind == 'dir':
                item.type = tarfile.DIRTYPE
            elif kind in ('link', 'hard'):
                item.type = tarfile.SYMTYPE if kind == 'link' else tarfile.LNKTYPE
                item.linkname = value
            elif kind == 'device':
                item.type = tarfile.CHRTYPE
            else:
                item.size = len(value)
            stream.addfile(item, io.BytesIO(value) if kind == 'file' else None)
    return result.getvalue()


class RuntimeExtractorTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='harbor-runtime-extract-')
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.layout = self.base / 'oci'
        (self.layout / 'blobs/sha256').mkdir(parents=True)
        self.target = self.base / 'root'
        self.target.mkdir(mode=0o700)
        self.scratch = self.base / 'scratch'
        self.scratch.mkdir(mode=0o700)

    def layer(self, rows):
        raw = archive(rows)
        packed = gzip.compress(raw, mtime=0)
        digest = hashlib.sha256(packed).hexdigest()
        (self.layout / 'blobs/sha256' / digest).write_bytes(packed)
        return {'mediaType': 'application/vnd.oci.image.layer.v1.tar+gzip',
                'digest': 'sha256:' + digest, 'size': len(packed)}, 'sha256:' + hashlib.sha256(raw).hexdigest()

    def run_layers(self, rows, budget=None):
        layers, ids = zip(*(self.layer(row) for row in rows))
        return extractor.extract_layers(layout=self.layout, layers=layers, diff_ids=ids,
                                        destination=self.target, scratch=self.scratch,
                                        budget=budget or extractor.Budget(max_bytes=10**7,
                                            max_files=1000, timeout_seconds=10))

    def test_empty_directory_root_marker_is_ignored_without_mutating_root(self):
        before = self.target.stat()
        budget = extractor.Budget(max_bytes=10**7, max_files=10, timeout_seconds=10)
        self.run_layers([[('', 'dir', None), ('safe', 'file', b'kept')]], budget)
        after = self.target.stat()
        self.assertEqual((before.st_dev, before.st_ino, before.st_mode),
                         (after.st_dev, after.st_ino, after.st_mode))
        self.assertEqual((self.target / 'safe').read_bytes(), b'kept')
        self.assertEqual(budget.files, 2)
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_empty_non_directory_name_and_unscoped_empty_path_still_fail(self):
        with self.assertRaisesRegex(extractor.ExtractionError, 'tar_path'):
            extractor.name_parts('')
        for kind in ['file', 'link', 'hard']:
            with self.subTest(kind=kind):
                with self.assertRaisesRegex(extractor.ExtractionError, 'tar_path'):
                    self.run_layers([[('', kind, b'must not write' if kind == 'file' else 'safe')]])
                self.assertEqual(list(self.target.iterdir()), [])

    def test_layer_type_cover_whiteout_and_opaque_apply_to_lower_only(self):
        report = self.run_layers([
            [('a', 'dir', None), ('a/old', 'file', b'old'), ('a/keep', 'file', b'keep'),
             ('replace', 'dir', None), ('replace/lower', 'file', b'lower'),
             ('remove', 'file', b'remove')],
            [('a/new', 'file', b'new'), ('a/.wh..wh..opq', 'file', b''),
             ('replace', 'file', b'changed'), ('.wh.remove', 'file', b''),
             ('remove', 'file', b'new same-layer value')]])
        self.assertEqual(sorted(p.name for p in (self.target / 'a').iterdir()), ['new'])
        self.assertEqual((self.target / 'replace').read_bytes(), b'changed')
        self.assertEqual((self.target / 'remove').read_bytes(), b'new same-layer value')
        self.assertFalse(report['formalQualified'])
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_symlink_parent_aliases_and_hardlinks_stay_inside_owned_root(self):
        outside = self.base / 'outside'
        outside.mkdir()
        (outside / 'sentinel').write_text('keep')
        self.run_layers([[('usr', 'dir', None), ('usr/bin', 'dir', None),
            ('bin', 'link', 'usr/bin'), ('bin/tool', 'file', b'actual'),
            ('bin/copy', 'hard', 'usr/bin/tool'),
            ('absolute', 'link', str(outside)),
            ('absolute/sentinel', 'file', b'owned'),
            ('nested', 'link', 'usr/bin'), ('nested/rootlink', 'link', '/target')]])
        self.assertEqual((outside / 'sentinel').read_text(), 'keep')
        self.assertEqual((self.target / 'usr/bin/tool').read_bytes(), b'actual')
        self.assertEqual((self.target / 'usr/bin/tool').stat().st_ino,
                         (self.target / 'usr/bin/copy').stat().st_ino)
        self.assertEqual(os.readlink(self.target / 'usr/bin/rootlink'), '../../target')
        self.assertFalse(os.readlink(self.target / 'absolute').startswith('/'))

    def test_symlink_and_directory_type_replacement_removes_lower_subtree(self):
        self.run_layers([[('node', 'link', '/somewhere')],
                         [('node', 'dir', None), ('node/file', 'file', b'yes')]])
        self.assertFalse((self.target / 'node').is_symlink())
        self.assertEqual((self.target / 'node/file').read_bytes(), b'yes')

    def test_escape_names_links_cycles_devices_and_nonempty_whiteouts_fail(self):
        for row in [('../escape', 'file', b'x'), ('/escape', 'file', b'x'),
                    ('link', 'link', '../escape'), ('node', 'device', None),
                    ('.wh.old', 'file', b'not empty'), ('alias', 'hard', '../outside')]:
            with self.subTest(row=row), self.assertRaises(extractor.ExtractionError):
                self.run_layers([[row]])
        with self.assertRaisesRegex(extractor.ExtractionError, 'cycle'):
            self.run_layers([[('cycle', 'link', 'cycle'), ('cycle/file', 'file', b'x')]])

    def test_corrupt_blob_and_wrong_uncompressed_diff_id_are_rejected(self):
        layer, diff = self.layer([('file', 'file', b'x')])
        path = self.layout / 'blobs/sha256' / layer['digest'][7:]
        path.write_bytes(path.read_bytes()[:-1] + b'!')
        for descriptor, expected in [(layer, diff)]:
            with self.assertRaisesRegex(extractor.ExtractionError, 'blob_digest'):
                extractor.extract_layers(layout=self.layout, layers=[descriptor], diff_ids=[expected],
                    destination=self.target, scratch=self.scratch,
                    budget=extractor.Budget(max_bytes=10**7, max_files=10, timeout_seconds=10))
        layer, _ = self.layer([('file', 'file', b'x')])
        with self.assertRaisesRegex(extractor.ExtractionError, 'diff_id'):
            extractor.extract_layers(layout=self.layout, layers=[layer], diff_ids=['sha256:' + '0'*64],
                destination=self.target, scratch=self.scratch,
                budget=extractor.Budget(max_bytes=10**7, max_files=10, timeout_seconds=10))
        self.assertEqual(list(self.target.iterdir()), [])

    def test_budget_file_count_expansion_and_cancelled_state_fail_before_copy(self):
        for budget in [extractor.Budget(max_bytes=100, max_files=10, timeout_seconds=10),
                       extractor.Budget(max_bytes=10**7, max_files=1, timeout_seconds=10),
                       extractor.Budget(max_bytes=10**7, max_files=10, timeout_seconds=10)]:
            if budget.limit > 100 and budget.max_files > 1:
                budget.cancelled = True
            with self.subTest(limit=budget.limit, files=budget.max_files), self.assertRaises(extractor.ExtractionError):
                self.run_layers([[('one', 'file', b'a'), ('two', 'file', b'b')]], budget)
            self.assertEqual(list(self.target.iterdir()), [])
            self.assertEqual(list(self.scratch.iterdir()), [])


    def test_upper_directory_type_precedes_opaque_without_erasing_other_lower_target(self):
        for kind, value in [('link', 'b'), ('file', b'lower file')]:
            with self.subTest(kind=kind):
                # Each variant has its own exact empty task root.
                import shutil
                shutil.rmtree(self.target)
                self.target.mkdir(mode=0o700)
                self.run_layers([[('b', 'dir', None), ('b/keep', 'file', b'keep'),
                                  ('a', kind, value)],
                                 [('a', 'dir', None), ('a/.wh..wh..opq', 'file', b''),
                                  ('a/new', 'file', b'new')]])
                self.assertEqual((self.target / 'b/keep').read_bytes(), b'keep')
                self.assertFalse((self.target / 'a').is_symlink())
                self.assertEqual((self.target / 'a/new').read_bytes(), b'new')

    def test_alias_is_expanded_before_parent_component_for_links_and_writes(self):
        self.run_layers([[('real', 'dir', None), ('real/sub', 'dir', None),
            ('real/target', 'dir', None), ('alias', 'link', 'real/sub'),
            ('link', 'link', 'alias/../target'), ('link/written', 'file', b'correct')]])
        self.assertEqual((self.target / 'real/target/written').read_bytes(), b'correct')
        self.assertEqual((self.target / 'link').resolve(), self.target / 'real/target')
        self.assertFalse((self.target / 'target').exists())

    def test_later_alias_cannot_turn_an_earlier_link_into_a_root_escape(self):
        with self.assertRaisesRegex(extractor.ExtractionError, 'link_escape'):
            self.run_layers([[('a', 'dir', None),
                              ('a/link', 'link', 'missing/../../outside')],
                             [('a/missing', 'link', '/')]])

    def test_verified_bytes_uses_one_snapshot_when_source_changes_after_read(self):
        old, new = b'pinned source', b'changed source'
        class MutatingFile(io.BytesIO):
            calls = 0
            def read(self, limit=-1):
                self.calls += 1
                value = super().read(limit)
                # Equivalent to an external in-place rewrite immediately after
                # the one read has returned its immutable bytes snapshot.
                self.seek(0)
                self.truncate(0)
                self.write(new)
                return value
        file = MutatingFile(old)
        budget = extractor.Budget(max_bytes=1000, max_files=1, timeout_seconds=10)
        copied = extractor.verified_bytes(file, 100, hashlib.sha256(old).hexdigest(), budget)
        self.assertEqual(copied, old)
        self.assertEqual(file.calls, 1)
        self.assertEqual(file.getvalue(), new)
        with self.assertRaisesRegex(extractor.ExtractionError, 'copy_source'):
            extractor.verified_bytes(io.BytesIO(new), 100,
                                     hashlib.sha256(old).hexdigest(), budget)


    def test_changed_upper_alias_does_not_prewrite_lower_target(self):
        self.run_layers([[('b', 'dir', None), ('c', 'dir', None),
                          ('a', 'link', 'b')],
                         [('a', 'link', 'c'), ('a/sub', 'dir', None)]])
        self.assertTrue((self.target / 'c/sub').is_dir())
        self.assertFalse((self.target / 'b/sub').exists())

    def test_layer_namespace_plan_does_not_replay_from_upper_end_state(self):
        self.run_layers([[('b', 'dir', None), ('c', 'dir', None),
                          ('a', 'link', 'b')],
                         [('a/sub', 'dir', None), ('b', 'file', b'replaced'),
                          ('a', 'link', 'c'), ('a/other', 'dir', None)]])
        self.assertTrue((self.target / 'c/other').is_dir())
        self.assertFalse((self.target / 'c/sub').exists())
        self.assertEqual((self.target / 'b').read_bytes(), b'replaced')

    def test_directory_merge_applies_upper_mode_and_sticky_without_suid_sgid(self):
        self.run_layers([[('a', 'dir', None)]])
        os.chmod(self.target / 'a', 0o400)
        root = extractor.Root.__new__(extractor.Root)
        root.fd = os.open(self.target, os.O_RDONLY | os.O_DIRECTORY)
        root.path = str(self.target)
        try:
            item = tarfile.TarInfo('a')
            item.type = tarfile.DIRTYPE
            item.mode = 0o7755
            root.write(None, item, ['a'], extractor.Budget(
                max_bytes=100, max_files=1, timeout_seconds=10))
            self.assertEqual((self.target / 'a').stat().st_mode & 0o7777, 0o1755)
        finally:
            root.close()

    def test_fifo_copy_source_is_rejected_without_a_writer_or_blocking(self):
        fifo = self.base / 'changed-source'
        os.mkfifo(fifo)
        import subprocess
        import sys
        code = ('import importlib.util,sys;'
                's=importlib.util.spec_from_file_location("extractor",sys.argv[1]);'
                'm=importlib.util.module_from_spec(s);s.loader.exec_module(m);'
                'm.open_regular(sys.argv[2])')
        result = subprocess.run([sys.executable, '-c', code, spec.origin, str(fifo)],
                                capture_output=True, text=True, timeout=2)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('capacity_runtime_copy_source_type', result.stderr)


    def test_opaque_removal_is_visible_when_resolving_lower_alias_parents(self):
        self.run_layers([[('a', 'dir', None), ('x', 'dir', None),
                          ('x/keep', 'file', b'keep'), ('a/sub', 'link', '../x')],
                         [('a/.wh..wh..opq', 'file', b''),
                          ('a/sub/new', 'file', b'new'),
                          ('a/sub/.wh.keep', 'file', b'')]])
        self.assertEqual((self.target / 'x/keep').read_bytes(), b'keep')
        self.assertFalse((self.target / 'a/sub').is_symlink())
        self.assertEqual((self.target / 'a/sub/new').read_bytes(), b'new')


    def test_material_fd_remains_the_verified_inode_after_path_replacement(self):
        source = self.base / 'material'
        source.mkdir()
        (source / 'sentinel').write_text('private')
        identity = source.stat()
        item = {'source': str(source), 'device': identity.st_dev, 'inode': identity.st_ino}
        fd = extractor.open_material_directory(item)
        try:
            source.rename(self.base / 'original-material')
            source.symlink_to('/', target_is_directory=True)
            reference = '/proc/self/fd/' + str(fd)
            self.assertEqual(os.stat(reference).st_ino, identity.st_ino)
            self.assertEqual(os.listdir(reference), ['sentinel'])
            with self.assertRaises(extractor.ExtractionError):
                extractor.open_material_directory(item)
        finally:
            os.close(fd)

    def test_root_headers_count_against_file_budget_before_they_are_ignored(self):
        budget = extractor.Budget(max_bytes=10**7, max_files=2, timeout_seconds=10)
        with self.assertRaisesRegex(extractor.ExtractionError, 'extract_budget'):
            self.run_layers([[('', 'dir', None), ('./', 'dir', None),
                              ('.', 'dir', None), ('./', 'dir', None)]], budget)
        self.assertEqual(budget.files, 3)
        self.assertEqual(list(self.target.iterdir()), [])

    def test_root_header_parse_observes_cancellation(self):
        class CancelAtThirdHeader(extractor.Budget):
            def file(self, size):
                super().file(size)
                if self.files == 3:
                    self.cancelled = True
        budget = CancelAtThirdHeader(max_bytes=10**7, max_files=100, timeout_seconds=10)
        with self.assertRaisesRegex(extractor.ExtractionError, 'extract_cancelled'):
            self.run_layers([[('.', 'dir', None)] * 10], budget)
        self.assertLessEqual(budget.files, 4)
        self.assertEqual(list(self.target.iterdir()), [])


    def test_identity_uses_exact_uint64_strings_and_rejects_lossy_numbers(self):
        self.assertEqual(extractor.identity_value('18446744073709551615'), 2**64 - 1)
        for value in [2**53, True, 1.0, '-1', '01', '18446744073709551616']:
            with self.subTest(value=value), self.assertRaises(extractor.ExtractionError):
                extractor.identity_value(value)

    def test_boolean_resource_budgets_are_not_integer_configuration(self):
        for key in ['max_bytes', 'max_files', 'timeout_seconds']:
            options = {'max_bytes': 1000, 'max_files': 10, 'timeout_seconds': 10}
            options[key] = True
            with self.subTest(key=key), self.assertRaises(extractor.ExtractionError):
                extractor.Budget(**options)


if __name__ == '__main__':
    unittest.main()
