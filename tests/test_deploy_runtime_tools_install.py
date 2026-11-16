import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import patch
import test_deploy_image_stage_install as base
i, upgrade, controller = base.i, base.upgrade, base.controller
REAL_READ, REAL_PARENTS, REAL_MODULE = i.read, i.parents, i.module
REAL_ENTRY_LOADER = controller.runtime_upgrade_module


class RuntimeToolsInstallTests(unittest.TestCase):
    def fixture_runtime(self, directory, stack):
        paths, ticket, manifest, historical, history_before, args = base.ImageStageInstallTests.fixture(self, directory, stack)
        root = Path(directory)
        Path(i.LOCKS[0]).chmod(0o644)  # Existing wrapper may create the deploy lock under umask022.
        compose, runtime_upgrade = root / 'compose.py', root / 'upgrade.py'
        stack.enter_context(patch.object(i, 'COMPOSE', compose))
        stack.enter_context(patch.object(i, 'UPGRADE', runtime_upgrade))
        for path in [compose, runtime_upgrade, root / 'host-3.py']:
            path.write_bytes(b'OLD_RUNTIME = True\n');path.chmod(0o644)
        paths['wrapper.py'].write_bytes(b'OLD_WRAPPER = True\n')
        proxy = b'from test_postgres_runtime_upgrade import upgrade as fixture_upgrade\ndef __getattr__(name): return getattr(fixture_upgrade, name)\n'
        runtime_upgrade.write_bytes(proxy + b'VERSION = \"old\"\n')
        stack.enter_context(patch.object(i, 'module', side_effect=lambda path,name: controller if path == i.ENTRY else REAL_MODULE(path,name)))
        tools = (*upgrade.HOST_FILES, str(compose), str(runtime_upgrade), str(root / 'host-3.py'))
        stack.enter_context(patch.object(upgrade, 'HOST_FILES', tools))
        manifest['files'] = {name: i.sha(Path(name).read_bytes()) for name in tools}
        paths['manifest.json'].write_bytes(i.body(manifest))
        paths['helper.py'].write_bytes(b'PRESERVED_STAGE_HELPER = True\n');paths['helper.py'].chmod(0o644)
        source = args[0]
        (source / 'postgres-deploy.py').write_bytes(b'NEW_COMPOSE = True\n')
        (source / 'postgres-upgrade-deploy.py').write_bytes(proxy + b'VERSION = \"new\"\n')
        # A different unapproved source stage helper must never be installed.
        (source / 'release-image.py').write_bytes(b'UNAPPROVED_HELPER = True\n')
        args = (source, args[1], i.sha(paths['manifest.json'].read_bytes()), args[3], args[4], i.sha(paths['helper.py'].read_bytes()))
        kwargs = dict(runtime=True, expected_compose=i.sha((source / 'postgres-deploy.py').read_bytes()), expected_upgrade=i.sha((source / 'postgres-upgrade-deploy.py').read_bytes()))
        paths.update(compose=compose, runtime_upgrade=runtime_upgrade)
        return paths, ticket, manifest, historical, history_before, args, kwargs

    def test_runtime_batch_preserves_helper_durable_authorization_and_retry(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            paths, old, manifest, historical, history_before, args, kwargs = self.fixture_runtime(directory, stack)
            before = {p: p.read_bytes() for p in [i.WRAPPER, i.COMPOSE, i.UPGRADE, i.HELPER, i.MANIFEST, i.TICKET]}
            authorization = controller.read_active_authorization(old['to']['image'])
            self.assertTrue(i.install(*args, **kwargs)['installed'])
            self.assertEqual(i.HELPER.read_bytes(), before[i.HELPER])
            self.assertEqual(REAL_MODULE(i.UPGRADE,'verified_candidate').VERSION,'new')
            self.assertEqual(i.COMPOSE.read_bytes(), (args[0] / 'postgres-deploy.py').read_bytes())
            self.assertEqual(i.UPGRADE.read_bytes(), (args[0] / 'postgres-upgrade-deploy.py').read_bytes())
            self.assertEqual(controller.read_active_authorization(old['to']['image']), authorization)
            self.assertEqual(historical.read_bytes(), history_before)
            self.assertTrue(i.install(*args, **kwargs)['reused'])
            self.assertFalse((Path(directory)/'installs'/('image-stage-'+args[1])).exists())
            with self.assertRaises((FileNotFoundError, AssertionError)):i.rollback(args[1])
            self.assertTrue(i.rollback(args[1], runtime=True)['rolledBack'])
            for p, raw in before.items():self.assertEqual(p.read_bytes(), raw)
            self.assertTrue(i.install(*args, **kwargs)['installed'])

    def test_runtime_failure_restores_all_before_bytes_then_retry(self):
        for failing in ['compose', 'runtime_upgrade', 'wrapper.py', 'manifest.json', 'ticket.json']:
            with self.subTest(failing=failing), tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
                paths, old, manifest, historical, history_before, args, kwargs = self.fixture_runtime(directory, stack)
                before = {p: p.read_bytes() for p in [i.WRAPPER, i.COMPOSE, i.UPGRADE, i.HELPER, i.MANIFEST, i.TICKET]}
                write = i.write;failed = False
                def injected(path, raw, mode):
                    nonlocal failed
                    write(path, raw, mode)
                    if path == paths[failing] and not failed:
                        failed = True
                        raise OSError('synthetic durable-write acknowledgement failure')
                with patch.object(i, 'write', side_effect=injected), self.assertRaises(OSError):i.install(*args, **kwargs)
                for p, raw in before.items():self.assertEqual(p.read_bytes(), raw)
                self.assertEqual(historical.read_bytes(), history_before)
                self.assertTrue(i.install(*args, **kwargs)['installed'])

    def test_wrong_source_bytes_root_and_invalid_syntax_refused(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            paths, old, manifest, historical, history_before, args, kwargs = self.fixture_runtime(directory, stack)
            original = i.MANIFEST.read_bytes()
            with self.assertRaisesRegex(ValueError, 'runtime_source_digest'):i.install(*args, **{**kwargs, 'expected_compose':'0'*64})
            self.assertEqual(i.MANIFEST.read_bytes(), original)
            with patch.object(i.os, 'geteuid', return_value=1000), self.assertRaisesRegex(ValueError, 'root_required'):i.install(*args, **kwargs)
            (args[0] / 'postgres-deploy.py').write_bytes(b'not valid python !')
            with self.assertRaises(SyntaxError):i.install(*args, **{**kwargs, 'expected_compose':i.sha((args[0]/'postgres-deploy.py').read_bytes())})
            self.assertEqual(i.MANIFEST.read_bytes(), original)

    def test_runtime_pending_ticket_rejected_before_write(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
            paths, old, manifest, historical, history_before, args, kwargs = self.fixture_runtime(directory, stack)
            pending = copy.deepcopy(old);pending['deploymentId'] = '12345678-1234-4234-8234-123456789012'
            i.TICKET.write_bytes(i.body(pending));changed = (*args[:3], i.sha(i.TICKET.read_bytes()), *args[4:])
            with self.assertRaisesRegex(ValueError, 'current_release'):i.install(*changed, **kwargs)
            self.assertEqual(i.TICKET.read_bytes(), i.body(pending))
            self.assertEqual(historical.read_bytes(), history_before)

    def test_safe_helper_modes_survive_runtime_rollback(self):
        for mode in [0o400,0o640]:
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as directory,ExitStack() as stack:
                paths,old,manifest,historical,history_before,args,kwargs=self.fixture_runtime(directory,stack)
                i.HELPER.chmod(mode);inode=i.HELPER.stat().st_ino
                i.install(*args,**kwargs);i.rollback(args[1],runtime=True)
                self.assertEqual(i.HELPER.stat().st_mode&0o777,mode);self.assertEqual(i.HELPER.stat().st_ino,inode)
                self.assertTrue(i.install(*args,**kwargs)['installed'])

    def test_invalid_module_initialization_rolls_back_and_crash_uses_old_backup(self):
        for target in ['deploy-wrapper.py','postgres-deploy.py','postgres-upgrade-deploy.py']:
            with self.subTest(target=target),tempfile.TemporaryDirectory() as directory,ExitStack() as stack:
                paths,old,manifest,historical,history_before,args,kwargs=self.fixture_runtime(directory,stack)
                source=args[0]/target;source.write_bytes(b'raise RuntimeError("synthetic import failure")\n')
                if target=='deploy-wrapper.py':args=(*args[:4],i.sha(source.read_bytes()),args[5])
                else:kwargs['expected_compose' if target=='postgres-deploy.py' else 'expected_upgrade']=i.sha(source.read_bytes())
                before={p:p.read_bytes() for p in [i.WRAPPER,i.COMPOSE,i.UPGRADE,i.HELPER,i.MANIFEST,i.TICKET]}
                with self.assertRaisesRegex(RuntimeError,'synthetic import failure'):i.install(*args,**kwargs)
                for p,raw in before.items():self.assertEqual(p.read_bytes(),raw)
                if target!='postgres-upgrade-deploy.py':continue
                write=i.write
                def interrupted(path,raw,mode):
                    if path==i.UPGRADE:
                        if raw==source.read_bytes():write(path,raw,mode)
                        raise KeyboardInterrupt('synthetic crash before unwind completes')
                    write(path,raw,mode)
                with patch.object(i,'write',side_effect=interrupted),self.assertRaises(KeyboardInterrupt):i.install(*args,**kwargs)
                with self.assertRaisesRegex(RuntimeError,'synthetic import failure'):REAL_MODULE(i.UPGRADE,'broken_candidate')
                self.assertTrue(i.rollback(args[1],runtime=True)['rolledBack'])
                for p,raw in before.items():self.assertEqual(p.read_bytes(),raw)
                self.assertEqual(historical.read_bytes(),history_before)

    def test_module_ignores_same_size_timestamp_bytecode(self):
        import py_compile
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'module.py';path.write_bytes(b'VERSION="old"\n');stamp=path.stat().st_mtime
            py_compile.compile(str(path),doraise=True)
            path.write_bytes(b'VERSION="new"\n');os.utime(path,(stamp,stamp))
            with patch.object(i,'read',side_effect=lambda p:Path(p).read_bytes()):
                self.assertEqual(i.module(path,'new_bytes').VERSION,'new')
                path.write_bytes(b'VERSION="old"\n');os.utime(path,(stamp,stamp));self.assertEqual(i.module(path,'old_bytes').VERSION,'old')

    def test_runtime_entry_uses_loaded_bytes_despite_same_timestamp_cache(self):
        import py_compile
        with tempfile.TemporaryDirectory() as directory,ExitStack() as stack:
            paths,old,manifest,historical,history_before,args,kwargs=self.fixture_runtime(directory,stack)
            stamp=i.UPGRADE.stat().st_mtime;py_compile.compile(str(i.UPGRADE),doraise=True)
            self.assertEqual(len(i.UPGRADE.read_bytes()),len((args[0]/'postgres-upgrade-deploy.py').read_bytes()))
            # Restore the real ENTRY loader; only its fixed path/host ownership
            # checks are redirected to the private fixture, not its loading logic.
            controller.runtime_upgrade_module=REAL_ENTRY_LOADER
            stack.enter_context(patch.object(controller,'Path',side_effect=lambda *a:i.UPGRADE if a==('/usr/local/lib/harbor/postgres-upgrade-deploy.py',) else Path(*a)))
            stack.enter_context(patch.object(controller,'trusted_parents'))
            require=controller.require
            stack.enter_context(patch.object(controller,'require',side_effect=lambda ok,code:None if code=='runtime_upgrade_helper_untrusted' else require(ok,code)))
            self.assertEqual(controller.runtime_upgrade_module().VERSION,'old')
            write=i.write
            def same_timestamp(path,raw,mode):
                write(path,raw,mode)
                if path==i.UPGRADE:os.utime(path,(stamp,stamp))
            with patch.object(i,'write',side_effect=same_timestamp):i.install(*args,**kwargs)
            self.assertEqual(controller.runtime_upgrade_module().VERSION,'new')
            self.assertEqual(controller.read_active_authorization(old['to']['image'])['state'],'active')
            i.rollback(args[1],runtime=True)
            self.assertEqual(controller.runtime_upgrade_module().VERSION,'old')

    @unittest.skipUnless(os.geteuid() == 0, '真实root文件边界由默认root的CI guard执行')
    def test_root_owned_real_files_and_failed_write_rollback(self):
        # A root-owned home parent avoids /tmp's intentionally untrusted 1777
        # ancestor. Every target, lock and backup remains inside this scratch.
        with tempfile.TemporaryDirectory(prefix='harbor-runtime-install-', dir='/root') as directory, ExitStack() as stack:
            paths, old, manifest, historical, history_before, args, kwargs = self.fixture_runtime(directory, stack)
            stack.enter_context(patch.object(i, 'read', REAL_READ))
            stack.enter_context(patch.object(i, 'parents', REAL_PARENTS))
            before = {p:p.read_bytes() for p in [i.WRAPPER,i.COMPOSE,i.UPGRADE,i.HELPER,i.MANIFEST,i.TICKET]}
            helper_inode=i.HELPER.stat().st_ino
            write=i.write;failed=False
            def fault(path, raw, mode):
                nonlocal failed
                write(path,raw,mode)
                if path==i.UPGRADE and not failed:
                    failed=True;raise OSError('synthetic root write failure')
            with patch.object(i,'write',side_effect=fault),self.assertRaises(OSError):i.install(*args,**kwargs)
            for p,raw in before.items():self.assertEqual(p.read_bytes(),raw)
            self.assertTrue(i.install(*args,**kwargs)['installed'])
            self.assertEqual(i.HELPER.stat().st_ino,helper_inode)
            for p in [i.WRAPPER,i.COMPOSE,i.UPGRADE,i.HELPER,i.MANIFEST,i.TICKET]:
                self.assertEqual(p.stat().st_uid,0)
                self.assertFalse(p.stat().st_mode&0o022)
            self.assertTrue(i.rollback(args[1],runtime=True)['rolledBack'])
            for p,raw in before.items():self.assertEqual(p.read_bytes(),raw)
            self.assertEqual(historical.read_bytes(),history_before)


if __name__ == '__main__':unittest.main()
