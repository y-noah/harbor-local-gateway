import importlib.util,unittest,stat,ctypes,resource
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
from contextlib import ExitStack

spec=importlib.util.spec_from_file_location('upgrade',Path(__file__).parents[1]/'ci/postgres-upgrade-deploy.py');u=importlib.util.module_from_spec(spec);spec.loader.exec_module(u)
class ResidencyTests(unittest.TestCase):
 def test_default_has_no_file_or_memory_side_effects(self):
  with patch.object(u.os,'open') as opened,patch('ctypes.CDLL') as libc:
   with u.metadata_residency({}):pass
   opened.assert_not_called();libc.assert_not_called()
 def test_non_boolean_rejected(self):
  for value in [1,'true',None,{}]:
   with self.assertRaises(Exception):
    with u.metadata_residency({'postgres_metadata_residency':value}):pass
 def scenario(self,size=4096,lock_result=0,body_error=False):
  paths=[Mock(parents=[]),Mock(parents=[])];lib=Mock();lib.mmap.side_effect=[1234,5678];lib.mlock.return_value=lock_result
  info=SimpleNamespace(st_mode=stat.S_IFREG|0o600,st_uid=0,st_nlink=1,st_size=size)
  with ExitStack() as stack:
   stack.enter_context(patch.object(u,'METADATA_FILES',paths));stack.enter_context(patch.object(u.os,'open',side_effect=[11,12]));stack.enter_context(patch.object(u.os,'fstat',return_value=info));closed=stack.enter_context(patch.object(u.os,'close'));stack.enter_context(patch('ctypes.CDLL',return_value=lib));stack.enter_context(patch('resource.getrlimit',return_value=(65536,65536)));limit=stack.enter_context(patch('resource.setrlimit'))
   try:
    with u.metadata_residency({'postgres_metadata_residency':True}):
     if body_error:raise RuntimeError('operation failed')
   finally:
    self.assertEqual(closed.call_count,2)
    if size*2<=u.METADATA_MAX_BYTES:self.assertEqual(limit.call_args.args[1],(65536,65536))
  return lib
 def test_success_releases_both_mappings(self):
  lib=self.scenario();self.assertEqual(lib.munmap.call_count,2)
 def test_body_failure_releases_resources(self):
  with self.assertRaisesRegex(RuntimeError,'operation failed'):self.scenario(body_error=True)
 def test_lock_failure_releases_resources_and_refuses_operation(self):
  with self.assertRaises(OSError):self.scenario(lock_result=-1)
 def test_aggregate_memory_budget_is_enforced(self):
  with self.assertRaises(Exception):self.scenario(size=u.METADATA_MAX_BYTES)
