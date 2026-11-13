import tempfile,unittest
from unittest.mock import patch
import test_deploy_postgres as base
pg=base.pg
class DepartmentCompositionTests(unittest.TestCase):
 prepare=base.PostgreSQLDeploymentTests.prepare
 api=base.PostgreSQLDeploymentTests.api
 def test_activation_runs_after_source_stops_and_before_new_intake(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);events=[]
   class Department:
    uncertain=False
    def install(this):
     for suffix in ['', '-worker']:
      self.assertNotIn(base.NAME+suffix,state);self.assertFalse(state[base.NAME+suffix+'-previous']['State']['Running'])
     events.append('install')
    def rollback_guard(this):self.fail('healthy candidate must not roll back')
   api['department_release']=Department();docker=api['docker']
   def controlled(*args,**kwargs):
    if args[:2]==('run','-d'):self.assertEqual(events,['install'])
    return docker(*args,**kwargs)
   api['docker']=controlled;pg.deploy(base.IMAGE,base.CONFIG,None,api);self.assertEqual(events,['install'])
 def test_failed_candidate_is_removed_before_department_compatibility_check_and_source_restart(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);events=[]
   class Department:
    uncertain=False
    def install(this):events.append('install')
    def rollback_guard(this):
     for suffix in ['', '-worker']:self.assertNotIn(base.NAME+suffix,state)
     events.append('guard')
   api['department_release']=Department();docker=api['docker']
   def controlled(*args,**kwargs):
    if args[0]=='start':self.assertEqual(events,['install','guard'])
    return docker(*args,**kwargs)
   api['docker']=controlled
   with patch.object(pg,'verify_started_composition',side_effect=RuntimeError('synthetic failed candidate')),self.assertRaises(RuntimeError):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertEqual(events,['install','guard'])
 def test_unknown_activation_does_not_start_candidate_or_restore_old_source(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory)
   class Department:
    uncertain=True
    def install(this):raise RuntimeError('synthetic commit acknowledgement lost')
    def rollback_guard(this):self.fail('unknown activation must retain stopped source')
   api['department_release']=Department()
   with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertFalse(any(call[0]=='start' or call[:2]==('run','-d') for call in calls))
 def test_incompatible_rollback_never_starts_old_source(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory)
   class Department:
    uncertain=False
    def install(this):pass
    def rollback_guard(this):raise RuntimeError('synthetic incompatible source')
   api['department_release']=Department()
   with patch.object(pg,'verify_started_composition',side_effect=RuntimeError('synthetic failed candidate')),self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertFalse(any(call[0]=='start' for call in calls))
if __name__=='__main__':unittest.main()
