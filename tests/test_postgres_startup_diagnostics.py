import copy
import importlib.util
from pathlib import Path
import subprocess
import types
import unittest
from unittest.mock import Mock,patch
ROOT=Path(__file__).parents[1]
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,ROOT/path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
pg=load('startup_pg','ci/postgres-deploy.py');upgrade=load('startup_upgrade','ci/postgres-upgrade-deploy.py')
class StartupDiagnosticTests(unittest.TestCase):
 def check(self,*,health='healthy',http=None,device=None,progress=None,inspection=None):
  self.now=0
  row={'Image':'sha256:'+'a'*64,'Config':{'Image':'candidate'},'State':{'Running':True,'Health':{'Status':health}}}
  self.api={'STACK':'/private/materials','NAME':'harbor','http_code':http or Mock(side_effect=lambda port,path,headers=None:403 if headers else 401 if path=='/api/admin/state' else 200),'docker':progress or Mock(return_value=types.SimpleNamespace(returncode=0))}
  self.device=device or Mock(side_effect=lambda stack,config,path,headers=None:403 if headers else 401 if path=='/api/admin/state' else 200)
  def sleep(seconds):self.now+=seconds
  with patch.object(pg.time,'monotonic',side_effect=lambda:self.now),patch.object(pg.time,'sleep',side_effect=sleep),patch.object(pg,'container_inspection',inspection or Mock(return_value=row)),patch.object(pg,'device_http_code',self.device):
   return pg.verify_started_composition(self.api,'candidate',{'port':8092},123,{'configDigest':row['Image']})
 def failure(self,**kwargs):
  with self.assertRaises(Exception) as caught:self.check(**kwargs)
  self.error=caught.exception;self.observed=self.error.startup_observation
  self.assertEqual(upgrade.deployment_failure(self.error)['startup'],self.observed)
  return self.observed
 def test_success_preserves_all_gates_and_full_progress_command(self):
  self.check();self.api['docker'].assert_called_once_with('exec','harbor-worker','node','/app/ci/postgres-preflight.mjs','--progress-since','123',check=False,timeout=10)
  self.assertEqual(self.api['http_code'].call_count,4);self.assertEqual(self.device.call_count,3)
 def test_unhealthy_worker_preserves_budget_and_skips_http_progress(self):
  result=self.failure(health='unhealthy');self.assertEqual(result['attempts'],45);self.assertEqual(result['elapsedMs'],90000)
  self.assertFalse(result['checks']['progress']['attempted']);self.api['http_code'].assert_not_called();self.api['docker'].assert_not_called()
 def test_device_readiness_identified_without_progress_call(self):
  result=self.failure(device=Mock(return_value=503));self.assertEqual(result['checks']['device_ready_http']['status'],503);self.assertFalse(result['checks']['progress']['attempted'])
 def test_progress_nonzero_keeps_only_returncode_not_output(self):
  result=self.failure(progress=Mock(return_value=types.SimpleNamespace(returncode=1,stdout='secret',stderr='credential')))
  self.assertEqual(result['checks']['progress']['returncode'],1);self.assertNotIn('secret',str(result));self.assertNotIn('credential',str(result))
 def test_timeout_type_and_stage_survive(self):
  result=self.failure(progress=Mock(side_effect=subprocess.TimeoutExpired(['private','key'],10,output=b'secret')))
  self.assertIsInstance(self.error,subprocess.TimeoutExpired);self.assertEqual(result['stage'],'progress');self.assertIsNone(result['checks']['progress']['returncode']);self.assertEqual(result['attempts'],1)
 def test_auth_still_rejected_and_recorded(self):
  result=self.failure(http=Mock(side_effect=lambda port,path,headers=None:200));self.assertEqual(result['checks']['gateway_auth_http']['status'],200);self.assertEqual(result['stage'],'gateway_auth_http')
 def test_previous_attempt_values_are_cleared(self):
  device=Mock(side_effect=[200]+[503]*44)
  progress=Mock(return_value=types.SimpleNamespace(returncode=1))
  result=self.failure(device=device,progress=progress);self.assertFalse(result['checks']['progress']['attempted']);self.assertIsNone(result['checks']['progress']['returncode']);progress.assert_called_once()
 def test_completed_inspection_is_kept_when_next_inspection_times_out(self):
  row={'Image':'sha256:'+'a'*64,'Config':{'Image':'candidate'},'State':{'Running':True,'Health':{'Status':'healthy'}}}
  result=self.failure(inspection=Mock(side_effect=[row,subprocess.TimeoutExpired(['private'],10)]))
  self.assertEqual(result['stage'],'worker_inspection');self.assertTrue(result['containers']['gateway']['imageMatches']);self.assertNotIn('worker',result['containers'])
 def test_elapsed_time_records_real_operation_duration(self):
  def progress(*args,**kwargs):self.now+=2.5;return types.SimpleNamespace(returncode=1)
  result=self.failure(progress=Mock(side_effect=progress))
  self.assertEqual(result['checks']['progress']['elapsedMs'],2500);self.assertGreaterEqual(result['elapsedMs'],90000)
 def test_sanitizer_rejects_unknown_fields_and_invalid_scalars(self):
  result=self.failure(health='starting')
  for key,value in [('address','private'),('stage',[]),('attempts',True)]:
   bad=copy.deepcopy(result);bad[key]=value;self.assertIsNone(upgrade.startup_failure_observation(bad))
  for changes in [{'log':'secret'},{'returncode':True},{'elapsedMs':-1}]:
   bad=copy.deepcopy(result);bad['checks']['progress'].update(changes);self.assertIsNone(upgrade.startup_failure_observation(bad))
  clean=upgrade.startup_failure_observation(result);clean['checks'].clear();self.assertTrue(result['checks'])
class StartupInspectionRetryTests(unittest.TestCase):
 def setUp(self):
  self.wrapper=load('startup_retry_wrapper','ci/deploy-wrapper.py');self.now=0;self.reads=[]
  self.row={'Id':'synthetic-container','Image':'sha256:'+'a'*64,'Config':{'Image':'candidate'},'State':{'Running':True,'Health':{'Status':'healthy'}}}
  self.api={'STACK':'/private/materials','NAME':'harbor','inspect_container':self.wrapper.inspect_container,'http_code':Mock(side_effect=lambda port,path,headers=None:403 if headers else 401 if path=='/api/admin/state' else 200),'docker':Mock(return_value=types.SimpleNamespace(returncode=0))}
 def run_check(self,read):
  def sleep(seconds):self.now+=seconds
  device=Mock(side_effect=lambda stack,config,path,headers=None:403 if headers else 401 if path=='/api/admin/state' else 200)
  with patch.object(pg.time,'monotonic',side_effect=lambda:self.now),patch.object(pg.time,'sleep',side_effect=sleep),patch.object(self.wrapper,'docker_container_read',side_effect=read),patch.object(pg,'record_shutdown_diagnostic') as diagnostic,patch.object(pg,'device_http_code',device):
   self.diagnostic=diagnostic
   return pg.verify_started_composition(self.api,'candidate',{'port':8092},123,{'configDigest':self.row['Image']})
 def test_gateway_and_worker_retry_only_marked_transient_then_finish_full_checks(self):
  def read(name):
   self.reads.append(name)
   if self.reads.count(name)==1:raise self.wrapper.ContainerInspectionUnavailable('synthetic timeout')
   return 200,self.row
  self.run_check(read)
  self.assertEqual(self.reads,['harbor','harbor','harbor-worker','harbor-worker']);self.assertEqual(self.now,.5);self.diagnostic.assert_not_called();self.api['docker'].assert_called_once()
 def test_persistent_gateway_transient_uses_original_deadline(self):
  def read(name):self.reads.append(name);raise self.wrapper.ContainerInspectionUnavailable('synthetic timeout')
  with self.assertRaises(pg.ShutdownUnconfirmed) as caught:self.run_check(read)
  self.assertEqual(self.now,90);self.assertEqual(caught.exception.deployment_code,'preflight_observation_timeout');self.assertEqual(caught.exception.startup_observation['stage'],'gateway_inspection');self.assertEqual(set(self.reads),{'harbor'});self.api['docker'].assert_not_called();self.diagnostic.assert_called_once()
 def test_worker_reuses_deadline_spent_by_gateway(self):
  def read(name):
   self.reads.append(name)
   if name=='harbor':self.now=89;return 200,self.row
   raise self.wrapper.ContainerInspectionUnavailable('synthetic timeout')
  with self.assertRaises(pg.ShutdownUnconfirmed) as caught:self.run_check(read)
  self.assertEqual(self.now,90);self.assertEqual(self.reads.count('harbor-worker'),4);self.assertEqual(caught.exception.startup_observation['stage'],'worker_inspection');self.assertTrue(caught.exception.startup_observation['containers']['gateway']['imageMatches'])
 def test_identity_and_malformed_errors_reject_immediately(self):
  for value in [RuntimeError('Docker inspection socket changed'),(200,{'Id':'synthetic','Config':'malformed','State':{'Running':True}}),(404,{'message':'unexpected absence'})]:
   with self.subTest(value=type(value).__name__):
    self.now=0;self.reads=[]
    def read(name):
     self.reads.append(name)
     if isinstance(value,Exception):raise value
     return value
    with self.assertRaises(pg.ShutdownUnconfirmed):self.run_check(read)
    self.assertEqual(self.now,0);self.assertEqual(self.reads,['harbor']);self.api['docker'].assert_not_called()
 def test_shutdown_default_inspection_still_fails_once_without_retry(self):
  read=Mock(side_effect=self.wrapper.ContainerInspectionUnavailable('synthetic timeout'));rows=[]
  with patch.object(self.wrapper,'docker_container_read',read),patch.object(pg,'record_shutdown_diagnostic') as diagnostic,patch.object(pg.time,'sleep') as sleep:
   with self.assertRaises(pg.ShutdownUnconfirmed):pg.container_inspection(self.api,'harbor',rows)
  read.assert_called_once();sleep.assert_not_called();diagnostic.assert_called_once();self.assertEqual(rows[0]['reason'],'container_inspection_failed')

if __name__=='__main__':unittest.main()
