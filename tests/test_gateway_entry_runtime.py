import copy
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value
runtime=load('gateway_entry_runtime',ROOT/'ci/gateway-entry-runtime.py')
controller=load('gateway_entry_controller',ROOT/'ci/gateway-entry-controller.py')

def contract():return {'version':1,'containerId':'e'*64,'imageId':'sha256:'+'a'*64,'memoryBytes':536870912,'pidsLimit':128}
def entry():
 policy=contract()
 return {'Id':policy['containerId'],'Image':policy['imageId'],'Config':{'Image':policy['imageId'],'Labels':{'com.aistrong.project':'harbor-entry'},'User':'0:0','Healthcheck':{'Test':['NONE']},'Entrypoint':['/usr/local/bin/node'],'Cmd':['/app/gateway-entry-process.mjs']},'HostConfig':{'NetworkMode':'host','RestartPolicy':{'Name':'no'},'ReadonlyRootfs':True,'CapDrop':['ALL'],'UsernsMode':'host','SecurityOpt':['no-new-privileges:true'],'Privileged':False,'CapAdd':None,'Memory':policy['memoryBytes'],'PidsLimit':policy['pidsLimit']},'Mounts':[{'Destination':d,'Source':s,'RW':rw} for d,s,rw in [('/etc/harbor-entry','/etc/harbor-entry',False),('/run/harbor-entry','/run/harbor-entry',True),('/app','/usr/local/lib/harbor/entry-app',False)]]}

class EntryRuntimeTests(unittest.TestCase):
 def test_reused_entry_rejects_privilege_or_resource_changes_without_mutation(self):
  runtime.verify_entry(entry(),contract())
  for field,value in [('Privileged',True),('Privileged',None),('CapAdd',['SYS_ADMIN']),('Memory',0),('Memory',1073741824),('PidsLimit',0),('PidsLimit',129),('PidsLimit',True),('SecurityOpt',['no-new-privileges:true','seccomp=unconfined'])]:
   row=entry();row['HostConfig'][field]=value;before=copy.deepcopy(row)
   with self.subTest(field=field,value=value),self.assertRaises(RuntimeError):runtime.verify_entry(row,contract())
   self.assertEqual(row,before)
  for key in ['Id','Image']:
   row=entry();row[key]='f'*64
   with self.subTest(key=key),self.assertRaises(RuntimeError):runtime.verify_entry(row,contract())

 def test_container_contract_requires_exact_server_binding(self):
  runtime.validate_container_contract(contract())
  for field,value in [('version',True),('containerId',None),('imageId','tag:latest'),('memoryBytes',True),('pidsLimit',0),('pidsLimit',2**31),('extra','forged')]:
   value_map=contract();value_map[field]=value
   with self.subTest(field=field),self.assertRaises(RuntimeError):runtime.validate_container_contract(value_map)

 def test_missing_or_changed_actual_guard_refuses_before_entry_start(self):
  config={'entryPort':18092,'bypassProcesses':[]};state={'phase':'guarded','guardFingerprint':controller.guard_fingerprint(config),'rulesDigest':'expected'}
  class Boundary:
   PROFILE=Path('/fixed/profile');STATE=Path('/fixed/state')
   validate=staticmethod(lambda value:value)
   guard_fingerprint=staticmethod(controller.guard_fingerprint)
   fingerprint=staticmethod(controller.fingerprint)
   @staticmethod
   def private_json(path):return config if path==Boundary.PROFILE else state
   @staticmethod
   def state_rules_digest():return 'different'
   @staticmethod
   def assert_start():raise AssertionError('entry start reached without a real guard')
  with self.assertRaisesRegex(RuntimeError,'actual_guard_changed'):runtime.entry_pre(Boundary)
  state['phase']='prepared';state['configDigest']=controller.fingerprint(config)
  with patch.object(Boundary,'verify_resources',side_effect=RuntimeError('rules absent'),create=True),self.assertRaisesRegex(RuntimeError,'rules absent'):runtime.entry_pre(Boundary)

 def test_candidate_waits_inside_one_start_job_for_observed_readiness(self):
  class Boundary:
   calls=[]
   @staticmethod
   def slot_start(slot):
    Boundary.calls.append(slot)
    if len(Boundary.calls)<3:raise RuntimeError('entry_controller_active_database_not_ready')
    return {'slot':slot}
  with patch.object(runtime,'verify_start_guard') as guard,patch.object(runtime.time,'sleep'):
   self.assertEqual(runtime.candidate_pre(Boundary),{'slot':'candidate'})
  self.assertEqual(Boundary.calls,['candidate']*3);self.assertEqual(guard.call_count,2)

 def test_candidate_identity_or_authorization_mismatch_is_not_a_readiness_retry(self):
  for code in ['entry_controller_candidate_image_changed','entry_controller_candidate_activation_changed','entry_controller_active_database_authorization_changed','entry_controller_active_database_status_denied','entry_controller_command_failed_node']:
   class Boundary:
    @staticmethod
    def slot_start(slot):raise RuntimeError(code)
   with self.subTest(code=code),patch.object(Boundary,'slot_start',side_effect=RuntimeError(code)) as start,patch.object(runtime.time,'sleep') as sleep:
    with self.assertRaisesRegex(RuntimeError,code):runtime.candidate_pre(Boundary)
    start.assert_called_once_with('candidate');sleep.assert_not_called()

 def test_candidate_readiness_has_a_deadline(self):
  clock=[0.0]
  class Boundary:
   @staticmethod
   def slot_start(slot):raise RuntimeError('entry_controller_candidate_not_running')
  with patch.object(runtime,'verify_start_guard'),patch.object(runtime.time,'monotonic',side_effect=lambda:clock[0]),patch.object(runtime.time,'sleep',side_effect=lambda seconds:clock.__setitem__(0,clock[0]+runtime.CANDIDATE_WAIT_SECONDS+1)):
   with self.assertRaisesRegex(RuntimeError,'candidate_readiness_timeout'):runtime.candidate_pre(Boundary)

 def test_temporary_authorization_read_failure_does_not_delete_boot_guard(self):
  class Boundary:
   ACTIVE=type('Active',(),{'exists':lambda _:True})()
   STATE=Path('/fixed/state')
   @staticmethod
   def private_json(path):return {'phase':'boot-guarded'}
   @staticmethod
   def refresh_active():return {'phase':'active-prepared'}
  inventory=type('Inventory',(),{'container_holds_resources':staticmethod(lambda row:False)})()
  with patch.object(runtime,'verify_start_guard') as guard,patch.object(runtime,'inspect_entry',return_value={}),patch.object(runtime,'trusted'),patch.object(runtime,'module',return_value=inventory),patch.object(runtime.time,'sleep'),patch.object(Boundary,'refresh_active',side_effect=[RuntimeError('entry_controller_active_database_not_ready'),{'phase':'active-prepared'}]) as refresh:
   result=runtime.candidate_post(Boundary)
  self.assertEqual(result['phase'],'active-prepared');self.assertFalse(result['sourceStarted']);self.assertEqual(refresh.call_count,2);self.assertGreaterEqual(guard.call_count,3)

 def test_persistent_units_require_runtime_before_consumers(self):
  names=['harbor-entry-runtime','harbor-entry-deploy','harbor-entry-legacy','harbor-entry-candidate','harbor-gateway-entry']
  for name in names:
   unit=(ROOT/'ci'/(name+'.service.example')).read_text();self.assertIn('WantedBy=multi-user.target',unit)
   if name!='harbor-entry-runtime':self.assertIn('harbor-entry-runtime.service',unit)
  self.assertIn('RemainAfterExit=yes',(ROOT/'ci/harbor-entry-runtime.service.example').read_text())
  self.assertIn('entry-runtime.py entry-pre',(ROOT/'ci/harbor-gateway-entry.service.example').read_text())
  self.assertIn('entry-runtime.py candidate-post',(ROOT/'ci/harbor-entry-candidate.service.example').read_text())
  candidate=(ROOT/'ci/harbor-entry-candidate.service.example').read_text();self.assertIn('entry-runtime.py candidate-pre',candidate)
  timeout=int(next(line.split('=',1)[1] for line in candidate.splitlines() if line.startswith('TimeoutStartSec=')));self.assertGreaterEqual(timeout,2*runtime.CANDIDATE_WAIT_SECONDS+5)

if __name__=='__main__':unittest.main()
