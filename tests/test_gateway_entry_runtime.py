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

 def test_first_committed_restart_uses_active_pg_identity_without_retired_source(self):
  config={'source':{'containerId':'old'}};candidate={'containerId':'new'}
  state={'phase':'activated','configDigest':controller.fingerprint(config),'candidate':candidate}
  class Boundary:
   PROFILE=Path('/fixed/profile');STATE=Path('/fixed/state')
   validate=staticmethod(lambda value:value)
   fingerprint=staticmethod(controller.fingerprint)
   @staticmethod
   def private_json(path):return config if path==Boundary.PROFILE else state
  with patch.object(Boundary,'active_record',return_value=({'candidate':candidate},config,{}),create=True) as active,patch.object(Boundary,'verify_source',create=True) as fence,patch.object(Boundary,'verify_resources',create=True) as resources:
   runtime.verify_start_guard(Boundary)
   active.assert_called_once();fence.assert_not_called();resources.assert_called_once_with(state,config,fenced=True)
   resources.reset_mock();fence.side_effect=AssertionError('retired source must not be queried')
   runtime.verify_start_guard(Boundary);resources.assert_called_once_with(state,config,fenced=True);resources.reset_mock()
   active.return_value=({'candidate':{'containerId':'foreign'}},config,{})
   with self.assertRaisesRegex(RuntimeError,'active_guard_binding'):runtime.verify_start_guard(Boundary)
   resources.assert_not_called()
   active.side_effect=RuntimeError('active_database_authorization_changed')
   with self.assertRaisesRegex(RuntimeError,'active_database_authorization_changed'):runtime.verify_start_guard(Boundary)
   resources.assert_not_called()

 def test_repeated_committed_start_checks_runtime_candidate_and_auth_each_time(self):
  config={'source':{'containerId':'old'}};candidate={'containerId':'new','imageId':'pg-image','address':'10.0.0.2'};view={'source':dict(candidate)}
  state={'phase':'active-prepared','configDigest':controller.fingerprint(config),'candidate':candidate,'activeRuntimeConfig':view}
  class Boundary:
   PROFILE=Path('/fixed/profile');STATE=Path('/fixed/state')
   validate=staticmethod(lambda value:value);fingerprint=staticmethod(controller.fingerprint)
   private_json=staticmethod(lambda path:config if path==Boundary.PROFILE else state)
  with patch.object(Boundary,'active_record',return_value=({'candidate':candidate},config,{}),create=True) as active,patch.object(Boundary,'verify_resources',create=True) as resources:
   for phase in ['active-prepared','activated']:
    state['phase']=phase;runtime.verify_start_guard(Boundary)
   self.assertEqual(active.call_count,2);self.assertEqual(resources.call_count,2)
   view['source']['containerId']='old';resources.reset_mock()
   with self.assertRaisesRegex(RuntimeError,'active_runtime_candidate_binding'):runtime.verify_start_guard(Boundary)
   resources.assert_not_called();view['source']=dict(candidate)
   active.side_effect=RuntimeError('active_database_authorization_changed')
   with self.assertRaisesRegex(RuntimeError,'active_database_authorization_changed'):runtime.verify_start_guard(Boundary)
   resources.assert_not_called()

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

 def test_entry_post_waits_for_real_control_and_listener_before_restore(self):
  class Boundary:
   ACTIVE=type('Active',(),{'exists':lambda _:True})()
   @staticmethod
   def restore_active():return {'phase':'activated','candidateOnly':True}
  with patch.object(runtime,'verify_start_guard') as guard,patch.object(runtime,'entry_listener_ready',side_effect=[False,False,False,True]) as ready,patch.object(runtime.time,'sleep') as sleep,patch.object(Boundary,'restore_active',wraps=Boundary.restore_active) as restore:
   value=runtime.entry_post(Boundary)
  self.assertEqual(value['phase'],'activated');self.assertEqual(ready.call_count,4);self.assertEqual(sleep.call_count,3);restore.assert_called_once();self.assertEqual(guard.call_count,5)

 def test_entry_post_identity_and_authorization_errors_are_not_readiness(self):
  class Boundary:
   ACTIVE=type('Active',(),{'exists':lambda _:True})()
   restore_active=staticmethod(lambda:(_ for _ in ()).throw(AssertionError('restore must not run')))
  with patch.object(runtime,'verify_start_guard'),patch.object(runtime,'entry_listener_ready',side_effect=RuntimeError('entry_runtime_entry_control_identity')),patch.object(runtime.time,'sleep') as sleep:
   with self.assertRaisesRegex(RuntimeError,'entry_control_identity'):runtime.entry_post(Boundary)
   sleep.assert_not_called()
  with patch.object(runtime,'verify_start_guard',side_effect=RuntimeError('entry_controller_active_database_authorization_changed')),patch.object(runtime,'entry_listener_ready') as ready:
   with self.assertRaisesRegex(RuntimeError,'active_database_authorization_changed'):runtime.entry_post(Boundary)
   ready.assert_not_called()

 def test_entry_post_rechecks_authority_before_each_readiness_attempt(self):
  class Boundary:
   ACTIVE=type('Active',(),{'exists':lambda _:True})()
  for code in ['entry_controller_active_database_authorization_changed','entry_runtime_actual_guard_changed']:
   with self.subTest(code=code),patch.object(runtime,'verify_start_guard',side_effect=[None,RuntimeError(code)]),patch.object(runtime,'entry_listener_ready',return_value=False) as ready,patch.object(runtime.time,'sleep') as sleep:
    with self.assertRaisesRegex(RuntimeError,code):runtime.entry_post(Boundary)
    self.assertEqual(ready.call_count,1);self.assertEqual(sleep.call_count,1)

 def test_entry_listener_only_retries_uncreated_or_refused_control(self):
  class Boundary:
   PROFILE=Path('/fixed/profile');ENTRY_CONFIG=Path('/fixed/process');CONTROL='/fixed/control'
   validate=staticmethod(lambda value:value)
   private_json=staticmethod(lambda path:{'entryPort':18093,'entryCgroup':'/fixed/group'} if path==Boundary.PROFILE else {'listenPort':18093})
   socket_owner=staticmethod(lambda path:1)
   process_cgroup=staticmethod(lambda pid:'/fixed/group')
   entry_state=staticmethod(lambda:{'slot':'candidate','phase':'active'})
  for error in [FileNotFoundError(),ConnectionRefusedError()]:
   with self.subTest(error=type(error).__name__),patch.object(Boundary,'socket_owner',side_effect=error):self.assertFalse(runtime.entry_listener_ready(Boundary))
  with patch.object(Boundary,'private_json',side_effect=FileNotFoundError('missing configuration')),patch.object(Boundary,'socket_owner') as owner:
   with self.assertRaisesRegex(FileNotFoundError,'missing configuration'):runtime.entry_listener_ready(Boundary)
   owner.assert_not_called()
  with patch.object(Boundary,'process_cgroup',side_effect=FileNotFoundError('identified owner disappeared')):
   with self.assertRaisesRegex(FileNotFoundError,'identified owner disappeared'):runtime.entry_listener_ready(Boundary)
  with patch.object(runtime.Path,'iterdir',side_effect=FileNotFoundError('identified owner fd disappeared')):
   with self.assertRaisesRegex(FileNotFoundError,'identified owner fd disappeared'):runtime.entry_listener_ready(Boundary)

 def test_entry_post_readiness_has_a_bounded_deadline(self):
  class Boundary:
   ACTIVE=type('Active',(),{'exists':lambda _:True})()
  clock=[0.0]
  with patch.object(runtime,'verify_start_guard'),patch.object(runtime,'entry_listener_ready',return_value=False),patch.object(runtime.time,'monotonic',side_effect=lambda:clock[0]),patch.object(runtime.time,'sleep',side_effect=lambda _:clock.__setitem__(0,runtime.CANDIDATE_WAIT_SECONDS+1)):
   with self.assertRaisesRegex(RuntimeError,'entry_readiness_timeout'):runtime.entry_post(Boundary)

 def test_persistent_units_require_runtime_before_consumers(self):
  names=['harbor-entry-runtime','harbor-entry-deploy','harbor-entry-legacy','harbor-entry-candidate','harbor-gateway-entry']
  for name in names:
   unit=(ROOT/'ci'/(name+'.service.example')).read_text();self.assertIn('WantedBy=multi-user.target',unit)
   if name!='harbor-entry-runtime':self.assertIn('harbor-entry-runtime.service',unit)
  self.assertIn('RemainAfterExit=yes',(ROOT/'ci/harbor-entry-runtime.service.example').read_text())
  self.assertIn('entry-runtime.py entry-pre',(ROOT/'ci/harbor-gateway-entry.service.example').read_text())
  entry_unit=(ROOT/'ci/harbor-gateway-entry.service.example').read_text();self.assertIn('entry-runtime.py entry-post',entry_unit);self.assertNotIn('gateway-entry-controller.py restore-active',entry_unit)
  self.assertGreaterEqual(int(next(line.split('=',1)[1] for line in entry_unit.splitlines() if line.startswith('TimeoutStartSec='))),2*runtime.CANDIDATE_WAIT_SECONDS+5)
  self.assertIn('entry-runtime.py candidate-post',(ROOT/'ci/harbor-entry-candidate.service.example').read_text())
  candidate=(ROOT/'ci/harbor-entry-candidate.service.example').read_text();self.assertIn('entry-runtime.py candidate-pre',candidate)
  timeout=int(next(line.split('=',1)[1] for line in candidate.splitlines() if line.startswith('TimeoutStartSec=')));self.assertGreaterEqual(timeout,2*runtime.CANDIDATE_WAIT_SECONDS+5)

if __name__=='__main__':unittest.main()
