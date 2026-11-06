import copy
import importlib.util
import json
import os
import fcntl
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
recovery=load('source_recovery',ROOT/'ci/gateway-source-recovery.py')
controller=load('entry_controller',ROOT/'ci/gateway-entry-controller.py')
inventory=load('reviewed_container_inventory',ROOT/'ci/storage-recovery/rotate-tools.py')

class RecoveryBoundaryTests(unittest.TestCase):
 def test_any_durable_writer_or_candidate_authorization_blocks_sqlite(self):
  with tempfile.TemporaryDirectory() as directory:
   class Boundary:
    require=staticmethod(controller.require)
    ACTIVE=Path(directory)/'active'
    WRITER_AUTHORIZED=Path(directory)/'writer'
   for path in [Boundary.ACTIVE,Boundary.WRITER_AUTHORIZED]:
    path.touch()
    with self.assertRaises(RuntimeError):recovery.no_writer(Boundary,{})
    path.unlink()
   for field in ['writerAuthorized','candidate','candidateIntent','candidateOperations','activeRuntimeConfig']:
    with self.subTest(field=field),self.assertRaises(RuntimeError):recovery.no_writer(Boundary,{field:True})
   recovery.no_writer(Boundary,{'deploymentBinding':{'pending':'preconsume'}})

 def test_full_inventory_rejects_unlabelled_writer_and_historical_candidate(self):
  class Boundary:
   require=staticmethod(controller.require)
   rows=[]
   @classmethod
   def local_container_inventory(cls):return cls.rows
  for row in [{'Id':'other','Name':'/harbor-local-gateway-worker','Config':{},'State':{'Running':False}}, {'Id':'other','Name':'/removed-candidate','Config':{'Labels':{'com.aistrong.project':'harbor-local-gateway','com.harbor.activation-deployment-id':'old'}},'State':{'Running':False}}]:
   Boundary.rows=[row]
   with patch.object(recovery,'inventory_module',return_value=inventory),self.assertRaises(Exception):recovery.no_candidates(Boundary,{'containerId':'source'})

 def test_nonrunning_other_project_container_requires_positive_resource_release(self):
  class Boundary:
   require=staticmethod(controller.require)
   rows=[]
   @classmethod
   def local_container_inventory(cls):return cls.rows
  base={'Status':'exited','Pid':0,'Running':False,'Paused':False,'Restarting':False,'Dead':False}
  with patch.object(recovery,'inventory_module',return_value=inventory):
   for state in [base,{**base,'Status':'created'}]:
    Boundary.rows=[{'Id':'other','Name':'/archive','Config':{'Labels':{'com.aistrong.project':'harbor-local-gateway'}},'State':state}];recovery.no_candidates(Boundary,{'containerId':'source'})
   for state in [{**base,'Status':'removing'},{**base,'Status':'dead'},{**base,'Pid':123},{**base,'Dead':True},{**base,'Paused':True},{**base,'Restarting':True},{**base,'Status':'unknown'},{**base,'Pid':None},{key:value for key,value in base.items() if key!='Running'}]:
    Boundary.rows=[{'Id':'other','Name':'/archive','Config':{'Labels':{'com.aistrong.project':'harbor-local-gateway'}},'State':state}]
    with self.subTest(state=state),self.assertRaises(Exception):recovery.no_candidates(Boundary,{'containerId':'source'})

 def test_proof_requires_actual_fence_source_mount_roles_and_preconsume(self):
  class Boundary:require=staticmethod(controller.require)
  source={'containerId':'a'*64,'imageId':'sha256:'+'b'*64,'startedAt':'old-generation'}
  row={'State':{'FinishedAt':'finished'},'Mounts':[{'Type':'bind','Source':'/tmp','Destination':'/app/data','RW':True}]}
  proof={'contract':'harbor-source-recovery-v1','runId':'12345678-1234-4234-8234-123456789abc','configurationDigest':'a'*64,'sourceFence':{'scope':'docker-systemd','sourceContainerId':source['containerId'],'sourceImageId':source['imageId'],'sourceStartedAt':source['startedAt'],'sourceFinishedAt':'finished','sourceStopped':True,'supervisorFenced':True,'productionVerified':True},'sourceDataDir':'/tmp','supervisorKind':'docker-restart','activationState':'pending','deploymentActivationAbsent':True,'rolesNoLogin':True,'roleStates':{'migration':'NOLOGIN','owner':'NOLOGIN','runtime':'NOLOGIN'},'targetSessionCount':0,'sourceVerified':True,'sourceVerificationMode':'authenticated-captured-source-match','registryVerified':True,'inventoryVerified':True}
  recovery.validate_proof(Boundary,proof,{'source':source},row)
  baseline=copy.deepcopy(proof);baseline['activationState']='not-created';baseline['roleStates']={key:'not-created' for key in ['migration','owner','runtime']};recovery.validate_proof(Boundary,baseline,{'source':source},row)
  for field,value in [('contract','harbor-source-recovery-diagnostic'),('supervisorKind','systemd'),('activationState','active'),('deploymentActivationAbsent',False),('targetSessionCount',1),('sourceDataDir','/other'),('registryVerified',False),('sourceVerified',False),('roleStates',{'migration':'NOLOGIN','owner':'NOLOGIN','runtime':'LOGIN'})]:
   changed=copy.deepcopy(proof);changed[field]=value
   with self.subTest(field=field),self.assertRaises(RuntimeError):recovery.validate_proof(Boundary,changed,{'source':source},row)
  proof['sourceFence']['sourceStartedAt']='another-generation'
  with self.assertRaises(RuntimeError):recovery.validate_proof(Boundary,proof,{'source':source},row)


class RunningSourceRecoveryTests(unittest.TestCase):
 def fixture(self,root):
  class Boundary:
   require=staticmethod(controller.require)
   fingerprint=staticmethod(controller.fingerprint)
   validate=staticmethod(lambda config:config)
  c=Boundary();c.ACTIVE=root/'active';c.WRITER_AUTHORIZED=root/'writer';c.PROFILE=root/'controller.json';c.STATE=root/'state.json';c.config={'source':{'containerId':'a'*64,'imageId':'sha256:'+'b'*64,'pid':123,'startTicks':'456','startedAt':'original','address':'172.18.0.2'}};c.row={'Id':'a'*64,'Image':'sha256:'+'b'*64,'State':{'Running':True,'Pid':123,'StartedAt':'original','Dead':False,'OOMKilled':False,'Error':''},'HostConfig':{'RestartPolicy':{'Name':'no','MaximumRetryCount':0}},'Mounts':[{'Source':'original','Destination':'/app/data'}]};c.entry={'slot':'legacy','phase':'holding','inflight':2};c.calls=[]
  c.private_json=lambda path:json.loads(Path(path).read_text())
  def save(path,value):Path(path).write_text(json.dumps(value))
  c.save_json=save;c.save_state=lambda value:save(c.STATE,value);save(c.PROFILE,c.config);save(c.STATE,{'phase':'holding','configDigest':c.fingerprint(c.config),'sourceRestartPolicy':{'Name':'unless-stopped','MaximumRetryCount':0}})
  def source(config):
   c.require(c.row['Id']==config['source']['containerId'] and c.row['Image']==config['source']['imageId'] and c.row['State']['Pid']==config['source']['pid'] and c.row['State']['StartedAt']==config['source']['startedAt'],'source_generation_changed');return copy.deepcopy(c.row)
  c.verify_source=source;c.verify_environment=lambda config:c.calls.append('verify-network');c.verify_resources=lambda state,config:c.calls.append('verify-rules');c.entry_state=lambda:copy.deepcopy(c.entry)
  def execute(args):
   c.calls.append(args);c.require(args[:2]==['/usr/bin/docker','update'],'unexpected_command');c.row['HostConfig']['RestartPolicy']={'Name':'unless-stopped','MaximumRetryCount':0};return ''
  c.execute=execute
  def action(config,action):
   c.calls.append(action);state=c.private_json(c.STATE)
   if action=='withdraw':state['phase']='withdrawn';c.entry['phase']='active'
   elif action=='arm':state['phase']='routed'
   else:raise RuntimeError('unexpected_action')
   c.save_state(state)
  c.run_action=action;return c
 def test_running_original_restores_entry_policy_and_retires_run_without_node(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);c=self.fixture(root);operator=root/'operator.json';operator.write_text(json.dumps({'runDirectory':str(root/'run')}));journal=root/'recovery.json'
   with patch.object(recovery,'verify_original_source_health'),patch.object(recovery,'OPERATOR_PROFILE',operator),patch.object(recovery,'JOURNAL',journal),patch.object(recovery,'no_candidates'):
    result=recovery.recover_running_source(c,c.config,c.private_json(c.STATE))
   self.assertTrue(result['freshRunRequired']);self.assertFalse(result['sourceRestarted']);self.assertFalse(operator.exists());self.assertEqual(c.entry['phase'],'active');self.assertEqual(c.private_json(c.STATE)['phase'],'routed');self.assertTrue(c.private_json(c.STATE)['recoveryComplete']);self.assertEqual([x for x in c.calls if x in ['withdraw','arm']],['withdraw','arm']);self.assertEqual(c.row['HostConfig']['RestartPolicy']['Name'],'unless-stopped')
 def test_hold_interrupted_before_state_save_can_restore_original(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);c=self.fixture(root);state=c.private_json(c.STATE);state['phase']='routed';c.save_state(state);operator=root/'operator.json';operator.write_text(json.dumps({'runDirectory':str(root/'run')}))
   with patch.object(recovery,'verify_original_source_health'),patch.object(recovery,'OPERATOR_PROFILE',operator),patch.object(recovery,'JOURNAL',root/'recovery.json'),patch.object(recovery,'no_candidates'):
    result=recovery.recover_running_source(c,c.config,c.private_json(c.STATE))
   self.assertTrue(result['freshRunRequired']);self.assertEqual(c.entry['phase'],'active')
 def test_writer_marker_refuses_running_branch(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);c=self.fixture(root);c.WRITER_AUTHORIZED.touch()
   with patch.object(recovery,'verify_original_source_health'),patch.object(recovery,'no_candidates'),self.assertRaises(RuntimeError):recovery.recover_running_source(c,c.config,c.private_json(c.STATE))
   self.assertEqual(c.calls,[])
 def test_changed_original_generation_refused_before_any_mutation(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);c=self.fixture(root);c.row['State']['Pid']=124
   with patch.object(recovery,'verify_original_source_health'),patch.object(recovery,'no_candidates'),self.assertRaises(RuntimeError):recovery.recover_running_source(c,c.config,c.private_json(c.STATE))
   self.assertEqual(c.calls,[])
 def test_candidate_entry_cannot_be_resumed_as_original(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);c=self.fixture(root);c.entry['slot']='candidate'
   with patch.object(recovery,'verify_original_source_health'),patch.object(recovery,'no_candidates'),self.assertRaises(RuntimeError):recovery.recover_running_source(c,c.config,c.private_json(c.STATE))
   self.assertNotIn('withdraw',c.calls)
 def test_retry_after_profile_retirement_finishes_same_original(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);c=self.fixture(root);operator=root/'operator.json';operator.write_text(json.dumps({'runDirectory':str(root/'run')}));journal=root/'recovery.json';original_action=c.run_action
   def interrupted(config,action):
    if action=='arm':raise RuntimeError('simulated_disconnect')
    return original_action(config,action)
   with patch.object(recovery,'verify_original_source_health'),patch.object(recovery,'OPERATOR_PROFILE',operator),patch.object(recovery,'JOURNAL',journal),patch.object(recovery,'no_candidates'):
    c.run_action=interrupted
    with self.assertRaises(RuntimeError):recovery.recover_running_source(c,c.config,c.private_json(c.STATE))
    self.assertFalse(operator.exists());self.assertEqual(c.private_json(c.STATE)['phase'],'withdrawn');c.run_action=original_action;result=recovery.recover_running_source(c,c.config,c.private_json(c.STATE));self.assertTrue(result['freshRunRequired']);self.assertEqual(c.private_json(c.STATE)['phase'],'routed')

class SupervisedRecoveryLockTests(unittest.TestCase):
 def test_inherited_open_file_descriptions_leave_supervisor_locks_held(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);paths=[root/'deploy.lock',root/'controller-state.lock'];original=[];inherited=[]
   class Boundary:
    STATE=root/'controller-state.json'
    require=staticmethod(controller.require)
    trusted_parents=staticmethod(lambda path:None)
   real_fstat=os.fstat
   def root_metadata(fd):
    s=real_fstat(fd);return os.stat_result((s.st_mode,s.st_ino,s.st_dev,s.st_nlink,0,s.st_gid,s.st_size,s.st_atime,s.st_mtime,s.st_ctime))
   for path in paths:
    fd=os.open(path,os.O_CREAT|os.O_RDWR,0o600);fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);original.append(fd);inherited.append(os.dup(fd))
   try:
    with patch.object(recovery,'DEPLOY_LOCK',paths[0]),patch.object(recovery,'load_controller',return_value=Boundary()),patch.object(recovery.os,'geteuid',return_value=0),patch.object(recovery.os,'fstat',side_effect=root_metadata),patch.object(recovery,'recover',return_value={'phase':'recovery-complete','freshRunRequired':True}),patch('sys.stdout',new_callable=io.StringIO):recovery.main(supervised_locks=tuple(inherited))
    for path in paths:
     probe=os.open(path,os.O_RDWR)
     try:
      with self.assertRaises(BlockingIOError):fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB)
     finally:os.close(probe)
   finally:
    for fd in original:os.close(fd)

if __name__=='__main__':unittest.main()
