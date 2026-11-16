"""Registered preflights reconcile Docker ACKs without attaching to stdout."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock,patch
ROOT=Path(__file__).parents[1]
def module(name,path):
 spec=importlib.util.spec_from_file_location(name,ROOT/path);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value
boundary=module('registered_boundary','tests/test_deploy_postgres.py');pg=boundary.pg
IMAGE=boundary.IMAGE;NAME=boundary.NAME;PROOF=boundary.PROOF

class RegisteredPreflightTests(unittest.TestCase):
 def fixture(self,directory):
  boundary.PostgreSQLDeploymentTests.prepare(self,directory)
  root=Path(directory)
  for name in ['secrets/postgres-report.key','secrets/postgres-ca.crt','secrets/device/server.key','secrets/device/server.pem','secrets/device/ca.key','secrets/device/ca.pem','pg-data/key-encryption.key','pg-data/recording-encryption.key']:
   path=root/name;path.parent.mkdir(exist_ok=True,parents=True);path.write_text('synthetic')
  with patch.object(pg,'secure'):common,_=pg.configuration({**boundary.CONFIG,'postgres_network':'private-test'},directory)
  image={'Id':'sha256:'+'a'*64,'Config':{'Env':['PATH=/usr/bin'],'Entrypoint':None,'Labels':{'com.harbor.storage.postgres':pg.CONTRACT,'com.harbor.postgres-preflight':'v1','com.harbor.extra-usage-dispatch':pg.EXTRA_USAGE_DISPATCH_CONTRACT,'org.opencontainers.image.revision':'a'*40}}}
  rows={};calls=[]
  api={'STACK':directory,'NAME':NAME,'preflight_directory':str(root/'registered'),'preflight_read':lambda path:json.loads(path.read_text()),'preflight_save':lambda path,value:path.write_text(json.dumps(value)),'inspect_image':lambda _:image,'inspect_container':lambda name:copy.deepcopy(rows.get(name))}
  environment,mounts,options=pg.preflight_spec(common,image,directory)
  def docker(*args,**kwargs):
   calls.append(args)
   if args[0]=='create':
    name=args[args.index('--name')+1];labels=dict(args[index+1].split('=',1) for index,item in enumerate(args[:-1]) if item=='--label')
    rows[name]={'Id':'id-'+name,'Name':'/'+name,'Image':image['Id'],'Config':{'Image':IMAGE,'Cmd':['node','/app/ci/postgres-preflight.mjs'],'Env':[key+'='+value for key,value in environment.items()],'Entrypoint':None,'User':options['--user'],'Labels':labels},'HostConfig':{'RestartPolicy':{'Name':'no'},'IpcMode':'private','ReadonlyRootfs':True,'NetworkMode':options['--network'],'Tmpfs':{'/tmp':'rw,noexec,nosuid,size=64m'},'CapDrop':['ALL'],'SecurityOpt':[options['--security-opt']]},'Mounts':[{'Type':'bind','Source':source,'Destination':destination,'RW':rw} for destination,(source,rw) in mounts.items()],'State':{'Status':'created','Running':False,'Pid':0}}
    return Mock(stdout=rows[name]['Id'],returncode=0)
   if args[0]=='start':
    self.assertNotIn('--attach',args);rows[args[-1]]['State']={'Status':'exited','Running':False,'ExitCode':0,'Pid':0};return Mock(stdout=args[-1],returncode=0)
   if args[0]=='logs':return Mock(stdout=json.dumps(PROOF),stderr='',returncode=0)
   if args[0]=='rm':self.assertNotIn('-f',args);rows.pop(args[-1]);return Mock(stdout='',returncode=0)
   raise AssertionError(args)
  api['docker']=docker
  return api,common,rows,calls,docker
 def test_fresh_proof_uses_inspected_exit_and_logs_never_attach_or_cached_output(self):
  with tempfile.TemporaryDirectory() as directory:
   api,common,rows,calls,_=self.fixture(directory)
   self.assertEqual(pg.image_proof(api,IMAGE,common),PROOF);self.assertEqual(pg.image_proof(api,IMAGE,common),PROOF)
   self.assertEqual([call[0] for call in calls],['create','start','logs','rm']*2)
   self.assertNotEqual(calls[0][calls[0].index('--name')+1],calls[4][calls[4].index('--name')+1]);self.assertEqual(rows,{})
 def test_start_ack_timeout_after_real_child_completion_reconciles_success(self):
  # The child runs independently of the observer and reaches terminal state
  # after its CLI ACK is lost; this is not a mock-only sequence of states.
  with tempfile.TemporaryDirectory() as directory:
   api,common,rows,calls,original=self.fixture(directory);result=Path(directory)/'child-exit.json';children=[];inspect=api['inspect_container']
   def docker(*args,**kwargs):
    if args[0]=='start':
     calls.append(args);name=args[-1];rows[name]['State']={'Status':'running','Running':True,'Pid':123}
     children.append(subprocess.Popen([sys.executable,'-c',"import json,pathlib,sys,time;time.sleep(.05);pathlib.Path(sys.argv[1]).write_text(json.dumps({'Status':'exited','Running':False,'Pid':0,'ExitCode':0}))",str(result)]))
     raise subprocess.TimeoutExpired(['docker','start'],kwargs['timeout'])
    return original(*args,**kwargs)
   def observe(name):
    if result.exists() and name in rows:rows[name]['State']=json.loads(result.read_text())
    return inspect(name)
   api.update(docker=docker,inspect_container=observe)
   try:self.assertEqual(pg.image_proof(api,IMAGE,common),PROOF)
   finally:
    for child in children:child.wait(timeout=5)
   self.assertEqual(len([call for call in calls if call[0]=='start']),1);self.assertEqual(rows,{})
 def test_transient_inspection_after_start_uses_existing_execution_budget(self):
  with tempfile.TemporaryDirectory() as directory:
   api,common,rows,calls,original=self.fixture(directory);observe=api['inspect_container'];missed=[]
   def inspect(name):
    if rows.get(name,{}).get('State',{}).get('Status')=='exited' and not missed:
     missed.append(name);raise boundary.wrapper.ContainerInspectionUnavailable('synthetic timeout')
    return observe(name)
   api['inspect_container']=inspect
   with patch.object(pg.time,'sleep'):
    self.assertEqual(pg.image_proof(api,IMAGE,common),PROOF)
   self.assertEqual(len(missed),1);self.assertEqual([call[0] for call in calls],['create','start','logs','rm']);self.assertEqual(rows,{})
 def test_inspection_timeout_stops_at_original_budget_and_retains_unknown(self):
  with tempfile.TemporaryDirectory() as directory:
   inspect=Mock(side_effect=boundary.wrapper.ContainerInspectionUnavailable('synthetic timeout'));now=[0]
   with patch.object(pg.time,'monotonic',side_effect=lambda:now[0]),patch.object(pg.time,'sleep',side_effect=lambda delay:now.__setitem__(0,now[0]+delay)) as sleep,self.assertRaises(pg.ShutdownUnconfirmed) as failure:
    pg.container_inspection({'STACK':directory,'inspect_container':inspect},'synthetic',[],retry_deadline=.5)
   self.assertEqual(inspect.call_count,2);self.assertEqual(sleep.call_count,2);self.assertEqual(now[0],.5);self.assertEqual(failure.exception.deployment_code,'preflight_observation_timeout')
 def test_inspection_wait_consuming_last_budget_never_reads_again(self):
  with tempfile.TemporaryDirectory() as directory:
   inspect=Mock(side_effect=[boundary.wrapper.ContainerInspectionUnavailable('synthetic timeout'),{'Id':'late'}]);now=[0]
   with patch.object(pg.time,'monotonic',side_effect=lambda:now[0]),patch.object(pg.time,'sleep',side_effect=lambda delay:now.__setitem__(0,now[0]+delay)),self.assertRaises(pg.ShutdownUnconfirmed) as failure:
    pg.container_inspection({'STACK':directory,'inspect_container':inspect},'synthetic',[],retry_deadline=.1)
   self.assertEqual(inspect.call_count,1);self.assertEqual(failure.exception.deployment_code,'preflight_observation_timeout')
 def test_inspection_returning_success_after_deadline_is_still_unknown(self):
  with tempfile.TemporaryDirectory() as directory:
   now=[0]
   def observe(name):now[0]=2;return {'Id':'late'}
   with patch.object(pg.time,'monotonic',side_effect=lambda:now[0]),self.assertRaises(pg.ShutdownUnconfirmed) as failure:
    pg.container_inspection({'STACK':directory,'inspect_container':observe},'synthetic',[],retry_deadline=1)
   self.assertEqual(failure.exception.deployment_code,'preflight_observation_timeout')
 def test_non_transient_inspection_failure_never_retries(self):
  with tempfile.TemporaryDirectory() as directory:
   inspect=Mock(side_effect=RuntimeError('synthetic invalid identity'))
   with patch.object(pg.time,'sleep') as sleep,self.assertRaises(pg.ShutdownUnconfirmed):
    pg.container_inspection({'STACK':directory,'inspect_container':inspect},'synthetic',[],retry_deadline=time.monotonic()+30)
   self.assertEqual(inspect.call_count,1);sleep.assert_not_called()
 def test_identity_change_after_transient_read_is_not_accepted(self):
  with tempfile.TemporaryDirectory() as directory:
   api,common,rows,calls,original=self.fixture(directory);observe=api['inspect_container'];missed=[]
   def inspect(name):
    if rows.get(name,{}).get('State',{}).get('Status')=='exited':
     if not missed:
      missed.append(name);raise boundary.wrapper.ContainerInspectionUnavailable('synthetic timeout')
     value=observe(name);value['Image']='sha256:'+'b'*64;return value
    return observe(name)
   api['inspect_container']=inspect
   with patch.object(pg.time,'sleep'),self.assertRaises(pg.ShutdownUnconfirmed) as failure:pg.image_proof(api,IMAGE,common)
   self.assertEqual(failure.exception.deployment_code,'preflight_execution_identity_changed');self.assertFalse(any(call[0] in ['logs','rm'] for call in calls))
 def test_create_unknown_does_not_create_again_and_late_exact_row_resumes(self):
  with tempfile.TemporaryDirectory() as directory:
   api,common,rows,calls,original=self.fixture(directory);late={}
   def docker(*args,**kwargs):
    if args[0]=='create':
     original(*args,**kwargs);late.update(rows);rows.clear();raise subprocess.TimeoutExpired(['docker','create'],30)
    return original(*args,**kwargs)
   api['docker']=docker
   with self.assertRaises(pg.ShutdownUnconfirmed) as failure:pg.image_proof(api,IMAGE,common)
   self.assertEqual(failure.exception.deployment_code,'preflight_create_unknown')
   with self.assertRaises(pg.ShutdownUnconfirmed) as failure:pg.image_proof(api,IMAGE,common)
   self.assertEqual(failure.exception.deployment_code,'preflight_creation_pending');rows.update(late)
   self.assertEqual(pg.image_proof(api,IMAGE,common),PROOF);self.assertEqual(len([call for call in calls if call[0]=='create']),1)
 def test_unconfirmed_execution_resumes_same_exited_object_without_second_start(self):
  with tempfile.TemporaryDirectory() as directory:
   api,common,rows,calls,original=self.fixture(directory);now=[0]
   def docker(*args,**kwargs):
    if args[0]=='start':calls.append(args);now[0]=31;raise subprocess.TimeoutExpired(['docker','start'],10)
    return original(*args,**kwargs)
   api['docker']=docker
   with patch.object(pg.time,'monotonic',side_effect=lambda:now[0]),self.assertRaises(pg.ShutdownUnconfirmed) as failure:pg.image_proof(api,IMAGE,common)
   self.assertEqual(failure.exception.deployment_code,'preflight_observation_timeout')
   self.assertEqual(json.loads(next((Path(directory)/'registered').glob('*.json')).read_text())['phase'],'start_requested')
   next(iter(rows.values()))['State']={'Status':'exited','Running':False,'ExitCode':0,'Pid':0}
   self.assertEqual(pg.image_proof(api,IMAGE,common),PROOF);self.assertEqual(len([call for call in calls if call[0]=='start']),1)
 def test_changed_identity_environment_mount_or_isolation_never_starts_or_removes(self):
  for variant in ['id','environment','mount','port','cpu','entrypoint']:
   with self.subTest(variant=variant),tempfile.TemporaryDirectory() as directory:
    api,common,rows,calls,original=self.fixture(directory)
    def docker(*args,**kwargs):
     value=original(*args,**kwargs)
     if args[0]=='create':
      row=next(iter(rows.values()))
      if variant=='id':row['Image']='sha256:'+'b'*64
      elif variant=='environment':row['Config']['Env'].append('NODE_OPTIONS=malicious')
      elif variant=='mount':row['Mounts'][0]['RW']=True
      elif variant=='port':row['HostConfig']['PortBindings']={'80/tcp':[{'HostPort':'80'}]}
      elif variant=='cpu':row['HostConfig']['NanoCpus']=1000000
      else:row['Config']['Entrypoint']=['sh']
     return value
    api['docker']=docker
    with self.assertRaises(pg.ShutdownUnconfirmed):pg.image_proof(api,IMAGE,common)
    self.assertEqual([call[0] for call in calls],['create'])
 def test_nonzero_exit_and_invalid_proof_fail_without_removing_evidence(self):
  for variant in ['exit','oom','proof']:
   with self.subTest(variant=variant),tempfile.TemporaryDirectory() as directory:
    api,common,rows,calls,original=self.fixture(directory)
    def docker(*args,**kwargs):
     value=original(*args,**kwargs)
     if args[0]=='start':
      if variant=='exit':next(iter(rows.values()))['State']['ExitCode']=1
      if variant=='oom':next(iter(rows.values()))['State']['OOMKilled']=True
     if args[0]=='logs' and variant=='proof':value.stdout=json.dumps({**PROOF,'credentials_verified':False})
     return value
    api['docker']=docker
    with self.assertRaises(pg.ShutdownUnconfirmed):pg.image_proof(api,IMAGE,common)
    self.assertFalse(any(call[0]=='rm' for call in calls))
 def test_cleanup_timeout_retains_registered_success_then_requires_new_fresh_proof(self):
  with tempfile.TemporaryDirectory() as directory:
   api,common,rows,calls,original=self.fixture(directory)
   def docker(*args,**kwargs):
    if args[0]=='rm':calls.append(args);raise subprocess.TimeoutExpired(['docker','rm'],10)
    return original(*args,**kwargs)
   api['docker']=docker;self.assertEqual(pg.image_proof(api,IMAGE,common),PROOF)
   self.assertEqual(len(rows),1);api['docker']=original;self.assertEqual(pg.image_proof(api,IMAGE,common),PROOF)
   self.assertEqual(len([call for call in calls if call[0]=='create']),2);self.assertEqual(rows,{})

if __name__=='__main__':unittest.main()
