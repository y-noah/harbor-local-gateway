import copy,importlib.util,json,os,stat,tempfile,unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
ROOT=Path(__file__).parents[1]
def module(name,path):
 spec=importlib.util.spec_from_file_location(name,ROOT/path);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value
upgrade=module('upgrade','ci/postgres-upgrade-deploy.py')
wrapper=module('upgrade_wrapper','ci/deploy-wrapper.py')
pg=module('upgrade_pg','ci/postgres-deploy.py')
controller=module('upgrade_controller','ci/gateway-entry-controller.py')
IMAGE='registry.test/harbor/harbor-local-gateway:'+40*'a'
OLD='registry.test/harbor/harbor-local-gateway:'+40*'b'
NAME='harbor-local-gateway'
MATERIALS='/opt/product-development/harbor-local-gateway/local-assurance/00000000-0000-0000-0000-000000000001'
CONFIG={'image_prefix':IMAGE.rsplit(':',1)[0],'port':8092,'storage_engine':'postgres','local_assurance_release':True,'entry_installed_release':True,'postgres_network':'owned-pg-net','device_tls_port':8093}
def active():
 return {'version':1,'image':OLD,'approval':{'contract':'harbor-local-assurance-deploy-v1','deploymentId':'00000000-0000-0000-0000-000000000001','receiptDigest':'e'*64,'configDigest':'sha256:'+'b'*64,'manifestDigest':'sha256:'+'f'*64,'revision':'b'*40,'sourceFence':{'scope':'docker-systemd'}},'candidate':{'containerId':'a'*64,'imageId':'sha256:'+'b'*64,'address':'192.0.2.2','port':8092},'mounts':[{'source':MATERIALS+'/pg-data','destination':'/app/data','readWrite':True},{'source':MATERIALS+'/runtime','destination':'/app/runtime','readWrite':True}],'configDigest':'2'*64,'storageMarkerPath':MATERIALS+'/postgres-storage.json','storageMarkerDigest':upgrade.digest({'contract':'postgres-conversation-v2','database_digest':'3'*64}),'networkSnapshot':{}}
def ticket():
 a=active();return {'version':1,'contract':upgrade.CONTRACT,'approved':True,'deploymentId':'00000000-0000-0000-0000-000000000002','hostToolsRevision':'9'*40,'to':{'image':IMAGE,'revision':'a'*40,'manifestDigest':'sha256:'+'c'*64,'configDigest':'sha256:'+'a'*64},'from':{'image':OLD,'imageId':'sha256:'+'b'*64,'containerId':'a'*64,'workerId':'b'*64,'activeIdentityDigest':upgrade.digest(upgrade.active_identity(a))},'materialsDirectory':MATERIALS,'database':{'name':'harbor_owned','oid':'123','cluster':'123456'},'databaseDigest':'3'*64,'schemaDigest':'4'*64,'deviceCaDigest':'5'*64,'storageMarkerDigest':a['storageMarkerDigest'],'network':{'name':'owned-pg-net','id':'6'*64,'bridge':'br-owned','bridgeIfindex':123,'bridgeMac':'02:00:00:00:00:01','infrastructureContainerIds':['1'*64]}}
def target(t,role='gateway'):
 authorization=upgrade.approval(t)
 return {'Id':('c' if role=='gateway' else 'd')*64,'Image':t['to']['configDigest'],'Config':{'Image':IMAGE,'Cmd':['node','/app/postgres-'+role+'.mjs'],'Labels':{'com.aistrong.project':NAME,'com.harbor.activation-deployment-id':t['deploymentId'],'com.harbor.activation-receipt':authorization['receiptDigest']}},'State':{'Running':True,'Health':{'Status':'healthy'}},'Mounts':[{'Source':MATERIALS+'/pg-data','Destination':'/app/data','RW':True},{'Source':MATERIALS+'/runtime','Destination':'/app/runtime','RW':True}],'NetworkSettings':{'Networks':{'owned-pg-net':{'NetworkID':'6'*64,'IPAddress':'192.0.2.3'}}}}

class RuntimeUpgradeTests(unittest.TestCase):
 def test_invalid_root_preflight_budget_rejects_before_source_or_entry_operations(self):
  for value in [None,True,False,29,301,30.0,'300',{},[]]:
   entry=Mock()
   with self.subTest(value=value),patch.object(upgrade,'load',return_value=pg),self.assertRaises(ValueError):upgrade.run_upgrade(IMAGE,{**CONFIG,'postgres_preflight_timeout_seconds':value},{},ticket(),entry)
   entry.active_record.assert_not_called();entry.entry_state.assert_not_called()
 def test_source_preflight_failure_happens_before_entry_hold_or_composition(self):
  t=ticket();entry=Mock();entry.active_record.return_value=(active(),{},Mock());composition=Mock();composition.preflight_timeout_seconds.side_effect=pg.preflight_timeout_seconds;composition.configuration.return_value=([],{});composition.image_proof.return_value={'database_digest':t['databaseDigest'],'schema_digest':t['schemaDigest'],'device_ca_digest':t['deviceCaDigest'],'consumption_policy_contract':'v1'};composition.prepare_source_image_preflights.side_effect=pg.ShutdownUnconfirmed('source proof failed')
  with patch.object(upgrade,'load',return_value=composition),patch.object(upgrade,'verify_source'),patch.object(upgrade,'check_storage'),self.assertRaises(pg.ShutdownUnconfirmed):upgrade.run_upgrade(IMAGE,CONFIG,{'NAME':NAME,'inspect_container':Mock(),'inspect_image':Mock()},t,entry)
  entry.entry_state.assert_not_called();composition.deploy.assert_not_called()
 def test_upgrade_runner_retains_private_success_and_failure_evidence(self):
  for fail in [False,True]:
   with self.subTest(fail=fail),tempfile.TemporaryDirectory() as directory,patch.object(upgrade,'RELEASES',Path(directory)):
    def save(path,value):path.write_text(json.dumps(value));path.chmod(0o600)
    entry=SimpleNamespace(save_json=save,private_json=lambda p:json.loads(p.read_text()),process_ticks=lambda pid:'synthetic')
    def operation():
     if fail:raise RuntimeError('synthetic failure')
     return {'completed':True,'sessionLeader':os.getsid(0)==os.getpid()}
    if fail:
     with self.assertRaisesRegex(RuntimeError,'inspect private runner log'):upgrade.supervised_upgrade(entry,ticket(),operation)
    else:self.assertEqual(upgrade.supervised_upgrade(entry,ticket(),operation),{'completed':True,'sessionLeader':True})
    run=Path(directory)/'runners'/ticket()['deploymentId'];self.assertEqual(stat.S_IMODE((run/'runner.log').stat().st_mode),0o600)
    self.assertEqual(json.loads((run/'result.json').read_text())['ok'],not fail)
    if fail:self.assertIn('synthetic failure',(run/'runner.log').read_text())
    with self.assertRaises(FileExistsError):upgrade.supervised_upgrade(entry,ticket(),operation)

 def test_observer_death_does_not_release_lock_or_kill_upgrade_child(self):
  import subprocess,time,fcntl,signal
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory)
   script="""import os,json,time,fcntl,importlib.util
from pathlib import Path
from types import SimpleNamespace
root=Path(ROOT_VALUE)
spec=importlib.util.spec_from_file_location('u',MODULE_VALUE);u=importlib.util.module_from_spec(spec);spec.loader.exec_module(u);u.RELEASES=root
lock=os.open(root/'deploy.lock',os.O_CREAT|os.O_RDWR,0o600);fcntl.flock(lock,fcntl.LOCK_EX)
def save(path,value):path.write_text(json.dumps(value));path.chmod(0o600)
e=SimpleNamespace(save_json=save,private_json=lambda p:json.loads(p.read_text()),process_ticks=lambda pid:'synthetic')
def operation():
 (root/'started').write_text(str(os.getpid()))
 end=time.monotonic()+5
 while not (root/'release').exists() and time.monotonic()<end:time.sleep(.01)
 return {'complete':True}
u.supervised_upgrade(e,{'deploymentId':'owned-test'},operation)
""".replace('ROOT_VALUE',repr(directory)).replace('MODULE_VALUE',repr(str(ROOT/'ci/postgres-upgrade-deploy.py')))
   observer=subprocess.Popen(['python3','-c',script],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
   def wait_for(path):
    deadline=time.monotonic()+5
    while not path.exists() and time.monotonic()<deadline:time.sleep(.01)
    self.assertTrue(path.exists())
   try:
    wait_for(root/'started');observer.terminate();observer.wait(timeout=3)
    fd=os.open(root/'deploy.lock',os.O_RDWR)
    try:
     with self.assertRaises(BlockingIOError):fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
     (root/'release').touch();result=root/'runners'/'owned-test'/'result.json';wait_for(result)
     self.assertTrue(json.loads(result.read_text())['ok'])
     deadline=time.monotonic()+3
     while True:
      try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);break
      except BlockingIOError:
       if time.monotonic()>deadline:raise
       time.sleep(.01)
    finally:os.close(fd)
   finally:
    (root/'release').touch()
    if observer.poll() is None:observer.terminate();observer.wait(timeout=3)

 def test_ticket_binds_same_database_materials_network_source_and_ci_target(self):
  t=ticket();self.assertIs(upgrade.validate_ticket(t,IMAGE,CONFIG),t)
  mutations=[lambda x:x.update(approved=False),lambda x:x['to'].update(image=OLD),lambda x:x['to'].update(configDigest='latest'),lambda x:x.update(materialsDirectory='/tmp/untrusted'),lambda x:x['database'].update(oid='unknown'),lambda x:x['network'].update(name='bridge'),lambda x:x['from'].update(containerId='unknown'),lambda x:x.update(hostToolsRevision='main')]
  for mutation in mutations:
   bad=copy.deepcopy(t);mutation(bad)
   with self.subTest(ticket=bad),self.assertRaises(RuntimeError):upgrade.validate_ticket(bad,IMAGE,CONFIG)
  with self.assertRaises(RuntimeError):upgrade.validate_ticket(t,IMAGE,{**CONFIG,'entry_installed_release':False})
 def test_host_tools_admission_checks_revision_and_actual_bytes_independent_of_business_image(self):
  import hashlib
  t=ticket();content={path:('synthetic '+path).encode() for path in upgrade.HOST_FILES}
  manifest={'version':1,'contract':'harbor-postgres-runtime-tools-v1','revision':t['hostToolsRevision'],'files':{path:hashlib.sha256(raw).hexdigest() for path,raw in content.items()}}
  with patch.object(upgrade,'private_json',return_value=manifest),patch.object(upgrade,'trusted_code_bytes',side_effect=content.__getitem__):
   self.assertEqual(upgrade.verify_installed_tools(t),manifest)
   with self.assertRaisesRegex(RuntimeError,'revision differs'):upgrade.verify_installed_tools({**t,'hostToolsRevision':'8'*40})
   replaced=dict(content);replaced[upgrade.HOST_FILES[2]]=b'replaced controller'
   with patch.object(upgrade,'trusted_code_bytes',side_effect=replaced.__getitem__),self.assertRaisesRegex(RuntimeError,'bytes differ'):upgrade.verify_installed_tools(t)
   changed={**t,'hostToolsRevision':'7'*40};new_manifest={**manifest,'revision':'7'*40};new_content={**content,upgrade.HOST_FILES[-1]:b'root hotfix'};new_manifest['files']={path:hashlib.sha256(raw).hexdigest() for path,raw in new_content.items()}
   with patch.object(upgrade,'private_json',return_value=new_manifest),patch.object(upgrade,'trusted_code_bytes',side_effect=new_content.__getitem__):self.assertEqual(upgrade.verify_installed_tools(changed)['revision'],'7'*40)
   self.assertEqual(changed['to'],t['to'],'host update must not require rebuilding the frozen business image')

 def test_from_active_identity_excludes_only_runtime_ip_and_network_snapshot(self):
  first=active();second=copy.deepcopy(first);second['candidate']['address']='192.0.2.9';second['networkSnapshot']={'changedRuntime':True}
  self.assertEqual(upgrade.active_identity(first),upgrade.active_identity(second))
  second['candidate']['containerId']='f'*64;self.assertNotEqual(upgrade.active_identity(first),upgrade.active_identity(second))
 def test_storage_check_uses_identity_only_never_migration_or_revoke(self):
  t=ticket();marker={'contract':'postgres-conversation-v2','database_digest':t['databaseDigest']};legacy=Mock();legacy.helper.return_value={'identityAndDataVerified':True,'database':t['database'],'materialsDirectory':MATERIALS,'connectionEnvFile':MATERIALS+'/secrets/postgres.env'}
  with patch.object(upgrade,'load',return_value=legacy),patch.object(upgrade,'private_json',return_value=marker):upgrade.check_storage(t)
  legacy.helper.assert_called_once_with('check',180)
  legacy.helper.return_value['database']={**t['database'],'oid':'999'}
  with patch.object(upgrade,'load',return_value=legacy),patch.object(upgrade,'private_json',return_value=marker),self.assertRaises(RuntimeError):upgrade.check_storage(t)
 def test_new_composition_must_be_owned_same_materials_and_network(self):
  t=ticket();gateway,worker=target(t),target(t,'worker');upgrade.verify_target(t,gateway,worker)
  mutations=[lambda x:x.update(Id=t['from']['containerId']),lambda x:x['Config']['Labels'].update({'com.harbor.activation-receipt':'x'*64}),lambda x:x['Mounts'][0].update(Source='/other/pg-data'),lambda x:x['NetworkSettings']['Networks']['owned-pg-net'].update(NetworkID='9'*64),lambda x:x['Config'].update(Cmd=['node','/app/server.mjs'])]
  for mutation in mutations:
   bad=copy.deepcopy(gateway);mutation(bad)
   with self.assertRaises(RuntimeError):upgrade.verify_target(t,bad,worker)
 def test_previous_sqlite_is_only_renamed_when_exactly_stopped_and_approved(self):
  t=ticket();t['sqliteRetirement']={'containerId':'7'*64,'imageId':'sha256:'+'8'*64};old={'Id':'7'*64,'Image':'sha256:'+'8'*64,'Config':{'Cmd':['node','server.mjs'],'Labels':{'com.aistrong.project':NAME}},'State':{'Running':False,'Pid':0,'Status':'exited'},'HostConfig':{'RestartPolicy':{'Name':'no'}}};state={NAME+'-previous':old};calls=[]
  def docker(*args,**kwargs):
   if args[:2]==('container','inspect'):
    row=inspect(args[2]);return SimpleNamespace(returncode=0 if row else 1,stdout=json.dumps([row]) if row else '')
   if args[:2]==('container','ls'):return SimpleNamespace(returncode=0,stdout='')
   calls.append(args);self.assertEqual(args[0],'rename');row=state.pop(NAME+'-previous');state[args[2]]=row
  api={'NAME':NAME,'inspect_container':state.get,'docker':docker};name=upgrade.retire_sqlite(api,t);self.assertEqual(state[name]['Id'],old['Id']);self.assertEqual(len(calls),1)
  for field,value in [('Running',True),('Pid',123),('OOMKilled',True)]:
   bad=copy.deepcopy(old);bad['State'][field]=value
   with patch.dict(api,{'inspect_container':lambda _:bad,'docker':Mock()}),self.assertRaises(RuntimeError):upgrade.retire_sqlite(api,t)
  self.assertTrue(all(call[0] not in ['start','rm','stop'] for call in calls))
 def test_wrapper_existing_active_selects_upgrade_and_never_first_migration(self):
  helper=Mock();helper.deploy.return_value={'sameDatabase':True}
  with patch.object(wrapper.os.path,'lexists',return_value=True),patch.object(wrapper.Path,'lstat',return_value=Mock(st_uid=0,st_mode=stat.S_IFREG|0o600)),patch.object(wrapper.importlib.util,'spec_from_file_location',return_value=Mock(loader=Mock())) as spec,patch.object(wrapper.importlib.util,'module_from_spec',return_value=helper):
   self.assertEqual(wrapper.deploy_entry_postgres(IMAGE,CONFIG),{'sameDatabase':True});self.assertEqual(str(spec.call_args.args[1]),'/usr/local/lib/harbor/postgres-upgrade-deploy.py')
 def test_one_time_release_directory_prevents_second_dispatch_before_hold(self):
  t=ticket();entry=Mock();entry.active_record.return_value=(active(),{},Mock());proof={'database_digest':t['databaseDigest'],'schema_digest':t['schemaDigest'],'device_ca_digest':t['deviceCaDigest'],'consumption_policy_contract':'v1'};composition=Mock();composition.ShutdownUnconfirmed=pg.ShutdownUnconfirmed;composition.SourcePreflightRejected=pg.SourcePreflightRejected;composition.configuration.return_value=([],{});composition.image_proof.return_value=proof
  with tempfile.TemporaryDirectory() as directory,patch.object(upgrade,'RELEASES',Path(directory)),patch.object(upgrade,'private_json',return_value=t),patch.object(upgrade,'load',side_effect=[entry,composition]),patch.object(upgrade,'verify_source'),patch.object(upgrade,'check_storage'),patch.object(upgrade,'network_snapshot'),patch.object(upgrade,'controller_lock',return_value=nullcontext()),patch.object(upgrade,'supervised_upgrade',side_effect=lambda entry,ticket,operation:operation()),patch.object(upgrade,'verify_installed_tools'):
   (Path(directory)/t['deploymentId']).mkdir()
   with self.assertRaises(FileExistsError):upgrade.deploy(IMAGE,CONFIG,None,{'NAME':NAME,'ENTRY_HELPER':'fixed','inspect_container':Mock(),'inspect_image':Mock()})
   entry.entry_state.assert_not_called()
 def test_real_composition_code_retains_previous_until_commit_and_rolls_back_only_pg(self):
  boundary=module('existing_pg_boundary','tests/test_deploy_postgres.py')
  for fail in [False,True]:
   case=boundary.PostgreSQLDeploymentTests()
   try:
    with self.subTest(failure=fail),tempfile.TemporaryDirectory() as directory,patch.object(boundary.pg,'secure'):
     case.prepare(directory);api,calls,state=case.api(directory,fail=fail);original=api['activation']
     def activation(action,image):return {**original(action,image),'contract':upgrade.CONTRACT}
     api.update(runtime_upgrade=True,retain_previous=True,activation=activation)
     if fail:
      with self.assertRaises(boundary.subprocess.CalledProcessError):boundary.pg.deploy(boundary.IMAGE,boundary.CONFIG,None,api)
      for name in [NAME,NAME+'-worker']:
       self.assertTrue(state[name]['State']['Running']);self.assertEqual(state[name]['Config']['Image'],boundary.OLD);self.assertIn('postgres-',state[name]['Config']['Cmd'][-1])
     else:
      boundary.pg.deploy(boundary.IMAGE,boundary.CONFIG,None,api)
      for name in [NAME,NAME+'-worker']:
       self.assertIn(name+'-previous',state);self.assertFalse(state[name+'-previous']['State']['Running']);self.assertNotIn(('rm',name+'-previous'),calls)
     self.assertFalse(any('sqlite' in str(call) for call in calls))
   finally:case.doCleanups()

 def test_next_start_uses_current_upgrade_candidate_not_sqlite_or_original_image(self):
  t=ticket();authorization=upgrade.approval(t);a={**active(),'image':IMAGE,'approval':upgrade.approval_binding(authorization),'candidate':{'containerId':'c'*64}}
  record={'contract':upgrade.CONTRACT,'ticket':t,'ticketDigest':upgrade.digest(t),'phase':'committed','candidate':a['candidate']};entry=Mock();entry.private_json.return_value=a
  with patch.object(upgrade,'load',return_value=entry),patch.object(upgrade,'record_for',return_value=record),patch.object(upgrade,'check_storage') as check,patch.object(upgrade,'verify_installed_tools',side_effect=AssertionError('active startup must not re-apply deploy ticket tool version')):
   status=upgrade.active_authorization(IMAGE);self.assertEqual(status['state'],'active');check.assert_called_once_with(t)
   with self.assertRaises(RuntimeError):upgrade.active_authorization(OLD)
  with patch.object(controller,'ACTIVE',Mock(exists=lambda:True)),patch.object(controller,'private_json',return_value=a),patch.object(controller,'runtime_upgrade_module',return_value=Mock(active_authorization=lambda image:status)),patch.object(controller,'verify_source',side_effect=AssertionError('must not inspect SQLite')):
   self.assertEqual(controller.read_active_authorization(IMAGE),status)
 def test_upgrade_gate_only_blocks_new_connections_and_cleans_exact_snapshot(self):
  entry=Mock();entry.execute.return_value=json.dumps({'nftables':[]});snapshot={'owned':True}
  with patch.object(upgrade,'gate_snapshot',return_value=snapshot):
   self.assertEqual(upgrade.gate_install(entry,ticket(),CONFIG),snapshot);script=entry.execute.call_args.kwargs['body'];self.assertEqual(script.count('ct state new'),2);self.assertNotIn('systemctl',script);upgrade.gate_remove(entry,snapshot)
  with patch.object(upgrade,'gate_snapshot',return_value={'unknown':True}),self.assertRaises(RuntimeError):upgrade.gate_remove(entry,snapshot)
 def test_orchestration_success_pg_rollback_and_unknown_shutdown_keep_sqlite_untouched(self):
  for outcome in ['success','busy-source','idle-timeout','hold-race','bad-inflight','bad-inflight-unhealthy','drain-nonzero','drain-timeout','drain-timeout-unhealthy','pg-rollback','preflight-rejected','preflight-rejected-stale-worker','preflight-rejected-core-unready','preflight-rejected-source-changed','preflight-rejected-source-stopped','preflight-rejected-network-changed','unknown-stop','unknown-stop-healthy','commit-unknown']:
   with self.subTest(outcome=outcome),tempfile.TemporaryDirectory() as directory:
    t=ticket();old=active();old_gateway=target(t);old_gateway.update(Id=t['from']['containerId'],Image=t['from']['imageId']);old_gateway['Config'].update(Image=OLD);old_worker=copy.deepcopy(old_gateway);old_worker['Id']=t['from']['workerId'];old_worker['Config']['Cmd']=['node','/app/postgres-worker.mjs'];rows={NAME:old_gateway,NAME+'-worker':old_worker};docker_calls=[]
    entry=Mock();entry.ACTIVE=Path(directory)/'active.json';entry.STATE=Path(directory)/'controller.json';entry.CANDIDATE_PROFILE=Path(directory)/'candidate.json';entry.ENTRY_CONFIG=Path(directory)/'entry.json';data={str(entry.ACTIVE):old,str(entry.STATE):{'phase':'activated','candidate':old['candidate'],'activeRuntimeConfig':{'bridge':{'address':'192.0.2.1'}}},str(entry.CANDIDATE_PROFILE):{'socketPath':'/run/harbor-entry/candidate.sock'},str(entry.ENTRY_CONFIG):{'drainTimeoutMs':500}};entry.active_record.return_value=(copy.deepcopy(old),{},old_gateway);entry.private_json.side_effect=lambda p:copy.deepcopy(data[str(p)]);entry.save_json.side_effect=lambda p,v:data.__setitem__(str(p),copy.deepcopy(v));entry.fingerprint=upgrade.digest
    control={'slot':'candidate','phase':'active','inflight':9 if outcome in ['busy-source','idle-timeout'] else -1 if outcome.startswith('bad-inflight') else 0,'epoch':0}
    def entry_state(command=None,epoch=None,slot=None,**kwargs):
     if command:
      self.assertEqual(epoch,control['epoch']);control['epoch']+=1
      if command=='hold':
       self.assertEqual(control['phase'],'active');control['phase']='holding'
       if outcome.startswith('hold-race'):control['inflight']=1
      elif command=='drain':
       self.assertEqual(control['phase'],'holding')
       if outcome.startswith('drain-timeout'):raise RuntimeError('entry drain timeout')
       control['phase']='drained';control['inflight']=1 if outcome=='drain-nonzero' else 0
      elif command=='resume':
       self.assertIn(control['phase'],['holding','drained'])
       if outcome.startswith('preflight-rejected'):remove_gate.assert_called_once()
       control['phase']='active'
      elif command=='activate':self.assertEqual(control['phase'],'drained');control['phase']='active';self.assertEqual(slot,'candidate')
     elif control['phase']=='holding' and control['inflight']>0 and outcome!='idle-timeout':control['inflight']=0
     return dict(control)
    entry.entry_state.side_effect=entry_state
    def wait_idle():
     self.assertEqual(control['phase'],'active')
     self.assertFalse(any(c.args and c.args[0] in ['hold','drain'] for c in entry.entry_state.call_args_list))
     if outcome=='idle-timeout':raise RuntimeError('entry_idle_timeout')
     if outcome.startswith('bad-inflight'):raise RuntimeError('entry_idle_count')
     control['inflight']=0
     return dict(control)
    entry.wait_entry_idle.side_effect=wait_idle
    entry.candidate_slot_ready.return_value=not outcome.endswith('unhealthy')
    if outcome=='commit-unknown':entry.restore_active.side_effect=RuntimeError('publication outcome unknown')
    composition=Mock();composition.preflight_timeout_seconds.side_effect=pg.preflight_timeout_seconds;composition.ShutdownUnconfirmed=pg.ShutdownUnconfirmed;composition.SourcePreflightRejected=pg.SourcePreflightRejected;composition.configuration.return_value=([],{});composition.image_proof.return_value={'database_digest':t['databaseDigest'],'schema_digest':t['schemaDigest'],'device_ca_digest':t['deviceCaDigest'],'consumption_policy_contract':'v1'}
    def deploy(*args):
     self.assertEqual(control['phase'],'drained');self.assertEqual(control['inflight'],0)
     self.assertEqual(args[-1]['postgres_preflight_timeout_seconds'],300)
     if outcome.startswith('preflight-rejected'):
      if outcome=='preflight-rejected-stale-worker':old_worker['State']['Health']['Status']='unhealthy'
      if outcome=='preflight-rejected-source-changed':old_gateway['Id']='9'*64
      if outcome=='preflight-rejected-source-stopped':old_worker['State']['Running']=False
      raise pg.SourcePreflightRejected('source report expired before stop')
     if outcome=='pg-rollback':raise RuntimeError('candidate failed and old PG pair restored')
     if outcome=='unknown-stop':old_gateway['State']['Running']=False;raise pg.ShutdownUnconfirmed('unknown shutdown')
     if outcome=='unknown-stop-healthy':raise pg.ShutdownUnconfirmed('stop timed out but daemon may stop late')
     old_gateway['State']['Running']=False;old_worker['State']['Running']=False
     rows[NAME+'-previous']=rows.pop(NAME);rows[NAME+'-worker-previous']=rows.pop(NAME+'-worker');rows[NAME]=target(t);rows[NAME+'-worker']=target(t,'worker');return {'activation':{'state':'active'}}
    if outcome=='preflight-rejected-network-changed':entry.verify_resources.side_effect=RuntimeError('approved network changed')
    composition.deploy.side_effect=deploy
    if outcome.startswith('preflight-rejected'):composition.verify_started_composition.side_effect=RuntimeError('the same expired report cannot satisfy fresh Worker proof')
    def docker(*args,**kwargs):
     if args[:2]==('container','inspect'):
      row=next((row for row in rows.values() if row['Id']==args[2]),None);return SimpleNamespace(returncode=0 if row else 1,stdout=json.dumps([row]) if row else '')
     if args[:2]==('container','ls'):return SimpleNamespace(returncode=0,stdout='')
     docker_calls.append(args);self.assertEqual(args[0],'rm');key=next(k for k,v in rows.items() if v['Id']==args[1]);rows.pop(key)
    api={'NAME':NAME,'inspect_container':lambda name:rows.get(name) or next((row for row in rows.values() if row['Id']==name),None),'inspect_image':Mock(),'docker':docker,'postgres_preflight_timeout_seconds':999};config={**CONFIG,'postgres_preflight_timeout_seconds':300}
    with patch.object(upgrade,'RELEASES',Path(directory)),patch.object(upgrade,'load',return_value=composition),patch.object(upgrade,'check_storage'),patch.object(upgrade,'network_snapshot',return_value={'candidateNetworkIdentity':{'id':'6'*64}}),patch.object(upgrade,'gate_install',return_value={'owned':True}) as install_gate,patch.object(upgrade,'gate_remove') as remove_gate,patch.object(upgrade,'probe',return_value=503 if outcome=='preflight-rejected-core-unready' else 200) as core_probe,patch.object(upgrade,'verify_installed_tools'):
     if outcome in ['success','busy-source','hold-race']:
      result=upgrade.run_upgrade(IMAGE,config,api,t,entry);self.assertEqual(result['contract'],upgrade.CONTRACT);self.assertEqual(data[str(entry.ACTIVE)]['candidate']['containerId'],'c'*64);self.assertEqual(control['phase'],'active');self.assertEqual(len(docker_calls),2);self.assertEqual(result['cleanup']['state'],'complete');remove_gate.assert_called_once()
     else:
      with self.assertRaisesRegex(RuntimeError,'entry drain failed' if outcome=='drain-nonzero' else ''):upgrade.run_upgrade(IMAGE,config,api,t,entry)
      if outcome=='pg-rollback':entry.refresh_active.assert_called_once();remove_gate.assert_called_once();self.assertEqual(data[str(entry.ACTIVE)]['candidate']['containerId'],t['from']['containerId'])
      elif outcome in ['preflight-rejected','preflight-rejected-stale-worker']:
       entry.refresh_active.assert_not_called();remove_gate.assert_called_once();self.assertEqual(control['phase'],'active');self.assertEqual(data[str(upgrade.release_path(t['deploymentId'])/'release.json')]['errorCategory'],'source_preflight_rejected')
      else:entry.refresh_active.assert_not_called();remove_gate.assert_not_called()
      if outcome.startswith('preflight-rejected'):
       composition.verify_started_composition.assert_not_called()
       if outcome in ['preflight-rejected-core-unready','preflight-rejected-source-changed','preflight-rejected-source-stopped','preflight-rejected-network-changed']:self.assertEqual(control['phase'],'drained')
       else:core_probe.assert_called_once();self.assertEqual(rows[NAME]['Id'],t['from']['containerId']);self.assertTrue(rows[NAME]['State']['Running'])
      if outcome.startswith('drain-'):
       install_gate.assert_not_called();composition.deploy.assert_not_called();composition.verify_started_composition.assert_not_called();entry.refresh_active.assert_not_called();remove_gate.assert_not_called()
       self.assertEqual(control['phase'],'holding' if outcome.endswith('unhealthy') else 'active')
       self.assertEqual(data[str(entry.ACTIVE)]['candidate']['containerId'],t['from']['containerId'])
       entry.candidate_slot_ready.assert_called_once_with('192.0.2.1',8092)
      if outcome=='idle-timeout':
       install_gate.assert_not_called();composition.deploy.assert_not_called();composition.verify_started_composition.assert_not_called();entry.refresh_active.assert_not_called();remove_gate.assert_not_called()
       self.assertEqual(control['phase'],'active');self.assertFalse(any(c.args and c.args[0]=='drain' for c in entry.entry_state.call_args_list))
       self.assertTrue(any(c.args and c.args[0]=='hold' for c in entry.entry_state.call_args_list))
       self.assertEqual(rows[NAME]['Id'],t['from']['containerId']);self.assertTrue(rows[NAME]['State']['Running']);self.assertTrue(rows[NAME+'-worker']['State']['Running'])
      if outcome.startswith('bad-inflight'):
       install_gate.assert_not_called();composition.deploy.assert_not_called();composition.verify_started_composition.assert_not_called();entry.refresh_active.assert_not_called();remove_gate.assert_not_called()
       self.assertEqual(control['phase'],'holding' if outcome.endswith('unhealthy') else 'active');self.assertTrue(any(call.args and call.args[0]=='hold' for call in entry.entry_state.call_args_list));self.assertFalse(any(call.args and call.args[0]=='drain' for call in entry.entry_state.call_args_list))
       self.assertEqual(data[str(entry.ACTIVE)]['candidate']['containerId'],t['from']['containerId'])
      if outcome=='unknown-stop-healthy':self.assertTrue(old_gateway['State']['Running']);self.assertTrue(old_worker['State']['Running']);composition.verify_started_composition.assert_not_called();self.assertEqual(data[str(upgrade.release_path(t['deploymentId'])/'release.json')]['phase'],'stop-unknown')
      if outcome=='commit-unknown':self.assertEqual(data[str(entry.ACTIVE)]['candidate']['containerId'],'c'*64)
     entry.wait_entry_idle.assert_not_called()
     self.assertEqual(composition.image_proof.call_args.args[0]['postgres_preflight_timeout_seconds'],300)
     self.assertTrue(all(call[0]=='rm' and len(call)==2 for call in docker_calls));self.assertFalse(any('sqlite' in str(call) for call in docker_calls))

 def test_publish_commits_real_candidate_then_rebuilds_current_network_and_activates(self):
  t=ticket();gateway,worker=target(t),target(t,'worker');record={'ticket':t};saved={};entry=Mock();entry.CANDIDATE_PROFILE=Path('/synthetic/profile');entry.ACTIVE=Path('/synthetic/active');entry.private_json.return_value={'socketPath':'/run/harbor-entry/candidate.sock'};entry.fingerprint=upgrade.digest;entry.save_json.side_effect=lambda p,v:saved.__setitem__(str(p),copy.deepcopy(v));entry.entry_state.side_effect=[{'slot':'candidate','phase':'drained','inflight':0,'epoch':10},{'slot':'candidate','phase':'active'}]
  with patch.object(upgrade,'network_snapshot',return_value={'candidateNetworkIdentity':{'id':'6'*64}}),patch.object(upgrade,'check_storage'):upgrade.publish(entry,t,record,gateway,worker,active())
  self.assertEqual(saved[str(entry.ACTIVE)]['candidate']['containerId'],gateway['Id']);self.assertEqual(saved[str(entry.ACTIVE)]['approval']['contract'],upgrade.CONTRACT);self.assertEqual(record['phase'],'committed');entry.entry_state.assert_any_call('activate',10,'candidate');entry.prepare_active_network.assert_called_once();entry.restore_active.assert_called_once()


class RetiredServiceCleanupTests(unittest.TestCase):
 def fixture(self,directory):
  t=ticket();rows={};calls=[]
  for index,(name,field,role) in enumerate([(NAME,'containerId','gateway'),(NAME+'-worker','workerId','worker')]):
   row=target(t,role);row.update(Id=t['from'][field],Image=t['from']['imageId']);row['State']['Running']=False;rows[name+'-previous']=row
  entry=Mock();entry.save_json.side_effect=lambda path,value:Path(path).write_text(json.dumps(value));entry.private_json.side_effect=lambda path:json.loads(Path(path).read_text())
  def inspect(name):return rows.get(name) or next((row for row in rows.values() if row['Id']==name),None)
  def docker(*args,**kwargs):
   if args[:2]==('container','inspect'):
    row=inspect(args[2]);return SimpleNamespace(returncode=0 if row else 1,stdout=json.dumps([row]) if row else '')
   if args[:2]==('container','ls'):return SimpleNamespace(returncode=0,stdout='')
   calls.append(args);self.assertEqual(args[0],'rm');self.assertEqual(len(args),2);key=next(name for name,row in rows.items() if row['Id']==args[1]);rows.pop(key)
  api={'NAME':NAME,'inspect_container':inspect,'docker':docker};record={'contract':upgrade.CONTRACT,'ticket':t,'ticketDigest':upgrade.digest(t),'phase':'committed'};entry.ACTIVE=Path(directory)/'active.json';entry.save_json(entry.ACTIVE,{'candidate':{'containerId':'c'*64},'networkSnapshot':{'candidateNetworkContainerIds':['c'*64,'d'*64]}});(Path(directory)/t['deploymentId']).mkdir(exist_ok=True)
  return rows,calls,entry,api,record
 def test_default_removes_exact_stopped_pair_without_volumes_or_images(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(upgrade,'RELEASES',Path(directory)):
   rows,calls,entry,api,record=self.fixture(directory);rows['unrelated']=target(ticket());rows['unrelated']['Id']='f'*64
   value=upgrade.finish_retired_cleanup({},api,entry,record,Path(directory)/record['ticket']['deploymentId']);self.assertEqual(value['state'],'complete');self.assertEqual(set(rows),{'unrelated'});self.assertEqual(calls,[('rm','a'*64),('rm','b'*64)]);self.assertEqual(entry.private_json(Path(directory)/'retired-services.json')['pairs'],[])
 def test_partial_deletion_failure_is_pending_and_retry_only_removes_remaining_member(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(upgrade,'RELEASES',Path(directory)):
   rows,calls,entry,api,record=self.fixture(directory);original=api['docker']
   def fail(*args,**kwargs):
    if args[0]=='rm' and args[1]=='b'*64:raise RuntimeError('synthetic unavailable')
    return original(*args,**kwargs)
   api['docker']=fail;result=upgrade.finish_retired_cleanup({},api,entry,record,Path(directory)/record['ticket']['deploymentId']);self.assertEqual(result['state'],'pending');self.assertEqual(record['phase'],'complete');self.assertEqual(set(rows),{NAME+'-worker-previous'});self.assertEqual(len(entry.private_json(Path(directory)/'retired-services.json')['pairs']),1)
   api['docker']=original;self.assertEqual(upgrade.retired_cleanup({},api,entry)['state'],'complete');self.assertEqual(calls,[('rm','a'*64),('rm','b'*64)]);self.assertFalse(rows)
 def test_foreign_running_active_and_changed_identity_refuse_entire_pair(self):
  mutations=[lambda rows:rows[NAME+'-previous']['State'].update(Running=True),lambda rows:rows[NAME+'-previous']['Config']['Labels'].update({'com.aistrong.project':'other'}),lambda rows:rows[NAME+'-previous'].update(Image='sha256:'+'9'*64),lambda rows:rows.update({NAME:rows[NAME+'-previous']}),lambda rows:rows[NAME+'-worker-previous']['Config'].update(Cmd=['other'])]
  for mutation in mutations:
   with self.subTest(mutation=mutation),tempfile.TemporaryDirectory() as directory,patch.object(upgrade,'RELEASES',Path(directory)):
    rows,calls,entry,api,record=self.fixture(directory);mutation(rows);result=upgrade.finish_retired_cleanup({},api,entry,record,Path(directory)/record['ticket']['deploymentId']);self.assertEqual(result['state'],'pending');self.assertEqual(calls,[])
 def test_intent_write_failure_never_deletes_and_reports_pending(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(upgrade,'RELEASES',Path(directory)):
   rows,calls,entry,api,record=self.fixture(directory);entry.save_json.side_effect=OSError('synthetic disk full');result=upgrade.finish_retired_cleanup({},api,entry,record,Path(directory)/record['ticket']['deploymentId']);self.assertEqual(result['state'],'pending');self.assertEqual(result['errorCategory'],'retired_service_cleanup_record_failed');self.assertEqual(calls,[])
 def test_unqualified_backup_and_bad_policy_reject_before_any_deployment_effect(self):
  for value in [1,True,False,None,-1,2,'0',0.0]:
   with self.subTest(value=value),patch.object(upgrade,'load') as loader,self.assertRaises(RuntimeError):upgrade.run_upgrade(IMAGE,{**CONFIG,'postgres_retained_service_pairs':value},{},ticket(),Mock())
   loader.assert_not_called()

 def test_process_interruption_keeps_intent_and_retry_updates_original_release(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(upgrade,'RELEASES',Path(directory)):
   rows,calls,entry,api,record=self.fixture(directory);path=Path(directory)/record['ticket']['deploymentId'];path.mkdir(exist_ok=True);original=api['docker']
   def interrupt(*args,**kwargs):
    value=original(*args,**kwargs)
    if args[0]=='rm':raise KeyboardInterrupt()
    return value
   api['docker']=interrupt
   with self.assertRaises(KeyboardInterrupt):upgrade.finish_retired_cleanup({},api,entry,record,path)
   self.assertEqual(entry.private_json(path/'release.json')['cleanup']['state'],'pending');self.assertEqual(len(entry.private_json(Path(directory)/'retired-services.json')['pairs']),1)
   api['docker']=original;self.assertEqual(upgrade.retired_cleanup({},api,entry)['state'],'complete');self.assertEqual(entry.private_json(path/'release.json')['cleanup']['state'],'complete');self.assertFalse(rows)

 def test_first_ledger_write_failure_recovers_from_committed_pending_release(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(upgrade,'RELEASES',Path(directory)):
   rows,calls,entry,api,record=self.fixture(directory);save=entry.save_json.side_effect
   def fail(path,value):
    if Path(path).name=='retired-services.json':raise OSError('ledger unavailable')
    save(path,value)
   entry.save_json.side_effect=fail;path=Path(directory)/record['ticket']['deploymentId'];self.assertEqual(upgrade.finish_retired_cleanup({},api,entry,record,path)['state'],'pending');self.assertEqual(calls,[])
   entry.save_json.side_effect=save;self.assertEqual(upgrade.retired_cleanup({},api,entry)['state'],'complete');self.assertFalse(rows);self.assertEqual(entry.private_json(path/'release.json')['cleanup']['state'],'complete')
 def test_failed_inspect_with_matching_full_id_keeps_pending(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(upgrade,'RELEASES',Path(directory)):
   rows,calls,entry,api,record=self.fixture(directory);original=api['docker'];queries=[]
   def fail(*args,**kwargs):
    if args[:2]==('container','inspect'):return SimpleNamespace(returncode=1,stdout='')
    if args[:2]==('container','ls'):queries.append(args);return SimpleNamespace(returncode=0,stdout='a'*64+'\n')
    return original(*args,**kwargs)
   api['docker']=fail;self.assertEqual(upgrade.finish_retired_cleanup({},api,entry,record,Path(directory)/record['ticket']['deploymentId'])['state'],'pending');self.assertEqual(calls,[]);self.assertTrue(all('--no-trunc' in args and 'id='+'a'*64 in args for args in queries));self.assertEqual(len(entry.private_json(Path(directory)/'retired-services.json')['pairs']),1)
 def test_deploy_rejects_unqualified_policy_before_supervisor_or_ticket_access(self):
  for value in [1,None,True,2,'0']:
   with self.subTest(value=value),patch.object(upgrade,'private_json') as read,patch.object(upgrade,'supervised_upgrade') as supervise,patch.object(upgrade,'load') as loader,self.assertRaises(RuntimeError):upgrade.deploy(IMAGE,{**CONFIG,'postgres_retained_service_pairs':value},None,{})
   read.assert_not_called();supervise.assert_not_called();loader.assert_not_called()
 def test_rollback_record_and_active_reference_prevent_cleanup_before_rm(self):
  for reason in ['rollback','ticket','active_gateway','active_worker']:
   with self.subTest(reason=reason),tempfile.TemporaryDirectory() as directory,patch.object(upgrade,'RELEASES',Path(directory)):
    rows,calls,entry,api,record=self.fixture(directory);path=Path(directory)/record['ticket']['deploymentId'];record['phase']='complete';record['cleanup']={'state':'pending'};pair=upgrade.retired_pair(record,api);entry.save_json(Path(directory)/'retired-services.json',{'version':1,'pairs':[pair]})
    if reason=='rollback':record['phase']='rolled-back-pg'
    if reason=='ticket':record['ticket']['from']['workerId']='f'*64
    if reason=='active_gateway':entry.save_json(entry.ACTIVE,{'candidate':{'containerId':'a'*64}})
    if reason=='active_worker':entry.save_json(entry.ACTIVE,{'candidate':{'containerId':'c'*64},'networkSnapshot':{'candidateNetworkContainerIds':['b'*64]}})
    entry.save_json(path/'release.json',record)
    if reason in ['rollback','ticket']:
     with self.assertRaises(RuntimeError):upgrade.retired_cleanup({},api,entry)
    else:self.assertEqual(upgrade.retired_cleanup({},api,entry)['state'],'pending')
    self.assertEqual(calls,[])

 def test_both_release_writes_failed_recovery_refuses_before_claiming_complete(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(upgrade,'RELEASES',Path(directory)):
   rows,calls,entry,api,record=self.fixture(directory);path=Path(directory)/record['ticket']['deploymentId'];save=entry.save_json.side_effect;save(path/'release.json',record)
   entry.save_json.side_effect=OSError('persistent disk failure');self.assertEqual(upgrade.finish_retired_cleanup({},api,entry,record,path)['errorCategory'],'retired_service_cleanup_record_failed');entry.save_json.side_effect=save
   with self.assertRaisesRegex(RuntimeError,'retired_cleanup_commit_confirmation_required'):upgrade.retired_cleanup({},api,entry)
   self.assertEqual(calls,[]);self.assertEqual(entry.private_json(path/'release.json')['phase'],'committed')
   # An operator-confirmed completion retries the same exact record and pair.
   self.assertEqual(upgrade.finish_retired_cleanup({},api,entry,record,path)['state'],'complete');self.assertFalse(rows)

if __name__=='__main__':unittest.main()
