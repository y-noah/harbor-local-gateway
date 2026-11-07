import importlib.util,json,subprocess,tempfile,types,unittest
from pathlib import Path
from unittest.mock import Mock,patch
ROOT=Path(__file__).parents[1]
spec=importlib.util.spec_from_file_location('local_release',ROOT/'ci/local-assurance-deploy.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Tests(unittest.TestCase):
 def test_disabled_or_wrong_target_rejected(self):
  config={'port':8092,'storage_engine':'postgres','image_prefix':'registry.test/app'};image=config['image_prefix']+':'+m.REVISION
  p={'enabled':True,'version':1,'mode':'local-assurance','releaseId':m.RELEASE,'businessRevision':m.REVISION,'businessConfigDigest':'sha256:'+'a'*64,'runDirectory':str(m.RUN)}
  self.assertIs(m.validate_profile(p,image,config),p)
  for change in [{'enabled':False},{'businessConfigDigest':None},{'runDirectory':'/tmp/other'}]:
   with self.assertRaises(ValueError):m.validate_profile({**p,**change},image,config)
  with self.assertRaises(ValueError):m.validate_profile(p,image,{**config,'port':8093})
 def test_helper_timeout_is_unknown_not_failed_exit(self):
  with patch.object(m.subprocess,'run',side_effect=subprocess.TimeoutExpired('helper',1)),self.assertRaises(m.LocalOperationUnknown):m.helper('migrate')
 def scenario(self,failure):
  with tempfile.TemporaryDirectory() as td:
   run=Path(td)/'run';entryprofile=Path(td)/'entry.json';deployprofile=Path(td)/'deploy.json';entryprofile.write_text('{}');deployprofile.write_text('{}');active=Path(td)/'active';calls=[];saved={}
   source={'Id':'source','Image':'old','HostConfig':{'RestartPolicy':{'Name':'unless-stopped','MaximumRetryCount':0}}}
   config={'port':8092,'storage_engine':'postgres','image_prefix':'registry.test/app','local_assurance_release':True,'device_tls_port':8443};image=config['image_prefix']+':'+m.REVISION
   profile={'enabled':True,'version':1,'mode':'local-assurance','releaseId':m.RELEASE,'businessRevision':m.REVISION,'businessConfigDigest':'sha256:'+'a'*64,'businessImage':'registry.test/app@sha256:'+'b'*64,'runDirectory':str(run),'source':{'containerId':'source','imageId':'old'}}
   ctx=types.SimpleNamespace(activated=False,candidate={'containerId':'new'})
   def activate(value):
    calls.append('activate')
    if failure=='after-commit':active.write_text('{}');raise RuntimeError('lost final reply')
    if failure=='before-commit':raise RuntimeError('pre-public health')
    active.write_text('{}');ctx.activated=True
   ctx.activate=activate
   def save(path,value):saved.clear();saved.update(json.loads(json.dumps(value)))
   entry=types.SimpleNamespace(EntryDeployment=types.SimpleNamespace(enter_probe_worker=lambda scope:calls.append('scope')),PROFILE=entryprofile,STATE=Path(td)/'entry-state',ACTIVE=active,STORAGE_MARKER=None,validate=lambda x:x,private_json=lambda p:{} if p==entryprofile else {'phase':'prepared'},verify_source=lambda x:source,run_action=lambda c,a:calls.append(a),save_json=save,authorize_deployment=lambda **kw:ctx)
   class Unknown(RuntimeError):pass
   comp=types.SimpleNamespace(ShutdownUnconfirmed=Unknown,configuration=lambda *a:([],{}),image_proof=lambda *a:(_ for _ in ()).throw(Unknown('capability rejected')) if failure=='preflight-rejected' else {'contract':'postgres-conversation-v2'},container_inspection=lambda *a:None,deploy=lambda *a:calls.append('compose') or {'healthy':True})
   def helper(action,timeout=0):
    calls.append(action)
    if action=='migrate':
     (run/'source-fence.json').write_text(json.dumps({'state':'unknown' if failure=='fence-unknown' else 'fenced','sourceFence':{'sourceStopped':True}}))
     if failure=='helper-timeout':raise m.LocalOperationUnknown('timeout')
     if failure in ['lost-migration-reply','fence-unknown']:raise RuntimeError('helper failed')
     return {'state':'data-verified','identityAndDataVerified':True,'database':{'name':'newdb'}}
    if action=='close':return {'state':'closed','runtimeLoginRevoked':True,'targetSessions':0,'database':'newdb'}
    return {'state':'prepared'}
   recovery=types.SimpleNamespace(recover=lambda x:calls.append('recover') or {'sourceRunning':True})
   def load(path,name):return entry if name=='local_entry_controller' else comp if name=='local_postgres_composition' else recovery
   def private(path):
    if Path(path)==m.PROFILE:return profile
    if str(path)=='/etc/harbor-storage-operator.json':return {'source':{'supervisorKind':'docker-restart'}}
    if Path(path)==run/'source-fence.json':return json.loads(Path(path).read_text())
    if Path(path)==run/'release.json':return saved
    raise AssertionError(str(path))
   api={'ENTRY_HELPER':str(entryprofile),'CONFIG':str(deployprofile),'NAME':'harbor-local-gateway','docker':Mock()}
   if failure=='preflight-run-unknown':
    def attempted(local_api,*args):local_api['docker']('run','synthetic');raise Unknown('outcome unknown')
    comp.image_proof=attempted
   with patch.object(m,'RUN',run),patch.object(m,'STATE',run/'release.json'),patch.object(m,'private_json',side_effect=private),patch.object(m,'load_file',side_effect=load),patch.object(m,'helper',side_effect=helper),patch.object(m,'reclaim_before_public',side_effect=lambda *a:calls.append('reclaim')),patch.object(m,'restore_publisher',side_effect=lambda db:calls.append('restore-publisher')),patch.object(m,'recover_local_source',side_effect=lambda *a:calls.append('recover') or {'sourceRunning':True}),patch.object(m,'device_gate_install',return_value={}),patch.object(m,'device_gate_remove'),patch.object(m,'candidate_network',return_value={}):
    if failure is None:m.run_release(image,config,None,api)
    else:
     with self.assertRaises(RuntimeError):m.run_release(image,config,None,api)
   return calls,saved
 def test_success_is_verified_then_single_cutover(self):
  calls,state=self.scenario(None);self.assertEqual(calls,['scope','prepare','arm','hold-drain','migrate','compose','activate']);self.assertEqual(state['phase'],'committed');self.assertFalse(state['offhostVerified'])
 def test_prepublic_activation_failure_reclaims_before_recovery(self):
  calls,state=self.scenario('before-commit');self.assertEqual(calls[-4:],['reclaim','close','restore-publisher','recover']);self.assertEqual(state['phase'],'source-restored')
 def test_preflight_rejection_before_candidate_start_recovers_verified_absence(self):
  calls,state=self.scenario('preflight-rejected');self.assertEqual(calls[-3:],['close','restore-publisher','recover']);self.assertEqual(state['phase'],'source-restored')
 def test_lost_migration_reply_still_closes_and_restores_publisher(self):
  calls,state=self.scenario('lost-migration-reply');self.assertEqual(calls[-3:],['close','restore-publisher','recover']);self.assertEqual(state['phase'],'source-restored')
 def test_durable_commit_failure_never_recovers_sqlite(self):
  calls,state=self.scenario('after-commit');self.assertNotIn('reclaim',calls);self.assertNotIn('recover',calls);self.assertNotIn('close',calls);self.assertTrue(state['sqliteRollbackForbidden'])
 def test_timeout_and_uncertain_fence_never_restart_source(self):
  for failure in ['helper-timeout','fence-unknown','preflight-run-unknown']:
   calls,state=self.scenario(failure);self.assertNotIn('recover',calls);self.assertNotIn('close',calls)
if __name__=='__main__':unittest.main()

class DetachedRunnerTests(unittest.TestCase):
 def test_ci_disconnect_keeps_host_runner_and_inherited_deploy_lock(self):
  import fcntl,os,signal,sys,time
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);run=root/'run';gate=root/'gate';os.mkfifo(gate);lock=root/'deploy.lock';launcher=root/'launcher.py'
   launcher.write_text('''import importlib.util,json,os,fcntl,select
from pathlib import Path
s=importlib.util.spec_from_file_location('release',__MODULE__);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
m.RUN=Path(__RUN__);m.validate_profile=lambda *a:None
m.private_json=lambda path:json.loads(Path(path).read_text()) if Path(path).exists() else {}
def work(*a):
 fd=os.open(__GATE__,os.O_RDONLY|os.O_NONBLOCK)
 try:
  if not select.select([fd],[],[],10)[0]:raise RuntimeError('fixture timeout')
  os.read(fd,1)
 finally:os.close(fd)
 return {'accepted':True}
m.run_release=work
held=open(__LOCK__,'w');fcntl.flock(held,fcntl.LOCK_EX)
m.deploy('synthetic',{},None,{})
'''.replace('__MODULE__',repr(str(ROOT/'ci/local-assurance-deploy.py'))).replace('__RUN__',repr(str(run))).replace('__GATE__',repr(str(gate))).replace('__LOCK__',repr(str(lock))))
   child=subprocess.Popen([sys.executable,'-I','-B',str(launcher)],stdout=subprocess.PIPE,stderr=subprocess.PIPE);pid=None;writer=None
   try:
    deadline=time.monotonic()+3
    while not (run/'runner-process.json').exists() and time.monotonic()<deadline:time.sleep(.01)
    record=json.loads((run/'runner-process.json').read_text());pid=record['pid'];self.assertEqual(record['session'],pid)
    deadline=time.monotonic()+3
    while writer is None:
     try:writer=os.open(gate,os.O_WRONLY|os.O_NONBLOCK)
     except OSError as error:
      if error.errno!=6 or time.monotonic()>=deadline:raise
      time.sleep(.01)
    child.kill();child.communicate(timeout=2)
    with lock.open('a') as attempt:
     with self.assertRaises(BlockingIOError):fcntl.flock(attempt,fcntl.LOCK_EX|fcntl.LOCK_NB)
    os.write(writer,b'1');os.close(writer);writer=None;deadline=time.monotonic()+3
    while not (run/'runner-result.json').exists() and time.monotonic()<deadline:time.sleep(.01)
    result=json.loads((run/'runner-result.json').read_text());self.assertEqual(result['state'],'complete');self.assertTrue(result['result']['accepted'])
   finally:
    if writer is not None:os.close(writer)
    if child.poll() is None:child.kill();child.communicate(timeout=2)
    if pid is not None:
     try:
      actual=Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
      if actual==record['startTicks']:os.kill(pid,signal.SIGKILL)
     except (ProcessLookupError,FileNotFoundError):pass

class CandidateReclaimTests(unittest.TestCase):
 def test_created_candidate_removed_without_shutdown_and_intent_unbound(self):
  cid='c'*64;calls=[];row={'Id':cid,'Config':{'Labels':{'com.harbor.activation-deployment-id':m.RELEASE,'com.harbor.activation-receipt':'receipt'}},'State':{'Running':False,'Status':'created','StartedAt':'0001-01-01T00:00:00Z'}}
  rows={'app':row,'app-worker':None};context=types.SimpleNamespace(activated=False,candidate=None,state={'candidateIntent':{'containerId':cid}},unbind_candidate=lambda **kw:calls.append(('unbind',kw['removed_container_ids'])))
  def docker(action,*args):
   calls.append((action,*args))
   if action=='rm':rows['app']=None
   elif action=='rename':rows['app']={'Id':'original'}
  comp=types.SimpleNamespace(container_inspection=lambda api,target,diag:rows.get(target),stop_confirmed=Mock(side_effect=AssertionError('never-started candidate must not execute shutdown')))
  class Activation:
   approval={'receiptDigest':'receipt'}
   def __call__(self,*a):calls.append(('revoke',));return {'state':'failed'}
  m.reclaim_before_public(context,comp,{'docker':docker},Activation(),'image','app','original')
  self.assertEqual(calls,[('rm',cid),('revoke',),('unbind',[cid]),('rename','original','app')])
 def test_removed_container_still_present_blocks_recovery(self):
  row={'Id':'c'*64,'Config':{'Labels':{'com.harbor.activation-deployment-id':m.RELEASE,'com.harbor.activation-receipt':'receipt'}},'State':{'Running':False,'Status':'created','StartedAt':'0001-01-01T00:00:00Z'}}
  context=types.SimpleNamespace(activated=False,candidate=None,state={});comp=types.SimpleNamespace(container_inspection=lambda *a:row,stop_confirmed=Mock())
  with self.assertRaises(m.LocalOperationUnknown):m.reclaim_before_public(context,comp,{'docker':Mock()},types.SimpleNamespace(approval={'receiptDigest':'receipt'}),'image','app','original')

class LocalRecoveryAuthorizationTests(unittest.TestCase):
 def fixture(self,root):
  source={'containerId':'c'*64,'imageId':'sha256:'+'a'*64};config={'source':source,'bridge':{'name':'br-test','ports':['old','other']},'entryCgroup':'entry'};state={'phase':'holding','bridgeIfindex':9,'sourceBridgePort':'old','drainReceipt':{'entryEpoch':3}}
  row={'Id':source['containerId'],'Image':source['imageId'],'Mounts':[{'Destination':'/app/data','Source':'preserved','RW':True}],'State':{'Running':False,'StartedAt':'start','FinishedAt':'finish'}}
  fence={'state':'fenced','sourceFence':{'sourceContainerId':row['Id'],'sourceImageId':row['Image'],'sourceStartedAt':'start','sourceFinishedAt':'finish','sourceStopped':True,'supervisorFenced':True,'productionVerified':True}}
  def require(value,code):
   if not value:raise RuntimeError(code)
  entry=types.SimpleNamespace(PROFILE=root/'profile',STATE=root/'state.json',CONTROL='control',validate=lambda v:v,private_json=lambda path:config if path==root/'profile' else state,inspect_container=lambda cid:row,require=require,verify_source=Mock(return_value=row),verify_resources=Mock(),entry_state=lambda:{'slot':'legacy','phase':'drained','inflight':0,'epoch':3},execute=lambda args:json.dumps([{'ifindex':9}] if args[-2:]==['dev','br-test'] else [{'ifname':'other'}]),process_cgroup=lambda pid:'entry',socket_owner=lambda path:123,save_json=Mock(),save_state=Mock())
  recovery=types.SimpleNamespace(no_writer=Mock(),no_candidates=Mock(),finish_recovery=Mock(return_value={'sourceRunning':True}))
  saved={'phase':'failed-before-public','sourceContainerId':row['Id'],'sourceMounts':row['Mounts'],'sourceRestartPolicy':{'Name':'unless-stopped','MaximumRetryCount':0}};closed={'state':'closed','targetSessions':0,'runtimeLoginRevoked':True}
  return entry,recovery,{'source':source},saved,closed,fence,row
 def test_exact_fenced_source_and_closed_runtime_use_local_contract(self):
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);entry,recovery,profile,saved,closed,fence,row=self.fixture(root)
   with patch.object(m,'RUN',root),patch.object(m,'private_json',return_value=fence):result=m.recover_local_source(entry,recovery,profile,saved,closed)
   self.assertTrue(result['sourceRunning']);self.assertTrue(recovery.finish_recovery.call_args.kwargs['local_assurance']);journal=entry.save_json.call_args.args[1];self.assertEqual(journal['contract'],'harbor-local-assurance-source-recovery-v1');self.assertNotIn('proof',journal)
 def test_unknown_fence_live_role_or_changed_mounts_cannot_restart(self):
  for change in ['fence','role','mounts']:
   with self.subTest(change=change),tempfile.TemporaryDirectory() as td:
    root=Path(td);entry,recovery,profile,saved,closed,fence,row=self.fixture(root)
    if change=='fence':fence['state']='unknown'
    elif change=='role':closed['runtimeLoginRevoked']=False
    else:row['Mounts']=[]
    with patch.object(m,'RUN',root),patch.object(m,'private_json',return_value=fence),self.assertRaises(RuntimeError):m.recover_local_source(entry,recovery,profile,saved,closed)
    recovery.finish_recovery.assert_not_called();entry.save_json.assert_not_called()

class DeviceGateTests(unittest.TestCase):
 def test_dual_stack_nonloopback_gate_precedes_dnat_and_uses_configured_port(self):
  calls=[];snapshot={'nftables':[{'table':{'family':'inet','name':m.DEVICE_TABLE}}]}
  def execute(args,**kw):
   calls.append((args,kw))
   if args[0]=='/usr/bin/ss':return ''
   return json.dumps({'nftables':[]} if args[-1]=='tables' else snapshot)
  entry=types.SimpleNamespace(execute=execute,require=lambda ok,code:self.assertTrue(ok,code),save_json=Mock())
  with patch.object(m,'candidate_network',return_value={'bridge':'br-approved'}):result=m.device_gate_install(entry,8443)
  rule=calls[2][1]['body'];self.assertIn('table inet '+m.DEVICE_TABLE,rule);self.assertIn('hook prerouting priority -110',rule);self.assertIn('iifname != "lo" tcp dport 8443 drop',rule);self.assertIn('hook forward priority -110',rule);self.assertIn('oifname "br-approved" tcp dport 8092 drop',rule);self.assertNotIn('dport 5432',rule);self.assertNotIn('hook output',rule);self.assertEqual(result,snapshot)
  m.device_gate_remove(entry,result);self.assertIn((['/usr/sbin/nft','delete','table','inet',m.DEVICE_TABLE],{}),calls)
 def test_changed_gate_is_retained(self):
  commands=[]
  def require(ok,code):
   if not ok:raise m.LocalOperationUnknown(code)
  entry=types.SimpleNamespace(require=require,execute=lambda args:commands.append(args) or json.dumps({'nftables':[{'rule':{'expr':'foreign'}}]}))
  with self.assertRaises(m.LocalOperationUnknown):m.device_gate_remove(entry,{'nftables':[]})
  self.assertFalse(any('delete' in args for args in commands))

class PublisherRestoreTests(unittest.TestCase):
 def test_recovery_preserves_public_binder_mode_under_private_wrapper_umask(self):
  import os,stat
  with tempfile.TemporaryDirectory() as td:
   root=Path(td);(root/'backup').mkdir();before={'database':'old','contract':'fixed'};(root/'backup/publisher-before.json').write_text(json.dumps(before));publisher=root/'publisher.json';publisher.write_text(json.dumps({**before,'database':'new'}));publisher.chmod(0o644);old_umask=os.umask(0o077)
   try:
    with patch.object(m,'RUN',root),patch.object(m,'PUBLISHER',publisher),patch.object(m,'private_json',side_effect=lambda p:json.loads(Path(p).read_text())):m.restore_publisher('new')
   finally:os.umask(old_umask)
   self.assertEqual(json.loads(publisher.read_text()),before);self.assertEqual(stat.S_IMODE(publisher.stat().st_mode),0o644);self.assertEqual(publisher.stat().st_uid,os.getuid())

class DynamicRootPolicyTests(unittest.TestCase):
 def fixture(self):
  source={'containerId':'source','imageId':'old','startedAt':'start'};config={'port':8092,'storage_engine':'postgres','image_prefix':'registry.test/app','local_assurance_release':True,'postgres_network':'isolated'}
  profile={'enabled':True,'version':1,'mode':'local-assurance','releaseId':m.RELEASE,'businessRevision':m.REVISION,'businessConfigDigest':'sha256:'+'a'*64,'runDirectory':str(m.RUN),'source':source,'allowDynamicBridgePeers':True,'candidateNetwork':{'name':'isolated','id':'a'*64}}
  state={'allowDynamicBridgePeers':True,'localAssuranceReleaseId':m.RELEASE,'localAssuranceProfileIdentityDigest':m.profile_identity(profile),'source':source,'dynamicBridgeIdentity':{'name':'docker0','mac':'aa','ifindex':2},'dynamicPeerDeviceIndices':{},'dynamicSourceNamespace':[4,5]}
  view={'source':source,'bridge':{'name':'docker0','mac':'aa'}}
  return config,profile,state,view
 def test_default_and_disabled_root_flags_cannot_relax_peers(self):
  config,profile,state,view=self.fixture()
  self.assertFalse(m.allow_dynamic_bridge_peers(view,{}))
  for p,r in [({**profile,'allowDynamicBridgePeers':False},config),(profile,{**config,'local_assurance_release':False}),({**profile,'enabled':False},config)]:
   with patch.object(m,'private_json',side_effect=lambda path:p if Path(path)==m.PROFILE else r):self.assertFalse(m.allow_dynamic_bridge_peers(view,state))
 def test_only_exact_source_bridge_uses_dynamic_policy_active_candidate_stays_strict(self):
  config,profile,state,view=self.fixture()
  with patch.object(m,'private_json',side_effect=lambda path:profile if Path(path)==m.PROFILE else config):
   self.assertTrue(m.allow_dynamic_bridge_peers(view,state))
   self.assertFalse(m.allow_dynamic_bridge_peers({**view,'source':{**view['source'],'containerId':'candidate'}},state))
   self.assertFalse(m.allow_dynamic_bridge_peers({**view,'bridge':{'name':'br-candidate','mac':'aa'}},state))
   with self.assertRaisesRegex(ValueError,'identity changed'):m.allow_dynamic_bridge_peers(view,{**state,'localAssuranceReleaseId':'other'})
 def test_candidate_network_requires_root_operator_agreement_and_actual_single_membership(self):
  config,profile,state,view=self.fixture()
  def private(path):return profile if Path(path)==m.PROFILE else {'candidate':{'network':'isolated'}} if str(path)=='/etc/harbor-storage-operator.json' else config
  with patch.object(m,'private_json',side_effect=private):
   row={'NetworkSettings':{'Networks':{'isolated':{'NetworkID':'a'*64}}}};self.assertEqual(m.verify_candidate_network(row)['name'],'isolated')
   with self.assertRaisesRegex(ValueError,'network identity changed'):m.verify_candidate_network({'NetworkSettings':{'Networks':{'bridge':{'NetworkID':'a'*64}}}})
   config.pop('postgres_network')
   with self.assertRaisesRegex(ValueError,'approved isolated'):m.candidate_network()
