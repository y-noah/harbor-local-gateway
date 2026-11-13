import ast,copy,importlib.util,json,os,socket,tempfile,types,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
CI=ROOT/'ci'
from unittest.mock import patch

def load(path,name):
 s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
h=load(CI/'harbor-pg-storage-host.py','host');factory=load(CI/'postgres-deploy.py','factory')
class Tests(unittest.TestCase):
 def test_factory_default_and_off_preserve_original_restart(self):
  for cfg in [{},{'postgres_storage_startup_guard':False}]:self.assertEqual(factory.storage_restart_policy(cfg),'unless-stopped')
 def test_factory_bad_boolean_or_missing_witness_never_selects_no(self):
  for val in ['true',1,None]:
   with self.assertRaises(ValueError):factory.storage_restart_policy({'postgres_storage_startup_guard':val})
  with self.assertRaises((ValueError,FileNotFoundError)):factory.storage_restart_policy({'postgres_storage_startup_guard':True})
 def test_valid_root_witness_is_exact_typed_contract(self):
  good={'contract':factory.STORAGE_STARTUP_CONTRACT,'completed':True,'samePostgres':True,'mountsVerified':True}
  class FakePath:
   def __init__(self,*a):self.parents=[]
   def lstat(self):return types.SimpleNamespace(st_uid=0,st_mode=0o100644)
  class Loader:
   def exec_module(self,module):module.verify_factory_guard=lambda config:dict(good)
  with patch.object(factory,'Path',FakePath),patch('importlib.util.spec_from_file_location',return_value=types.SimpleNamespace(loader=Loader())),patch('importlib.util.module_from_spec',return_value=types.SimpleNamespace()):
   self.assertEqual(factory.storage_restart_policy({'postgres_storage_startup_guard':True}),'no')
   for bad in [dict(good,completed=1),dict(good,completed=False),dict(good,extra=True)]:
    with patch.object(Loader,'exec_module',side_effect=lambda module,bad=bad:setattr(module,'verify_factory_guard',lambda config:bad)):
     with self.assertRaises(ValueError):factory.storage_restart_policy({'postgres_storage_startup_guard':True})
 def test_config_signature_hashes_credentials_without_persisting_them_and_only_ignores_restart(self):
  row={'Id':'a','Image':'b','Config':{'Env':['SYNTHETIC_PASSWORD=must-not-output']},'HostConfig':{'RestartPolicy':{'Name':'unless-stopped'},'Memory':8},'Mounts':[]};before=h.sig(row);self.assertNotIn('must-not-output',before);row['HostConfig']['RestartPolicy']={'Name':'no'};self.assertEqual(h.sig(row),before);row['HostConfig']['Memory']=9;self.assertNotEqual(h.sig(row),before)
 def host(self):return object.__new__(h.Host)
 def test_current_boot_active_pair_is_not_fixed_to_migration_gateway_ids(self):
  host=self.host();host.entry=types.SimpleNamespace(PROFILE='PROFILE',validate=lambda value:value,fingerprint=lambda value:'cfg',candidate_mounts=lambda *args:[],ACTIVE='ACTIVE',private_json=lambda _: {'configDigest':'cfg','mounts':[],'image':'approved-image','approval':{'deploymentId':'new-release','receiptDigest':'new-proof'},'candidate':{'containerId':'current-gw','imageId':'approved-image-id'}});host.upgrade=types.SimpleNamespace(record_for=lambda _: {'phase':'complete','workerId':'current-worker','ticket':{'materialsDirectory':'/approved','database':{'cluster':'123'}}});host.api={};host.plan={'systemId':'123'};host.j={};host.core=load(CI/'harbor-pg-storage-coordinator.py','current_core');host.immutable=lambda name:self.assertEqual(name,h.PG)
  def row(name):
   return {'Id':'current-gw' if name==h.GW else 'current-worker','Image':'approved-image-id','Config':{'Image':'approved-image','Cmd':['node','/app/postgres-'+('gateway' if name==h.GW else 'worker')+'.mjs'],'Labels':{'com.aistrong.project':h.GW,'com.harbor.activation-deployment-id':'new-release','com.harbor.activation-receipt':'new-proof'}}}
  host.row=row;host.load_current_durable_active();self.assertEqual(host.gw['Id'],'current-gw');self.assertEqual(host.worker['Id'],'current-worker')
  host.row=lambda name:{**row(name),'Image':'wrong-image'}
  with self.assertRaisesRegex(RuntimeError,'host_boot_pair_identity_changed'):host.load_current_durable_active()
 def test_pending_release_loader_requires_and_preserves_explicit_selected_approval(self):
  host=self.host();host.core=load(CI/'harbor-pg-storage-coordinator.py','pending_core');host.plan={'systemId':'123'};host.api={};active={'configDigest':'cfg','mounts':[],'image':'approved-image','approval':{'deploymentId':'release','receiptDigest':'proof'},'candidate':{'containerId':'gw','imageId':'image-id'}};digest=lambda value:json.dumps(value,sort_keys=True);host.j={'runtimeControl':{'mode':'recovering','selectedActiveDigest':digest(active['approval'])}}
  host.entry=types.SimpleNamespace(PROFILE='PROFILE',ACTIVE='ACTIVE',private_json=lambda _:copy.deepcopy(active),validate=lambda value:value,fingerprint=lambda value:'cfg',candidate_mounts=lambda *args:[]);host.upgrade=types.SimpleNamespace(record_for=lambda _:{'phase':'committed','workerId':'worker','ticket':{'materialsDirectory':'/approved','database':{'cluster':'123'}}},active_identity=lambda value:value['approval'],digest=digest);host.immutable=lambda _:None
  host.row=lambda name:{'Id':'gw' if name==h.GW else 'worker','Image':'image-id','State':{'Running':False},'Config':{'Image':'approved-image','Cmd':['node','/app/postgres-'+('gateway' if name==h.GW else 'worker')+'.mjs'],'Labels':{'com.aistrong.project':h.GW,'com.harbor.activation-deployment-id':'release','com.harbor.activation-receipt':'proof'}}}
  host.load_current_durable_active();self.assertEqual(host.currentReleasePhase,'committed');host.j['runtimeControl']['mode']='paused';host.load_current_durable_active();self.assertEqual(host.currentReleasePhase,'committed');host.j['runtimeControl']={'mode':'running'}
  with self.assertRaisesRegex(RuntimeError,'host_boot_release_incomplete'):host.load_current_durable_active()
  host.j['runtimeControl']={'mode':'recovering','selectedActiveDigest':'wrong'}
  with self.assertRaisesRegex(RuntimeError,'host_runtime_recovery_approval_changed'):host.load_current_durable_active()
 def test_normal_network_refresh_cannot_change_immutable_active(self):
  host=self.host();host.ticket={'network':{}};host.row=lambda name:{'Id':name};host.active={'image':'old'};saved=[];host.entry=types.SimpleNamespace(ACTIVE='ACTIVE',private_json=lambda _: {'image':'unapproved'},save_json=lambda *args:saved.append(args));host.upgrade=types.SimpleNamespace(network_snapshot=lambda *args:{'candidateNetworkIdentity':{}},active_identity=lambda a:a)
  with self.assertRaisesRegex(RuntimeError,'host_active_changed'):host.update_dynamic_network_snapshot()
  self.assertEqual(saved,[])
 def test_network_recovery_uses_authenticated_active_bridge_after_boot(self):
  host=self.host();network={'Id':'net','Name':'net-name','Driver':'bridge','Options':{'bridge':'br'}};host.plan={'networkPersistentDigest':h.sha(json.dumps({k:network.get(k) for k in ['Name','Id','Driver','Internal','IPAM','Options','Labels']},sort_keys=True).encode())};host.ticket={'network':{'id':'net','name':'net-name','bridge':'br','bridgeIfindex':1,'bridgeMac':'old'}};host.row=lambda name:{'Id':name};host.active={'image':'approved','networkSnapshot':{'candidateNetworkIdentity':{'id':'net','name':'net-name','bridge':'br','ifindex':22,'mac':'new'}}};saved=[];host.entry=types.SimpleNamespace(ACTIVE='ACTIVE',private_json=lambda _:copy.deepcopy(host.active),save_json=lambda *args:saved.append(args));seen=[]
  def snapshot(ticket,*args):seen.append(ticket);return {'candidateNetworkIdentity':{'ifindex':22}}
  host.upgrade=types.SimpleNamespace(active_identity=lambda value:{'image':value['image']},network_snapshot=snapshot)
  with patch.object(h,'run',side_effect=lambda args:json.dumps([network] if args[0].endswith('docker') else [{'ifindex':22,'address':'new'}]).encode()):host.update_dynamic_network_snapshot()
  self.assertEqual(seen[0]['network']['bridgeIfindex'],22);self.assertEqual(host.ticket['network']['bridgeIfindex'],1);self.assertEqual(len(saved),1)
  host.active['networkSnapshot']['candidateNetworkIdentity']['ifindex']=33
  with patch.object(h,'run',side_effect=lambda args:json.dumps([network] if args[0].endswith('docker') else [{'ifindex':22,'address':'new'}]).encode()):
   with self.assertRaisesRegex(RuntimeError,'host_current_bridge_witness_changed'):host.update_dynamic_network_snapshot()
 def test_long_oneshot_restore_accepts_new_activating_process_without_waiting_for_completion(self):
  host=self.host();name='harbor-postgres-maintain-base.service';host.state_unit=lambda _: {'ActiveState':'activating','SubState':'start','MainPID':'123','InvocationID':'new'}
  with patch.object(h.time,'sleep',side_effect=AssertionError('normal long backup must not wait for completion')):host.verify_writer_restoration({name:{'ActiveState':'active','InvocationID':'old'}})
 def test_recovery_rejects_post_proof_unknown_directory_before_external_change(self):
  core=load(CI/'harbor-pg-storage-coordinator.py','proof_core');host=self.host();host.core=core;host.load_current_durable_active=lambda:None;host.entry=types.SimpleNamespace(entry_state=lambda:{'slot':'candidate','phase':'active'});effects=[];host.upgrade=types.SimpleNamespace(drain_active_entry=lambda _:effects.append('drain'));host.immutable=lambda name:{'State':{'Running':False}};host.fence_docker_autostart=lambda:effects.append('fence');host.bind_original_paths=lambda:effects.append('bind');host.target_disk=lambda:None
  with tempfile.TemporaryDirectory(prefix='harbor-proof-') as directory:
   base=Path(directory);target=base/'target';source=base/'source';target.mkdir(mode=0o700);source.mkdir(mode=0o700)
   for root in [source,target]:
    for label in ['db','archive']:(root/label).mkdir(mode=0o700)
   proofs={}
   for label in ['db','archive']:
    mirror=core.ArchiveMirror(source/label,target/label,os.getuid());proofs[label]=mirror.finalize(mirror.online(),source_stopped=True)
   host.j={key:h.sha(json.dumps(proofs[label]).encode()) for label,key in [('db','dbProofSha256'),('archive','archiveProofSha256')]};(target/'archive'/'unknown').mkdir(mode=0o700);original=core.inventory
   with patch.object(core,'TARGET',target),patch.object(h,'private',side_effect=lambda path:json.dumps(proofs['archive' if path==h.PROOF else 'db']).encode()),patch.object(core,'inventory',side_effect=lambda path,uid:original(path,os.getuid())):
    with self.assertRaisesRegex(RuntimeError,'host_cold_target_directories_changed'):host.verify_target_recovery_checkpoint()
   self.assertTrue((target/'archive'/'unknown').is_dir());self.assertEqual(effects,[])
 def test_worker_identity_is_readonly_and_locked_reload_keeps_latest_journal(self):
  host=self.host();host.plan={'scope':'scope','runId':'a'*32};raw=json.dumps(host.plan).encode();host.planSha256=h.sha(raw);host.j={'phase':'old'};unit,intent,_=h.worker_paths(host,'recover-target','b'*16);intent_raw=json.dumps({'runId':host.plan['runId'],'bootId':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'planSha256':host.planSha256,'unit':unit,'action':'recover-target','nonce':'b'*16}).encode();host.state_unit=lambda _:{'MainPID':str(os.getpid()),'ControlGroup':'/system.slice/'+unit};host.save=lambda _:self.fail('identity must never write STATE');latest={'scope':'scope','runId':host.plan['runId'],'phase':'boot-containers-ready','gate':{'new':True}}
  with patch.object(h,'private',side_effect=lambda path,*args:intent_raw if path==intent else raw if path==h.PLAN else json.dumps(latest).encode()),patch.dict(os.environ,{'INVOCATION_ID':'c'*32}),patch.object(Path,'exists',return_value=True):
   self.assertEqual(h.require_switch_worker(host,'recover-target','b'*16),unit);self.assertEqual(host.j,{'phase':'old'});host.reload_locked_state();self.assertEqual(host.j,latest)
  with patch.object(h,'private',return_value=b'changed'):
   with self.assertRaisesRegex(RuntimeError,'host_plan_changed'):host.reload_locked_state()
 def test_pause_snapshots_current_writers_once_before_any_stop(self):
  host=self.host();host.core=types.SimpleNamespace(WRITERS=['a.timer','base.service','schedule.service']);host.j={'writers':{'a.timer':{'ActiveState':'inactive'}}};states={name:{'ActiveState':'active','MainPID':'8','InvocationID':'fresh'} for name in host.core.WRITERS};host.state_unit=lambda name:copy.deepcopy(states[name]);events=[]
  host.save=lambda j:events.append(('saved',copy.deepcopy(j)))
  def run(args,**kwargs):
   self.assertTrue(host.j.get('writersPauseStarted'));self.assertEqual(events[0][0],'saved');events.append(('stop',args[-1]));states[args[-1]]={'ActiveState':'inactive','MainPID':'0'}
  with patch.object(h,'run',side_effect=run):original=host.pause_writers()
  self.assertEqual(original['a.timer']['ActiveState'],'active');self.assertEqual(len(events),4)
  before=copy.deepcopy(host.j['writers'])
  with patch.object(h,'run',side_effect=run):host.pause_writers()
  self.assertEqual(host.j['writers'],before)
 def test_cold_db_rejects_replaced_created_root_before_any_write(self):
  core=load(CI/'harbor-pg-storage-coordinator.py','created_core');host=self.host();host.core=core;host.target_disk=lambda:None;host.require_docker_restart_disabled=lambda:self.fail('must validate root before docker/copy');host.j={}
  with tempfile.TemporaryDirectory(prefix='harbor-created-') as directory:
   root=Path(directory);target=root/'target';target.mkdir(mode=0o700);db=target/'db';db.mkdir(mode=0o700);host.j['createdTargetRoots']={'db':core.identity(db.lstat())[:5]};db.rename(target/'old-db');db.mkdir(mode=0o700)
   with patch.object(core,'TARGET',target),patch.object(h,'ROOT',root):
    with self.assertRaisesRegex(RuntimeError,'host_created_target_root_changed'):host.copy_cold_database()
   self.assertEqual(list(db.iterdir()),[]);self.assertTrue((target/'old-db').is_dir())
 def test_created_root_rejects_other_device_before_copy(self):
  core=load(CI/'harbor-pg-storage-coordinator.py','device_core');host=self.host();host.core=core;host.target_disk=lambda:None;host.j={'createdTargetRoots':{'db':[1,2,3,4,5]}}
  with patch.object(core,'identity',return_value=[9,2,3,4,5]),patch.object(Path,'lstat',return_value=None),patch.object(Path,'stat',return_value=types.SimpleNamespace(st_dev=8)):
   with self.assertRaisesRegex(RuntimeError,'host_created_target_root_changed'):host.verify_created_target_root('db')
 def test_old_boot_missing_worker_is_proven_quiet_but_same_boot_and_unknown_are_rejected(self):
  host=self.host();host.plan={'runId':'a'*32};host.planSha256='pinned';boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip();old='00000000-0000-0000-0000-000000000000';unit,intent,_=h.worker_paths(host,'switch');record={'runId':host.plan['runId'],'bootId':old,'action':'switch','nonce':None,'unit':unit,'planSha256':'pinned'};host.state_unit=lambda name,**kwargs:{'LoadState':'not-found','MainPID':'0','ActiveState':'inactive','ControlGroup':''}
  with patch.object(Path,'glob',return_value=[intent]),patch.object(h,'private',side_effect=lambda *args:json.dumps(record).encode()):
   h.workers_quiet(host);record['bootId']=boot
   with self.assertRaisesRegex(RuntimeError,'host_previous_worker_not_quiet'):h.workers_quiet(host)
   record.pop('bootId')
   with self.assertRaisesRegex(RuntimeError,'host_unknown_worker_intent'):h.workers_quiet(host)
 def test_main_lock_failure_never_saves_stale_worker_state(self):
  host=self.host();host.j={'phase':'old'};host.save=lambda _:self.fail('failed lock must not write');host.reload_locked_state=lambda:self.fail('must acquire lock before reload')
  with patch.object(h,'Host',return_value=host),patch.object(h,'require_switch_worker',return_value='worker.service'),patch.object(h,'locks',side_effect=BlockingIOError('locked')),patch.object(h.sys,'argv',['host','recover-target','a'*16]):
   with self.assertRaises(BlockingIOError):h.main()
 def test_restore_backup_uses_no_block_to_avoid_online_self_dependency(self):
  host=self.host();host.core=types.SimpleNamespace(WRITERS=('a.timer','a.service','schedule.service'));calls=[]
  with patch.object(h,'run',side_effect=lambda args:calls.append(args)):
   host.restore_writers({'a.timer':{'ActiveState':'active'},'a.service':{'ActiveState':'inactive'},'schedule.service':{'ActiveState':'active'}},True)
  self.assertEqual(calls,[['/usr/bin/systemctl','start','--no-block','schedule.service'],['/usr/bin/systemctl','start','--no-block','a.timer']])
 def test_no_raw_child_output_on_error(self):
  with patch.object(h.subprocess,'run',return_value=types.SimpleNamespace(returncode=1,stderr=b'synthetic-credential',stdout=b'synthetic-runtime-data')):
   with self.assertRaisesRegex(RuntimeError,'^host_command_failed$'):h.run(['fixed'])
 def test_actual_instance_systemd_order_with_existing_units_has_no_cycle(self):
  core=load(CI/'harbor-pg-storage-coordinator.py','graph_core');root=CI;units={p.name.removesuffix('.example'):p.read_text() for p in root.glob('harbor-entry*.service.example')};units['harbor-gateway-entry.service']=(root/'harbor-gateway-entry.service.example').read_text();generated=core.startup_units('harbor-db.mount','harbor-archive.mount')
  for name,text in generated.items():
   name=name.split('.d/')[0];units[name]=units.get(name,'')+'\n'+text
  edges=set()
  for name,text in units.items():
   for line in text.splitlines():
    if line.startswith('After='):
     edges|={(dep,name) for dep in line[6:].split()}
    if line.startswith('Before='):
     edges|={(name,dep) for dep in line[7:].split()}
  pending={a for a,b in edges}|{b for a,b in edges};done=set()
  while pending:
   ready={v for v in pending if all(a in done for a,b in edges if b==v)};self.assertTrue(ready,'actual systemd ordering cycle');pending-=ready;done|=ready
  self.assertIn(('harbor-entry-runtime.service',core.STORAGE_START),edges);self.assertIn((core.STORAGE_START,'harbor-entry-candidate.service'),edges);self.assertIn(('harbor-gateway-entry.service',core.STORAGE_ONLINE),edges)
 def test_owned_boot_gate_reuses_exact_snapshot_and_rejects_foreign_or_changed_gate(self):
  host=self.host();host.plan={'runId':'a'*32};host.j={};host.active={'image':'approved'};host.ticket={};host.config={};host.save=lambda state:None;present=False;snapshot={'owned':'gate'};created=0
  def execute(args):return json.dumps({'nftables':[{'table':{'name':'gate'}}]} if present else {'nftables':[]})
  def install(*args):
   nonlocal present,created
   present=True;created+=1;return copy.deepcopy(snapshot)
  host.entry=types.SimpleNamespace(execute=execute);host.upgrade=types.SimpleNamespace(GATE='gate',digest=lambda value:'digest',active_identity=lambda value:value,gate_install=install,gate_snapshot=lambda entry:copy.deepcopy(snapshot))
  first=host.install_gate();self.assertEqual(host.install_gate(),first);self.assertEqual(created,1)
  snapshot={'foreign':'change'}
  with self.assertRaisesRegex(RuntimeError,'host_gate_changed'):host.install_gate()
  snapshot=first;host.j['gateIntent']['runId']='foreign'
  with self.assertRaisesRegex(RuntimeError,'host_gate_not_owned'):host.install_gate()
  host.j={}
  with self.assertRaisesRegex(RuntimeError,'host_gate_not_owned'):host.install_gate()
 def test_live_parent_handoff_does_not_restart_or_reenter_controller_lock(self):
  host=self.host();core=load(CI/'harbor-pg-storage-coordinator.py','handoff_core');host.core=core;boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip();host.j={'phase':'unit-handoff-intent','handoff':{'bootId':boot,'parentPid':12,'parentTicks':45,'parentUnit':'parent.service'}};before=copy.deepcopy(host.j);calls=[]
  host.state_unit=lambda name:{'MainPID':'12' if name=='parent.service' else str(os.getpid())};host.entry=types.SimpleNamespace(process_ticks=lambda pid:45,process_cgroup=lambda pid:'/system.slice/'+core.STORAGE_START);host.data_mounts=lambda:calls.append('mounts');host.require_docker_restart_disabled=lambda:calls.append('no');host.verify_existing_entry_restored=lambda:calls.append('ready')
  self.assertTrue(host.validate_handoff('boot-start')['noContainerRestart']);self.assertEqual(calls,['mounts','no','ready']);self.assertEqual(host.j,before)
  host.entry.process_ticks=lambda pid:46
  with self.assertRaisesRegex(RuntimeError,'host_handoff_parent_changed'):host.validate_handoff('boot-start')
 def test_notify_ready_uses_real_owned_unix_datagram_and_generated_notify_contract(self):
  core=load(CI/'harbor-pg-storage-coordinator.py','notify_core');units=core.startup_units('db.mount','archive.mount');self.assertIn('Type=notify\nNotifyAccess=main\n',units[core.STORAGE_ONLINE]);host=self.host();host.core=core;host.state_unit=lambda unit:{'MainPID':str(os.getpid())};host.entry=types.SimpleNamespace(process_cgroup=lambda pid:'/system.slice/'+core.STORAGE_ONLINE)
  with tempfile.TemporaryDirectory(prefix='harbor-notify-') as directory:
   path=str(Path(directory)/'notify.sock')
   with socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM) as server:
    server.bind(path);server.settimeout(1)
    with patch.dict(os.environ,{'NOTIFY_SOCKET':path}):host.signal_online_ready()
    self.assertEqual(server.recv(100),b'READY=1')
 def test_writer_verification_checks_real_state_or_new_successful_oneshot(self):
  host=self.host();states={'schedule.service':{'ActiveState':'active','MainPID':'12'},'maintain-base.timer':{'ActiveState':'active','MainPID':'0'},'harbor-postgres-maintain-base.service':{'ActiveState':'inactive','MainPID':'0','Result':'success','ExecMainStatus':'0','InvocationID':'new'}};host.state_unit=lambda name:states[name]
  original={name:{'ActiveState':'active','InvocationID':'old'} for name in states};host.verify_writer_restoration(original)
  states['schedule.service']={'ActiveState':'failed'}
  with self.assertRaisesRegex(RuntimeError,'host_writer_restore_failed'):host.verify_writer_restoration(original)
 def test_gate_removal_retry_only_accepts_known_removed_and_still_absent(self):
  host=self.host();host.plan={'runId':'a'*32};host.j={'gate':{'owned':True},'gateIntent':{'state':'removed','bootId':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'runId':'a'*32}};host.upgrade=types.SimpleNamespace(GATE='gate');host.entry=types.SimpleNamespace(execute=lambda args:json.dumps({'nftables':[]}));host.remove_gate(host.j['gate'])
  host.entry.execute=lambda args:json.dumps({'nftables':[{'table':{'name':'gate'}}]})
  with self.assertRaisesRegex(RuntimeError,'host_removed_gate_reappeared'):host.remove_gate(host.j['gate'])
 def test_recovery_workers_have_distinct_intents_and_fixed_actions(self):
  host=self.host();host.plan={'runId':'a'*32};one=h.worker_paths(host,'recover-source','b'*16);two=h.worker_paths(host,'recover-target','c'*16);self.assertNotEqual(one[0],two[0]);self.assertNotEqual(one[1],two[1]);self.assertNotEqual(one[0],h.worker_paths(host,'switch')[0])
  for action,nonce in [('unknown','a'*16),('recover-source','../../bad'),('switch','a'*16)]:
   with self.assertRaises(RuntimeError):h.worker_paths(host,action,nonce)
 def test_disabling_guard_after_arming_never_downgrades_to_unless_stopped(self):
  current={'scope':'harbor-pg-storage-move-v1','phase':'complete','oldDataRollbackForbidden':True,'targetBootAuthorized':True}
  class GuardState:
   parents=[]
   def lstat(self):return types.SimpleNamespace(st_uid=0,st_mode=0o100600,st_nlink=1,st_size=100)
   stat=lstat
   def read_bytes(self):return json.dumps(current).encode()
  with patch.object(factory,'Path',return_value=GuardState()),patch.object(factory.os.path,'lexists',return_value=True):
   with self.assertRaisesRegex(ValueError,'still requires'):factory.storage_restart_policy({'postgres_storage_startup_guard':False})
   current.update(phase='source-restored',oldDataRollbackForbidden=False,targetBootAuthorized=False);self.assertEqual(factory.storage_restart_policy({}),'unless-stopped');current['oldDataRollbackForbidden']=1
   with self.assertRaisesRegex(ValueError,'flags are invalid'):factory.storage_restart_policy({})
 def test_integrated_factory_uses_guarded_restart_choice(self):
  tree=ast.parse((CI/'postgres-deploy.py').read_text());deploy=next(node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='deploy');calls=[node for node in ast.walk(deploy) if isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and node.func.id=='storage_restart_policy'];self.assertEqual(len(calls),1);self.assertEqual(factory.STORAGE_STARTUP_CONTRACT,'harbor-pg-storage-boot-v1')
 def test_restart_decision_precedes_any_deploy_side_effect(self):
  tree=ast.parse((CI/'postgres-deploy.py').read_text());node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='deploy');self.assertEqual(node.body[0].targets[0].id,'restart_policy');self.assertEqual(node.body[0].value.func.id,'storage_restart_policy')
if __name__=='__main__':unittest.main()
