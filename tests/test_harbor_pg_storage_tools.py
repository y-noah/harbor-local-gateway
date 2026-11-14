import ast,copy,importlib.util,json,os,selectors,signal,stat,subprocess,sys,tempfile,time,types,unittest,uuid
from pathlib import Path
from unittest.mock import patch,Mock
CI=Path(__file__).resolve().parents[1]/'ci'
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
def line_before_timeout(stream):
 with selectors.DefaultSelector() as selector:
  selector.register(stream,selectors.EVENT_READ)
  if not selector.select(timeout=5):raise AssertionError('owned child readiness timed out')
  return stream.readline()

h=load('storage_tools_host',CI/'harbor-pg-storage-host.py')
b=load('storage_host_bridge',CI/'storage-recovery/bridge-pg-storage-host.py')
HOST_CLASS=h.Host

class Tools(unittest.TestCase):
 def fixture(self):
  self.plan={'scope':'harbor-pg-storage-move-v1','runId':'a'*32,'systemId':'123','materialsDirectory':'/materials','containers':{h.PG:{'id':'p'}},'toolSha256':{str(Path(h.__file__)):'a'*64,str(h.MATERIAL):h.sha(b'material')}}
  self.j={'scope':self.plan['scope'],'runId':self.plan['runId'],'phase':'complete','oldDataRollbackForbidden':True,'targetBootAuthorized':True}
  old={'contract':'harbor-postgres-runtime-upgrade-v1','deploymentId':str(uuid.uuid4()),'to':{'image':'approved:old','revision':'b'*40,'configDigest':'sha256:'+'1'*64,'manifestDigest':'sha256:'+'2'*64},'database':{'cluster':'123'},'materialsDirectory':'/materials'}
  digest=h.object_digest(old);auth={'contract':old['contract'],'deploymentId':old['deploymentId'],'receiptDigest':digest,**{k:old['to'][k] for k in ['revision','configDigest','manifestDigest']}}
  self.active={'version':1,'image':'approved:old','approval':auth,'candidate':{'containerId':'g','imageId':old['to']['configDigest'],'port':18092}}
  self.record={'contract':old['contract'],'phase':'complete','ticket':old,'ticketDigest':digest,'workerId':'w','candidate':copy.deepcopy(self.active['candidate'])}
  identity={k:copy.deepcopy(self.active.get(k)) for k in ['version','image','approval','configDigest','mounts','storageMarkerDigest','storageMarkerPath']}|{'candidate':{k:self.active['candidate'].get(k) for k in ['containerId','imageId','port']}}
  self.ticket={'version':1,'contract':old['contract'],'approved':True,'hostToolsRevision':'c'*40,'from':{'activeIdentityDigest':h.object_digest(identity),'containerId':'g','workerId':'w','imageId':old['to']['configDigest'],'image':'approved:old'},'to':{'image':'approved:new'},'database':{'cluster':'123'},'materialsDirectory':'/materials'}
  self.bytes={name:name.encode() for name in h.RUNTIME_TOOLS};self.bytes[str(h.MATERIAL)]=b'material'
  self.manifest={'version':1,'contract':'harbor-postgres-runtime-tools-v1','revision':'c'*40,'files':{name:h.sha(raw) for name,raw in self.bytes.items()}}
 def private(self,path,*args):
  if path==h.RUNTIME_MANIFEST:return json.dumps(self.manifest).encode()
  if path==h.RUNTIME_TICKET:return json.dumps(self.ticket).encode()
  if str(path)=='/var/lib/harbor-entry/active.json':return json.dumps(self.active).encode()
  if str(path).endswith('/release.json'):return json.dumps(self.record).encode()
  raise AssertionError(path)
 def docker(self,args):
  role='gateway' if args[-1]==h.GW else 'worker';identifier='g' if role=='gateway' else 'w';auth=self.active['approval']
  return json.dumps([{'Id':identifier,'Image':self.active['candidate']['imageId'],'Config':{'Image':self.active['image'],'Cmd':['node','/app/postgres-'+role+'.mjs'],'Labels':{'com.aistrong.project':h.GW,'com.harbor.activation-deployment-id':auth['deploymentId'],'com.harbor.activation-receipt':auth['receiptDigest']}}}])
 def verify(self):
  with patch.object(h,'private',side_effect=self.private),patch.object(h,'code_bytes',side_effect=lambda name:self.bytes[str(name)]),patch.object(h,'run',side_effect=self.docker):return h.verified_runtime_tools(self.plan,self.j)
 def test_approved_source_manifest_before_import(self):
  self.fixture()
  with patch.object(h,'load',side_effect=AssertionError('imports forbidden during qualification')):self.assertEqual(self.verify(),{k:self.manifest['files'][k] for k in h.MUTABLE_TOOLS})
 def test_approved_completed_target(self):
  self.fixture();self.ticket=copy.deepcopy(self.record['ticket']);self.ticket.update(version=1,approved=True,hostToolsRevision='c'*40);self.record['ticket']=copy.deepcopy(self.ticket);digest=h.object_digest(self.ticket);self.record['ticketDigest']=digest;self.active['approval']['receiptDigest']=digest
  self.verify()
 def test_pending_recovery_survives_runtime_ready_and_pause_without_operation(self):
  self.fixture();self.ticket=copy.deepcopy(self.record['ticket']);self.ticket.update(version=1,approved=True,hostToolsRevision='c'*40);self.record['ticket']=copy.deepcopy(self.ticket);digest=h.object_digest(self.ticket);self.record['ticketDigest']=digest;self.record['phase']='committed';self.active['approval']['receiptDigest']=digest
  identity={k:copy.deepcopy(self.active.get(k)) for k in ['version','image','approval','configDigest','mounts','storageMarkerDigest','storageMarkerPath']}|{'candidate':{k:self.active['candidate'].get(k) for k in ['containerId','imageId','port']}};binding=h.object_digest(identity)
  self.j.update(runtimeControl={'mode':'recovering','selectedActiveDigest':binding},plannedOperation={'nonce':'original','targetTicketDigest':digest})
  self.verify()
  host=object.__new__(HOST_CLASS);host.j=self.j;host.core=types.SimpleNamespace(runtime_mode=lambda j:j['runtimeControl']['mode']);host.entry=types.SimpleNamespace(ACTIVE='active',private_json=lambda _:self.active);host.upgrade=types.SimpleNamespace(digest=h.object_digest,active_identity=lambda active:identity,record_for=lambda auth:self.record);host.runtime_generation=lambda:{};host.save=lambda journal:None
  host.record_runtime_ready();self.assertNotIn('plannedOperation',self.j);self.verify();self.verify()
  self.j['runtimeControl']['mode']='paused';self.verify();self.j['runtimeControl']['mode']='recovering';self.verify()
  self.j['runtimeControl']['selectedActiveDigest']='0'*64
  with self.assertRaisesRegex(RuntimeError,'record_unapproved'):self.verify()
 def test_manifest_ticket_source_and_bytes_tampering_rejected(self):
  for field in ['revision','inventory','bytes','material','from','record','pair']:
   with self.subTest(field=field):
    self.fixture()
    if field=='revision':self.manifest['revision']='d'*40
    elif field=='inventory':self.manifest['files']['/unapproved']=h.sha(b'bad')
    elif field=='bytes':self.bytes[str(h.UPGRADE)]=b'unapproved root bytes'
    elif field=='material':self.bytes[str(h.MATERIAL)]=b'changed';self.manifest['files'][str(h.MATERIAL)]=h.sha(b'changed')
    elif field=='from':self.ticket['from']['workerId']='unknown'
    elif field=='record':self.record['ticketDigest']='0'*64
    else:self.active['candidate']['imageId']='unknown'
    with self.assertRaises((RuntimeError,KeyError)):self.verify()
 def test_target_candidate_and_config_digest_mismatch_rejected_before_import(self):
  for mismatch in ['recorded-candidate','unapproved-image']:
   self.fixture();self.ticket=copy.deepcopy(self.record['ticket']);self.ticket.update(version=1,approved=True,hostToolsRevision='c'*40);self.record['ticket']=copy.deepcopy(self.ticket);digest=h.object_digest(self.ticket);self.record['ticketDigest']=digest;self.active['approval']['receiptDigest']=digest
   if mismatch=='recorded-candidate':self.record['candidate']['containerId']='unapproved-pair'
   else:
    self.active['candidate']['imageId']='sha256:'+'9'*64;self.record['candidate']['imageId']=self.active['candidate']['imageId']
   with patch.object(h,'load',side_effect=AssertionError('must reject before import')) as imports:
    with self.assertRaisesRegex(RuntimeError,'candidate_unapproved'):self.verify()
    imports.assert_not_called()
 def test_incomplete_never_uses_manifest_without_bridge(self):
  self.fixture()
  for phase in ['planned','target-start-intent','unit-handoff-intent','boot-containers-ready']:
   self.j['phase']=phase
   with patch.object(h,'private',side_effect=FileNotFoundError),self.assertRaises(FileNotFoundError):h.verified_runtime_tools(self.plan,self.j)
 def bridge_fixture(self):
  self.fixture();self.approval={'version':1,'approved':True,'id':str(uuid.uuid4()),'runId':self.plan['runId'],'planSha256':'d'*64,'oldHostSha256':'a'*64,'newHostSha256':'e'*64,'installerSha256':'f'*64,'revision':'b'*40,'pipeline':123,'artifactJob':456}
  self.approval_raw=json.dumps(self.approval).encode();self.roots={'db':[1,2,999],'archive':[1,3,999]};self.receipt=b.receipt_for(self.approval,h.sha(self.approval_raw),self.roots)
 def check_bridge(self):
  with patch.object(h,'private',side_effect=lambda path,*args:json.dumps(self.receipt).encode() if path==h.HOST_GENERATION else self.approval_raw),patch.object(h,'bridge_roots',return_value=self.roots):return h.verify_host_bridge(self.plan,'d'*64,self.j,'e'*64)
 def test_bridge_reboot_state_flow_requires_completed_receipt(self):
  self.bridge_fixture()
  for phase in ['complete','boot-start-intent','boot-containers-ready','complete','runtime-recovering','runtime-entry-pending','complete']:
   self.j['phase']=phase;self.check_bridge()
  for phase in ['planned','archive-prepared','target-start-intent','unit-handoff-intent','boot-installed','runtime-invented']:
   self.j['phase']=phase
   with self.assertRaises(RuntimeError):self.check_bridge()
 def test_bridge_approval_roots_scope_or_permanent_rollback_tamper_rejected(self):
  for field in ['roots','sha','run','rollback','completed','approval']:
   with self.subTest(field=field):
    self.bridge_fixture()
    if field=='roots':self.receipt['roots']={}
    elif field=='sha':self.receipt['binding']['newHostSha256']='0'*64
    elif field=='run':self.j['runId']='0'*32
    elif field=='rollback':self.j['oldDataRollbackForbidden']=False
    elif field=='completed':self.receipt['migrationCompleted']=False
    else:self.approval_raw=json.dumps(dict(self.approval,approved=False)).encode()
    with self.assertRaises(RuntimeError):self.check_bridge()
 def test_material_completion_uses_approved_guards_without_changing_mover_api(self):
  host=object.__new__(HOST_CLASS);self.fixture();host.plan=self.plan;host.j=self.j;host.runtimeTools={str(h.COMPOSE):h.sha(b'new-compose')};selected={'nonce':str(uuid.uuid4()),'approvalSha256':''};value={'nonce':selected['nonce'],'materialsDirectory':'/materials','diskUuid':'disk','containers':{h.GW:'g',h.WORKER:'w'},'guards':{str(h.COMPOSE):h.sha(b'old-compose')},'tools':{str(h.MATERIAL):h.sha(b'material'),str(h.MATERIAL_MOVE):h.sha(b'move')}};raw=json.dumps(value).encode();selected['approvalSha256']=h.sha(raw);host.plan.update(materialMove=selected,targetFilesystemUuid='disk',containers={h.GW:{'id':'g'},h.WORKER:{'id':'w'}},toolSha256={str(h.MATERIAL):h.sha(b'material'),str(h.MATERIAL_MOVE):h.sha(b'move')});host.j['materialMove']=dict(selected,phase='complete')
  state=dict(selected,phase='complete');cfg={'roots':'verified'};guard=Mock();default=Mock(side_effect=AssertionError('strict migration API not used after complete'));g=types.SimpleNamespace(private_json=lambda path,*args:state if path=='state' else cfg,CONFIG='config',TARGETS={'data':'target'},guard=guard)
  mover=types.SimpleNamespace(APPROVAL='approval',STATE='state',GUARD_FILES={str(h.COMPOSE):b'marker'},g=g,guard_code=lambda path:b'new-compose',final_configuration=lambda *args:cfg,approval=default)
  with patch.object(h,'load',return_value=mover),patch.object(h,'private',return_value=raw),patch.object(h,'code_bytes',side_effect=lambda path:b'material' if str(path)==str(h.MATERIAL) else b'move'):
   # material_mover's fixed outer pin reads the exact two tools.
   with patch.object(h,'sha',side_effect=lambda payload: h.hashlib.sha256(payload).hexdigest()),patch.object(Path,'read_bytes',autospec=True,side_effect=lambda path:b'material' if path==h.MATERIAL else b'move'):
    host.material_mover();default.assert_not_called();guard.assert_called()
    host.runtimeTools[str(h.COMPOSE)]=h.sha(b'other')
    with self.assertRaises(RuntimeError):host.material_mover()

class HistoricalMaterialBoundary(unittest.TestCase):
 def test_stopped_apps_allow_protected_host_binding_but_not_ready_probe(self):
  selected={'nonce':'nonce','approvalSha256':'proof'};cfg={'roots':{'application-data':{'logical':'/materials/pg-data'},'application-runtime':{'logical':'/materials/runtime'}}};state=dict(selected,phase='complete');guard=Mock();g=types.SimpleNamespace(CONFIG='config',TARGETS={'application-data':'data','application-runtime':'runtime'},private_json=lambda path,*args:state if path=='state' else cfg,guard=guard)
  mover=types.SimpleNamespace(STATE='state',g=g,final_configuration=lambda value,current:cfg)
  host=object.__new__(HOST_CLASS);host.plan={'materialMove':selected,'materialsDirectory':'/materials'};host.j={'phase':'runtime-recovering','runtimeControl':{'mode':'recovering'},'materialMove':dict(selected,phase='complete')};host.active={'approval':'original'};host.upgrade=types.SimpleNamespace(digest=h.object_digest,active_identity=lambda active:active)
  host.material_mover=lambda:(mover,{'nonce':'nonce'},[]);host.load_current_durable_active=lambda:None;host.data_mounts=lambda:None;host.immutable=lambda name:None;host.require_docker_restart_disabled=lambda:None;host.verify_installed_unit_files=lambda:None;host.row=lambda name:{'Id':name,'Image':'approved','Config':{},'HostConfig':{},'Mounts':[],'State':{'Running':False}};host.gw=host.row(h.GW);host.worker=host.row(h.WORKER);old=types.SimpleNamespace(PG=h.PG,workers_quiet=lambda host:None)
  self.assertEqual(b.source_identity(old,host),h.object_digest(host.active));guard.assert_called()
  for mode in ['recovering','paused']:
   host.j['runtimeControl']['mode']=mode;self.assertEqual(b.source_identity(old,host),h.object_digest(host.active))
  state['phase']='copying'
  with self.assertRaisesRegex(RuntimeError,'material_completion_changed'):b.source_identity(old,host)
  state['phase']='complete';guard.side_effect=RuntimeError('actual_mount_changed')
  with self.assertRaisesRegex(RuntimeError,'actual_mount_changed'):b.source_identity(old,host)
  guard.side_effect=None;host.j.update(phase='complete',runtimeControl={'mode':'running'})
  with patch.object(Path,'stat',return_value=types.SimpleNamespace(st_dev=1,st_ino=2)),patch.object(h,'run',side_effect=AssertionError('stopped container cannot exec')):
   with self.assertRaisesRegex(RuntimeError,'host_material_application_not_running'):b.source_identity(old,host)

class BridgeFiles(unittest.TestCase):
 def read(self,path,*args):
  p=Path(path);info=p.lstat()
  if not stat.S_ISREG(info.st_mode):raise RuntimeError('untrusted')
  return p.read_bytes()
 def test_real_publish_no_unknown_overwrite_and_reversible_exact_bytes(self):
  with tempfile.TemporaryDirectory() as td,patch.object(b,'read',side_effect=self.read):
   p=Path(td)/'host';b.publish(p,b'old',0o644);old_stat=p.stat();self.assertEqual(p.read_bytes(),b'old');self.assertEqual(stat.S_IMODE(old_stat.st_mode),0o644)
   with self.assertRaises(RuntimeError):b.publish(p,b'unknown')
   b.publish(p,b'new',0o644,b.sha(b'old'));self.assertEqual(p.read_bytes(),b'new');b.publish(p,b'old',0o644,b.sha(b'new'));self.assertEqual(p.read_bytes(),b'old')
   p.unlink();p.symlink_to(Path(td)/'missing')
   with self.assertRaises(RuntimeError):b.publish(p,b'new')
   self.assertTrue(p.is_symlink())
 def test_link_ack_loss_preserves_published_receipt(self):
  with tempfile.TemporaryDirectory() as td,patch.object(b,'read',side_effect=self.read):
   p=Path(td)/'receipt'
   with patch.object(b,'sync',side_effect=OSError('simulated fsync ACK loss')):
    with self.assertRaises(OSError):b.publish(p,b'approved')
   self.assertEqual(p.read_bytes(),b'approved');self.assertEqual(list(Path(td).iterdir()),[p])
 def test_process_exit_after_noreplace_rename_keeps_single_link_owned_file(self):
  with tempfile.TemporaryDirectory() as td:
   target=Path(td)/'receipt';pid=os.fork()
   if pid==0:
    rename=b.rename_noreplace
    def exit_after_publish(source,destination):rename(source,destination);os._exit(23)
    b.rename_noreplace=exit_after_publish
    try:b.publish(target,b'approved')
    except BaseException:os._exit(24)
    os._exit(25)
   deadline=time.monotonic()+5;waited,status=os.waitpid(pid,os.WNOHANG)
   while waited==0 and time.monotonic()<deadline:time.sleep(0.01);waited,status=os.waitpid(pid,os.WNOHANG)
   if waited==0:os.kill(pid,signal.SIGKILL);os.waitpid(pid,0);self.fail('owned publish child did not exit')
   self.assertEqual(os.waitstatus_to_exitcode(status),23);self.assertEqual(target.read_bytes(),b'approved');self.assertEqual(target.stat().st_nlink,1);self.assertEqual(list(Path(td).iterdir()),[target])
   with patch.object(b,'read',side_effect=self.read):b.publish(target,b'restored',expected=b.sha(b'approved'))
   self.assertEqual(target.read_bytes(),b'restored')
 def test_writer_restore_only_original_active_dependency(self):
  snapshots={name:{'ActiveState':'active' if name==b.WRITERS[0] else 'inactive'} for name in b.WRITERS};states={name:{'ActiveState':'inactive'} for name in b.WRITERS};calls=[]
  def command(args):calls.append(args);states[args[-1]]={'ActiveState':'active'}
  with patch.object(b,'unit',side_effect=lambda name:states[name]),patch.object(b,'command',side_effect=command):b.restore_writers({'writers':snapshots})
  self.assertEqual(calls,[['/usr/bin/systemctl','start','--no-block',b.WRITERS[0]]])

class ProcessBoundary(unittest.TestCase):
 def test_final_lock_wait_retries_real_ready_before_unlock_contention(self):
  with tempfile.TemporaryDirectory() as td:
   path=Path(td)/'lock';path.touch(mode=0o600)
   code="import fcntl,sys; f=open(sys.argv[1],'r+'); fcntl.flock(f,fcntl.LOCK_EX); print('ready',flush=True); sys.stdin.readline(); fcntl.flock(f,fcntl.LOCK_UN); print('released',flush=True)"
   child=subprocess.Popen([sys.executable,'-c',code,str(path)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
   real_fstat=os.fstat;released=[]
   def metadata(fd):
    value=real_fstat(fd);return types.SimpleNamespace(st_mode=value.st_mode,st_uid=0,st_nlink=value.st_nlink)
   def release(seconds):
    if not released:child.stdin.write('release\n');child.stdin.flush();self.assertEqual(line_before_timeout(child.stdout).strip(),'released');released.append(True)
   try:
    self.assertEqual(line_before_timeout(child.stdout).strip(),'ready')
    with patch.object(b,'LOCK_PATHS',(str(path),)),patch.object(os,'fstat',side_effect=metadata),patch.object(b.time,'sleep',side_effect=release):
     with self.assertRaises(BlockingIOError):
      with b.locks():pass
     with b.locks(wait=True):self.assertEqual(released,[True])
   finally:child.stdin.close();child.wait(timeout=5);child.stdout.close()
 def test_real_owned_process_generation_observation(self):
  child=subprocess.Popen([sys.executable,'-c',"import sys; print('ready',flush=True); sys.stdin.read()"],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
  try:
   self.assertEqual(line_before_timeout(child.stdout).strip(),'ready');before=b.process_generation(child.pid);self.assertTrue(before.isdigit());self.assertEqual(b.process_generation(child.pid),before)
  finally:
   child.stdin.close();child.wait(timeout=5);child.stdout.close()
  self.assertFalse((Path('/proc')/str(child.pid)).exists())
 def test_supervisor_generation_change_protects_without_implicit_exit_or_work(self):
  callbacks={};step=Mock();host=types.SimpleNamespace(j={'phase':'complete'},runtimeTools={'old':'sha'},plan={},reload_locked_state=lambda:None,signal_online_ready=lambda:None,runtime_step=step,poll_interval_seconds=1)
  with patch.object(h.signal,'signal',side_effect=lambda number,handler:callbacks.update({number:handler})),patch.object(h,'locks',return_value=__import__('contextlib').nullcontext()),patch.object(h,'verified_runtime_tools',return_value={'new':'sha'}),patch.object(h.time,'sleep',side_effect=lambda seconds:callbacks[h.signal.SIGTERM]()),patch('builtins.print'):
   self.assertEqual(h.supervise_online(host),{'supervisorStopped':True})
  step.assert_not_called()

class BridgeLifecycle(unittest.TestCase):
 def setUp(self):
  self.actual_source_identity=b.source_identity;self.tmp=tempfile.TemporaryDirectory();self.base=Path(self.tmp.name);self.host=self.base/'host.py';self.host.write_bytes(b'# original host\n');self.plan=self.base/'plan.json';self.state=self.base/'state.json';self.receipt=self.base/'receipt.json';self.bridge_root=self.base/'bridge';self.identifier=str(uuid.uuid4());self.directory=self.bridge_root/self.identifier;self.directory.mkdir(parents=True);self.candidate=self.directory/'candidate.py';self.candidate.write_bytes(b'# approved new host\n')
  self.approval={'version':1,'approved':True,'id':self.identifier,'planSha256':'','runId':'a'*32,'oldHostSha256':b.sha(self.host.read_bytes()),'newHostSha256':b.sha(self.candidate.read_bytes()),'installerSha256':'f'*64,'revision':'b'*40,'pipeline':12,'artifactJob':34}
  p={'scope':'harbor-pg-storage-move-v1','approved':True,'runId':'a'*32,'toolSha256':{str(self.host):self.approval['oldHostSha256']}};self.plan.write_text(json.dumps(p));self.approval['planSha256']=b.sha(self.plan.read_bytes());self.plan_bytes=self.plan.read_bytes()
  self.state.write_text(json.dumps({'scope':p['scope'],'runId':p['runId'],'phase':'complete','oldDataRollbackForbidden':True,'targetBootAuthorized':True}))
  self.online={'LoadState':'loaded','ActiveState':'active','SubState':'running','MainPID':'999999999','InvocationID':'original','ControlGroup':'/system.slice/'+b.ONLINE,'ExecStart':str(self.host),'processStartTicks':'123'}
  self.states={b.ONLINE:dict(self.online),**{name:{'LoadState':'loaded','ActiveState':'active' if name.endswith('.timer') else 'inactive','MainPID':'0','ControlGroup':''} for name in b.WRITERS}};self.fail_start=False;self.generation=0
  self.patches=[patch.object(b,'HOST',self.host),patch.object(b,'PLAN',self.plan),patch.object(b,'STATE',self.state),patch.object(b,'RECEIPT',self.receipt),patch.object(b,'ROOT',self.bridge_root),patch.object(b,'read_approval',return_value=(self.approval,'d'*64)),patch.object(b,'read',side_effect=self.read),patch.object(b,'locks',return_value=__import__('contextlib').nullcontext()),patch.object(b,'bridge_roots',return_value={'db':[1,2]}),patch.object(b,'original_host',return_value=types.SimpleNamespace(Host=self.current_host,verify_factory_guard=self.factory_guard)),patch.object(b,'source_identity',return_value='active'),patch.object(h,'code_bytes',return_value=b'approved-host'),patch.object(b,'supervisor',side_effect=lambda:dict(self.states[b.ONLINE])),patch.object(b,'unit',side_effect=lambda name:dict(self.states[name])),patch.object(b,'command',side_effect=self.command),patch.object(os,'geteuid',return_value=0),patch.dict(os.environ,dict(os.environ))]
  for p in self.patches:p.start()
 def current_host(self):
  host=object.__new__(HOST_CLASS);host.config={'postgres_storage_startup_guard':True};host.j=json.loads(self.state.read_bytes());host.runtimeTools={'/approved-tool':'digest'};host.core=types.SimpleNamespace(STORAGE_START='harbor-pg-storage-start.service',STORAGE_ONLINE=b.ONLINE)
  host.data_mounts=lambda:None;host.immutable=lambda name:None;host.require_docker_restart_disabled=lambda:None;host.verify_installed_unit_files=lambda:None;host.row=lambda name:{'HostConfig':{'RestartPolicy':{'Name':'no'}}}
  host.state_unit=lambda name:dict(self.states[b.ONLINE],UnitFileState='enabled') if name==b.ONLINE else {'UnitFileState':'enabled'}
  return host
 def factory_guard(self,config):
  with patch.object(h,'Host',side_effect=self.current_host):return h.verify_factory_guard(config)
 def tearDown(self):
  for p in reversed(self.patches):p.stop()
  self.tmp.cleanup()
 def read(self,path,*args):
  path=Path(path);info=path.lstat()
  if not stat.S_ISREG(info.st_mode):raise RuntimeError('untrusted fixture file')
  return path.read_bytes()
 def command(self,args,**kwargs):
  if args[1]=='stop':
   for name in self.states:self.states[name].update(ActiveState='inactive',MainPID='0',ControlGroup='')
  elif args[1]=='start':
   if self.fail_start:raise RuntimeError('host_bridge_unit_command_failed')
   name=args[-1];self.generation+=1
   self.states[name].update(ActiveState='active',SubState='running',MainPID='999999998' if name==b.ONLINE else '0',InvocationID='new-'+str(self.generation))
   if name==b.ONLINE:
    state=json.loads(self.state.read_bytes());state['supervisorTools']={'files':{'/approved-tool':'digest'},'hostSha256':h.sha(b'approved-host'),'pid':int(self.states[name]['MainPID']),'bootId':Path('/proc/sys/kernel/random/boot_id').read_text().strip()};self.state.write_text(json.dumps(state))
  return b''
 def test_install_refresh_and_rollback_preserve_plan_and_old_intents(self):
  intent=self.base/'historical-intent.json';intent.write_text(json.dumps({'planSha256':self.approval['planSha256']}));before=intent.read_bytes()
  self.assertTrue(b.bridge('inspect')['ready']);self.assertFalse((self.directory/'journal.json').exists())
  self.assertTrue(b.bridge('install')['installed']);self.assertEqual(self.host.read_bytes(),self.candidate.read_bytes());self.assertTrue(self.receipt.exists())
  self.assertTrue(b.bridge('refresh')['refreshed']);self.assertEqual(self.states[b.WRITERS[0]]['ActiveState'],'active')
  self.assertTrue(b.bridge('rollback')['rolledBack']);self.assertEqual(b.sha(self.host.read_bytes()),self.approval['oldHostSha256']);self.assertFalse(self.receipt.exists());self.assertEqual(self.plan.read_bytes(),self.plan_bytes);self.assertEqual(intent.read_bytes(),before)
 def test_failed_start_explicit_rollback_uses_preserved_ownership(self):
  self.fail_start=True
  with self.assertRaisesRegex(RuntimeError,'unit_command_failed'):b.bridge('install')
  self.assertEqual(json.loads((self.directory/'journal.json').read_bytes())['phase'],'starting');self.assertTrue(self.receipt.exists());self.fail_start=False
  self.assertTrue(b.bridge('rollback')['rolledBack']);self.assertFalse(self.receipt.exists());self.assertEqual(b.sha(self.host.read_bytes()),self.approval['oldHostSha256'])
 def test_unfinished_migration_rejected_without_touching_host_or_units(self):
  j=json.loads(self.state.read_bytes());j['phase']='target-start-intent';self.state.write_text(json.dumps(j))
  with self.assertRaisesRegex(RuntimeError,'move_not_complete'):b.bridge('install')
  self.assertEqual(self.states[b.ONLINE]['InvocationID'],'original');self.assertFalse((self.directory/'journal.json').exists());self.assertFalse(self.receipt.exists())
 def test_refresh_failed_start_resumes_original_writer_snapshot(self):
  b.bridge('install');self.fail_start=True
  with self.assertRaisesRegex(RuntimeError,'unit_command_failed'):b.bridge('refresh')
  saved=json.loads((self.directory/'journal.json').read_bytes());self.assertEqual(saved['operation'],'refresh');self.assertEqual(saved['phase'],'starting');self.assertEqual(saved['writers'][b.WRITERS[0]]['ActiveState'],'active')
  self.fail_start=False;self.assertTrue(b.bridge('refresh')['refreshed']);self.assertEqual(self.states[b.WRITERS[0]]['ActiveState'],'active');self.assertEqual(self.plan.read_bytes(),self.plan_bytes)
 def test_refresh_complete_journal_ack_loss_can_resume_live_new_supervisor(self):
  b.bridge('install');original=b.save_journal
  def fail_complete(path,journal):
   if journal['operation']=='refresh' and journal['phase']=='complete':raise OSError('before completed journal publication')
   return original(path,journal)
  with patch.object(b,'save_journal',side_effect=fail_complete),self.assertRaises(OSError):b.bridge('refresh')
  self.assertEqual(json.loads((self.directory/'journal.json').read_bytes())['phase'],'starting');self.assertEqual(self.states[b.ONLINE]['ActiveState'],'active');self.assertTrue(b.bridge('refresh')['refreshed']);self.assertEqual(self.plan.read_bytes(),self.plan_bytes)
 def test_published_recovery_failure_can_rollback_host_without_resuming_writers(self):
  original=self.command;once=[]
  def recover_then_fail(args,**kwargs):
   result=original(args,**kwargs)
   if args[1]=='start' and args[-1]==b.ONLINE and not once:
    once.append(True);state=json.loads(self.state.read_bytes());state.update(phase='runtime-recovering',runtimeControl={'mode':'recovering'});self.state.write_text(json.dumps(state));raise RuntimeError('host_bridge_unit_command_failed')
   return result
  with patch.object(b,'command',side_effect=recover_then_fail),self.assertRaisesRegex(RuntimeError,'unit_command_failed'):b.bridge('install')
  result=b.bridge('rollback');self.assertTrue(result['rolledBack']);self.assertTrue(result['protectionRetained']);self.assertFalse(result['writersRestored']);self.assertFalse(self.receipt.exists());self.assertEqual(self.states[b.WRITERS[0]]['ActiveState'],'inactive');self.assertEqual(b.sha(self.host.read_bytes()),self.approval['oldHostSha256'])
  state=json.loads(self.state.read_bytes());state.update(phase='complete',runtimeControl={'mode':'running'});self.state.write_text(json.dumps(state))
  with patch.object(b,'command',side_effect=self.command) as commands:result=b.bridge('rollback')
  self.assertTrue(result['writersRestored']);self.assertFalse(result['protectionRetained']);self.assertEqual(self.states[b.WRITERS[0]]['ActiveState'],'active');self.assertFalse(any(call.args[0][-1]==b.ONLINE for call in commands.call_args_list));self.assertEqual(self.plan.read_bytes(),self.plan_bytes)
 def test_install_protected_state_can_finish_without_restarting_recovering_guard(self):
  original=self.command
  def recovering_start(args,**kwargs):
   result=original(args,**kwargs)
   if args[1]=='start' and args[-1]==b.ONLINE:
    state=json.loads(self.state.read_bytes());state.update(phase='runtime-entry-pending',runtimeControl={'mode':'recovering'});self.state.write_text(json.dumps(state))
   return result
  with patch.object(b,'command',side_effect=recovering_start):result=b.bridge('install')
  self.assertTrue(result['recoveryPending']);self.assertFalse(result['writersRestored']);self.assertEqual(json.loads((self.directory/'journal.json').read_bytes())['phase'],'protected')
  state=json.loads(self.state.read_bytes());state.update(phase='complete',runtimeControl={'mode':'running'});self.state.write_text(json.dumps(state))
  with patch.object(b,'command',side_effect=self.command) as commands:result=b.bridge('install')
  self.assertTrue(result['writersRestored']);self.assertFalse(any(call.args[0][-1]==b.ONLINE for call in commands.call_args_list))
 def test_no_completed_journal_or_receipt_cannot_adopt_recovery_phase(self):
  for phase in ['runtime-recovering','runtime-entry-pending','boot-containers-ready']:
   state=json.loads(self.state.read_bytes());state['phase']=phase;self.state.write_text(json.dumps(state))
   with self.assertRaisesRegex(RuntimeError,'move_not_complete'):b.bridge('install')
   with self.assertRaisesRegex(RuntimeError,'move_not_complete'):b.bridge('rollback')
   self.assertFalse((self.directory/'journal.json').exists());self.assertFalse(self.receipt.exists())
 def test_refresh_recovery_failure_retains_snapshot_and_can_finish(self):
  b.bridge('install');original=self.command;once=[]
  def recover_then_fail(args,**kwargs):
   result=original(args,**kwargs)
   if args[1]=='start' and args[-1]==b.ONLINE and not once:
    once.append(True);state=json.loads(self.state.read_bytes());state.update(phase='runtime-entry-pending',runtimeControl={'mode':'recovering'});self.state.write_text(json.dumps(state));raise RuntimeError('host_bridge_unit_command_failed')
   return result
  with patch.object(b,'command',side_effect=recover_then_fail),self.assertRaises(RuntimeError):b.bridge('refresh')
  with self.assertRaisesRegex(RuntimeError,'host_factory_move_not_complete'):self.factory_guard({'postgres_storage_startup_guard':True})
  result=b.bridge('refresh');self.assertTrue(result['recoveryPending']);self.assertEqual(self.states[b.WRITERS[0]]['ActiveState'],'inactive')
  state=json.loads(self.state.read_bytes());state.update(phase='complete',runtimeControl={'mode':'running'});self.state.write_text(json.dumps(state));result=b.bridge('refresh');self.assertTrue(result['writersRestored']);self.assertEqual(self.states[b.WRITERS[0]]['ActiveState'],'active')
 def test_historical_rollback_still_rejects_unowned_journal_and_migration_phase(self):
  b.bridge('install');state=json.loads(self.state.read_bytes());state.update(phase='runtime-recovering',runtimeControl={'mode':'recovering'});self.state.write_text(json.dumps(state));jp=self.directory/'journal.json';journal=json.loads(jp.read_bytes());original=self.host.read_bytes()
  journal['migrationCompleted']=False;jp.write_text(json.dumps(journal))
  with self.assertRaisesRegex(RuntimeError,'journal_unowned'):b.bridge('rollback')
  self.assertEqual(self.host.read_bytes(),original);self.assertTrue(self.receipt.exists())
  journal['migrationCompleted']=True;jp.write_text(json.dumps(journal));state['phase']='target-start-intent';self.state.write_text(json.dumps(state))
  with self.assertRaisesRegex(RuntimeError,'move_not_complete'):b.bridge('rollback')
  self.assertEqual(self.host.read_bytes(),original);self.assertTrue(self.receipt.exists())
 def material_stopped_pair_operation(self,action):
  selected={'nonce':'nonce','approvalSha256':'proof'};plan=json.loads(self.plan.read_bytes());plan.update(materialMove=selected,materialsDirectory='/materials');self.plan.write_text(json.dumps(plan));self.plan_bytes=self.plan.read_bytes();self.approval['planSha256']=b.sha(self.plan_bytes);state=json.loads(self.state.read_bytes());state['materialMove']=dict(selected,phase='complete');self.state.write_text(json.dumps(state));active={'approval':'original'};identity=h.object_digest(active)
  with patch.object(b,'source_identity',return_value=identity):b.bridge('install')
  cfg={'roots':{'application-data':{'logical':'/materials/pg-data'},'application-runtime':{'logical':'/materials/runtime'}}};material_state=dict(selected,phase='complete');guard=Mock();g=types.SimpleNamespace(CONFIG='config',TARGETS={'application-data':'data','application-runtime':'runtime'},private_json=lambda path,*args:material_state if path=='state' else cfg,guard=guard);mover=types.SimpleNamespace(STATE='state',g=g,final_configuration=lambda value,current:cfg);running=[False]
  class MaterialPath(type(Path())):
   def stat(path,*args,**kwargs):
    if str(path).startswith('/materials/'):return types.SimpleNamespace(st_dev=1,st_ino=2)
    return super().stat(*args,**kwargs)
  def source(old,host):
   old.PG=h.PG;old.workers_quiet=lambda host:None;host.plan=plan;host.active=active;host.upgrade=types.SimpleNamespace(digest=h.object_digest,active_identity=lambda value:value);host.material_mover=lambda:(mover,{'nonce':'nonce'},[]);host.load_current_durable_active=lambda:None
   host.row=lambda name:{'Id':name,'Image':'approved','Config':{},'HostConfig':{},'Mounts':[],'State':{'Running':running[0],'StartedAt':'original'},'NetworkSettings':{}}
   host.gw=host.row(h.GW);host.worker=host.row(h.WORKER)
   return self.actual_source_identity(old,host)
  if action=='refresh':
   original=self.command
   def recovering_start(args,**kwargs):
    result=original(args,**kwargs)
    if args[1]=='start' and args[-1]==b.ONLINE:
     state=json.loads(self.state.read_bytes());state.update(phase='runtime-entry-pending',runtimeControl={'mode':'recovering'});self.state.write_text(json.dumps(state));raise RuntimeError('host_bridge_unit_command_failed')
    return result
   with patch.object(b,'source_identity',return_value=identity),patch.object(b,'command',side_effect=recovering_start),self.assertRaises(RuntimeError):b.bridge('refresh')
  else:
   state=json.loads(self.state.read_bytes());state.update(phase='runtime-entry-pending',runtimeControl={'mode':'recovering'});self.state.write_text(json.dumps(state))
  with patch.object(b,'source_identity',side_effect=source),patch.object(h,'Path',MaterialPath),patch.object(h,'run',return_value=b'{"materialMountsVerified":true}') as namespace:
   result=b.bridge(action);self.assertTrue(result['recoveryPending']);self.assertFalse(result['writersRestored']);namespace.assert_not_called();guard.assert_called();self.assertEqual(self.states[b.WRITERS[0]]['ActiveState'],'inactive')
   state=json.loads(self.state.read_bytes());state.update(phase='complete',runtimeControl={'mode':'running'});self.state.write_text(json.dumps(state));running[0]=True
   result=b.bridge(action);self.assertTrue(result['writersRestored']);self.assertEqual(namespace.call_count,2 if action=='rollback' else 4);self.assertEqual(self.states[b.WRITERS[0]]['ActiveState'],'active');self.assertEqual(self.plan.read_bytes(),self.plan_bytes)
 def test_material_stopped_pair_rollback_protection(self):self.material_stopped_pair_operation('rollback')
 def test_material_stopped_pair_refresh_protection(self):self.material_stopped_pair_operation('refresh')
 def test_backup_publication_ack_loss_can_be_recovered(self):
  original=b.publish
  def fail_backup(path,*args,**kwargs):
   if path.name=='original-host.py':raise OSError('before backup publication')
   return original(path,*args,**kwargs)
  with patch.object(b,'publish',side_effect=fail_backup),self.assertRaises(OSError):b.bridge('install')
  self.assertEqual(json.loads((self.directory/'journal.json').read_bytes())['phase'],'prepared')
  self.assertTrue(b.bridge('rollback')['rolledBack']);self.assertEqual(self.plan.read_bytes(),self.plan_bytes)

if __name__=='__main__':unittest.main()
