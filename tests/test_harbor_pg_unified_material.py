"""Unified PG/material adapter: real cold files and core API, mocked privilege boundary."""
import contextlib,copy,importlib.util,json,os,subprocess,tempfile,types,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
def load(path,name):
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
h=load(ROOT/'ci/harbor-pg-storage-host.py','unified_host');c=load(ROOT/'ci/harbor-pg-storage-coordinator.py','unified_core');fixtures=load(ROOT/'tests/test_harbor_material_move.py','material_fixtures');m=fixtures.m
class Unified(unittest.TestCase):
 @contextlib.contextmanager
 def fixture(self,failure=None):
  real_run=m.run
  with fixtures.RecoveryRegressionTests().prepare_fixture() as (root,source,row,value),fixtures.CutoverTests().harness(root,row,failure) as (calls,mounts):
   value['diskUuid']='12345678-abcd'
   cutover_run=m.run
   def run(args,**kwargs):return real_run(args,**kwargs) if args[0]=='/usr/bin/rsync' else cutover_run(args,**kwargs)
   archive=root/'archive.lock';archive.touch(mode=0o600);marker=root/'maintenance.json';host=object.__new__(h.Host);host.plan={'materialMove':{'approvalSha256':h.sha(m.APPROVAL.read_bytes()),'nonce':value['nonce']}};host.j={'targetBootAuthorized':True,'oldDataRollbackForbidden':True};host.material_mover=lambda:(m,value,[row]);host.immutable=lambda name:{'State':{'Running':False}};host.save=lambda journal:(root/'host-state.json').write_text(json.dumps(journal));original_open=os.open;original_fstat=os.fstat
   def open_file(path,*args,**kwargs):
    if str(path)=='/run/harbor-material-move.lock':path=root/'material.lock'
    if str(path) in ['/run/harbor-gateway-deploy.lock','/run/harbor-entry/controller-state.lock']:raise AssertionError('outer lock must not be reentered')
    return original_open(path,*args,**kwargs)
   def descriptor(fd):
    info=original_fstat(fd);path=os.readlink('/proc/self/fd/'+str(fd))
    if path in [str(root/'material.lock'),str(archive)]:
     values=list(info);values[4]=999 if path==str(archive) else 0;return os.stat_result(values)
    return info
   def clear(_):marker.unlink(missing_ok=True)
   with patch.object(m,'run',side_effect=run),patch.object(m,'ARCHIVE_LOCK',archive),patch.object(m.g,'MAINTENANCE',marker),patch.object(m,'clear_maintenance',side_effect=clear),patch.object(h.os,'open',side_effect=open_file),patch.object(h.os,'fstat',side_effect=descriptor):
    yield host,root,source,row,value,calls,mounts
 def test_real_material_prepare_and_switch_keep_original_and_journal_complete(self):
  with self.fixture() as (host,root,source,row,value,calls,mounts):
   original=(source/'nested/data').read_bytes();host.prepare_application_materials();state=m.g.private_json(m.STATE)
   self.assertEqual(state['phase'],'complete');self.assertEqual(Path(state['rows'][0]['oldRoot']).joinpath('nested/data').read_bytes(),original);self.assertEqual(Path(row['target']).joinpath('nested/data').read_bytes(),original);self.assertEqual(host.j['materialMove']['phase'],'complete');self.assertFalse(m.g.MAINTENANCE.exists());self.assertTrue(mounts)
   before=len(calls);host.prepare_application_materials();self.assertEqual(len(calls),before)
 def test_interrupted_bind_resumes_same_owned_state_without_recopied_bytes(self):
  with self.fixture([True]) as (host,root,source,row,value,calls,mounts):
   with self.assertRaisesRegex(ValueError,'synthetic_start_failure'):host.prepare_application_materials()
   self.assertNotIn('materialMove',host.j);self.assertTrue(host.j['oldDataRollbackForbidden']);self.assertTrue(m.g.MAINTENANCE.exists())
   with patch.object(m,'copy_cold',side_effect=AssertionError('must resume switching, not recopy')):host.prepare_application_materials()
   self.assertEqual(host.j['materialMove']['phase'],'complete');self.assertFalse(m.g.MAINTENANCE.exists())
 def test_source_running_refused_before_marker_or_copy(self):
  with self.fixture() as (host,root,*_):
   host.immutable=lambda name:{'State':{'Running':True}}
   with self.assertRaisesRegex(RuntimeError,'host_material_postgres_running'):host.prepare_application_materials()
   self.assertFalse(m.g.MAINTENANCE.exists());self.assertFalse(m.STATE.exists())
 def test_cold_copy_failure_then_source_recovery_starts_original_pair_and_resumes(self):
  with self.fixture() as (host,root,source,row,value,calls,mounts):
   journal={'phase':'archive-prepared'};host.j=journal;events=[];entry={'phase':'active'};rows={}
   for name,identifier in [(h.GW,'a'*64),(h.WORKER,'b'*64)]:rows[name]={'Id':identifier,'Image':'approved','Config':{'Image':'approved'},'HostConfig':{},'State':{'Running':True,'StartedAt':'2026-11-14T00:00:00Z'}}
   host.gw=copy.deepcopy(rows[h.GW]);host.worker=copy.deepcopy(rows[h.WORKER]);host.row=lambda name:copy.deepcopy(rows[name]);original=(source/'nested/data').read_bytes()
   def event(name):return lambda *args,**kwargs:events.append(name)
   for name in ['observe_source_recovery_baseline','current_identity','fence_docker_autostart','stop_postgres_clean','finalize_archive','verify_original_recovery_checkpoint','restore_original_restart_policies','start_original_postgres','verify_original_postgres','verify_current_approval_and_private_readiness','update_dynamic_network_snapshot','verify_original_full_instance','remove_gate','verify_writer_restoration']:setattr(host,name,event(name))
   host.verify_original_roots=event('original-material-source-verified');host.hold_drain=lambda:entry.update(phase='drained');host.install_gate=lambda:{'owned':True};host.pause_writers=lambda:{};host.stop_applications=lambda:[item['State'].update(Running=False) for item in rows.values()];host.copy_cold_database=lambda:(_ for _ in ()).throw(RuntimeError('synthetic_cold_copy_failure'));host.record_failure=lambda *args,**kwargs:events.append(('failure',kwargs));host.refresh_active_entry=lambda:entry.update(phase='active');host.restore_writers=event('writers-restored')
   def docker(args):
    self.assertEqual(args[:2],['/usr/bin/docker','start']);found=next(item for item in rows.values() if item['Id']==args[2]);found['State'].update(Running=True);events.append(('started',args[2]));return b''
   coordinator=c.Coordinator(host,journal)
   with self.assertRaisesRegex(RuntimeError,'synthetic_cold_copy_failure'):coordinator.switch()
   self.assertNotIn('targetBootAuthorized',journal);self.assertFalse(m.g.CONFIG.exists());self.assertFalse(m.STATE.exists());self.assertEqual(entry['phase'],'drained')
   with patch.object(h,'run',side_effect=docker):result=coordinator.recover_source()
   self.assertTrue(result['sourceRestored']);self.assertEqual(entry['phase'],'active');self.assertEqual(journal['phase'],'source-restored');self.assertIn('writers-restored',events);self.assertTrue(all(item['State']['Running'] for item in rows.values()));self.assertEqual((source/'nested/data').read_bytes(),original);self.assertNotIn('materialMove',journal);self.assertGreaterEqual(events.count('original-material-source-verified'),2)
 def test_storage_low_source_recovery_uses_actual_host_gates_and_strict_target_mode(self):
  for fault in [None,'unknown-probe','worker-generation','forged-baseline','bad-auth','never-ready','generation-change','network-change','healthy-improved']:
   with self.subTest(fault=fault),self.fixture() as (host,root,source,row,value,calls,mounts):
    journal={'phase':'archive-prepared'};host.j=journal;host.planSha256='f'*64;host.plan.update(activeIdentityDigest='active',containers={});events=[];entry={'phase':'active'};rows={};clock=[0.0]
    for name,identifier,pid in [(h.GW,'a'*64,10),(h.WORKER,'b'*64,11)]:
     rows[name]={'Id':identifier,'Image':'approved','RestartCount':0,'Config':{'Image':'approved'},'HostConfig':{},'State':{'Running':True,'Pid':pid,'StartedAt':'2026-11-12T00:00:00Z','Health':{'Status':'healthy' if name==h.GW else 'unhealthy'}}};host.plan['containers'][name]={'id':identifier,'signature':h.sig(rows[name])}
    host.gw=copy.deepcopy(rows[h.GW]);host.worker=copy.deepcopy(rows[h.WORKER]);host.row=lambda name:copy.deepcopy(rows[name]);host.immutable=lambda name:{'State':{'Running':False}} if name==h.PG else copy.deepcopy(rows[name]);host.active={'image':'source','approval':{'revision':'approved'}};host.upgrade=types.SimpleNamespace(active_authorization=lambda image:{'state':'active','revision':'approved'},approval_binding=lambda item:{'revision':item['revision']});host.entry=types.SimpleNamespace(active_record=lambda:events.append('active-record-verified'));host.config={'port':8092}
    def response(port,path,headers=None):return (403 if headers else 200 if fault=='bad-auth' else 401) if path=='/api/admin/state' else 403 if headers else 200
    host.api={'STACK':str(root),'http_code':response};host.pg=types.SimpleNamespace(device_http_code=lambda stack,config,path,headers=None:response(8092,path,headers),verify_started_composition=lambda *args:(_ for _ in ()).throw(AssertionError('low recovery must not use target preflight')))
    def event(name):return lambda *args,**kwargs:events.append(name)
    for name in ['current_identity','fence_docker_autostart','stop_postgres_clean','finalize_archive','verify_original_recovery_checkpoint','restore_original_restart_policies','start_original_postgres','verify_original_postgres','update_dynamic_network_snapshot','remove_gate','verify_writer_restoration']:setattr(host,name,event(name))
    host.verify_original_roots=event('original-roots');host.verify_persistent_network=event('network-verified');host.source_recovery_network=lambda:{'network':'changed' if fault=='network-change' and clock[0]>0 else 'original'};host.hold_drain=lambda:entry.update(phase='drained');host.install_gate=lambda:{'owned':True};host.pause_writers=lambda:{};host.stop_applications=lambda:[item['State'].update(Running=False) for item in rows.values()];host.copy_cold_database=lambda:(_ for _ in ()).throw(RuntimeError('synthetic_cold_copy_failure'));host.record_failure=lambda *args,**kwargs:events.append(('failure',kwargs));host.refresh_active_entry=lambda:entry.update(phase='active');host.restore_writers=event('writers-restored');host.verify_existing_entry_restored=lambda:self.assertEqual(entry['phase'],'active');probe_inputs=[];strict_calls=[];allow_retry=[False]
    def docker(args):
     self.assertEqual(args[:2],['/usr/bin/docker','start']);item=next(item for item in rows.values() if item['Id']==args[2]);item['State'].update(Running=True,Pid=item['State']['Pid']+100,StartedAt='2026-11-14T00:00:00Z',Health={'Status':'starting'});return b''
    def probe(args,**kwargs):
     if '/app/ci/postgres-preflight.mjs' in args:strict_calls.append(args);return types.SimpleNamespace(returncode=0,stdout=b'{}',stderr=b'')
     if args[-1]!=h.SOURCE_RECOVERY_LOW_PROBE:return types.SimpleNamespace(returncode=0,stdout=b'false' if fault=='bad-auth' else b'true',stderr=b'')
     probe_inputs.append(json.loads(kwargs['input']));pending=len(probe_inputs)==2 or fault=='unknown-probe' and len(probe_inputs)>1 and not allow_retry[0];return types.SimpleNamespace(returncode=0,stdout=b'false' if pending else b'true',stderr=b'')
    def sleep(seconds):
     clock[0]+=seconds
     if fault=='generation-change':rows[h.WORKER]['State']['Pid']+=1
     if fault!='never-ready':
      rows[h.GW]['State']['Health']['Status']='healthy';rows[h.WORKER]['State']['Health']['Status']='healthy' if fault=='healthy-improved' else 'unhealthy'
    original_save=host.save
    def save(value):
     if fault=='worker-generation' and 'workerGeneration' in value:value['workerGeneration']['startedAt']='2026-11-12T00:00:00Z'
     original_save(value)
    host.save=save;coordinator=c.Coordinator(host,journal)
    with patch.object(h.subprocess,'run',side_effect=probe),patch.object(h.time,'monotonic',side_effect=lambda:clock[0]),patch.object(h.time,'sleep',side_effect=sleep):
     with self.assertRaisesRegex(RuntimeError,'synthetic_cold_copy_failure'):coordinator.switch()
     self.assertEqual(journal['sourceRecoveryBaseline']['kind'],'storage-low');self.assertEqual(entry['phase'],'drained');self.assertEqual(len(probe_inputs),1)
     if fault=='forged-baseline':journal['sourceRecoveryBaseline']={'kind':'storage-low'}
     with patch.object(h,'run',side_effect=docker):
      if fault not in [None,'healthy-improved']:
       with self.assertRaises(RuntimeError):coordinator.recover_source()
       self.assertNotIn('writers-restored',events);self.assertEqual(entry['phase'],'drained')
       if fault=='unknown-probe':
        preserved=copy.deepcopy(journal['sourceRecoveryProcesses']);allow_retry[0]=True;del host.source_recovery_deadline
        self.assertTrue(coordinator.recover_source()['sourceRestored']);self.assertEqual(journal['sourceRecoveryProcesses'],preserved);self.assertEqual(entry['phase'],'active')
      else:
       result=coordinator.recover_source();self.assertTrue(result['sourceRestored']);self.assertEqual(entry['phase'],'active');self.assertIn('writers-restored',events);self.assertIn('network-verified',events);self.assertIn('active-record-verified',events);self.assertGreaterEqual(len(probe_inputs),1 if fault=='healthy-improved' else 3)
       if fault is None:self.assertNotEqual(probe_inputs[0]['since'],probe_inputs[1]['since'])
       if fault=='healthy-improved':
        self.assertEqual(len(probe_inputs),1);self.assertEqual(len(strict_calls),4);self.assertTrue(all('--progress-since' in args for args in strict_calls if args[2]==rows[h.WORKER]['Id']))
    if fault is None:
     def strict_wait(check,seconds,code):
      if not check():raise RuntimeError(code)
     before=len(probe_inputs);rows[h.WORKER]['State']['Health']['Status']='unhealthy'
     with patch.object(h,'wait',side_effect=strict_wait):
      with self.assertRaisesRegex(RuntimeError,'host_applications_not_ready'):host.verify_current_approval_and_private_readiness()
     self.assertEqual(len(probe_inputs),before)
 def test_baseline_unknown_probe_or_changed_process_never_persists_low(self):
  for fault in ['probe','generation','health']:
   with self.subTest(fault=fault):
    host=object.__new__(h.Host);host.plan={'materialMove':{},'activeIdentityDigest':'active','containers':{}};host.planSha256='f'*64;host.j={};rows={}
    for name,identifier in [(h.GW,'a'*64),(h.WORKER,'b'*64)]:rows[name]={'Id':identifier,'Image':'approved','RestartCount':0,'Config':{},'HostConfig':{},'State':{'Running':True,'Pid':10,'StartedAt':'2026-11-14T00:00:00Z','Health':{'Status':'healthy' if name==h.GW else 'starting' if fault=='health' else 'unhealthy'}}}
    host.immutable=lambda name:copy.deepcopy(rows[name]);host.save=lambda journal:self.fail('unknown must not save baseline')
    def probe(args,**kwargs):
     if fault=='generation':rows[h.WORKER]['State']['Pid']+=1
     return types.SimpleNamespace(returncode=0,stdout=b'false' if fault=='probe' else b'true',stderr=b'')
    with patch.object(h.subprocess,'run',side_effect=probe):
     with self.assertRaises(RuntimeError):host.observe_source_recovery_baseline()
    self.assertNotIn('sourceRecoveryBaseline',host.j)
 def test_private_source_http_probe_runs_before_publish_and_rejects_auth_regression(self):
  import ast
  tree=ast.parse((ROOT/'ci/harbor-pg-storage-host.py').read_text());method=next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name=='verify_original_full_instance');code=next(ast.literal_eval(n.value) for n in ast.walk(method) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='code' for t in n.targets))
  for fault in ['normal','auth-regression','ready-hang']:
   with self.subTest(fault=fault):
    prelude="import http from 'node:http';import{once}from'node:events';const fault="+json.dumps(fault)+";const server=http.createServer((q,r)=>{if(fault==='ready-hang'&&q.url==='/ready')return;const status=q.headers.host==='untrusted.invalid'?403:q.url==='/api/admin/state'?(fault==='auth-regression'?200:401):200;r.statusCode=status;r.end('{}');});server.listen(0,'127.0.0.1');await once(server,'listening');const port=server.address().port;const AsyncFunction=Object.getPrototypeOf(async()=>{}).constructor;await new AsyncFunction("+json.dumps(code)+".replace('http://127.0.0.1:8092','http://127.0.0.1:'+port))();"
    reply=subprocess.run(['node','--input-type=module','--eval',prelude],capture_output=True,timeout=12)
    self.assertEqual(reply.stdout,b'true' if fault=='normal' else b'false');self.assertEqual(reply.returncode,0 if fault=='normal' else 1);self.assertEqual(reply.stderr,b'')
 def test_probe_uses_verified_default_service_user_without_root_override(self):
  host=object.__new__(h.Host);gateway={'Id':'a'*64,'Config':{'User':'1000:1000'}};worker={'State':{'StartedAt':'2026-11-14T00:00:00Z'}};calls=[]
  def execute(args,**kwargs):calls.append(args);return types.SimpleNamespace(returncode=0,stdout=b'true',stderr=b'')
  with patch.object(h.subprocess,'run',side_effect=execute):host.probe_source_storage_low(gateway,worker)
  self.assertEqual(gateway['Config']['User'],'1000:1000');self.assertNotIn('--user',calls[0]);self.assertEqual(calls[0][-1],h.SOURCE_RECOVERY_LOW_PROBE)
 def test_mount_order_is_canonical_but_complete_records_and_ordered_config_remain_bound(self):
  row={'Id':'a'*64,'Image':'approved','Config':{'User':'1000:1000','Env':['A=1','B=2'],'Cmd':['node','program']},'HostConfig':{},'Mounts':[{'Type':'bind','Source':'/source/a','Destination':'/a','RW':True,'Propagation':'rprivate','extra':{'nested':'a'}},{'Type':'volume','Source':'/source/b','Destination':'/b','RW':False,'Name':'named','Driver':'local'}]};frozen=h.sig(row);row['Mounts'].reverse();self.assertEqual(h.sig(row),frozen)
  for fault in ['mount-record','env','cmd','user']:
   changed=copy.deepcopy(row)
   if fault=='mount-record':changed['Mounts'][0]['Driver']='foreign'
   if fault=='env':changed['Config']['Env'].reverse()
   if fault=='cmd':changed['Config']['Cmd'].reverse()
   if fault=='user':changed['Config']['User']='0:0'
   self.assertNotEqual(h.sig(changed),frozen)
  host=object.__new__(h.Host);host.plan={'containers':{h.GW:{'id':row['Id'],'signature':frozen}}};host.row=lambda _:copy.deepcopy(row);self.assertEqual(host.immutable(h.GW)['Config']['User'],'1000:1000');row['Config']['User']='0:0'
  with self.assertRaisesRegex(RuntimeError,'host_container_changed'):host.immutable(h.GW)
 def test_external_plan_low_boolean_never_selects_source_qualification(self):
  host=object.__new__(h.Host);host.plan={'sourceStorageLow':True};host.j={};self.assertFalse(host.source_low_recovery_selected())
 def test_source_recovery_rejects_boot_arm_or_changed_original_material_before_start(self):
  for fault in ['targetBootAuthorized','oldDataRollbackForbidden','original-root']:
   with self.subTest(fault=fault),self.fixture() as (host,root,source,row,*_):
    host.j={};host.verify_original_roots=lambda:None
    if fault=='original-root':source.rename(root/'retained-original');source.mkdir(mode=0o700)
    else:host.j[fault]=True
    with patch.object(h,'run',side_effect=AssertionError('must not start source')):
     with self.assertRaises(RuntimeError):host.start_applications(source_recovery=True)
 def test_target_start_still_requires_complete_material_receipt(self):
  with self.fixture() as (host,root,source,row,*_):
   host.row=lambda name:copy.deepcopy(host.gw if name==h.GW else host.worker);host.gw={'Id':'a'*64,'Image':'approved','Config':{},'HostConfig':{},'State':{'Running':True}};host.worker={**copy.deepcopy(host.gw),'Id':'b'*64}
   with self.assertRaisesRegex(RuntimeError,'host_material_completion_missing'):host.start_applications()
 def test_without_explicit_material_plan_preserves_existing_pg_flow(self):
  host=object.__new__(h.Host);host.plan={};host.prepare_application_materials();host.verify_application_material_mounts()
 def test_cold_window_and_recovery_finish_materials_before_pg_start(self):
  class Ops:
   def __init__(self):self.calls=[]
   def save(self,j):pass
   def __getattr__(self,name):
    def method(*args,**kwargs):
     self.calls.append(name)
     if name=='install_gate':return {'owned':True}
     if name=='pause_writers':return {}
     if name=='prepare_application_materials':self.assert_armed=True
    return method
  for recovery in [False,True]:
   ops=Ops();journal={'phase':'archive-prepared','gate':{'owned':True},'writers':{}}
   if recovery:journal.update(targetBootAuthorized=True,oldDataRollbackForbidden=True)
   getattr(c.Coordinator(ops,journal),'recover_target' if recovery else 'switch')();names=ops.calls
   self.assertLess(names.index('prepare_application_materials'),names.index('install_instance_boot_guard'));self.assertLess(names.index('prepare_application_materials'),names.index('start_postgres'));self.assertTrue(journal['oldDataRollbackForbidden'])
 def test_material_failure_never_starts_postgres_or_restores_old_data(self):
  class Ops:
   def __init__(self):self.calls=[];self.failure=None
   def save(self,j):pass
   def record_failure(self,j,**kwargs):self.failure=kwargs
   def __getattr__(self,name):
    def method(*args,**kwargs):
     self.calls.append(name)
     if name=='prepare_application_materials':raise RuntimeError('material_failure')
     if name=='install_gate':return {}
     if name=='pause_writers':return {}
    return method
  ops=Ops();journal={'phase':'archive-prepared'}
  with self.assertRaisesRegex(RuntimeError,'material_failure'):c.Coordinator(ops,journal).switch()
  self.assertNotIn('start_postgres',ops.calls);self.assertNotIn('refresh_active_entry',ops.calls);self.assertFalse(ops.failure['old_data_allowed']);self.assertTrue(journal['targetBootAuthorized'])
 def test_real_node_namespace_stat_accepts_only_expected_inode_and_generation(self):
  for fault in [None,'inode','generation']:
   with self.subTest(fault=fault),tempfile.TemporaryDirectory() as directory:
    root=Path(directory);data=root/'pg-data';runtime=root/'runtime'
    for path in [data,runtime,data/'recording-spool',data/'metering-spool']:path.mkdir(mode=0o700)
    config=root/'config.json';config.write_text(json.dumps({'roots':{'application-data':{'logical':str(data)},'application-runtime':{'logical':str(runtime)}}}));value={'nonce':'synthetic'};host=object.__new__(h.Host);host.plan={'materialsDirectory':str(root),'materialMove':{'approvalSha256':'approved'}};host.j={'materialMove':{'nonce':'synthetic','approvalSha256':'approved','phase':'complete'}};module=types.SimpleNamespace(g=types.SimpleNamespace(TARGETS={'application-data':1,'application-runtime':1},CONFIG=config,guard=lambda *args,**kwargs:None,private_json=lambda path:json.loads(path.read_bytes())));host.material_mover=lambda:(module,value,[])
    def row(name):return {'Id':'a'*64 if name==h.GW else 'b'*64,'Image':'approved','Config':{'Image':'approved'},'HostConfig':{},'State':{'Running':True,'StartedAt':'synthetic'},'NetworkSettings':{}}
    host.gw=row(h.GW);host.worker=row(h.WORKER);changed=False;calls=[]
    def current(name):
     actual=row(name)
     if changed and fault=='generation':actual['Config']['Image']='foreign'
     return actual
    host.row=current
    def execute(args):
     nonlocal changed
     calls.append(args[2]);expected=json.loads(args[-1]);mapping={'/app/data':data,'/app/runtime':runtime,'/app/data/recording-spool':data/'recording-spool','/app/data/metering-spool':data/'metering-spool'}
     for item in expected:item['path']=str(mapping[item['path']])
     if fault=='inode':expected[0]['ino']=str(int(expected[0]['ino'])+1)
     result=subprocess.run(['node','--input-type=module','-e',args[-2],json.dumps(expected)],capture_output=True,timeout=5);h.need(result.returncode==0,'host_command_failed');changed=True;return result.stdout
    with patch.object(h,'run',side_effect=execute):
     if fault:
      with self.assertRaises(RuntimeError):host.verify_application_material_mounts()
     else:host.verify_application_material_mounts();self.assertEqual(len(calls),2)
 def test_interruption_requires_exact_current_approved_ticket(self):
  host=object.__new__(h.Host);raw=json.dumps({'interruptEntryClients':True,'to':{'image':'approved'},'materialsDirectory':'/materials','database':{'cluster':'123'}}).encode();host.plan={'interruptTicketSha256':h.sha(raw),'materialsDirectory':'/materials','systemId':'123'};calls=[];host.upgrade=types.SimpleNamespace(TICKET=Path('/ticket'),validate_ticket=lambda *args:calls.append('validate'),verify_installed_tools=lambda *args:calls.append('tools'),verify_source=lambda *args:calls.append('source'),drain_active_entry=lambda *args:calls.append(args));host.entry='entry';host.api={};host.active={'active':True};host.gw={};host.worker={};host.config={}
  with patch.object(h,'private',return_value=raw):host.hold_drain()
  self.assertEqual(calls[:3],['validate','tools','source']);self.assertEqual(len(calls[-1]),4)
  for data in [raw+b' ',json.dumps({'interruptEntryClients':False}).encode()]:
   with patch.object(h,'private',return_value=data),self.assertRaises(RuntimeError):host.hold_drain()
  self.assertEqual(len(calls),4)
if __name__=='__main__':unittest.main()
