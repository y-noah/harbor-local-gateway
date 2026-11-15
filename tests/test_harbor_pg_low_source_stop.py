"""Actual Host + PG shutdown contract; only Docker and signed probe transport mocked."""
import contextlib,copy,importlib.util,json,subprocess,tempfile,types,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
def load(path,name):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
c=load(ROOT/'ci/harbor-pg-storage-coordinator.py','low_stop_core');h=load(ROOT/'ci/harbor-pg-storage-host.py','low_stop_host');pg=load(ROOT/'ci/postgres-deploy.py','low_stop_pg')
class LowSourceStop(unittest.TestCase):
 @contextlib.contextmanager
 def fixture(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);materials=root/'materials';materials.mkdir();inode=materials.stat().st_ino;host=object.__new__(h.Host);rows={};events=[]
   for number,name in enumerate([h.PG,h.GW,h.WORKER],1):
    role='gateway' if name==h.GW else 'worker';rows[name]={'Id':str(number)*64,'Image':'sha256:'+'a'*64,'RestartCount':0,'Config':{'Image':'source:approved','Cmd':['node','/app/postgres-'+role+'.mjs'],'Entrypoint':[],'StopSignal':'SIGTERM','Labels':{'com.harbor.shutdown-contract':pg.SHUTDOWN_CONTRACT,'com.harbor.shutdown-role':role},'Env':[value+'='+str(pg.SHUTDOWN_DEFAULTS[key]) for key,value in pg.SHUTDOWN_ENV.items()]},'HostConfig':{'RestartPolicy':{'Name':'unless-stopped','MaximumRetryCount':0}},'NetworkSettings':{'SandboxID':'sandbox-'+name,'Networks':{}},'State':{'Running':True,'Status':'running','Pid':100+number,'StartedAt':'2026-11-14T00:00:00Z','Health':{'Status':'unhealthy' if name==h.WORKER else 'healthy'}}}
   host.j={};host.planSha256='f'*64;host.plan={'materialMove':{},'activeIdentityDigest':'active','containers':{name:{'id':row['Id'],'signature':h.sig(row),'restartPolicy':copy.deepcopy(row['HostConfig']['RestartPolicy'])} for name,row in rows.items()}};host.row=lambda name:copy.deepcopy(rows[name]);host.gw=host.row(h.GW);host.worker=host.row(h.WORKER);host.save=lambda _:None;host.config={};host.pg=pg
   host.entry=types.SimpleNamespace(ACTIVE='active',private_json=lambda path:'active');host.upgrade=types.SimpleNamespace(active_identity=lambda value:value,digest=lambda value:value);host.material_mover=lambda:None;host.ticket={};host.upgrade.network_snapshot=lambda *args:{'candidateNetworkContainerIds':[rows[h.GW]['Id'],rows[h.WORKER]['Id']]}
   host.verify_original_roots=lambda:h.need(materials.stat().st_ino==inode,'host_source_root_changed')
   def docker(*args,**kwargs):
    self.assertEqual(args[0],'stop');name=args[-1];events.append(('stop',name));rows[name]['State'].update(Running=False,Pid=0,Status='exited',ExitCode=0);rows[name]['NetworkSettings']={}
   host.api={'STACK':str(root),'subprocess':subprocess,'inspect_container':host.row,'inspect_image':lambda _: {'Id':'sha256:'+'a'*64,'Config':{'Labels':{'com.harbor.storage.postgres':pg.CONTRACT}}},'docker':docker}
   def probe(args,**kwargs):
    self.assertTrue(rows[h.GW]['State']['Running']);self.assertTrue(rows[h.WORKER]['State']['Running']);self.assertEqual(args[-1],h.SOURCE_RECOVERY_LOW_PROBE);events.append(('probe',json.loads(kwargs['input'])));return types.SimpleNamespace(returncode=0,stdout=b'true',stderr=b'')
   with patch.object(h.subprocess,'run',side_effect=probe):
    host.observe_source_recovery_baseline();yield host,rows,events,materials
 def fence(self,rows):
  for row in rows.values():row['HostConfig']['RestartPolicy']={'Name':'no','MaximumRetryCount':0}
 def test_low_source_real_shutdown_has_no_fullproof_and_no_probe_after_gateway_stop(self):
  with self.fixture() as (host,rows,events,_):
   host.prepare_low_source_stop();self.fence(rows)
   with patch.object(pg,'prepare_source_image_preflights',side_effect=AssertionError('low source must not demand captureAllowed')):host.stop_applications()
   self.assertEqual([item[0] for item in events],['probe','probe','stop','stop']);self.assertTrue(rows[h.PG]['State']['Running']);self.assertFalse(rows[h.GW]['State']['Running']);self.assertFalse(rows[h.WORKER]['State']['Running'])
 def test_prehold_contract_failure_never_holds(self):
  with self.fixture() as (host,rows,events,_):
   rows[h.WORKER]['Config']['Labels']['com.harbor.shutdown-contract']='unknown';host.plan['containers'][h.WORKER]['signature']=h.sig(rows[h.WORKER]);host.j['sourceRecoveryBaseline']['originalProcesses'][h.WORKER]['signature']=h.sig(rows[h.WORKER]);host.capture_early_hold=lambda:self.fail('must fail before hold')
   with self.assertRaises(pg.ShutdownUnconfirmed):host.hold_drain()
   self.assertFalse(any(item[0]=='stop' for item in events))
 def test_changed_source_or_probe_never_stops_applications(self):
  for fault in ['missing-prehold','forged-baseline','active','material','pid','started','restart','network','pg-generation','restart-policy','probe','during-probe']:
   with self.subTest(fault=fault),self.fixture() as (host,rows,events,materials):
    host.prepare_low_source_stop();self.fence(rows)
    if fault=='missing-prehold':del host.low_source_stop_original
    if fault=='forged-baseline':host.j['sourceRecoveryBaseline']['planSha256']='changed'
    if fault=='active':host.entry.private_json=lambda path:'changed'
    if fault=='material':materials.rename(materials.with_name('original'));materials.mkdir()
    if fault=='pid':rows[h.WORKER]['State']['Pid']+=1
    if fault=='started':rows[h.WORKER]['State']['StartedAt']='2026-11-15T00:00:00Z'
    if fault=='restart':rows[h.WORKER]['RestartCount']+=1
    if fault=='network':rows[h.WORKER]['NetworkSettings']['SandboxID']='foreign'
    if fault=='pg-generation':rows[h.PG]['State']['Pid']+=1
    if fault=='restart-policy':rows[h.WORKER]['HostConfig']['RestartPolicy']['Name']='always'
    def fail_probe(*args,**kwargs):
     if fault=='during-probe':rows[h.PG]['State']['Pid']+=1
     else:raise RuntimeError('probe-failed')
    context=patch.object(host,'probe_source_storage_low',side_effect=fail_probe) if fault in ['probe','during-probe'] else contextlib.nullcontext()
    with context,self.assertRaises(RuntimeError):host.stop_applications()
    self.assertFalse(any(item[0]=='stop' for item in events));self.assertTrue(rows[h.PG]['State']['Running'])
 def test_post_gateway_change_blocks_worker_and_pg(self):
  for fault in ['worker-pid','pg-pid','material','gateway-restart','unknown-exit']:
   with self.subTest(fault=fault),self.fixture() as (host,rows,events,materials):
    host.prepare_low_source_stop();self.fence(rows);original=host.api['docker']
    def stop(*args,**kwargs):
     original(*args,**kwargs)
     if fault=='worker-pid':rows[h.WORKER]['State']['Pid']+=1
     if fault=='pg-pid':rows[h.PG]['State']['Pid']+=1
     if fault=='material':materials.rename(materials.with_name('original'));materials.mkdir()
     if fault=='gateway-restart':rows[h.GW]['State'].update(Running=True,Pid=999)
     if fault=='unknown-exit':rows[h.GW]['State']['ExitCode']=137
    host.api['docker']=stop
    with self.assertRaises(RuntimeError):host.stop_applications()
    self.assertEqual([item for item in events if item[0]=='stop'],[('stop',h.GW)]);self.assertTrue(rows[h.PG]['State']['Running']);self.assertTrue(rows[h.WORKER]['State']['Running'])
 def test_coordinator_reaches_cold_copy_only_after_actual_pair_shutdown(self):
  for broken in [False,True]:
   with self.subTest(broken=broken),self.fixture() as (host,rows,events,_):
    host.j['phase']='archive-prepared';host.current_identity=lambda:None;host.capture_early_hold=lambda:events.append(('hold',));host.drain_with_early_receipt=lambda:None;host.install_gate=lambda:{};host.pause_writers=lambda:{};host.fence_docker_autostart=lambda:self.fence(rows);host.record_failure=lambda *args,**kwargs:None
    host.stop_postgres_clean=lambda:events.append(('pg-stop',));host.finalize_archive=lambda:None
    def cold():events.append(('cold-copy',));raise RuntimeError('stop-test-at-cold-copy')
    host.copy_cold_database=cold
    if broken:host.probe_source_storage_low=lambda *args,**kwargs:(_ for _ in ()).throw(RuntimeError('low-proof-failed'))
    with self.assertRaisesRegex(RuntimeError,'low-proof-failed' if broken else 'stop-test-at-cold-copy'):c.Coordinator(host,host.j).switch()
    self.assertEqual(any(item[0]=='pg-stop' for item in events),not broken);self.assertEqual(any(item[0]=='cold-copy' for item in events),not broken)
    if not broken:self.assertLess(events.index(('stop',h.WORKER)),events.index(('pg-stop',)))
 def test_healthy_or_missing_baseline_uses_fullproof_without_fallback(self):
  for kind in ['healthy',None]:
   with self.subTest(kind=kind),self.fixture() as (host,rows,events,_):
    if kind is None:host.j.pop('sourceRecoveryBaseline')
    else:host.j['sourceRecoveryBaseline']['kind']=kind
    host.config={}
    with patch.object(pg,'configuration',return_value=([],None)),patch.object(pg,'prepare_source_image_preflights',side_effect=RuntimeError('fullproof-failed')) as proof,patch.object(host,'stop_low_source_applications',side_effect=AssertionError('no fallback')):
     with self.assertRaisesRegex(RuntimeError,'fullproof-failed'):host.stop_applications()
     proof.assert_called_once()
    self.assertFalse(any(item[0]=='stop' for item in events))
if __name__=='__main__':unittest.main()
