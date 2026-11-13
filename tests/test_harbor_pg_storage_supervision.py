import contextlib,copy,fcntl,socket,importlib.util,json,os,signal,subprocess,tempfile,time,types,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
CI=ROOT/'ci'
from unittest.mock import patch

def load(path,name):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
h=load(CI/'harbor-pg-storage-host.py','supervised_host');core=load(CI/'harbor-pg-storage-coordinator.py','supervised_core');wrapper=load(CI/'deploy-wrapper.py','supervised_wrapper')

class Harness:
 def __init__(self):
  self.host=object.__new__(h.Host);self.host.core=core;self.calls=[];self.releasePhase='complete';self.active={'image':'new-release','approval':{'id':'new'},'networkSnapshot':{'ports':'generation-2'}};self.host.j={'phase':'complete','oldDataRollbackForbidden':True,'targetBootAuthorized':True,'runtimeControl':{'mode':'running'},'writers':{}}
  self.rows={name:{'Id':name+'-new','State':{'Running':True,'StartedAt':'2026-11-13T00:00:01Z','Pid':1,'Status':'running'},'Config':{'StopSignal':'SIGTERM'},'HostConfig':{'RestartPolicy':{'Name':'no'}}} for name in [h.PG,h.GW,h.WORKER]}
  self.host.verify_postgres_facts=self.effect('pg_readonly_facts');self.host.inspect_runtime_containers=lambda:copy.deepcopy(self.rows);self.host.verify_runtime_facts=self.effect('facts');self.host.row=lambda name:copy.deepcopy(self.rows[name]);self.host.immutable=lambda name:copy.deepcopy(self.rows[name]);self.host.entry=types.SimpleNamespace(ACTIVE='ACTIVE',private_json=lambda _:copy.deepcopy(self.active));self.host.upgrade=types.SimpleNamespace(active_identity=lambda active:{'approval':active['approval']},digest=lambda value:json.dumps(value,sort_keys=True),record_for=lambda _: {'phase':self.releasePhase});self.host.save=lambda state:self.calls.append(('saved',copy.deepcopy(state)))
  self.host.load_current_durable_active=self.select
  for name in ['data_mounts','require_docker_restart_disabled','verify_installed_unit_files','hold_runtime_entry','verify_current_approval_and_private_readiness','update_dynamic_network_snapshot','refresh_active_entry','verify_full_instance','verify_existing_entry_restored','verify_postgres_mount_tls_report_wal','verify_writer_restoration']:
   setattr(self.host,name,self.effect(name))
  self.host.install_gate=lambda:self.gate();self.host.remove_gate=lambda gate:self.calls.append(('remove_gate',gate));self.host.start_postgres=lambda:self.start(h.PG);self.host.start_applications=self.start_apps;self.host.stop_runtime_container=self.stop;self.host.adopt_release_gate_for_resume=self.effect('adopt_release_gate');self.host.pause_writers=lambda:self.pause_writers();self.host.restore_writers=lambda *args,**kwargs:self.calls.append(('restore_writers',));self.select();self.host.record_runtime_ready();self.calls=[]
 def effect(self,name):return lambda *args,**kwargs:self.calls.append((name,))
 def select(self,*args,**kwargs):self.host.currentReleasePhase=self.releasePhase;self.host.active=copy.deepcopy(self.active);self.host.gw=copy.deepcopy(self.rows[h.GW]);self.host.worker=copy.deepcopy(self.rows[h.WORKER]);self.calls.append(('select',self.host.gw['Id']))
 def gate(self):self.host.j['gate']={'owned':True};self.calls.append(('gate',));return self.host.j['gate']
 def pause_writers(self):self.calls.append(('pause_writers',));self.host.j['writers']={};return {}
 def stop(self,name):self.calls.append(('stop',name));self.rows[name]['State'].update(Running=False,Pid=0,Status='exited')
 def start(self,name):
  if not self.rows[name]['State']['Running']:self.calls.append(('start',name));self.rows[name]['State'].update(Running=True,Pid=2,Status='running',StartedAt='2026-11-13T00:00:02Z')
 def start_apps(self):
  for name in [h.GW,h.WORKER]:self.start(name)
 def fail(self,name,code):self.rows[name]['State'].update(Running=False,Pid=0,Status='exited',ExitCode=code,OOMKilled=code==137)

class Tests(unittest.TestCase):
 def test_worker_exit_zero_nonzero_oom_all_restore_without_recycling_healthy_pg(self):
  for code in [0,1,137]:
   fixture=Harness();fixture.fail(h.WORKER,code);result=fixture.host.runtime_step();self.assertTrue(result['recovered']);self.assertIn(('start',h.WORKER),fixture.calls);self.assertNotIn(('stop',h.PG),fixture.calls);self.assertNotIn(('stop',h.GW),fixture.calls);self.assertEqual(fixture.host.j['phase'],'complete')
 def test_pg_loss_recycles_authorized_apps_before_pg_start_and_checks_health_after_start(self):
  fixture=Harness();fixture.fail(h.PG,137);fixture.host.runtime_step();calls=fixture.calls
  self.assertLess(calls.index(('stop',h.GW)),calls.index(('stop',h.WORKER)));self.assertLess(calls.index(('stop',h.WORKER)),calls.index(('start',h.PG)));self.assertLess(calls.index(('start',h.PG)),calls.index(('verify_postgres_mount_tls_report_wal',)));self.assertLess(calls.index(('update_dynamic_network_snapshot',)),calls.index(('refresh_active_entry',)));self.assertLess(calls.index(('verify_full_instance',)),calls.index(('remove_gate',{'owned':True})))
 def test_failed_pg_boot_preserves_gate_and_intent_and_never_selects_old_source(self):
  fixture=Harness();fixture.fail(h.PG,1);fixture.host.start_postgres=lambda:(_ for _ in ()).throw(RuntimeError('host_command_failed'))
  with self.assertRaisesRegex(RuntimeError,'host_command_failed'):fixture.host.runtime_step()
  self.assertEqual(fixture.host.j['phase'],'runtime-recovering');self.assertTrue(fixture.host.j['oldDataRollbackForbidden']);self.assertTrue(fixture.host.j['gate']['owned']);self.assertFalse(any(call[0]=='remove_gate' for call in fixture.calls))
 def test_changed_pg_generation_recycles_owner_even_when_all_containers_running(self):
  fixture=Harness();fixture.rows[h.PG]['State']['StartedAt']='2026-11-13T00:00:03Z';fixture.host.runtime_step();self.assertIn(('stop',h.GW),fixture.calls);self.assertIn(('start',h.GW),fixture.calls)
 def test_new_active_is_selected_after_publish_and_new_network_is_refreshed(self):
  fixture=Harness();fixture.active['approval']={'id':'newest'};fixture.rows[h.GW]['Id']='latest-gateway';fixture.rows[h.WORKER]['Id']='latest-worker';fixture.host.runtime_step();self.assertIn(('select','latest-gateway'),fixture.calls);self.assertIn(('update_dynamic_network_snapshot',),fixture.calls);self.assertEqual(fixture.host.j['runtimeReceipt']['containers'][h.GW]['id'],'latest-gateway')
 def test_network_endpoint_generation_change_is_detected_without_extra_hot_path_cli(self):
  fixture=Harness();fixture.rows[h.WORKER]['NetworkSettings']={'SandboxID':'new-sandbox','Networks':{'owned':{'NetworkID':'network','EndpointID':'new-endpoint'}}};fixture.host.runtime_step();self.assertIn(('update_dynamic_network_snapshot',),fixture.calls);self.assertIn(('refresh_active_entry',),fixture.calls);self.assertNotIn(('stop',h.PG),fixture.calls)
 def test_unchanged_running_generation_is_readonly(self):
  fixture=Harness();before=copy.deepcopy(fixture.host.j);self.assertTrue(fixture.host.runtime_step()['unchanged']);self.assertEqual(fixture.host.j,before);self.assertFalse(any(call[0] in ['saved','gate','start','stop'] for call in fixture.calls))
 def test_planned_and_paused_never_restart_any_process_including_exit_zero(self):
  for mode in ['planned','paused']:
   fixture=Harness();fixture.host.j['runtimeControl']={'mode':mode};fixture.fail(h.PG,0);fixture.fail(h.WORKER,0);before=copy.deepcopy(fixture.host.j);self.assertTrue(fixture.host.runtime_step()['paused']);self.assertEqual(fixture.host.j,before);self.assertEqual(fixture.calls,[])
 def test_pause_intent_is_saved_before_any_stop_and_retry_does_not_enable_recovery(self):
  fixture=Harness();fixture.host.pause_runtime();saved=next(call[1] for call in fixture.calls if call[0]=='saved');self.assertEqual(saved['runtimeControl']['mode'],'paused');self.assertLess(next(i for i,call in enumerate(fixture.calls) if call[0]=='saved'),fixture.calls.index(('stop',h.GW)));self.assertEqual([call[1] for call in fixture.calls if call[0]=='stop'],[h.GW,h.WORKER,h.PG]);fixture.calls=[];self.assertTrue(fixture.host.runtime_step()['paused']);self.assertEqual(fixture.calls,[])
 def test_failed_publish_original_pair_can_resolve_without_starting_or_resuming(self):
  fixture=Harness();binding=fixture.host.upgrade.digest(fixture.host.upgrade.active_identity(fixture.active));fixture.host.j.update(runtimeControl={'mode':'planned'},plannedOperation={'nonce':'n','beforeActiveDigest':binding,'requestedImage':'uncommitted-new'});fixture.host.verify_runtime_facts=fixture.effect('facts')
  result=fixture.host.reconcile_planned_operation(False);self.assertTrue(result['released']);self.assertTrue(result['originalRestored']);self.assertEqual(fixture.host.j['runtimeControl'],{'mode':'running'});self.assertNotIn('plannedOperation',fixture.host.j);self.assertFalse(any(call[0] in ['start','stop','refresh_active_entry','gate'] for call in fixture.calls))
 def test_unknown_or_unhealthy_publish_result_keeps_protection_and_has_explicit_resume(self):
  fixture=Harness();fixture.host.j.update(runtimeControl={'mode':'planned'},plannedOperation={'nonce':'n','beforeActiveDigest':'other','requestedImage':'unknown'});fixture.host.verify_runtime_facts=lambda:(_ for _ in ()).throw(RuntimeError('host_runtime_pg_not_ready'))
  with self.assertRaisesRegex(RuntimeError,'host_planned_result_unknown'):fixture.host.reconcile_planned_operation(False)
  self.assertEqual(fixture.host.j['runtimeControl']['mode'],'planned');fixture.host.entry.STATE='STATE';fixture.host.entry.CONTROL='/tmp/harbor-no-such-control';fixture.host.entry.private_json=lambda path:{'phase':'activated'} if path=='STATE' else copy.deepcopy(fixture.active);fixture.fail(h.PG,1);self.assertTrue(fixture.host.prepare_runtime_resume()['recovered']);self.assertNotIn('plannedOperation',fixture.host.j)
 def test_held_entry_fact_check_cannot_release_plan(self):
  fixture=Harness();binding=fixture.host.upgrade.digest(fixture.host.upgrade.active_identity(fixture.active));fixture.host.j.update(runtimeControl={'mode':'planned'},plannedOperation={'nonce':'n','beforeActiveDigest':binding,'requestedImage':'x'});fixture.host.verify_runtime_facts=lambda:(_ for _ in ()).throw(RuntimeError('host_entry_not_restored'))
  with self.assertRaisesRegex(RuntimeError,'host_entry_not_restored'):fixture.host.reconcile_planned_operation(False)
  self.assertEqual(fixture.host.j['runtimeControl']['mode'],'planned');self.assertIn('plannedOperation',fixture.host.j)
 def test_pg_recovery_retry_preserves_original_writer_snapshot(self):
  fixture=Harness();fixture.fail(h.PG,1);captures=[]
  def pause():
   if not fixture.host.j.get('writersPauseStarted'):
    captures.append('captured');fixture.host.j['writers']={'base.service':{'ActiveState':'active'}};fixture.host.j['writersPauseStarted']=True
   return fixture.host.j['writers']
  fixture.host.pause_writers=pause;fixture.host.verify_current_approval_and_private_readiness=lambda:(_ for _ in ()).throw(RuntimeError('host_applications_not_ready'))
  with self.assertRaisesRegex(RuntimeError,'host_applications_not_ready'):fixture.host.runtime_step()
  fixture.host.verify_current_approval_and_private_readiness=fixture.effect('ready');fixture.host.runtime_step();self.assertEqual(captures,['captured']);self.assertEqual(fixture.host.j['writers']['base.service']['ActiveState'],'active');self.assertNotIn('writersPauseStarted',fixture.host.j)
 def test_boot_resume_selects_durable_approval_without_application_health(self):
  fixture=Harness();fixture.host.j['runtimeControl']={'mode':'paused'};fixture.host.entry.STATE='STATE';fixture.host.entry.CONTROL='/tmp/harbor-no-control';fixture.host.entry.private_json=lambda _: {'phase':'boot-guarded'};fixture.host.recover_runtime=lambda:self.fail('ordinary boot chain must handle missing IPC')
  with patch.object(Path,'exists',return_value=False):self.assertTrue(fixture.host.prepare_runtime_resume()['bootstrap'])
  self.assertEqual(fixture.host.j['runtimeControl']['mode'],'recovering');self.assertFalse(any(call[0]=='verify_current_approval_and_private_readiness' for call in fixture.calls))
 def test_unknown_runtime_mode_fails_closed(self):
  fixture=Harness();fixture.host.j['runtimeControl']={'mode':'unknown'}
  with self.assertRaisesRegex(RuntimeError,'storage_runtime_mode_unknown'):fixture.host.runtime_step()
  self.assertEqual(fixture.calls,[])
 def test_pending_explicit_authority_survives_success_exit_and_boot_then_promotes_only_on_complete(self):
  fixture=Harness();binding=fixture.host.upgrade.digest(fixture.host.upgrade.active_identity(fixture.active));fixture.releasePhase='committed';fixture.host.j['runtimeControl']={'mode':'recovering','selectedActiveDigest':binding};fixture.host.record_runtime_ready();self.assertEqual(fixture.host.j['runtimeControl'],{'mode':'recovering','selectedActiveDigest':binding})
  fixture.fail(h.WORKER,0);fixture.host.runtime_step();self.assertEqual(fixture.host.j['runtimeControl']['mode'],'recovering');self.assertIn(('start',h.WORKER),fixture.calls)
  calls=[]
  class BootOps:
   def __getattr__(self,name):
    def invoke(*args,**kwargs):calls.append(name);return {'owned':True} if name=='install_gate' else None
    return invoke
  journal=copy.deepcopy(fixture.host.j);core.Coordinator(BootOps(),journal).boot_start();self.assertIn('start_postgres',calls)
  fixture.releasePhase='complete';fixture.host.runtime_step();self.assertEqual(fixture.host.j['runtimeControl'],{'mode':'running'})
 def test_unknown_mode_is_rejected_before_boot_side_effects_even_after_constructor(self):
  fixture=Harness();coordinator=core.Coordinator(fixture.host,fixture.host.j);fixture.host.j['runtimeControl']={'mode':'unknown'};before=copy.deepcopy(fixture.host.j)
  with self.assertRaisesRegex(RuntimeError,'storage_runtime_mode_unknown'):coordinator.boot_start()
  self.assertEqual(fixture.host.j,before);self.assertEqual(fixture.calls,[])
  for mode in ['paused','planned']:
   fixture.host.j['runtimeControl']={'mode':mode};self.assertTrue(coordinator.boot_start()['paused']);self.assertEqual(fixture.calls,[])
 def release_gate_fixture(self):
  fixture=Harness();host=fixture.host;host.plan={'runId':'a'*32,'systemId':'123'};host.config={};host.ticket={'network':{'id':'net','name':'net-name','bridge':'br'}};fixture.active['candidate']={'containerId':fixture.rows[h.GW]['Id']};fixture.active['networkSnapshot']={'candidateNetworkIdentity':{'id':'net','name':'net-name','bridge':'br','ifindex':22,'mac':'new'}};fixture.select();binding=host.upgrade.digest(host.upgrade.active_identity(fixture.active));host.j['runtimeControl']={'mode':'recovering','selectedActiveDigest':binding};host.j['gateIntent']={'state':'removed'}
  source={'image':'old-image','approval':{'id':'old'},'candidate':{'containerId':'old-gw'},'networkSnapshot':copy.deepcopy(fixture.active['networkSnapshot'])};before=host.upgrade.digest(host.upgrade.active_identity(source));ticket={'from':{'activeIdentityDigest':before,'containerId':'old-gw','workerId':'old-worker'},'to':{'image':fixture.active['image']},'database':{'cluster':'123'},'network':copy.deepcopy(host.ticket['network'])};snapshot={'nftables':[{'chain':{'name':'owned'}}]};record={'phase':'committed','ticket':ticket,'ticketDigest':host.upgrade.digest(ticket),'gate':copy.deepcopy(snapshot),'candidate':copy.deepcopy(fixture.active['candidate']),'workerId':fixture.rows[h.WORKER]['Id'],'fromActive':source}
  target=copy.deepcopy(fixture.active['approval']);host.j['plannedOperation']={'nonce':'b'*32,'bootId':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'runId':'a'*32,'requestedImage':fixture.active['image'],'beforeActiveDigest':before,'targetApproval':target,'targetTicketDigest':record['ticketDigest']};host.upgrade.record_for=lambda approval:copy.deepcopy(record) if approval==target else {'phase':'complete'};host.upgrade.approval_binding=lambda value:copy.deepcopy(value);host.upgrade.GATE='gate';host.upgrade.gate_snapshot=lambda _:copy.deepcopy(snapshot);present=[True];host.entry.execute=lambda _:json.dumps({'nftables':[{'table':{'name':'gate'}}]} if present[0] else {'nftables':[]});host.upgrade.gate_remove=lambda entry,expected:present.__setitem__(0,False) if expected==snapshot else self.fail('wrong gate delete');host.verify_persistent_network=lambda:{'ifindex':22,'address':'new'};host.adopt_release_gate_for_resume=types.MethodType(h.Host.adopt_release_gate_for_resume,host);host.install_gate=types.MethodType(h.Host.install_gate,host);host.remove_gate=types.MethodType(h.Host.remove_gate,host);return fixture,snapshot,record
 def test_explicit_resume_adopts_exact_pending_release_gate_and_reuses_it_without_deleting_unknown_table(self):
  fixture,snapshot,record=self.release_gate_fixture();host=fixture.host;host.entry.STATE='STATE';host.entry.private_json=lambda path:{'phase':'activated'} if path=='STATE' else copy.deepcopy(fixture.active);self.assertTrue(host.prepare_runtime_resume()['recovered']);self.assertEqual(host.j['gate'],snapshot);self.assertEqual(host.j['gateIntent']['state'],'removed');self.assertEqual(host.j['gateOrigin']['source'],'explicit-release-recovery')
 def test_release_gate_unknown_changed_wrong_boot_or_wrong_candidate_is_never_adopted(self):
  for case in ['unknown','changed','boot','candidate','phase','bridge']:
   fixture,snapshot,record=self.release_gate_fixture();host=fixture.host
   if case=='unknown':host.j.pop('plannedOperation')
   elif case=='changed':snapshot['nftables'].append({'rule':{'foreign':True}})
   elif case=='boot':host.j['plannedOperation']['bootId']='00000000-0000-0000-0000-000000000000'
   elif case=='candidate':record['candidate']['containerId']='foreign'
   elif case=='phase':record['phase']='complete'
   else:host.verify_persistent_network=lambda:{'ifindex':99,'address':'unknown'}
   before=copy.deepcopy(host.j);fixture.calls=[]
   with self.assertRaises(RuntimeError):host.adopt_release_gate_for_resume()
   self.assertEqual(host.j,before);self.assertEqual(fixture.calls,[])
 def test_hold_before_active_commit_adopts_target_ticket_gate_and_restores_original_pair(self):
  fixture,snapshot,record=self.release_gate_fixture();host=fixture.host;record['phase']='holding';fixture.active=copy.deepcopy(record['fromActive']);fixture.rows[h.GW]['Id']='old-gw';fixture.rows[h.WORKER]['Id']='old-worker';fixture.select();binding=host.upgrade.digest(host.upgrade.active_identity(fixture.active));host.j['runtimeControl']={'mode':'planned'};host.entry.STATE='STATE';host.entry.private_json=lambda path:{'phase':'activated'} if path=='STATE' else copy.deepcopy(fixture.active)
  result=host.prepare_runtime_resume();self.assertTrue(result['recovered']);self.assertEqual(host.j['runtimeControl'],{'mode':'running'});self.assertNotIn('plannedOperation',host.j);self.assertEqual(host.j['runtimeReceipt']['activeIdentityDigest'],binding);self.assertEqual(host.j['runtimeReceipt']['containers'][h.GW]['id'],'old-gw');self.assertEqual(host.j['gateIntent']['state'],'removed')
 def test_source_ticket_or_target_digest_tampering_cannot_transfer_gate(self):
  for case in ['from','target','image','fromActive']:
   fixture,snapshot,record=self.release_gate_fixture();host=fixture.host
   if case=='from':host.j['plannedOperation']['beforeActiveDigest']='foreign'
   elif case=='target':host.j['plannedOperation']['targetTicketDigest']='foreign'
   elif case=='image':host.j['plannedOperation']['requestedImage']='foreign'
   else:record['fromActive']['approval']={'id':'foreign'}
   before=copy.deepcopy(host.j);fixture.calls=[]
   with self.assertRaises(RuntimeError):host.adopt_release_gate_for_resume()
   self.assertEqual(host.j,before);self.assertEqual(fixture.calls,[])
 def test_poll_interval_is_root_config_bounded_default_and_zero_cannot_disable(self):
  self.assertEqual(h.supervision_interval({}),5)
  for value in [1,5,60]:self.assertEqual(h.supervision_interval({'postgres_storage_supervision_poll_seconds':value}),value)
  for value in [0,61,True,False,None,5.0,'5']:
   with self.assertRaisesRegex(RuntimeError,'host_supervision_interval_invalid'):h.supervision_interval({'postgres_storage_supervision_poll_seconds':value})
 def test_steady_path_uses_exactly_one_batched_inspect_and_zero_proofs_or_state_writes(self):
  host=object.__new__(h.Host);host.core=core;active={'approval':{'id':'approved'}};host.upgrade=types.SimpleNamespace(active_identity=lambda value:value['approval'],digest=lambda value:json.dumps(value,sort_keys=True));host.entry=types.SimpleNamespace(ACTIVE='ACTIVE',private_json=lambda _:copy.deepcopy(active));rows=[{'Name':'/'+name,'Id':str(i)*64,'State':{'Running':True,'StartedAt':'stable'},'HostConfig':{'RestartPolicy':{'Name':'no'}}} for i,name in enumerate([h.PG,h.GW,h.WORKER],1)];host.j={'phase':'complete','runtimeControl':{'mode':'running'},'runtimeReceipt':{'containers':{row['Name'][1:]:h.container_generation(row) for row in rows},'activeIdentityDigest':host.upgrade.digest(active['approval'])}};host.save=lambda _:self.fail('steady loop must not save');host.recover_runtime=lambda:self.fail('steady loop must not recover');before=copy.deepcopy(host.j)
  with patch.object(h,'run',return_value=json.dumps(rows).encode()) as called:
   self.assertTrue(host.runtime_step()['unchanged']);self.assertEqual(called.call_count,1);self.assertEqual(called.call_args.args[0],['/usr/bin/docker','container','inspect',h.PG,h.GW,h.WORKER])
  self.assertEqual(host.j,before)
 def test_all_recovery_entries_reject_paused_and_planned_before_any_effect(self):
  for mode in ['paused','planned']:
   fixture=Harness();fixture.host.j['runtimeControl']={'mode':mode};before=copy.deepcopy(fixture.host.j)
   for operation in [lambda:core.Coordinator(fixture.host,fixture.host.j).recover_target(),lambda:core.Coordinator(fixture.host,fixture.host.j).recover_source(),lambda:core.Coordinator(fixture.host,fixture.host.j).boot_online(),fixture.host.recover_runtime,lambda:h.Host.verify_target_recovery_checkpoint(fixture.host)]:
    with self.assertRaisesRegex(RuntimeError,'storage_resume_required'):operation()
    self.assertEqual(fixture.host.j,before);self.assertEqual(fixture.calls,[])
 def test_main_recovery_worker_race_rejects_latest_pause_or_plan_before_worker_mark_save(self):
  for action in ['recover-source','recover-target']:
   for mode in ['paused','planned']:
    fixture=Harness();host=fixture.host;latest={**copy.deepcopy(host.j),'workerUnit':'previous-worker.service','runtimeControl':{'mode':mode}};host.reload_locked_state=lambda:setattr(host,'j',copy.deepcopy(latest));host.save=lambda _:self.fail('rejected worker must not save STATE')
    @contextlib.contextmanager
    def locked(*args):yield
    with patch.object(h,'Host',return_value=host),patch.object(h,'require_switch_worker',return_value='new-worker.service'),patch.object(h,'locks',side_effect=locked),patch.object(h.sys,'argv',['host',action,'b'*16]):
     with self.assertRaisesRegex(RuntimeError,'storage_resume_required'):h.main()
    self.assertEqual(host.j,latest);self.assertEqual(host.j['workerUnit'],'previous-worker.service');self.assertEqual(fixture.calls,[])
 def test_launch_recovery_checks_latest_state_instead_of_stale_constructor_mode(self):
  fixture=Harness();fixture.host.plan={'scope':core.SCOPE,'runId':'a'*32};fresh={'scope':core.SCOPE,'runId':'a'*32,'runtimeControl':{'mode':'paused'}}
  with patch.object(h,'private',return_value=json.dumps(fresh).encode()),patch.object(Path,'exists',return_value=True),patch.object(h,'run',side_effect=AssertionError('must not submit unit')):
   for action in ['recover-source','recover-target']:
    with self.assertRaisesRegex(RuntimeError,'storage_resume_required'):h.require_recovery_launch(fixture.host,action)
  self.assertEqual(fixture.host.j['runtimeControl'],{'mode':'running'})
 def test_pending_authority_can_pause_repeat_pause_and_resume_without_becoming_running(self):
  fixture=Harness();fixture.releasePhase='committed';binding=fixture.host.upgrade.digest(fixture.host.upgrade.active_identity(fixture.active));fixture.host.j['runtimeControl']={'mode':'recovering','selectedActiveDigest':binding};fixture.host.pause_runtime();self.assertEqual(fixture.host.j['runtimeControl']['mode'],'paused');self.assertEqual(fixture.host.j['runtimeControl']['selectedActiveDigest'],binding);fixture.host.pause_runtime();fixture.host.entry.STATE='STATE';fixture.host.entry.private_json=lambda path:{'phase':'activated'} if path=='STATE' else copy.deepcopy(fixture.active);self.assertTrue(fixture.host.prepare_runtime_resume()['recovered']);self.assertEqual(fixture.host.j['runtimeControl'],{'mode':'recovering','selectedActiveDigest':binding})
 def absent_entry_fixture(self):
  fixture=Harness();host=fixture.host;host.plan={'runId':'a'*32};fixture.active['candidate']={'containerId':fixture.rows[h.GW]['Id']};fixture.select();profile={'entryPort':0,'entryCgroup':'/harbor-no-existing-test-cgroup','source':{'port':8092}};saved={'phase':'activated','configDigest':'profile','rulesDigest':'rules','mangleDigest':'mangle','candidate':copy.deepcopy(fixture.active['candidate']),'tcDigests':{'live-owned':'tc','gone-pg-veth':'gone'}};entryrow={'Id':'owned-entry','Image':'owned-image','State':{'Running':False,'Pid':0,'Status':'exited','Paused':False,'Restarting':False,'Dead':False,'Error':''}};host.runtime=types.SimpleNamespace(inspect_entry=lambda _:copy.deepcopy(entryrow));host.entry.PROFILE='PROFILE';host.entry.STATE='STATE';host.entry.CANDIDATE_PROFILE='CANDIDATE';host.entry.CONTROL='/tmp/harbor-owned-test-missing-control';host.entry.TABLE='entry-table';host.entry.validate=lambda value:value;host.entry.fingerprint=lambda value:'profile';host.entry.guard_fingerprint=lambda value:'guard';host.entry.state_rules_digest=lambda:'rules';host.entry.mangle_digest=lambda:'mangle';host.entry.tc_digest=lambda device:'tc';host.entry.trusted_parents=lambda path:None;host.entry.refreshed_bypass=lambda profile:[{'pid':1}];host.entry.private_json=lambda path:copy.deepcopy(profile if path=='PROFILE' else saved if path=='STATE' else {'socketPath':'/run/harbor-entry/candidate.sock'} if path=='CANDIDATE' else fixture.active);host.entry.execute=lambda args:json.dumps([{'ifname':'live-owned'}] if args[0].endswith('/ip') else {'nftables':[{'set':{'elem':[8092]}}]});host.upgrade.GATE='gate';host.upgrade.gate_snapshot=lambda _:copy.deepcopy(host.j['gate'])
  def gate():
   host.j['gate']={'owned':True};host.j['gateIntent']={'state':'installed','bootId':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'runId':'a'*32,'activeIdentityDigest':host.upgrade.digest(host.upgrade.active_identity(fixture.active))};fixture.calls.append(('gate',));return host.j['gate']
  host.install_gate=gate;host.hold_runtime_entry=types.MethodType(h.Host.hold_runtime_entry,host);return fixture,profile,saved,entryrow
 @contextlib.contextmanager
 def root_host_namespace(self):
  original=Path.stat
  def observed(path,*args,**kwargs):
   if str(path) in ['/proc/self/ns/net','/proc/1/ns/net']:return types.SimpleNamespace(st_dev=1,st_ino=1)
   return original(path,*args,**kwargs)
  with patch.object(Path,'stat',observed):yield
 @contextlib.contextmanager
 def entry_cgroup_layout(self,profile,*,v2=True,populated=None):
  # Model the complete cgroup evidence independently of the Runner's host.
  group=Path('/sys/fs/cgroup')/profile['entryCgroup'].lstrip('/')
  original_file=Path.is_file;original_exists=Path.exists;original_read=Path.read_text
  def is_file(path,*args,**kwargs):
   return v2 if path==Path('/sys/fs/cgroup/cgroup.controllers') else original_file(path,*args,**kwargs)
  def exists(path,*args,**kwargs):
   return populated is not None if path==group else original_exists(path,*args,**kwargs)
  def read(path,*args,**kwargs):
   if path==group/'cgroup.events':
    if populated is None:raise FileNotFoundError(str(path))
    return f'populated {populated}\nfrozen 0\n'
   return original_read(path,*args,**kwargs)
  with patch.object(Path,'is_file',is_file),patch.object(Path,'exists',exists),patch.object(Path,'read_text',read):yield
 def test_pg_and_listener_absent_bootstrap_pg_before_entry_then_finish_original_entry(self):
  fixture,profile,saved,entryrow=self.absent_entry_fixture();host=fixture.host;fixture.fail(h.PG,137);up=[False]
  def control(*args,**kwargs):
   if not up[0]:raise ConnectionRefusedError('owned entry stopped')
   return {'slot':'candidate','phase':'active','epoch':1}
  host.entry.entry_state=control;commands=[]
  with self.root_host_namespace(),self.entry_cgroup_layout(profile),patch.object(h,'no_entry_listeners',return_value=None),patch.object(h.os.path,'lexists',return_value=False),patch.object(h,'run',side_effect=lambda args,**kwargs:commands.append(args) or b''):
   result=host.runtime_step();self.assertTrue(result['entryBootstrapRequired']);self.assertIn(('start',h.PG),fixture.calls);self.assertEqual(host.j['phase'],'runtime-entry-pending');self.assertFalse(any(call[0]=='remove_gate' for call in fixture.calls));self.assertEqual(commands[-1],['/usr/bin/systemctl','start','--no-block','harbor-gateway-entry.service'])
   up[0]=True;entryrow['State'].update(Running=True,Pid=444,Status='running');host.entry.socket_owner=lambda path:444;host.entry.process_cgroup=lambda pid:profile['entryCgroup'];self.assertTrue(host.runtime_step()['entryRestored'])
  self.assertEqual(host.j['phase'],'complete');self.assertTrue(all(call[1].get('entryBootstrap') for call in fixture.calls if call[0]=='saved' and call[1].get('phase')=='runtime-entry-pending'));self.assertNotIn('entryBootstrap',host.j);self.assertTrue(any(call[0]=='remove_gate' for call in fixture.calls));self.assertTrue(any(call[0]=='restore_writers' for call in fixture.calls))
 def test_unknown_entry_resource_or_changed_guard_never_bootstraps_pg(self):
  for case,error in [('running','host_entry_resources_not_absent'),('guard','host_entry_guard_changed'),('mangle','host_entry_guard_changed'),('tc','host_entry_guard_tc_changed'),('candidate','candidate_unowned')]:
   fixture,profile,saved,entryrow=self.absent_entry_fixture();host=fixture.host;fixture.fail(h.PG,1);host.entry.entry_state=lambda *args,**kwargs:(_ for _ in ()).throw(ConnectionRefusedError())
   if case=='running':entryrow['State'].update(Running=True,Pid=9,Status='running')
   elif case=='guard':saved['rulesDigest']='changed'
   elif case=='mangle':saved['mangleDigest']='changed'
   elif case=='tc':host.entry.tc_digest=lambda device:'changed'
   else:host.entry.refreshed_bypass=lambda profile:(_ for _ in ()).throw(RuntimeError('candidate_unowned'))
   with self.root_host_namespace(),self.entry_cgroup_layout(profile),patch.object(h,'no_entry_listeners',return_value=None),patch.object(h.os.path,'lexists',return_value=False),patch.object(h,'run',side_effect=AssertionError('no systemd submission')):
    with self.assertRaisesRegex(RuntimeError,error):host.runtime_step()
   self.assertFalse(any(call[0] in ['start','stop','remove_gate'] for call in fixture.calls));self.assertNotIn('entryBootstrap',host.j)
 def test_empty_v2_group_proves_entry_absent_without_host_state(self):
  fixture,profile,saved,entryrow=self.absent_entry_fixture();host=fixture.host;host.install_gate()
  with self.root_host_namespace(),self.entry_cgroup_layout(profile,populated=0),patch.object(h,'no_entry_listeners',return_value=None),patch.object(h.os.path,'lexists',return_value=False):host.verify_absent_entry_bootstrap()
  self.assertEqual(host.j['entryBootstrap']['containerId'],'owned-entry');self.assertFalse(any(call[0]=='start' for call in fixture.calls))
 def test_missing_v2_layout_never_bootstraps_pg(self):
  fixture,profile,saved,entryrow=self.absent_entry_fixture();host=fixture.host;fixture.fail(h.PG,1);host.entry.entry_state=lambda *args,**kwargs:(_ for _ in ()).throw(ConnectionRefusedError())
  with self.root_host_namespace(),self.entry_cgroup_layout(profile,v2=False),patch.object(h,'no_entry_listeners',side_effect=AssertionError('layout must be checked first')),patch.object(h,'run',side_effect=AssertionError('no systemd submission')):
   with self.assertRaisesRegex(RuntimeError,'host_entry_cgroup_layout_unknown'):host.runtime_step()
  self.assertFalse(any(call[0] in ['start','stop','remove_gate','restore_writers'] for call in fixture.calls));self.assertNotIn('entryBootstrap',host.j)
 def test_listener_cgroup_descendants_and_wrong_control_owner_keep_gate(self):
  fixture,profile,saved,entryrow=self.absent_entry_fixture();host=fixture.host;host.entry.entry_state=lambda *args,**kwargs:(_ for _ in ()).throw(ConnectionRefusedError());fixture.fail(h.PG,1)
  with self.root_host_namespace(),self.entry_cgroup_layout(profile,populated=1),patch.object(h,'no_entry_listeners',return_value=None):
   with self.assertRaisesRegex(RuntimeError,'host_entry_resources_not_absent'):host.runtime_step()
  self.assertFalse(any(call[0]=='start' for call in fixture.calls))
  host.j['phase']='runtime-entry-pending';binding=host.upgrade.digest(host.upgrade.active_identity(fixture.active));host.j['entryBootstrap']={'bootId':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'profileDigest':'profile','activeIdentityDigest':binding,'containerId':'owned-entry','imageId':'owned-image'};entryrow['State'].update(Running=True,Pid=444,Status='running');host.entry.entry_state=lambda *args,**kwargs:{'phase':'active','slot':'candidate'};host.entry.socket_owner=lambda path:999;host.entry.process_cgroup=lambda pid:profile['entryCgroup'];fixture.calls=[]
  with self.assertRaisesRegex(RuntimeError,'host_entry_bootstrap_owner_changed'):host.finish_runtime_entry()
  self.assertFalse(any(call[0] in ['start','remove_gate','restore_writers'] for call in fixture.calls))
 def test_entry_pending_phase_remains_armed_for_ordinary_cold_boot(self):
  self.assertIn('runtime-entry-pending',core.BOOT_PHASES)
 def test_actual_owned_tcp_and_unix_listener_collision_is_not_absence(self):
  with tempfile.TemporaryDirectory(prefix='harbor-entry-proof-') as directory:
   path=Path(directory)/'control.sock'
   with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as peer:
    peer.bind(str(path));peer.listen()
    with self.assertRaisesRegex(RuntimeError,'host_entry_control_collision'):h.no_entry_listeners(0,path)
   h.no_entry_listeners(0,path)
   with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as peer:
    peer.bind(('127.0.0.1',0));peer.listen()
    with self.assertRaisesRegex(RuntimeError,'host_entry_listener_collision'):h.no_entry_listeners(peer.getsockname()[1],path)
 def test_wrapper_always_checks_failed_deploy_and_preserves_original_error(self):
  calls=[];helper=types.SimpleNamespace(deployment_begin=lambda *args:'nonce',deployment_finish=lambda cfg,nonce,success,fd:calls.append(success) or {'released':True})
  class OriginalFailure(RuntimeError):pass
  with patch.object(wrapper,'storage_operation_helper',return_value=helper),patch.object(wrapper,'deploy_entry_postgres',side_effect=OriginalFailure('synthetic')):
   with self.assertRaises(OriginalFailure):wrapper.storage_deployment({},'image',None,'deploy-entry-postgres',12)
  self.assertEqual(calls,[False])
 def test_wrapper_does_not_report_success_while_planned_result_remains_unknown(self):
  helper=types.SimpleNamespace(deployment_begin=lambda *args:'nonce',deployment_finish=lambda *args:{'released':False})
  with patch.object(wrapper,'storage_operation_helper',return_value=helper),patch.object(wrapper,'deploy_entry_postgres',return_value={}):
   with self.assertRaisesRegex(RuntimeError,'planned operation result remains protected'):wrapper.storage_deployment({},'image',None,'deploy-entry-postgres',12)
 def test_begin_and_failed_finish_share_owned_deploy_lock_and_reload_state(self):
  fixture=Harness();fixture.host.config={'postgres_storage_startup_guard':True};fixture.host.plan={'runId':'a'*32};fixture.host.reload_locked_state=lambda:fixture.calls.append(('reload',));fixture.host.verify_runtime_facts=fixture.effect('facts');before=fixture.host.upgrade.digest(fixture.host.upgrade.active_identity(fixture.active));ticket={'from':{'activeIdentityDigest':before},'to':{'image':'new-image'}};fixture.host.upgrade.TICKET='TICKET';fixture.host.upgrade.private_json=lambda _:copy.deepcopy(ticket);fixture.host.upgrade.validate_ticket=lambda value,*args:value;fixture.host.upgrade.approval=lambda value:{'id':'next'};fixture.host.upgrade.approval_binding=lambda value:value
  @contextlib.contextmanager
  def locked(host,fd):
   self.assertEqual(fd,45);fixture.calls.append(('locked',));yield
  with patch.object(h,'Host',return_value=fixture.host),patch.object(h,'locks',side_effect=locked):
   token=h.deployment_begin(fixture.host.config,'new-image',45);self.assertEqual(fixture.host.j['runtimeControl']['mode'],'planned');self.assertEqual(fixture.host.j['plannedOperation']['targetTicketDigest'],fixture.host.upgrade.digest(ticket));self.assertEqual(fixture.host.j['plannedOperation']['targetApproval'],{'id':'next'});self.assertEqual(fixture.calls[0],('locked',));self.assertEqual(fixture.calls[1],('reload',));self.assertTrue(h.deployment_finish(fixture.host.config,token,False,45)['released'])
  self.assertEqual(fixture.host.j['runtimeControl'],{'mode':'running'})
 def test_approval_source_mismatch_rejects_before_planned_state_is_written(self):
  fixture=Harness();fixture.host.config={'postgres_storage_startup_guard':True};fixture.host.plan={'runId':'a'*32};fixture.host.reload_locked_state=lambda:None;fixture.host.upgrade.TICKET='TICKET';fixture.host.upgrade.private_json=lambda _:{'from':{'activeIdentityDigest':'foreign'}};fixture.host.upgrade.validate_ticket=lambda value,*args:value;before=copy.deepcopy(fixture.host.j);fixture.calls=[]
  @contextlib.contextmanager
  def locked(*args):yield
  with patch.object(h,'Host',return_value=fixture.host),patch.object(h,'locks',side_effect=locked):
   with self.assertRaisesRegex(RuntimeError,'host_planned_source_approval_changed'):h.deployment_begin(fixture.host.config,'image',45)
  self.assertEqual(fixture.host.j,before);self.assertFalse(any(call[0]=='saved' for call in fixture.calls))
 def test_legacy_wrapper_has_no_storage_side_effects(self):
  self.assertIsNone(wrapper.storage_operation_helper({}));self.assertIsNone(wrapper.storage_operation_helper({'postgres_storage_startup_guard':False}))
  with self.assertRaises(ValueError):wrapper.storage_operation_helper({'postgres_storage_startup_guard':1})
 def test_supervisor_unit_is_one_existing_notify_process_and_no_per_container_units(self):
  units=core.startup_units('db.mount','archive.mount');self.assertIn('Type=notify\nNotifyAccess=main\nRestart=always',units[core.STORAGE_ONLINE]);self.assertNotIn('RemainAfterExit=',units[core.STORAGE_ONLINE]);self.assertEqual({name for name in units if name.endswith('.service')},{core.STORAGE_START,core.STORAGE_ONLINE})
 def test_paused_boot_cannot_start_pg_or_require_application_health(self):
  ops=types.SimpleNamespace();journal={'runtimeControl':{'mode':'paused'}};self.assertTrue(core.Coordinator(ops,journal).boot_start()['paused'])
 def test_actual_owned_process_exit_zero_and_nonzero_enter_same_recovery_path(self):
  for code in [0,7]:
   child=subprocess.Popen(['/bin/sh','-c','exit '+str(code)]);self.assertEqual(child.wait(timeout=2),code);fixture=Harness();fixture.fail(h.WORKER,child.returncode);self.assertTrue(fixture.host.runtime_step()['recovered']);self.assertIn(('start',h.WORKER),fixture.calls)
if __name__=='__main__':unittest.main()
