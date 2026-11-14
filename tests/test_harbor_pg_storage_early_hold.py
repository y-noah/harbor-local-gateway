import copy,importlib.util,json,tempfile,types,unittest
from pathlib import Path
from unittest.mock import Mock
ROOT=Path(__file__).resolve().parents[1]
def load(name):
 spec=importlib.util.spec_from_file_location(name,ROOT/'ci'/('harbor-pg-storage-'+name+'.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
h=load('host');c=load('coordinator')
class Fixture:
 def __init__(self):
  self.host=object.__new__(h.Host);o=self.host;o.j={'phase':'holding-intent','writers':{}};o.plan={'runId':'test'};o.planSha256='fixed';self.facts={'identity':'same'};self.control={'slot':'candidate','phase':'active','epoch':8};self.commands=[]
  o.early_source_facts=lambda:copy.deepcopy(self.facts);o.save=Mock();o.load_current_durable_active=Mock();o.source_low_recovery_selected=lambda:True
  o.immutable=lambda name:{'State':{'Health':{'Status':'healthy' if name!=h.WORKER else 'unhealthy'}}}
  o.entry=types.SimpleNamespace(entry_state=self.entry,execute=lambda args:json.dumps({'nftables':[]}).encode());o.upgrade=types.SimpleNamespace(GATE='gate');o.core=c
  o.capture_early_hold()
 def entry(self,command=None,epoch=None):
  if command is not None:
   self.commands.append((command,epoch));assert command=='resume';assert epoch==self.control['epoch'];self.control.update(phase='active',epoch=epoch+1)
  return copy.deepcopy(self.control)
 def held(self):self.control.update(phase='holding',epoch=9);self.host.j['earlyHoldRecovery']['holdRequested']=True;return self.host
class Tests(unittest.TestCase):
 def test_hold_only_resumes_even_with_inflight_without_draining_or_restarting(self):
  f=Fixture();o=f.held();f.control['inflight']=19
  result=c.Coordinator(o,o.j).recover_source();self.assertTrue(result['holdOnly']);self.assertFalse(result['servicesRestarted']);self.assertEqual(f.commands,[('resume',9)]);self.assertEqual(o.j['phase'],'source-restored')
 def test_hold_not_applied_needs_no_resume(self):
  f=Fixture();self.assertTrue(f.host.recover_held_source()['entryAlreadyActive']);self.assertEqual(f.commands,[])
 def test_generation_epoch_candidate_drift_refused(self):
  for kind in ['facts','epoch','slot','phase']:
   f=Fixture();o=f.held()
   if kind=='facts':f.facts['identity']='changed'
   else:f.control[kind]={'epoch':12,'slot':'legacy','phase':'drained'}[kind]
   with self.assertRaisesRegex(RuntimeError,'host_early_'):o.recover_held_source()
   self.assertEqual(f.commands,[])
 def test_progressed_switch_keeps_full_recovery(self):
  for key in ['gate','gateIntent','writersPauseStarted','restartPolicies','autostartFenced','oldRoots','mountpointIdentities','oldDataRollbackForbidden','targetBootAuthorized']:
   f=Fixture();o=f.held();o.j[key]=True;self.assertIsNone(o.recover_held_source());self.assertEqual(f.commands,[])
 def test_unrecorded_gate_refused(self):
  f=Fixture();o=f.held();o.entry.execute=lambda args:json.dumps({'nftables':[{'table':{'name':'gate'}}]}).encode()
  with self.assertRaisesRegex(RuntimeError,'host_early_gate_present'):o.recover_held_source()
  self.assertEqual(f.commands,[])
 def test_no_receipt_does_not_infer_permission(self):
  f=Fixture();del f.host.j['earlyHoldRecovery'];self.assertIsNone(f.host.recover_held_source())
 def test_unhealthy_gateway_refuses_resume(self):
  f=Fixture();o=f.held();o.immutable=lambda name:{'State':{'Health':{'Status':'unhealthy'}}}
  with self.assertRaisesRegex(RuntimeError,'host_early_source_not_ready'):o.recover_held_source()
 def test_resume_lost_ack_no_second_mutation(self):
  f=Fixture();o=f.held();original=f.entry
  def lost(command=None,epoch=None):
   result=original(command,epoch)
   if command:raise TimeoutError('private detail')
   return result
  o.entry.entry_state=lost
  with self.assertRaises(TimeoutError):o.recover_held_source()
  self.assertTrue(o.recover_held_source()['resumeAcknowledgedByObservation'])
  self.assertEqual(f.commands,[('resume',9)])
 def test_hold_cas_rejects_changed_epoch_before_side_effect(self):
  f=Fixture();o=f.host;o.upgrade.drain_active_entry=lambda entry:entry.entry_state('hold',11)
  with self.assertRaisesRegex(RuntimeError,'host_early_hold_epoch_changed'):o.drain_with_early_receipt()
  self.assertNotIn('holdRequested',o.j['earlyHoldRecovery']);self.assertEqual(f.commands,[])
 def test_foreign_hold_without_recorded_request_is_not_resumed(self):
  f=Fixture();f.control.update(phase='holding',epoch=9)
  with self.assertRaisesRegex(RuntimeError,'host_early_hold_unconfirmed'):f.host.recover_held_source()
  self.assertEqual(f.commands,[])
 def test_actual_source_facts_track_all_container_generations_and_roots(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory);db=root/'db';archive=root/'archive';db.mkdir();archive.mkdir();o=object.__new__(h.Host);o.j={}
   rows={name:{'Id':name,'Image':'same','Config':{},'HostConfig':{'RestartPolicy':{'Name':'unless-stopped'}},'Mounts':[],'State':{'Running':True,'Pid':22,'StartedAt':'start'},'RestartCount':0,'NetworkSettings':{'SandboxID':'sandbox','Networks':{}}} for name in [h.PG,h.GW,h.WORKER]}
   o.row=lambda name:copy.deepcopy(rows[name]);o.plan={'containers':{name:{'id':name,'signature':h.sig(row),'restartPolicy':row['HostConfig']['RestartPolicy']} for name,row in rows.items()},'sourceRoots':{'db':c.identity(db.stat())[:5],'archive':c.identity(archive.stat())[:5]},'activeIdentityDigest':'active'}
   o.core=types.SimpleNamespace(SOURCE_DB=db,SOURCE_ARCHIVE=archive,BASE=root,identity=c.identity);active={'candidate':{'containerId':h.GW}};o.ticket={}
   o.upgrade=types.SimpleNamespace(active_identity=lambda value:'active',digest=lambda value:value if isinstance(value,str) else json.dumps(value,sort_keys=True),network_snapshot=lambda *args:{'candidateNetworkContainerIds':[h.WORKER,h.PG,h.GW]})
   o.entry=types.SimpleNamespace(ACTIVE='active',STATE='state',verify_resources=Mock(),CONTROL='/control',private_json=lambda path:copy.deepcopy(active) if path=='active' else {'phase':'activated','activeRuntimeConfig':{},'candidate':active['candidate']},socket_owner=lambda path:44,process_ticks=lambda pid:100,process_cgroup=lambda pid:'entry-group')
   o.runtime=types.SimpleNamespace(inspect_entry=lambda entry:{'Id':'entry','State':{'Pid':44,'Running':True,'StartedAt':'entry-start'}})
   baseline=o.early_source_facts()
   for name in [h.PG,h.GW,h.WORKER]:
    rows[name]['State']['Pid']+=1;self.assertNotEqual(o.early_source_facts(),baseline);rows[name]['State']['Pid']-=1
    rows[name]['RestartCount']+=1;self.assertNotEqual(o.early_source_facts(),baseline);rows[name]['RestartCount']-=1
   for rule in ['nft','mangle','tc']:
    o.entry.verify_resources.side_effect=RuntimeError('rule_'+rule)
    with self.assertRaisesRegex(RuntimeError,'rule_'+rule):o.early_source_facts()
   o.entry.verify_resources.side_effect=None
   data=root/'application-data';data.mkdir();expected=[data.stat().st_dev,data.stat().st_ino]
   mover=types.SimpleNamespace(STATE=root/'move-state',g=types.SimpleNamespace(CONFIG=root/'config',MAINTENANCE=root/'maintenance',identity=lambda path:c.identity(path.lstat())))
   o.material_mover=lambda:(mover,{},[{'logical':str(data),'sourceId':expected}])
   for marker in [mover.STATE,mover.g.CONFIG,mover.g.MAINTENANCE]:
    marker.touch()
    with self.assertRaisesRegex(RuntimeError,'host_source_material_authority_changed'):o.early_source_facts()
    marker.unlink()
   data.rename(root/'old-data');data.mkdir()
   with self.assertRaisesRegex(RuntimeError,'host_source_material_root_changed'):o.early_source_facts()
   o.material_mover=lambda:None
   db.rename(root/'old-db');db.mkdir()
   with self.assertRaisesRegex(RuntimeError,'host_original_root_changed'):o.early_source_facts()
 def test_successful_drain_goes_to_complete_recovery(self):
  for phase in ['holding-intent','held']:
   f=Fixture();o=f.held();o.j['phase']=phase;o.j['earlyHoldRecovery']['drainRequested']=True;f.control.update(phase='drained',epoch=10)
   self.assertIsNone(o.recover_held_source());self.assertEqual(f.commands,[])
 def test_explicit_hold_rejection_never_resumes_competitor(self):
  f=Fixture();o=f.host
  def reject(command=None,epoch=None):
   f.control.update(phase='holding',epoch=9)
   class EntryControlError(RuntimeError):pass
   error=EntryControlError('entry_control_rejected');error.diagnostic={'reason':'entry_epoch_or_operation_conflict'};raise error
  o.entry.entry_state=reject;o.upgrade.drain_active_entry=lambda entry:entry.entry_state('hold',8)
  with self.assertRaises(RuntimeError):o.drain_with_early_receipt()
  o.entry.entry_state=f.entry
  with self.assertRaisesRegex(RuntimeError,'host_early_hold_unconfirmed'):o.recover_held_source()
  self.assertEqual(f.commands,[])
 def test_resume_without_own_persisted_intent_is_not_acknowledged(self):
  f=Fixture();o=f.held();f.control.update(phase='active',epoch=10)
  with self.assertRaisesRegex(RuntimeError,'host_early_entry_changed'):o.recover_held_source()
  self.assertEqual(f.commands,[])
 def test_drained_unhealthy_or_stopped_source_keeps_complete_recovery(self):
  for code in ['host_early_process_not_running','host_early_source_not_ready']:
   f=Fixture();o=f.held();o.j['earlyHoldRecovery']['drainRequested']=True;f.control.update(phase='drained',epoch=10)
   o.early_source_facts=Mock(side_effect=RuntimeError(code));o.immutable=Mock(side_effect=RuntimeError(code))
   o.verify_original_recovery_checkpoint=Mock();o.restore_original_restart_policies=Mock();o.start_original_postgres=Mock();o.verify_original_postgres=Mock();o.start_applications=Mock();o.verify_current_approval_and_private_readiness=Mock();o.update_dynamic_network_snapshot=Mock();o.managed_source_recovery=lambda:False;o.refresh_active_entry=Mock();o.verify_original_full_instance=Mock();o.restore_writers=Mock();o.verify_writer_restoration=Mock()
   result=c.Coordinator(o,o.j).recover_source();self.assertTrue(result['sourceRestored']);o.start_original_postgres.assert_called_once();o.start_applications.assert_called_once();o.early_source_facts.assert_not_called();self.assertEqual(f.commands,[])
 def test_unknown_control_exception_is_not_mislabelled_as_rejection(self):
  self.assertFalse(h.rejected_control(RuntimeError('entry_control_unknown')))
 def test_safe_failure_has_stage_and_type_but_no_exception_text(self):
  h.diagnostic_stage('hold_drain');out=h.failure_summary(KeyError('secret-value'));self.assertEqual(out,{'error':'storage_move_failed_closed','stage':'hold_drain','exceptionType':'KeyError'});self.assertNotIn('secret-value',json.dumps(out))
 def test_entry_diagnostic_and_upgrade_reasons_are_bounded(self):
  class EntryControlError(RuntimeError):pass
  error=EntryControlError('secret raw error');error.diagnostic={'reason':'gateway_entry_drain_timeout','phase':'holding','slot':'candidate','epoch':9,'inflight':4,'queued':12,'secret':'private','body':'private','other':True}
  value=h.failure_summary(error);self.assertEqual(value['entryControl'],{'reason':'gateway_entry_drain_timeout','phase':'holding','slot':'candidate','epoch':9,'inflight':4,'queued':12});self.assertNotIn('private',json.dumps(value));self.assertNotIn('secret',json.dumps(value))
  error.diagnostic={'reason':'private','phase':'private','slot':'private','epoch':True,'inflight':-1,'queued':2**60};self.assertNotIn('entryControl',h.failure_summary(error))
  self.assertEqual(h.failure_summary(RuntimeError('PostgreSQL runtime upgrade: entry drain timeout'))['upgradeReason'],'entry_drain_timeout')
  self.assertEqual(h.failure_summary(RuntimeError('PostgreSQL runtime upgrade: entry TCP socket ownership unknown'))['upgradeReason'],'entry_tcp_socket_ownership_unknown')
  self.assertNotIn('upgradeReason',h.failure_summary(RuntimeError('PostgreSQL runtime upgrade: arbitrary private data')))
if __name__=='__main__':unittest.main()
