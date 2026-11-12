import copy,importlib.util,json,subprocess,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
ROOT=Path(__file__).parents[1]
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,ROOT/path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
base=load('consumption_upgrade_base','tests/test_postgres_runtime_upgrade.py');upgrade=base.upgrade

def fixture():
 ticket=base.ticket();gateway={'Id':ticket['from']['containerId'],'Image':ticket['from']['imageId'],'Config':{'Image':base.OLD,'Cmd':['node','/app/postgres-gateway.mjs'],'Labels':{'com.aistrong.project':base.NAME}},'State':{'Running':True}}
 worker={**gateway,'Id':ticket['from']['workerId'],'Config':{**gateway['Config'],'Cmd':['node','/app/postgres-worker.mjs']}}
 rows={base.NAME:gateway,base.NAME+'-worker':worker};entry=Mock();entry.private_json.return_value=base.active()
 api={'NAME':base.NAME,'inspect_container':rows.get,'postgres_preflight_timeout_seconds':30,'source_image_preflight':Mock(return_value={}), 'docker':Mock(return_value=SimpleNamespace(stdout=json.dumps({'contract':'harbor-consumption-legacy-key-snapshot-v1','database_digest':ticket['databaseDigest'],'explicit':False,'invalid':False})))}
 pg=SimpleNamespace(SourcePreflightRejected=base.pg.SourcePreflightRejected,SHUTDOWN_ENV=base.pg.SHUTDOWN_ENV,container_shutdown=Mock(return_value={'gateway_drain_ms':1000,'cleanup_grace_ms':1000,'worker_stop_ms':1000}))
 common=['--env','HARBOR_GATEWAY_DRAIN_TIMEOUT_MS=1000','--env','HARBOR_SHUTDOWN_CLEANUP_GRACE_MS=1000','--env','HARBOR_WORKER_STOP_TIMEOUT_MS=1000']
 return ticket,gateway,worker,entry,api,pg,common

class ConsumptionUpgradeTests(unittest.TestCase):
 def test_new_target_does_not_add_source_queries_or_change_original_hook(self):
  t,g,w,e,a,p,c=fixture();original=a['source_image_preflight'];upgrade.consumption_upgrade_guard(p,a,t,e,g,c,{'consumption_policy_contract':'v1'});self.assertIs(a['source_image_preflight'],original);original.assert_not_called();a['docker'].assert_not_called();p.container_shutdown.assert_not_called()
 def test_new_source_rejects_old_target_even_without_explicit_keys(self):
  t,g,w,e,a,p,c=fixture();a['source_image_preflight'].return_value={'consumption_policy_contract':'v1'}
  with self.assertRaisesRegex(RuntimeError,'consumption_policy_target_unsupported'):upgrade.consumption_upgrade_guard(p,a,t,e,g,c,{})
  a['docker'].assert_not_called();e.entry_state.assert_not_called()
 def test_legacy_combination_reads_only_verified_source_and_rechecks_on_stop_path(self):
  t,g,w,e,a,p,c=fixture();upgrade.consumption_upgrade_guard(p,a,t,e,g,c,{});a['source_image_preflight'](g,c);self.assertEqual(a['docker'].call_count,2)
  command=a['docker'].call_args.args;self.assertEqual(command[:5],('exec',g['Id'],'node','--input-type=module','-e'));self.assertIn("readOnly:true",command[5]);self.assertNotIn('HARBOR_DATABASE_URL=',command[5]);self.assertNotIn('UPDATE harbor.keys',command[5])
 def test_explicit_key_between_checks_rejects_before_stop(self):
  t,g,w,e,a,p,c=fixture();safe=json.loads(a['docker'].return_value.stdout);a['docker'].side_effect=[SimpleNamespace(stdout=json.dumps(safe)),SimpleNamespace(stdout=json.dumps({**safe,'explicit':True}))];upgrade.consumption_upgrade_guard(p,a,t,e,g,c,{})
  with self.assertRaisesRegex(RuntimeError,'consumption_policy_explicit_keys_require_v1_target'):a['source_image_preflight'](g,c)
 def test_source_contract_drift_rejects_before_second_query(self):
  t,g,w,e,a,p,c=fixture();original=a['source_image_preflight'];original.side_effect=[{}, {'consumption_policy_contract':'v1'}];upgrade.consumption_upgrade_guard(p,a,t,e,g,c,{})
  with self.assertRaisesRegex(RuntimeError,'consumption_policy_target_unsupported'):a['source_image_preflight'](g,c)
  self.assertEqual(a['docker'].call_count,1)
 def test_malformed_contracts_fail_closed(self):
  for value in [None,False,True,1,'v2',{},[]]:
   with self.subTest(value=value):
    t,g,w,e,a,p,c=fixture()
    with self.assertRaisesRegex(RuntimeError,'consumption_policy_contract_unknown'):upgrade.consumption_upgrade_guard(p,a,t,e,g,c,{'consumption_policy_contract':value})
    a['docker'].assert_not_called()
 def test_unknown_source_proof_is_not_treated_as_legacy(self):
  for value in [None,False,[],Mock(),{'consumption_policy_contract':'v2'}]:
   with self.subTest(value=type(value).__name__):
    t,g,w,e,a,p,c=fixture();a['source_image_preflight'].return_value=value
    with self.assertRaises(RuntimeError):upgrade.consumption_upgrade_guard(p,a,t,e,g,c,{})
    a['docker'].assert_not_called()
 def test_query_unknown_or_invalid_output_rejected_without_echo(self):
  for patch_data in [{'database_digest':'wrong'},{'explicit':1},{'invalid':True},{'secret':'synthetic-hidden'}]:
   with self.subTest(value=patch_data):
    t,g,w,e,a,p,c=fixture();value={**json.loads(a['docker'].return_value.stdout),**patch_data};a['docker'].return_value=SimpleNamespace(stdout=json.dumps(value))
    with self.assertRaises(RuntimeError) as error:upgrade.source_consumption_snapshot(a,t,e,g)
    self.assertNotIn('synthetic-hidden',str(error.exception));e.entry_state.assert_not_called()
 def test_unavailable_query_fails_closed(self):
  t,g,w,e,a,p,c=fixture();a['docker'].side_effect=subprocess.TimeoutExpired('synthetic',30)
  with self.assertRaisesRegex(RuntimeError,'consumption_policy_query_unavailable'):upgrade.source_consumption_snapshot(a,t,e,g)
 def test_source_identity_rechecked_before_query(self):
  t,g,w,e,a,p,c=fixture();a['inspect_container']=lambda name:{**g,'Id':'f'*64} if name==base.NAME else w
  with self.assertRaisesRegex(RuntimeError,'from PostgreSQL composition changed'):upgrade.source_consumption_snapshot(a,t,e,g)
  a['docker'].assert_not_called()
 def test_new_source_old_target_is_rejected_before_hold_or_composition(self):
  t,g,w,e,a,p,c=fixture();e.active_record.return_value=(base.active(),{},g);p.preflight_timeout_seconds=base.pg.preflight_timeout_seconds;p.configuration=Mock(return_value=(c,{}));p.verify_candidate_image=Mock();p.image_proof=Mock(return_value={'database_digest':t['databaseDigest'],'schema_digest':t['schemaDigest'],'device_ca_digest':t['deviceCaDigest']});p.prepare_source_image_preflights=Mock(return_value=Mock(return_value={'consumption_policy_contract':'v1'}));p.deploy=Mock();a['inspect_image']=Mock()
  with tempfile.TemporaryDirectory() as directory,patch.object(upgrade,'RELEASES',Path(directory)),patch.object(upgrade,'load',return_value=p),patch.object(upgrade,'check_storage'),patch.object(upgrade,'verify_installed_tools'),self.assertRaisesRegex(RuntimeError,'consumption_policy_target_unsupported'):
   upgrade.run_upgrade(base.IMAGE,base.CONFIG,a,t,e)
  e.entry_state.assert_not_called();e.execute.assert_not_called();p.deploy.assert_not_called()
 def test_second_guard_failure_resumes_unchanged_core_ready_source_without_full_preflight(self):
  for reason in ['query-timeout','metadata-unknown']:
   with self.subTest(reason=reason),tempfile.TemporaryDirectory() as directory:
    t,g,w,e,a,p,c=fixture();old=base.active();e.ACTIVE=Path(directory)/'active.json';e.STATE=Path(directory)/'controller.json';e.ENTRY_CONFIG=Path(directory)/'entry.json'
    data={str(e.ACTIVE):copy.deepcopy(old),str(e.STATE):{'phase':'activated','candidate':old['candidate'],'activeRuntimeConfig':{'bridge':{'address':'192.0.2.1'}}},str(e.ENTRY_CONFIG):{'drainTimeoutMs':500}}
    e.private_json.side_effect=lambda path:copy.deepcopy(data[str(path)]);e.save_json.side_effect=lambda path,value:data.__setitem__(str(path),copy.deepcopy(value));e.active_record.return_value=(copy.deepcopy(old),{},g)
    control={'slot':'candidate','phase':'active','inflight':0,'epoch':0}
    def entry_state(command=None,epoch=None,**kwargs):
     if command:
      self.assertEqual(epoch,control['epoch']);control['epoch']+=1;control['phase']={'hold':'holding','drain':'drained','resume':'active'}[command]
     return dict(control)
    e.entry_state.side_effect=entry_state
    p.preflight_timeout_seconds=base.pg.preflight_timeout_seconds;p.configuration=Mock(return_value=(c,{}));p.verify_candidate_image=Mock();p.image_proof=Mock(return_value={'database_digest':t['databaseDigest'],'schema_digest':t['schemaDigest'],'device_ca_digest':t['deviceCaDigest']});p.prepare_source_image_preflights=Mock(return_value=Mock(return_value={}));p.ShutdownUnconfirmed=base.pg.ShutdownUnconfirmed;p.verify_started_composition=Mock(side_effect=RuntimeError('full old storage preflight remains unavailable'))
    snapshots=0
    def query(*args,**kwargs):
     nonlocal snapshots
     snapshots+=1
     if snapshots==2 and reason=='query-timeout':raise subprocess.TimeoutExpired('synthetic source query',30)
     return SimpleNamespace(stdout=json.dumps({'contract':'harbor-consumption-legacy-key-snapshot-v1','database_digest':t['databaseDigest'],'explicit':False,'invalid':snapshots==2}))
    a['docker']=Mock(side_effect=query);a['inspect_image']=Mock()
    def compose(*args):
     self.assertEqual(control['phase'],'drained');args[-1]['source_image_preflight'](g,c);self.fail('guard must reject before source stop or candidate start')
    p.deploy=Mock(side_effect=compose)
    with patch.object(upgrade,'RELEASES',Path(directory)),patch.object(upgrade,'load',return_value=p),patch.object(upgrade,'check_storage'),patch.object(upgrade,'network_snapshot',return_value={}),patch.object(upgrade,'verify_installed_tools'),patch.object(upgrade,'gate_install',return_value={'owned':True}),patch.object(upgrade,'gate_remove') as remove,patch.object(upgrade,'probe',return_value=200):
     with self.assertRaises(base.pg.SourcePreflightRejected):upgrade.run_upgrade(base.IMAGE,base.CONFIG,a,t,e)
    self.assertEqual(snapshots,2);self.assertEqual(control['phase'],'active');self.assertTrue(g['State']['Running']);self.assertTrue(w['State']['Running']);remove.assert_called_once();p.verify_started_composition.assert_not_called();e.refresh_active.assert_not_called();self.assertEqual(data[str(Path(directory)/t['deploymentId']/'release.json')]['errorCategory'],'source_preflight_rejected')
if __name__=='__main__':unittest.main()
