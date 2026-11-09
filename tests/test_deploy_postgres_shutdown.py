import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch,Mock
import test_deploy_postgres as base
pg=base.pg

class PostgreSQLShutdownTests(unittest.TestCase):
 prepare=base.PostgreSQLDeploymentTests.prepare
 api=base.PostgreSQLDeploymentTests.api
 def test_root_shutdown_bounds_and_boolean_values_are_rejected(self):
  for value in [{},{**pg.SHUTDOWN_DEFAULTS,'gateway_drain_ms':True},{**pg.SHUTDOWN_DEFAULTS,'gateway_drain_ms':99},{**pg.SHUTDOWN_DEFAULTS,'cleanup_grace_ms':0},{**pg.SHUTDOWN_DEFAULTS,'worker_stop_ms':3600001},{**pg.SHUTDOWN_DEFAULTS,'other':1}]:
   with self.assertRaises(ValueError):pg.shutdown_configuration(value)
 def test_running_legacy_is_rejected_before_marker_or_any_stop(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);state[base.NAME]['Config']['Labels'].pop('com.harbor.shutdown-contract')
   with self.assertRaisesRegex(pg.ShutdownUnconfirmed,'maintenance'):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertFalse((Path(directory)/'postgres-storage.json').exists());self.assertFalse(any(call[0]=='stop' for call in calls))
 def test_stopped_legacy_is_explicitly_unclassified(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory)
   for name in [base.NAME,base.NAME+'-worker']:
    state[name]['State'].update(Running=False,Status='exited');state[name]['Config']['Labels'].pop('com.harbor.shutdown-contract')
   result=pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertEqual([row['outcome'] for row in result['shutdown']],['pre_stopped_unclassified']*2)
 def test_old_actual_budget_controls_stop_and_precedes_worker_stop(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory)
   values={**pg.SHUTDOWN_DEFAULTS,'gateway_drain_ms':600000,'cleanup_grace_ms':45000,'worker_stop_ms':70000}
   for name in [base.NAME,base.NAME+'-worker']:state[name]['Config']['Env']=[label+'='+str(values[key]) for key,label in pg.SHUTDOWN_ENV.items()]
   api['proofs'][base.OLD]['shutdown']={'contract':pg.SHUTDOWN_CONTRACT,**values}
   pg.deploy(base.IMAGE,base.CONFIG,None,api)
   stops=[call for call in calls if call[0]=='stop'];self.assertEqual(stops,[('stop','-t','645',base.NAME),('stop','-t','70',base.NAME+'-worker')])
   runs=[call for call in calls if call[:2]==('run','-d')];self.assertEqual([call[call.index('--stop-timeout')+1] for call in runs],['330','30'])
 def test_changed_tag_unknown_command_or_duplicate_effective_value_reject_before_stop(self):
  for alteration in ['image','command','env','node-options']:
   with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
    self.prepare(directory);api,calls,state=self.api(directory)
    if alteration=='image':state[base.NAME]['Image']='sha256:'+'f'*64
    elif alteration=='command':state[base.NAME]['Config']['Cmd']=['node','other.mjs']
    elif alteration=='env':state[base.NAME]['Config']['Env'].append('HARBOR_GATEWAY_DRAIN_TIMEOUT_MS=1')
    else:state[base.NAME]['Config']['Env'].append('NODE_OPTIONS=--require=/tmp/inject.js')
    with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
    self.assertFalse(any(call[0]=='stop' for call in calls));self.assertFalse((Path(directory)/'postgres-storage.json').exists())
 def test_forced_gateway_exit_two_is_explicit_and_can_continue_after_cleanup(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);original=api['docker']
   def docker(*args,**kwargs):
    result=original(*args,**kwargs)
    if args[0]=='stop' and args[-1]==base.NAME:state[base.NAME]['State']['ExitCode']=2
    return result
   api['docker']=docker;result=pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertEqual(result['shutdown'][0]['outcome'],'interrupted_cleanup_complete');self.assertEqual(result['shutdown'][0]['exit_code'],2)
 def test_hard_kill_pending_or_missing_stop_proof_retains_pair_without_rollback(self):
  for code in [137,1,None]:
   with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
    self.prepare(directory);api,calls,state=self.api(directory);original=api['docker']
    def docker(*args,**kwargs):
     result=original(*args,**kwargs)
     if args[0]=='stop':state[args[-1]]['State']['ExitCode']=code
     return result
    api['docker']=docker
    with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
    self.assertTrue(state[base.NAME+'-worker']['State']['Running']);self.assertFalse(any(call[0] in ('start','rm','rename') for call in calls if not call[-1].startswith(base.NAME+'-postgres-check-')))
    self.assertEqual(json.loads((Path(directory)/'postgres-shutdown-diagnostics.json').read_text())['containers'][0]['outcome'],'unknown')
 def test_failed_candidate_shutdown_unknown_keeps_both_and_never_force_removes(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory,auth=False);original=api['docker']
   def docker(*args,**kwargs):
    result=original(*args,**kwargs)
    if args[0]=='stop' and state[args[-1]]['Config']['Image']==base.IMAGE:state[args[-1]]['State']['ExitCode']=137
    return result
   api['docker']=docker
   with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertIn(base.NAME,state);self.assertIn(base.NAME+'-worker',state);self.assertTrue(state[base.NAME+'-worker']['State']['Running']);self.assertFalse(any(call[0] in ('rm','start') for call in calls if not call[-1].startswith(base.NAME+'-postgres-check-')))

 def test_stopped_legacy_is_not_automatically_restarted_after_candidate_failure(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory,auth=False)
   for name in [base.NAME,base.NAME+'-worker']:
    state[name]['State'].update(Running=False,Status='exited');state[name]['Config']['Labels'].pop('com.harbor.shutdown-contract')
   with self.assertRaisesRegex(RuntimeError,'authentication'):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertFalse(any(call[0]=='start' for call in calls));self.assertFalse(state[base.NAME]['State']['Running'])

 def test_paused_restarting_and_unknown_container_state_reject_before_marker(self):
  for value in [{'Paused':True},{'Restarting':True},{'Running':None},{'Running':False,'Status':'dead'}]:
   with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
    self.prepare(directory);api,calls,state=self.api(directory);state[base.NAME]['State'].update(value)
    with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
    self.assertFalse((Path(directory)/'postgres-storage.json').exists());self.assertFalse(any(call[0]=='stop' for call in calls))

 def test_created_entrypoint_is_cleared_and_redeploys_under_its_own_contract(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);pg.deploy(base.IMAGE,base.CONFIG,None,api)
   runs=[call for call in calls if call[:2]==('run','-d')]
   self.assertTrue(all(call[call.index('--entrypoint')+1]=='' for call in runs));self.assertTrue(all(call[call.index('--stop-signal')+1]=='SIGTERM' for call in runs))
   pg.deploy(base.IMAGE,base.CONFIG,None,api)
 def test_stop_then_inspect_timeout_is_unknown_and_never_restarts_or_removes(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);original=api['docker'];original_inspect=api['inspect_container'];fault=False
   def docker(*args,**kwargs):
    nonlocal fault
    result=original(*args,**kwargs)
    if args[0]=='stop' and args[-1]==base.NAME+'-worker':state[base.NAME+'-worker']['State']['ExitCode']=137;fault=True
    return result
   def inspect(name):
    if fault and name==base.NAME+'-worker':raise subprocess.TimeoutExpired('inspect',10)
    return original_inspect(name)
   api['docker']=docker;api['inspect_container']=inspect
   with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertFalse(any(call[0] in ('start','rm') for call in calls if not call[-1].startswith(base.NAME+'-postgres-check-')));self.assertNotIn(('rename',base.NAME+'-previous',base.NAME),calls)
   self.assertEqual(json.loads((Path(directory)/'postgres-shutdown-diagnostics.json').read_text())['containers'][-1]['outcome'],'unknown')
 def test_label_without_actual_old_image_shutdown_proof_never_authorizes_stop(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);api['proofs'][base.OLD].pop('shutdown')
   with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertFalse(any(call[0]=='stop' for call in calls));self.assertFalse((Path(directory)/'postgres-storage.json').exists())

 def test_stop_timeout_while_worker_still_running_never_restores_gateway(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);original=api['docker']
   def docker(*args,**kwargs):
    if args[0]=='stop' and args[-1]==base.NAME+'-worker':calls.append(args);raise subprocess.TimeoutExpired('stop',30)
    return original(*args,**kwargs)
   api['docker']=docker
   with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertFalse(any(call[0] in ('start','rm') for call in calls if not call[-1].startswith(base.NAME+'-postgres-check-')));self.assertNotIn(('rename',base.NAME+'-previous',base.NAME),calls)
   self.assertTrue(state[base.NAME+'-worker']['State']['Running'])
 def test_stopped_label_without_image_proof_is_unclassified_not_normal(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory)
   for name in [base.NAME,base.NAME+'-worker']:state[name]['State'].update(Running=False,Status='exited')
   api['proofs'][base.OLD].pop('shutdown')
   result=pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertEqual([row['outcome'] for row in result['shutdown']],['pre_stopped_unclassified']*2)

 def test_unknown_diagnostic_disk_failure_cannot_turn_into_automatic_rollback(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);original=api['docker']
   def docker(*args,**kwargs):
    result=original(*args,**kwargs)
    if args[0]=='stop' and args[-1]==base.NAME+'-worker':state[base.NAME+'-worker']['State']['ExitCode']=137
    return result
   api['docker']=docker;original_diagnostic=pg.shutdown_diagnostic
   def diagnostic(stack,rows):
    if any(row.get('exit_code')==137 for row in rows):raise OSError('synthetic disk failure')
    return original_diagnostic(stack,rows)
   with patch.object(pg,'shutdown_diagnostic',side_effect=diagnostic),self.assertRaises(pg.ShutdownUnconfirmed) as failure:pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertTrue(failure.exception.diagnostic_write_failed);self.assertFalse(any(call[0] in ('start','rm') for call in calls if not call[-1].startswith(base.NAME+'-postgres-check-')));self.assertNotIn(('rename',base.NAME+'-previous',base.NAME),calls)
 def test_candidate_inspection_failure_retains_whole_pair_and_no_old_restart(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory,auth=False);original=api['inspect_container']
   def inspect(name):
    value=original(name)
    if name==base.NAME and value and value['Config']['Image']==base.IMAGE:raise RuntimeError('synthetic daemon unavailable')
    return value
   api['inspect_container']=inspect
   with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertTrue(state[base.NAME]['State']['Running']);self.assertTrue(state[base.NAME+'-worker']['State']['Running']);self.assertFalse(any(call[0] in ('rm','start') for call in calls if not call[-1].startswith(base.NAME+'-postgres-check-')))
 def test_real_wrapper_lookup_distinguishes_absence_and_failed_daemon(self):
  from unittest.mock import Mock
  wrapper=base.wrapper
  for responses,missing in [([Mock(returncode=1,stdout=''),Mock(returncode=0,stdout='')],True),([Mock(returncode=1,stdout=''),Mock(returncode=1,stdout='')],False),([Mock(returncode=1,stdout=''),Mock(returncode=0,stdout='"harbor-local-gateway"')],False)]:
   with patch.object(wrapper,'docker',side_effect=responses):
    if missing:self.assertIsNone(wrapper.inspect_container(base.NAME))
    else:
     with self.assertRaises(RuntimeError):wrapper.inspect_container(base.NAME)
 def test_final_candidate_inspection_failure_happens_before_any_removal(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory,auth=False);original=api['inspect_container'];reads=0
   def inspect(name):
    nonlocal reads
    value=original(name)
    if name==base.NAME+'-worker' and value and value['Config']['Image']==base.IMAGE and value['State']['Running'] is False:
     reads+=1
     if reads==2:raise RuntimeError('synthetic final verification failure')
    return value
   api['inspect_container']=inspect
   with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertEqual(reads,2);self.assertFalse(any(call[0] in ('rm','start') for call in calls if not call[-1].startswith(base.NAME+'-postgres-check-')));self.assertIn(base.NAME,state);self.assertIn(base.NAME+'-worker',state)
 def test_foreign_candidate_ownership_never_skips_into_mixed_rollback(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory,auth=False);original=api['inspect_container'];http=api['http_code'];failed=False
   def http_code(port,path,headers=None):
    nonlocal failed
    if path=='/api/admin/state':failed=True
    return http(port,path,headers)
   def inspect(name):
    value=original(name)
    if failed and name==base.NAME and value:return {**value,'Config':{**value['Config'],'Image':'unexpected-image'}}
    return value
   api['http_code']=http_code;api['inspect_container']=inspect
   with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertTrue(state[base.NAME+'-worker']['State']['Running']);self.assertFalse(any(call[0] in ('rm','start') for call in calls if not call[-1].startswith(base.NAME+'-postgres-check-')))

 def test_stopped_old_image_lookup_unknown_is_never_downgraded_to_missing_capability(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);state[base.NAME]['State'].update(Running=False,Status='exited');original=api['inspect_container'];checks=0
   def inspect(name):
    nonlocal checks
    if name.startswith(base.NAME+'-postgres-check-'):
     checks+=1
     if checks==3:raise RuntimeError('synthetic old preflight inspection failure')
    return original(name)
   api['inspect_container']=inspect
   with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertFalse((Path(directory)/'postgres-storage.json').exists());self.assertFalse(any(call[0] in ('stop','rename','rm','start') for call in calls if not call[-1].startswith(base.NAME+'-postgres-check-')));self.assertTrue(state[base.NAME+'-worker']['State']['Running'])
 def test_image_metadata_has_bounded_read_budget_and_no_mutation_retry(self):
  from unittest.mock import Mock,patch
  with patch.object(base.wrapper,'docker',return_value=Mock(returncode=0,stdout=json.dumps([{'Id':'sha256:synthetic','Config':{}}]))) as docker:
   self.assertEqual(base.wrapper.inspect_image(base.IMAGE)['Id'],'sha256:synthetic')
   docker.assert_called_once_with('image','inspect',base.IMAGE,check=False,timeout=60)
  with patch.object(base.wrapper,'docker',side_effect=subprocess.TimeoutExpired('image inspect',60)) as docker:
   with self.assertRaises(subprocess.TimeoutExpired):base.wrapper.inspect_image(base.IMAGE)
   self.assertEqual(docker.call_count,1)
 def test_zero_exit_inspect_with_incomplete_json_is_not_absence(self):
  from unittest.mock import Mock
  for value in [[],[{}],[{'Id':'synthetic','Config':{},'State':{}}]]:
   with patch.object(base.wrapper,'docker',return_value=Mock(returncode=0,stdout=json.dumps(value))),self.assertRaises(RuntimeError):base.wrapper.inspect_container(base.NAME)

 def test_stopped_candidate_missing_its_expected_contract_is_unknown(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory,auth=False);original=api['inspect_container'];http=api['http_code'];failed=False
   def http_code(port,path,headers=None):
    nonlocal failed
    if path=='/api/admin/state':failed=True
    return http(port,path,headers)
   def inspect(name):
    value=original(name)
    if failed and name==base.NAME and value:
     value['State'].update(Running=False,Status='exited');value['Config']['Labels'].pop('com.harbor.shutdown-contract',None)
    return value
   api['http_code']=http_code;api['inspect_container']=inspect
   with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertTrue(state[base.NAME+'-worker']['State']['Running']);self.assertFalse(any(call[0] in ('rm','start') for call in calls if not call[-1].startswith(base.NAME+'-postgres-check-')))

 def sqlite_source_fixture(self,directory):
  api,calls,state=self.api(directory)
  state.pop(base.NAME+'-worker');source=state[base.NAME]
  source['State'].update(Running=False,Status='exited')
  source['Config'].update(Cmd=['node','server.mjs'],WorkingDir='/app',Entrypoint=['docker-entrypoint.sh'],Env=['HARBOR_DATA_DIR=/app/data'],Labels={'com.aistrong.project':base.NAME})
  state[base.OLD]['Config']['Labels']={'com.harbor.data-contract':'session-metadata-only-v1','org.opencontainers.image.revision':base.OLD.rsplit(':',1)[-1]}
  state[source['Image']]=state[base.OLD]
  config={**base.CONFIG,'image_prefix':base.IMAGE.rsplit(':',1)[0],'postgres_legacy_source_engine':'sqlite'}
  return api,calls,state,config
 def test_sqlite_provenance_uses_exact_container_image_when_tag_was_rebuilt(self):
  with tempfile.TemporaryDirectory() as directory:
   self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);source=state[base.NAME];state[base.OLD]={'Id':'sha256:'+'f'*64,'Config':{'Labels':{}}}
   self.assertTrue(pg.legacy_sqlite_source(api,source,'gateway',config))
   state[source['Image']]['Config']['Labels']['org.opencontainers.image.revision']='e'*40
   with self.assertRaises(pg.ShutdownUnconfirmed):pg.legacy_sqlite_source(api,source,'gateway',config)
 def test_explicit_stopped_main_sqlite_shape_can_first_cutover_without_pg_preflight(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);original=api['docker']
   def docker(*args,**kwargs):
    if args[0]=='run' and '/app/ci/postgres-preflight.mjs' in args:self.assertNotEqual(args[args.index('node')-1],base.OLD,'legacy SQLite has no PG preflight file')
    return original(*args,**kwargs)
   api['docker']=docker;result=pg.deploy(base.IMAGE,config,None,api)
   self.assertEqual(result['shutdown'],[{'container':base.NAME,'role':'gateway','outcome':'pre_stopped_unclassified'}]);self.assertFalse(any(call[0] in ('stop','start') for call in calls if not call[-1].startswith(base.NAME+'-postgres-check-')));self.assertTrue((Path(directory)/'postgres-storage.json').exists())
 def test_legacy_sqlite_requires_root_source_engine_stopped_cmd_and_actual_contract(self):
  for variant in ['no-root-source','running','unknown-cmd','wrong-contract','image-id','daemon']:
   with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
    self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory)
    if variant=='no-root-source':config.pop('postgres_legacy_source_engine')
    elif variant=='running':state[base.NAME]['State'].update(Running=True,Status='running')
    elif variant=='unknown-cmd':state[base.NAME]['Config']['Cmd']=['node','unrecognized.mjs']
    elif variant=='wrong-contract':state[base.OLD]['Config']['Labels']['com.harbor.data-contract']='unrecognized'
    elif variant=='image-id':state[base.NAME]['Image']='sha256:'+'f'*64
    else:api['inspect_image']=lambda name:(_ for _ in ()).throw(RuntimeError('synthetic daemon failure'))
    with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,config,None,api)
    self.assertFalse(any(call[0] in ('stop','rename','rm','start') for call in calls if not call[-1].startswith(base.NAME+'-postgres-check-')));self.assertFalse((Path(directory)/'postgres-storage.json').exists())
 def test_old_pg_unavailable_proof_never_becomes_sqlite_or_known_unsupported(self):
  for variant in ['timeout','io','malformed-json','missing-schema','null-shutdown','image-daemon']:
   with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
    self.prepare(directory);api,calls,state=self.api(directory);config={**base.CONFIG,'postgres_legacy_source_engine':'sqlite','image_prefix':base.IMAGE.rsplit(':',1)[0]}
    for name in [base.NAME,base.NAME+'-worker']:state[name]['State'].update(Running=False,Status='exited')
    original=api['docker'];images=api['inspect_image']
    if variant=='missing-schema':api['proofs'][base.OLD].pop('schema_digest')
    if variant=='null-shutdown':api['proofs'][base.OLD]['shutdown']=None
    def docker(*args,**kwargs):
     if args[0]=='run' and '/app/ci/postgres-preflight.mjs' in args and args[args.index('node')-1]==base.OLD:
      if variant=='timeout':raise subprocess.TimeoutExpired('preflight',30)
      if variant=='io':raise OSError('synthetic IO')
      if variant=='malformed-json':
       from unittest.mock import Mock
       return Mock(stdout='{',returncode=0)
     return original(*args,**kwargs)
    def inspect_image(name):
     if variant=='image-daemon' and name==base.OLD:raise RuntimeError('synthetic daemon')
     return images(name)
    api['docker']=docker;api['inspect_image']=inspect_image
    with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,config,None,api)
    self.assertFalse(any(call[0] in ('stop','rename','rm','start') for call in calls if not call[-1].startswith(base.NAME+'-postgres-check-')));self.assertFalse((Path(directory)/'postgres-storage.json').exists())
 def test_explicit_unsupported_shutdown_version_is_only_allowed_for_stopped_old_pg(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory)
   for name in [base.NAME,base.NAME+'-worker']:state[name]['State'].update(Running=False,Status='exited')
   api['proofs'][base.OLD]['shutdown']={'contract':'postgres-shutdown-v0'}
   result=pg.deploy(base.IMAGE,base.CONFIG,None,api);self.assertTrue(all(row['outcome']=='pre_stopped_unclassified' for row in result['shutdown']))
 def test_real_wrapper_image_inspect_does_not_turn_daemon_error_into_legacy_capability(self):
  from unittest.mock import Mock
  for response in [Mock(returncode=1,stdout=''),Mock(returncode=0,stdout='[{}]')]:
   with patch.object(base.wrapper,'docker',return_value=response),self.assertRaises(RuntimeError):base.wrapper.inspect_image(base.OLD)

 def test_entry_pg_binds_before_private_probe_preserves_stopped_sqlite_and_never_uses_old_http(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);bound=[]
   context=Mock();context.bind_candidate.side_effect=lambda row:bound.append(row['Id'])
   def probe(port,path,headers=None):
    self.assertTrue(bound,'candidate identity must be bound before readiness/auth probe')
    return 403 if headers else 401 if path=='/api/admin/state' else 200
   context.probe.side_effect=probe;api['entry_deployment']=context;api['http_code']=context.probe
   result=pg.deploy(base.IMAGE,config,None,api)
   self.assertEqual(result['activation']['state'],'active');context.check.assert_called_once();self.assertEqual(bound,[state[base.NAME]['Id']]);self.assertIn(base.NAME+'-previous',state);self.assertFalse(state[base.NAME+'-previous']['State']['Running']);self.assertFalse(any(call[0]=='start' for call in calls));self.assertNotIn(('rm',base.NAME+'-previous'),calls)
 def test_entry_failure_preserves_held_admission_without_automatic_previous_pg_restart(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory,fail=True);context=Mock();api['entry_deployment']=context
   with self.assertRaises(subprocess.CalledProcessError):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertFalse(any(call[0]=='start' for call in calls));self.assertFalse(context.activate.called)

 def test_entry_cleanup_does_not_revoke_or_unbind_when_docker_rm_did_not_remove_candidate(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory,fail=True);context=Mock();api['entry_deployment']=context;original=api['docker']
   def docker(*args,**kwargs):
    if args[0]=='rm':return Mock(returncode=0,stdout='')
    return original(*args,**kwargs)
   api['docker']=docker
   with self.assertRaisesRegex(pg.ShutdownUnconfirmed,'removal is not confirmed'):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertNotIn('revoke',api['activation_calls']);self.assertFalse(any(call[0]=='start' for call in calls))

 def test_bound_entry_candidate_is_unbound_only_after_absence_and_failed_activation_receipt(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);context=Mock();api['entry_deployment']=context;api['http_code']=lambda port,path,headers=None:200
   def unbind(*,activation,removed_container_ids):
    self.assertEqual(activation['state'],'failed');self.assertNotIn(base.NAME+'-worker',state);self.assertTrue(all(row.get('Id') not in removed_container_ids for row in state.values()));self.assertEqual(len(removed_container_ids),2)
   context.unbind_candidate.side_effect=unbind
   with self.assertRaisesRegex(RuntimeError,'authentication'):pg.deploy(base.IMAGE,config,None,api)
   context.bind_candidate.assert_called_once();context.unbind_candidate.assert_called_once();self.assertFalse(any(call[0]=='start' for call in calls));self.assertFalse(context.activate.called)
 def test_failed_activation_revoke_never_unbinds_entry_candidate(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory,fail=True);context=Mock();api['entry_deployment']=context;original=api['activation']
   def activation(action,image):
    if action=='revoke':raise RuntimeError('revocation not confirmed')
    return original(action,image)
   api['activation']=activation
   with self.assertRaisesRegex(pg.ShutdownUnconfirmed,'revocation unconfirmed'):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   context.unbind_candidate.assert_not_called();self.assertFalse(any(call[0]=='start' for call in calls))
 def test_failed_controller_unbind_remains_blocked_without_legacy_restart(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory,fail=True);context=Mock();context.unbind_candidate.side_effect=RuntimeError('controller guard changed');api['entry_deployment']=context
   with self.assertRaisesRegex(pg.ShutdownUnconfirmed,'revocation unconfirmed'):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   context.unbind_candidate.assert_called_once();self.assertFalse(any(call[0]=='start' for call in calls))
 def test_resumed_bound_candidate_failure_uses_the_same_confirmed_cleanup_callback(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);api['entry_deployment']=Mock();pg.deploy(base.IMAGE,config,None,api)
   expected={state[base.NAME]['Id'],state[base.NAME+'-worker']['Id']};context=Mock();api['entry_deployment']=context;api['http_code']=lambda port,path,headers=None:200
   with self.assertRaisesRegex(RuntimeError,'authentication'):pg.deploy(base.IMAGE,config,None,api)
   context.unbind_candidate.assert_called_once();kwargs=context.unbind_candidate.call_args.kwargs;self.assertEqual(set(kwargs['removed_container_ids']),expected);self.assertEqual(kwargs['activation']['state'],'failed');self.assertFalse(any(call[0]=='start' for call in calls));self.assertFalse(state[base.NAME+'-previous']['State']['Running'])
