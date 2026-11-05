"""Composition boundary tests; root proof/SQL activation are tested separately."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import test_deploy_postgres as base
import test_deploy_postgres_shutdown as shutdown
pg=base.pg

class ActivationCompositionTests(unittest.TestCase):
 prepare=base.PostgreSQLDeploymentTests.prepare
 api=base.PostgreSQLDeploymentTests.api
 sqlite_source_fixture=shutdown.PostgreSQLShutdownTests.sqlite_source_fixture
 def test_missing_activation_consumer_rejects_before_docker_or_marker(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);del api['activation']
   with self.assertRaisesRegex(ValueError,'activation consumer'):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertEqual(calls,[]);self.assertFalse((Path(directory)/'postgres-storage.json').exists())
 def test_candidate_config_digest_must_match_signed_root_approval_before_mutation(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);original=api['activation']
   api['activation']=lambda action,image:{**original(action,image),'configDigest':'sha256:'+'7'*64,'manifestDigest':'sha256:'+'7'*64}
   with self.assertRaisesRegex(ValueError,'config digest'):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertFalse(any(row[0] in ('stop','rename','rm') for row in calls));self.assertNotIn('consume',api['activation_calls'])
 def test_actual_preflight_precedes_consume_and_consume_precedes_public_candidate_start(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);events=[];docker=api['docker'];activation=api['activation']
   def record_docker(*args,**kwargs):events.append(('docker',args));return docker(*args,**kwargs)
   def record_activation(action,image):events.append(('activation',action));return activation(action,image)
   api['docker']=record_docker;api['activation']=record_activation;pg.deploy(base.IMAGE,config,None,api)
   preflight=events.index(('activation','preflight'));consume=events.index(('activation','consume'));started=next(i for i,row in enumerate(events) if row[0]=='docker' and row[1][:2]==('run','-d'))
   self.assertLess(preflight,consume);self.assertLess(consume,started);self.assertFalse(any(row[:2]==('start',base.NAME) for row in calls))
 def test_lost_consume_acknowledgement_reclaims_login_without_starting_or_restarting_sqlite(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);activation=api['activation']
   def lost_ack(action,image):
    value=activation(action,image)
    if action=='consume':raise RuntimeError('lost consume acknowledgement')
    return value
   api['activation']=lost_ack
   with self.assertRaisesRegex(RuntimeError,'lost consume'):pg.deploy(base.IMAGE,config,None,api)
   self.assertIn('revoke',api['activation_calls']);self.assertFalse(any(row[:2]==('run','-d') or row[0]=='start' for row in calls));self.assertFalse(state[base.NAME]['State']['Running']);self.assertTrue((Path(directory)/'postgres-storage.json').exists())
 def test_same_generation_crash_with_one_candidate_recovers_without_using_sqlite_as_a_writer(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);original=api['docker'];crashed=False
   def crash(*args,**kwargs):
    nonlocal crashed
    if not crashed and args[:2]==('run','-d') and args[args.index('--name')+1]==base.NAME+'-worker':crashed=True;raise KeyboardInterrupt('simulate process death')
    return original(*args,**kwargs)
   api['docker']=crash
   with self.assertRaises(KeyboardInterrupt):pg.deploy(base.IMAGE,config,None,api)
   self.assertIn(base.NAME+'-previous',state);self.assertIn(base.NAME,state);self.assertNotIn(base.NAME+'-worker',state)
   result=pg.deploy(base.IMAGE,config,None,api);self.assertEqual(result['activation']['state'],'active');self.assertTrue(state[base.NAME]['State']['Running']);self.assertTrue(state[base.NAME+'-worker']['State']['Running']);self.assertFalse(any(row[0]=='start' for row in calls))
 def test_healthy_same_generation_retry_is_observational(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);pg.deploy(base.IMAGE,config,None,api);before=sum(row[:2]==('run','-d') for row in calls);result=pg.deploy(base.IMAGE,config,None,api);self.assertTrue(result['activation']['resumed']);self.assertEqual(sum(row[:2]==('run','-d') for row in calls),before)
 def test_healthy_retry_checks_authentication_and_fresh_worker_progress(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);pg.deploy(base.IMAGE,config,None,api);before=len([row for row in calls if row[:2]==('exec',base.NAME+'-worker')]);pg.deploy(base.IMAGE,config,None,api);self.assertGreater(len([row for row in calls if row[:2]==('exec',base.NAME+'-worker')]),before)
   original=api['http_code'];api['http_code']=lambda port,path,headers=None:200 if path=='/api/admin/state' else original(port,path,headers)
   with self.assertRaisesRegex(RuntimeError,'authentication'):pg.deploy(base.IMAGE,config,None,api)
   self.assertIn('revoke',api['activation_calls']);self.assertNotIn(base.NAME,state);self.assertNotIn(base.NAME+'-worker',state);self.assertFalse(any(row[0]=='start' for row in calls))
 def test_same_generation_created_container_recovers_without_stopping_or_starting_sqlite(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);original=api['docker'];crashed=False
   def crash(*args,**kwargs):
    nonlocal crashed
    value=original(*args,**kwargs)
    if not crashed and args[:2]==('run','-d') and args[args.index('--name')+1]==base.NAME:
     crashed=True;state[base.NAME]['State'].update(Running=False,Status='created',StartedAt='0001-01-01T00:00:00Z');raise KeyboardInterrupt('created before start')
    return value
   api['docker']=crash
   with self.assertRaises(KeyboardInterrupt):pg.deploy(base.IMAGE,config,None,api)
   self.assertEqual(state[base.NAME]['State']['Status'],'created');result=pg.deploy(base.IMAGE,config,None,api);self.assertEqual(result['activation']['state'],'active');self.assertFalse(any(row[0]=='start' for row in calls))
 def test_healthy_retry_progress_timeout_stops_both_and_revokes_without_sqlite(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);pg.deploy(base.IMAGE,config,None,api);docker=api['docker']
   def stalled(*args,**kwargs):
    value=docker(*args,**kwargs)
    if args[:2]==('exec',base.NAME+'-worker') and '--progress-since' in args:value.returncode=1
    return value
   api['docker']=stalled
   with patch.object(pg.time,'monotonic',side_effect=[0,0,91]),patch.object(pg.time,'sleep'):
    with self.assertRaisesRegex(RuntimeError,'progress'):pg.deploy(base.IMAGE,config,None,api)
   self.assertIn('revoke',api['activation_calls']);self.assertNotIn(base.NAME,state);self.assertNotIn(base.NAME+'-worker',state);self.assertFalse(any(row[0]=='start' for row in calls))
 def test_previous_pg_with_stale_persistent_credentials_cannot_restart_after_failure(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory,fail=True)
   for target in [base.NAME,base.NAME+'-worker']:
    state[target]['Config']['Env']=[value if not value.startswith('HARBOR_DATABASE_URL=') else value.replace(':synthetic@',':previous@') for value in state[target]['Config']['Env']]
   with self.assertRaises(base.subprocess.CalledProcessError):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertIn('revoke',api['activation_calls']);self.assertFalse(any(row[0]=='start' for row in calls));self.assertFalse(state[base.NAME]['State']['Running']);self.assertFalse(state[base.NAME+'-worker']['State']['Running'])

 def test_preinstalled_candidate_is_inspected_and_preflighted_without_load_or_pull(self):
  for archive in [None,'private-unused-transport.tar']:
   with self.subTest(archive=archive),tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
    self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);events=[];inspect=api['inspect_image'];activate=api['activation']
    def observe_image(image):events.append('inspect');return inspect(image)
    def observe_activation(action,image):events.append(action);return activate(action,image)
    api['inspect_image']=observe_image;api['activation']=observe_activation;api['validate_archive'].side_effect=lambda *_:events.append('archive-validated')
    result=pg.deploy(base.IMAGE,config,archive,api)
    self.assertEqual(result['activation']['state'],'active');self.assertFalse(any(row[0] in ('load','pull') for row in calls));self.assertLess(events.index('inspect'),events.index('preflight'));self.assertLess(events.index('preflight'),events.index('consume'))
    if archive:api['validate_archive'].assert_called_once_with(archive,base.IMAGE);self.assertLess(events.index('archive-validated'),events.index('preflight'))
    else:api['validate_archive'].assert_not_called()
 def test_missing_preinstalled_candidate_rejects_without_fetching_or_consuming(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);state.pop(base.IMAGE)
   with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,config,None,api)
   self.assertFalse(any(row[0] in ('pull','load','stop','rename','rm','start') for row in calls));self.assertNotIn('preflight',api['activation_calls']);self.assertNotIn('consume',api['activation_calls']);self.assertFalse(state[base.NAME]['State']['Running'])


class SnapshotterActivationCompositionTests(ActivationCompositionTests):
 """Same composition cases; Docker ID is the signed manifest, not config."""
 def api(self,directory,**kwargs):
  api,calls,state=base.PostgreSQLDeploymentTests.api(self,directory,**kwargs);docker=api['docker'];manifest='sha256:'+'8'*64
  state[base.IMAGE].update(Id=manifest,Descriptor={'digest':manifest},RepoDigests=[base.IMAGE.rsplit(':',1)[0]+'@'+manifest])
  def snapshotter(*args,**options):
   result=docker(*args,**options)
   if args[:2]==('run','-d'):
    target=args[args.index('--name')+1]
    if target in state:state[target]['Image']=manifest
   return result
  api['docker']=snapshotter;return api,calls,state
 def test_snapshotter_requires_exact_descriptor_and_approved_repo_digest(self):
  import copy
  for field,value in [('Descriptor',None),('Descriptor',{'digest':'sha256:'+'7'*64}),('RepoDigests',[]),('RepoDigests',[base.IMAGE.rsplit(':',1)[0]+'@sha256:'+'7'*64]),('RepoDigests',[base.IMAGE+'@sha256:'+'8'*64]),('RepoDigests',base.IMAGE.rsplit(':',1)[0]+'@sha256:'+'8'*64)]:
   with self.subTest(field=field,value=value),tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
    self.prepare(directory);api,calls,state=self.api(directory);state[base.IMAGE][field]=copy.deepcopy(value)
    with self.assertRaisesRegex(ValueError,'manifest descriptor'):pg.deploy(base.IMAGE,base.CONFIG,None,api)
    self.assertNotIn('consume',api.get('activation_calls',[]));self.assertFalse(any(row[0] in ('stop','rename','rm') for row in calls))
 def test_unknown_container_digest_prevents_same_generation_cleanup(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state,config=self.sqlite_source_fixture(directory);pg.deploy(base.IMAGE,config,None,api);state[base.NAME]['Image']='sha256:'+'7'*64
   with self.assertRaisesRegex(pg.ShutdownUnconfirmed,'identity'):pg.deploy(base.IMAGE,config,None,api)
   self.assertTrue(state[base.NAME]['State']['Running']);self.assertNotIn('revoke',api.get('activation_calls',[]))
