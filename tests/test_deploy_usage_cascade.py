import importlib.util,json,os,selectors,subprocess,tempfile,time,unittest
from pathlib import Path
from urllib.parse import urlsplit,quote
from unittest.mock import Mock,patch
import test_deploy_postgres as base
pg=base.pg
spec=importlib.util.spec_from_file_location('usage_cascade',Path(__file__).parents[1]/'ci/postgres-usage-cascade.py');cascade=importlib.util.module_from_spec(spec);spec.loader.exec_module(cascade)

class UsageCascadeDeploymentTests(unittest.TestCase):
 prepare=base.PostgreSQLDeploymentTests.prepare
 api=base.PostgreSQLDeploymentTests.api

 def test_repair_only_after_both_source_stops_before_candidate_start(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);events=[]
   class Repair:
    uncertain=False
    def repair(this):
     for suffix in ['','-worker']:
      self.assertNotIn(base.NAME+suffix,state);self.assertFalse(state[base.NAME+suffix+'-previous']['State']['Running'])
     events.append('repair')
    def rollback(this):self.fail('successful release must not roll back')
   api['usage_cascade_release']=Repair();original=api['docker']
   def docker(*args,**kwargs):
    if args[:2]==('run','-d'):self.assertEqual(events,['repair'])
    return original(*args,**kwargs)
   api['docker']=docker;pg.deploy(base.IMAGE,base.CONFIG,None,api);self.assertEqual(events,['repair'])

 def test_failed_candidate_removed_before_acl_rollback_and_old_start(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);events=[]
   class Repair:
    uncertain=False
    def repair(this):events.append('repair')
    def rollback(this):
     for suffix in ['','-worker']:
      self.assertNotIn(base.NAME+suffix,state);self.assertFalse(state[base.NAME+suffix+'-previous']['State']['Running'])
     events.append('rollback')
   api['usage_cascade_release']=Repair();original=api['docker']
   def docker(*args,**kwargs):
    if args[0]=='start':self.assertEqual(events,['repair','rollback'])
    return original(*args,**kwargs)
   api['docker']=docker
   with patch.object(pg,'verify_started_composition',side_effect=RuntimeError('candidate_failed')),self.assertRaises(Exception):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertEqual(events,['repair','rollback'])

 def test_unknown_repair_never_restarts_old_code_or_attempts_rollback(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory)
   class Repair:
    uncertain=True
    def repair(this):raise RuntimeError('lost_ack_and_readback_unavailable')
    def rollback(this):self.fail('unknown repair must preserve stopped composition')
   api['usage_cascade_release']=Repair()
   with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertFalse(any(call[0]=='start' or call[:2]==('run','-d') for call in calls))
   self.assertFalse(state[base.NAME+'-previous']['State']['Running'])

 def test_failed_acl_rollback_never_restarts_incompatible_old_code(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory)
   class Repair:
    uncertain=False
    def repair(this):pass
    def rollback(this):raise RuntimeError('acl_changed')
   api['usage_cascade_release']=Repair()
   with patch.object(pg,'verify_started_composition',side_effect=RuntimeError('candidate_failed')),self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(base.IMAGE,base.CONFIG,None,api)
   self.assertFalse(any(call[0]=='start' for call in calls));self.assertFalse(state[base.NAME+'-previous']['State']['Running'])

 def test_template_db_port_and_query_cannot_override_authenticated_target(self):
  template=urlsplit('postgresql://administrator:synthetic@db-alias:15432/template_db?sslmode=verify-full&host=evil&port=9&dbname=evil&database=evil&user=evil&password=evil&options=-crole=evil&connectionString=evil')
  runtime=urlsplit('postgresql://runtime@db-alias:5432/actual_db?sslmode=verify-full');ssl={'rejectUnauthorized':True,'servername':'db-alias','ca':'BEGIN CERTIFICATE'}
  connection=cascade.owner_connection_fields(template,runtime,{'database':'actual_db'},'192.0.2.22',ssl)
  self.assertEqual(connection,{'host':'192.0.2.22','port':5432,'database':'actual_db','user':'administrator','password':'synthetic','ssl':ssl,'application_name':'harbor-usage-cascade-maintenance'})

 def test_authenticated_transport_uses_unique_network_peer_and_runtime_ca(self):
  ticket={'materialsDirectory':'/owned/run','database':{'name':'actual_db','oid':'1','cluster':'2'},'network':{'name':'owned','id':'network-id','infrastructureContainerIds':['peer']}}
  row={'Id':'peer','Name':'/db-alias','Config':{},'State':{'Running':True},'NetworkSettings':{'Networks':{'owned':{'NetworkID':'network-id','IPAddress':'192.0.2.22','Aliases':['db-alias']}}}}
  gateway={'Config':{'Env':['HARBOR_DATABASE_URL=postgresql://runtime@db-alias:5432/actual_db?sslmode=verify-full&sslrootcert=/app/tls/ca.pem']},'Mounts':[{'Type':'bind','Source':'/owned/tls','Destination':'/app/tls'}]}
  manifest={'database':'actual_db','databaseOid':'1','runtime':'runtime','runtimeOid':'3','owner':'owner'};entry=Mock();entry.inspect_container.return_value=row
  with patch.object(cascade,'authenticated_manifest',return_value=manifest),patch.object(cascade,'private_env',return_value='postgresql://administrator:synthetic@db-alias:15432/template_db?sslmode=verify-full&host=evil&port=9&dbname=evil&user=evil&options=-crole=evil&sslrootcert=/untrusted/ca'),patch.object(cascade,'trusted_text',return_value='BEGIN CERTIFICATE') as read:
   connection,actual=cascade.owner_transport(entry,ticket,gateway);self.assertEqual(actual,manifest);self.assertEqual(connection['port'],5432);self.assertEqual(connection['database'],'actual_db');self.assertEqual(connection['host'],'192.0.2.22');self.assertEqual(connection['user'],'administrator');read.assert_called_once_with('/owned/tls/ca.pem')
   gateway['Config']['Env'][0]+='&host=evil'
   with self.assertRaisesRegex(RuntimeError,'runtime_query_target_override'):cascade.owner_transport(entry,ticket,gateway)

 @unittest.skipUnless(os.environ.get('HARBOR_TEST_DATABASE_URL'),'需独立PG测试库')
 def test_real_template_db_port_and_query_redirect_to_authenticated_database(self):
  runtime=urlsplit(os.environ['HARBOR_TEST_DATABASE_URL']);userinfo=runtime.netloc.rsplit('@',1)[0]
  template=urlsplit('postgresql://'+userinfo+'@'+runtime.hostname+':1/template_wrong_database?sslmode=verify-full&host=192.0.2.250&port=2&dbname=query_wrong&database=query_wrong&user=query_wrong&password=query_wrong&options=-crole=query_wrong&connectionString=query_wrong')
  connection=cascade.owner_connection_fields(template,runtime,{'database':runtime.path.lstrip('/')},runtime.hostname,None)
  node=os.environ.get('HARBOR_TEST_NODE','node');module=str(Path(__file__).parents[1]/'postgres-store.mjs');code="import {PostgresStore} from '"+module+"';let raw='';for await(const c of process.stdin)raw+=c;const s=new PostgresStore({connection:JSON.parse(raw),allowPlaintextLocalNetwork:true});try{console.log(JSON.stringify(await s.get('SELECT current_database() database,current_user AS role')));}finally{await s.close();}"
  result=subprocess.run([node,'--input-type=module','--eval',code],input=json.dumps(connection).encode(),capture_output=True,timeout=5)
  self.assertEqual(result.returncode,0,'structured administrator connection failed');self.assertEqual(json.loads(result.stdout),{'database':runtime.path.lstrip('/'),'role':runtime.username})

 @unittest.skipUnless(os.environ.get('HARBOR_TEST_DATABASE_URL'),'需独立PG测试库')
 def test_real_witness_survives_pool_ten_second_idle_eviction(self):
  root=Path(__file__).parents[1];module=str(root/'postgres-store.mjs');node=os.environ.get('HARBOR_TEST_NODE','node');code=cascade.RUNTIME_WITNESS.replace('/app/postgres-store.mjs',module)
  env={**os.environ,'HARBOR_DATABASE_URL':os.environ['HARBOR_TEST_DATABASE_URL']};process=subprocess.Popen([node,'--input-type=module','--eval',code],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,env=env)
  try:
   with selectors.DefaultSelector() as selector:
    selector.register(process.stdout,selectors.EVENT_READ);self.assertTrue(selector.select(5),'witness did not become ready');witness=json.loads(process.stdout.readline(4097))
   self.assertRegex(witness['backendPid'],'^[0-9]+$')
   # This checks the real 10s pg-pool idle-eviction timer itself. Holding a
   # checked-out connection must keep its exact backend alive past the timer.
   time.sleep(10.25)
   observer="import {PostgresStore} from '"+module+"';const s=new PostgresStore({connectionString:process.env.HARBOR_DATABASE_URL});try{console.log(JSON.stringify(await s.get('SELECT count(*) n FROM pg_stat_activity WHERE pid=$1 AND usesysid::text=$2',Number(process.env.WITNESS_PID),process.env.WITNESS_ROLE)));}finally{await s.close();}"
   result=subprocess.run([node,'--input-type=module','--eval',observer],env={**env,'WITNESS_PID':witness['backendPid'],'WITNESS_ROLE':witness['sessionRoleOid']},capture_output=True,timeout=5)
   self.assertEqual(result.returncode,0,'observer failed');self.assertEqual(json.loads(result.stdout),{'n':1});self.assertIsNone(process.poll())
   process.stdin.close();self.assertEqual(process.wait(timeout=5),0)
  finally:
   if process.poll() is None:process.kill();process.wait(timeout=5)
   for stream in [process.stdin,process.stdout]:
    if stream and not stream.closed:stream.close()

 def test_lost_ack_requires_resolved_state_before_clearing_uncertainty(self):
  for outcome in ['legacy','committed','unknown']:
   with self.subTest(outcome=outcome):
    release=object.__new__(cascade.UsageCascadeRelease);release.plan={'safe':False,'binding':{}};release.api={'NAME':'harbor','inspect_container':lambda name:None};release.record={'usageCascade':{'repairId':'synthetic'}};release.receipt=None;release.uncertain=False;release.check_active=lambda:None;release.check_sources=lambda **kw:None;release.save=lambda:None;calls=[]
    def command(action,data):
     calls.append(action)
     if action=='cascade-repair':raise RuntimeError('lost_ack')
     if action=='cascade-status':
      if outcome=='unknown':raise RuntimeError('maintenance_lock_timeout')
      return {'safe':outcome=='committed'}
     if action=='cascade-receipt':return {'receipt':{'synthetic':True}}
     self.fail('unexpected action')
    release.command=command
    with self.assertRaises(RuntimeError):release.repair()
    self.assertEqual(release.uncertain,outcome=='unknown');self.assertEqual(bool(release.receipt),outcome=='committed');self.assertEqual(calls,['cascade-repair','cascade-status']+(['cascade-receipt'] if outcome=='committed' else []))

 def test_stop_proof_binds_config_and_exact_container_generation(self):
  row={'Id':'a'*64,'Image':'sha256:'+'b'*64,'Config':{'Env':[]},'HostConfig':{},'State':{'Running':False,'Status':'exited','Pid':0,'FinishedAt':'2026-11-13'}}
  release=object.__new__(cascade.UsageCascadeRelease);release.sources=[cascade.generation(row)]*2;release.api={'NAME':'harbor','inspect_container':lambda name:row}
  release.check_sources(stopped=True)
  for key,value in [('Running',True),('Pid',12),('FinishedAt','')]:
   before=row['State'][key];row['State'][key]=value
   with self.assertRaises(RuntimeError):release.check_sources(stopped=True)
   row['State'][key]=before
  row['Config']['Env'].append('CHANGED=1')
  with self.assertRaises(RuntimeError):release.check_sources(stopped=True)

if __name__=='__main__':unittest.main()
