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

class SharedMaintenanceCreationTests(unittest.TestCase):
 def test_retryable_maintenance_reads_preserve_primary_outcome_without_replaying_mutations(self):
  import copy
  from types import SimpleNamespace
  import test_postgres_runtime_upgrade as boundary
  for cls,original,action in self.cases():
   for outcome in ['cleanup_recovers','removed_confirmation_recovers','permanent_timeout','primary_error','nonretryable']:
    with self.subTest(cls=cls.__name__,outcome=outcome):
     row=copy.deepcopy(original);value=object.__new__(cls);value.image='synthetic';value.image_id=row['Image'];value.connection={};value.record={};value.save=Mock();ops=[];started=[];removed=[];clock=[0];missed=[];sleeps=[]
     expected={'action':action,'installed':False,'safe':True}
     def raw_inspect(target):
      key='after_remove' if removed else 'cleanup' if started else 'before_start'
      inject=(outcome=='removed_confirmation_recovers' and key=='after_remove' and not missed) or (outcome=='cleanup_recovers' and key=='cleanup' and not missed) or (outcome in ['permanent_timeout','primary_error','nonretryable'] and key=='cleanup')
      if inject:
       missed.append(key)
       if outcome=='nonretryable':raise RuntimeError('synthetic_nonretryable_read')
       clock[0]+=10;raise boundary.wrapper.ContainerInspectionUnavailable('synthetic timeout')
      return None if removed else row
     value.api={'STACK':'synthetic-stack','inspect_image':lambda _: {'Id':value.image_id},'inspect_container':raw_inspect}
     value.api['maintenance_container_inspection']=lambda target:pg.container_inspection(value.api,target,[],retry_deadline=clock[0]+pg.preflight_timeout_seconds(30))
     def run(args,**options):
      ops.append(args[1])
      if args[1]=='create':return SimpleNamespace(returncode=0,stdout=row['Id'].encode(),stderr=b'')
      if args[1]=='start':started.append(True);row['State']={'Running':False,'Pid':0,'Status':'exited'};return SimpleNamespace(returncode=1 if outcome=='primary_error' else 0,stdout=json.dumps(expected).encode(),stderr=b'{}')
      self.assertEqual(args[1],'rm');removed.append(True);return SimpleNamespace(returncode=0)
     def sleep(seconds):sleeps.append(seconds);clock[0]+=seconds
     with patch.object(cascade.uuid,'uuid4',return_value=SimpleNamespace(hex='synthetic')),patch.object(subprocess,'run',side_effect=run),patch.object(pg.time,'monotonic',side_effect=lambda:clock[0]),patch.object(pg.time,'sleep',side_effect=sleep),patch.object(pg,'record_shutdown_diagnostic'):
      if outcome in ['cleanup_recovers','removed_confirmation_recovers']:
       self.assertEqual(value.command(action,{}),expected);self.assertFalse(value.maintenance_unconfirmed);self.assertEqual(len(missed),1);self.assertEqual(len(sleeps),1)
      elif outcome=='primary_error':
       with self.assertRaisesRegex(RuntimeError,'maintenance_'+action.replace('-','_')+'_failed') as failure:value.command(action,{})
       self.assertIsInstance(failure.exception.maintenance_cleanup_error,pg.ShutdownUnconfirmed);self.assertIs(failure.exception.__cause__,failure.exception.maintenance_cleanup_error);self.assertTrue(value.maintenance_unconfirmed)
      else:
       with self.assertRaisesRegex(RuntimeError,'cleanup_unconfirmed_after_result') as failure:value.command(action,{})
       self.assertEqual(failure.exception.maintenance_primary_result,expected);self.assertEqual(failure.exception.deployment_code,'preflight_maintenance_cleanup_unconfirmed_after_result');self.assertTrue(value.maintenance_unconfirmed)
       failure.exception.deployment_diagnostic=boundary.upgrade.deployment_failure(failure.exception)
       self.assertEqual(boundary.wrapper.deployment_error(failure.exception)['code'],'preflight_maintenance_cleanup_unconfirmed_after_result')
       self.assertNotIn('maintenance_primary_result',json.dumps(boundary.wrapper.deployment_error(failure.exception)))
       if outcome=='nonretryable':self.assertEqual(len(missed),1);self.assertEqual(sleeps,[])
       else:self.assertGreaterEqual(clock[0],30)
     self.assertEqual(ops.count('create'),1);self.assertEqual(ops.count('start'),1);self.assertEqual(ops.count('rm'),1 if outcome in ['cleanup_recovers','removed_confirmation_recovers'] else 0)
 def test_real_constructors_open_witness_after_slow_create_and_close_on_factory_failures(self):
  import copy,io
  from contextlib import nullcontext
  from types import SimpleNamespace
  import test_deploy_department_release as department
  for cls,original,action in self.cases():
   for delay,failure in [(150,None),(300,None),(150,'source_changed'),(150,'active_changed'),(150,'witness_read'),(300,'creation_pending')]:
    with self.subTest(cls=cls.__name__,delay=delay,failure=failure):
     clock=[0];events=[];processes=[];created=[];removed=[];row=copy.deepcopy(original);image_id=row['Image'];active={'approved':True}
     gateway={'Id':'b'*64,'Image':'sha256:'+'d'*64,'Config':{},'HostConfig':{},'State':{'Running':True}};worker=copy.deepcopy(gateway);worker['Id']='c'*64
     witness={'databaseOid':'1','currentRoleOid':'2','sessionRoleOid':'3','backendPid':'123'}
     entry=SimpleNamespace(ACTIVE=Path('/unused/active.json'),private_json=lambda p:{} if p==Path('/etc/harbor-gateway-deploy.json') else active,save_json=Mock())
     def inspect(name):
      if name=='harbor':return gateway
      if name=='harbor-worker':return worker
      return row if created and not removed else None
     api={'NAME':'harbor','inspect_image':lambda _: {'Id':image_id},'inspect_container':inspect,'postgres_preflight_timeout_seconds':300}
     def open_witness(*args,**options):
      events.append(('witness',clock[0]));process=SimpleNamespace(stdin=io.BytesIO(),stdout=io.BytesIO(json.dumps(witness).encode()+b'\n'),stderr=io.BytesIO(),opened_at=clock[0],wait=Mock(return_value=0),kill=Mock());processes.append(process);return process
     def run(args,**options):
      operation=args[1];events.append((operation,clock[0]))
      if operation=='create':
       self.assertEqual(options['timeout'],300);clock[0]+=delay
       if failure=='creation_pending':raise subprocess.TimeoutExpired('synthetic create',300)
       created.append(True)
       if failure=='source_changed':gateway['Config']['changed']=True
       if failure=='active_changed':active['approved']=False
       return SimpleNamespace(returncode=0,stdout=row['Id'].encode(),stderr=b'')
      if operation=='start':
       self.assertEqual(options['timeout'],75);self.assertEqual(len(processes),1);process=processes[0]
       payload=json.loads(options['input']);self.assertEqual(payload['cascade']['runtimeWitness'],witness)
       clock[0]+=74;self.assertFalse(process.stdin.closed);self.assertLess(clock[0]-process.opened_at,120)
       row['State']={'Running':False,'Pid':0,'Status':'exited'}
       result={'action':action,'installed':False,'safe':True,'contract':department.release.CONTRACT}
       return SimpleNamespace(returncode=0,stdout=json.dumps(result).encode(),stderr=b'')
      self.assertEqual(operation,'rm')
      if failure:self.assertNotIn('--force',args)
      removed.append(True);return SimpleNamespace(returncode=0)
     transport=cascade if cls is cascade.UsageCascadeRelease else department.release.transport
     selector=Mock();selector.select.return_value=[] if failure=='witness_read' else [(None,None)]
     proof={'usage_rollup_cascade_contract':'v1','department_isolation_contract':department.release.CONTRACT,'department_isolation_installed':False}
     arguments={'api':api,'entry':entry,'ticket':{'to':{'configDigest':image_id,'manifestDigest':'sha256:'+'f'*64},'database':{'oid':'1'}},'active':active,'gateway':gateway,'worker':worker,'image':'approved-image','proof':proof,'record':{},'path':Path('/unused')}
     if cls is not cascade.UsageCascadeRelease:arguments['source_proofs']=[proof,proof]
     with patch.object(transport,'owner_transport',return_value=({}, {'owner':'approved_owner','runtime':'approved_runtime','runtimeOid':'2','databaseOid':'1'})),patch.object(cascade.uuid,'uuid4',return_value=SimpleNamespace(hex='synthetic')),patch.object(subprocess,'run',side_effect=run),patch.object(subprocess,'Popen',side_effect=open_witness),patch.object(cascade.selectors,'DefaultSelector',side_effect=lambda:nullcontext(selector)):
      if failure:
       with self.assertRaisesRegex(RuntimeError,'source_generation_changed' if failure=='source_changed' else 'active_changed' if failure=='active_changed' else 'runtime_witness_timeout' if failure=='witness_read' else 'maintenance_creation_pending'):cls(**arguments)
      else:cls(**arguments)
     operations=[event[0] for event in events]
     self.assertEqual(operations.count('create'),1)
     if failure=='creation_pending':self.assertEqual(operations,['create']);self.assertFalse(processes)
     elif failure in ['source_changed','active_changed']:self.assertEqual(operations,['create','rm']);self.assertFalse(processes)
     elif failure=='witness_read':self.assertEqual(operations,['create','witness','rm'])
     else:self.assertEqual(operations,['create','witness','start','rm']);self.assertEqual(processes[0].opened_at,delay)
     for process in processes:
      self.assertTrue(process.stdin.closed);self.assertTrue(process.stdout.closed);self.assertTrue(process.stderr.closed);process.wait.assert_called_once_with(timeout=5);process.kill.assert_not_called()
 def test_create_budget_uses_existing_preflight_configuration_contract(self):
  from types import SimpleNamespace
  cid='a'*64
  for api in [{},{'postgres_preflight_timeout_seconds':30},{'postgres_preflight_timeout_seconds':180},{'postgres_preflight_timeout_seconds':300}]:
   expected=pg.preflight_timeout_seconds(api.get('postgres_preflight_timeout_seconds',30))
   with self.subTest(api=api),patch.object(subprocess,'run',return_value=SimpleNamespace(returncode=0,stdout=cid.encode())) as run:
    self.assertEqual(cascade.create_maintenance_task(api,['synthetic'],'name','image','program','action'),(cid,False));self.assertEqual(run.call_args.kwargs['timeout'],expected);run.assert_called_once()
  for invalid in [None,True,False,0,29,301,30.0,'180',{},[]]:
   with self.subTest(invalid=invalid),patch.object(subprocess,'run') as run:
    with self.assertRaises(ValueError):pg.preflight_timeout_seconds(invalid)
    with self.assertRaisesRegex(RuntimeError,'maintenance_creation_budget'):cascade.create_maintenance_task({'postgres_preflight_timeout_seconds':invalid},['synthetic'],'name','image','program','action')
    run.assert_not_called()
 def cases(self):
  import test_deploy_department_release as department
  for cls,prefix,program,action in [(cascade.UsageCascadeRelease,'harbor-usage-cascade-','/app/postgres-usage-rollups-cli.mjs','cascade-plan'),(department.release.DepartmentRelease,'harbor-department-release-','/app/postgres-department-release.mjs','plan')]:
   row=department.DepartmentReleaseTests.unstarted(None);name=prefix+'synthetic';row['Name']='/'+name;row['Config']['Labels']={'com.harbor.maintenance':name};row['Config']['Cmd']=[program,action]
   yield cls,row,action
 def test_both_transports_reconcile_one_create_ack_and_preserve_pre_start_cleanup(self):
  from types import SimpleNamespace
  for cls,original,action in self.cases():
   for before_start_failure in [False,True]:
    with self.subTest(cls=cls.__name__,before_start_failure=before_start_failure):
     import copy
     row=copy.deepcopy(original);value=object.__new__(cls);value.image='synthetic';value.image_id=row['Image'];value.connection={'oversized':'x'*131072} if before_start_failure else {};value.record={};value.save=Mock();ops=[];removed=[]
     value.api={'inspect_image':lambda _: {'Id':value.image_id},'inspect_container':lambda _:None if removed else row,'postgres_preflight_timeout_seconds':180}
     def run(args,**options):
      ops.append(args[1]);self.assertNotIn('kill',ops)
      if args[1]=='create':self.assertEqual(options['timeout'],180);raise subprocess.TimeoutExpired('synthetic create',180)
      if args[1]=='start':self.assertEqual(options['timeout'],75);row['State']={'Running':False,'Pid':0,'Status':'exited'};return SimpleNamespace(returncode=0,stdout=json.dumps({'action':action}).encode(),stderr=b'')
      self.assertEqual(args[1],'rm');self.assertEqual(args[-1],row['Id'])
      if before_start_failure:self.assertNotIn('--force',args)
      removed.append(row['Id']);return SimpleNamespace(returncode=0)
     with patch.object(cascade.uuid,'uuid4',return_value=SimpleNamespace(hex='synthetic')),patch.object(subprocess,'run',side_effect=run):
      if before_start_failure:
       with self.assertRaisesRegex(RuntimeError,'maintenance_input_budget'):value.command(action,{})
      else:self.assertEqual(value.command(action,{}),{'action':action})
     self.assertEqual(ops,['create','rm'] if before_start_failure else ['create','start','rm']);self.assertFalse(value.maintenance_unconfirmed)
 def test_both_transports_reject_missing_changed_started_or_running_create_results(self):
  import copy
  from types import SimpleNamespace
  changes=[(('Image',),'sha256:'+'c'*64),(('Config','Cmd'),['unexpected']),(('HostConfig','ReadonlyRootfs'),False),(('HostConfig','SecurityOpt'),[]),(('HostConfig','CapDrop'),[]),(('HostConfig','NetworkMode'),'bridge'),(('HostConfig','PidsLimit'),0),(('HostConfig','Memory'),0),(('HostConfig','NanoCpus'),0),(('HostConfig','Tmpfs'),{}),(('State','StartedAt'),'2026-11-17T00:00:00Z'),(('State',),{'Status':'running','Running':True,'Pid':123}),(('State',),{'Status':'exited','Running':False,'Pid':0})]
  for cls,baseline,action in self.cases():
   rows=[None]
   for path,replacement in changes:
    row=copy.deepcopy(baseline);target=row
    for key in path[:-1]:target=target[key]
    target[path[-1]]=replacement;rows.append(row)
   for row in rows:
    with self.subTest(cls=cls.__name__,row=row):
     value=object.__new__(cls);value.image='synthetic';value.image_id=baseline['Image'];value.connection={};value.record={};value.save=Mock();ops=[]
     value.api={'inspect_image':lambda _: {'Id':value.image_id},'inspect_container':lambda _:row}
     def run(args,**options):ops.append(args[1]);self.assertEqual(args[1],'create');raise subprocess.TimeoutExpired('synthetic create',30)
     with patch.object(cascade.uuid,'uuid4',return_value=SimpleNamespace(hex='synthetic')),patch.object(subprocess,'run',side_effect=run):
      with self.assertRaisesRegex(RuntimeError,'maintenance_creation_pending' if row is None else 'maintenance_creation_(identity|unconfirmed)'):value.command(action,{})
      with self.assertRaisesRegex(RuntimeError,'maintenance_process_unconfirmed'):value.command(action,{})
     self.assertEqual(ops,['create']);self.assertTrue(value.maintenance_unconfirmed)
 def test_both_transports_recheck_reconciled_task_before_start(self):
  import copy
  from types import SimpleNamespace
  for cls,row,action in self.cases():
   with self.subTest(cls=cls.__name__):
    changed=copy.deepcopy(row);changed['State']={'Status':'running','Running':True,'Pid':123};seen=[];value=object.__new__(cls);value.image='synthetic';value.image_id=row['Image'];value.record={};value.save=Mock();ops=[]
    def inspect(_):seen.append(True);return row if len(seen)==1 else changed
    value.api={'inspect_image':lambda _: {'Id':value.image_id},'inspect_container':inspect}
    def run(args,**options):ops.append(args[1]);self.assertEqual(args[1],'create');raise subprocess.TimeoutExpired('synthetic create',30)
    with patch.object(cascade.uuid,'uuid4',return_value=SimpleNamespace(hex='synthetic')),patch.object(subprocess,'run',side_effect=run):
     with self.assertRaisesRegex(RuntimeError,'maintenance_creation_unconfirmed'):value.command(action,{})
    self.assertEqual(ops,['create']);self.assertTrue(value.maintenance_unconfirmed)

if __name__=='__main__':unittest.main()
