import importlib.util
import json
import os
from pathlib import Path
import subprocess
import stat
import tempfile
import unittest
from unittest.mock import Mock,patch
ROOT=Path(__file__).parents[1]
spec=importlib.util.spec_from_file_location('pg_guard',ROOT/'ci/postgres-deploy.py');pg=importlib.util.module_from_spec(spec);spec.loader.exec_module(pg)
spec=importlib.util.spec_from_file_location('wrapper',ROOT/'ci/deploy-wrapper.py');wrapper=importlib.util.module_from_spec(spec);spec.loader.exec_module(wrapper)
IMAGE='registry.test/harbor/gateway:'+40*'a';OLD='registry.test/harbor/gateway:'+40*'b';NAME='harbor-local-gateway';CONFIG={'storage_engine':'postgres','port':43127,'origin':'http://gateway.test:43127','device_tls_port':43128,'device_origin':'https://gateway.test:43128'}
PROOF={'revision':40*'a','contract':pg.CONTRACT,'extra_usage_dispatch_contract':pg.EXTRA_USAGE_DISPATCH_CONTRACT,'schema_digest':64*'c','database_digest':64*'d','source_digest':64*'e','storage_verified':True,'credentials_verified':True,'device_tls_verified':True,'device_ca_digest':64*'1','shutdown':{'contract':pg.SHUTDOWN_CONTRACT,**pg.SHUTDOWN_DEFAULTS}}
class PostgreSQLDeploymentTests(unittest.TestCase):
 def test_secure_material_checks_real_permissions_owner_type_and_symlink(self):
  with tempfile.TemporaryDirectory() as directory:
   path=Path(directory)/'key';path.write_bytes(b'test');path.chmod(0o600);pg.secure(path,os.getuid(),0o600)
   for mode in [0o644,0o666,0o400]:
    path.chmod(mode)
    with self.assertRaises(ValueError):pg.secure(path,os.getuid(),0o600)
   path.chmod(0o600);link=Path(directory)/'link';link.symlink_to(path)
   with self.assertRaises(ValueError):pg.secure(link,os.getuid(),0o600)
   with self.assertRaises(ValueError):pg.secure(path,os.getuid()+1,0o600)
 def prepare(self,directory):
  path=Path(directory);(path/'secrets').mkdir();(path/'secrets/postgres.env').write_text('HARBOR_DATABASE_URL=postgresql://synthetic:synthetic@database.test/harbor?sslmode=verify-full&sslrootcert=/run/secrets/postgres-ca.crt\nHARBOR_RECORDING_STORAGE_MODE=report\n')
  return path
 def test_fixed_environment_and_resources_do_not_accept_process_mount_or_tls_overrides(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   path=self.prepare(directory);common,budgets=pg.configuration(CONFIG,directory)
   self.assertEqual(budgets['gateway'],{'cpus':3,'memory_mib':6144,'pids':256});self.assertEqual(budgets['worker'],{'cpus':1,'memory_mib':2048,'pids':256})
   self.assertIn('HARBOR_RECORDING_ENABLED=1',common);self.assertIn('HARBOR_RECORDING_WORKER_ENABLED=1',common)
   original=(path/'secrets/postgres.env').read_text()
   for addition in ['NODE_OPTIONS=--inspect=0.0.0.0','HARBOR_DATA_DIR=/etc','HARBOR_RECORDING_ENABLED=0','HARBOR_DATABASE_URL=duplicate']:
    (path/'secrets/postgres.env').write_text(original+addition+'\n')
    with self.assertRaises(ValueError):pg.configuration(CONFIG,directory)
   for value in [original.replace('verify-full','disable'),original.replace('/run/secrets/postgres-ca.crt','/etc/other'),original.replace('MODE=report','MODE=local')]:
    (path/'secrets/postgres.env').write_text(value)
    with self.assertRaises(ValueError):pg.configuration(CONFIG,directory)
   (path/'secrets/postgres.env').write_text(original)
   for resources in [{'other':{}},{'gateway':{'cpus':True,'memory_mib':6144,'pids':256}},{'worker':{'cpus':1,'memory_mib':1,'pids':256}}]:
    with self.assertRaises(ValueError):pg.configuration({**CONFIG,'postgres_resources':resources},directory)
 def api(self,directory,old=True,compatible=True,fail=False,auth=True):
  tls=patch.object(pg,'device_http_code',side_effect=lambda stack,config,path,headers=None:403 if headers else 401 if path=='/api/admin/state' else 200);tls.start();self.addCleanup(tls.stop)
  calls=[];proofs={IMAGE:dict(PROOF),OLD:{**PROOF,'revision':40*'b','schema_digest':64*('c' if compatible else 'f')}}
  remembered={};original_configuration=pg.configuration
  def remember_configuration(config,stack):
   value=original_configuration(config,stack);remembered['common']=value[0];return value
  configuration_patch=patch.object(pg,'configuration',side_effect=remember_configuration);configuration_patch.start();self.addCleanup(configuration_patch.stop)
  labels=lambda image:{'com.harbor.storage.postgres':pg.CONTRACT,'com.harbor.postgres-preflight':'v1','com.harbor.extra-usage-dispatch':pg.EXTRA_USAGE_DISPATCH_CONTRACT,'org.opencontainers.image.revision':image.rsplit(':',1)[-1]}
  image_id=lambda image:'sha256:'+image.rsplit(':',1)[-1][0]*64
  state={image:{'Id':image_id(image),'Config':{'Labels':labels(image)}} for image in [IMAGE,OLD]}
  def container(image,role='gateway'):return {'Id':'container-'+role+'-'+image_id(image),'Image':image_id(image),'Config':{'Image':image,'Cmd':['node','/app/postgres-'+role+'.mjs'],'Entrypoint':[],'Env':[name+'='+str(pg.SHUTDOWN_DEFAULTS[key]) for key,name in pg.SHUTDOWN_ENV.items()]+(Path(directory)/'secrets/postgres.env').read_text().splitlines(),'Labels':{'com.aistrong.project':NAME,'com.harbor.shutdown-contract':pg.SHUTDOWN_CONTRACT,'com.harbor.shutdown-role':role}},'State':{'Running':True,'Status':'running','ExitCode':0,'OOMKilled':False,'Health':{'Status':'healthy'}},'HostConfig':{'PortBindings':{'43127/tcp':[{'HostPort':'43127'}]}}}
  if old:state.update({NAME:container(OLD),NAME+'-worker':container(OLD,'worker')})
  def docker(*args,**kwargs):
   calls.append(args)
   if args[0]=='run' and '--rm' in args:
    image=args[args.index('node')-1];return Mock(stdout=json.dumps(proofs[image]),returncode=0)
   if args[0]=='run':
    target=args[args.index('--name')+1]
    if fail and target==NAME+'-worker':raise subprocess.CalledProcessError(1,['docker'])
    state[target]=container(IMAGE,'worker' if target.endswith('-worker') else 'gateway')
    state[target]['Config']['Entrypoint']=[] if args[args.index('--entrypoint')+1]=='' else [args[args.index('--entrypoint')+1]]
    state[target]['Config']['StopSignal']=args[args.index('--stop-signal')+1]
    state[target]['Config']['Env']=[args[i+1] for i,value in enumerate(args[:-1]) if value=='--env']
    state[target]['Config']['Labels'].update(dict(args[i+1].split('=',1) for i,value in enumerate(args[:-1]) if value=='--label'))
   if args[0]=='stop':state[args[-1]]['State'].update(Running=False,Status='exited')
   if args[0]=='rename':state[args[2]]=state.pop(args[1])
   if args[0]=='rm':
    target=next((name for name,value in state.items() if value.get('Id')==args[-1]),args[-1]);state.pop(target,None)
   if args[0]=='start':state[args[1]]['State']['Running']=True
   return Mock(stdout='',returncode=0)
  api={'proofs':proofs,'STACK':directory,'NAME':NAME,'docker':docker,'inspect':state.get,'inspect_container':state.get,'inspect_image':state.get,'run':lambda *args:Mock(stdout=''),'validate_archive':Mock(),'subprocess':subprocess,'http_code':lambda port,path,headers=None:403 if headers else (401 if path=='/api/admin/state' and auth else 200)}
  authorization={'state':'pending'}
  # Component boundary only: production activation is the fixed root Node
  # consumer. Here preserve the real Python image/shutdown proof assertions.
  def activation(action,image):
   api.setdefault('activation_calls',[]).append(action);metadata={'contract':'harbor-storage-activation-deploy-v1','deploymentId':'00000000-0000-4000-8000-000000000001','receiptDigest':'9'*64,'configDigest':image_id(IMAGE),'manifestDigest':'sha256:'+'8'*64,'revision':'a'*40}
   if action=='check':
    source=state.get(NAME)
    if source and source.get('Config',{}).get('Cmd') in (['node','server.mjs'],['node','/app/server.mjs']):authorization['sourceFence']={'sourceContainerId':source['Id'],'sourceImageId':source['Image']}
   if action=='consume':authorization['state']='active'
   if action=='revoke':authorization['state']='failed'
   result={**metadata,**authorization}
   if action=='preflight':result['proof']=pg.image_proof(api,image,remembered['common'])
   return result
  api['activation']=activation
  return api,calls,state
 def test_pair_runs_fixed_commands_and_persists_pg_marker_after_real_image_preflight(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);pg.deploy(IMAGE,CONFIG,None,api)
   runs=[args for args in calls if args[0]=='run' and '-d' in args];self.assertEqual(len(runs),2)
   self.assertTrue(any(args[-1]=='/app/postgres-gateway.mjs' for args in runs));self.assertTrue(any(args[-1]=='/app/postgres-worker.mjs' for args in runs))
   self.assertTrue(all(args[args.index('--user')+1]=='1000:1000' for args in runs));self.assertTrue(all('--env-file' in args for args in runs));self.assertTrue(any('43128:43128' in args for args in runs));self.assertTrue(all('HARBOR_DEVICE_TLS_ENABLED=1' in args for args in runs))
   self.assertEqual(json.loads((Path(directory)/'postgres-storage.json').read_text()),{'contract':pg.CONTRACT,'database_digest':64*'d'})
   self.assertNotIn(NAME+'-previous',state);self.assertNotIn(NAME+'-worker-previous',state)
   self.assertTrue(any(args[:3]==('exec',NAME+'-worker','node') and '--progress-since' in args for args in calls))
 def test_failure_rolls_back_both_only_when_previous_image_passes_schema_and_identity_preflight(self):
  for compatible in [True,False]:
   with self.subTest(compatible=compatible),tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
    self.prepare(directory);api,calls,state=self.api(directory,compatible=compatible,fail=True)
    with self.assertRaises(subprocess.CalledProcessError):pg.deploy(IMAGE,CONFIG,None,api)
    self.assertEqual(('start',NAME) in calls,compatible);self.assertEqual(('start',NAME+'-worker') in calls,compatible)
    self.assertEqual(state[NAME]['Config']['Image'],OLD);self.assertTrue((Path(directory)/'postgres-storage.json').exists())
 def test_authentication_failure_removes_candidate_pair_and_preserves_data(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory,auth=False)
   with self.assertRaisesRegex(RuntimeError,'authentication'):pg.deploy(IMAGE,CONFIG,None,api)
   self.assertIn(('rm','container-gateway-sha256:'+64*'a'),calls);self.assertIn(('rm','container-worker-sha256:'+64*'a'),calls);self.assertFalse(any(call[:2]==('rm','-f') for call in calls));self.assertIn(('start',NAME),calls)
 def test_pg_marker_rejects_legacy_before_any_docker_command_even_when_symlink_is_broken(self):
  for broken in [False,True]:
   with tempfile.TemporaryDirectory() as directory,patch.object(wrapper,'STACK',directory),patch.object(wrapper,'docker') as docker:
    marker=Path(directory)/'postgres-storage.json'
    if broken:marker.symlink_to(Path(directory)/'missing')
    else:marker.write_text('{}')
    with self.assertRaisesRegex(ValueError,'marker'):wrapper.deploy(IMAGE,{'port':43127,'origin':'http://gateway.test'})
    docker.assert_not_called()
 def test_failed_candidate_preflight_never_stops_existing_pair(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);state[IMAGE]['Config']['Labels']={}
   with self.assertRaisesRegex(pg.ShutdownUnconfirmed,'capability'):pg.deploy(IMAGE,CONFIG,None,api)
   self.assertFalse(any(args[0]=='stop' for args in calls))
 def test_extra_dispatch_label_is_required_even_when_database_schema_and_identity_match(self):
  for contract in [None,'v0',True]:
   with self.subTest(contract=contract),tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
    self.prepare(directory);api,calls,state=self.api(directory)
    if contract is None:state[IMAGE]['Config']['Labels'].pop('com.harbor.extra-usage-dispatch')
    else:state[IMAGE]['Config']['Labels']['com.harbor.extra-usage-dispatch']=contract
    with self.assertRaisesRegex(pg.ShutdownUnconfirmed,'capability'):pg.deploy(IMAGE,CONFIG,None,api)
    self.assertEqual(calls,[]);self.assertTrue(state[NAME]['State']['Running']);self.assertTrue(state[NAME+'-worker']['State']['Running'])
 def test_extra_dispatch_runtime_proof_is_required_before_marker_or_stop(self):
  for contract in [None,'v0',True]:
   with self.subTest(contract=contract),tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
    self.prepare(directory);api,calls,state=self.api(directory);original=api['docker']
    def docker(*args,**kwargs):
     result=original(*args,**kwargs)
     if args[0]=='run' and '--rm' in args:
      proof=json.loads(result.stdout)
      if contract is None:proof.pop('extra_usage_dispatch_contract')
      else:proof['extra_usage_dispatch_contract']=contract
      result.stdout=json.dumps(proof)
     return result
    api['docker']=docker
    with self.assertRaisesRegex(pg.ShutdownUnconfirmed,'proof'):pg.deploy(IMAGE,CONFIG,None,api)
    self.assertFalse((Path(directory)/'postgres-storage.json').exists());self.assertFalse(any(args[0]=='stop' for args in calls));self.assertTrue(state[NAME]['State']['Running']);self.assertTrue(state[NAME+'-worker']['State']['Running'])
 def test_failed_deployment_cannot_restart_an_old_image_without_extra_dispatch_control(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory,fail=True)
   state[OLD]['Config']['Labels'].pop('com.harbor.extra-usage-dispatch')
   with self.assertRaisesRegex(pg.ShutdownUnconfirmed,'proof'):pg.deploy(IMAGE,CONFIG,None,api)
   self.assertNotIn(('start',NAME),calls);self.assertNotIn(('start',NAME+'-worker'),calls)
   self.assertFalse(any(call[0]=='stop' for call in calls));self.assertTrue(state[NAME]['State']['Running'])
 def test_R62_missing_or_failed_credential_verification_rejects_before_marker_and_stop(self):
  for verification in [None,False]:
   with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
    self.prepare(directory);api,calls,state=self.api(directory);original=api['docker']
    def docker(*args,**kwargs):
     result=original(*args,**kwargs)
     if args[0]=='run' and '--rm' in args:
      proof=json.loads(result.stdout)
      if verification is None:proof.pop('credentials_verified',None)
      else:proof['credentials_verified']=verification
      result.stdout=json.dumps(proof)
     return result
    api['docker']=docker
    with self.assertRaisesRegex(pg.ShutdownUnconfirmed,'proof'):pg.deploy(IMAGE,CONFIG,None,api)
    self.assertFalse((Path(directory)/'postgres-storage.json').exists());self.assertFalse(any(args[0]=='stop' for args in calls));self.assertTrue(state[NAME]['State']['Running']);self.assertTrue(state[NAME+'-worker']['State']['Running'])
 def test_marker_database_change_and_foreign_worker_fail_closed(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);(Path(directory)/'postgres-storage.json').write_text(json.dumps({'contract':pg.CONTRACT,'database_digest':64*'f'}))
   with self.assertRaisesRegex(ValueError,'identity'):pg.deploy(IMAGE,CONFIG,None,api)
   self.assertFalse(any(args[0]=='stop' for args in calls));state[NAME+'-worker']['Config']['Labels']={'com.aistrong.project':'other'}
   with self.assertRaisesRegex(ValueError,'owned'):pg.deploy(IMAGE,CONFIG,None,api)
 def test_ci_validation_image_and_missing_storage_proof_are_never_service_candidates(self):
  for variant in ['ci-image','missing-report']:
   with self.subTest(variant=variant),tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
    self.prepare(directory);api,calls,state=self.api(directory)
    if variant=='ci-image':state[IMAGE]['Config']['Labels']['com.harbor.ci-validation-only']='true'
    else:
     original=api['docker']
     def docker(*args,**kwargs):
      result=original(*args,**kwargs)
      if args[0]=='run' and '--rm' in args:result.stdout=json.dumps({**PROOF,'storage_verified':False})
      return result
     api['docker']=docker
    with self.assertRaises(pg.ShutdownUnconfirmed):pg.deploy(IMAGE,CONFIG,None,api)
    self.assertFalse(any(args[0]=='stop' for args in calls))
 def test_first_pg_failure_keeps_the_marker_without_restarting_a_legacy_service(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory,old=False,fail=True)
   with self.assertRaises(subprocess.CalledProcessError):pg.deploy(IMAGE,CONFIG,None,api)
   self.assertTrue((Path(directory)/'postgres-storage.json').exists());self.assertNotIn(NAME,state)
   self.assertFalse(any(args[0]=='start' for args in calls))
 def test_missing_device_material_endpoint_and_occupied_device_port_reject_before_service_stop(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory)
   for changed in [{**CONFIG,'device_tls_port':None},{**CONFIG,'device_origin':'http://gateway.test:43128'},{**CONFIG,'device_tls_port':43127}]:
    with self.assertRaises(ValueError):pg.configuration(changed,directory)
   api,calls,state=self.api(directory);api['run']=lambda *args:Mock(stdout='LISTEN' if args[-1]=='sport = :43128' else '')
   with self.assertRaisesRegex(ValueError,'occupied'):pg.deploy(IMAGE,CONFIG,None,api)
   self.assertFalse(any(args[0]=='stop' for args in calls))
 def test_G2_partial_stop_and_rename_failures_restore_each_compatible_old_member(self):
  for fault in ['stop-worker','rename-gateway']:
   with self.subTest(fault=fault),tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
    self.prepare(directory);api,calls,state=self.api(directory);original=api['docker'];failed=False
    def docker(*args,**kwargs):
     nonlocal failed
     matches=fault=='stop-worker' and args[:2]==('stop','-t') and args[-1]==NAME+'-worker' or fault=='rename-gateway' and args[:2]==('rename',NAME)
     if matches and not failed:
      failed=True;calls.append(args);raise subprocess.CalledProcessError(1,['docker'])
     return original(*args,**kwargs)
    api['docker']=docker
    with self.assertRaises(pg.ShutdownUnconfirmed if fault=='stop-worker' else subprocess.CalledProcessError):pg.deploy(IMAGE,CONFIG,None,api)
    if fault=='stop-worker':
     self.assertIn(NAME+'-previous',state);self.assertTrue(state[NAME+'-worker']['State']['Running']);self.assertFalse(any(call[0]=='start' for call in calls))
    else:
     self.assertTrue(state[NAME]['State']['Running']);self.assertTrue(state[NAME+'-worker']['State']['Running'])
     self.assertEqual(state[NAME]['Config']['Image'],OLD);self.assertIn(('start',NAME),calls)
 def test_G3_directory_fsync_failure_preserves_marker_and_never_stops_or_starts_service(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(pg,'secure'):
   self.prepare(directory);api,calls,state=self.api(directory);original=os.fsync
   def fsync(fd):
    if stat.S_ISDIR(os.fstat(fd).st_mode):raise OSError('synthetic directory sync failure')
    return original(fd)
   with patch.object(pg.os,'fsync',side_effect=fsync),self.assertRaises(OSError):pg.deploy(IMAGE,CONFIG,None,api)
   self.assertTrue((Path(directory)/'postgres-storage.json').exists());self.assertFalse(any(args[0]=='stop' for args in calls));self.assertFalse(any(args[0]=='run' and '-d' in args for args in calls))
if __name__=='__main__':unittest.main()

class LocalMigrationMaterialHandoffTests(unittest.TestCase):
 def test_secrets_parent_must_remain_root_private_while_app_children_use_runtime_uid(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory)
   for relative in ['secrets/device','pg-data/recording-spool','runtime']:(root/relative).mkdir(parents=True,mode=0o700)
   for p in root.rglob('*'):
    if p.is_dir():p.chmod(0o700)
   for relative in ['secrets/postgres.env','secrets/postgres-report.key','secrets/postgres-ca.crt','secrets/device/server.key','secrets/device/ca.key','secrets/device/server.pem','secrets/device/ca.pem','pg-data/key-encryption.key','pg-data/recording-encryption.key','pg-data/settings.json']:
    p=root/relative;p.write_text('synthetic');p.chmod(0o644 if p.suffix in ['.crt','.pem'] else 0o600)
   (root/'secrets/postgres.env').write_text('HARBOR_DATABASE_URL=postgresql://synthetic:synthetic@database.test/harbor?sslmode=verify-full&sslrootcert=/run/secrets/postgres-ca.crt\nHARBOR_RECORDING_STORAGE_MODE=report\n')
   original=Path.lstat;secrets_uid=0
   def metadata(path,*args,**kw):
    result=original(path,*args,**kw);uid=0
    if path==root/'secrets':uid=secrets_uid
    elif path==root/'runtime' or path==root/'pg-data' or root/'pg-data' in path.parents or path==root/'secrets/device' or root/'secrets/device' in path.parents or path==root/'secrets/postgres-report.key':uid=1000
    mode=result.st_mode & ~0o022 if path in root.parents else result.st_mode
    return os.stat_result((mode,result.st_ino,result.st_dev,result.st_nlink,uid,result.st_gid,result.st_size,result.st_atime,result.st_mtime,result.st_ctime))
   with patch.object(Path,'lstat',metadata):
    common,_=pg.configuration(CONFIG,directory);self.assertIn('--env-file',common)
    secrets_uid=1000
    with self.assertRaisesRegex(ValueError,'ownership'):pg.configuration(CONFIG,directory)
