import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('bootstrap',ROOT/'ci/storage-recovery/bootstrap.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)

def configuration():return json.loads((ROOT/'ci/storage-recovery/bootstrap.example.json').read_text())
def source():return {'Image':configuration()['sourceImageId'],'Config':{'Labels':{'com.aistrong.project':b.PROJECT}},'State':{'Running':True,'Restarting':False,'Paused':False,'Pid':123,'ExitCode':0},'HostConfig':{'RestartPolicy':{'Name':'unless-stopped'}},'Mounts':[{'Type':'bind','Source':'/opt/product-development/harbor-local-gateway/data','Destination':'/app/data'},{'Type':'bind','Source':'/opt/product-development/harbor-local-gateway/runtime','Destination':'/app/runtime'}]}
class BootstrapTests(unittest.TestCase):
 def test_origins_keep_http_and_require_separate_device_tls(self):
  self.assertEqual(b.config_value(configuration())['publicOrigin'],'http://harbor.example:8092')
  for change in [{'deviceOrigin':'http://harbor.example:8093'},{'databasePort':8092},{'businessImage':'anything:latest'},{'publisherDatabase':'x; DROP DATABASE postgres'},{'deviceOrigin':'https://user:secret@host:8093'},{'publicOrigin':'http://host/x'}]:
   with self.subTest(change=change),self.assertRaises((ValueError,TypeError)):b.config_value({**configuration(),**change})
 def test_actual_docker_restart_fence_is_observed_never_performed(self):
  actual=source();self.assertFalse(b.source_observation(configuration(),actual)['containerFenced'])
  actual['State'].update(Running=False,Pid=0);self.assertFalse(b.source_observation(configuration(),actual)['containerFenced'])
  actual['HostConfig']['RestartPolicy']['Name']='no';self.assertTrue(b.source_observation(configuration(),actual)['containerFenced']);self.assertFalse(b.source_observation(configuration(),actual)['sourceFenced']);self.assertFalse(b.source_observation(configuration(),actual)['exclusiveManagerVerified'])
  actual['State']['Paused']=True;self.assertFalse(b.source_observation(configuration(),actual)['containerFenced'])
 def test_source_mount_cannot_overlap_new_materials(self):
  actual=source();actual['Mounts'][0]['Source']=str(b.ROOT)
  with self.assertRaisesRegex(ValueError,'source_overlap'):b.source_observation(configuration(),actual)
 def test_failed_inventory_is_not_absence(self):
  with patch.object(b,'docker',side_effect=ValueError('unavailable')):
   with self.assertRaises(ValueError):b.inspect('container',b.PROJECT)
 def test_container_plan_preserves_old_ports_and_data(self):
  plan=b.container_arguments(configuration(),{'owner':'a'*32});text=' '.join(plan)
  self.assertIn('127.0.0.1:55432:5432',plan);self.assertNotIn('8092:',text);self.assertNotIn('8093:',text)
  self.assertNotIn('dst=/app/data',text);self.assertNotIn('dst=/app/runtime',text);self.assertIn('dst=/run/harbor-infra-control,readonly',text)
  self.assertNotIn('--pid',plan);self.assertNotIn('--privileged',plan);self.assertNotIn('docker.sock',text)
  expected=str(b.ROOT/'db/18/docker');self.assertIn('PGDATA='+expected,plan);self.assertIn('POSTGRES_INITDB_ARGS=--auth-local=peer --auth-host=scram-sha-256 --no-locale --encoding=UTF8',plan);self.assertEqual(b.endpoint_configuration()['pgDataDirectory'],expected);self.assertEqual(b.publisher_configuration(configuration())['pgDataDir'],expected);self.assertIn('type=bind,src='+str(b.ROOT/'db')+',dst='+str(b.ROOT/'db'),plan)
 def test_new_files_refuse_overwrite_and_symlink(self):
  with tempfile.TemporaryDirectory() as root:
   file=Path(root)/'secret';b.write_new(file,b'original',uid=__import__('os').getuid())
   with self.assertRaises(FileExistsError):b.write_new(file,b'changed',uid=__import__('os').getuid())
   self.assertEqual(file.read_bytes(),b'original');link=Path(root)/'link';link.symlink_to(file)
   with self.assertRaises(FileExistsError):b.write_new(link,b'changed',uid=__import__('os').getuid())
 def test_ci_only_business_and_wrong_infrastructure_are_refused(self):
  cfg=configuration();business={'RepoDigests':[cfg['businessImage']],'Config':{'Labels':{'com.harbor.ci-validation-only':'true','com.harbor.storage.postgres':'postgres-conversation-v2','com.harbor.postgres-preflight':'v1'}}}
  with patch.object(b,'docker',return_value=json.dumps([business])):
   with self.assertRaisesRegex(ValueError,'business_image'):b.image_checks(cfg)
  business['Config']['Labels']['com.harbor.ci-validation-only']='false'
  infra={'RepoDigests':[cfg['infrastructureImage']],'Config':{'Labels':{'com.harbor.infrastructure':b.CONTRACT,'com.harbor.publisher-business-image':'wrong'}}}
  with patch.object(b,'docker',side_effect=[json.dumps([business]),json.dumps([infra])]):
   with self.assertRaisesRegex(ValueError,'infrastructure_image'):b.image_checks(cfg)
 def test_missing_database_never_becomes_report_ready(self):self.assertIsNone(b.publisher_configuration(configuration())['database'])
 def test_endpoint_material_has_no_admin_or_runtime_credentials(self):
  endpoint=b.endpoint_configuration();self.assertEqual(endpoint['network'],endpoint['databaseHost']);self.assertFalse(endpoint['runtimeCredentialsPrepared']);self.assertNotIn('password',endpoint);self.assertNotIn('user',endpoint);self.assertNotIn('database',endpoint)
 def test_existing_directory_is_never_overwritten(self):
  with tempfile.TemporaryDirectory() as directory,patch.object(b,'ROOT',Path(directory)):
   with self.assertRaisesRegex(ValueError,'existing_directory'):b.prepare(configuration())

class CertificateTests(unittest.TestCase):
 def test_generated_server_identity_is_verified_and_wrong_name_is_rejected(self):
  import os
  with tempfile.TemporaryDirectory() as directory:
   target=Path(directory)/'tls';b.certificate(target,['127.0.0.1','db.synthetic.invalid'],os.getuid())
   command=['/usr/bin/openssl','verify','-no-CApath','-no-CAstore','-CAfile',str(target/'ca.pem'),'-purpose','sslserver','-verify_hostname']
   valid=subprocess.run(command+['db.synthetic.invalid',str(target/'server.pem')],capture_output=True);self.assertEqual(valid.returncode,0)
   invalid=subprocess.run(command+['different.synthetic.invalid',str(target/'server.pem')],capture_output=True);self.assertNotEqual(invalid.returncode,0)
   self.assertEqual((target/'server.key').stat().st_mode&0o777,0o600)



class UmaskTests(unittest.TestCase):
 def test_prepare_modes_survive_umask_077_with_ownership_boundary_mocked(self):
  import os
  with tempfile.TemporaryDirectory() as directory:
   target=Path(directory)/'infra';original=b.protected
   def checked(path,uid=0,mode=None,directory=False):
    if Path(path) in target.parents:return Path(path).lstat()
    return original(path,os.getuid(),mode,directory)
   previous=os.umask(0o077)
   try:
    with patch.object(b,'ROOT',target),patch.object(b,'ARCHIVE_PROFILE',Path(directory)/'archive-profile.json'),patch.object(b,'protected',side_effect=checked),patch.object(b.os,'chown'),patch.object(b.os,'fchown'):
     b.prepare(configuration());b.verify_materials(configuration())
     self.assertEqual(target.stat().st_mode&0o777,0o711)
     self.assertEqual((target/'control').stat().st_mode&0o777,0o755)
     for name in ['publisher.json','pg_hba.conf']:self.assertEqual((target/'control'/name).stat().st_mode&0o777,0o644)
     hba=(target/'control/pg_hba.conf').read_text().splitlines();self.assertEqual(hba[:2],['local all postgres peer','local all all scram-sha-256']);self.assertIn('hostnossl all all all reject',hba)
     self.assertEqual((target/'app-secrets/device/ca.key').stat().st_mode&0o777,0o600)
     self.assertFalse((target/'pg-tls/ca.key').exists())
     self.assertEqual(set(p.name for p in (target/'app-secrets').iterdir()),{'device','postgres-ca.crt','postgres-report.key'})
     self.assertEqual((target/'app-secrets/postgres-report.key').read_bytes(),(target/'report-pg.key').read_bytes())
     self.assertEqual((target/'app-secrets/postgres-ca.crt').read_bytes(),(target/'pg-tls/ca.pem').read_bytes())
     self.assertEqual((target/'app-secrets').stat().st_mode&0o777,0o700)
     b.write_new(target/'app-secrets/device/unexpected',b'not-for-export')
     with self.assertRaisesRegex(ValueError,'application_secrets'):b.verify_materials(configuration())
   finally:os.umask(previous)



@unittest.skipUnless(__import__('os').geteuid()==0,'requires real root and private UID999 filesystem fixture')
class HostArchiveAccessTests(unittest.TestCase):
 def test_pg_uid_can_read_own_archive_but_not_root_or_device_secrets(self):
  import os
  with tempfile.TemporaryDirectory(prefix='harbor-bootstrap-access-') as directory:
   parent=Path(directory);parent.chmod(0o711);target=parent/'infra';original=b.protected
   def private_fixture(path,uid=0,mode=None,directory=False):
    # The private fixture lives under shared /tmp; production parents remain strict.
    if Path(path)==parent.parent:return Path(path).lstat()
    return original(path,uid,mode,directory)
   with patch.object(b,'ROOT',target),patch.object(b,'ARCHIVE_PROFILE',parent/'archive-profile.json'),patch.object(b,'protected',side_effect=private_fixture):
    b.prepare(configuration());b.verify_materials(configuration())
    b.write_new(target/'archive/segment.enc',b'private-wal',uid=999)
    program="""import os,sys
from pathlib import Path
root=Path(sys.argv[1])
assert os.getuid()==999 and os.getgroups()==[]
assert os.access(root,os.X_OK) and not os.access(root,os.R_OK|os.W_OK)
assert os.access(root/'archive',os.R_OK|os.X_OK)
assert (root/'archive/segment.enc').read_bytes()==b'private-wal'
assert len((root/'archive.key').read_bytes())==32
for name in ['state.json','endpoint.json','pg-ca.key','app-secrets/postgres-report.key','app-secrets/device/ca.key','app-secrets/device/server.key']:
 assert not os.access(root/name,os.R_OK),name
try:list(root.iterdir())
except PermissionError:pass
else:raise AssertionError('root directory listing allowed')
"""
    def as_pg():os.setgroups([]);os.setgid(999);os.setuid(999)
    subprocess.run(['/usr/bin/python3','-I','-c',program,str(target)],preexec_fn=as_pg,check=True,capture_output=True)
    def unrelated():os.setgroups([]);os.setgid(65534);os.setuid(65534)
    subprocess.run(['/usr/bin/python3','-I','-c',"import os,sys;from pathlib import Path;r=Path(sys.argv[1]);assert not os.access(r/'archive.key',os.R_OK);assert not os.access(r/'archive',os.R_OK|os.X_OK)",str(target)],preexec_fn=unrelated,check=True,capture_output=True)
    target.chmod(0o700)
    with self.assertRaisesRegex(ValueError,'material_permissions'):b.verify_materials(configuration())
    self.assertEqual(target.stat().st_mode&0o777,0o700)
    target.chmod(0o711);b.verify_materials(configuration())


class ExistingContainerTests(unittest.TestCase):
 def test_same_label_cannot_hide_changed_mounts_ports_or_entrypoint(self):
  cfg=configuration();name=b.PROJECT+'-postgres-infra'
  mounts=[{'Type':'bind','Source':str(b.ROOT/source),'Destination':destination,'RW':rw} for source,destination,rw in [('db',str(b.ROOT/'db'),True),('image-volume','/var/lib/postgresql',False),('pg-tls','/run/harbor-pg-tls',False),('control','/run/harbor-infra-control',False),('report-pg.key','/run/harbor-report.key',False),('postgres-password','/run/harbor-postgres-password',False),('archive',str(b.ROOT/'archive'),True),('archive.key',str(b.ROOT/'archive.key'),False),(b.ARCHIVE_PROFILE,str(b.ARCHIVE_PROFILE),False)]]
  value={'Config':{'Image':cfg['infrastructureImage'],'Entrypoint':['/usr/local/bin/harbor-postgres-infra'],'Cmd':[]},'State':{'Running':True},'NetworkSettings':{'Networks':{name:{}}},'HostConfig':{'NetworkMode':name,'PortBindings':{'5432/tcp':[{'HostIp':'127.0.0.1','HostPort':str(cfg['databasePort'])}]}},'Mounts':mounts};b.verify_container(cfg,value)
  import copy
  for change in ['mount','port','entry']:
   candidate=copy.deepcopy(value)
   if change=='mount':candidate['Mounts'][1]['RW']=True
   if change=='port':candidate['HostConfig']['PortBindings']['5432/tcp'][0]['HostIp']='0.0.0.0'
   if change=='entry':candidate['Config']['Entrypoint']=['sh']
   with self.subTest(change=change),self.assertRaises(ValueError):b.verify_container(cfg,candidate)



class SourceRecordingPreparationTests(unittest.TestCase):
 def test_missing_source_recording_key_is_only_reported_and_existing_bytes_are_never_changed(self):
  import os
  with tempfile.TemporaryDirectory() as directory:
   actual=source();actual['Mounts'][0]['Source']=directory;file=Path(directory)/'recording-encryption.key'
   missing=b.source_material_preparations(actual)[0];self.assertEqual(missing['observed'],'missing');self.assertFalse(missing['automaticInstallation']);self.assertFalse(missing['overwriteAllowed']);self.assertEqual(missing['ownerUid'],1000);self.assertEqual(missing['mode'],'0600');self.assertFalse(file.exists())
   file.write_bytes(b'a'*32);file.chmod(0o600);before=file.read_bytes();present=b.source_material_preparations(actual)[0]
   self.assertEqual(present['observed'],'metadata_valid_content_not_verified' if os.getuid()==1000 else 'metadata_invalid');self.assertEqual(file.read_bytes(),before)
   file.chmod(0o644);self.assertEqual(b.source_material_preparations(actual)[0]['observed'],'metadata_invalid');self.assertEqual(file.read_bytes(),before)

if __name__=='__main__':unittest.main()
