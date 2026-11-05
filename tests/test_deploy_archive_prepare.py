"""Private root file/inode tests; Docker/SQL production identity is not claimed."""
import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import secrets
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import patch

PROJECT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('archive_prepare',PROJECT/'ci/storage-recovery/prepare-archive.py')
a=importlib.util.module_from_spec(spec);spec.loader.exec_module(a)
b=a.b

class ArchiveContractTests(unittest.TestCase):
 def test_placeholder_is_invalid_until_observed_cluster_binding(self):
  value=b.archive_configuration();self.assertIsNone(value['expectedCluster']);self.assertEqual(value['uid'],999);self.assertEqual(value['pgDataDirectory'],str(b.ROOT/'db/18/docker'));self.assertEqual(value['archiveDirectory'],str(b.ROOT/'archive'));self.assertEqual(value['keyFile'],str(b.ROOT/'archive.key'))
 def test_mounts_keep_profile_key_readonly_and_archive_at_the_same_host_path(self):
  config=json.loads((PROJECT/'ci/storage-recovery/bootstrap.example.json').read_text());args=b.container_arguments(config,{'owner':'a'*32})
  self.assertIn('type=bind,src='+str(b.ARCHIVE_PROFILE)+',dst='+str(b.ARCHIVE_PROFILE)+',readonly',args);self.assertIn('type=bind,src='+str(b.ROOT/'archive')+',dst='+str(b.ROOT/'archive'),args);self.assertIn('type=bind,src='+str(b.ROOT/'archive.key')+',dst='+str(b.ROOT/'archive.key')+',readonly',args)

@unittest.skipUnless(os.geteuid()==0,'needs real UID0 private filesystem fixture; does not install /etc or /opt')
class ArchiveRootFilesTests(unittest.TestCase):
 def setUp(self):
  self.directory=tempfile.TemporaryDirectory(prefix='harbor-archive-root-');self.parent=Path(self.directory.name);self.root=self.parent/'infra';self.root.mkdir(mode=0o700);self.etc=self.parent/'etc';self.etc.mkdir(mode=0o700)
  self.patches=[patch.object(b,'ROOT',self.root),patch.object(b,'ARCHIVE_PROFILE',self.etc/'archive.json'),patch.object(b,'ARCHIVE_BACKUP_KEY',self.etc/'backup.key')]
  for item in self.patches:item.start()
  self.key=secrets.token_bytes(32);b.write_new(self.root/'archive.key',self.key,uid=999);b.write_new(self.root/'report-pg.key',secrets.token_bytes(32),uid=999);(self.root/'app-secrets').mkdir(mode=0o700);b.write_new(self.root/'app-secrets/postgres-report.key',(self.root/'report-pg.key').read_bytes(),uid=1000)
  b.write_new(b.ARCHIVE_PROFILE,json.dumps(b.archive_configuration())+'\n',mode=0o644);info=b.ARCHIVE_PROFILE.stat();self.state={'owner':'a'*32,'archiveProfileIdentity':[info.st_dev,info.st_ino]};self.inode=info.st_ino
 def tearDown(self):
  for item in reversed(self.patches):item.stop()
  self.directory.cleanup()
 def bind(self,**kwargs):a.bind_profile({},self.state,'123456789',**kwargs)
 def test_root_profile_and_backup_copy_bind_once_without_changing_inode(self):
  self.bind();self.assertEqual(b.ARCHIVE_PROFILE.stat().st_ino,self.inode);self.assertEqual(json.loads(b.ARCHIVE_PROFILE.read_text()),b.archive_configuration('123456789'));self.assertEqual(b.ARCHIVE_BACKUP_KEY.read_bytes(),self.key);self.assertEqual(b.ARCHIVE_BACKUP_KEY.stat().st_uid,0);self.assertEqual(b.ARCHIVE_BACKUP_KEY.stat().st_mode&0o777,0o600)
  before=b.ARCHIVE_PROFILE.stat().st_mtime_ns;self.bind();self.assertEqual(b.ARCHIVE_PROFILE.stat().st_mtime_ns,before)
 def test_wrong_existing_backup_key_does_not_overwrite_either_file(self):
  wrong=secrets.token_bytes(32);b.write_new(b.ARCHIVE_BACKUP_KEY,wrong)
  with self.assertRaisesRegex(ValueError,'backup_key_mismatch'):self.bind()
  self.assertEqual(b.ARCHIVE_BACKUP_KEY.read_bytes(),wrong);self.assertIsNone(json.loads(b.ARCHIVE_PROFILE.read_text())['expectedCluster'])
 def test_unknown_profile_content_and_replaced_inode_are_rejected(self):
  b.ARCHIVE_PROFILE.write_text('{broken')
  with self.assertRaisesRegex(ValueError,'profile_conflict'):self.bind()
  self.assertEqual(b.ARCHIVE_PROFILE.read_text(),'{broken');self.assertFalse(b.ARCHIVE_BACKUP_KEY.exists())
  mounted_reference=os.open(b.ARCHIVE_PROFILE,os.O_RDONLY)
  try:
   b.ARCHIVE_PROFILE.unlink();b.write_new(b.ARCHIVE_PROFILE,json.dumps(b.archive_configuration()),mode=0o644)
   with self.assertRaisesRegex(ValueError,'profile_identity'):self.bind()
  finally:os.close(mounted_reference)
 def test_durable_journal_recovers_a_torn_same_inode_update(self):
  def interrupted():
   b.ARCHIVE_PROFILE.write_text('{torn');raise RuntimeError('simulated controller loss after write')
  with self.assertRaisesRegex(RuntimeError,'simulated'):self.bind(after_profile_write=interrupted)
  self.assertTrue((self.root/'archive-profile.pending.json').exists());self.assertEqual(b.ARCHIVE_PROFILE.stat().st_ino,self.inode);self.bind();self.assertEqual(json.loads(b.ARCHIVE_PROFILE.read_text()),b.archive_configuration('123456789'));self.assertFalse((self.root/'archive-profile.pending.json').exists())
 def test_interrupted_control_file_creation_never_publishes_partial_target(self):
  for prefix in ['.backup.key.','.archive-profile.pending.json.']:
   def interrupted():
    original=b.write_new
    def partial(path,body,*args,**kwargs):
     if path.name.startswith(prefix):
      descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.write(descriptor,b'{');os.close(descriptor);os._exit(23)
     return original(path,body,*args,**kwargs)
    b.write_new=partial;self.bind()
   child=multiprocessing.get_context('fork').Process(target=interrupted);child.start();child.join(3)
   try:self.assertFalse(child.is_alive());self.assertEqual(child.exitcode,23)
   finally:
    if child.is_alive():child.kill();child.join(3)
   self.assertIsNone(json.loads(b.ARCHIVE_PROFILE.read_text())['expectedCluster'])
  self.bind();self.assertEqual(json.loads(b.ARCHIVE_PROFILE.read_text()),b.archive_configuration('123456789'))
 def test_reentry_fsyncs_complete_profile_before_removing_recovery_journal(self):
  original=os.fsync
  def interrupted(descriptor):
   if os.fstat(descriptor).st_ino==self.inode:raise RuntimeError('profile not yet synced')
   return original(descriptor)
  with patch.object(a.os,'fsync',side_effect=interrupted):
   with self.assertRaisesRegex(RuntimeError,'not yet synced'):self.bind()
  self.assertEqual(json.loads(b.ARCHIVE_PROFILE.read_text()),b.archive_configuration('123456789'));seen=[]
  def recording(descriptor):
   if os.fstat(descriptor).st_ino==self.inode:seen.append((self.root/'archive-profile.pending.json').exists())
   return original(descriptor)
  with patch.object(a.os,'fsync',side_effect=recording):self.bind()
  self.assertEqual(seen,[True]);self.assertFalse((self.root/'archive-profile.pending.json').exists())
 def test_nonroot_journal_cannot_authorize_recovery(self):
  b.ARCHIVE_PROFILE.write_text('{torn');b.write_new(self.root/'archive-profile.pending.json',b'{}',uid=1000)
  with self.assertRaisesRegex(ValueError,'material_permissions'):self.bind()
  self.assertEqual(b.ARCHIVE_PROFILE.read_text(),'{torn')
 def test_parallel_writer_is_refused_while_first_holds_the_actual_inode_lock(self):
  entered=threading.Event();release=threading.Event()
  def held():entered.set();self.assertTrue(release.wait(3))
  with ThreadPoolExecutor(max_workers=1) as pool:
   first=pool.submit(self.bind,after_profile_write=held);self.assertTrue(entered.wait(3))
   try:
    with self.assertRaisesRegex(ValueError,'profile_busy'):self.bind()
   finally:release.set()
   first.result(timeout=3)
 def test_archive_key_cannot_reuse_report_or_source_encryption_material(self):
  (self.root/'archive.key').write_bytes((self.root/'report-pg.key').read_bytes())
  with self.assertRaisesRegex(ValueError,'key_reuse'):self.bind()
  (self.root/'archive.key').write_bytes(self.key);data=self.parent/'data';data.mkdir(mode=0o700);b.write_new(data/'key-encryption.key',self.key,uid=1000)
  with self.assertRaisesRegex(ValueError,'key_reuse'):self.bind()
  self.assertFalse(b.ARCHIVE_BACKUP_KEY.exists())
 def test_symlink_profile_never_follows_to_another_material(self):
  b.ARCHIVE_PROFILE.unlink();target=self.etc/'other';b.write_new(target,b'untouched');b.ARCHIVE_PROFILE.symlink_to(target)
  with self.assertRaises(OSError):self.bind()
  self.assertEqual(target.read_bytes(),b'untouched')

if __name__=='__main__':unittest.main()
