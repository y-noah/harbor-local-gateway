import importlib.util,json,os,tempfile,unittest,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
CI=ROOT/'ci'
from unittest.mock import patch
spec=importlib.util.spec_from_file_location('move',CI/'harbor-pg-storage-coordinator.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class Mirror(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(prefix='harbor-pg-mirror-');self.addCleanup(self.temp.cleanup);base=Path(self.temp.name);self.source=base/'source';self.target=base/'target';self.source.mkdir(mode=0o700);self.target.mkdir(mode=0o700);self.m=m.ArchiveMirror(self.source,self.target,os.getuid())
 def put(self,root,name,value):
  p=root/name;p.write_bytes(value);p.chmod(0o600);return p
 def test_scandir_errors_fail_closed_before_target_mutation(self):
  self.put(self.source,'removed.enc',b'old');proof=self.m.online();(self.source/'removed.enc').unlink();(self.source/'sub').mkdir(mode=0o700)
  original=m.os.scandir
  for root in [self.source,self.source/'sub',self.target]:
   for error in [OSError(5,'synthetic EIO'),PermissionError(13,'synthetic denied')]:
    def scan(path):
     if Path(path)==root:raise error
     return original(path)
    with patch.object(m.os,'scandir',side_effect=scan):
     with self.assertRaises(OSError):self.m.finalize(proof,source_stopped=True)
    self.assertFalse(proof['eligible']);self.assertEqual((self.target/'removed.enc').read_bytes(),b'old')
  for live in [False,True]:
   with patch.object(m.os,'scandir',side_effect=PermissionError(13,'synthetic denied')):
    with self.assertRaises(PermissionError):m.inventory(self.source,os.getuid(),live=live)
 def test_only_live_walk_tolerates_enoent(self):
  with patch.object(m.os,'scandir',side_effect=FileNotFoundError(2,'synthetic gone')):
   self.assertEqual(m.inventory(self.source,os.getuid(),live=True)[0],{})
   with self.assertRaises(FileNotFoundError):m.inventory(self.source,os.getuid())
 def test_online_is_not_eligible_and_cold_reuses_bytes_without_rehashing(self):
  self.put(self.source,'segment.enc',b'synthetic-archive');proof=self.m.online();self.assertFalse(proof['eligible'])
  with patch.object(self.m,'copy_one',side_effect=AssertionError('unchanged bytes recopied')),patch.object(m,'digest_file',side_effect=AssertionError('cold full rehash')):
   result=self.m.finalize(proof,source_stopped=True)
  self.assertTrue(result['eligible']);self.assertEqual((result['reusedFiles'],result['copiedFiles']),(1,0))
 def test_cold_copies_new_changed_and_removes_only_proven_old_files(self):
  self.put(self.source,'same.enc',b'same');self.put(self.source,'changed.enc',b'before');self.put(self.source,'removed.enc',b'old');proof=self.m.online()
  self.put(self.source,'changed.enc',b'after');self.put(self.source,'new.enc',b'new');(self.source/'removed.enc').unlink();result=self.m.finalize(proof,source_stopped=True)
  self.assertEqual((result['reusedFiles'],result['copiedFiles']),(1,2));self.assertEqual({p.name for p in self.target.iterdir()},{'same.enc','changed.enc','new.enc'});self.assertEqual((self.target/'changed.enc').read_bytes(),b'after')
 def test_same_size_content_and_restored_mtime_still_invalidates_ctime(self):
  p=self.put(self.source,'a.enc',b'aaa');proof=self.m.online();stamp=p.stat();self.put(self.source,'a.enc',b'bbb');os.utime(p,ns=(stamp.st_atime_ns,stamp.st_mtime_ns));deadline=time.monotonic()+1
  while p.stat().st_ctime_ns==stamp.st_ctime_ns:
   self.assertLess(time.monotonic(),deadline);time.sleep(.001);os.utime(p,ns=(stamp.st_atime_ns,stamp.st_mtime_ns))
  result=self.m.finalize(proof,source_stopped=True);self.assertEqual(result['copiedFiles'],1);self.assertEqual((self.target/'a.enc').read_bytes(),b'bbb')
 def test_changed_target_is_preserved_and_not_overwritten(self):
  self.put(self.source,'a.enc',b'aaa');proof=self.m.online();self.put(self.target,'a.enc',b'bad')
  with self.assertRaisesRegex(RuntimeError,'storage_unknown_or_changed_target_file'):self.m.finalize(proof,source_stopped=True)
  self.assertEqual((self.target/'a.enc').read_bytes(),b'bad')
 def test_unknown_target_is_never_deleted(self):
  self.put(self.source,'a.enc',b'aaa');proof=self.m.online();foreign=self.put(self.target,'foreign.enc',b'preserve')
  with self.assertRaisesRegex(RuntimeError,'storage_unknown_or_changed_target_file'):self.m.finalize(proof,source_stopped=True)
  self.assertEqual(foreign.read_bytes(),b'preserve')
 def test_symlinks_hardlinks_and_source_not_stopped_are_rejected(self):
  p=self.put(self.source,'a.enc',b'aaa');proof=self.m.online()
  with self.assertRaisesRegex(RuntimeError,'storage_source_not_stopped'):self.m.finalize(proof,source_stopped=False)
  (self.source/'link').symlink_to(p)
  with self.assertRaisesRegex(RuntimeError,'storage_entry_type_untrusted'):m.inventory(self.source,os.getuid())
  (self.source/'link').unlink();os.link(p,self.source/'hard')
  with self.assertRaisesRegex(RuntimeError,'storage_entry_type_untrusted'):m.inventory(self.source,os.getuid())
 def test_replaced_source_or_target_roots_cannot_be_adopted(self):
  self.put(self.source,'a.enc',b'aaa');proof=self.m.online();self.source.rename(self.source.with_name('original'));self.source.mkdir(mode=0o700)
  with self.assertRaisesRegex(RuntimeError,'storage_prepared_roots_changed'):self.m.finalize(proof,source_stopped=True)
 def test_archive_nested_directories_match_exactly(self):
  (self.source/'sub').mkdir(mode=0o700);self.put(self.source/'sub','a.enc',b'aaa');proof=self.m.online();(self.source/'new').mkdir(mode=0o700);self.put(self.source/'new','b.enc',b'bbb');result=self.m.finalize(proof,source_stopped=True);self.assertTrue(result['eligible']);self.assertEqual((self.target/'new'/'b.enc').read_bytes(),b'bbb')

 def test_unknown_same_name_file_and_unknown_empty_directory_are_untouched(self):
  self.put(self.source,'known.enc',b'known');proof=self.m.online();self.put(self.source,'new.enc',b'new');self.put(self.target,'new.enc',b'foreign')
  with self.assertRaisesRegex(RuntimeError,'storage_unknown_or_changed_target_file'):self.m.finalize(proof,source_stopped=True)
  self.assertEqual((self.target/'new.enc').read_bytes(),b'foreign');(self.target/'new.enc').unlink();(self.target/'unknown').mkdir(mode=0o700)
  with self.assertRaisesRegex(RuntimeError,'storage_unknown_or_changed_target_directory'):self.m.finalize(proof,source_stopped=True)
  self.assertTrue((self.target/'unknown').is_dir());self.assertFalse((self.target/'new.enc').exists())
 def test_online_ignores_only_disappeared_live_entries_cold_fills_them(self):
  path=self.put(self.source,'a.enc',b'late');original=Path.lstat
  def disappearing(p,*args,**kwargs):
   if p==path:raise FileNotFoundError()
   return original(p,*args,**kwargs)
  with patch.object(Path,'lstat',disappearing):proof=self.m.online()
  self.assertEqual(proof['files'],{});result=self.m.finalize(proof,source_stopped=True);self.assertEqual(result['copiedFiles'],1)

class Ops:
 def __init__(self,fail=None):self.calls=[];self.saved=[];self.fail=fail;self.failure=None
 def save(self,journal):self.saved.append(dict(journal))
 def record_failure(self,journal,**kw):self.failure=kw
 def __getattr__(self,name):
  def call(*args,**kw):
   self.calls.append((name,args,kw))
   if name==self.fail:raise RuntimeError('synthetic_failure')
   if name=='pause_writers':return {'original':'preserved'}
   if name=='install_gate':return {'scope':'owned'}
  return call

class Lifecycle(unittest.TestCase):
 def test_window_preserves_source_and_records_target_start_before_starting_it(self):
  ops=Ops();journal={'phase':'archive-prepared'};result=m.Coordinator(ops,journal).switch();names=[c[0] for c in ops.calls]
  self.assertLess(names.index('stop_postgres_clean'),names.index('finalize_archive'));self.assertLess(names.index('install_instance_boot_guard'),names.index('start_postgres'));self.assertLess(names.index('update_dynamic_network_snapshot'),names.index('refresh_active_entry'));self.assertTrue(result['oldSourceRetained']);self.assertTrue(journal['oldDataRollbackForbidden']);self.assertEqual(journal['phase'],'complete');self.assertTrue(next(v for v in ops.calls if v[0]=='restore_writers')[2]['nonblocking'])
 def test_failure_after_target_start_intent_never_selects_old_data(self):
  for fail in ['start_postgres','verify_postgres_mount_tls_report_wal','start_applications','update_dynamic_network_snapshot','verify_full_instance']:
   ops=Ops(fail);journal={'phase':'archive-prepared'}
   with self.assertRaisesRegex(RuntimeError,'synthetic_failure'):m.Coordinator(ops,journal).switch()
   self.assertFalse(ops.failure['old_data_allowed']);self.assertFalse(any(c[0]=='restore_writers' for c in ops.calls));self.assertFalse(any(c[0]=='remove_gate' for c in ops.calls))
 def test_early_failure_does_not_unconditionally_resume_entry(self):
  ops=Ops('stop_postgres_clean');journal={'phase':'archive-prepared'}
  with self.assertRaises(RuntimeError):m.Coordinator(ops,journal).switch()
  self.assertTrue(ops.failure['old_data_allowed']);self.assertFalse(any(c[0]=='refresh_active_entry' for c in ops.calls))
 def test_boot_leaves_refresh_for_existing_candidate_post_and_holds_device_gate(self):
  ops=Ops();journal={'phase':'complete','writers':{'schedule':'active'},'targetBootAuthorized':True,'oldDataRollbackForbidden':True};c=m.Coordinator(ops,journal);c.boot_start();names=[v[0] for v in ops.calls]
  self.assertEqual(names[:3],['require_boot_guard_and_mounts','require_docker_restart_disabled','load_current_durable_active']);self.assertLess(names.index('install_gate'),names.index('start_postgres'));self.assertNotIn('refresh_active_entry',names);self.assertNotIn('remove_gate',names);self.assertEqual(journal['phase'],'boot-containers-ready')
  c.boot_online();self.assertEqual(journal['phase'],'complete');self.assertTrue(next(v for v in ops.calls if v[0]=='restore_writers')[2]['nonblocking'])
 def test_arm_is_durable_before_enable_and_boot_refuses_unarmed_bound_state(self):
  ops=Ops();journal={'phase':'archive-prepared'}
  def installed():
   self.assertTrue(journal['oldDataRollbackForbidden']);self.assertTrue(journal['targetBootAuthorized']);self.assertEqual(ops.saved[-1]['phase'],'boot-arm-intent')
  ops.install_instance_boot_guard=installed;m.Coordinator(ops,journal).switch()
  unarmed={'phase':'bound','oldDataRollbackForbidden':False};calls=Ops()
  with self.assertRaisesRegex(RuntimeError,'storage_boot_not_armed'):m.Coordinator(calls,unarmed).boot_start()
  self.assertEqual(calls.calls,[])
  armed={'phase':'boot-arm-intent','targetBootAuthorized':True,'oldDataRollbackForbidden':True};o=Ops()
  def start():self.assertTrue(o.saved[-1]['oldDataRollbackForbidden']);self.assertEqual(o.saved[-1]['phase'],'boot-start-intent')
  o.start_postgres=start;m.Coordinator(o,armed).boot_start()
 def test_unit_handoff_precedes_backup_restore_and_has_verified_result(self):
  ops=Ops();journal={'phase':'archive-prepared'};active=False
  def handoff():
   nonlocal active
   self.assertEqual(journal['phase'],'unit-handoff-intent');active=True
  def restore(*args,**kwargs):self.assertTrue(active)
  ops.activate_running_units=handoff;ops.restore_writers=restore;m.Coordinator(ops,journal).switch();self.assertTrue(any(c[0]=='verify_writer_restoration' for c in ops.calls))
 def test_recovery_entry_restores_original_after_real_cold_copy_io_failure(self):
  with tempfile.TemporaryDirectory(prefix='harbor-cold-fault-') as temp:
   source=Path(temp)/'source';target=Path(temp)/'target';source.mkdir(mode=0o700);target.mkdir(mode=0o700)
   for name in ['one','two']:(source/name).write_bytes(b'synthetic-source');(source/name).chmod(0o600)
   before={p.name:p.read_bytes() for p in source.iterdir()};ops=Ops();journal={'phase':'archive-prepared'};mirror=m.ArchiveMirror(source,target,os.getuid());original=mirror.copy_one;counter=0
   def interrupted(name):
    nonlocal counter
    counter+=1
    if counter==2:raise OSError('synthetic-cold-io-failure')
    return original(name)
   ops.copy_cold_database=lambda:mirror.online()
   with patch.object(mirror,'copy_one',side_effect=interrupted):
    with self.assertRaisesRegex(OSError,'synthetic-cold-io-failure'):m.Coordinator(ops,journal).switch()
   self.assertEqual(journal['phase'],'source-cold');self.assertFalse(journal.get('oldDataRollbackForbidden',False));result=m.Coordinator(ops,journal).recover_source();self.assertTrue(result['sourceRestored']);self.assertEqual(journal['phase'],'source-restored');self.assertEqual(before,{p.name:p.read_bytes() for p in source.iterdir()});self.assertTrue((target/'one').exists())
 def test_source_recovery_never_runs_after_arming_and_target_recovery_does_not_use_old_data(self):
  ops=Ops();journal={'phase':'target-start-intent','oldDataRollbackForbidden':True,'targetBootAuthorized':True,'gate':{'scope':'owned'},'writers':{}}
  with self.assertRaisesRegex(RuntimeError,'storage_old_data_rollback_forbidden'):m.Coordinator(ops,journal).recover_source()
  self.assertEqual(ops.calls,[]);result=m.Coordinator(ops,journal).recover_target();self.assertTrue(result['oldDataRollbackForbidden']);self.assertFalse(any(c[0]=='start_original_postgres' for c in ops.calls))
 def test_added_boot_graph_has_no_cycle_or_docker_global_changes(self):
  units=m.startup_units('harbor-db.mount','harbor-archive.mount');self.assertFalse(any(v.startswith('docker.service') for v in units));self.assertIn('After=harbor-pg-storage-start.service',units['harbor-entry-candidate.service.d/90-pg-storage.conf']);self.assertIn('After=harbor-pg-storage-online.service',units['harbor-postgres-backup-schedule.service.d/90-pg-storage.conf'])
  edges=list(m.BOOT_EDGES);pending={a for a,b in edges}|{b for a,b in edges};done=set()
  while pending:
   ready={v for v in pending if all(a in done for a,b in edges if b==v)};self.assertTrue(ready,'cycle');pending-=ready;done|=ready

if __name__=='__main__':unittest.main()
