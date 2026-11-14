"""Archive adoption: real same-filesystem rename/stat/journal, no archived-byte reads.
Root ownership admission and systemd/Docker facts are substituted in user fixtures.
"""
import copy,importlib.util,json,os,stat,tempfile,types,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
def load(path,name):
 s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
h=load(ROOT/'ci/harbor-pg-storage-host.py','adopt_host')
class AdoptTests(unittest.TestCase):
 def fixture(self):
  temporary=tempfile.TemporaryDirectory(prefix='harbor-adopt-');self.addCleanup(temporary.cleanup);root=Path(temporary.name);purpose=root/'approved-copy';purpose.mkdir(mode=0o700);source=root/'source';source.mkdir(mode=0o700);db=source/'db';db.mkdir(mode=0o700);archive=source/'archive';archive.mkdir(mode=0o700);old=purpose/'archive';old.mkdir(mode=0o700);(archive/'wal').write_bytes(b'synthetic archived bytes');(archive/'wal').chmod(0o600)
  core=load(ROOT/'ci/harbor-pg-storage-coordinator.py','adopt_core');core.TARGET=root/'postgres';core.SOURCE_DB=db;core.SOURCE_ARCHIVE=archive;uid=os.getuid();proof=core.ArchiveMirror(archive,old,uid).online()
  host=object.__new__(h.Host);host.core=core;host.j={'phase':'planned','scope':'harbor-pg-storage-move-v1','runId':'a'*32};host.planSha256='b'*64;approved={'directory':str(purpose),'proofSha256':h.sha(json.dumps(proof).encode()),'targetRoot':proof['targetRoot']};host.plan={'archivePrecopy':approved,'scope':'harbor-pg-storage-move-v1','runId':'a'*32};host.archive_adopt_facts=lambda:(approved,purpose,proof);host.archive_adopt_unmounted=lambda _:None;host.saved=None
  def save(value):host.saved=copy.deepcopy(value);h.STATE.write_text(json.dumps(value))
  host.save=save;original_inventory=core.inventory;core.inventory=lambda path,_uid,**kwargs:original_inventory(path,uid,**kwargs)
  for name,value in [('ROOT',root),('PROOF',root/'proof.json'),('STATE',root/'state.json')]:
   p=patch.object(h,name,value);p.start();self.addCleanup(p.stop)
  owner=patch.object(h.os,'chown',lambda *args:None);owner.start();self.addCleanup(owner.stop)
  security=patch.object(h,'private',lambda path,*args:path.read_bytes());security.start();self.addCleanup(security.stop);host.save(host.j)
  return host,root,purpose,proof
 def test_adopt_renames_without_copy_and_cold_finalize_remains_required(self):
  host,root,purpose,proof=self.fixture();inode=(purpose/'archive'/'wal').stat().st_ino;source_before=(host.core.SOURCE_ARCHIVE/'wal').read_bytes()
  with patch.object(host.core.ArchiveMirror,'copy_one',side_effect=AssertionError('must not recopy archive')),patch.object(host.core,'digest_file',side_effect=AssertionError('must not reread archived bytes')):
   result=host.adopt_archive();self.assertTrue(result['adopted']);self.assertFalse(result['eligible']);self.assertFalse(result['databaseCopied']);self.assertFalse(json.loads(h.PROOF.read_bytes())['eligible']);self.assertEqual((host.core.TARGET/'archive'/'wal').stat().st_ino,inode);self.assertEqual(list((host.core.TARGET/'db').iterdir()),[]);self.assertFalse((purpose/'archive').exists());self.assertEqual(host.j['phase'],'archive-prepared')
   self.assertEqual(host.adopt_archive(),result)
  self.assertEqual((host.core.SOURCE_ARCHIVE/'wal').read_bytes(),source_before)
  (host.core.SOURCE_ARCHIVE/'new').write_bytes(b'delta');(host.core.SOURCE_ARCHIVE/'new').chmod(0o600)
  mirror=host.core.ArchiveMirror(host.core.SOURCE_ARCHIVE,host.core.TARGET/'archive',os.getuid())
  with self.assertRaisesRegex(RuntimeError,'storage_source_not_stopped'):mirror.finalize(proof,source_stopped=False)
  final=mirror.finalize(proof,source_stopped=True);self.assertTrue(final['eligible']);self.assertEqual(final['reusedFiles'],1);self.assertEqual(final['copiedFiles'],1)
 def test_rename_ack_loss_and_journal_save_failures_are_resumable(self):
  for fault in ['claim-write-before','claim-write-after','publish-before','publish-after','archive-before','archive-after','proof-write','complete-write-before','complete-write-after']:
   with self.subTest(fault=fault):
    host,root,purpose,proof=self.fixture();original_rename=h.rename_noreplace;original_save=host.save;original_proof_save=h.save;fired=False
    def rename(src,dst):
     nonlocal fired
     match=(fault.startswith('publish') and Path(dst)==host.core.TARGET) or (fault.startswith('archive') and Path(dst)==host.core.TARGET/'archive')
     if match and not fired:
      fired=True
      if fault.endswith('after'):original_rename(src,dst)
      raise TimeoutError('synthetic_rename_ack')
     return original_rename(src,dst)
    def save(value):
     nonlocal fired
     if ((value['phase']=='archive-prepared' and fault.startswith('complete-write')) or (value['phase']=='archive-adopting' and fault.startswith('claim-write'))) and not fired:
      fired=True
      if fault.endswith('after'):original_save(value)
      raise TimeoutError('synthetic_journal_ack')
     original_save(value)
    def proof_save(path,value):
     nonlocal fired
     if fault=='proof-write' and not fired:fired=True;raise TimeoutError('synthetic_proof_ack')
     original_proof_save(path,value)
    host.save=save
    with patch.object(h,'rename_noreplace',side_effect=rename),patch.object(h,'save',side_effect=proof_save):
     with self.assertRaises(TimeoutError):host.adopt_archive()
    self.assertTrue(fired);host.j=copy.deepcopy(host.saved);host.save=original_save
    result=host.adopt_archive();self.assertTrue(result['adopted']);self.assertFalse(result['eligible']);self.assertEqual(host.j['phase'],'archive-prepared')
 def test_foreign_target_or_proof_never_moves_archive(self):
  for foreign in ['target','proof']:
   with self.subTest(foreign=foreign):
    host,root,purpose,_=self.fixture()
    if foreign=='target':host.core.TARGET.mkdir()
    else:h.PROOF.write_text('{}')
    with self.assertRaisesRegex(RuntimeError,'host_archive_adopt_foreign_target'):host.adopt_archive()
    self.assertTrue((purpose/'archive'/'wal').exists())
 def test_changed_inode_file_and_unknown_directory_are_rejected(self):
  for change in ['root','file','unknown-directory']:
   with self.subTest(change=change):
    host,root,purpose,_=self.fixture();old=purpose/'archive'
    if change=='root':old.rename(purpose/'retained');old.mkdir(mode=0o700)
    elif change=='file':(old/'wal').write_bytes(b'foreign')
    else:(old/'unknown').mkdir(mode=0o700)
    with self.assertRaises(RuntimeError):host.adopt_archive()
    self.assertFalse(host.core.TARGET.exists())
 def test_unknown_target_entries_after_rename_are_rejected(self):
  host,root,purpose,_=self.fixture();original=h.rename_noreplace
  def fail(src,dst):
   original(src,dst)
   if Path(dst)==host.core.TARGET:raise TimeoutError('lost_publish_ack')
  with patch.object(h,'rename_noreplace',side_effect=fail):
   with self.assertRaises(TimeoutError):host.adopt_archive()
  host.j=copy.deepcopy(host.saved);(host.core.TARGET/'foreign').mkdir()
  with self.assertRaisesRegex(RuntimeError,'host_archive_adopt_foreign_target'):host.adopt_archive()
  self.assertTrue((purpose/'archive'/'wal').exists())
 def factual_fixture(self):
  host,root,purpose,proof=self.fixture();control=host.core.SOURCE_DB/'18/docker/global/pg_control';control.parent.mkdir(parents=True);control.write_bytes((123).to_bytes(8,'little'))
  paths={}
  for name,path in [('db',host.core.SOURCE_DB),('archive',host.core.SOURCE_ARCHIVE),('control',control),('targetRoot',root)]:
   x=path.lstat();paths[name]={'path':str(path),'dev':x.st_dev,'ino':x.st_ino,'uid':x.st_uid,'gid':x.st_gid,'mode':stat.S_IMODE(x.st_mode)}
  pg={'Id':'a'*64,'Image':'sha256:'+'b'*64,'State':{'Running':True,'Pid':123,'StartedAt':'synthetic-generation'},'Mounts':[{'Source':str(host.core.SOURCE_ARCHIVE),'Destination':'/archive','RW':True}]}
  binding={'cluster':'123','paths':paths,'container':{'Id':pg['Id'],'Image':pg['Image'],'Pid':123,'StartedAt':'synthetic-generation'},'mounts':[{'source':str(host.core.SOURCE_ARCHIVE),'destination':'/archive','rw':True}]}
  approved=host.plan['archivePrecopy'];approved.update(binding=binding,directoryIdentity=host.core.identity(purpose.lstat())[:5],deploymentId='release',activeIdentityDigest='active',writerUnits=['harbor-pg-archive-precopy-synthetic.service'])
  # Only the root UID/GID admission of the user-owned fixture is substituted.
  original_identity=host.core.identity
  def identity(info):
   value=original_identity(info)
   if info.st_ino==purpose.stat().st_ino and info.st_dev==purpose.stat().st_dev:value[2:4]=[0,0]
   return value
  host.core.identity=identity;approved['directoryIdentity']=identity(purpose.lstat())[:5];host.plan.update(systemId='123',sourceRoots={'archive':proof['sourceRoot']},activeIdentityDigest='active');host.ticket={'deploymentId':'release'};host.current_identity=lambda:None;host.immutable=lambda _:copy.deepcopy(pg);host.state_unit=lambda *args,**kwargs:{'ActiveState':'inactive','MainPID':'0','ControlGroup':''}
  def documents():
   raw=json.dumps(proof).encode();(purpose/'archive-proof.json').write_bytes(raw);approved['proofSha256']=h.sha(raw)
   intent={'binding':binding,'eligible':False,'sourceDeletionAllowed':False};complete={'binding':binding,'eligible':False,'sourceDeleted':False,'sourceMounted':False,'servicesChanged':False,'proofSha256':approved['proofSha256'],'files':len(proof['files']),'unstableFiles':len(proof['unstable'])}
   for label,value,name in [('intent',intent,'copy.intent.json'),('complete',complete,'copy.complete.json')]:
    raw=json.dumps(value).encode();(purpose/name).write_bytes(raw);approved[label+'Sha256']=h.sha(raw)
  documents();return host,root,purpose,proof,pg,documents
 def test_original_binding_sha_generation_release_and_writer_fences(self):
  for fault in [None,'sha','cluster','source-path','source-inode','pg-generation','release','target-root','eligible','writer']:
   with self.subTest(fault=fault):
    host,root,purpose,proof,pg,documents=self.factual_fixture();approved=host.plan['archivePrecopy']
    if fault=='sha':approved['intentSha256']='0'*64
    if fault=='cluster':host.plan['systemId']='456'
    if fault=='source-path':approved['binding']['paths']['archive']['path']=str(root/'foreign');documents()
    if fault=='source-inode':approved['binding']['paths']['archive']['ino']+=1;documents()
    if fault=='pg-generation':pg['State']['Pid']+=1
    if fault=='release':approved['deploymentId']='another-release'
    if fault=='target-root':approved['targetRoot']=[0]*5
    if fault=='eligible':proof['eligible']=True;documents()
    if fault=='writer':host.state_unit=lambda *args,**kwargs:{'ActiveState':'active','MainPID':'123'}
    if fault:
     with self.assertRaises(RuntimeError):h.Host.archive_adopt_facts(host)
    else:self.assertEqual(h.Host.archive_adopt_facts(host)[2],proof)
    self.assertFalse(host.core.TARGET.exists())
 def test_no_replace_publication_rejects_concurrent_empty_foreign_target(self):
  host,root,purpose,_=self.fixture();original=h.rename_noreplace
  def collide(src,dst):
   if Path(dst)==host.core.TARGET:host.core.TARGET.mkdir()
   return original(src,dst)
  with patch.object(h,'rename_noreplace',side_effect=collide):
   with self.assertRaises(FileExistsError):host.adopt_archive()
  self.assertTrue((purpose/'archive'/'wal').exists());self.assertEqual(list(host.core.TARGET.iterdir()),[])
 def test_mount_metadata_rejects_exact_child_and_ancestor_references(self):
  host=object.__new__(h.Host);root=Path('/sdc-disk/harbor-data');purpose=root/'approved';host.core=types.SimpleNamespace(TARGET=root/'postgres');host.j={};original_stat=Path.stat;original_read=Path.read_text;disk=os.makedev(8,34);base='1 0 8:34 / /sdc-disk rw - xfs /dev/sdc2 rw\n'
  for fault in [None,'kernel-target','kernel-child','kernel-ancestor','docker-target','docker-parent','tmpfs']:
   with self.subTest(fault=fault):
    lines=base
    if fault=='kernel-target':lines+='2 1 8:34 /harbor-data/postgres /else rw - xfs /dev/sdc2 rw\n'
    if fault=='kernel-child':lines+='2 1 8:34 /harbor-data/postgres/archive /else rw - xfs /dev/sdc2 rw\n'
    if fault=='kernel-ancestor':lines+='2 1 8:34 /harbor-data /else rw - xfs /dev/sdc2 rw\n'
    rows=[]
    if fault in ['docker-target','docker-parent']:rows=[{'Id':'a'*64,'Mounts':[{'Type':'bind','Source':str(host.core.TARGET) if fault=='docker-target' else str(root)}]}]
    if fault=='tmpfs':rows=[{'Id':'a'*64,'Mounts':[{'Type':'tmpfs','Source':''}]}]
    def run(args):return (('a'*64+'\n').encode() if args[2]=='ls' else json.dumps(rows).encode()) if rows else b''
    with patch.object(h,'ROOT',root),patch.object(Path,'stat',lambda path,*args,**kwargs:types.SimpleNamespace(st_dev=disk) if path==root else original_stat(path,*args,**kwargs)),patch.object(Path,'read_text',lambda path,*args,**kwargs:lines if str(path)=='/proc/self/mountinfo' else original_read(path,*args,**kwargs)),patch.object(h,'run',side_effect=run):
     if fault not in [None,'tmpfs']:
      with self.assertRaises(RuntimeError):host.archive_adopt_unmounted(purpose)
     else:host.archive_adopt_unmounted(purpose)
 def test_copy_lock_conflict_uses_same_open_inode(self):
  import fcntl
  with tempfile.TemporaryDirectory() as directory:
   lock=Path(directory)/'copy.lock';lock.touch(mode=0o600)
   with lock.open('r+') as stream:
    fcntl.flock(stream.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    real=os.fstat
    def identity(fd):
     x=real(fd);return types.SimpleNamespace(st_mode=x.st_mode,st_uid=0,st_nlink=x.st_nlink)
    with patch.object(h,'ARCHIVE_COPY_LOCK',lock),patch.object(h.os,'fstat',side_effect=identity):
     with self.assertRaises(BlockingIOError):
      with h.archive_copy_lock():self.fail('must not enter competing copy')
if __name__=='__main__':unittest.main()
