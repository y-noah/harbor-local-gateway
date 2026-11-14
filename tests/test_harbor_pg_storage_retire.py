import copy,hashlib,importlib.util,json,os,stat,tempfile,types,unittest,uuid
from pathlib import Path
from unittest.mock import Mock,patch

CI=Path(__file__).resolve().parents[1]/'ci'
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
r=load('retire_unstarted_pg',CI/'storage-recovery/retire-unstarted-pg-move.py')
# Exercise the pure helper contract; production loads the old PLAN-pinned bytes.
old=load('retirement_test_old',CI/'harbor-pg-storage-host.py')

def local_read(path,private=False):
 path=Path(path);value=path.lstat()
 if not stat.S_ISREG(value.st_mode):raise RuntimeError('fixture_untrusted_file')
 return path.read_bytes()

def inventory(root,uid):
 files={};directories={}
 for parent,names,leaves in os.walk(root):
  path=Path(parent);relative=str(path.relative_to(root));directories[relative]={}
  for name in leaves:
   info=(path/name).lstat();files[str((path/name).relative_to(root))]=[info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode,info.st_nlink,info.st_size,info.st_mtime_ns,info.st_ctime_ns]
 return files,directories

class Retirement(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();base=Path(self.tmp.name);self.disk=base/'disk';self.etc=base/'etc';self.disk.mkdir();self.etc.mkdir();self.runid='a'*32;self.purpose=self.disk/'archive-precopy';self.purpose.mkdir(mode=0o700);self.target=self.disk/'postgres';self.target.mkdir(mode=0o700);(self.target/'db').mkdir(mode=0o700);archive=self.target/'archive';archive.mkdir(mode=0o700);(archive/'wal').write_bytes(b'original encrypted WAL payload');self.archive_inode=archive.stat().st_ino;self.payload=(archive/'wal').read_bytes()
  # Systemd/Docker are already synthetic here; their cgroup filesystem must be
  # synthetic too, including negative layout and residual-population cases.
  self.cgroup=base/'cgroup';self.cgroup.mkdir();(self.cgroup/'cgroup.controllers').write_text('cpu memory pids\n')
  for name in ['harbor-pg-storage-move-'+self.runid+'.service','harbor-pg-archive-copy.service']:
   group=self.cgroup/'system.slice'/name;group.mkdir(parents=True);(group/'cgroup.events').write_text('populated 0\nfrozen 0\n')
  cgroup=self.cgroup;system=Path('/sys/fs/cgroup')
  class FixturePath(type(Path())):
   def __new__(cls,*args):
    path=super().__new__(cls,*args)
    return cgroup/path.relative_to(system) if path==system or system in path.parents else path
  paths={'ROOT':self.disk,'PLAN':self.disk/'.pg-storage-plan.json','STATE':self.disk/'.pg-storage-state.json','PROOF':self.disk/'.pg-storage-archive-proof.json','MATERIAL':self.etc/'material.json','HISTORY':self.disk/'.pg-storage-retired','ETC_HISTORY':self.etc/'retired','ABSENT':(self.disk/'cold-db-proof',self.etc/'material-state',self.etc/'material-maintenance',self.etc/'material-config')}
  self.patches=[patch.object(r,key,value) for key,value in paths.items()]+[patch.object(r,'Path',FixturePath),patch.object(r,'MANDATORY',{paths[k] for k in ['PLAN','STATE','PROOF','MATERIAL']}),patch.object(r,'read',side_effect=local_read),patch.object(r,'directory',side_effect=lambda path: self.assertEqual(stat.S_IMODE(Path(path).lstat().st_mode),0o700)),patch.object(old,'ROOT',self.disk)]
  for p in self.patches:p.start()
  self.rows={}
  for i,name in enumerate([old.PG,old.GW,old.WORKER]):self.rows[name]={'Id':str(i+1)*64,'Image':'sha256:'+'1'*64,'Config':{},'HostConfig':{'RestartPolicy':{'Name':'unless-stopped'}},'Mounts':[],'NetworkSettings':{},'RestartCount':0,'State':{'Running':True,'Pid':100+i,'StartedAt':'2026-01-01T00:00:00Z'}}
  containers={name:{'id':row['Id'],'signature':old.sig(row),'pid':row['State']['Pid'],'startedAt':row['State']['StartedAt'],'restartPolicy':row['HostConfig']['RestartPolicy']} for name,row in self.rows.items()}
  baseline={name:{'id':row['Id'],'signature':old.sig(row),'pid':row['State']['Pid'],'startedAt':row['State']['StartedAt'],'restarts':row['RestartCount'],'networkDigest':old.container_generation(row)['networkDigest']} for name,row in self.rows.items() if name!=old.PG}
  proof={'version':1,'eligible':False,'sourceRoot':[1,2,999,999,stat.S_IFDIR|0o700],'targetRoot':r.identity(archive),'files':{},'directories':{},'unstable':[]}
  files,dirs=inventory(archive,999);proof['files']={name:{'target':stamp} for name,stamp in files.items()};proof['directories']={name:r.identity(archive/name) for name in dirs}
  binding={'cluster':'123'};self.plan={'scope':'harbor-pg-storage-move-v1','approved':True,'runId':self.runid,'systemId':'123','activeIdentityDigest':r.sha(r.canonical({'active':'original'})),'containers':containers,'sourceRoots':{'archive':proof['sourceRoot']},'archivePrecopy':{'directory':str(self.purpose),'directoryIdentity':r.identity(self.purpose),'targetRoot':proof['targetRoot'],'deploymentId':'source-release','activeIdentityDigest':r.sha(r.canonical({'active':'original'})),'binding':binding,'writerUnits':['harbor-pg-archive-copy.service']}}
  intent={'eligible':False,'sourceDeletionAllowed':False,'binding':binding};complete={'eligible':False,'sourceDeleted':False,'sourceMounted':False,'servicesChanged':False,'binding':binding,'proofSha256':r.sha(r.canonical(proof))}
  for label,name,value in [('intent','copy.intent.json',intent),('complete','copy.complete.json',complete),('proof','archive-proof.json',proof)]:
   (self.purpose/name).write_bytes(r.canonical(value));self.plan['archivePrecopy'][label+'Sha256']=r.sha(r.canonical(value))
  r.PROOF.write_bytes(r.canonical(proof));r.MATERIAL.write_bytes(b'original approval bytes')
  ticket=self.etc/'global-ticket.json';ticket.write_bytes(r.canonical({'deploymentId':'pending'}));self.plan['interruptTicketSha256']=r.sha(ticket.read_bytes());self.ticket=ticket
  r.PLAN.write_bytes(r.canonical(self.plan));self.plan_sha=r.sha(r.PLAN.read_bytes())
  self.state={'scope':self.plan['scope'],'runId':self.runid,'phase':'recovering-source','failure':{'oldDataRollbackAllowedForExplicitRecovery':True,'automaticOldDataRollback':False},'archiveProofSha256':r.sha(r.PROOF.read_bytes()),'archiveAdopt':{'version':1,'planSha256':self.plan_sha,'directory':str(self.purpose),'proofSha256':self.plan['archivePrecopy']['proofSha256'],'archiveRoot':proof['targetRoot'],'targetRoot':r.identity(self.target),'dbRoot':r.identity(self.target/'db'),'staging':str(self.disk/('.pg-storage-adopt-'+'b'*32))},'createdTargetRoots':{'db':r.identity(self.target/'db'),'archive':proof['targetRoot']},'sourceRecoveryBaseline':{'version':1,'kind':'healthy','planSha256':self.plan_sha,'activeIdentityDigest':self.plan['activeIdentityDigest'],'originalProcesses':baseline}}
  self.network={'id':'original-network'};self.state.update(writers={},sourceRecoveryProcesses=copy.deepcopy(baseline),sourceRecoveryNetwork=copy.deepcopy(self.network),workerGeneration={'id':self.rows[old.WORKER]['Id'],'startedAt':self.rows[old.WORKER]['State']['StartedAt']},applicationsStartEpochMs=int(r.datetime.fromisoformat(self.rows[old.WORKER]['State']['StartedAt'].replace('Z','+00:00')).timestamp()*1000))
  r.STATE.write_bytes(r.canonical(self.state));launch=self.disk/'.pg-storage-switch-launch.json';log=self.disk/('.pg-storage-'+self.runid+'.log');unit='harbor-pg-storage-move-'+self.runid+'.service';launch.write_bytes(r.canonical({'action':'switch','nonce':None,'unit':unit,'runId':self.runid,'planSha256':self.plan_sha,'bootId':str(uuid.uuid4())}));log.write_bytes(b'original fixed failure evidence\n')
  self.a={'runId':self.runid,'files':{str(p):r.sha(p.read_bytes()) for p in [r.PLAN,r.STATE,r.PROOF,r.MATERIAL,launch,log]}};self.raws={p:p.read_bytes() for p in map(Path,self.a['files'])};self.inodes={p:p.stat().st_ino for p in self.raws};self.control={'phase':'active','slot':'candidate','queued':0,'epoch':4};self.worker_live=False;self.mounted=False
  self.core=types.SimpleNamespace(TARGET=self.target,SOURCE_DB=base/'source-db',SOURCE_ARCHIVE=base/'source-archive',STORAGE_START='start.service',STORAGE_ONLINE='online.service',sync_dir=old.__dict__['save'].__globals__.get('unused',None),inventory=inventory)
  control=self.core.SOURCE_DB/'18/docker/global/pg_control';control.parent.mkdir(parents=True);control.write_bytes((123).to_bytes(8,'little'))
  self.core.sync_dir=self.sync;self.core.identity=lambda info:[info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode,info.st_nlink,info.st_size,info.st_mtime_ns,info.st_ctime_ns]
  entry=types.SimpleNamespace(STATE='entry-state',PROFILE='entry-profile',CONTROL='control.sock',validate=lambda value:value,private_json=lambda path:{'postgres_storage_startup_guard':False,'image_staging_enabled':True} if str(path)=='/etc/harbor-gateway-deploy.json' else {'entryCgroup':'/system.slice/entry'} if path=='entry-profile' else {'phase':'activated','activeRuntimeConfig':{}},entry_state=lambda:dict(self.control),verify_resources=lambda *args:None,socket_owner=lambda path:900,process_ticks=lambda pid:'ticks',process_cgroup=lambda pid:'/system.slice/entry',execute=self.execute)
  upgrade=types.SimpleNamespace(active_identity=lambda active:active,release_path=lambda identifier:self.disk/'pending-release',verify_source=lambda *args:None,GATE='gate')
  self.host=object.__new__(old.Host);self.host.core=self.core;self.host.plan=self.plan;self.host.planSha256=self.plan_sha;self.host.j=self.state;self.host.ticket={'deploymentId':'source-release'};self.host.active={'active':'original'};self.host.entry=entry;self.host.upgrade=upgrade;self.host.runtime=types.SimpleNamespace(inspect_entry=lambda entry:{'Id':'entry','State':{'Pid':900,'Running':True,'StartedAt':'original'}})
  self.host.target_disk=lambda:None;self.host.load_current_durable_active=lambda:None;self.host.verify_source_recovery_materials=lambda:None;self.host.immutable=lambda name:copy.deepcopy(self.rows[name]);self.host.state_unit=self.unit;self.host.archive_adopt_unmounted=self.unmounted;self.host.source_recovery_network=lambda:copy.deepcopy(self.network)
  self.retire=object.__new__(r.Retirement);x=self.retire;x.a=self.a;x.approval_sha='c'*64;x.history=r.HISTORY/self.runid;x.etc=r.ETC_HISTORY/self.runid;x.intent=x.history/'retirement.json';x.destinations={Path(path):(x.etc if Path(path)==r.MATERIAL else x.history)/Path(path).name for path in self.a['files']};x.plan=self.plan;x.state=self.state;x.old=old;x.old_save=old.save;x.host=self.host;x.target=self.target;x.shell=x.history/'target-shell';x.purpose=self.purpose;x.archive_old=archive;x.archive_new=self.purpose/'archive';x.claim=self.state['archiveAdopt'];x.journal=None
  original=r.read
  def reads(path,private=False):return self.ticket.read_bytes() if str(path)=='/etc/harbor-postgres-runtime-upgrade.json' else local_read(path,private)
  self.patches.append(patch.object(r,'read',side_effect=reads));self.patches[-1].start()
 def tearDown(self):
  for p in reversed(self.patches):p.stop()
  self.tmp.cleanup()
 def sync(self,path):
  fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY)
  try:os.fsync(fd)
  finally:os.close(fd)
 def execute(self,args):
  self.assertEqual(args,['/usr/sbin/nft','--json','list','tables']);return b'{"nftables":[]}'
 def unit(self,name,allow_absent=False):
  if name in ['start.service','online.service']:return {'LoadState':'not-found'}
  if name=='harbor-pg-archive-copy.service':return {'LoadState':'not-found','MainPID':'0','ActiveState':'inactive','ControlGroup':''}
  return {'LoadState':'loaded','MainPID':'123' if self.worker_live else '0','ActiveState':'active' if self.worker_live else 'failed','SubState':'running' if self.worker_live else 'failed','ControlGroup':'/system.slice/'+name,'InvocationID':'original'}
 def unmounted(self,purpose):
  if self.mounted:raise RuntimeError('target_referenced')
  self.assertEqual(purpose,self.purpose)
 def restart(self):
  fresh=object.__new__(r.Retirement);fresh.__dict__.update(self.retire.__dict__);fresh.plan=json.loads(fresh.evidence(r.PLAN));fresh.state=json.loads(fresh.evidence(r.STATE));fresh.journal=json.loads(local_read(fresh.intent,True)) if fresh.intent.exists() else None
  if fresh.journal is not None:self.assertEqual(fresh.journal['binding'],fresh.binding())
  self.retire=fresh;return fresh
 def test_constructor_reloads_archived_controls_after_retirement(self):
  hostfile=self.etc/'frozen-host.py';hostfile.write_bytes(b'# approved frozen test adapter\n');paths={str(hostfile),str(hostfile.with_name('harbor-pg-storage-coordinator.py')),'/usr/local/bin/harbor-gateway-deploy',*[str(hostfile.with_name(name)) for name in ['gateway-entry-controller.py','postgres-upgrade-deploy.py','postgres-deploy.py','entry-runtime.py','harbor-material-storage.py','harbor-material-move.py']]};tool_bytes={name:(b'# fixed '+name.encode()) for name in paths};tool_bytes[str(hostfile)]=hostfile.read_bytes();self.plan.update(toolSha256={name:r.sha(raw) for name,raw in tool_bytes.items()},materialMove={'approvalSha256':self.a['files'][str(r.MATERIAL)],'nonce':'approved'})
  r.PLAN.write_bytes(r.canonical(self.plan));self.plan_sha=r.sha(r.PLAN.read_bytes());self.host.planSha256=self.plan_sha;self.state['sourceRecoveryBaseline']['planSha256']=self.plan_sha;self.state['archiveAdopt']['planSha256']=self.plan_sha;r.STATE.write_bytes(r.canonical(self.state))
  for path in self.retire.destinations:
   if path.name.endswith('launch.json'):
    value=json.loads(path.read_bytes());value['planSha256']=self.plan_sha;path.write_bytes(r.canonical(value))
  self.a['files']={str(p):r.sha(p.read_bytes()) for p in self.retire.destinations};self.retire.run('retire')
  original_type=types.ModuleType
  def frozen_module(name):
   module=original_type(name);module.__dict__.update(old.__dict__);module.Host=lambda:self.host;return module
  def reads(path,private=False):return tool_bytes[str(path)] if str(path) in tool_bytes else local_read(path,private)
  with patch.object(r,'HOST',hostfile),patch.object(r,'read',side_effect=reads),patch.object(types,'ModuleType',side_effect=frozen_module):fresh=r.Retirement(self.a,self.retire.approval_sha)
  self.assertEqual(fresh.plan['runId'],self.runid);self.assertEqual(fresh.state['phase'],'recovering-source');self.assertEqual(fresh.journal['phase'],'retired-source-untouched');self.assertEqual(fresh.evidence(r.PLAN),r.canonical(self.plan))
 def test_inspect_is_readonly_then_retire_preserves_bytes_and_inodes(self):
  self.assertTrue(self.retire.run('inspect')['eligible']);self.assertFalse(r.HISTORY.exists());self.assertTrue(self.retire.run('retire')['retired'])
  self.assertEqual((self.purpose/'archive').stat().st_ino,self.archive_inode);self.assertEqual((self.purpose/'archive'/'wal').read_bytes(),self.payload);self.assertFalse(self.target.exists())
  for source,destination in self.retire.destinations.items():self.assertFalse(source.exists());self.assertEqual(destination.read_bytes(),self.raws[source]);self.assertEqual(destination.stat().st_ino,self.inodes[source])
  self.assertEqual(json.loads(self.retire.destinations[r.STATE].read_bytes())['phase'],'recovering-source');self.assertTrue(self.retire.run('retire')['retired'])
 def test_atomic_move_ack_loss_at_every_move_resumes_without_recopy(self):
  original=self.retire.move;calls=[]
  def fail_once(source,target):
   original(source,target);calls.append((source,target))
   if len(calls)==1:raise OSError('after atomic rename before ACK')
  with patch.object(self.retire,'move',side_effect=fail_once),self.assertRaises(OSError):self.retire.run('retire')
  self.assertEqual((self.purpose/'archive').stat().st_ino,self.archive_inode);self.assertTrue(self.restart().run('retire')['retired']);self.assertEqual((self.purpose/'archive'/'wal').read_bytes(),self.payload)
 def test_control_rename_ack_loss_reentry_keeps_original_journal_bytes(self):
  original=self.retire.move
  def fail_state(source,target):
   original(source,target)
   if source==r.STATE:raise OSError('after state evidence rename')
  with patch.object(self.retire,'move',side_effect=fail_state),self.assertRaises(OSError):self.retire.run('retire')
  self.assertFalse(r.STATE.exists());self.assertEqual(self.retire.evidence(r.STATE),self.raws[r.STATE]);self.assertTrue(self.restart().run('retire')['retired'])
 def test_each_atomic_rename_ack_loss_resumes_from_persisted_inventory(self):
  for index in range(2+len(self.retire.destinations)):
   with self.subTest(index=index):
    case=Retirement('test_inspect_is_readonly_then_retire_preserves_bytes_and_inodes');case.setUp()
    try:
     targets=[case.retire.archive_new,case.retire.shell]+sorted(case.retire.destinations.values());target=targets[index];rename=old.rename_noreplace
     def fail(source,destination):
      rename(source,destination)
      if destination==target:raise OSError('rename completed before acknowledgement')
     with patch.object(old,'rename_noreplace',side_effect=fail),self.assertRaises(OSError):case.retire.run('retire')
     self.assertTrue(case.restart().run('retire')['retired']);self.assertEqual(case.retire.archive_new.stat().st_ino,case.archive_inode)
     for source,destination in case.retire.destinations.items():self.assertEqual(destination.read_bytes(),case.raws[source]);self.assertEqual(destination.stat().st_ino,case.inodes[source])
    finally:case.tearDown()
 def test_intent_directory_fsync_ack_loss_can_restart_without_prior_moves(self):
  sync=self.sync;failed=[]
  def lose_ack(path):
   sync(path)
   if path==self.retire.history and self.retire.intent.exists() and not failed:failed.append(True);raise OSError('intent durable but fsync ack lost')
  with patch.object(self.core,'sync_dir',side_effect=lose_ack),self.assertRaises(OSError):self.retire.run('retire')
  self.assertTrue(self.retire.intent.exists());self.assertTrue(r.PLAN.exists());self.assertTrue(self.restart().run('retire')['retired'])
 def test_prepare_create_write_fsync_and_prerename_failure_fresh_retry(self):
  for stage in ['create','write','fsync','rename']:
   with self.subTest(stage=stage):
    case=Retirement('test_inspect_is_readonly_then_retire_preserves_bytes_and_inodes');case.setUp()
    try:
     fdopen=os.fdopen;fsync=os.fsync;rename=old.rename_noreplace
     class PartialWriter:
      def __init__(self,stream):self.stream=stream
      def __enter__(self):return self
      def __exit__(self,*args):self.stream.close()
      def write(self,raw):self.stream.write(raw[:len(raw)//2]);raise OSError('partial preparation write')
     def opening(fd,*args,**kwargs):
      if stage=='create':os.close(fd);raise OSError('created preparation file')
      return PartialWriter(fdopen(fd,*args,**kwargs))
     def syncing(fd):
      fsync(fd)
      if stat.S_ISREG(os.fstat(fd).st_mode):raise OSError('preparation file durable')
     def renaming(source,target):
      if target==case.retire.intent:raise OSError('before intent publish')
      rename(source,target)
     context=patch.object(os,'fdopen',side_effect=opening) if stage in ['create','write'] else patch.object(os,'fsync',side_effect=syncing) if stage=='fsync' else patch.object(old,'rename_noreplace',side_effect=renaming)
     with context,self.assertRaises(OSError):case.retire.run('retire')
     self.assertFalse(case.retire.intent.exists());self.assertEqual(list(case.retire.history.iterdir()),[]);residues=list(r.HISTORY.glob('.retirement-*'));self.assertEqual(len(residues),1);residue=residues[0];raw=residue.read_bytes()
     self.assertTrue(case.restart().run('retire')['retired']);self.assertEqual(residue.read_bytes(),raw);self.assertEqual(case.retire.archive_new.stat().st_ino,case.archive_inode)
     for source,target in case.retire.destinations.items():self.assertEqual(target.read_bytes(),case.raws[source]);self.assertEqual(target.stat().st_ino,case.inodes[source])
    finally:case.tearDown()
 def test_each_history_mkdir_parent_fsync_failure_rechecks_all_parents_before_moves(self):
  for index in range(4):
   with self.subTest(index=index):
    case=Retirement('test_inspect_is_readonly_then_retire_preserves_bytes_and_inodes');case.setUp()
    try:
     directories=[r.HISTORY,case.retire.history,r.ETC_HISTORY,case.retire.etc];selected=directories[index];sync=case.sync;failed=[]
     def fail(path):
      if path==selected.parent and selected.exists() and not failed:failed.append(True);raise OSError('mkdir done before parent fsync')
      sync(path)
     with patch.object(case.core,'sync_dir',side_effect=fail),self.assertRaises(OSError):case.retire.run('retire')
     self.assertTrue(selected.exists());self.assertFalse(case.retire.intent.exists());self.assertTrue(r.PLAN.exists());case.restart();observed=[];move=case.retire.move
     def record(path):sync(path);observed.append(path)
     def checked_move(source,target):
      self.assertTrue({path.parent for path in directories}<=set(observed));move(source,target)
     with patch.object(case.core,'sync_dir',side_effect=record),patch.object(case.retire,'move',side_effect=checked_move):self.assertTrue(case.retire.run('retire')['retired'])
     for source,target in case.retire.destinations.items():self.assertEqual(target.read_bytes(),case.raws[source]);self.assertEqual(target.stat().st_ino,case.inodes[source])
    finally:case.tearDown()
 def test_original_evidence_file_fsync_before_rename_and_failure_resume(self):
  fsync=os.fsync;rename=old.rename_noreplace;synced=set();failed=[];chosen=self.inodes[r.MATERIAL]
  def syncing(fd):
   value=os.fstat(fd);fsync(fd)
   if stat.S_ISREG(value.st_mode):
    synced.add(value.st_ino)
    if value.st_ino==chosen and not failed:failed.append(True);raise OSError('original evidence file fsync ack lost')
  def renaming(source,target):
   if source in self.retire.destinations:self.assertIn(source.stat().st_ino,synced)
   rename(source,target)
  with patch.object(os,'fsync',side_effect=syncing),patch.object(old,'rename_noreplace',side_effect=renaming),self.assertRaises(OSError):self.retire.run('retire')
  self.assertTrue(r.MATERIAL.exists());self.assertEqual(r.MATERIAL.read_bytes(),self.raws[r.MATERIAL]);self.assertTrue(self.restart().run('retire')['retired'])
 def test_fresh_reload_requires_full_durability_barrier_before_first_move(self):
  sync=self.sync;failed=[]
  def fail_before_sync(path):
   if path==self.retire.history and self.retire.intent.exists() and not failed:failed.append(True);raise OSError('published intent before actual directory fsync')
   sync(path)
  with patch.object(self.core,'sync_dir',side_effect=fail_before_sync),patch.object(self.retire,'move') as moves,self.assertRaises(OSError):self.retire.run('retire')
  moves.assert_not_called();self.assertTrue(self.retire.intent.exists());self.assertTrue(self.retire.archive_old.exists());self.restart();directories=[];files=[];fsync=os.fsync;move=self.retire.move
  def record_directory(path):sync(path);directories.append(path)
  def record_file(fd):
   info=os.fstat(fd);fsync(fd)
   if stat.S_ISREG(info.st_mode):files.append(info.st_ino)
  def checked_move(source,target):
   paths=[r.HISTORY,self.retire.history,r.ETC_HISTORY,self.retire.etc];self.assertTrue(set(paths+[p.parent for p in paths])<=set(directories));self.assertIn(self.retire.intent.stat().st_ino,files);move(source,target)
  with patch.object(self.core,'sync_dir',side_effect=record_directory),patch.object(os,'fsync',side_effect=record_file),patch.object(self.retire,'move',side_effect=checked_move):self.assertTrue(self.retire.run('retire')['retired'])
 def test_reloaded_intent_file_fsync_failure_never_begins_move(self):
  sync=self.sync
  def fail_published(path):
   if path==self.retire.history and self.retire.intent.exists():raise OSError('intent parent sync not yet executed')
   sync(path)
  with patch.object(self.core,'sync_dir',side_effect=fail_published),self.assertRaises(OSError):self.retire.run('retire')
  self.restart();fsync=os.fsync;chosen=self.retire.intent.stat().st_ino
  def fail_file(fd):
   if os.fstat(fd).st_ino==chosen and stat.S_ISREG(os.fstat(fd).st_mode):raise OSError('barrier file sync before actual execution')
   fsync(fd)
  with patch.object(os,'fsync',side_effect=fail_file),patch.object(self.retire,'move') as moves,self.assertRaises(OSError):self.retire.run('retire')
  moves.assert_not_called();self.assertTrue(self.retire.archive_old.exists());self.assertTrue(self.restart().run('retire')['retired'])
 def test_cross_filesystem_move_refused_without_copy(self):
  try:other=tempfile.TemporaryDirectory(prefix='harbor-retire-owned-',dir='/dev/shm')
  except PermissionError:self.skipTest('owned second filesystem unavailable')
  try:
   if Path(other.name).stat().st_dev==self.target.stat().st_dev:self.skipTest('second filesystem has same device')
   destination=Path(other.name)/'archive';before=(self.target/'archive').stat().st_ino
   with self.assertRaisesRegex(RuntimeError,'cross_device'):self.retire.move(self.target/'archive',destination)
   self.assertFalse(destination.exists());self.assertEqual((self.target/'archive').stat().st_ino,before)
  finally:other.cleanup()
 def test_same_bytes_replacement_after_intent_is_not_reclaimed(self):
  original=self.retire.move
  def replacement(source,target):
   original(source,target);raw=r.PLAN.read_bytes();temporary=r.PLAN.with_name('replacement');temporary.write_bytes(raw);os.replace(temporary,r.PLAN)
  with patch.object(self.retire,'move',side_effect=replacement),self.assertRaisesRegex(RuntimeError,'retirement_binding_changed'):self.retire.run('retire')
  self.assertTrue(self.retire.intent.exists());self.assertEqual(self.retire.archive_new.stat().st_ino,self.archive_inode)
 def test_changed_source_entry_worker_or_target_refused_without_touching_data(self):
  for fault in ['source','entry','worker','mount','armed','material','db']:
   with self.subTest(fault=fault):
    undo=None
    if fault=='source':self.rows[old.PG]['State']['Pid']+=1;undo=lambda:self.rows[old.PG]['State'].update(Pid=self.plan['containers'][old.PG]['pid'])
    elif fault=='entry':self.control['phase']='holding';undo=lambda:self.control.update(phase='active')
    elif fault=='worker':self.worker_live=True;undo=lambda:setattr(self,'worker_live',False)
    elif fault=='mount':self.mounted=True;undo=lambda:setattr(self,'mounted',False)
    elif fault=='armed':self.state['targetBootAuthorized']=True;undo=lambda:self.state.pop('targetBootAuthorized')
    elif fault=='material':r.ABSENT[0].touch();undo=lambda:r.ABSENT[0].unlink()
    else:(self.target/'db'/'unknown').write_bytes(b'data');undo=lambda:(self.target/'db'/'unknown').unlink()
    try:
     with self.assertRaises(RuntimeError):self.retire.run('retire')
     self.assertFalse(r.HISTORY.exists());self.assertEqual((self.target/'archive').stat().st_ino,self.archive_inode)
    finally:undo()
 def test_observed_nonrestart_recovery_witnesses_allowed_but_changed_content_refused(self):
  self.assertEqual(self.state['writers'],{});self.assertTrue(self.retire.run('inspect')['eligible'])
  for field in ['writers','applicationsStartEpochMs','workerGeneration','sourceRecoveryProcesses','sourceRecoveryNetwork']:
   with self.subTest(field=field):
    saved=copy.deepcopy(self.state[field])
    if field=='writers':self.state[field]={'backup.service':{'ActiveState':'inactive'}}
    elif field=='applicationsStartEpochMs':self.state[field]+=1
    elif field=='workerGeneration':self.state[field]['startedAt']='changed'
    elif field=='sourceRecoveryProcesses':self.state[field][old.WORKER]['restarts']+=1
    else:self.state[field]['id']='foreign-network'
    try:
     with self.assertRaises(RuntimeError):self.retire.run('retire')
     self.assertFalse(r.HISTORY.exists())
    finally:self.state[field]=saved
 def test_archive_not_found_is_terminal_but_migration_worker_not_found_is_not(self):
  self.assertEqual(self.unit('harbor-pg-archive-copy.service')['LoadState'],'not-found');self.assertTrue(self.retire.run('inspect')['eligible']);original=self.unit
  def missing(name,allow_absent=False):
   if name.startswith('harbor-pg-storage-move-'):return {'LoadState':'not-found','ActiveState':'inactive','MainPID':'0','ControlGroup':''}
   return original(name,allow_absent)
  with patch.object(self.host,'state_unit',side_effect=missing),self.assertRaisesRegex(RuntimeError,'worker_not_quiet'):self.retire.run('retire')
  self.assertFalse(r.HISTORY.exists())
 def test_synthetic_missing_v1_and_wrong_type_cgroup_layouts_refused(self):
  marker=self.cgroup/'cgroup.controllers';marker.unlink()
  for layout in ['missing','v1','wrong-type']:
   with self.subTest(layout=layout):
    if layout=='v1':
     (self.cgroup/'memory').mkdir();(self.cgroup/'memory/tasks').write_text('')
    if layout=='wrong-type':marker.mkdir()
    try:
     self.assertEqual(r.Path('/sys/fs/cgroup/cgroup.controllers'),marker)
     with self.assertRaisesRegex(RuntimeError,'cgroup_layout_unknown'):self.retire.run('inspect')
     self.assertFalse(r.HISTORY.exists())
    finally:
     if marker.is_dir():marker.rmdir()
  marker.write_text('cpu memory pids\n');self.assertTrue(self.retire.run('inspect')['eligible'])
 def test_synthetic_populated_and_missing_population_refuse_both_worker_kinds(self):
  for name in ['harbor-pg-storage-move-'+self.runid+'.service','harbor-pg-archive-copy.service']:
   events=self.cgroup/'system.slice'/name/'cgroup.events'
   for value in ['populated 1\nfrozen 0\n','frozen 0\n']:
    with self.subTest(unit=name,events=value):
     events.write_text(value)
     with self.assertRaisesRegex(RuntimeError,'worker_descendants_alive'):self.retire.run('inspect')
     self.assertFalse(r.HISTORY.exists());events.write_text('populated 0\nfrozen 0\n')
  self.assertTrue(self.retire.run('inspect')['eligible'])
 def test_unexpected_global_worker_intent_is_not_ignored(self):
  path=self.disk/'.pg-storage-recover-source-launch-1234567890abcdef.json';path.write_text('{}')
  with self.assertRaisesRegex(RuntimeError,'unknown_worker_intent'):self.retire.run('retire')
  self.assertFalse(r.HISTORY.exists())
 def test_external_change_after_move_retains_intent_and_original_files(self):
  original=self.retire.move
  def change(source,target):original(source,target);self.control['epoch']+=1
  with patch.object(self.retire,'move',side_effect=change),self.assertRaisesRegex(RuntimeError,'external_state_changed'):self.retire.run('retire')
  self.assertTrue(self.retire.intent.exists());self.assertTrue(r.PLAN.exists());self.assertEqual((self.purpose/'archive').stat().st_ino,self.archive_inode)
 def test_unknown_destination_or_suspicious_archive_kept(self):
  (self.purpose/'archive').mkdir()
  with self.assertRaisesRegex(RuntimeError,'archive_ambiguous'):self.retire.run('retire')
  self.assertTrue(r.PLAN.exists());self.assertEqual((self.target/'archive').stat().st_ino,self.archive_inode)

if __name__=='__main__':unittest.main()
