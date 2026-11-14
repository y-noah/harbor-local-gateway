"""Fixed-instance migration core. No I/O at import; no online command entry.
Host adapters must supply authenticated current ACTIVE/container/mount evidence.
"""
import hashlib,json,os,stat,uuid
from pathlib import Path
BASE=Path('/opt/product-development/harbor-local-gateway/postgres-infra')
SOURCE_DB=BASE/'db';SOURCE_ARCHIVE=BASE/'archive'
TARGET=Path('/sdc-disk/harbor-data/postgres')
PG_NAME='harbor-local-gateway-postgres-infra'
WRITERS=('harbor-postgres-maintain-base.timer','harbor-postgres-maintain-base.service','harbor-postgres-backup-schedule.service')
SCOPE='harbor-pg-storage-move-v1'
STORAGE_START='harbor-pg-storage-start.service';STORAGE_ONLINE='harbor-pg-storage-online.service'
MAX_FILES=100000

def need(value,code):
 if not value:raise RuntimeError(code)

def runtime_mode(journal):
 control=journal.get('runtimeControl',{});need(isinstance(control,dict),'storage_runtime_mode_unknown');mode=control.get('mode','running');need(mode in ['running','paused','planned','recovering'],'storage_runtime_mode_unknown');return mode

def recovery_mode(journal):
 mode=runtime_mode(journal);need(mode in ['running','recovering'],'storage_resume_required');return mode

def identity(info):
 return [info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode,info.st_nlink,info.st_size,info.st_mtime_ns,info.st_ctime_ns]

def directory(path,uid):
 value=path.lstat();need(stat.S_ISDIR(value.st_mode) and value.st_uid==uid and not value.st_mode&0o077,'storage_directory_untrusted');return value

def inventory(root,uid,*,live=False):
 directory(root,uid);device=root.stat().st_dev;files={};dirs={'.':{'mode':stat.S_IMODE(root.stat().st_mode),'gid':root.stat().st_gid}}
 def scan_error(error):
  if live and isinstance(error,FileNotFoundError):return
  raise error
 for parent,names,leaves in os.walk(root,followlinks=False,onerror=scan_error):
  for name in sorted(names+leaves):
   path=Path(parent)/name
   try:info=path.lstat()
   except FileNotFoundError:
    if live:continue
    raise
   relative=str(path.relative_to(root))
   need(stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode),'storage_entry_type_untrusted')
   need(info.st_dev==device and info.st_uid==uid and not info.st_mode&0o077,'storage_entry_untrusted')
   if stat.S_ISDIR(info.st_mode):dirs[relative]={'mode':stat.S_IMODE(info.st_mode),'gid':info.st_gid}
   else:
    need(stat.S_ISREG(info.st_mode) and info.st_nlink==1,'storage_entry_type_untrusted');files[relative]=identity(info)
   need(len(files)+len(dirs)<=MAX_FILES,'storage_inventory_capacity')
 return files,dirs

def digest_file(path,uid):
 fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
 try:
  before=os.fstat(fd);need(stat.S_ISREG(before.st_mode) and before.st_uid==uid and before.st_nlink==1 and not before.st_mode&0o077,'storage_file_untrusted')
  digest=hashlib.sha256()
  while True:
   chunk=os.read(fd,1048576)
   if not chunk:break
   digest.update(chunk)
  after=os.fstat(fd);need(identity(before)==identity(after)==identity(path.lstat()),'storage_file_changed')
  return {'identity':identity(after),'sha256':digest.hexdigest()}
 finally:os.close(fd)

def sync_dir(path):
 fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
 try:os.fsync(fd)
 finally:os.close(fd)

class ArchiveMirror:
 """Online stable byte proof, then cold metadata reuse plus changed-file proof.
 The destination root must already be exclusively claimed in a durable journal;
 it must never be mounted into PG before finalize returns. No source deletion.
 """
 def __init__(self,source,target,uid):
  self.source=Path(source);self.target=Path(target);self.uid=uid
 def copy_one(self,name):
  source=self.source/name;target=self.target/name;fd=os.open(source,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK);temp=None
  try:
   before=os.fstat(fd);need(stat.S_ISREG(before.st_mode) and before.st_uid==self.uid and before.st_nlink==1 and not before.st_mode&0o077,'storage_file_untrusted')
   # Every parent was inventoried without following symlinks. Check the chain
   # again immediately before creating a private sibling, including target root.
   parent=target.parent
   while True:
    directory(parent,self.uid)
    if parent==self.target:break
    need(self.target in parent.parents,'storage_target_escape');parent=parent.parent
   temp=target.with_name('.migration-copy-'+uuid.uuid4().hex)
   out=os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600);digest=hashlib.sha256()
   with os.fdopen(out,'wb') as stream:
    while True:
     chunk=os.read(fd,1048576)
     if not chunk:break
     stream.write(chunk);digest.update(chunk)
    stream.flush();os.fchmod(stream.fileno(),stat.S_IMODE(before.st_mode));os.fchown(stream.fileno(),before.st_uid,before.st_gid);os.fsync(stream.fileno())
   need(identity(before)==identity(os.fstat(fd))==identity(source.lstat()),'storage_file_changed')
   os.replace(temp,target);temp=None;sync_dir(target.parent)
   proof=digest_file(target,self.uid);need(proof['sha256']==digest.hexdigest(),'storage_copy_digest_mismatch')
   return {'source':identity(before),'target':proof['identity'],'sha256':proof['sha256']}
  finally:
   os.close(fd)
   if temp is not None:temp.unlink(missing_ok=True)
 def online(self):
  source,dirs=inventory(self.source,self.uid,live=True);target,target_dirs=inventory(self.target,self.uid)
  need(not target and set(target_dirs)=={'.'},'storage_initial_target_not_empty')
  for name,attrs in sorted(dirs.items(),key=lambda row:len(Path(row[0]).parts)):
   if name=='.':continue
   path=self.target/name;path.mkdir(mode=attrs['mode']);os.chown(path,self.uid,attrs['gid'])
  proofs={};unstable=[]
  for name in sorted(source):
   try:proofs[name]=self.copy_one(name)
   except FileNotFoundError:unstable.append(name)
   except RuntimeError as error:
    if str(error)!='storage_file_changed':raise
    unstable.append(name)
  return {'version':1,'sourceRoot':identity(self.source.stat())[:5],'targetRoot':identity(self.target.stat())[:5],'files':proofs,'directories':{name:identity((self.target/name).stat())[:5] for name in dirs},'unstable':unstable,'eligible':False}
 def finalize(self,prior,*,source_stopped):
  need(source_stopped is True,'storage_source_not_stopped')
  need(prior.get('version')==1 and prior.get('eligible') is False and prior.get('sourceRoot')==identity(self.source.stat())[:5] and prior.get('targetRoot')==identity(self.target.stat())[:5],'storage_prepared_roots_changed')
  source,dirs=inventory(self.source,self.uid);target,target_dirs=inventory(self.target,self.uid);proofs=prior['files'];reused=0;copied=0
  for name,stamp in target.items():
   need(name in proofs and stamp==proofs[name]['target'],'storage_unknown_or_changed_target_file')
  for name in target_dirs:
   need(name in prior.get('directories',{}) and identity((self.target/name).lstat())[:5]==prior['directories'][name],'storage_unknown_or_changed_target_directory')
  # Online proof binds both file identities. Reuse only exact unchanged tuples;
  # changed/new bytes are copied and independently hashed in the cold window.
  for name,attrs in sorted(dirs.items(),key=lambda row:len(Path(row[0]).parts)):
   path=self.target/name
   if name not in target_dirs:path.mkdir(mode=attrs['mode'])
   os.chmod(path,attrs['mode']);os.chown(path,self.uid,attrs['gid'])
  final={}
  for name,stamp in sorted(source.items()):
   old=proofs.get(name)
   if old and stamp==old['source'] and target.get(name)==old['target']:
    final[name]=old;reused+=1
   else:final[name]=self.copy_one(name);copied+=1
  for name,stamp in target.items():
   if name in source:continue
   old=proofs.get(name);need(old is not None and stamp==old['target'],'storage_unknown_target_entry')
   (self.target/name).unlink();sync_dir((self.target/name).parent)
  for name in sorted(set(target_dirs)-set(dirs),key=lambda value:len(Path(value).parts),reverse=True):
   need(name!='.','storage_target_root_delete');(self.target/name).rmdir()
  actual,actual_dirs=inventory(self.target,self.uid)
  need(set(actual)==set(source) and actual_dirs==dirs,'storage_cold_inventory_mismatch')
  need(inventory(self.source,self.uid)==(source,dirs),'storage_cold_source_changed')
  need(all(actual[name]==row['target'] for name,row in final.items()),'storage_cold_target_changed')
  sync_dir(self.target)
  return {'eligible':True,'files':final,'directories':{name:identity((self.target/name).stat())[:5] for name in dirs},'reusedFiles':reused,'copiedFiles':copied,'sourceRoot':prior['sourceRoot'],'targetRoot':prior['targetRoot']}

def startup_units(db_mount,archive_mount):
 need(all(isinstance(v,str) and v.endswith('.mount') and '/' not in v and '\n' not in v for v in [db_mount,archive_mount]),'storage_unit_invalid')
 mounts=db_mount+' '+archive_mount
 common='Requires='+mounts+'\nBindsTo='+mounts+'\nAfter=docker.service '+mounts+' harbor-entry-runtime.service\n'
 start='[Unit]\nDescription=Harbor PG storage and authorized original instance start\nRequires=docker.service harbor-entry-runtime.service\n'+common+'Before=harbor-entry-candidate.service harbor-gateway-entry.service\n\n[Service]\nType=oneshot\nRemainAfterExit=yes\nUMask=0077\nExecStart=/usr/bin/python3 -I -B /usr/local/lib/harbor/harbor-pg-storage-host.py boot-start\nTimeoutStartSec=infinity\nTimeoutStopSec=infinity\nSendSIGKILL=no\n\n[Install]\nWantedBy=multi-user.target\n'
 online='[Unit]\nDescription=Harbor migrated instance online acceptance and supervision\nRequires='+STORAGE_START+' harbor-gateway-entry.service\nAfter='+STORAGE_START+' harbor-gateway-entry.service\n\n[Service]\nType=notify\nNotifyAccess=main\nRestart=always\nRestartSec=2\nUMask=0077\nExecStart=/usr/bin/python3 -I -B /usr/local/lib/harbor/harbor-pg-storage-host.py boot-online\nTimeoutStartSec=180\n\n[Install]\nWantedBy=multi-user.target\n'
 candidate='[Unit]\nRequires='+STORAGE_START+'\nAfter='+STORAGE_START+'\n'
 writer='[Unit]\nRequires='+STORAGE_ONLINE+'\nAfter='+STORAGE_ONLINE+'\n'
 return {STORAGE_START:start,STORAGE_ONLINE:online,'harbor-entry-candidate.service.d/90-pg-storage.conf':candidate,'harbor-gateway-entry.service.d/90-pg-storage.conf':candidate,**{v+'.d/90-pg-storage.conf':writer for v in WRITERS}}

# Only these edges are added to the existing instance graph, never docker.service.
BOOT_PHASES=frozenset({'boot-arm-intent','boot-installed','target-start-intent','pg-ready','applications-ready','network-rebound','entry-restored','unit-handoff-intent','units-active','complete','boot-start-intent','boot-containers-ready','runtime-recovering','runtime-entry-pending'})
BOOT_EDGES=(('docker','entry-guard'),('entry-guard','storage-start'),('mount-db','storage-start'),('mount-archive','storage-start'),('storage-start','candidate-bridge'),('candidate-bridge','entry-listener'),('entry-listener','storage-online'),('storage-online','backup-writers'))

class Coordinator:
 """Ordered transaction boundary; ops are supplied by the hash-pinned host adapter.
 Each intent is durable before irreversible action. Recovery must never select
 the old data once target-start-intent was persisted, including publisher writes.
 """
 def __init__(self,ops,journal):runtime_mode(journal);self.ops=ops;self.journal=journal
 def mark(self,phase,**values):
  self.journal.update(phase=phase,**values);self.ops.save(self.journal)
 def switch(self):
  o=self.ops;need(self.journal.get('phase')=='archive-prepared','storage_phase_invalid');o.current_identity()
  try:
   o.observe_source_recovery_baseline()
   self.mark('holding-intent');o.hold_drain();self.mark('held')
   gate=o.install_gate();self.mark('gated',gate=gate)
   writers=o.pause_writers();self.mark('writers-paused',writers=writers)
   o.fence_docker_autostart();self.mark('autostart-fenced')
   o.stop_applications();self.mark('applications-stopped')
   self.mark('pg-stop-intent');o.stop_postgres_clean();self.mark('source-cold')
   o.finalize_archive();o.copy_cold_database();self.mark('cold-verified')
   o.bind_original_paths();self.mark('bound')
   self.mark('boot-arm-intent',oldDataRollbackForbidden=True,targetBootAuthorized=True)
   o.prepare_application_materials()
   o.install_instance_boot_guard();self.mark('boot-installed')
   self.mark('target-start-intent')
   o.start_postgres();o.verify_postgres_mount_tls_report_wal();self.mark('pg-ready')
   o.start_applications();o.verify_current_approval_and_private_readiness();self.mark('applications-ready')
   o.update_dynamic_network_snapshot();self.mark('network-rebound')
   o.refresh_active_entry();o.verify_full_instance();o.record_runtime_ready();self.mark('entry-restored')
   self.mark('unit-handoff-intent');o.activate_running_units();self.mark('units-active')
   o.remove_gate(gate);o.restore_writers(writers,nonblocking=True);o.verify_writer_restoration(writers);self.mark('complete')
   return {'complete':True,'sameOriginalContainers':True,'oldSourceRetained':True,'restorePerformed':False}
  except BaseException:
   # No broad catch that starts old data or unconditionally releases holding.
   # A pre-target failure may recover only through explicit original-state proof.
   self.ops.record_failure(self.journal,old_data_allowed=not self.journal.get('oldDataRollbackForbidden',False));raise
 def recover_source(self):
  recovery_mode(self.journal)
  try:
   need(not self.journal.get('oldDataRollbackForbidden',False) and not self.journal.get('targetBootAuthorized',False),'storage_old_data_rollback_forbidden')
   o=self.ops;o.verify_original_recovery_checkpoint();self.mark('recovering-source')
   o.restore_original_restart_policies();o.start_original_postgres();o.verify_original_postgres();o.start_applications(source_recovery=True);o.verify_current_approval_and_private_readiness(source_recovery=True);o.update_dynamic_network_snapshot()
   if o.managed_source_recovery():o.verify_original_full_instance(before_publish=True);o.refresh_active_entry();o.verify_existing_entry_restored()
   else:o.refresh_active_entry();o.verify_original_full_instance()
   if self.journal.get('gate') is not None:o.remove_gate(self.journal['gate'])
   o.restore_writers(self.journal['writers'],nonblocking=True);o.verify_writer_restoration(self.journal['writers']);self.mark('source-restored');return {'sourceRestored':True,'partialTargetRetained':True,'targetNeverArmed':True}
  except BaseException:
   self.ops.record_failure(self.journal,old_data_allowed=not self.journal.get("oldDataRollbackForbidden",False));raise
 def recover_target(self):
  recovery_mode(self.journal)
  try:
   o=self.ops;o.verify_target_recovery_checkpoint();self.mark('boot-arm-intent',oldDataRollbackForbidden=True,targetBootAuthorized=True);o.prepare_application_materials();o.install_instance_boot_guard();self.mark('target-start-intent');o.start_postgres();o.verify_postgres_mount_tls_report_wal();o.start_applications();o.verify_current_approval_and_private_readiness();o.update_dynamic_network_snapshot();o.refresh_active_entry();o.verify_full_instance();o.record_runtime_ready();self.mark('unit-handoff-intent');o.activate_running_units();self.mark('units-active');o.remove_gate(self.journal['gate']);o.restore_writers(self.journal['writers'],nonblocking=True);o.verify_writer_restoration(self.journal['writers']);self.mark('complete');return {'targetRestored':True,'oldDataRollbackForbidden':True}
  except BaseException:
   self.ops.record_failure(self.journal,old_data_allowed=not self.journal.get("oldDataRollbackForbidden",False));raise
 def boot_start(self):
  o=self.ops
  if runtime_mode(self.journal) in ['paused','planned']:return {'paused':True,'containersStarted':False}
  need(self.journal.get('targetBootAuthorized') is True and self.journal.get('oldDataRollbackForbidden') is True and self.journal.get('phase') in BOOT_PHASES,'storage_boot_not_armed');o.require_boot_guard_and_mounts();o.require_docker_restart_disabled();o.load_current_durable_active();o.ensure_instance_boot_guard()
  gate=o.install_gate();self.mark('boot-start-intent',gate=gate,oldDataRollbackForbidden=True)
  o.start_postgres();o.verify_postgres_mount_tls_report_wal();o.start_applications();o.verify_current_approval_and_private_readiness();o.update_dynamic_network_snapshot(boot=True)
  # candidate.sock does not exist yet: existing candidate-post owns refresh_active.
  self.mark('boot-containers-ready');return {'containersReady':True,'entryNotPublished':True}
 def boot_online(self):
  recovery_mode(self.journal)
  o=self.ops;need(self.journal.get('phase')=='boot-containers-ready','storage_boot_phase_invalid');o.verify_existing_entry_restored();o.verify_full_instance();o.remove_gate(self.journal['gate']);o.signal_online_ready();o.restore_writers(self.journal['writers'],nonblocking=True);o.verify_writer_restoration(self.journal['writers']);o.record_runtime_ready();self.mark('complete');return {'online':True}

if __name__=='__main__':
 import sys
 print(json.dumps({'failed':True,'error':'storage_host_adapter_not_bound','onlineActionExecuted':False}))
 raise SystemExit(2)
