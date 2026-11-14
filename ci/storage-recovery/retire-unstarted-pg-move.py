"""Retire one approved, failed, source-untouched PG move. Never restore data.

inspect is read-only. retire preserves original bytes/inodes, returns the adopted
archive to its original precopy purpose, and frees only the bound control names.
It never holds entry, stops/starts services, changes the old journal, or recopies.
"""
import contextlib,hashlib,json,os,re,stat,sys,types,uuid
from datetime import datetime
from pathlib import Path

ROOT=Path('/sdc-disk/harbor-data')
PLAN=ROOT/'.pg-storage-plan.json'
STATE=ROOT/'.pg-storage-state.json'
PROOF=ROOT/'.pg-storage-archive-proof.json'
MATERIAL=Path('/etc/harbor-material-move.json')
HOST=Path('/usr/local/lib/harbor/harbor-pg-storage-host.py')
APPROVAL=Path('/etc/harbor-pg-storage-retirement.json')
HISTORY=ROOT/'.pg-storage-retired'
ETC_HISTORY=Path('/etc/harbor-pg-storage-retired')
ENV={'PATH':'/usr/bin:/bin','LANG':'C','HOME':'/nonexistent','DOCKER_CONFIG':'/nonexistent','DOCKER_HOST':'unix:///run/docker.sock'}
ABSENT=(ROOT/'.pg-storage-db-proof.json',ROOT/'.material-move-state.json',Path('/etc/harbor-material-maintenance.json'),Path('/etc/harbor-data-paths.json'))
TOUCHED={'gate','gateIntent','writersPauseStarted','restartPolicies','restartPolicyIds','autostartFenced','oldRoots','mountpointIdentities','installedUnitSha256','handoff','dbProofSha256','runtimeReceipt','runtimeRecovery','plannedOperation','runtimeControl'}
MANDATORY={PLAN,STATE,PROOF,MATERIAL}


def need(ok,code):
 if not ok:raise RuntimeError('pg_retirement_'+code)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':')).encode()

def read(path,private=False):
 path=Path(path)
 for parent in path.parents:
  s=parent.lstat();need(stat.S_ISDIR(s.st_mode) and s.st_uid==0 and not s.st_mode&0o022,'untrusted_parent')
 fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
 with os.fdopen(fd,'rb') as stream:
  s=os.fstat(stream.fileno());need(stat.S_ISREG(s.st_mode) and s.st_uid==0 and s.st_nlink==1 and not s.st_mode&0o022 and s.st_size<=33554432 and (not private or stat.S_IMODE(s.st_mode)==0o600),'untrusted_file');raw=stream.read(33554433);after=path.lstat()
  need(len(raw)==s.st_size and (s.st_dev,s.st_ino,s.st_mtime_ns,s.st_ctime_ns)==(after.st_dev,after.st_ino,after.st_mtime_ns,after.st_ctime_ns),'file_changed')
 return raw

def identity(path):
 s=Path(path).lstat();return [s.st_dev,s.st_ino,s.st_uid,s.st_gid,s.st_mode]

def directory(path):
 s=Path(path).lstat();need(stat.S_ISDIR(s.st_mode) and s.st_uid==0 and stat.S_IMODE(s.st_mode)==0o700,'history_untrusted')

def approval():
 raw=read(APPROVAL,True);value=json.loads(raw)
 need(set(value)=={'version','approved','id','runId','files','installerSha256','revision','pipeline','artifactJob'} and type(value['version']) is int and value['version']==1 and value['approved'] is True and str(uuid.UUID(value['id']))==value['id'],'approval_invalid')
 need(re.fullmatch('[a-f0-9]{32}',value['runId']) and re.fullmatch('[a-f0-9]{40}',value['revision']) and all(type(value[k]) is int and value[k]>0 for k in ['pipeline','artifactJob']),'approval_identity_invalid')
 need(isinstance(value['files'],dict) and all(isinstance(k,str) and str(Path(k))==k and isinstance(v,str) and re.fullmatch('[a-f0-9]{64}',v) for k,v in value['files'].items()),'file_inventory_invalid')
 paths=set(map(Path,value['files']));need(MANDATORY<=paths,'required_evidence_missing')
 for path in paths-MANDATORY:
  need(path.parent==ROOT and (re.fullmatch(r'\.pg-storage-(?:switch|recover-source)-launch(?:-[a-f0-9]{16})?\.json',path.name) or re.fullmatch(r'\.pg-storage-'+value['runId']+r'(?:-recover-source-[a-f0-9]{16})?\.log',path.name)),'unapproved_path')
 need(sha(read(Path(__file__)))==value['installerSha256'],'installer_unapproved')
 return value,sha(raw)

class Retirement:
 def __init__(self,value,approval_sha):
  self.a=value;self.approval_sha=approval_sha;self.history=HISTORY/value['runId'];self.etc=ETC_HISTORY/value['runId'];self.intent=self.history/'retirement.json'
  self.destinations={Path(path):(self.etc if Path(path)==MATERIAL else self.history)/Path(path).name for path in value['files']}
  self.plan=json.loads(self.evidence(PLAN));self.state=json.loads(self.evidence(STATE))
  need(self.plan.get('runId')==value['runId']==self.state.get('runId') and self.plan.get('scope')==self.state.get('scope')=='harbor-pg-storage-move-v1' and self.plan.get('approved') is True,'run_changed')
  tools={str(HOST),str(HOST.with_name('harbor-pg-storage-coordinator.py')),'/usr/local/bin/harbor-gateway-deploy',*[str(HOST.with_name(name)) for name in ['gateway-entry-controller.py','postgres-upgrade-deploy.py','postgres-deploy.py','entry-runtime.py','harbor-material-storage.py','harbor-material-move.py']]}
  need(set(self.plan.get('toolSha256',{}))==tools and self.plan.get('materialMove',{}).get('approvalSha256')==value['files'][str(MATERIAL)],'old_tool_inventory_changed')
  self.old=types.ModuleType('retirement_frozen_host');self.old.__file__=str(HOST)
  for name,digest in self.plan['toolSha256'].items():need(sha(read(Path(name)))==digest,'old_tool_changed')
  exec(compile(read(HOST),str(HOST),'exec'),self.old.__dict__)
  # Only control read locations change in this private module. No old save,
  # recovery, current_identity/active_record, or worker entrypoint may execute.
  self.old.PLAN=self.location(PLAN);self.old.STATE=self.location(STATE)
  self.old_save=self.old.save;self.old.save=lambda *args,**kwargs:need(False,'old_evidence_write_forbidden')
  original_load=self.old.load
  def load(path,name):
   module=original_load(path,name)
   if path==self.old.MATERIAL_MOVE:module.APPROVAL=self.location(MATERIAL)
   return module
  self.old.load=load;self.host=self.old.Host();self.target=self.host.core.TARGET;self.shell=self.history/'target-shell'
  self.purpose=Path(self.plan['archivePrecopy']['directory']);need(self.purpose.parent==ROOT and self.purpose.name not in ['postgres','',HISTORY.name],'precopy_path_invalid')
  self.archive_old=self.target/'archive';self.archive_new=self.purpose/'archive';self.claim=self.state.get('archiveAdopt',{})
  self.journal=json.loads(read(self.intent,True)) if os.path.lexists(self.intent) else None
  if self.journal is not None:need(set(self.journal)=={'version','approvalSha256','runId','binding','before','phase'} and self.journal['version']==1 and self.journal['approvalSha256']==approval_sha and self.journal['runId']==value['runId'] and self.journal['binding']==self.binding() and self.journal['phase'] in ['moving','retired-source-untouched'],'retirement_journal_changed')

 def location(self,path):
  destination=self.destinations[path];a=os.path.lexists(path);b=os.path.lexists(destination);need(a!=b,'evidence_ambiguous');return path if a else destination

 def evidence(self,path):
  raw=read(self.location(path),True);need(sha(raw)==self.a['files'][str(path)],'evidence_changed');return raw

 def binding(self):return {'files':self.a['files'],'evidenceRoots':{str(path):identity(self.location(path)) for path in self.destinations},'archiveRoot':self.plan['archivePrecopy']['targetRoot'],'archiveFrom':str(self.archive_old),'archiveTo':str(self.archive_new),'targetRoot':self.claim.get('targetRoot'),'dbRoot':self.claim.get('dbRoot'),'targetFrom':str(self.target),'targetTo':str(self.shell)}

 def quiet(self,name,archive_writer=False):
  state=self.host.state_unit(name,allow_absent=True)
  need(isinstance(state.get('ControlGroup'),str),'cgroup_unknown')
  absent=archive_writer and state.get('LoadState')=='not-found' and state.get('ActiveState')=='inactive' and state.get('ControlGroup')==''
  terminal=state.get('LoadState')=='loaded' and (state.get('ActiveState') in ['inactive','failed'] or state.get('ActiveState')=='active' and state.get('SubState')=='exited')
  need(state.get('MainPID')=='0' and (absent or terminal),'worker_not_quiet')
  need(Path('/sys/fs/cgroup/cgroup.controllers').is_file(),'cgroup_layout_unknown');group=state.get('ControlGroup','')
  if group:
   need(group.startswith('/') and group!='/' and group==os.path.normpath(group),'cgroup_unknown');events=Path('/sys/fs/cgroup')/group.lstrip('/')/'cgroup.events'
   if events.exists():need(dict(line.split() for line in events.read_text().splitlines()).get('populated')=='0','worker_descendants_alive')
  if absent:
   events=Path('/sys/fs/cgroup/system.slice')/name/'cgroup.events'
   if events.exists():need(dict(line.split() for line in events.read_text().splitlines()).get('populated')=='0','worker_descendants_alive')
  return {k:state.get(k) for k in ['LoadState','ActiveState','SubState','MainPID','ControlGroup','InvocationID']}

 def workers(self):
  intents=[p for p in self.destinations if p.suffix=='.json' and p not in MANDATORY];expected=set();result={}
  need(intents,'worker_evidence_missing')
  for path in intents:
   value=json.loads(self.evidence(path));action=value.get('action');need(action in ['switch','recover-source'],'worker_action_invalid')
   unit,name,log=self.old.worker_paths(self.host,action,value.get('nonce'));need(name==path and log in self.destinations and value.get('unit')==unit and value.get('runId')==self.a['runId'] and value.get('planSha256')==self.a['files'][str(PLAN)],'worker_binding_changed')
   need(re.fullmatch('[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}',value.get('bootId','')),'worker_boot_invalid');expected.add(path);result[unit]=self.quiet(unit)
  need(set(ROOT.glob('.pg-storage-*launch*.json'))<=expected,'unknown_worker_intent')
  need({p for p in self.destinations if p.suffix=='.log'}=={self.old.worker_paths(self.host,json.loads(self.evidence(p))['action'],json.loads(self.evidence(p)).get('nonce'))[2] for p in intents},'worker_log_inventory_changed')
  return result

 def archive(self,full):
  p=self.plan['archivePrecopy'];need(identity(self.purpose)==p['directoryIdentity'],'precopy_purpose_changed');values={}
  for label,name in [('intent','copy.intent.json'),('complete','copy.complete.json'),('proof','archive-proof.json')]:
   raw=read(self.purpose/name,True);need(sha(raw)==p[label+'Sha256'],'precopy_proof_changed');values[label]=json.loads(raw)
  proof=values['proof'];complete=values['complete'];intent=values['intent'];binding=p['binding']
  need(intent.get('binding')==binding==complete.get('binding') and binding.get('cluster')==self.plan['systemId'] and intent.get('eligible') is False and intent.get('sourceDeletionAllowed') is False and complete.get('eligible') is False and all(complete.get(k) is False for k in ['sourceDeleted','sourceMounted','servicesChanged']) and complete.get('proofSha256')==p['proofSha256'],'precopy_binding_changed')
  need(proof.get('eligible') is False and proof.get('targetRoot')==p['targetRoot']==self.claim.get('archiveRoot') and proof.get('sourceRoot')==self.plan['sourceRoots']['archive'] and self.state.get('archiveProofSha256')==self.a['files'][str(PROOF)] and json.loads(self.evidence(PROOF))==proof,'archive_proof_changed')
  scope={'version':1,'planSha256':self.a['files'][str(PLAN)],'directory':str(self.purpose),'proofSha256':p['proofSha256'],'archiveRoot':proof['targetRoot']}
  need(all(self.claim.get(k)==v for k,v in scope.items()) and self.state.get('createdTargetRoots')=={'db':self.claim['dbRoot'],'archive':proof['targetRoot']},'archive_claim_changed')
  need(p.get('deploymentId')==self.host.ticket['deploymentId'] and p.get('activeIdentityDigest')==self.plan['activeIdentityDigest'],'precopy_source_release_changed')
  a=os.path.lexists(self.archive_old);b=os.path.lexists(self.archive_new);need(a!=b,'archive_ambiguous');archive=self.archive_old if a else self.archive_new;need(identity(archive)==proof['targetRoot'],'archive_inode_changed')
  a=os.path.lexists(self.target);b=os.path.lexists(self.shell);need(a!=b,'target_shell_ambiguous');shell=self.target if a else self.shell
  need(identity(shell)==self.claim.get('targetRoot') and identity(shell/'db')==self.claim.get('dbRoot') and not any((shell/'db').iterdir()) and set(p.name for p in shell.iterdir())==({'archive','db'} if os.path.lexists(self.archive_old) else {'db'}),'target_shell_changed')
  need(not os.path.lexists(Path(self.claim['staging'])),'adopt_stage_present')
  for target in [self.target,self.shell]:
   original=self.host.core.TARGET
   try:self.host.core.TARGET=target;self.host.archive_adopt_unmounted(self.purpose)
   finally:self.host.core.TARGET=original
  if full:self.host.verify_archive_adopt_target(archive,proof)
  units=p['writerUnits'];need(isinstance(units,list) and units and len(set(units))==len(units) and all(isinstance(name,str) and re.fullmatch(r'harbor-pg-archive-[a-z0-9-]+\.service',name) for name in units),'archive_writer_inventory_invalid')
  for name in units:self.quiet(name,archive_writer=True)

 def qualify(self,full=False):
  for path in self.destinations:self.evidence(path)
  if self.journal is not None:need(self.journal['binding']==self.binding(),'retirement_binding_changed')
  j=self.state;need(j.get('phase') in ['holding-intent','held','recovering-source','source-restored'] and j.get('failure',{}).get('oldDataRollbackAllowedForExplicitRecovery') is True and j.get('failure',{}).get('automaticOldDataRollback') is False and j.get('oldDataRollbackForbidden',False) is False and j.get('targetBootAuthorized',False) is False and not any(k in j for k in TOUCHED),'source_was_touched')
  need(type(j.get('writers',{})) is dict and not j.get('writers',{}),'writers_were_paused')
  need(not any(os.path.lexists(p) for p in ABSENT),'material_or_cold_state_present')
  h=self.host;h.target_disk();h.load_current_durable_active();need(self.old.sha(canonical(h.upgrade.active_identity(h.active)))==self.plan['activeIdentityDigest'],'active_changed');h.verify_source_recovery_materials()
  for line in Path('/proc/self/mountinfo').read_text().splitlines():
   fields=line.split();need(len(fields)>=10,'mount_inventory_unknown');point=re.sub(r'\\([0-7]{3})',lambda match:chr(int(match[1],8)),fields[4]);need(point not in [str(h.core.SOURCE_DB),str(h.core.SOURCE_ARCHIVE)],'source_bind_present')
  fd=os.open(h.core.SOURCE_DB/'18/docker/global/pg_control',os.O_RDONLY|os.O_NOFOLLOW)
  try:need(str(int.from_bytes(os.read(fd,8),'little'))==self.plan['systemId'],'source_cluster_changed')
  finally:os.close(fd)
  config=h.entry.private_json(Path('/etc/harbor-gateway-deploy.json'));need(config.get('postgres_storage_startup_guard',False) is False,'startup_guard_armed')
  for name in [h.core.STORAGE_START,h.core.STORAGE_ONLINE]:need(h.state_unit(name,allow_absent=True).get('LoadState')=='not-found','startup_unit_present')
  processes={}
  for name in [self.old.PG,self.old.GW,self.old.WORKER]:
   row=h.immutable(name);s=row['State'];expected=self.plan['containers'][name]
   need(s.get('Running') is True and s.get('Pid')==expected['pid'] and s.get('StartedAt')==expected['startedAt'] and type(row.get('RestartCount')) is int and not any(s.get(k) for k in ['Paused','Restarting','Dead','OOMKilled','Error']) and row['HostConfig']['RestartPolicy']==expected['restartPolicy'],'source_generation_changed')
   processes[name]={'id':row['Id'],'signature':self.old.sig(row),'pid':s['Pid'],'startedAt':s['StartedAt'],'restarts':row['RestartCount'],'networkDigest':self.old.container_generation(row)['networkDigest']}
  baseline=j.get('sourceRecoveryBaseline',{});need(baseline.get('version')==1 and baseline.get('kind') in ['healthy','storage-low'] and baseline.get('planSha256')==self.a['files'][str(PLAN)] and baseline.get('activeIdentityDigest')==self.plan['activeIdentityDigest'],'source_baseline_missing')
  for name in [self.old.GW,self.old.WORKER]:need(processes[name]==baseline.get('originalProcesses',{}).get(name),'source_baseline_generation_changed')
  recovery={'sourceRecoveryProcesses','sourceRecoveryNetwork','workerGeneration','applicationsStartEpochMs'}
  if recovery&set(j):
   worker=processes[self.old.WORKER]
   need(recovery<=set(j) and j['sourceRecoveryProcesses']=={name:processes[name] for name in [self.old.GW,self.old.WORKER]} and j['sourceRecoveryNetwork']==h.source_recovery_network(),'source_recovery_witness_changed')
   need(j['workerGeneration']=={'id':worker['id'],'startedAt':worker['startedAt']} and type(j['applicationsStartEpochMs']) is int and j['applicationsStartEpochMs']==int(datetime.fromisoformat(worker['startedAt'].replace('Z','+00:00')).timestamp()*1000),'source_recovery_generation_changed')
  raw=read(Path('/etc/harbor-postgres-runtime-upgrade.json'),True);need(sha(raw)==self.plan['interruptTicketSha256'],'global_ticket_changed');ticket=json.loads(raw);need(not os.path.lexists(h.upgrade.release_path(ticket['deploymentId'])),'pending_target_started');h.upgrade.verify_source(ticket,h.active,h.immutable(self.old.GW),h.immutable(self.old.WORKER))
  control=h.entry.entry_state();need(control.get('phase')=='active' and control.get('slot')=='candidate' and control.get('queued')==0,'entry_not_active')
  state=h.entry.private_json(h.entry.STATE);need(state.get('phase')=='activated' and isinstance(state.get('activeRuntimeConfig'),dict),'entry_resources_unknown');h.entry.verify_resources(state,state['activeRuntimeConfig'])
  runtime=h.runtime.inspect_entry(h.entry);pid=runtime['State'].get('Pid');need(runtime['State'].get('Running') is True and type(pid) is int and pid>1 and h.entry.socket_owner(Path(h.entry.CONTROL))==pid,'entry_owner_changed')
  profile=h.entry.validate(h.entry.private_json(h.entry.PROFILE));need(h.entry.process_cgroup(pid)==profile['entryCgroup'],'entry_cgroup_changed')
  tables=json.loads(h.entry.execute(['/usr/sbin/nft','--json','list','tables']));need(not any(row.get('table',{}).get('name')==h.upgrade.GATE for row in tables.get('nftables',[])),'gate_present')
  worker_states=self.workers();self.archive(full)
  return {'processes':processes,'activeIdentityDigest':self.plan['activeIdentityDigest'],'entry':{'id':runtime['Id'],'pid':pid,'startedAt':runtime['State']['StartedAt'],'ticks':h.entry.process_ticks(pid),'cgroup':h.entry.process_cgroup(pid),'epoch':control['epoch']},'workers':worker_states}

 def save(self,value):
  raw=canonical(value)
  if not os.path.lexists(self.intent):
   # Preparation residues stay outside the empty run directory. They are never
   # overwritten/deleted or mistaken for authority when a fresh process retries.
   temporary=HISTORY/('.retirement-'+self.a['runId']+'-'+uuid.uuid4().hex);fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
   with os.fdopen(fd,'wb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
   self.old.rename_noreplace(temporary,self.intent)
   self.host.core.sync_dir(HISTORY)
  else:
   need(json.loads(read(self.intent,True))==self.journal,'retirement_journal_changed');self.old_save(self.intent,value)
  self.host.core.sync_dir(self.history);self.journal=value

 def move(self,source,target):
  expected=self.plan['archivePrecopy']['targetRoot'] if source==self.archive_old else self.claim['targetRoot'] if source==self.target else self.journal['binding']['evidenceRoots'][str(source)]
  if os.path.lexists(source):
   need(identity(source)==expected,'move_source_changed');need(not os.path.lexists(target) and source.lstat().st_dev==target.parent.stat().st_dev,'move_collision_or_cross_device')
   if stat.S_ISREG(source.lstat().st_mode):
    # Terminal worker logs need not have been fsynced by their old producer.
    # Commit only these bounded original metadata files, never archive payloads.
    fd=os.open(source,os.O_RDONLY|os.O_NOFOLLOW)
    try:
     info=os.fstat(fd);need([info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode]==expected,'move_source_changed');os.fsync(fd)
    finally:os.close(fd)
   self.old.rename_noreplace(source,target)
  else:need(os.path.lexists(target),'move_missing')
  need(identity(target)==expected,'move_target_changed')
  parent=source.parent
  if source==self.archive_old and not os.path.lexists(parent):
   need(identity(self.shell)==self.claim['targetRoot'],'archive_parent_changed');parent=self.shell
  self.host.core.sync_dir(parent);self.host.core.sync_dir(target.parent)

 def durable_before_moves(self):
  # One invariant for both first publication and fresh-process reload: every
  # directory entry and the exact binding intent are durable before any move.
  need(self.journal is not None and self.journal['binding']==self.binding(),'retirement_binding_changed')
  paths=[HISTORY,self.history,ETC_HISTORY,self.etc]
  for path in paths:directory(path);self.host.core.sync_dir(path.parent)
  need(read(self.intent,True)==canonical(self.journal),'retirement_journal_changed');expected=identity(self.intent)
  fd=os.open(self.intent,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
  try:
   info=os.fstat(fd);need(info.st_nlink==1 and [info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode]==expected and os.read(fd,33554433)==canonical(self.journal),'retirement_journal_changed');os.fsync(fd)
  finally:os.close(fd)
  for path in paths:self.host.core.sync_dir(path)

 def run(self,action):
  before=self.qualify(full=True)
  if self.journal is not None:need(before==self.journal['before'],'external_state_changed')
  if action=='inspect':return {'readOnly':True,'eligible':True,'retired':self.journal is not None and self.journal['phase']=='retired-source-untouched','archiveRecopyAllowed':False}
  if self.journal is None:
   for path in [HISTORY,self.history,ETC_HISTORY,self.etc]:
    if not os.path.lexists(path):path.mkdir(mode=0o700)
    directory(path);self.host.core.sync_dir(path.parent)
   need(not any(self.history.iterdir()) and not any(self.etc.iterdir()),'history_not_empty')
   self.save({'version':1,'approvalSha256':self.approval_sha,'runId':self.a['runId'],'binding':self.binding(),'before':before,'phase':'moving'})
  self.durable_before_moves()
  moves=[(self.archive_old,self.archive_new),(self.target,self.shell)]+[(path,self.destinations[path]) for path in sorted(self.destinations,key=lambda p:(p in [STATE,PLAN],str(p)))]
  for source,target in moves:
   need(self.qualify()==before,'external_state_changed');self.move(source,target);need(self.qualify()==before,'external_state_changed')
  need(all(not os.path.lexists(p) for p in self.destinations) and not os.path.lexists(self.target),'canonical_names_not_free');need(self.qualify(full=True)==before,'external_state_changed')
  self.save(dict(self.journal,phase='retired-source-untouched'))
  return {'retired':True,'sourceUnchanged':True,'entryUnchanged':True,'originalEvidencePreserved':True,'archiveSameInode':True,'archiveRecopied':False,'oldRunCompleted':False,'canonicalNamesFree':True}

@contextlib.contextmanager
def locks():
 import fcntl
 descriptors=[]
 try:
  for path in ['/run/harbor-gateway-deploy.lock','/run/harbor-entry/controller-state.lock',str(ROOT/'.backup-copy.lock'),str(ROOT/'.pg-storage-launch.lock'),'/run/harbor-pg-archive-copy.lock']:
   fd=os.open(path,os.O_RDWR|os.O_NOFOLLOW);descriptors.append(fd);s=os.fstat(fd);need(stat.S_ISREG(s.st_mode) and s.st_uid==0 and stat.S_IMODE(s.st_mode)==0o600 and s.st_nlink==1,'lock_untrusted');fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
  yield
 finally:
  for fd in reversed(descriptors):os.close(fd)

def main(action='inspect'):
 need(os.geteuid()==0 and action in ['inspect','retire'] and not os.path.lexists('/nonexistent'),'invocation_invalid');os.environ.clear();os.environ.update(ENV);value,digest=approval()
 with locks():
  return Retirement(value,digest).run(action)

if __name__=='__main__':
 try:
  if sys.argv[1:]==['--help']:
   print('用法：python3 -I -B retire-unstarted-pg-move.py inspect|retire\n'
         '这是未触源失败尝试的退役准备，不是恢复服务或完成迁盘；不 hold、stop/start、不改旧 journal、不重拷归档。\n'
         '默认 inspect 只读。retire 必须有 root/0600 的 /etc/harbor-pg-storage-retirement.json 批准，固定旧 run、原文件 SHA、installer SHA 与 CI 身份。\n'
         '批准字段：version=1、approved=true、id(UUID)、runId、files(原 PLAN/STATE/归档proof/材料批准/worker intent与log的SHA)、installerSha256、revision、pipeline、artifactJob。\n'
         '旧 PG 证据与空目标壳归 /sdc-disk/harbor-data/.pg-storage-retired/<旧run>/；材料批准原件归 /etc/harbor-pg-storage-retired/<旧run>/，均原字节同文件系统移动。\n'
         '归档原 inode 送回旧 plan.archivePrecopy.directory/archive，原三份复制证明不变。必须先核退役 receipt 为 retired-source-untouched 且旧 canonical 全缺失，再更新批准新工具、重新 collect-readonly 和材料 plan、写新 run 批准计划、adopt-archive；中断时先续完退役，不跨步换工具。禁止 precopy 重拷或伪造旧 run complete。')
   raise SystemExit(0)
  need(len(sys.argv) in [1,2],'arguments_invalid');print(json.dumps(main(sys.argv[1] if len(sys.argv)==2 else 'inspect'),sort_keys=True))
 except Exception as error:
  code=str(error) if isinstance(error,RuntimeError) and re.fullmatch('pg_retirement_[a-z_]+',str(error)) else 'pg_retirement_failed_closed'
  print(json.dumps({'failed':True,'error':code,'originalEvidencePreserved':True,'archiveRecopied':False,'sourceMutationRequested':False}));raise SystemExit(1)
