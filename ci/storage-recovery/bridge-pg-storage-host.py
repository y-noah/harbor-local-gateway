"""One approved Host replacement after migration; no PLAN/data rewrite.

Only install/rollback mutate files or units. Approval and candidate must come
from reviewed CI artifacts. This does not download artifacts or approve itself.
"""
import contextlib,ctypes,fcntl,hashlib,json,os,re,stat,subprocess,sys,time,types,uuid
from pathlib import Path

HOST=Path('/usr/local/lib/harbor/harbor-pg-storage-host.py')
APPROVAL=Path('/etc/harbor-pg-storage-host-bridge.json')
RECEIPT=Path('/etc/harbor-pg-storage-host-generation.json')
PLAN=Path('/sdc-disk/harbor-data/.pg-storage-plan.json')
STATE=Path('/sdc-disk/harbor-data/.pg-storage-state.json')
ROOT=Path('/var/lib/harbor-pg-storage-host-bridge')
ONLINE='harbor-pg-storage-online.service'
WRITERS=('harbor-postgres-maintain-base.timer','harbor-postgres-maintain-base.service','harbor-postgres-backup-schedule.service')
ENV={'PATH':'/usr/bin:/bin','LANG':'C','HOME':'/nonexistent','DOCKER_CONFIG':'/nonexistent','DOCKER_HOST':'unix:///run/docker.sock'}
KEYS={'version','approved','id','planSha256','runId','oldHostSha256','newHostSha256','installerSha256','revision','pipeline','artifactJob'}
LOCK_PATHS=('/run/harbor-gateway-deploy.lock','/run/harbor-entry/controller-state.lock','/sdc-disk/harbor-data/.backup-copy.lock')


def need(ok,code):
 if not ok:raise RuntimeError('host_bridge_'+code)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':')).encode()

def read(path,mode=None):
 path=Path(path)
 for parent in path.parents:
  s=parent.lstat();need(stat.S_ISDIR(s.st_mode) and s.st_uid==0 and not s.st_mode&0o022,'untrusted_parent')
 fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
 with os.fdopen(fd,'rb') as stream:
  s=os.fstat(stream.fileno());need(stat.S_ISREG(s.st_mode) and s.st_uid==0 and not s.st_mode&0o022 and s.st_nlink==1 and s.st_size<=1048576 and (mode is None or stat.S_IMODE(s.st_mode)==mode),'untrusted_file');raw=stream.read(1048577);after=path.lstat()
  need(len(raw)==s.st_size and (s.st_dev,s.st_ino,s.st_mtime_ns,s.st_ctime_ns)==(after.st_dev,after.st_ino,after.st_mtime_ns,after.st_ctime_ns),'file_changed')
 return raw

def sync(path):
 fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
 try:os.fsync(fd)
 finally:os.close(fd)

def rename_noreplace(source,target):
 libc=ctypes.CDLL(None,use_errno=True);rename=getattr(libc,'renameat2',None);need(rename is not None,'atomic_publish_unavailable')
 rename.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_int,ctypes.c_char_p,ctypes.c_uint];rename.restype=ctypes.c_int
 if rename(-100,os.fsencode(source),-100,os.fsencode(target),1)!=0:
  code=ctypes.get_errno();raise OSError(code,os.strerror(code))

def publish(path,raw,mode=0o600,expected=None):
 # No replacement of an unknown file, including a symlink. Caller holds locks.
 if os.path.lexists(path):need(expected is not None and sha(read(path))==expected,'unknown_destination')
 else:need(expected is None,'destination_missing')
 temporary=path.with_name('.'+path.name+'.bridge-'+uuid.uuid4().hex)
 fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,mode)
 try:
  with os.fdopen(fd,'wb') as stream:os.fchmod(stream.fileno(),mode);stream.write(raw);stream.flush();os.fsync(stream.fileno())
  if expected is None:rename_noreplace(temporary,path)
  else:
   need(sha(read(path))==expected,'destination_changed');os.replace(temporary,path)
  sync(path.parent)
 finally:
  if temporary.exists():temporary.unlink()

def command(args,timeout=180):
 result=subprocess.run(args,stdin=subprocess.DEVNULL,capture_output=True,env=ENV,timeout=timeout);need(result.returncode==0,'unit_command_failed');return result.stdout

def unit(name):
 raw=command(['/usr/bin/systemctl','show',name,'--property=LoadState,ActiveState,SubState,MainPID,ControlGroup,InvocationID,Result,ExecMainStatus,FragmentPath,ExecStart'])
 value=dict(line.split('=',1) for line in raw.decode().splitlines() if '=' in line);need(value.get('LoadState')=='loaded','unit_unknown');return value

def quiet(name):
 value=unit(name);need(value.get('MainPID')=='0' and value.get('ActiveState') in ['inactive','failed'],'unit_not_stopped')
 group=value.get('ControlGroup','')
 if group:
  need(group.startswith('/system.slice/') and group==os.path.normpath(group),'unit_cgroup_unknown');events=Path('/sys/fs/cgroup')/group.lstrip('/')/'cgroup.events'
  if events.exists():need(dict(line.split() for line in events.read_text().splitlines()).get('populated')=='0','unit_descendants_active')
 return value

def process_generation(pid):
 raw=Path('/proc')/str(pid)/'stat';text=raw.read_text();return text[text.rfind(')')+2:].split()[19]

def supervisor():
 value=unit(ONLINE);pid=value.get('MainPID','0')
 need(value.get('ActiveState')=='active' and value.get('SubState')=='running' and pid.isdigit() and int(pid)>0 and str(HOST) in value.get('ExecStart',''),'supervisor_unknown')
 command_line=(Path('/proc')/pid/'cmdline').read_bytes().split(b'\0')
 need(command_line[1:]==[b'-I',b'-B',os.fsencode(HOST),b'boot-online',b''] and command_line[0] in [b'/usr/bin/python3',b'python3'],'supervisor_command_changed')
 need((Path('/proc')/pid/'cgroup').read_text().strip()=='0::/system.slice/'+ONLINE and value.get('ControlGroup')=='/system.slice/'+ONLINE,'supervisor_cgroup_changed')
 value['processStartTicks']=process_generation(pid);return value

def read_approval():
 raw=read(APPROVAL,0o600);value=json.loads(raw)
 need(set(value)==KEYS and type(value['version']) is int and value['version']==1 and value['approved'] is True and str(uuid.UUID(value['id']))==value['id'],'approval_invalid')
 need(re.fullmatch('[a-f0-9]{32}',value['runId']) and re.fullmatch('[a-f0-9]{40}',value['revision']) and all(isinstance(value[k],str) and re.fullmatch('[a-f0-9]{64}',value[k]) for k in ['planSha256','oldHostSha256','newHostSha256','installerSha256']) and all(type(value[k]) is int and value[k]>0 for k in ['pipeline','artifactJob']),'approval_identity_invalid')
 need(value['oldHostSha256']!=value['newHostSha256'] and sha(read(Path(__file__)))==value['installerSha256'],'installer_unapproved')
 return value,sha(raw)

@contextlib.contextmanager
def locks(wait=False):
 descriptors=[]
 try:
  deadline=time.monotonic()+120
  while True:
   try:
    for path in LOCK_PATHS:
     fd=os.open(path,os.O_RDWR|os.O_NOFOLLOW);descriptors.append(fd);s=os.fstat(fd);need(stat.S_ISREG(s.st_mode) and s.st_uid==0 and stat.S_IMODE(s.st_mode)==0o600 and s.st_nlink==1,'lock_untrusted');fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
    break
   except BlockingIOError:
    for fd in reversed(descriptors):os.close(fd)
    descriptors=[]
    if not wait:raise
    need(time.monotonic()<deadline,'lock_wait_expired');time.sleep(0.1)
  yield
 finally:
  for fd in reversed(descriptors):os.close(fd)

def original_host(raw):
 # Preserve canonical __file__; the old module checks its own planned path.
 module=types.ModuleType('harbor_original_storage_host');module.__file__=str(HOST);exec(compile(raw,str(HOST),'exec'),module.__dict__);return module

def bound_move(approval):
 raw=read(PLAN,0o600);need(sha(raw)==approval['planSha256'],'plan_changed');plan=json.loads(raw);state=json.loads(read(STATE,0o600))
 need(plan.get('approved') is True and plan.get('runId')==approval['runId'] and state.get('runId')==approval['runId'] and plan.get('scope')==state.get('scope')=='harbor-pg-storage-move-v1' and state.get('oldDataRollbackForbidden') is True and state.get('targetBootAuthorized') is True,'move_binding_changed')
 need(plan['toolSha256'].get(str(HOST))==approval['oldHostSha256'],'original_host_binding_changed')
 selected=plan.get('materialMove')
 need(selected is None or state.get('materialMove')==dict(selected,phase='complete'),'material_not_complete')
 return plan,state

def completed(approval,historical=False):
 plan,state=bound_move(approval)
 # historical=True is only used after the caller has authenticated our durable
 # completed journal, backup and (when present) exact generation receipt.
 phases=['complete','boot-start-intent','boot-containers-ready','runtime-recovering','runtime-entry-pending'] if historical else ['complete']
 need(state.get('phase') in phases,'move_not_complete')
 modes=['running','recovering','paused'] if historical else ['running']
 need(not state.get('plannedOperation') and state.get('runtimeControl',{}).get('mode','running') in modes,'runtime_operation_pending')
 return plan,state

def source_identity(old,host):
 host.load_current_durable_active();host.data_mounts();host.immutable(old.PG);host.require_docker_restart_disabled();host.verify_installed_unit_files();old.workers_quiet(host)
 ready=host.j['phase']=='complete' and host.j.get('runtimeControl',{}).get('mode','running')=='running'
 if ready:host.verify_application_material_mounts()
 else:
  selected=host.material_mover()
  if selected is not None:
   mover,value,_=selected;binding=host.plan['materialMove'];state=mover.g.private_json(mover.STATE,33554432)
   need(state.get('phase')=='complete' and state.get('nonce')==binding['nonce'] and state.get('approvalSha256')==binding['approvalSha256'],'material_completion_changed')
   need(mover.g.private_json(mover.g.CONFIG)==mover.final_configuration(value,state),'material_configuration_changed');mover.g.guard(list(mover.g.TARGETS),required=True)
 return host.upgrade.digest(host.upgrade.active_identity(host.active))

def bridge_roots(plan):
 def identity(path):
  info=Path(path).lstat();need(stat.S_ISDIR(info.st_mode),'root_invalid');return [info.st_dev,info.st_ino,info.st_uid,info.st_gid,stat.S_IMODE(info.st_mode)]
 base=Path('/opt/product-development/harbor-local-gateway/postgres-infra')
 value={'systemId':plan['systemId'],'pgContainer':plan['containers']['harbor-local-gateway-postgres-infra'],'db':identity(base/'db'),'archive':identity(base/'archive')}
 if plan.get('materialMove') is not None:
  raw=read(Path('/etc/harbor-data-paths.json'),0o600);config=json.loads(raw)
  value['materialConfigSha256']=sha(raw);value['materialRoots']={name:{side:identity(row[side]) for side in ['logical','target']} for name,row in config['roots'].items()}
 return value

def receipt_for(value,approval_sha,roots):return {'contract':'harbor-pg-storage-host-generation-v1','approvalSha256':approval_sha,'binding':{k:value[k] for k in KEYS-{'approved'}},'migrationCompleted':True,'roots':roots}

def current_host_is_new(approval):return sha(read(HOST))==approval['newHostSha256']

def verify_receipt(expected):need(json.loads(read(RECEIPT,0o600))==expected,'receipt_changed')

def save_journal(path,journal):publish(path,canonical(journal),expected=sha(read(path,0o600)) if os.path.lexists(path) else None)

def restore_writers(journal):
 # Requires=online can stop dependent writers. Never restart an initially idle
 # backup job, and never invent units outside the fixed dependency inventory.
 for name,snapshot in journal['writers'].items():
  current=unit(name)
  if snapshot['ActiveState'] in ['active','activating','reloading'] and current['ActiveState'] in ['inactive','failed']:
   command(['/usr/bin/systemctl','start','--no-block',name])
 for name,snapshot in journal['writers'].items():
  if snapshot['ActiveState'] not in ['active','activating','reloading']:continue
  deadline=time.monotonic()+120
  while True:
   current=unit(name);need(current.get('ActiveState')!='failed','writer_restore_failed')
   if name.endswith('.timer'):ready=current.get('ActiveState')=='active'
   elif name.endswith('maintain-base.service'):
    fresh=current.get('InvocationID') not in [None,'',snapshot.get('InvocationID')]
    ready=fresh and (current.get('ActiveState')=='inactive' and current.get('Result')=='success' and current.get('ExecMainStatus')=='0' or current.get('ActiveState') in ['active','activating'] and int(current.get('MainPID','0'))>0)
   else:ready=current.get('ActiveState')=='active' and int(current.get('MainPID','0'))>0
   if ready:break
   need(time.monotonic()<deadline,'writer_not_restored');time.sleep(0.1)

def bridge(action='inspect'):
 need(os.geteuid()==0 and action in ['inspect','install','refresh','rollback'],'invocation_invalid');need(not os.path.lexists('/nonexistent'),'docker_configuration_present');os.environ.clear();os.environ.update(ENV)
 approval,approval_sha=read_approval();directory=ROOT/approval['id'];candidate=directory/'candidate.py';journal_path=directory/'journal.json';backup=directory/'original-host.py'
 need(sha(read(candidate))==approval['newHostSha256'],'candidate_unapproved');compile(read(candidate),str(HOST),'exec')
 with locks():
  plan,_=bound_move(approval);roots=bridge_roots(plan);expected_receipt=receipt_for(approval,approval_sha,roots);resume_only=False;historical=False
  if os.path.lexists(journal_path):
   journal=json.loads(read(journal_path,0o600));need(journal.get('migrationCompleted') is True and journal.get('approvalSha256')==approval_sha and journal.get('planSha256')==approval['planSha256'] and journal.get('roots')==roots and journal.get('operation') in ['install','refresh','rollback'] and journal.get('phase') in ['prepared','stopping','stopped','receipt-published','host-published','starting','protected','complete','rollback','rolled-back'],'journal_unowned')
   if os.path.lexists(backup):old_raw=read(backup,0o600)
   else:
    need(journal['phase']=='prepared' and sha(read(HOST))==approval['oldHostSha256'],'backup_missing');old_raw=read(HOST)
    if action!='inspect':publish(backup,old_raw)
   need(sha(old_raw)==approval['oldHostSha256'],'backup_changed')
   if os.path.lexists(RECEIPT):verify_receipt(expected_receipt)
   historical=action in ['inspect','rollback'] or journal['phase']=='protected' and journal['operation']==action or action=='refresh' and journal['operation']=='refresh' and journal['phase'] in ['prepared','stopping','starting']
   completed(approval,historical=historical)
  else:
   completed(approval)
   need(action in ['inspect','install'] and sha(read(HOST))==approval['oldHostSha256'] and not os.path.lexists(RECEIPT) and not os.path.lexists(backup),'unknown_initial_state')
   old_raw=read(HOST);old=original_host(old_raw);host=old.Host();identity=source_identity(old,host);online=supervisor()
   writers={name:unit(name) for name in WRITERS};journal={'version':1,'approvalSha256':approval_sha,'planSha256':approval['planSha256'],'migrationCompleted':True,'operation':'install','phase':'prepared','activeDigest':identity,'online':online,'writers':writers,'roots':roots}
   if action=='inspect':return {'readOnly':True,'ready':True,'planUnchanged':True,'dataRollbackAllowed':False}
   save_journal(journal_path,journal);publish(backup,old_raw)
  if action=='inspect':return {'readOnly':True,'phase':journal['phase'],'planUnchanged':True}
  if action=='refresh':
   retry=journal.get('operation')=='refresh' and journal['phase'] in ['prepared','stopping','starting','protected']
   need((journal['phase']=='complete' or retry) and current_host_is_new(approval),'refresh_not_ready');verify_receipt(expected_receipt)
   current_module=original_host(read(HOST));current_host=current_module.Host();identity=source_identity(current_module,current_host)
   if not retry:
    journal.update(operation='refresh',phase='prepared',activeDigest=identity,online=supervisor(),writers={name:unit(name) for name in WRITERS});save_journal(journal_path,journal)
   else:need(identity==journal['activeDigest'],'active_changed')
  if journal['phase']=='complete' and action=='install':
   need(sha(read(HOST))==approval['newHostSha256'],'installed_host_changed');verify_receipt(expected_receipt);need(unit(ONLINE)['ActiveState']=='active','supervisor_not_active');return {'installed':True,'alreadyComplete':True,'planUnchanged':True}
  if journal['phase']=='rolled-back' and action=='rollback':need(sha(read(HOST))==approval['oldHostSha256'] and not os.path.lexists(RECEIPT),'rollback_changed');return {'rolledBack':True,'planUnchanged':True}
  if journal['phase']=='protected' and journal['operation']==action and unit(ONLINE)['ActiveState']=='active':
   observed=supervisor();need(all(observed[k]==journal.get('restoredOnline',{}).get(k) for k in ['MainPID','InvocationID','ControlGroup','processStartTicks']),'protected_supervisor_changed');resume_only=True
  # Re-entered partial publication is rolled back explicitly; no guessed resume
  # across an unknown systemd invocation or active worker generation.
  if action=='install':need(journal['operation']=='install' and (journal['phase']=='prepared' or resume_only),'rollback_required')
  current=sha(read(HOST));need(current in [approval['oldHostSha256'],approval['newHostSha256']],'unknown_current_host')
  if action=='rollback':
   plan,_=completed(approval,historical=historical)
   for name,digest in plan['toolSha256'].items():
    if name!=str(HOST):need(sha(read(Path(name)))==digest,'original_tools_changed')
  if os.path.lexists(RECEIPT):verify_receipt(expected_receipt)
  if not resume_only:
   if action in ['install','refresh']:
    old=original_host(old_raw if action=='install' else read(HOST));host=old.Host();need(source_identity(old,host)==journal['activeDigest'],'active_changed')
    if action=='refresh' and journal['phase'] in ['stopping','starting','protected'] and unit(ONLINE)['ActiveState'] in ['inactive','failed']:quiet(ONLINE)
    else:
     current_online=supervisor()
     if action=='refresh' and journal['phase']=='starting':
      host.verify_supervisor_tools(current_online);journal['online']=current_online
     else:need(all(current_online[k]==journal['online'][k] for k in ['MainPID','InvocationID','ControlGroup','processStartTicks']),'supervisor_generation_changed')
   journal['operation']=action
   journal['phase']='rollback' if action=='rollback' else 'stopping';save_journal(journal_path,journal)
   command(['/usr/bin/systemctl','stop',ONLINE]);quiet(ONLINE)
   original=journal['online'];pid=original['MainPID']
   need(not (Path('/proc')/pid).exists() or process_generation(pid)!=original['processStartTicks'],'old_supervisor_alive')
   for name in WRITERS:quiet(name)
   plan,_=completed(approval,historical=historical);need(bridge_roots(plan)==roots,'roots_changed')
   try:
    if action=='install':
     old=original_host(old_raw);host=old.Host();need(source_identity(old,host)==journal['activeDigest'],'active_changed');journal['phase']='stopped';save_journal(journal_path,journal)
     publish(RECEIPT,canonical(expected_receipt));journal['phase']='receipt-published';save_journal(journal_path,journal)
     publish(HOST,read(candidate),0o644,approval['oldHostSha256']);journal['phase']='host-published';save_journal(journal_path,journal)
    elif action=='rollback':
     if sha(read(HOST))==approval['newHostSha256']:publish(HOST,old_raw,0o644,approval['newHostSha256'])
     if os.path.lexists(RECEIPT):verify_receipt(expected_receipt);RECEIPT.unlink();sync(RECEIPT.parent)
    plan,_=completed(approval,historical=historical);need(bridge_roots(plan)==roots,'roots_changed')
    journal['phase']='starting';save_journal(journal_path,journal)
   except BaseException:
    # Preserve journal/backups; explicit rollback is valid even after ACK loss.
    raise
 # The supervisor acquires these locks during start; never wait holding them.
 if not resume_only:
  command(['/usr/bin/systemctl','start',ONLINE]);online=supervisor();need((online['MainPID'],online['processStartTicks'],online['InvocationID'])!=(original['MainPID'],original['processStartTicks'],original['InvocationID']),'supervisor_not_replaced')
 else:online=observed
 with locks(wait=True):
  latest=supervisor();need(all(latest[k]==online[k] for k in ['MainPID','InvocationID','ControlGroup','processStartTicks']),'ready_supervisor_changed')
  plan,state=completed(approval,historical=True);need(bridge_roots(plan)==roots,'roots_changed');need(sha(read(HOST))==approval['oldHostSha256' if action=='rollback' else 'newHostSha256'],'final_host_changed')
  if action!='rollback':verify_receipt(expected_receipt)
  else:need(not os.path.lexists(RECEIPT),'rollback_receipt_present')
  module=original_host(read(HOST));host=module.Host();need(source_identity(module,host)==journal['activeDigest'],'final_active_changed')
  ready=state['phase']=='complete' and state.get('runtimeControl',{}).get('mode','running')=='running'
  if ready:
   if action!='rollback':module.verify_factory_guard(host.config)
   # Keep the instance lock while starting dependent writers, just as the
   # original Host does; recovery cannot begin between qualification and start.
   restore_writers(journal);journal['phase']='rolled-back' if action=='rollback' else 'complete'
  else:
   journal.update(phase='protected',restoredOnline=online)
  save_journal(journal_path,journal)
 return {'installed':action=='install','refreshed':action=='refresh','rolledBack':action=='rollback','planUnchanged':True,'dataRollbackAllowed':False,'writersRestored':ready,'protectionRetained':not ready,'recoveryPending':not ready}

def main(action='inspect'):
 need(os.geteuid()==0 and action in ['inspect','install','refresh','rollback'],'invocation_invalid')
 if action=='inspect':return bridge(action)
 # Serializes the lock-free systemd start/notify interval as well as metadata.
 path=Path('/run/harbor-pg-storage-host-bridge.lock');fd=os.open(path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
 try:
  info=os.fstat(fd);need(stat.S_ISREG(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600 and info.st_nlink==1,'lock_untrusted');fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
  return bridge(action)
 finally:os.close(fd)

if __name__=='__main__':
 try:print(json.dumps(main(sys.argv[1] if len(sys.argv)==2 else 'inspect'),sort_keys=True))
 except Exception as error:
  code=str(error) if isinstance(error,RuntimeError) and re.fullmatch('host_bridge_[a-z_]+',str(error)) else 'host_bridge_failed_closed'
  print(json.dumps({'failed':True,'error':code,'unknownStatePreserved':True,'dataRollbackAllowed':False}));raise SystemExit(1)
