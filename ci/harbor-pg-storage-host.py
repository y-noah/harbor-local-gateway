"""Single Harbor instance adapter, prepared offline. No import-time operations.
No credentials, arbitrary paths/images, activation creation or global Docker edits.
"""
import contextlib,copy,fcntl,hashlib,http.client,importlib.machinery,importlib.util,json,os,re,socket,signal,stat,subprocess,sys,time,uuid
from urllib.parse import urlsplit,parse_qs
from datetime import datetime
from pathlib import Path
CORE=Path(__file__).with_name('harbor-pg-storage-coordinator.py')
ROOT=Path('/sdc-disk/harbor-data');PLAN=ROOT/'.pg-storage-plan.json';STATE=ROOT/'.pg-storage-state.json';PROOF=ROOT/'.pg-storage-archive-proof.json'
PG='harbor-local-gateway-postgres-infra';GW='harbor-local-gateway';WORKER=GW+'-worker'
DEPLOY=Path('/usr/local/bin/harbor-gateway-deploy');ENTRY=Path('/usr/local/lib/harbor/gateway-entry-controller.py');UPGRADE=Path('/usr/local/lib/harbor/postgres-upgrade-deploy.py');COMPOSE=Path('/usr/local/lib/harbor/postgres-deploy.py')
RUNTIME=Path('/usr/local/lib/harbor/entry-runtime.py')
_EMIT_PROGRESS=False
BACKUP=['/opt/harbor-storage-tools/node/bin/node','/opt/harbor-storage-tools/app/storage-backup.mjs']

def need(value,code):
 if not value:raise RuntimeError(code)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def private(path,limit=33554432):
 for p in path.parents:
  s=p.lstat();need(stat.S_ISDIR(s.st_mode) and s.st_uid==0 and not s.st_mode&0o022,'host_parent_untrusted')
 fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
 with os.fdopen(fd,'rb') as stream:
  s=os.fstat(stream.fileno());need(stat.S_ISREG(s.st_mode) and s.st_uid==0 and stat.S_IMODE(s.st_mode)==0o600 and s.st_nlink==1 and s.st_size<=limit,'host_private_file_untrusted');raw=stream.read(limit+1)
 need(len(raw)<=limit,'host_file_budget');return raw

def save(path,value):
 raw=json.dumps(value,sort_keys=True,separators=(',',':')).encode();need(len(raw)<=33554432,'host_state_budget');temporary=path.with_name('.'+path.name+'.writing-'+uuid.uuid4().hex)
 fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
 with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
 os.replace(temporary,path);fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
 try:os.fsync(fd)
 finally:os.close(fd)

def load(path,name):
 for p in [path,*path.parents]:
  s=p.lstat();need(s.st_uid==0 and not s.st_mode&0o022 and not stat.S_ISLNK(s.st_mode),'host_tool_untrusted')
 loader=importlib.machinery.SourceFileLoader(name,str(path));spec=importlib.util.spec_from_loader(name,loader);module=importlib.util.module_from_spec(spec);loader.exec_module(module);return module

def run(args,timeout=60):
 p=subprocess.run(args,stdin=subprocess.DEVNULL,capture_output=True,timeout=timeout)
 # Raw child stderr is never serialized. A fixed stage code is the public boundary.
 need(p.returncode==0,'host_command_failed');return p.stdout

def sig(row):
 return sha(json.dumps({'Id':row.get('Id'),'Image':row.get('Image'),'Config':row.get('Config'),'HostConfig':{k:v for k,v in row.get('HostConfig',{}).items() if k!='RestartPolicy'},'Mounts':[{k:m.get(k) for k in ['Type','Source','Destination','Mode','RW','Propagation']} for m in row.get('Mounts',[])]},sort_keys=True,separators=(',',':')).encode())

def wait(check,seconds,code):
 end=time.monotonic()+seconds
 while True:
  value=check()
  if value:return value
  need(time.monotonic()<end,code);time.sleep(.2)

def container_generation(row):
 network=row.get('NetworkSettings',{});endpoints={name:{key:value.get(key) for key in ['NetworkID','EndpointID']} for name,value in network.get('Networks',{}).items()}
 return {'id':row['Id'],'startedAt':row['State']['StartedAt'],'networkDigest':sha(json.dumps({'sandbox':network.get('SandboxID'),'endpoints':endpoints},sort_keys=True).encode())}

def supervision_interval(config):
 # Root host configuration; five seconds bounds routine observation overhead,
 # not an outage-recovery SLA. Zero/false cannot disable required supervision.
 value=config.get('postgres_storage_supervision_poll_seconds',5)
 need(type(value) is int and 1<=value<=60,'host_supervision_interval_invalid');return value

def no_entry_listeners(port,control):
 for name in ['tcp','tcp6']:
  for line in (Path('/proc/net')/name).read_text().splitlines()[1:]:
   fields=line.split();need(len(fields)>=4,'host_entry_socket_inventory_unknown');need(not (fields[3]=='0A' and int(fields[1].rsplit(':',1)[1],16)==port),'host_entry_listener_collision')
 for line in Path('/proc/net/unix').read_text().splitlines()[1:]:
  fields=line.split(maxsplit=7);need(len(fields)>=7,'host_entry_socket_inventory_unknown');need(len(fields)<8 or fields[7]!=str(control),'host_entry_control_collision')

class Host:
 def __init__(self):
  need(os.geteuid()==0,'host_root_required');raw=private(PLAN,262144);self.planSha256=sha(raw);self.plan=json.loads(raw);p=self.plan
  need(p.get('scope')=='harbor-pg-storage-move-v1' and p.get('approved') is True and re.fullmatch('[a-f0-9]{32}',p.get('runId','')),'host_plan_unapproved')
  need(p.get('tlsVerifyFull') is True and re.fullmatch('[0-9]{1,20}',p.get('systemId','')),'host_database_identity_missing')
  need(set(p.get('containers',{}))=={PG,GW,WORKER} and all(re.fullmatch('[a-f0-9]{64}',v.get('id','')) and re.fullmatch('[a-f0-9]{64}',v.get('signature','')) for v in p['containers'].values()),'host_inventory_missing')
  self.core=load(CORE,'harbor_pg_storage_core')
  expected={str(v) for v in [CORE,Path(__file__),DEPLOY,ENTRY,UPGRADE,COMPOSE,RUNTIME]};need(set(p.get('toolSha256',{}))==expected,'host_tool_inventory_missing')
  for name,digest in p['toolSha256'].items():need(re.fullmatch('[a-f0-9]{64}',digest) and sha(Path(name).read_bytes())==digest,'host_tool_changed')
  self.wrapper=load(DEPLOY,'harbor_storage_wrapper');self.entry=load(ENTRY,'harbor_storage_entry');self.upgrade=load(UPGRADE,'harbor_storage_upgrade');self.pg=load(COMPOSE,'harbor_storage_compose');self.runtime=load(RUNTIME,'harbor_storage_entry_runtime')
  need(getattr(self.pg,'STORAGE_STARTUP_CONTRACT',None)=='harbor-pg-storage-boot-v1','host_factory_guard_not_installed')
  self.config=self.entry.private_json(Path('/etc/harbor-gateway-deploy.json'));self.poll_interval_seconds=supervision_interval(self.config);self.j=json.loads(private(STATE)) if STATE.exists() else {'scope':p['scope'],'runId':p['runId'],'phase':'planned'}
  need(self.j.get('scope')==p['scope'] and self.j.get('runId')==p['runId'],'host_state_scope_changed');self.api={**vars(self.wrapper),'STACK':p.get('materialsDirectory')};self.gw=None;self.worker=None;self.active=None;self.ticket=None
 def reload_locked_state(self):
  need(sha(private(PLAN,262144))==self.planSha256,'host_plan_changed')
  current=json.loads(private(STATE)) if STATE.exists() else {'scope':self.plan['scope'],'runId':self.plan['runId'],'phase':'planned'}
  need(current.get('scope')==self.plan['scope'] and current.get('runId')==self.plan['runId'],'host_state_scope_changed');self.j=current
 def save(self,journal):
  save(STATE,journal)
  if _EMIT_PROGRESS:print(json.dumps({'stage':journal.get('phase')}),flush=True)
 def row(self,name):
  rows=json.loads(run(['/usr/bin/docker','container','inspect',name]));need(len(rows)==1,'host_container_ambiguous');return rows[0]
 def immutable(self,name,expected=None):
  row=self.row(name);frozen=(expected or self.plan['containers'][name]);need(row.get('Id')==frozen['id'] and sig(row)==frozen['signature'],'host_container_changed');return row
 def state_unit(self,name,allow_absent=False):
  result=subprocess.run(['/usr/bin/systemctl','show',name,'--property=LoadState,ActiveState,SubState,MainPID,UnitFileState,ControlGroup,Result,ExecMainStatus,InvocationID'],stdin=subprocess.DEVNULL,capture_output=True,timeout=30);state=dict(v.split('=',1) for v in result.stdout.decode().splitlines() if '=' in v)
  need(result.returncode==0 or allow_absent and result.returncode==1 and state.get('LoadState')=='not-found','host_command_failed');return state
 def target_disk(self):
  uuid_expected=self.plan.get('targetFilesystemUuid');need(isinstance(uuid_expected,str) and re.fullmatch('[a-fA-F0-9-]{8,64}',uuid_expected),'host_target_uuid_missing')
  mounted=json.loads(run(['/usr/bin/findmnt','--json','--target','/sdc-disk','--output','TARGET,UUID']))['filesystems'];need(len(mounted)==1 and mounted[0].get('target')=='/sdc-disk' and mounted[0].get('uuid')==uuid_expected,'host_target_mount_changed')
  need(ROOT.stat().st_dev==Path('/sdc-disk').stat().st_dev!=Path('/').stat().st_dev,'host_target_wrong_filesystem')
 def data_mounts(self):
  self.target_disk()
  for source,target in [(self.core.SOURCE_DB,self.core.TARGET/'db'),(self.core.SOURCE_ARCHIVE,self.core.TARGET/'archive')]:
   a,b=source.lstat(),target.lstat();need(stat.S_ISDIR(a.st_mode) and stat.S_ISDIR(b.st_mode) and (a.st_dev,a.st_ino)==(b.st_dev,b.st_ino) and a.st_uid==999 and b.st_dev==ROOT.stat().st_dev,'host_bound_path_mismatch')
   mounted=json.loads(run(['/usr/bin/findmnt','--json','--mountpoint',str(source),'--output','TARGET']))['filesystems'];need(len(mounted)==1 and mounted[0].get('target')==str(source),'host_original_path_not_mount')
  need((self.core.SOURCE_DB/'18/docker/PG_VERSION').read_text().strip()=='18','host_pgdata_version_changed')
  for label in ['db','archive']:
   raw=private(PROOF if label=='archive' else ROOT/'.pg-storage-db-proof.json');need(sha(raw)==self.j.get('archiveProofSha256' if label=='archive' else 'dbProofSha256'),'host_cold_proof_changed');proof=json.loads(raw);actual=self.core.identity((self.core.TARGET/label).stat())[:5];need(proof.get('eligible') is True and actual[1:]==proof.get('targetRoot',[])[1:],'host_target_root_identity_changed')
 def current_identity(self):
  self.target_disk()
  for label,path in [('db',self.core.SOURCE_DB),('archive',self.core.SOURCE_ARCHIVE)]:
   need(self.core.identity(path.lstat())[:5]==self.plan.get('sourceRoots',{}).get(label),'host_source_root_changed')
  self.active,_,self.gw=self.entry.active_record();self.worker=self.row(WORKER);self.immutable(PG);self.immutable(GW);self.immutable(WORKER)
  need(self.upgrade.digest(self.upgrade.active_identity(self.active))==self.plan.get('activeIdentityDigest'),'host_active_changed')
  for name in [PG,GW,WORKER]:need(self.row(name)['HostConfig']['RestartPolicy']==self.plan['containers'][name].get('restartPolicy'),'host_source_restart_policy_changed')
  record=self.upgrade.record_for(self.active['approval']);need(record.get('phase')=='complete','host_current_release_not_complete');self.currentReleasePhase=record['phase'];self.ticket=record['ticket'];need(self.ticket['database']['cluster']==self.plan['systemId'],'host_system_id_changed');self.api['STACK']=self.ticket['materialsDirectory'];self.j.setdefault('writers',{});self.save(self.j)
 def hold_drain(self):self.upgrade.drain_active_entry(self.entry)
 def install_gate(self):
  boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip();binding=self.upgrade.digest(self.upgrade.active_identity(self.active));intent=self.j.get('gateIntent',{})
  tables=json.loads(self.entry.execute(['/usr/sbin/nft','--json','list','tables']));exists=any(row.get('table',{}).get('name')==self.upgrade.GATE for row in tables.get('nftables',[]))
  if exists:
   need(intent=={'state':'installed','bootId':boot_id,'runId':self.plan['runId'],'activeIdentityDigest':binding} and self.j.get('gate') is not None,'host_gate_not_owned');need(self.upgrade.gate_snapshot(self.entry)==self.j['gate'],'host_gate_changed');return self.j['gate']
  self.j.pop('gateOrigin',None);self.j['gateIntent']={'state':'installing','bootId':boot_id,'runId':self.plan['runId'],'activeIdentityDigest':binding};self.save(self.j)
  gate=self.upgrade.gate_install(self.entry,self.ticket,self.config);self.j['gate']=gate;self.j['gateIntent']['state']='installed';self.save(self.j);return gate
 def pause_writers(self):
  if not self.j.get('writersPauseStarted',False):
   self.j['writers']={v:self.state_unit(v) for v in self.core.WRITERS};self.j['writersPauseStarted']=True;self.save(self.j)
  original=self.j['writers']
  for name in self.core.WRITERS:run(['/usr/bin/systemctl','stop',name],timeout=600)
  for name in self.core.WRITERS:
   row=self.state_unit(name);need(row.get('ActiveState') in ['inactive','failed'] and row.get('MainPID')=='0','host_writer_not_stopped')
   group=row.get('ControlGroup','')
   if group:
    need(group.startswith('/') and group==os.path.normpath(group) and group!='/','host_writer_cgroup_invalid');path=Path('/sys/fs/cgroup')/group.lstrip('/')
    if path.exists():need(dict(v.split() for v in (path/'cgroup.events').read_text().splitlines()).get('populated')=='0','host_writer_descendants_active')
  return original
 def stop_applications(self):
  common,_=self.pg.configuration(self.config,self.api['STACK']);self.api['source_image_preflight']=self.pg.prepare_source_image_preflights(self.api,[(self.gw,'gateway'),(self.worker,'worker')],common)
  for name,row,role in [(GW,self.gw,'gateway'),(WORKER,self.worker,'worker')]:self.pg.stop_confirmed(self.api,name,row,role,[])
 def stop_postgres_clean(self):
  row=self.immutable(PG);need(row['State']['Running'] is True,'host_pg_not_running')
  # A finite docker stop can escalate to SIGKILL. Request the image's graceful
  # stop signal and poll; an unknown result never reaches a copy operation.
  signal=row['Config'].get('StopSignal','SIGINT');need(signal in ['SIGINT','SIGTERM'],'host_pg_stop_signal_unknown');run(['/usr/bin/docker','kill','--signal='+signal,row['Id']])
  def stopped():
   row=self.immutable(PG);s=row['State']
   if s.get('Running'):return False
   need(s.get('Pid')==0 and not any(s.get(k) for k in ['Paused','Restarting','Dead','OOMKilled','Error']),'host_pg_stop_unknown');return True
  wait(stopped,120,'host_pg_stop_unknown');self.require_clean_control(self.core.SOURCE_DB)
  lock=self.core.SOURCE_ARCHIVE/'.backup-coordination.lock';self.archive_lease=os.open(lock,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK);s=os.fstat(self.archive_lease);need(stat.S_ISREG(s.st_mode) and s.st_uid==999 and stat.S_IMODE(s.st_mode)==0o600 and s.st_nlink==1,'host_archive_lock_untrusted');fcntl.flock(self.archive_lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
  for path in [self.core.SOURCE_DB,self.core.SOURCE_ARCHIVE]:
   p=subprocess.run(['/usr/bin/lsof','-t','+D',str(path)],stdin=subprocess.DEVNULL,capture_output=True,timeout=90);need(p.returncode in [0,1] and not p.stderr and all(v==str(os.getpid()).encode() for v in p.stdout.splitlines()),'host_source_open_or_unknown')
 def require_clean_control(self,db):
  # Use the original pinned PG image solely as a read-only pg_controldata tool;
  # no network, no server, no cluster initialization or restore.
  image=self.row(PG)['Image'];raw=run(['/usr/bin/docker','run','--rm','--network','none','--read-only','--user','999:999','--cap-drop','ALL','--security-opt','no-new-privileges','--entrypoint','/usr/lib/postgresql/18/bin/pg_controldata','--mount','type=bind,src='+str(db)+',dst=/data,readonly',image,'/data/18/docker']).decode()
  fields=dict(v.split(':',1) for v in raw.splitlines() if ':' in v);need(fields.get('Database system identifier','').strip()==self.plan['systemId'] and fields.get('Database cluster state','').strip()=='shut down','host_pg_not_clean')
 def finalize_archive(self):
  self.require_docker_restart_disabled();need(self.immutable(PG)['State']['Running'] is False,'host_source_not_stopped')
  proof=json.loads(private(PROOF));result=self.core.ArchiveMirror(self.core.SOURCE_ARCHIVE,self.core.TARGET/'archive',999).finalize(proof,source_stopped=True);save(PROOF,result);self.j['archiveProofSha256']=sha(private(PROOF));self.save(self.j)
 def verify_created_target_root(self,label):
  self.target_disk();path=self.core.TARGET/label;actual=self.core.identity(path.lstat())[:5];expected=self.j.get('createdTargetRoots',{}).get(label,[])
  need(actual[1:]==expected[1:] and actual[0]==ROOT.stat().st_dev,'host_created_target_root_changed')
 def copy_cold_database(self):
  self.verify_created_target_root('db');self.require_docker_restart_disabled();need(self.immutable(PG)['State']['Running'] is False,'host_source_not_stopped')
  need(not any((self.core.TARGET/'db').iterdir()),'host_db_target_not_empty');mirror=self.core.ArchiveMirror(self.core.SOURCE_DB,self.core.TARGET/'db',999);proof=mirror.online();need(not proof['unstable'],'host_cold_database_changed');result=mirror.finalize(proof,source_stopped=True);save(ROOT/'.pg-storage-db-proof.json',result);self.j['dbProofSha256']=sha(private(ROOT/'.pg-storage-db-proof.json'));self.save(self.j);self.require_clean_control(self.core.TARGET/'db')
 def bind_original_paths(self):
  for label in ['db','archive']:self.verify_cold_target_proof(label)
  self.j.setdefault('oldRoots',{});self.j.setdefault('mountpointIdentities',{});self.save(self.j)
  for label,source,target in [('db',self.core.SOURCE_DB,self.core.TARGET/'db'),('archive',self.core.SOURCE_ARCHIVE,self.core.TARGET/'archive')]:
   old=source.with_name('.harbor-pg-rollback-'+self.plan['runId']+'-'+label);expected=self.plan['sourceRoots'][label];receipt={'path':str(old),'identity':expected}
   need(self.j['oldRoots'].get(label,receipt)==receipt,'host_rollback_receipt_changed');self.j['oldRoots'][label]=receipt;self.save(self.j)
   if old.exists():need(self.core.identity(old.lstat())[:5]==expected,'host_rollback_root_changed')
   else:
    need(self.core.identity(source.lstat())[:5]==expected,'host_original_root_changed');source.rename(old);self.core.sync_dir(source.parent)
   if source.exists():
    if (source.stat().st_dev,source.stat().st_ino)==(target.stat().st_dev,target.stat().st_ino):continue
    need(self.j['mountpointIdentities'].get(label)==self.core.identity(source.lstat())[:5] and not any(source.iterdir()),'host_mountpoint_unwitnessed')
   else:
    source.mkdir(mode=0o700);self.core.sync_dir(source.parent);self.j['mountpointIdentities'][label]=self.core.identity(source.stat())[:5];self.save(self.j)
   run(['/usr/bin/mount','--bind',str(target),str(source)])
  self.data_mounts()
 def install_instance_boot_guard(self):
  units={};mount_names=[]
  disk=run(['/usr/bin/systemd-escape','--path','--suffix=mount','/sdc-disk']).decode().strip()
  for label,source in [('db',self.core.SOURCE_DB),('archive',self.core.SOURCE_ARCHIVE)]:
   name=run(['/usr/bin/systemd-escape','--path','--suffix=mount',str(source)]).decode().strip();mount_names.append(name)
   units[name]='[Unit]\nDescription=Harbor fixed PG '+label+' data bind\nRequiresMountsFor='+str(self.core.TARGET/label)+'\nBindsTo='+disk+'\nAfter='+disk+'\n\n[Mount]\nWhat='+str(self.core.TARGET/label)+'\nWhere='+str(source)+'\nType=none\nOptions=bind\n\n[Install]\nWantedBy=multi-user.target\n'
  units.update(self.core.startup_units(*mount_names))
  # Artifacts must already have been explicitly installed and hash-bound in PLAN.
  need(Path(__file__)==Path('/usr/local/lib/harbor/harbor-pg-storage-host.py') and CORE==Path('/usr/local/lib/harbor/harbor-pg-storage-coordinator.py'),'host_persistent_tools_not_installed')
  for name,text in units.items():
   path=Path('/etc/systemd/system')/name
   if path.exists():need(path.read_text()==text,'host_unit_foreign')
   else:
    path.parent.mkdir(mode=0o755,exist_ok=True);fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o644)
    with os.fdopen(fd,'w') as f:f.write(text);f.flush();os.fsync(f.fileno())
  self.j['installedUnitSha256']={name:sha(text.encode()) for name,text in units.items()};self.save(self.j)
  # Exactly the selected composition loses daemon auto-start. The instance
  # units own boot order and preserve the original policies in the journal.
  current_config=self.entry.private_json(Path('/etc/harbor-gateway-deploy.json'));need(current_config==self.config,'host_server_configuration_changed');self.j.setdefault('originalStorageGuardFlag',{'present':'postgres_storage_startup_guard' in self.config,'value':self.config.get('postgres_storage_startup_guard')});self.save(self.j);self.config={**self.config,'postgres_storage_startup_guard':True};self.entry.save_json(Path('/etc/harbor-gateway-deploy.json'),self.config)
  self.fence_docker_autostart()
  run(['/usr/bin/systemctl','daemon-reload']);run(['/usr/bin/systemctl','enable',*mount_names,self.core.STORAGE_START,self.core.STORAGE_ONLINE])
 def fence_docker_autostart(self):
  rows={PG:self.immutable(PG),GW:self.gw,WORKER:self.worker};need(all(row is not None for row in rows.values()),'host_autostart_source_not_loaded')
  if 'restartPolicies' not in self.j:
   self.j['restartPolicies']={name:row['HostConfig']['RestartPolicy'] for name,row in rows.items()};self.j['restartPolicyIds']={name:row['Id'] for name,row in rows.items()};self.save(self.j)
  for name,row in rows.items():
   current=self.row(name);need(current['Id']==row['Id'] and sig(current)==sig(row),'host_autostart_container_changed')
   if current['HostConfig']['RestartPolicy']['Name']!='no':run(['/usr/bin/docker','update','--restart=no',row['Id']])
  self.require_docker_restart_disabled();self.j['autostartFenced']=True;self.save(self.j)
 def restore_original_restart_policies(self):
  if 'restartPolicies' not in self.j:return
  need(not self.j.get('oldDataRollbackForbidden',False),'host_restart_policy_restore_forbidden')
  for name,policy in self.j['restartPolicies'].items():
   row=self.row(name);need(row['Id']==self.j['restartPolicyIds'][name]==self.plan['containers'][name]['id'],'host_restart_policy_restore_identity_changed');value=policy['Name'];need(value in ['no','always','unless-stopped','on-failure'],'host_restart_policy_unknown')
   if value=='on-failure' and policy.get('MaximumRetryCount',0):value+=':'+str(policy['MaximumRetryCount'])
   run(['/usr/bin/docker','update','--restart='+value,row['Id']])
 def start_postgres(self):
  self.data_mounts()
  if getattr(self,'archive_lease',None) is not None:os.close(self.archive_lease);self.archive_lease=None
  row=self.immutable(PG);need(row['HostConfig']['RestartPolicy']['Name']=='no','host_pg_autostart_enabled')
  if not row['State']['Running']:run(['/usr/bin/docker','start',row['Id']])
  self.j['pgStartedAt']=self.row(PG)['State']['StartedAt'];self.save(self.j)
 def verify_postgres_mount_tls_report_wal(self,on_target=True):
  self.data_mounts() if on_target else self.verify_original_roots()
  def ready():
   row=self.immutable(PG)
   if not row['State']['Running'] or row['State'].get('Health',{}).get('Status')!='healthy':return False
   p=subprocess.run(BACKUP+['check'],stdin=subprocess.DEVNULL,capture_output=True,timeout=60);return p.returncode==0
  wait(ready,120,'host_pg_tls_not_ready')
  # Exact container namespace must see the newly bound host device/inode.
  node="import {stat,readFile} from 'node:fs/promises';const cfg=JSON.parse(process.argv[1]);for(const row of cfg){const v=await stat(row.path,{bigint:true});if(String(v.dev)!==row.dev||String(v.ino)!==row.ino)process.exit(1);}console.log(JSON.stringify({mountsVerified:true}));"
  cfg=[{'path':str(p),'dev':str(p.stat().st_dev),'ino':str(p.stat().st_ino)} for p in [self.core.SOURCE_DB,self.core.SOURCE_ARCHIVE]]
  need(json.loads(run(['/usr/bin/docker','exec',PG,'/usr/local/bin/node','--input-type=module','-e',node,json.dumps(cfg)]))=={'mountsVerified':True},'host_pg_namespace_mount_mismatch')
  # This is one fresh archived WAL segment, not a basebackup or PITR rehearsal.
  run(BACKUP+['retention-prepare'],timeout=120)
  budget=self.pg.preflight_timeout_seconds(self.config.get('postgres_preflight_timeout_seconds',30));run(['/usr/bin/docker','exec',PG,'gosu','postgres','/usr/local/bin/node','--input-type=module','-e',WAL_PROBE,str(budget*1000)],timeout=budget*2+10)
 def start_applications(self):
  for name in [GW,WORKER]:
   row=self.gw if name==GW else self.worker;need(row is not None,'host_current_active_not_loaded');actual=self.row(name);need(actual['Id']==row['Id'] and sig(actual)==sig(row),'host_application_changed')
   if not actual['State']['Running']:run(['/usr/bin/docker','start',actual['Id']])
  self.gw=self.row(GW);self.worker=self.row(WORKER);self.j['applicationsStartEpochMs']=int(datetime.fromisoformat(self.worker['State']['StartedAt'].replace('Z','+00:00')).timestamp()*1000);self.j['workerGeneration']={'id':self.worker['Id'],'startedAt':self.worker['State']['StartedAt']};self.save(self.j)
 def verify_current_approval_and_private_readiness(self):
  def ready():
   a,b=self.row(GW),self.row(WORKER)
   if any(not row['State']['Running'] or row['State'].get('Health',{}).get('Status')!='healthy' for row in [a,b]):return False
   for name in [GW,WORKER]:
    p=subprocess.run(['/usr/bin/docker','exec',name,'node','/app/ci/postgres-preflight.mjs'],stdin=subprocess.DEVNULL,capture_output=True,timeout=30)
    if p.returncode:return False
   return True
  wait(ready,120,'host_applications_not_ready');status=self.upgrade.active_authorization(self.active['image']);need(status.get('state')=='active' and self.upgrade.approval_binding(status)==self.active['approval'],'host_database_approval_changed')
 def verify_persistent_network(self):
  ticket=self.ticket
  network=json.loads(run(['/usr/bin/docker','network','inspect',ticket['network']['name']]))[0]
  need(network['Id']==ticket['network']['id'] and network['Name']==ticket['network']['name'] and sha(json.dumps({k:network.get(k) for k in ['Name','Id','Driver','Internal','IPAM','Options','Labels']},sort_keys=True).encode())==self.plan.get('networkPersistentDigest'),'host_boot_network_configuration_changed')
  bridge=json.loads(run(['/usr/sbin/ip','-j','link','show','dev',ticket['network']['bridge']]))[0]
  return bridge
 def update_dynamic_network_snapshot(self,boot=False):
  self.gw=self.row(GW);self.worker=self.row(WORKER);ticket=copy.deepcopy(self.ticket)
  current=self.entry.private_json(self.entry.ACTIVE)
  need(self.upgrade.active_identity(current)==self.upgrade.active_identity(self.active),'host_active_changed')
  bridge=self.verify_persistent_network()
  if not boot:
   witness=current.get('networkSnapshot',{}).get('candidateNetworkIdentity',{})
   need(all(witness.get(k)==ticket['network'].get(k) for k in ['id','name','bridge']) and witness.get('ifindex')==bridge['ifindex'] and witness.get('mac')==bridge['address'],'host_current_bridge_witness_changed')
  ticket['network']['bridgeIfindex']=bridge['ifindex'];ticket['network']['bridgeMac']=bridge['address']
  snapshot=self.upgrade.network_snapshot(ticket,self.entry,self.gw,self.worker);current=self.entry.private_json(self.entry.ACTIVE)
  need(self.upgrade.active_identity(current)==self.upgrade.active_identity(self.active),'host_active_changed')
  current['networkSnapshot']={**current.get('networkSnapshot',{}),**snapshot};self.entry.save_json(self.entry.ACTIVE,current)
 def refresh_active_entry(self):self.entry.refresh_active()
 def verify_full_instance(self,on_target=True):
  self.data_mounts() if on_target else self.verify_original_roots();self.pg.verify_started_composition(self.api,self.active['image'],self.config,self.j['applicationsStartEpochMs'],self.active['approval']);self.entry.active_record()
 def remove_gate(self,gate):
  intent=self.j.get('gateIntent',{});need(intent.get('bootId')==Path('/proc/sys/kernel/random/boot_id').read_text().strip() and intent.get('runId')==self.plan['runId'] and self.j.get('gate')==gate,'host_gate_not_owned')
  if intent.get('state')=='removed':
   tables=json.loads(self.entry.execute(['/usr/sbin/nft','--json','list','tables']));need(not any(row.get('table',{}).get('name')==self.upgrade.GATE for row in tables.get('nftables',[])),'host_removed_gate_reappeared');return
  need(intent.get('state')=='installed','host_gate_not_owned');self.upgrade.gate_remove(self.entry,gate);self.j['gateIntent']['state']='removed';self.save(self.j)
 def restore_writers(self,original,nonblocking):
  need(nonblocking is True,'host_writer_restore_must_not_wait_on_self')
  for name in reversed(self.core.WRITERS):
   if original.get(name,{}).get('ActiveState') in ['active','activating','reloading']:run(['/usr/bin/systemctl','start','--no-block',name])
 def record_failure(self,journal,old_data_allowed):
  journal['failure']={'oldDataRollbackAllowedForExplicitRecovery':old_data_allowed,'automaticOldDataRollback':False};self.save(journal)
 def require_boot_guard_and_mounts(self):
  self.data_mounts();state=self.entry.private_json(self.entry.STATE);need(state.get('phase')=='boot-guarded','host_boot_guard_required')
  need(state.get('guardFingerprint')==self.entry.guard_fingerprint(self.entry.validate(self.entry.private_json(self.entry.PROFILE))) and state.get('rulesDigest')==self.entry.state_rules_digest(),'host_boot_guard_changed');self.j['bootId']=Path('/proc/sys/kernel/random/boot_id').read_text().strip();self.save(self.j)
 def require_docker_restart_disabled(self):
  for name in [PG,GW,WORKER]:need(self.row(name)['HostConfig']['RestartPolicy']['Name']=='no','host_autostart_enabled')
 def load_current_durable_active(self,allow_pending=False):
  self.active=self.entry.private_json(self.entry.ACTIVE);need(self.active.get('configDigest')==self.entry.fingerprint(self.entry.validate(self.entry.private_json(self.entry.PROFILE))),'host_boot_entry_configuration_changed');mode=self.core.runtime_mode(self.j)
  if not allow_pending and mode in ['recovering','paused']:need(self.j['runtimeControl'].get('selectedActiveDigest')==self.upgrade.digest(self.upgrade.active_identity(self.active)),'host_runtime_recovery_approval_changed')
  record=self.upgrade.record_for(self.active['approval']);need(record.get('phase')=='complete' or (allow_pending or mode in ['recovering','paused']) and record.get('phase') in ['committing','committed'],'host_boot_release_incomplete');self.currentReleasePhase=record['phase'];self.ticket=record['ticket'];need(self.ticket['database']['cluster']==self.plan['systemId'],'host_system_id_changed');self.api['STACK']=self.ticket['materialsDirectory'];self.immutable(PG)
  # Future legitimate releases may replace the GW/Worker IDs; only the exact
  # current durable ACTIVE approval can select that pair at the next host boot.
  self.gw=self.row(GW);self.worker=self.row(WORKER)
  for row,role in [(self.gw,'gateway'),(self.worker,'worker')]:
   labels=row['Config'].get('Labels',{});need(row['Config'].get('Image')==self.active['image'] and row['Config'].get('Cmd')==['node','/app/postgres-'+role+'.mjs'] and labels.get('com.aistrong.project')==GW and labels.get('com.harbor.activation-deployment-id')==self.active['approval']['deploymentId'] and labels.get('com.harbor.activation-receipt')==self.active['approval']['receiptDigest'],'host_boot_pair_unapproved')
  need(self.gw['Id']==self.active['candidate']['containerId'] and self.gw['Image']==self.active['candidate']['imageId'] and self.worker['Image']==self.gw['Image'] and self.worker['Id']==record.get('workerId'),'host_boot_pair_identity_changed')
  for row in [self.gw,self.worker]:need(self.entry.candidate_mounts(row,self.active['approval'])==self.active['mounts'],'host_boot_material_mounts_changed')
 def verify_existing_entry_restored(self):
  state=self.entry.private_json(self.entry.STATE);control=self.entry.entry_state();need(state.get('phase')=='activated' and control.get('phase')=='active' and control.get('slot')=='candidate','host_entry_not_restored');self.load_current_durable_active();self.entry.verify_resources(state,state['activeRuntimeConfig'])
 def ensure_instance_boot_guard(self):
  if self.j.get('phase') in ['boot-arm-intent','boot-installed']:self.install_instance_boot_guard()
  else:need(self.config.get('postgres_storage_startup_guard') is True,'host_migrated_storage_guard_disabled')
  self.verify_installed_unit_files()
 def verify_installed_unit_files(self):
  entries=self.j.get('installedUnitSha256',{});need(self.core.STORAGE_START in entries and self.core.STORAGE_ONLINE in entries and len(entries)==9,'host_boot_unit_receipt_missing')
  for name,digest in entries.items():
   need('..' not in Path(name).parts and not name.startswith('/') and re.fullmatch('[a-f0-9]{64}',digest),'host_boot_unit_receipt_invalid');path=Path('/etc/systemd/system')/name;self.entry.trusted_parents(path);info=path.lstat();need(stat.S_ISREG(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o644 and info.st_nlink==1 and sha(path.read_bytes())==digest,'host_boot_unit_changed')
 def signal_online_ready(self):
  unit=self.core.STORAGE_ONLINE;state=self.state_unit(unit);need(state.get('MainPID')==str(os.getpid()) and self.entry.process_cgroup(os.getpid())=='/system.slice/'+unit,'host_notify_unit_not_owned');address=os.environ.get('NOTIFY_SOCKET','');need(address.startswith(('/', '@')),'host_notify_socket_missing')
  with socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM) as peer:peer.settimeout(1);peer.connect(('\0'+address[1:]) if address.startswith('@') else address);peer.sendall(b'READY=1')
 def activate_running_units(self):
  unit=self.j.get('workerUnit');need(isinstance(unit,str),'host_handoff_parent_not_owned');parent=self.state_unit(unit);need(parent.get('MainPID')==str(os.getpid()),'host_handoff_parent_not_owned')
  self.j['handoff']={'bootId':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'parentPid':os.getpid(),'parentTicks':self.entry.process_ticks(os.getpid()),'parentUnit':unit};self.save(self.j)
  for unit in [self.core.STORAGE_START,self.core.STORAGE_ONLINE]:run(['/usr/bin/systemctl','start',unit],timeout=180)
  for unit in [self.core.STORAGE_START,self.core.STORAGE_ONLINE]:
   def done():
    state=self.state_unit(unit)
    if state.get('ActiveState')=='failed':raise RuntimeError('host_unit_handoff_failed')
    return state.get('ActiveState')=='active' and (state.get('SubState')=='running' and int(state.get('MainPID','0'))>0 if unit==self.core.STORAGE_ONLINE else state.get('SubState')=='exited' and state.get('Result')=='success' and state.get('ExecMainStatus')=='0')
   wait(done,180,'host_unit_handoff_failed')
 def validate_handoff(self,action):
  receipt=self.j.get('handoff',{});need(self.j.get('phase')=='unit-handoff-intent' and receipt.get('bootId')==Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'host_handoff_not_current')
  parent=self.state_unit(receipt['parentUnit']);pid=receipt['parentPid'];need(parent.get('MainPID')==str(pid) and self.entry.process_ticks(pid)==receipt['parentTicks'],'host_handoff_parent_changed')
  unit=self.core.STORAGE_START if action=='boot-start' else self.core.STORAGE_ONLINE;state=self.state_unit(unit);need(state.get('MainPID')==str(os.getpid()) and self.entry.process_cgroup(os.getpid())=='/system.slice/'+unit,'host_handoff_child_not_owned')
  self.data_mounts();self.require_docker_restart_disabled();self.verify_existing_entry_restored()
  if action=='boot-online':self.signal_online_ready()
  return {'runningInstanceHandoff':True,'noContainerRestart':True}
 def verify_writer_restoration(self,original):
  for name,previous in original.items():
   if previous.get('ActiveState') not in ['active','activating','reloading']:continue
   def restored():
    state=self.state_unit(name)
    if state.get('ActiveState')=='failed':raise RuntimeError('host_writer_restore_failed')
    if name.endswith('.timer'):return state.get('ActiveState')=='active'
    if name.endswith('maintain-base.service'):
     if state.get('InvocationID') in [None,'',previous.get('InvocationID')]:return False
     if state.get('ActiveState')=='inactive':return state.get('Result')=='success' and state.get('ExecMainStatus')=='0'
     return int(state.get('MainPID','0'))>0 and (state.get('ActiveState')=='active' or state.get('ActiveState')=='activating' and state.get('SubState')=='start')
    return state.get('ActiveState')=='active' and int(state.get('MainPID','0'))>0
   wait(restored,120,'host_writer_restore_failed')
 def verify_original_roots(self):
  need(not self.j.get('oldDataRollbackForbidden',False) and not self.j.get('targetBootAuthorized',False),'host_original_data_recovery_forbidden')
  for label,row in self.j.get('oldRoots',{}).items():need(label in ['db','archive'] and row.get('path')==str(self.core.BASE/('.harbor-pg-rollback-'+self.plan['runId']+'-'+label)) and not os.path.lexists(row['path']),'host_original_data_already_renamed')
  for label,path in [('db',self.core.SOURCE_DB),('archive',self.core.SOURCE_ARCHIVE)]:need(self.core.identity(path.lstat())[:5]==self.plan['sourceRoots'][label],'host_original_root_changed')
 def verify_original_recovery_checkpoint(self):
  self.verify_original_roots();self.load_current_durable_active();need(self.upgrade.digest(self.upgrade.active_identity(self.active))==self.plan['activeIdentityDigest'],'host_original_approval_changed')
  for name in [PG,GW,WORKER]:self.immutable(name)
  row=self.row(PG)
  if not row['State']['Running']:
   need(row['State'].get('Pid')==0 and not any(row['State'].get(k) for k in ['Restarting','Paused','Dead','OOMKilled','Error']),'host_original_pg_state_unknown');self.require_clean_control(self.core.SOURCE_DB)
 def start_original_postgres(self):
  self.verify_original_roots()
  if getattr(self,'archive_lease',None) is not None:os.close(self.archive_lease);self.archive_lease=None
  row=self.immutable(PG)
  if not row['State']['Running']:run(['/usr/bin/docker','start',row['Id']])
 def verify_original_postgres(self):self.verify_postgres_mount_tls_report_wal(on_target=False)
 def verify_original_full_instance(self):self.verify_full_instance(on_target=False)
 def verify_cold_target_proof(self,label):
  self.target_disk();path=PROOF if label=='archive' else ROOT/'.pg-storage-db-proof.json';raw=private(path)
  need(sha(raw)==self.j.get('dbProofSha256' if label=='db' else 'archiveProofSha256'),'host_cold_proof_changed');proof=json.loads(raw);need(proof.get('eligible') is True,'host_target_not_cold_verified')
  target=self.core.TARGET/label;need(self.core.identity(target.lstat())[1:5]==proof.get('targetRoot',[])[1:5],'host_target_root_identity_changed')
  actual,dirs=self.core.inventory(target,999)
  need(set(dirs)==set(proof.get('directories',{})) and all(self.core.identity((target/name).lstat())[1:5]==stamp[1:5] for name,stamp in proof['directories'].items()),'host_cold_target_directories_changed')
  need(set(actual)==set(proof['files']) and all(stamp[1:]==proof['files'][name]['target'][1:] for name,stamp in actual.items()),'host_cold_target_changed')
 def verify_target_recovery_checkpoint(self):
  self.core.recovery_mode(self.j);self.load_current_durable_active();control=self.entry.entry_state();need(control.get('slot')=='candidate' and control.get('phase') in ['active','holding','drained'],'host_target_recovery_entry_unknown')
  if not self.j.get('targetBootAuthorized',False):
   for name in [PG,GW,WORKER]:need(self.immutable(name)['State']['Running'] is False,'host_prestart_recovery_container_running')
   # Both inventories must be complete and authentic before any external change.
   for label in ['db','archive']:self.verify_cold_target_proof(label)
  if control['phase']=='active':self.upgrade.drain_active_entry(self.entry)
  if not self.j.get('targetBootAuthorized',False):self.fence_docker_autostart();self.bind_original_paths()
  self.data_mounts();self.install_gate()
 def runtime_generation(self):
  return {name:container_generation(self.row(name)) for name in [PG,GW,WORKER]}
 def record_runtime_ready(self,resolve_operation=False):
  control=self.j.get('runtimeControl',{});mode=self.core.runtime_mode(self.j);binding=self.upgrade.digest(self.upgrade.active_identity(self.entry.private_json(self.entry.ACTIVE)))
  need(mode!='paused' and (mode!='planned' or resolve_operation),'host_runtime_operation_pending')
  if mode=='recovering':need(control.get('selectedActiveDigest')==binding,'host_runtime_recovery_approval_changed')
  self.j['runtimeReceipt']={'activeIdentityDigest':binding,'containers':self.runtime_generation(),'bootId':Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
  record=self.upgrade.record_for(self.entry.private_json(self.entry.ACTIVE)['approval'])
  if record.get('phase')=='complete':next_control={'mode':'running'}
  else:
   need(mode=='recovering' and control.get('selectedActiveDigest')==binding and record.get('phase') in ['committing','committed'],'host_runtime_pending_authorization_missing');next_control={'mode':'recovering','selectedActiveDigest':binding}
  self.j['runtimeControl']=next_control;operation=self.j.pop('plannedOperation',None)
  if operation:self.j['resolvedOperation']={'nonce':operation['nonce'],'activeIdentityDigest':binding}
  self.j.pop('runtimeRecovery',None);self.j.pop('entryBootstrap',None);self.j.pop('writersPauseStarted',None);self.save(self.j)
 def verify_absent_entry_bootstrap(self):
  own=Path('/proc/self/ns/net').stat();init=Path('/proc/1/ns/net').stat();need((own.st_dev,own.st_ino)==(init.st_dev,init.st_ino),'host_entry_network_namespace_unknown')
  row=self.runtime.inspect_entry(self.entry);state=row['State'];need(state.get('Running') is False and state.get('Pid')==0 and state.get('Status') in ['created','exited'] and not any(state.get(k) for k in ['Paused','Restarting','Dead','Error']),'host_entry_resources_not_absent')
  profile=self.entry.validate(self.entry.private_json(self.entry.PROFILE));group=Path('/sys/fs/cgroup')/profile['entryCgroup'].lstrip('/');need(Path('/sys/fs/cgroup/cgroup.controllers').is_file(),'host_entry_cgroup_layout_unknown')
  if group.exists():need(dict(line.split() for line in (group/'cgroup.events').read_text().splitlines()).get('populated')=='0','host_entry_resources_not_absent')
  no_entry_listeners(profile['entryPort'],self.entry.CONTROL)
  if os.path.lexists(self.entry.CONTROL):
   path=Path(self.entry.CONTROL);self.entry.trusted_parents(path);info=path.lstat();need(stat.S_ISSOCK(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600,'host_entry_stale_control_untrusted')
  saved=self.entry.private_json(self.entry.STATE);need(saved.get('rulesDigest')==self.entry.state_rules_digest(),'host_entry_guard_changed')
  if saved.get('phase')=='boot-guarded':
   need(saved.get('configDigest')==self.entry.fingerprint(profile) and saved.get('guardFingerprint')==self.entry.guard_fingerprint(profile),'host_entry_guard_changed')
  else:
   need(saved.get('phase') in ['activated','active-prepared'] and saved.get('configDigest')==self.entry.fingerprint(profile) and saved.get('mangleDigest')==self.entry.mangle_digest(),'host_entry_guard_changed')
   allowed={self.active['candidate']['containerId']};operation=self.j.get('plannedOperation',{})
   if operation and self.j.get('gateOrigin',{}).get('operationNonce')==operation.get('nonce'):
    release=self.upgrade.record_for(operation['targetApproval']);need(release.get('ticketDigest')==operation.get('targetTicketDigest') and self.upgrade.digest(self.upgrade.active_identity(release.get('fromActive',{})))==operation.get('beforeActiveDigest'),'host_entry_previous_approval_changed');allowed.add(release['fromActive']['candidate']['containerId'])
   need(saved.get('candidate',{}).get('containerId') in allowed,'host_entry_guard_candidate_changed')
   live={row['ifname'] for row in json.loads(self.entry.execute(['/usr/sbin/ip','-j','link','show']))}
   for device,digest in saved.get('tcDigests',{}).items():
    if device in live:need(self.entry.tc_digest(device)==digest,'host_entry_guard_tc_changed')
   sets=json.loads(self.entry.execute(['/usr/sbin/nft','--json','list','set','inet',self.entry.TABLE,'enabled_ports']));elements=next(item['set'].get('elem',[]) for item in sets['nftables'] if 'set' in item);need(elements==([profile['source']['port']] if saved['phase']=='activated' else []),'host_entry_guard_ports_changed')
  # This warm recovery reuses the still-owned candidate bridge. A missing or
  # foreign bridge is not invented here; cold boot retains its existing chain.
  candidate=self.entry.private_json(self.entry.CANDIDATE_PROFILE);need(candidate.get('socketPath')=='/run/harbor-entry/candidate.sock','host_entry_candidate_profile_changed');self.entry.refreshed_bypass(profile)
  gate=self.j.get('gateIntent',{});need(gate.get('state')=='installed' and gate.get('bootId')==Path('/proc/sys/kernel/random/boot_id').read_text().strip() and gate.get('runId')==self.plan['runId'] and gate.get('activeIdentityDigest')==self.upgrade.digest(self.upgrade.active_identity(self.active)) and self.upgrade.gate_snapshot(self.entry)==self.j.get('gate'),'host_entry_bootstrap_gate_not_owned')
  self.j['entryBootstrap']={'bootId':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'containerId':row['Id'],'imageId':row['Image'],'activeIdentityDigest':self.upgrade.digest(self.upgrade.active_identity(self.active)),'profileDigest':self.entry.fingerprint(profile)};self.save(self.j)
 def request_existing_entry_start(self):
  self.core.recovery_mode(self.j);need(self.j.get('phase')=='runtime-entry-pending','host_entry_bootstrap_phase_changed')
  # Submission never waits for entry-pre/post to acquire this same lock.
  run(['/usr/bin/systemctl','reset-failed','harbor-gateway-entry.service']);run(['/usr/bin/systemctl','start','--no-block','harbor-gateway-entry.service'])
 def finish_runtime_entry(self):
  self.core.recovery_mode(self.j);self.load_current_durable_active();self.data_mounts();self.require_docker_restart_disabled();self.verify_installed_unit_files()
  proof=self.j.get('entryBootstrap',{});profile=self.entry.validate(self.entry.private_json(self.entry.PROFILE));need(proof.get('bootId')==Path('/proc/sys/kernel/random/boot_id').read_text().strip() and proof.get('profileDigest')==self.entry.fingerprint(profile) and proof.get('activeIdentityDigest')==self.upgrade.digest(self.upgrade.active_identity(self.active)),'host_entry_bootstrap_approval_changed')
  try:control=self.entry.entry_state(timeout=2)
  except (OSError,http.client.HTTPException):self.request_existing_entry_start();return {'entryBootstrapRequired':True}
  need(control.get('slot')=='candidate' and control.get('phase')=='active','host_entry_not_restored');row=self.runtime.inspect_entry(self.entry);need(row['Id']==proof.get('containerId') and row['Image']==proof.get('imageId') and row['State'].get('Running') is True and row['State'].get('Pid',0)>0,'host_entry_bootstrap_container_changed');pid=self.entry.socket_owner(Path(self.entry.CONTROL));need(pid==row['State']['Pid'] and self.entry.process_cgroup(pid)==profile['entryCgroup'],'host_entry_bootstrap_owner_changed')
  self.verify_postgres_facts();self.verify_current_approval_and_private_readiness();self.verify_existing_entry_restored();self.verify_full_instance();self.remove_gate(self.j['gate'])
  if self.j.get('runtimeRecovery',{}).get('cascade'):self.restore_writers(self.j['writers'],nonblocking=True);self.verify_writer_restoration(self.j['writers'])
  self.j['phase']='complete';self.record_runtime_ready();return {'recovered':True,'entryRestored':True}
 def hold_runtime_entry(self):
  try:state=self.entry.entry_state(timeout=2)
  except (OSError,http.client.HTTPException):
   self.verify_absent_entry_bootstrap();return False
  need(state.get('slot')=='candidate' and state.get('phase') in ['active','holding','drained'],'host_runtime_entry_unknown')
  if state['phase']=='active':state=self.entry.entry_state('hold',state['epoch'])
  need(state.get('phase') in ['holding','drained'],'host_runtime_entry_not_held');return True
 def stop_runtime_container(self,name):
  expected=self.immutable(PG) if name==PG else self.gw if name==GW else self.worker
  need(expected is not None,'host_current_active_not_loaded');row=self.row(name);need(row['Id']==expected['Id'] and sig(row)==sig(expected),'host_runtime_container_changed')
  if row['State']['Running']:
   need(not any(row['State'].get(k) for k in ['Paused','Restarting','Dead','Error']),'host_runtime_container_state_unknown')
   signal_value=row['Config'].get('StopSignal','SIGINT' if name==PG else 'SIGTERM');need(signal_value in ['SIGINT','SIGTERM'],'host_runtime_stop_signal_unknown');run(['/usr/bin/docker','kill','--signal='+signal_value,row['Id']])
  def stopped():
   actual=self.row(name);need(actual['Id']==expected['Id'] and sig(actual)==sig(expected),'host_runtime_container_changed');state=actual['State']
   if state.get('Running'):return False
   need(state.get('Pid')==0 and state.get('Status') in ['exited','created'] and not any(state.get(k) for k in ['Paused','Restarting','Dead','Error']),'host_runtime_container_state_unknown');return True
  # No SIGKILL escalation, source image preflight or health requirement here.
  budget=self.pg.shutdown_configuration(self.config.get('postgres_shutdown'));seconds=(budget.get('gateway_drain_ms',300000)+budget.get('cleanup_grace_ms',30000))/1000 if name==GW else budget.get('worker_stop_ms',30000)/1000 if name==WORKER else 120
  wait(stopped,seconds,'host_runtime_stop_unconfirmed')
 def verify_postgres_facts(self):
  row=self.immutable(PG);need(row['State']['Running'] and row['State'].get('Health',{}).get('Status')=='healthy','host_runtime_pg_not_ready')
  cfg={'systemId':self.plan['systemId'],'mounts':[{'path':str(path),'dev':str(path.stat().st_dev),'ino':str(path.stat().st_ino)} for path in [self.core.SOURCE_DB,self.core.SOURCE_ARCHIVE]]}
  need(json.loads(run(['/usr/bin/docker','exec',PG,'gosu','postgres','/usr/local/bin/node','--input-type=module','-e',READONLY_PROBE,json.dumps(cfg)],timeout=60))=={'readOnlyFactsVerified':True},'host_runtime_pg_facts_failed')
 def verify_runtime_facts(self):
  self.load_current_durable_active();self.data_mounts();self.require_docker_restart_disabled();self.verify_installed_unit_files();self.verify_postgres_facts();self.verify_current_approval_and_private_readiness()
  worker=self.row(WORKER);self.j['applicationsStartEpochMs']=int(datetime.fromisoformat(worker['State']['StartedAt'].replace('Z','+00:00')).timestamp()*1000)
  tables=json.loads(self.entry.execute(['/usr/sbin/nft','--json','list','tables']));need(not any(row.get('table',{}).get('name')==self.upgrade.GATE for row in tables.get('nftables',[])),'host_runtime_gate_still_installed')
  self.verify_existing_entry_restored();self.entry.verify_candidate_bridge(self.active['networkSnapshot']);self.pg.verify_started_composition(self.api,self.active['image'],self.config,self.j['applicationsStartEpochMs'],self.active['approval'])
 def inspect_runtime_containers(self):
  names=[PG,GW,WORKER];values=json.loads(run(['/usr/bin/docker','container','inspect',*names],timeout=15))
  need(isinstance(values,list) and len(values)==3 and all(isinstance(row,dict) and isinstance(row.get('State'),dict) and type(row['State'].get('Running')) is bool for row in values),'host_runtime_inspection_invalid')
  rows={row.get('Name'):row for row in values};need(set(rows)=={'/'+name for name in names},'host_runtime_inspection_invalid');return {name:rows['/'+name] for name in names}
 def runtime_step(self):
  mode=self.core.runtime_mode(self.j)
  if mode in ['paused','planned']:return {'paused':True}
  need(self.j.get('phase') in ['complete','runtime-recovering','runtime-entry-pending'],'host_runtime_phase_not_ready')
  # The steady path is one bounded Docker inspect plus cheap root JSON/tuple
  # comparisons. Full proofs remain in the locked recovery path below.
  rows=self.inspect_runtime_containers();generation={name:container_generation(row) for name,row in rows.items()};active=self.entry.private_json(self.entry.ACTIVE);binding=self.upgrade.digest(self.upgrade.active_identity(active));old=self.j.get('runtimeReceipt',{})
  need(all(row.get('HostConfig',{}).get('RestartPolicy',{}).get('Name')=='no' for row in rows.values()),'host_autostart_enabled')
  if mode=='recovering':need(self.j['runtimeControl'].get('selectedActiveDigest')==binding,'host_runtime_recovery_approval_changed')
  steady=all(row['State']['Running'] and not any(row['State'].get(k) for k in ['Paused','Restarting','Dead','Error']) for row in rows.values())
  if self.j.get('phase')=='complete' and old.get('containers')==generation and old.get('activeIdentityDigest')==binding and steady:
   if mode=='recovering':
    record=self.upgrade.record_for(active['approval']);need(record.get('phase') in ['committing','committed','complete'],'host_boot_release_incomplete')
    if record['phase']=='complete':self.verify_runtime_facts();self.record_runtime_ready()
   return {'unchanged':True}
  if self.j.get('phase')=='runtime-entry-pending' and steady:return self.finish_runtime_entry()
  return self.recover_runtime()
 def recover_runtime(self):
  self.core.recovery_mode(self.j);need(self.j.get('oldDataRollbackForbidden') is True and self.j.get('targetBootAuthorized') is True,'host_runtime_target_not_armed')
  # Durable authorization and physical mounts are checked before starting any
  # process. Live DB approval/readiness is checked only after PG can run.
  self.load_current_durable_active();self.data_mounts();self.require_docker_restart_disabled();self.verify_installed_unit_files();self.install_gate();entry_available=self.hold_runtime_entry()
  pg=self.immutable(PG);last=self.j.get('runtimeReceipt',{}).get('containers',{}).get(PG,{})
  previous=self.j.get('runtimeRecovery',{});binding=self.upgrade.digest(self.upgrade.active_identity(self.active))
  need(not previous or previous.get('activeIdentityDigest')==binding,'host_runtime_recovery_approval_changed')
  cascade=previous.get('cascade') is True or not pg['State']['Running'] or last.get('startedAt')!=pg['State']['StartedAt']
  self.j['phase']='runtime-recovering';self.j['runtimeRecovery']={'activeIdentityDigest':self.upgrade.digest(self.upgrade.active_identity(self.active)),'cascade':cascade};self.save(self.j)
  if cascade:
   # PG generation loss invalidates the GW advisory owner. Recycle the exact
   # authorized applications even if Docker still reports them running.
   self.pause_writers()
   for name in [GW,WORKER]:self.stop_runtime_container(name)
  self.start_postgres();self.verify_postgres_mount_tls_report_wal();self.start_applications();self.verify_current_approval_and_private_readiness();self.update_dynamic_network_snapshot();self.refresh_active_entry()
  if entry_available is False:
   self.j['phase']='runtime-entry-pending';self.save(self.j);self.request_existing_entry_start();return {'entryBootstrapRequired':True}
  self.verify_full_instance();self.verify_existing_entry_restored()
  self.remove_gate(self.j['gate'])
  if cascade:self.restore_writers(self.j['writers'],nonblocking=True);self.verify_writer_restoration(self.j['writers'])
  self.j['phase']='complete';self.record_runtime_ready();return {'recovered':True,'sameDatabase':True,'oldDataRollbackForbidden':True}
 def pause_runtime(self):
  need(self.core.runtime_mode(self.j) in ['running','recovering','paused'],'host_runtime_operation_pending');self.load_current_durable_active();self.data_mounts();self.require_docker_restart_disabled()
  if self.j.get('runtimeControl',{}).get('mode')!='paused':
   self.j['runtimeControl']={'mode':'paused','activeIdentityDigest':self.upgrade.digest(self.upgrade.active_identity(self.active)),'selectedActiveDigest':self.upgrade.digest(self.upgrade.active_identity(self.active))};self.save(self.j)
  self.install_gate();self.hold_runtime_entry();self.pause_writers()
  for name in [GW,WORKER,PG]:self.stop_runtime_container(name)
  return {'paused':True,'automaticRecoveryDisabled':True}
 def adopt_release_gate_for_resume(self):
  tables=json.loads(self.entry.execute(['/usr/sbin/nft','--json','list','tables']))
  if not any(row.get('table',{}).get('name')==self.upgrade.GATE for row in tables.get('nftables',[])):return
  boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip();binding=self.upgrade.digest(self.upgrade.active_identity(self.active));snapshot=self.upgrade.gate_snapshot(self.entry);expected={'state':'installed','bootId':boot_id,'runId':self.plan['runId'],'activeIdentityDigest':binding}
  if self.j.get('gateIntent')==expected:
   need(snapshot==self.j.get('gate'),'host_gate_changed');return
  operation=self.j.get('plannedOperation',{})
  need(self.core.runtime_mode(self.j)=='recovering' and self.j['runtimeControl'].get('selectedActiveDigest')==binding and operation.get('bootId')==boot_id and operation.get('runId')==self.plan['runId'] and re.fullmatch('[a-f0-9]{32}',operation.get('nonce','')) and isinstance(operation.get('targetApproval'),dict),'host_release_gate_not_owned')
  record=self.upgrade.record_for(operation['targetApproval']);ticket=record['ticket']
  need(record.get('ticketDigest')==operation.get('targetTicketDigest')==self.upgrade.digest(ticket) and ticket['to']['image']==operation.get('requestedImage') and ticket['from']['activeIdentityDigest']==operation.get('beforeActiveDigest')==self.upgrade.digest(self.upgrade.active_identity(record.get('fromActive',{}))),'host_release_gate_approval_changed')
  source=binding==operation['beforeActiveDigest'] and record.get('phase') in ['holding','stop-unknown'] and self.gw['Id']==ticket['from']['containerId'] and self.worker['Id']==ticket['from']['workerId']
  target=self.upgrade.approval_binding(self.active['approval'])==operation['targetApproval'] and self.active['image']==operation['requestedImage'] and record.get('phase') in ['committing','committed'] and record.get('candidate',{}).get('containerId')==self.gw['Id']==self.active['candidate']['containerId'] and record.get('workerId')==self.worker['Id']
  need((source or target) and ticket['database']['cluster']==self.plan['systemId'] and all(ticket['network'].get(k)==self.ticket['network'].get(k) for k in ['id','name','bridge']) and isinstance(record.get('gate'),dict) and snapshot==record['gate'],'host_release_gate_proof_changed')
  bridge=self.verify_persistent_network();network=self.ticket['network'];witness=self.active.get('networkSnapshot',{}).get('candidateNetworkIdentity',{})
  need(all(witness.get(k)==network.get(k) for k in ['id','name','bridge']) and witness.get('ifindex')==bridge['ifindex'] and witness.get('mac')==bridge['address'],'host_current_bridge_witness_changed')
  self.j['gate']=snapshot;self.j['gateIntent']=expected;self.j['gateOrigin']={'source':'explicit-release-recovery','operationNonce':operation['nonce'],'approvalDigest':self.upgrade.digest(operation['targetApproval'])};self.save(self.j)
 def prepare_runtime_resume(self):
  self.core.runtime_mode(self.j);self.load_current_durable_active(allow_pending=True);self.data_mounts();self.require_docker_restart_disabled();self.verify_installed_unit_files()
  self.j['runtimeControl']={'mode':'recovering','selectedActiveDigest':self.upgrade.digest(self.upgrade.active_identity(self.active))};self.save(self.j)
  self.adopt_release_gate_for_resume();state=self.entry.private_json(self.entry.STATE)
  if state.get('phase')=='boot-guarded' and not Path(self.entry.CONTROL).exists():return {'bootstrap':True}
  return self.recover_runtime()
 def reconcile_planned_operation(self,success=None):
  self.core.runtime_mode(self.j);operation=self.j.get('plannedOperation');need(isinstance(operation,dict) and self.j.get('runtimeControl',{}).get('mode')=='planned','host_no_planned_operation')
  self.load_current_durable_active();binding=self.upgrade.digest(self.upgrade.active_identity(self.active))
  allowed=binding==operation['beforeActiveDigest'] or success is not False and self.active['image']==operation['requestedImage']
  need(allowed,'host_planned_result_unknown');self.verify_runtime_facts();need(self.upgrade.digest(self.upgrade.active_identity(self.entry.private_json(self.entry.ACTIVE)))==binding,'host_active_changed')
  # This is fact-only: it cannot resume a held entry or start a failed service.
  self.record_runtime_ready(resolve_operation=True);return {'released':True,'originalRestored':binding==operation['beforeActiveDigest']}
 def precopy(self):
  need(self.j['phase']=='planned','host_precopy_already_started');self.current_identity()
  need(not self.core.TARGET.exists(),'host_target_already_exists');self.core.TARGET.mkdir(mode=0o700)
  for label in ['db','archive']:
   path=self.core.TARGET/label;path.mkdir(mode=0o700);os.chown(path,999,999)
  self.j['createdTargetRoots']={label:self.core.identity((self.core.TARGET/label).lstat())[:5] for label in ['db','archive']};self.j['phase']='archive-precopying';self.save(self.j)
  proof=self.core.ArchiveMirror(self.core.SOURCE_ARCHIVE,self.core.TARGET/'archive',999).online();save(PROOF,proof);self.j.update(phase='archive-prepared',archiveProofSha256=sha(private(PROOF)));self.save(self.j);return {'prepared':True,'eligible':False}

READONLY_PROBE=r"""import {readFile,stat} from 'node:fs/promises';import {createRecordingProbeStore} from '/opt/harbor-storage-report/postgres-recording-storage.mjs';import {recordingDatabaseIdentity,loadRecordingStorageReportKey,verifyRecordingStorageReport} from '/opt/harbor-storage-report/postgres-storage-report.mjs';const expected=JSON.parse(process.argv[1]),cfg=JSON.parse(await readFile('/run/harbor-infra-control/publisher.json','utf8')),store=createRecordingProbeStore({connection:{host:cfg.socketDirectory,port:cfg.port,user:'postgres',database:cfg.database}});let key;try{const identity=await recordingDatabaseIdentity(store);if(identity.cluster!==expected.systemId)throw Error();key=await loadRecordingStorageReportKey(cfg.keyFile);const row=await store.get('SELECT payload,signature FROM harbor.recording_storage_reports WHERE singleton=TRUE'),report=verifyRecordingStorageReport(row,key,identity),started=await store.get('SELECT ceil(extract(epoch FROM pg_postmaster_start_time())*1000)::bigint started');if(report.sampled_at<Number(started.started))throw Error();for(const path of expected.mounts){const value=await stat(path.path,{bigint:true});if(String(value.dev)!==path.dev||String(value.ino)!==path.ino)throw Error();}console.log(JSON.stringify({readOnlyFactsVerified:true}));}catch{process.exitCode=1;}finally{key?.fill(0);await store.close();}"""

WAL_PROBE=r"""import {readFile} from 'node:fs/promises';import {setTimeout as delay} from 'node:timers/promises';import {createRecordingProbeStore} from '/opt/harbor-storage-report/postgres-recording-storage.mjs';import {recordingDatabaseIdentity,loadRecordingStorageReportKey,verifyRecordingStorageReport} from '/opt/harbor-storage-report/postgres-storage-report.mjs';import {loadPostgresBackupArchiveProfile} from '/opt/harbor-storage-report/postgres-backup-context.mjs';import {inspectArchivedWal} from '/opt/harbor-storage-report/postgres-recovery.mjs';
const budget=Number(process.argv[1]);if(!Number.isSafeInteger(budget)||budget<30000||budget>300000)process.exit(1);const cfg=JSON.parse(await readFile('/run/harbor-infra-control/publisher.json','utf8')),store=createRecordingProbeStore({connection:{host:cfg.socketDirectory,port:cfg.port,user:'postgres',database:cfg.database}});let archive;try{let valid=false;for(const deadline=performance.now()+budget;performance.now()<deadline;){try{const row=await store.get('SELECT payload,signature FROM harbor.recording_storage_reports WHERE singleton=TRUE');const verified=verifyRecordingStorageReport(row,await loadRecordingStorageReportKey(cfg.keyFile),await recordingDatabaseIdentity(store)),started=await store.get('SELECT ceil(extract(epoch FROM pg_postmaster_start_time())*1000)::bigint started');if(verified.sampled_at<Number(started.started))throw Error();valid=true;break;}catch{await delay(100);}}if(!valid)throw Error();archive=await loadPostgresBackupArchiveProfile();const {name}=await store.get('SELECT pg_walfile_name(pg_switch_wal()) name');let ok=false;for(const deadline=performance.now()+budget;performance.now()<deadline;){try{const proof=await inspectArchivedWal({name,directory:archive.configuration.archiveDirectory,key:archive.key,requireBoundName:true});if(proof.systemIdentifier!==archive.configuration.expectedCluster)throw Error();ok=true;break;}catch{await delay(100);}}if(!ok)throw Error();console.log(JSON.stringify({freshReport:true,newWalAuthenticated:true,restorePerformed:false}));}catch{process.exitCode=1;}finally{archive?.key.fill(0);await store.close();}"""

@contextlib.contextmanager
def locks(host,deployment_fd=None):
 fds=[]
 try:
  paths=[host.entry.STATE.with_suffix('.lock'),ROOT/'.backup-copy.lock']
  if deployment_fd is None:paths.insert(0,Path('/run/harbor-gateway-deploy.lock'))
  else:verify_deployment_lock(deployment_fd)
  for path in paths:
   fd=os.open(path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600);info=os.fstat(fd);need(stat.S_ISREG(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022 and info.st_nlink==1,'host_lock_untrusted');fds.append(fd);fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
  yield
 finally:
  for fd in reversed(fds):os.close(fd)

def collect_readonly():
 need(os.geteuid()==0,'host_root_required')
 core=load(CORE,'harbor_storage_collect_core');entry=load(ENTRY,'harbor_storage_collect_entry');upgrade=load(UPGRADE,'harbor_storage_collect_upgrade')
 active=entry.private_json(entry.ACTIVE);record=upgrade.record_for(active['approval']);need(record.get('phase')=='complete','host_collect_release_incomplete');ticket=record['ticket']
 rows={}
 for name in [PG,GW,WORKER]:
  found=json.loads(run(['/usr/bin/docker','container','inspect',name]));need(len(found)==1 and found[0]['State'].get('Running') is True,'host_collect_container_not_running');row=found[0];rows[name]={'id':row['Id'],'signature':sig(row),'startedAt':row['State']['StartedAt'],'pid':row['State']['Pid'],'restartPolicy':row['HostConfig']['RestartPolicy']}
 need(rows[GW]['id']==active['candidate']['containerId'],'host_collect_active_mismatch')
 disk=json.loads(run(['/usr/bin/findmnt','--json','--target','/sdc-disk','--output','TARGET,UUID']))['filesystems'];need(len(disk)==1 and disk[0]['target']=='/sdc-disk','host_collect_sdc_not_mounted')
 network=json.loads(run(['/usr/bin/docker','network','inspect',ticket['network']['name']]))[0];need(network['Id']==ticket['network']['id'] and set(network.get('Containers',{}))=={v['id'] for v in rows.values()},'host_collect_network_changed')
 raw=private(Path(ticket['materialsDirectory'])/'secrets/postgres.env',65536).decode();values=dict(line.split('=',1) for line in raw.splitlines() if line and not line.startswith('#'));url=urlsplit(values.get('HARBOR_DATABASE_URL',''));query=parse_qs(url.query);need(url.scheme in ['postgres','postgresql'] and query.get('sslmode')==['verify-full'] and query.get('sslrootcert')==['/run/secrets/postgres-ca.crt'],'host_collect_tls_not_verified')
 return {'scope':core.SCOPE,'approved':False,'readOnly':True,'runId':uuid.uuid4().hex,'systemId':ticket['database']['cluster'],'tlsVerifyFull':True,'activeIdentityDigest':upgrade.digest(upgrade.active_identity(active)),'materialsDirectory':ticket['materialsDirectory'],'containers':rows,'sourceRoots':{k:core.identity(v.lstat())[:5] for k,v in [('db',core.SOURCE_DB),('archive',core.SOURCE_ARCHIVE)]},'targetFilesystemUuid':disk[0]['uuid'],'networkPersistentDigest':sha(json.dumps({k:network.get(k) for k in ['Name','Id','Driver','Internal','IPAM','Options','Labels']},sort_keys=True).encode()),'toolSha256':{str(v):sha(v.read_bytes()) for v in [CORE,Path(__file__),DEPLOY,ENTRY,UPGRADE,COMPOSE,RUNTIME]},'noLargeDirectoryScan':True,'noCredentialValuesRetained':True}

def verify_factory_guard(config):
 host=Host();need(config==host.config and config.get('postgres_storage_startup_guard') is True,'host_factory_root_configuration_changed');need(host.j.get('phase')=='complete' and host.j.get('oldDataRollbackForbidden') is True,'host_factory_move_not_complete');host.data_mounts();host.immutable(PG);host.require_docker_restart_disabled();host.verify_installed_unit_files();need(host.row(PG)['HostConfig']['RestartPolicy']['Name']=='no','host_factory_pg_autostart_enabled')
 for name in [host.core.STORAGE_START,host.core.STORAGE_ONLINE]:
  row=host.state_unit(name);need(row.get('UnitFileState')=='enabled','host_factory_boot_guard_not_enabled')
  if name==host.core.STORAGE_ONLINE:need(row.get('ActiveState')=='active' and row.get('SubState')=='running' and int(row.get('MainPID','0'))>0,'host_factory_supervisor_not_running')
 return {'contract':'harbor-pg-storage-boot-v1','completed':True,'samePostgres':True,'mountsVerified':True}

def worker_paths(host,action,nonce=None):
 need(action in ['switch','recover-source','recover-target'],'host_worker_action_invalid')
 if action=='switch':need(nonce is None,'host_worker_nonce_invalid');suffix=''
 else:need(isinstance(nonce,str) and re.fullmatch('[a-f0-9]{16}',nonce),'host_worker_nonce_invalid');suffix='-'+action+'-'+nonce
 unit='harbor-pg-storage-move-'+host.plan['runId']+suffix+'.service'
 intent=ROOT/('.pg-storage-switch-launch.json' if action=='switch' else '.pg-storage-'+action+'-launch-'+nonce+'.json')
 return unit,intent,ROOT/('.pg-storage-'+host.plan['runId']+suffix+'.log')

def workers_quiet(host):
 boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
 for path in ROOT.glob('.pg-storage-*launch*.json'):
  intent=json.loads(private(path,262144));unit=intent.get('unit','');old_boot=intent.get('bootId','')
  expected_unit,expected_path,_=worker_paths(host,intent.get('action'),intent.get('nonce'))
  need(path==expected_path and unit==expected_unit and intent.get('runId')==host.plan['runId'] and intent.get('planSha256')==host.planSha256 and re.fullmatch('[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}',old_boot),'host_unknown_worker_intent')
  state=host.state_unit(unit,allow_absent=True)
  if state.get('LoadState')=='not-found':
   need(old_boot!=boot_id and state.get('MainPID')=='0' and state.get('ActiveState')=='inactive' and state.get('ControlGroup','')=='','host_previous_worker_not_quiet');continue
  need(state.get('LoadState')=='loaded' and state.get('MainPID')=='0' and (state.get('ActiveState') in ['inactive','failed'] or state.get('ActiveState')=='active' and state.get('SubState')=='exited'),'host_previous_worker_not_quiet')
  need(Path('/sys/fs/cgroup/cgroup.controllers').is_file(),'host_worker_cgroup_layout_unknown');group=state.get('ControlGroup','')
  if group:
   need(group.startswith('/') and group==os.path.normpath(group) and group!='/','host_worker_cgroup_invalid');directory=Path('/sys/fs/cgroup')/group.lstrip('/')
   if directory.exists():need(dict(line.split() for line in (directory/'cgroup.events').read_text().splitlines()).get('populated')=='0','host_previous_worker_descendants_active')

def require_recovery_launch(host,action):
 if action not in ['recover-source','recover-target']:return
 latest=json.loads(private(STATE)) if STATE.exists() else {'scope':host.plan['scope'],'runId':host.plan['runId']}
 need(latest.get('scope')==host.plan['scope'] and latest.get('runId')==host.plan['runId'],'host_state_scope_changed');host.core.recovery_mode(latest)

def launch_switch(host,action='switch'):
 nonce=None if action=='switch' else uuid.uuid4().hex[:16];unit,intent,log=worker_paths(host,action,nonce)
 fd=os.open(ROOT/'.pg-storage-launch.lock',os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
 try:
  info=os.fstat(fd);need(stat.S_ISREG(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600 and info.st_nlink==1,'host_launch_lock_untrusted');fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);require_recovery_launch(host,action)
  if action=='switch':need(host.j.get('phase')=='archive-prepared','host_switch_phase_invalid')
  workers_quiet(host);need(not intent.exists() and not log.exists(),'host_switch_already_requested')
  p=subprocess.run(['/usr/bin/systemctl','show',unit,'--property=LoadState'],stdin=subprocess.DEVNULL,capture_output=True,timeout=30);need(p.returncode in [0,1] and p.stdout.strip()==b'LoadState=not-found','host_switch_unit_exists')
  need(Path(__file__)==Path('/usr/local/lib/harbor/harbor-pg-storage-host.py'),'host_persistent_tools_not_installed')
  out=os.open(log,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600);os.close(out)
  marker=os.open(intent,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
  with os.fdopen(marker,'w') as stream:json.dump({'runId':host.plan['runId'],'bootId':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'action':action,'nonce':nonce,'unit':unit,'planSha256':host.planSha256,'state':'launch-requested'},stream);stream.flush();os.fsync(stream.fileno())
  host.core.sync_dir(ROOT)
  run(['/usr/bin/systemd-run','--unit='+unit,'--property=Type=exec','--property=RemainAfterExit=yes','--property=UMask=0077','--property=KillMode=process','--property=SendSIGKILL=no','--property=TimeoutStopSec=infinity','--property=Nice=10','--property=IOSchedulingClass=idle','--property=StandardOutput=append:'+str(log),'--property=StandardError=append:'+str(log),'/usr/bin/python3','-I','-B',str(Path(__file__)),action,*([nonce] if nonce else [])])
  return {'submitted':True,'unit':unit,'inspectUnitBeforeAnyRetry':True}
 finally:os.close(fd)

def require_switch_worker(host,action,nonce=None):
 unit,path,_=worker_paths(host,action,nonce);intent=json.loads(private(path));need(intent.get('bootId')==Path('/proc/sys/kernel/random/boot_id').read_text().strip() and intent.get('runId')==host.plan['runId'] and intent.get('planSha256')==sha(private(PLAN)) and intent.get('unit')==unit and intent.get('action')==action and intent.get('nonce')==nonce,'host_switch_intent_changed')
 need(re.fullmatch('[a-f0-9]{32}',os.environ.get('INVOCATION_ID','')) is not None,'host_switch_requires_supervision')
 row=host.state_unit(unit);need(row.get('MainPID')==str(os.getpid()) and row.get('ControlGroup')=='/system.slice/'+unit,'host_switch_worker_not_owned');return unit

def verify_deployment_lock(fd):
 path=Path('/run/harbor-gateway-deploy.lock');expected=path.lstat();actual=os.fstat(fd)
 need(stat.S_ISREG(actual.st_mode) and actual.st_uid==0 and actual.st_nlink==1 and not actual.st_mode&0o022 and (actual.st_dev,actual.st_ino)==(expected.st_dev,expected.st_ino),'host_deployment_lock_untrusted')
 need(re.search(r'^lock:\s+\d+:\s+FLOCK\s+ADVISORY\s+WRITE\s+',Path('/proc/self/fdinfo',str(fd)).read_text(),re.M) is not None,'host_deployment_lock_not_held')

def deployment_begin(config,image,deployment_fd):
 host=Host();need(config==host.config and config.get('postgres_storage_startup_guard') is True,'host_factory_root_configuration_changed')
 with locks(host,deployment_fd):
  host.reload_locked_state();need(host.j.get('phase')=='complete' and host.j.get('runtimeControl',{}).get('mode','running')=='running' and not host.j.get('plannedOperation'),'host_runtime_operation_pending');host.load_current_durable_active();host.data_mounts();host.require_docker_restart_disabled()
  before=host.upgrade.digest(host.upgrade.active_identity(host.active));ticket=host.upgrade.validate_ticket(host.upgrade.private_json(host.upgrade.TICKET),image,config);need(ticket['from']['activeIdentityDigest']==before,'host_planned_source_approval_changed');target=host.upgrade.approval_binding(host.upgrade.approval(ticket))
  nonce=uuid.uuid4().hex;host.j['plannedOperation']={'nonce':nonce,'runId':host.plan['runId'],'bootId':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'beforeActiveDigest':before,'requestedImage':image,'targetTicketDigest':host.upgrade.digest(ticket),'targetApproval':target};host.j['runtimeControl']={'mode':'planned'};host.save(host.j);return nonce

def deployment_finish(config,nonce,success,deployment_fd):
 need(type(success) is bool,'host_deployment_result_invalid');host=Host();need(config==host.config,'host_factory_root_configuration_changed')
 with locks(host,deployment_fd):
  host.reload_locked_state();need(host.j.get('plannedOperation',{}).get('nonce')==nonce,'host_planned_operation_changed')
  try:return host.reconcile_planned_operation(success)
  except (RuntimeError,OSError,ValueError,subprocess.TimeoutExpired):return {'released':False,'protectionRetained':True}

def supervise_online(host):
 stopped=False
 def stop(*args):
  nonlocal stopped
  stopped=True
 for name in [signal.SIGTERM,signal.SIGINT]:signal.signal(name,stop)
 handoff=host.j.get('phase')=='unit-handoff-intent' and host.j.get('handoff',{}).get('bootId')==Path('/proc/sys/kernel/random/boot_id').read_text().strip()
 if handoff:host.validate_handoff('boot-online')
 else:
  # After a supervisor crash, readiness means the supervisor is installed, not
  # that unhealthy applications already satisfy their own recovery conditions.
  if host.j.get('phase')=='boot-containers-ready':
   with locks(host):host.reload_locked_state();host.core.Coordinator(host,host.j).boot_online()
  else:host.signal_online_ready()
 while not stopped:
  try:
   with locks(host):host.reload_locked_state();host.runtime_step()
  except BlockingIOError:pass
  except (RuntimeError,OSError,ValueError,subprocess.TimeoutExpired):print(json.dumps({'supervision':'protected','error':'host_runtime_reconcile_failed'}),flush=True)
  if not stopped:time.sleep(host.poll_interval_seconds)
 return {'supervisorStopped':True}

def main():
 global _EMIT_PROGRESS
 actions=['collect-readonly','precopy','launch-switch','launch-recover-source','launch-recover-target','switch','recover-source','recover-target','boot-start','boot-online','pause','resume','reconcile','status']
 need(len(sys.argv) in [2,3] and sys.argv[1] in actions and (len(sys.argv)==3)==(sys.argv[1] in ['recover-source','recover-target']),'host_action_invalid')
 action=sys.argv[1]
 if action=='collect-readonly':return collect_readonly()
 host=Host();_EMIT_PROGRESS=action in ['precopy','switch','recover-source','recover-target','boot-start','boot-online','pause','resume']
 if action.startswith('launch-'):return launch_switch(host,action.removeprefix('launch-'))
 if action=='boot-online':return supervise_online(host)
 worker_unit=None
 if action in ['switch','recover-source','recover-target']:worker_unit=require_switch_worker(host,action,sys.argv[2] if len(sys.argv)==3 else None)
 if action=='status':return {'phase':host.j['phase'],'runtimeMode':host.j.get('runtimeControl',{}).get('mode','running'),'oldDataRollbackForbidden':host.j.get('oldDataRollbackForbidden',False)}
 if action=='boot-start' and host.j.get('phase')=='unit-handoff-intent' and host.j.get('handoff',{}).get('bootId')==Path('/proc/sys/kernel/random/boot_id').read_text().strip():return host.validate_handoff(action)
 with locks(host):
  host.reload_locked_state()
  if worker_unit is not None:
   if action in ['recover-source','recover-target']:host.core.recovery_mode(host.j)
   need(require_switch_worker(host,action,sys.argv[2] if len(sys.argv)==3 else None)==worker_unit,'host_switch_worker_changed');host.j['workerUnit']=worker_unit;host.save(host.j)
  coordinator=host.core.Coordinator(host,host.j)
  if action=='precopy':return host.precopy()
  if action=='switch':return coordinator.switch()
  if action=='recover-source':return coordinator.recover_source()
  if action=='recover-target':return coordinator.recover_target()
  if action=='boot-start':return coordinator.boot_start()
  if action=='pause':return host.pause_runtime()
  if action=='reconcile':return host.reconcile_planned_operation()
  result=host.prepare_runtime_resume()
 if result.get('bootstrap'):
  # Release the instance locks before systemd enters the ordinary boot chain.
  run(['/usr/bin/systemctl','restart',host.core.STORAGE_START,'harbor-entry-candidate.service','harbor-gateway-entry.service',host.core.STORAGE_ONLINE],timeout=180)
  return {'resumeSubmitted':True,'ordinaryBootChain':True}
 return result
if __name__=='__main__':
 try:print(json.dumps(main()))
 except BaseException as error:
  code=str(error) if isinstance(error,RuntimeError) and re.fullmatch('(?:host|storage)_[a-z0-9_]+',str(error)) else 'storage_move_failed_closed'
  print(json.dumps({'failed':True,'error':code,'inspectOriginalUnitBeforeRetry':True}));raise SystemExit(1)
