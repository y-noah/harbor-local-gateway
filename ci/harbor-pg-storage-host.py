"""Single Harbor instance adapter, prepared offline. No import-time operations.
No credentials, arbitrary paths/images, activation creation or global Docker edits.
"""
import contextlib,copy,ctypes,fcntl,hashlib,http.client,importlib.machinery,importlib.util,json,os,re,socket,signal,stat,subprocess,sys,time,uuid
from urllib.parse import urlsplit,parse_qs
from datetime import datetime
from pathlib import Path
CORE=Path(__file__).with_name('harbor-pg-storage-coordinator.py')
ROOT=Path('/sdc-disk/harbor-data');PLAN=ROOT/'.pg-storage-plan.json';STATE=ROOT/'.pg-storage-state.json';PROOF=ROOT/'.pg-storage-archive-proof.json'
PG='harbor-local-gateway-postgres-infra';GW='harbor-local-gateway';WORKER=GW+'-worker'
DEPLOY=Path('/usr/local/bin/harbor-gateway-deploy');ENTRY=Path('/usr/local/lib/harbor/gateway-entry-controller.py');UPGRADE=Path('/usr/local/lib/harbor/postgres-upgrade-deploy.py');COMPOSE=Path('/usr/local/lib/harbor/postgres-deploy.py')
ARCHIVE_COPY_LOCK=Path('/run/harbor-pg-archive-copy.lock')
RUNTIME=Path('/usr/local/lib/harbor/entry-runtime.py')
MATERIAL=Path('/usr/local/lib/harbor/harbor-material-storage.py')
MATERIAL_MOVE=Path('/usr/local/lib/harbor/harbor-material-move.py')
HOST_BRIDGE_APPROVAL=Path('/etc/harbor-pg-storage-host-bridge.json')
HOST_GENERATION=Path('/etc/harbor-pg-storage-host-generation.json')
RUNTIME_MANIFEST=Path('/etc/harbor-postgres-runtime-tools.json')
RUNTIME_TICKET=Path('/etc/harbor-postgres-runtime-upgrade.json')
MUTABLE_TOOLS=frozenset(map(str,[DEPLOY,ENTRY,UPGRADE,COMPOSE]))
RUNTIME_TOOLS=MUTABLE_TOOLS|{str(MATERIAL),'/usr/local/lib/harbor/postgres-usage-cascade.py','/usr/local/lib/harbor/postgres-department-release.py'}
_EMIT_PROGRESS=False
_FAILURE_STAGE='startup'

def diagnostic_stage(name):
 global _FAILURE_STAGE
 need(name in {'startup','hold_ticket','hold_tools','hold_source','hold_receipt','hold_drain','early_source_verify','early_source_resume','source_checkpoint','source_restart_policies','source_postgres_start','source_postgres_verify','source_applications_start','source_readiness','source_network','source_full_verify','source_entry_refresh','source_entry_verify'},'host_diagnostic_stage_invalid')
 _FAILURE_STAGE=name

def rejected_control(error):
 detail=getattr(error,'diagnostic',None)
 return type(error).__name__=='EntryControlError' and isinstance(detail,dict) and detail.get('reason') in {'entry_epoch_or_operation_conflict','entry_phase_conflict','entry_stopping'}

def failure_summary(error):
 kind=type(error).__name__
 if kind not in {'RuntimeError','EntryControlError','TypeError','KeyError','ValueError','OSError','FileNotFoundError','PermissionError','TimeoutError','TimeoutExpired','JSONDecodeError','HTTPException','RemoteDisconnected','ConnectionResetError','BrokenPipeError','SystemExit','KeyboardInterrupt'}:kind='OtherError'
 code=str(error) if isinstance(error,RuntimeError) and re.fullmatch('(?:host|storage)_[a-z0-9_]+',str(error)) else 'storage_move_failed_closed'
 value={'error':code,'stage':_FAILURE_STAGE,'exceptionType':kind}
 # Revalidate the entry helper's bounded projection at this final log boundary.
 if kind=='EntryControlError':
  detail=getattr(error,'diagnostic',None);safe={}
  if isinstance(detail,dict):
   enums={'reason':{'gateway_entry_drain_timeout','entry_epoch_or_operation_conflict','entry_phase_conflict','entry_stopping','entry_operation_failed'},'phase':{'active','holding','drained','closing','closed'},'slot':{'legacy','candidate'}}
   for key,allowed in enums.items():
    if isinstance(detail.get(key),str) and detail[key] in allowed:safe[key]=detail[key]
   for key in ['epoch','inflight','queued']:
    item=detail.get(key)
    if type(item) is int and 0<=item<=9007199254740991:safe[key]=item
  if safe:value['entryControl']=safe
 reasons={'entry drain timeout':'entry_drain_timeout','entry drain failed':'entry_drain_failed','entry interrupt listener configuration':'entry_interrupt_listener_configuration','entry TCP socket ownership unknown':'entry_tcp_socket_ownership_unknown','entry TCP destroy unsupported or unconfirmed':'entry_tcp_destroy_unconfirmed','entry TCP inventory unavailable':'entry_tcp_inventory_unavailable','entry interrupt process changed':'entry_interrupt_process_changed','entry interrupt container changed':'entry_interrupt_container_changed','entry interrupt namespace changed':'entry_interrupt_namespace_changed','entry interrupt hold changed':'entry_interrupt_hold_changed','entry interrupt hold CAS':'entry_interrupt_hold_cas','held entry generation changed':'entry_held_generation_changed'}
 if isinstance(error,RuntimeError):
  for original,label in reasons.items():
   if str(error)=='PostgreSQL runtime upgrade: '+original:value['upgradeReason']=label;break
 return value


BACKUP=['/opt/harbor-storage-tools/node/bin/node','/opt/harbor-storage-tools/app/storage-backup.mjs']

# Source-recovery qualification only; identical to the reviewed online bootstrap
# probe. Target deployment continues to require the ordinary full preflight.
SOURCE_RECOVERY_LOW_PROBE = r'''
import {readFile} from 'node:fs/promises';
import {scryptSync,timingSafeEqual} from 'node:crypto';
import {PostgresStore} from '/app/postgres-store.mjs';
import {workerStartupKind,workerStartupResult} from '/app/postgres-worker-startup.mjs';
let cookie='',password='',store,storage,qualified=false,timedOut=false,outputSent=false;
const workAbort=new AbortController();
const deadline=new Promise((_,reject)=>workAbort.signal.addEventListener('abort',()=>reject(Error('deadline')),{once:true}));
const workTimer=setTimeout(()=>{timedOut=true;workAbort.abort();},15000);
const hardTimer=setTimeout(()=>finish(false,1),20000);
function finish(value,code=0){if(outputSent)return;outputSent=true;clearTimeout(workTimer);clearTimeout(hardTimer);process.stdout.write(value?'true':'false',()=>process.exit(code));}
const bounded=async(operation,ms)=>{let timer;try{return await Promise.race([Promise.resolve().then(operation),new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('cleanup deadline')),ms);})]);}finally{clearTimeout(timer);}};
const request=async(path,options={},cleanup=false)=>fetch('http://127.0.0.1:8092'+path,{...options,redirect:'error',signal:cleanup?AbortSignal.timeout(2000):AbortSignal.any([workAbort.signal,AbortSignal.timeout(5000)])});
const low=value=>{
 if(!value||typeof value!=='object')return false;
 const b=value.binding??{},d=value.diagnostics??{},now=Date.now();
 return value.status==='low'&&value.captureAllowed===false&&b.source==='signed_database_report'&&b.verified===true&&b.reason===null&&d.reason==='low'&&d.probeFailure===null&&Number.isSafeInteger(b.expires_at)&&b.expires_at>now&&Number.isSafeInteger(value.checked)&&now-value.checked>=0&&now-value.checked<=5000;
};
try{await Promise.race([deadline,(async()=>{
 let input='';for await(const part of process.stdin){input+=part;if(input.length>1024)throw Error('input');}
 const {since}=JSON.parse(input);if(!Number.isSafeInteger(since)||since<0||since>Date.now())throw Error('since');
 const settings=JSON.parse(await readFile('/app/data/settings.json','utf8')),expected=settings.adminPasswordHash;
 password=(await readFile('/app/data/管理密码.txt','utf8')).split(/\r?\n/).map(v=>v.trim()).filter(Boolean)[1];
 if(typeof password!=='string'||!expected||typeof expected.salt!=='string'||! /^[a-f0-9]{64}$/.test(expected.digest))throw Error('credential');
 if(!timingSafeEqual(scryptSync(password,expected.salt,32,{N:16384,r:8,p:1}),Buffer.from(expected.digest,'hex')))throw Error('credential');
 if((await request('/ready')).status!==200)throw Error('ready');
 const login=await request('/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({password})});password='';
 if(login.status!==200)throw Error('login');cookie=login.headers.get('set-cookie')?.split(';')[0]??'';if(!cookie)throw Error('cookie');await login.arrayBuffer();
 store=new PostgresStore({connectionString:process.env.HARBOR_DATABASE_URL,connection:{query_timeout:2500},connectionTimeoutMs:2500,statementTimeoutMs:2500,poolSize:1});
 const row=await store.transaction(()=>store.get("SELECT EXISTS(SELECT 1 FROM harbor.background_jobs WHERE kind=$1 AND status='completed' AND created>=$2 AND claimed_at>=$2 AND updated>=$2 AND result=$3::jsonb) progress",workerStartupKind,since,JSON.stringify(workerStartupResult)),{readOnly:true});
 if(row.progress!==true)throw Error('startup');
 const response=await request('/api/admin/status',{headers:{Cookie:cookie}});if(response.status!==200)throw Error('status');
 const body=await response.text();if(body.length>2097152)throw Error('budget');
 storage=JSON.parse(body).recordingStorage;qualified=low(storage)&&(await request('/ready')).status===200;
})()]);}catch{qualified=false;}finally{
 password='';clearTimeout(workTimer);workAbort.abort();
 // Logout must not wait behind a stuck database close or transaction rollback.
 await Promise.all([
  bounded(async()=>{if(cookie){const out=await request('/api/auth/logout',{method:'POST',headers:{'content-type':'application/json',Cookie:cookie},body:'{}'},true);await out.arrayBuffer();if(out.status!==200)throw Error('logout');}},2500).catch(()=>{qualified=false;}),
  bounded(()=>store?.close(),2500).catch(()=>{qualified=false;})
 ]);cookie='';
}
finish(qualified&&!timedOut&&low(storage));
'''

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

def rename_noreplace(source,target):
 # Linux same-filesystem atomic publication must never replace a foreign root,
 # including an empty directory appearing after the preflight stat.
 libc=ctypes.CDLL(None,use_errno=True);rename=getattr(libc,'renameat2',None)
 need(rename is not None,'host_archive_adopt_atomic_rename_unavailable')
 rename.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_int,ctypes.c_char_p,ctypes.c_uint];rename.restype=ctypes.c_int
 if rename(-100,os.fsencode(source),-100,os.fsencode(target),1)!=0:
  code=ctypes.get_errno();raise OSError(code,os.strerror(code))


def load(path,name):
 for p in [path,*path.parents]:
  s=p.lstat();need(s.st_uid==0 and not s.st_mode&0o022 and not stat.S_ISLNK(s.st_mode),'host_tool_untrusted')
 loader=importlib.machinery.SourceFileLoader(name,str(path));spec=importlib.util.spec_from_loader(name,loader);module=importlib.util.module_from_spec(spec);loader.exec_module(module);return module

def run(args,timeout=60):
 p=subprocess.run(args,stdin=subprocess.DEVNULL,capture_output=True,timeout=timeout)
 # Raw child stderr is never serialized. A fixed stage code is the public boundary.
 need(p.returncode==0,'host_command_failed');return p.stdout

def sig(row):
 return sha(json.dumps({'Id':row.get('Id'),'Image':row.get('Image'),'Config':row.get('Config'),'HostConfig':{k:v for k,v in row.get('HostConfig',{}).items() if k!='RestartPolicy'},'Mounts':sorted(copy.deepcopy(row.get('Mounts',[])),key=lambda value:json.dumps(value,sort_keys=True,separators=(',',':')))},sort_keys=True,separators=(',',':')).encode())

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

def code_bytes(path):
 path=Path(path)
 for parent in path.parents:
  info=parent.lstat();need(stat.S_ISDIR(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022,'host_tool_untrusted')
 fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
 with os.fdopen(fd,'rb') as stream:
  info=os.fstat(stream.fileno());need(stat.S_ISREG(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022 and info.st_nlink==1 and info.st_size<=1048576,'host_tool_untrusted');raw=stream.read(1048577)
  after=path.lstat();need(len(raw)==info.st_size and (info.st_dev,info.st_ino,info.st_mtime_ns,info.st_ctime_ns)==(after.st_dev,after.st_ino,after.st_mtime_ns,after.st_ctime_ns),'host_tool_changed')
 return raw

def object_digest(value):return sha(json.dumps(value,sort_keys=True,separators=(',',':')).encode())

def completed_move(plan,journal):
 if journal.get('scope')!=plan['scope'] or journal.get('runId')!=plan['runId'] or journal.get('phase')!='complete' or journal.get('oldDataRollbackForbidden') is not True:return False
 selected=plan.get('materialMove')
 return selected is None or journal.get('materialMove')==dict(selected,phase='complete')

def bridge_roots(plan):
 def identity(path):
  info=Path(path).lstat();need(stat.S_ISDIR(info.st_mode),'host_bridge_root_invalid');return [info.st_dev,info.st_ino,info.st_uid,info.st_gid,stat.S_IMODE(info.st_mode)]
 base=Path('/opt/product-development/harbor-local-gateway/postgres-infra')
 value={'systemId':plan['systemId'],'pgContainer':plan['containers'][PG],'db':identity(base/'db'),'archive':identity(base/'archive')}
 if plan.get('materialMove') is not None:
  raw=private(Path('/etc/harbor-data-paths.json'));config=json.loads(raw)
  value['materialConfigSha256']=sha(raw);value['materialRoots']={name:{side:identity(row[side]) for side in ['logical','target']} for name,row in config['roots'].items()}
 return value

def verify_host_bridge(plan,plan_sha,journal,actual):
 receipt=json.loads(private(HOST_GENERATION));approval_raw=private(HOST_BRIDGE_APPROVAL);approval=json.loads(approval_raw)
 keys={'version','approved','id','planSha256','runId','oldHostSha256','newHostSha256','installerSha256','revision','pipeline','artifactJob'}
 need(set(approval)==keys and approval.get('version')==1 and approval.get('approved') is True and str(uuid.UUID(approval['id']))==approval['id'],'host_bridge_approval_invalid')
 need(set(receipt)=={'contract','approvalSha256','binding','migrationCompleted','roots'} and receipt['contract']=='harbor-pg-storage-host-generation-v1' and receipt['migrationCompleted'] is True and receipt['approvalSha256']==sha(approval_raw) and receipt['binding']=={k:approval[k] for k in keys-{'approved'}},'host_bridge_receipt_changed')
 historical=journal.get('scope')==plan['scope'] and journal.get('runId')==plan['runId'] and journal.get('oldDataRollbackForbidden') is True and journal.get('targetBootAuthorized') is True and journal.get('phase') in ['complete','boot-start-intent','boot-containers-ready','runtime-recovering','runtime-entry-pending']
 selected=plan.get('materialMove');need(selected is None or journal.get('materialMove')==dict(selected,phase='complete'),'host_material_completion_missing')
 need(historical and approval['planSha256']==plan_sha and approval['runId']==plan['runId'] and approval['oldHostSha256']==plan['toolSha256'][str(Path(__file__))] and approval['newHostSha256']==actual,'host_bridge_scope_changed')
 need(receipt['roots']==bridge_roots(plan),'host_bridge_roots_changed')
 need(all(isinstance(approval[k],str) and re.fullmatch('[a-f0-9]{64}',approval[k]) for k in ['planSha256','oldHostSha256','newHostSha256','installerSha256']) and re.fullmatch('[a-f0-9]{40}',approval['revision']) and all(type(approval[k]) is int and approval[k]>0 for k in ['pipeline','artifactJob']),'host_bridge_ci_identity_invalid')
 return receipt

def verified_runtime_tools(plan,journal):
 # This runs before importing any replaceable module. Root approval is the
 # existing trust boundary; a root-owned executable alone is not approval.
 if not completed_move(plan,journal):verify_host_bridge(plan,sha(private(PLAN,262144)),journal,sha(code_bytes(Path(__file__))))
 manifest=json.loads(private(RUNTIME_MANIFEST));ticket=json.loads(private(RUNTIME_TICKET))
 need(set(manifest)=={'version','contract','revision','files'} and type(manifest['version']) is int and manifest['version']==1 and manifest['contract']=='harbor-postgres-runtime-tools-v1' and set(manifest['files'])==RUNTIME_TOOLS,'host_runtime_manifest_invalid')
 need(ticket.get('version')==1 and ticket.get('contract')=='harbor-postgres-runtime-upgrade-v1' and ticket.get('approved') is True and re.fullmatch('[a-f0-9]{40}',manifest.get('revision','')) and manifest['revision']==ticket.get('hostToolsRevision'),'host_runtime_revision_unapproved')
 for name,digest in manifest['files'].items():
  need(isinstance(digest,str) and re.fullmatch('[a-f0-9]{64}',digest) and sha(code_bytes(name))==digest,'host_runtime_tool_changed')
 need(manifest['files'][str(MATERIAL)]==plan['toolSha256'][str(MATERIAL)],'host_material_tool_changed')
 active=json.loads(private(Path('/var/lib/harbor-entry/active.json')))
 authorization=active.get('approval',{});identifier=authorization.get('deploymentId','')
 need(re.fullmatch('[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}',identifier),'host_runtime_active_invalid')
 record=json.loads(private(Path('/var/lib/harbor-postgres-upgrades')/identifier/'release.json'));previous=record.get('ticket',{})
 target=previous.get('to',{});previous_digest=object_digest(previous)
 identity={k:copy.deepcopy(active.get(k)) for k in ['version','image','approval','configDigest','mounts','storageMarkerDigest','storageMarkerPath']}|{'candidate':{k:active.get('candidate',{}).get(k) for k in ['containerId','imageId','port']}}
 control=journal.get('runtimeControl',{});planned=journal.get('plannedOperation',{}).get('targetTicketDigest')==previous_digest and control.get('mode') in ['planned','recovering','paused']
 recovering=control.get('mode') in ['recovering','paused'] and control.get('selectedActiveDigest')==object_digest(identity)
 pending=record.get('phase') in ['committing','committed'] and (planned or recovering)
 need((record.get('phase')=='complete' or pending) and record.get('contract')=='harbor-postgres-runtime-upgrade-v1' and record.get('ticketDigest')==previous_digest and previous.get('deploymentId')==identifier and authorization.get('receiptDigest')==previous_digest and authorization.get('contract')==previous.get('contract'),'host_runtime_record_unapproved')
 need(all(authorization.get(k)==target.get(k) for k in ['revision','configDigest','manifestDigest']) and active.get('image')==target.get('image') and previous.get('database',{}).get('cluster')==plan['systemId'],'host_runtime_active_changed')
 candidate=active.get('candidate',{});recorded=record.get('candidate',{})
 need(recorded.get('containerId')==candidate.get('containerId') and recorded.get('imageId')==candidate.get('imageId') and candidate.get('imageId') in [target.get('configDigest'),target.get('manifestDigest')],'host_runtime_candidate_unapproved')
 source=ticket.get('from',{});to=ticket.get('to',{})
 from_active=source.get('activeIdentityDigest')==object_digest(identity) and source.get('containerId')==active.get('candidate',{}).get('containerId') and source.get('workerId')==record.get('workerId') and source.get('imageId')==active.get('candidate',{}).get('imageId') and source.get('image')==active.get('image')
 to_active=object_digest(ticket)==previous_digest and to==target
 need(from_active or to_active,'host_runtime_ticket_not_current')
 need(ticket.get('materialsDirectory')==previous.get('materialsDirectory')==plan['materialsDirectory'] and ticket.get('database',{}).get('cluster')==plan['systemId'],'host_runtime_materials_changed')
 for name,identifier,role in [(GW,active['candidate']['containerId'],'gateway'),(WORKER,record['workerId'],'worker')]:
  rows=json.loads(run(['/usr/bin/docker','container','inspect',name]));need(len(rows)==1,'host_runtime_pair_ambiguous');row=rows[0];config=row.get('Config',{});labels=config.get('Labels',{})
  need(row.get('Id')==identifier and row.get('Image')==active['candidate']['imageId'] and config.get('Image')==active['image'] and config.get('Cmd')==['node','/app/postgres-'+role+'.mjs'] and labels.get('com.aistrong.project')==GW and labels.get('com.harbor.activation-deployment-id')==authorization['deploymentId'] and labels.get('com.harbor.activation-receipt')==authorization['receiptDigest'],'host_runtime_pair_changed')
 return {name:manifest['files'][name] for name in MUTABLE_TOOLS}

class Host:
 def __init__(self):
  need(os.geteuid()==0,'host_root_required');raw=private(PLAN,262144);self.planSha256=sha(raw);self.plan=json.loads(raw);p=self.plan
  need(p.get('scope')=='harbor-pg-storage-move-v1' and p.get('approved') is True and re.fullmatch('[a-f0-9]{32}',p.get('runId','')),'host_plan_unapproved')
  need(p.get('tlsVerifyFull') is True and re.fullmatch('[0-9]{1,20}',p.get('systemId','')),'host_database_identity_missing')
  need(set(p.get('containers',{}))=={PG,GW,WORKER} and all(re.fullmatch('[a-f0-9]{64}',v.get('id','')) and re.fullmatch('[a-f0-9]{64}',v.get('signature','')) for v in p['containers'].values()),'host_inventory_missing')
  expected={str(v) for v in [CORE,Path(__file__),DEPLOY,ENTRY,UPGRADE,COMPOSE,RUNTIME,MATERIAL]}
  material=p.get('materialMove');need(material is None or isinstance(material,dict) and set(material)=={'approvalSha256','nonce'} and re.fullmatch('[a-f0-9]{64}',material.get('approvalSha256','')) and str(uuid.UUID(material.get('nonce','')))==material['nonce'],'host_material_plan_invalid')
  if material is not None:expected.add(str(MATERIAL_MOVE))
  need(set(p.get('toolSha256',{}))==expected,'host_tool_inventory_missing')
  self.j=json.loads(private(STATE)) if STATE.exists() else {'scope':p['scope'],'runId':p['runId'],'phase':'planned'}
  need(self.j.get('scope')==p['scope'] and self.j.get('runId')==p['runId'],'host_state_scope_changed')
  self.runtimeTools=None
  if completed_move(p,self.j) or os.path.lexists(HOST_GENERATION):self.runtimeTools=verified_runtime_tools(p,self.j)
  for name,digest in p['toolSha256'].items():
   need(isinstance(digest,str) and re.fullmatch('[a-f0-9]{64}',digest),'host_tool_changed');actual=sha(code_bytes(name))
   if actual==digest:continue
   if name==str(Path(__file__)):verify_host_bridge(p,self.planSha256,self.j,actual)
   else:need(self.runtimeTools is not None and name in MUTABLE_TOOLS and self.runtimeTools[name]==actual,'host_tool_changed')
  self.core=load(CORE,'harbor_pg_storage_core')
  self.wrapper=load(DEPLOY,'harbor_storage_wrapper');self.entry=load(ENTRY,'harbor_storage_entry');self.upgrade=load(UPGRADE,'harbor_storage_upgrade');self.pg=load(COMPOSE,'harbor_storage_compose');self.runtime=load(RUNTIME,'harbor_storage_entry_runtime')
  need(getattr(self.pg,'STORAGE_STARTUP_CONTRACT',None)=='harbor-pg-storage-boot-v1','host_factory_guard_not_installed')
  self.config=self.entry.private_json(Path('/etc/harbor-gateway-deploy.json'));self.poll_interval_seconds=supervision_interval(self.config);self.j=json.loads(private(STATE)) if STATE.exists() else {'scope':p['scope'],'runId':p['runId'],'phase':'planned'}
  need(self.j.get('scope')==p['scope'] and self.j.get('runId')==p['runId'],'host_state_scope_changed');self.api={**vars(self.wrapper),'STACK':p.get('materialsDirectory')};self.gw=None;self.worker=None;self.active=None;self.ticket=None
  if self.runtimeTools is not None:
   ticket=self.upgrade.private_json(RUNTIME_TICKET);self.upgrade.validate_ticket(ticket,ticket['to']['image'],self.config);self.upgrade.verify_installed_tools(ticket);self.upgrade.record_for(self.entry.private_json(self.entry.ACTIVE)['approval'])
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
 def data_mounts(self,*,verify_material=True):
  self.target_disk()
  if verify_material:self.pg.material_storage_guard(self.plan['materialsDirectory'])
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
 def source_pair_processes(self):
  result={}
  for name in [GW,WORKER]:
   row=self.immutable(name);state=row['State'];need(state.get('Running') is True and type(state.get('Pid')) is int and state['Pid']>0 and isinstance(state.get('StartedAt'),str) and state['StartedAt'] and not any(state.get(k) for k in ['Paused','Restarting','Dead','OOMKilled','Error']) and type(row.get('RestartCount')) is int and row['RestartCount']>=0,'host_source_pair_not_running')
   result[name]={'id':row['Id'],'signature':sig(row),'pid':state['Pid'],'startedAt':state['StartedAt'],'restarts':row.get('RestartCount'),'networkDigest':container_generation(row)['networkDigest']}
  return result
 def probe_source_storage_low(self,gateway,worker,*,required=True,timeout=30):
  since=int(datetime.fromisoformat(worker['State']['StartedAt'].replace('Z','+00:00')).timestamp()*1000)
  result=subprocess.run(['/usr/bin/docker','--host','unix:///var/run/docker.sock','exec','--interactive','--env','NODE_OPTIONS=',gateway['Id'],'node','--input-type=module','--eval',SOURCE_RECOVERY_LOW_PROBE],input=json.dumps({'since':since}).encode(),capture_output=True,timeout=timeout,env={'PATH':'/usr/bin:/bin','HOME':'/root'})
  qualified=result.returncode==0 and result.stdout==b'true' and result.stderr==b''
  if required:need(qualified,'host_source_storage_low_unproven')
  return qualified
 def observe_source_recovery_baseline(self):
  if self.plan.get('materialMove') is None:return
  # This is a live observation before hold/stop, never an opt-in PLAN boolean.
  before=self.source_pair_processes();gateway=self.immutable(GW);worker=self.immutable(WORKER)
  need(gateway['State'].get('Health',{}).get('Status')=='healthy','host_source_gateway_not_healthy');health=worker['State'].get('Health',{}).get('Status');need(health in ['healthy','unhealthy'],'host_source_worker_health_unknown')
  kind='healthy'
  if health=='unhealthy':self.probe_source_storage_low(gateway,worker);kind='storage-low'
  need(self.source_pair_processes()==before,'host_source_baseline_generation_changed')
  baseline={'version':1,'kind':kind,'planSha256':self.planSha256,'activeIdentityDigest':self.plan['activeIdentityDigest'],'originalProcesses':before}
  if 'sourceRecoveryBaseline' in self.j:need(self.j['sourceRecoveryBaseline']==baseline,'host_source_baseline_changed')
  else:self.j['sourceRecoveryBaseline']=baseline;self.save(self.j)
 def source_low_recovery_selected(self):
  baseline=self.j.get('sourceRecoveryBaseline')
  if baseline is None:return False
  need(self.plan.get('materialMove') is not None and isinstance(baseline,dict) and type(baseline.get('version')) is int and baseline.get('version')==1 and baseline.get('kind') in ['healthy','storage-low'] and baseline.get('planSha256')==self.planSha256 and baseline.get('activeIdentityDigest')==self.plan['activeIdentityDigest'] and isinstance(baseline.get('originalProcesses'),dict) and set(baseline['originalProcesses'])=={GW,WORKER},'host_source_baseline_unproven')
  for name in [GW,WORKER]:
   original=baseline['originalProcesses'][name];need(original.get('id')==self.plan['containers'][name]['id'] and original.get('signature')==self.plan['containers'][name]['signature'] and type(original.get('pid')) is int and original['pid']>0 and isinstance(original.get('startedAt'),str) and original['startedAt'] and type(original.get('restarts')) is int and original['restarts']>=0,'host_source_baseline_unproven')
  return baseline['kind']=='storage-low'
 def managed_source_recovery(self):
  return self.plan.get('materialMove') is not None and self.j.get('sourceRecoveryBaseline') is not None
 def source_recovery_network(self):
  snapshot=self.upgrade.network_snapshot(self.ticket,self.entry,self.immutable(GW),self.immutable(WORKER));snapshot['candidateNetworkContainerIds']=sorted(snapshot['candidateNetworkContainerIds']);return snapshot
 def verify_source_recovery_processes(self):
  self.verify_source_recovery_materials();self.source_low_recovery_selected();current=self.source_pair_processes();baseline=self.j['sourceRecoveryBaseline']
  need(current==self.j.get('sourceRecoveryProcesses') and self.source_recovery_network()==self.j.get('sourceRecoveryNetwork') and all(current[name]['restarts']==baseline['originalProcesses'][name].get('restarts') for name in [GW,WORKER]),'host_source_recovery_generation_changed')
  worker=self.immutable(WORKER);need(self.j.get('workerGeneration')=={'id':worker['Id'],'startedAt':worker['State']['StartedAt']} and self.j.get('applicationsStartEpochMs')==int(datetime.fromisoformat(worker['State']['StartedAt'].replace('Z','+00:00')).timestamp()*1000),'host_source_recovery_worker_generation_changed')
  return current
 def wait_source_recovery_ready(self):
  deadline=getattr(self,'source_recovery_deadline',None);need(isinstance(deadline,(float,int)),'host_source_recovery_deadline_missing');low_allowed=self.source_low_recovery_selected()
  while True:
   need(time.monotonic()<deadline,'host_source_recovery_not_ready');before=self.verify_source_recovery_processes();gateway=self.immutable(GW);worker=self.immutable(WORKER);health=[row['State'].get('Health',{}).get('Status') for row in [gateway,worker]];need(all(value in ['starting','healthy','unhealthy'] for value in health),'host_source_recovery_health_unknown');mode=None
   if health==['healthy','healthy']:
    strict=True
    for name in [GW,WORKER]:
     remaining=deadline-time.monotonic();need(remaining>0,'host_source_recovery_not_ready');args=['/usr/bin/docker','exec',self.row(name)['Id'],'node','/app/ci/postgres-preflight.mjs']
     if name==WORKER:args+=['--progress-since',str(self.j['applicationsStartEpochMs'])]
     try:reply=subprocess.run(args,stdin=subprocess.DEVNULL,capture_output=True,timeout=min(30,remaining));strict=strict and reply.returncode==0
     except subprocess.TimeoutExpired:strict=False
     need(time.monotonic()<deadline,'host_source_recovery_not_ready');self.verify_source_recovery_processes()
    if strict:mode='healthy'
   if mode is None and low_allowed and health[0]=='healthy' and health[1] in ['healthy','unhealthy']:
    remaining=deadline-time.monotonic();need(remaining>0,'host_source_recovery_not_ready')
    try:
     if self.probe_source_storage_low(gateway,worker,required=False,timeout=min(30,remaining)):mode='storage-low'
    except (OSError,subprocess.SubprocessError):pass
   need(time.monotonic()<deadline,'host_source_recovery_not_ready');need(self.verify_source_recovery_processes()==before,'host_source_recovery_generation_changed')
   if mode is not None:
    status=self.upgrade.active_authorization(self.active['image']);need(status.get('state')=='active' and self.upgrade.approval_binding(status)==self.active['approval'],'host_database_approval_changed');need(time.monotonic()<deadline,'host_source_recovery_not_ready');return mode
   time.sleep(min(.2,max(0,deadline-time.monotonic())))
 def early_source_facts(self):
  # No active_record(): that helper republishes profiles and is not read-only.
  self.verify_source_recovery_materials();active=self.entry.private_json(self.entry.ACTIVE)
  need(self.upgrade.digest(self.upgrade.active_identity(active))==self.plan['activeIdentityDigest'],'host_early_active_changed')
  processes={}
  for name in [PG,GW,WORKER]:
   row=self.immutable(name);state=row['State']
   need(state.get('Running') is True and type(state.get('Pid')) is int and state['Pid']>0 and isinstance(state.get('StartedAt'),str) and state['StartedAt'] and type(row.get('RestartCount')) is int and not any(state.get(k) for k in ['Paused','Restarting','Dead','OOMKilled','Error']),'host_early_process_not_running')
   need(row['HostConfig']['RestartPolicy']==self.plan['containers'][name]['restartPolicy'],'host_early_restart_policy_changed')
   processes[name]={'id':row['Id'],'signature':sig(row),'pid':state['Pid'],'startedAt':state['StartedAt'],'restarts':row['RestartCount'],'networkDigest':container_generation(row)['networkDigest']}
  network=self.upgrade.network_snapshot(self.ticket,self.entry,self.immutable(GW),self.immutable(WORKER));network['candidateNetworkContainerIds']=sorted(network['candidateNetworkContainerIds'])
  entry_state=self.entry.private_json(self.entry.STATE);view=entry_state.get('activeRuntimeConfig')
  need(entry_state.get('phase')=='activated' and isinstance(view,dict) and entry_state.get('candidate')==active.get('candidate'),'host_early_entry_rules_unknown');self.entry.verify_resources(entry_state,view)
  runtime=self.runtime.inspect_entry(self.entry);pid=runtime.get('State',{}).get('Pid')
  need(type(pid) is int and pid>1 and runtime['State'].get('Running') is True and not any(runtime['State'].get(k) for k in ['Paused','Restarting','Dead']),'host_early_entry_not_running')
  need(self.entry.socket_owner(Path(self.entry.CONTROL))==pid,'host_early_entry_owner_changed')
  return {'activeDigest':self.upgrade.digest(active),'candidate':active['candidate'],'processes':processes,'network':network,'entryStateDigest':self.upgrade.digest(entry_state),'entry':{'id':runtime['Id'],'pid':pid,'startedAt':runtime['State']['StartedAt'],'ticks':self.entry.process_ticks(pid),'cgroup':self.entry.process_cgroup(pid)},'bootId':Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
 def early_source_window(self):
  return self.j.get('phase') in ['holding-intent','held'] and not any(key in self.j for key in ['gate','gateIntent','writersPauseStarted','restartPolicies','autostartFenced','oldRoots','mountpointIdentities']) and not self.j.get('oldDataRollbackForbidden',False) and not self.j.get('targetBootAuthorized',False)
 def capture_early_hold(self):
  diagnostic_stage('hold_receipt');need(self.early_source_window(),'host_early_hold_window_changed')
  tables=json.loads(self.entry.execute(['/usr/sbin/nft','--json','list','tables']));need(not any(row.get('table',{}).get('name')==self.upgrade.GATE for row in tables.get('nftables',[])),'host_early_gate_present')
  control=self.entry.entry_state();need(control.get('slot')=='candidate' and control.get('phase')=='active' and type(control.get('epoch')) is int and control['epoch']>=0,'host_early_entry_not_active')
  facts=self.early_source_facts();current=self.entry.entry_state();need(all(current.get(k)==control.get(k) for k in ['slot','phase','epoch']),'host_early_entry_changed')
  receipt={'version':1,'planSha256':self.planSha256,'runId':self.plan['runId'],'source':facts,'activeEpoch':control['epoch'],'holdingEpoch':control['epoch']+1}
  need('earlyHoldRecovery' not in self.j or self.j['earlyHoldRecovery']==receipt,'host_early_receipt_changed');self.j['earlyHoldRecovery']=receipt;self.save(self.j)
 def drain_with_early_receipt(self,ticket=None):
  # Bind the actual hold CAS to the epoch recorded before admission changes.
  import types
  proxy=types.SimpleNamespace(**vars(self.entry));original=self.entry.entry_state
  def controlled(command=None,epoch=None,*args,**kwargs):
   receipt=self.j['earlyHoldRecovery']
   if command in ['hold','drain']:
    expected=receipt['activeEpoch'] if command=='hold' else receipt['holdingEpoch']
    need(epoch==expected,'host_early_hold_epoch_changed');receipt[command+'Requested']=True;self.save(self.j)
   try:return original(command,epoch,*args,**kwargs)
   except RuntimeError as error:
    # Only a protocol-defined rejection proves this CAS was not applied.
    if command in ['hold','drain'] and rejected_control(error):receipt[command+'Rejected']=True;self.save(self.j)
    raise
  proxy.entry_state=controlled;diagnostic_stage('hold_drain')
  if ticket is None:return self.upgrade.drain_active_entry(proxy)
  return self.upgrade.drain_active_entry(proxy,ticket,self.api,self.active)
 def recover_held_source(self):
  # Legacy journals and any progressed switch retain the full recovery path.
  receipt=self.j.get('earlyHoldRecovery')
  if receipt is None or not self.early_source_window():return None
  diagnostic_stage('early_source_verify')
  need(receipt.get('version')==1 and receipt.get('runId')==self.plan['runId'] and receipt.get('planSha256')==self.planSha256 and type(receipt.get('activeEpoch')) is int and receipt.get('holdingEpoch')==receipt['activeEpoch']+1,'host_early_receipt_invalid')
  # A completed drain belongs to full recovery, which can safely restart an
  # otherwise verified source. Do not apply hold-only health/generation gates.
  observed=self.entry.entry_state()
  if receipt.get('holdRequested') is True and not receipt.get('holdRejected',False) and receipt.get('drainRequested') is True and not receipt.get('drainRejected',False) and observed.get('slot')=='candidate' and observed.get('phase')=='drained' and observed.get('epoch')==receipt['holdingEpoch']+1:return None
  self.load_current_durable_active();need(self.early_source_facts()==receipt.get('source'),'host_early_source_changed')
  tables=json.loads(self.entry.execute(['/usr/sbin/nft','--json','list','tables']));need(not any(row.get('table',{}).get('name')==self.upgrade.GATE for row in tables.get('nftables',[])),'host_early_gate_present')
  # No login probe, WAL switch or database mutation in the hold-only path.
  # The source baseline was established before hold; verify the same processes
  # remain healthy, retaining its previously qualified worker storage-low case.
  health={name:self.immutable(name)['State'].get('Health',{}).get('Status') for name in [PG,GW,WORKER]}
  need(health[PG]=='healthy' and health[GW]=='healthy' and (health[WORKER]=='healthy' or health[WORKER]=='unhealthy' and self.source_low_recovery_selected()),'host_early_source_not_ready')
  control=self.entry.entry_state();need(control.get('slot')=='candidate','host_early_entry_changed')
  if control.get('phase')=='active' and control.get('epoch')==receipt['activeEpoch']:
   return {'sourceRestored':True,'holdOnly':True,'entryAlreadyActive':True,'servicesRestarted':False}
  need(receipt.get('holdRequested') is True and not receipt.get('holdRejected',False),'host_early_hold_unconfirmed')
  resume=receipt.get('resumeRequested')
  if resume=={'phase':'holding','epoch':receipt['holdingEpoch']} and not receipt.get('resumeRejected',False) and control.get('phase')=='active' and control.get('epoch')==receipt['holdingEpoch']+1:
   return {'sourceRestored':True,'holdOnly':True,'resumeAcknowledgedByObservation':True,'servicesRestarted':False}
  need(control.get('phase')=='holding' and control.get('epoch')==receipt['holdingEpoch'],'host_early_entry_changed')
  need(self.early_source_facts()==receipt['source'],'host_early_source_changed');diagnostic_stage('early_source_resume')
  receipt['resumeRequested']={'phase':'holding','epoch':receipt['holdingEpoch']};self.save(self.j)
  try:resumed=self.entry.entry_state('resume',receipt['holdingEpoch'])
  except RuntimeError as error:
   if rejected_control(error):receipt['resumeRejected']=True;self.save(self.j)
   raise
  need(resumed.get('slot')=='candidate' and resumed.get('phase')=='active' and resumed.get('epoch')==receipt['holdingEpoch']+1,'host_early_resume_unconfirmed')
  return {'sourceRestored':True,'holdOnly':True,'servicesRestarted':False}
 def hold_drain(self):
  diagnostic_stage('hold_ticket');expected=self.plan.get('interruptTicketSha256')
  if expected is None:
   self.capture_early_hold();return self.drain_with_early_receipt()
  need(isinstance(expected,str) and re.fullmatch('[a-f0-9]{64}',expected),'host_interrupt_ticket_unapproved')
  raw=private(self.upgrade.TICKET);need(sha(raw)==expected,'host_interrupt_ticket_changed');ticket=json.loads(raw)
  self.upgrade.validate_ticket(ticket,ticket.get('to',{}).get('image'),self.config);need(ticket.get('interruptEntryClients') is True,'host_interrupt_ticket_unapproved');diagnostic_stage('hold_tools');self.upgrade.verify_installed_tools(ticket)
  diagnostic_stage('hold_source');self.upgrade.verify_source(ticket,self.active,self.gw,self.worker)
  need(ticket.get('materialsDirectory')==self.plan['materialsDirectory'] and ticket.get('database',{}).get('cluster')==self.plan['systemId'],'host_interrupt_source_changed')
  self.capture_early_hold();return self.drain_with_early_receipt(ticket)
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
  diagnostic_stage('source_restart_policies')
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
 def material_mover(self):
  selected=self.plan.get('materialMove')
  if selected is None:return None
  for path in [MATERIAL_MOVE,MATERIAL]:need(sha(path.read_bytes())==self.plan['toolSha256'][str(path)],'host_material_tool_changed')
  mover=load(MATERIAL_MOVE,'harbor_unified_material_move');raw=private(mover.APPROVAL);need(sha(raw)==selected['approvalSha256'],'host_material_approval_changed')
  if getattr(self,'runtimeTools',None) is not None:
   # The immutable approval was validated during migration. Revalidate the
   # completed bindings without weakening the mover's strict migration API.
   value=json.loads(raw);need(set(value.get('guards',{}))==set(mover.GUARD_FILES),'host_material_guards_missing')
   for name,digest in value['guards'].items():
    current=sha(mover.guard_code(Path(name)))
    need(current==digest or name in MUTABLE_TOOLS and self.runtimeTools.get(name)==current,'host_material_guard_changed')
   need(set(value.get('tools',{}))=={str(MATERIAL_MOVE),str(MATERIAL)} and all(sha(code_bytes(name))==digest for name,digest in value['tools'].items()),'host_material_tool_changed')
   durable=mover.g.private_json(mover.STATE,33554432);need(durable.get('phase')=='complete' and durable.get('nonce')==selected['nonce'] and durable.get('approvalSha256')==selected['approvalSha256'],'host_material_completion_missing')
   need(mover.g.private_json(mover.g.CONFIG)==mover.final_configuration(value,durable),'host_material_configuration_changed');mover.g.guard(list(mover.g.TARGETS),required=True);rows=[]
  else:value,rows=mover.approval()
  need(value['nonce']==selected['nonce'] and value['materialsDirectory']==self.plan['materialsDirectory'] and value['diskUuid']==self.plan['targetFilesystemUuid'] and value['containers']=={name:self.plan['containers'][name]['id'] for name in [GW,WORKER]},'host_material_scope_changed')
  return mover,value,rows
 def prepare_application_materials(self):
  selected=self.material_mover()
  if selected is None:return
  need(self.j.get('targetBootAuthorized') is True and self.j.get('oldDataRollbackForbidden') is True,'host_material_target_not_armed')
  mover,value,rows=selected
  # The caller already owns the deploy/controller locks. Only the additional
  # material lock is acquired here; calling mover.main/locks would reenter them.
  fd=os.open('/run/harbor-material-move.lock',os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
  try:
   info=os.fstat(fd);need(stat.S_ISREG(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600 and info.st_nlink==1,'host_material_lock_untrusted');fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
   state=mover.g.private_json(mover.STATE,33554432) if os.path.lexists(mover.STATE) else None
   if state is not None:need(state.get('nonce')==value['nonce'] and state.get('approvalSha256')==self.plan['materialMove']['approvalSha256'],'host_material_state_changed')
   if state is not None and state.get('phase') in ['activation-ready','complete']:
    result=mover.switch(value,state)
   else:
    mover.quiescent(value);need(self.immutable(PG)['State']['Running'] is False,'host_material_postgres_running')
    lease=os.open(mover.ARCHIVE_LOCK,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
     info=os.fstat(lease);need(stat.S_ISREG(info.st_mode) and info.st_uid==999 and stat.S_IMODE(info.st_mode)==0o600 and info.st_nlink==1,'host_material_archive_lock_untrusted');fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
     receipt=mover.maintenance_receipt(value)
     if os.path.lexists(mover.g.MAINTENANCE):need(mover.g.private_json(mover.g.MAINTENANCE)==receipt,'host_material_maintenance_changed')
     else:
      need(state is None and not os.path.lexists(mover.g.CONFIG),'host_material_maintenance_missing');mover.save(mover.g.MAINTENANCE,receipt)
    finally:os.close(lease)
    if state is None or state.get('phase') in ['copying','copied']:
     mover.prepare(value,rows);state=mover.g.private_json(mover.STATE,33554432)
     need(state.get('nonce')==value['nonce'] and state.get('approvalSha256')==self.plan['materialMove']['approvalSha256'] and state.get('phase')=='copied','host_material_state_changed')
    result=mover.switch(value,state)
   durable=mover.g.private_json(mover.STATE,33554432);need(result.get('phase')=='complete' and durable.get('phase')=='complete' and durable.get('nonce')==value['nonce'] and durable.get('approvalSha256')==self.plan['materialMove']['approvalSha256'],'host_material_activation_incomplete');mover.g.guard(list(mover.g.TARGETS),required=True)
   self.j['materialMove']={'nonce':value['nonce'],'approvalSha256':self.plan['materialMove']['approvalSha256'],'phase':'complete'};self.save(self.j)
  finally:os.close(fd)
 def verify_application_material_mounts(self):
  selected=self.material_mover()
  if selected is None:return
  mover,value,_=selected;mover.g.guard(list(mover.g.TARGETS),required=True)
  receipt=self.j.get('materialMove');need(receipt=={'nonce':value['nonce'],'approvalSha256':self.plan['materialMove']['approvalSha256'],'phase':'complete'},'host_material_completion_missing')
  config=mover.g.private_json(mover.g.CONFIG);paths=[]
  for purpose,destination in [('application-data','/app/data'),('application-runtime','/app/runtime')]:
   root=config['roots'][purpose];logical=Path(root['logical']);need(logical==Path(self.plan['materialsDirectory'])/('pg-data' if purpose=='application-data' else 'runtime'),'host_material_namespace_scope_changed')
   paths.append((logical,destination))
   if purpose=='application-data':paths.extend((logical/name,destination+'/'+name) for name in ['recording-spool','metering-spool'])
  expected=[{'path':destination,'dev':str(path.stat().st_dev),'ino':str(path.stat().st_ino)} for path,destination in paths]
  code="import{stat}from'node:fs/promises';for(const row of JSON.parse(process.argv[1])){const actual=await stat(row.path,{bigint:true});if(String(actual.dev)!==row.dev||String(actual.ino)!==row.ino)process.exit(1);}console.log(JSON.stringify({materialMountsVerified:true}));"
  for name in [GW,WORKER]:
   before=self.row(name);selected=self.gw if name==GW else self.worker;need(selected is not None and before.get('Id')==selected.get('Id') and sig(before)==sig(selected) and before['State'].get('Running') is True,'host_material_application_not_running')
   need(json.loads(run(['/usr/bin/docker','exec',before['Id'],'node','--input-type=module','-e',code,json.dumps(expected)]))=={'materialMountsVerified':True},'host_material_namespace_mismatch')
   after=self.row(name);need(sig(after)==sig(before) and container_generation(after)==container_generation(before),'host_material_application_generation_changed')
 def verify_source_recovery_materials(self):
  need(self.j.get('oldDataRollbackForbidden',False) is False and self.j.get('targetBootAuthorized',False) is False,'host_source_material_recovery_forbidden')
  self.verify_original_roots()
  selected=self.material_mover()
  if selected is None:return
  mover,_,rows=selected
  need(not any(os.path.lexists(path) for path in [mover.STATE,mover.g.CONFIG,mover.g.MAINTENANCE]),'host_source_material_authority_changed')
  for row in rows:
   path=Path(row['logical']);expected=row['sourceId']
   need(not os.path.lexists(path) if expected is None else os.path.lexists(path) and mover.g.identity(path)[:2]==expected,'host_source_material_root_changed')
 def start_applications(self,*,source_recovery=False):
  diagnostic_stage('source_applications_start')
  need(type(source_recovery) is bool,'host_application_start_mode_invalid')
  if source_recovery:
   self.verify_source_recovery_materials()
   if self.managed_source_recovery() and self.j.get('sourceRecoveryProcesses') is not None:need(self.source_pair_processes()==self.j['sourceRecoveryProcesses'] and self.source_recovery_network()==self.j.get('sourceRecoveryNetwork'),'host_source_recovery_generation_changed')
  for name in [GW,WORKER]:
   row=self.gw if name==GW else self.worker;need(row is not None,'host_current_active_not_loaded');actual=self.row(name);need(actual['Id']==row['Id'] and sig(actual)==sig(row),'host_application_changed')
   if not actual['State']['Running']:run(['/usr/bin/docker','start',actual['Id']])
  if source_recovery:
   self.verify_source_recovery_materials()
   if self.managed_source_recovery():
    current=self.source_pair_processes();network=self.source_recovery_network();saved=self.j.get('sourceRecoveryProcesses');need(saved is None or saved==current and self.j.get('sourceRecoveryNetwork')==network,'host_source_recovery_generation_changed');self.j['sourceRecoveryProcesses']=current;self.j['sourceRecoveryNetwork']=network
    if not hasattr(self,'source_recovery_deadline'):self.source_recovery_deadline=time.monotonic()+120
  else:self.verify_application_material_mounts()
  self.gw=self.row(GW);self.worker=self.row(WORKER);self.j['applicationsStartEpochMs']=int(datetime.fromisoformat(self.worker['State']['StartedAt'].replace('Z','+00:00')).timestamp()*1000);self.j['workerGeneration']={'id':self.worker['Id'],'startedAt':self.worker['State']['StartedAt']};self.save(self.j)
 def verify_current_approval_and_private_readiness(self,*,source_recovery=False):
  diagnostic_stage('source_readiness')
  need(type(source_recovery) is bool,'host_readiness_mode_invalid')
  if source_recovery and self.managed_source_recovery():return self.wait_source_recovery_ready()
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
  diagnostic_stage('source_network')
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
 def refresh_active_entry(self):
  diagnostic_stage('source_entry_refresh');self.entry.refresh_active()
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
  journal['failure']={'oldDataRollbackAllowedForExplicitRecovery':old_data_allowed,'automaticOldDataRollback':False,**failure_summary(sys.exc_info()[1])};self.save(journal)
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
  diagnostic_stage('source_entry_verify')
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
  if getattr(self,'runtimeTools',None) is not None:
   self.j['supervisorTools']={'files':self.runtimeTools,'hostSha256':sha(code_bytes(Path(__file__))),'pid':os.getpid(),'bootId':Path('/proc/sys/kernel/random/boot_id').read_text().strip()};self.save(self.j)
  with socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM) as peer:peer.settimeout(1);peer.connect(('\0'+address[1:]) if address.startswith('@') else address);peer.sendall(b'READY=1')
 def verify_supervisor_tools(self,row):
  if self.runtimeTools is not None:
   need(self.j.get('supervisorTools')=={'files':self.runtimeTools,'hostSha256':sha(code_bytes(Path(__file__))),'pid':int(row['MainPID']),'bootId':Path('/proc/sys/kernel/random/boot_id').read_text().strip()},'host_approved_tools_refresh_required')
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
  diagnostic_stage('source_checkpoint')
  self.verify_original_roots();self.load_current_durable_active();need(self.upgrade.digest(self.upgrade.active_identity(self.active))==self.plan['activeIdentityDigest'],'host_original_approval_changed')
  for name in [PG,GW,WORKER]:self.immutable(name)
  row=self.row(PG)
  if not row['State']['Running']:
   need(row['State'].get('Pid')==0 and not any(row['State'].get(k) for k in ['Restarting','Paused','Dead','OOMKilled','Error']),'host_original_pg_state_unknown');self.require_clean_control(self.core.SOURCE_DB)
 def start_original_postgres(self):
  diagnostic_stage('source_postgres_start')
  self.verify_original_roots()
  if getattr(self,'archive_lease',None) is not None:os.close(self.archive_lease);self.archive_lease=None
  row=self.immutable(PG)
  if not row['State']['Running']:run(['/usr/bin/docker','start',row['Id']])
 def verify_original_postgres(self):
  diagnostic_stage('source_postgres_verify');self.verify_postgres_mount_tls_report_wal(on_target=False)
 def verify_original_full_instance(self,*,before_publish=False):
  diagnostic_stage('source_full_verify')
  if not self.managed_source_recovery():return self.verify_full_instance(on_target=False)
  need(before_publish is True,'host_source_verification_after_publish');mode=self.wait_source_recovery_ready();self.verify_persistent_network();port=self.config['port'];stack=self.api['STACK'];deadline=self.source_recovery_deadline
  code="const{request}=await import('node:http');let done=false;const finish=v=>{if(done)return;done=true;process.stdout.write(v?'true':'false',()=>process.exit(v?0:1));};setTimeout(()=>finish(false),10000);const base=new URL('http://127.0.0.1:8092'),status=(path,headers)=>new Promise((resolve,reject)=>{const req=request(new URL(path,base),{headers:{Host:base.host,Connection:'close',...headers},signal:AbortSignal.timeout(2000)},reply=>{reply.resume();reply.once('end',()=>resolve(reply.statusCode));reply.once('error',reject);});req.once('error',reject);req.end();});try{for(const[path,expected,headers]of [['/health',200,{}],['/ready',200,{}],['/api/admin/state',401,{}],['/health',403,{Host:'untrusted.invalid'}]])if(await status(path,headers)!==expected)throw Error();finish(true);}catch{finish(false);}"
  remaining=deadline-time.monotonic();need(remaining>0,'host_source_recovery_not_ready');reply=subprocess.run(['/usr/bin/docker','exec',self.gw['Id'],'node','--input-type=module','--eval',code],stdin=subprocess.DEVNULL,capture_output=True,timeout=min(12,remaining));need(reply.returncode==0 and reply.stdout==b'true' and reply.stderr==b'','host_source_authentication_changed')
  need(self.pg.device_http_code(stack,self.config,'/ready')==200 and self.pg.device_http_code(stack,self.config,'/api/admin/state')==401 and self.pg.device_http_code(stack,self.config,'/health',{'Host':'untrusted.invalid'})==403,'host_source_authentication_changed');self.entry.active_record();self.verify_source_recovery_processes();need(time.monotonic()<deadline,'host_source_recovery_not_ready')
  need(before_publish is True,'host_source_verification_after_publish');return mode
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
  if self.plan.get('materialMove') is not None:self.data_mounts(verify_material=False)
  else:self.data_mounts()
  self.install_gate()
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
 def archive_adopt_facts(self):
  approved=self.plan.get('archivePrecopy');need(isinstance(approved,dict),'host_archive_adopt_unapproved')
  purpose=Path(approved.get('directory',''));need(purpose.parent==ROOT and purpose.name not in ['','postgres'] and purpose==Path(os.path.normpath(purpose)),'host_archive_adopt_path_invalid')
  stamp=self.core.identity(purpose.lstat())[:5]
  need(stamp==approved.get('directoryIdentity') and stat.S_ISDIR(stamp[4]) and stamp[2:4]==[0,0] and stat.S_IMODE(stamp[4])==0o700,'host_archive_adopt_purpose_changed')
  values={}
  for label,name in [('intent','copy.intent.json'),('complete','copy.complete.json'),('proof','archive-proof.json')]:
   raw=private(purpose/name);need(sha(raw)==approved.get(label+'Sha256'),'host_archive_adopt_'+label+'_changed');values[label]=json.loads(raw)
  intent,complete,proof=(values[k] for k in ['intent','complete','proof']);binding=approved.get('binding')
  need(isinstance(binding,dict) and intent.get('binding')==binding==complete.get('binding') and binding.get('cluster')==self.plan['systemId'] and intent.get('eligible') is False and intent.get('sourceDeletionAllowed') is False,'host_archive_adopt_binding_changed')
  need(complete.get('eligible') is False and all(complete.get(k) is False for k in ['sourceDeleted','sourceMounted','servicesChanged']) and complete.get('proofSha256')==approved['proofSha256'],'host_archive_adopt_complete_invalid')
  need(proof.get('version')==1 and proof.get('eligible') is False and isinstance(proof.get('files'),dict) and isinstance(proof.get('directories'),dict) and isinstance(proof.get('unstable'),list) and complete.get('files')==len(proof['files']) and complete.get('unstableFiles')==len(proof['unstable']),'host_archive_adopt_proof_invalid')
  need(proof.get('sourceRoot')==self.plan['sourceRoots']['archive'] and proof.get('targetRoot')==approved.get('targetRoot'),'host_archive_adopt_root_changed')
  self.current_identity()
  need(approved.get('deploymentId')==self.ticket.get('deploymentId') and isinstance(approved.get('deploymentId'),str) and approved.get('activeIdentityDigest')==self.plan['activeIdentityDigest'],'host_archive_adopt_release_changed')
  paths=binding.get('paths',{});need(set(paths)=={'db','archive','control','targetRoot'} and paths['db'].get('path')==str(self.core.SOURCE_DB) and paths['archive'].get('path')==str(self.core.SOURCE_ARCHIVE) and paths['control'].get('path')==str(self.core.SOURCE_DB/'18/docker/global/pg_control') and paths['targetRoot'].get('path')==str(ROOT),'host_archive_adopt_source_path_changed')
  for row in paths.values():
   path=Path(row['path']);info=path.lstat();need(not path.is_symlink() and {'path':str(path),'dev':info.st_dev,'ino':info.st_ino,'uid':info.st_uid,'gid':info.st_gid,'mode':stat.S_IMODE(info.st_mode)}==row,'host_archive_adopt_source_identity_changed')
  fd=os.open(paths['control']['path'],os.O_RDONLY|os.O_NOFOLLOW)
  try:need(str(int.from_bytes(os.read(fd,8),'little'))==self.plan['systemId'],'host_archive_adopt_cluster_changed')
  finally:os.close(fd)
  pg=self.immutable(PG);state=pg['State'];need({k:pg[k] for k in ['Id','Image']}|{k:state[k] for k in ['Pid','StartedAt']}==binding.get('container') and state.get('Running') is True and not any(state.get(k) for k in ['Paused','Restarting','Dead','OOMKilled']),'host_archive_adopt_pg_generation_changed')
  mounts=binding.get('mounts',[]);actual=[{'source':m['Source'],'destination':m['Destination'],'rw':m['RW']} for m in pg['Mounts'] if m['Destination'] in [r['destination'] for r in mounts]]
  need(mounts and sorted(actual,key=lambda r:r['source'])==sorted(mounts,key=lambda r:r['source']),'host_archive_adopt_pg_mounts_changed')
  units=approved.get('writerUnits');need(isinstance(units,list) and units and len(set(units))==len(units) and all(isinstance(v,str) and re.fullmatch(r'harbor-pg-archive-[a-z0-9-]+\.service',v) for v in units),'host_archive_adopt_units_invalid')
  for name in units:
   value=self.state_unit(name,allow_absent=True);need(value.get('ActiveState') in ['inactive','failed'] and value.get('MainPID')=='0','host_archive_adopt_writer_active')
   group=value.get('ControlGroup','')
   if group:
    need(group.startswith('/') and group!='/' and group==os.path.normpath(group),'host_archive_adopt_cgroup_invalid');events=Path('/sys/fs/cgroup')/group.lstrip('/')/'cgroup.events'
    if events.exists():need(dict(line.split() for line in events.read_text().splitlines()).get('populated')=='0','host_archive_adopt_writer_active')
  return approved,purpose,proof
 def verify_archive_adopt_target(self,path,proof):
  need(self.core.identity(path.lstat())[:5]==proof['targetRoot'] and path.stat().st_dev==ROOT.stat().st_dev,'host_archive_adopt_target_changed')
  files,dirs=self.core.inventory(path,999)
  need(set(files)==set(proof['files']) and all(stamp==proof['files'][name].get('target') for name,stamp in files.items()),'host_archive_adopt_files_changed')
  need(set(dirs)==set(proof['directories']) and all(self.core.identity((path/name).lstat())[:5]==proof['directories'][name] for name in dirs),'host_archive_adopt_directories_changed')
 def archive_adopt_unmounted(self,purpose):
  # Inspect kernel mount metadata only: never read archived file contents.
  disk=ROOT.stat().st_dev;roots=[purpose,self.core.TARGET]
  staging=self.j.get('archiveAdopt',{}).get('staging')
  if staging is not None:
   path=Path(staging);need(path.parent==ROOT and re.fullmatch(r'\.pg-storage-adopt-[a-f0-9]{32}',path.name),'host_archive_adopt_staging_invalid');roots.append(path)
  forbidden=[str(p) for p in roots];fsroots=['/'+str(p.relative_to('/sdc-disk')) for p in roots]
  for line in Path('/proc/self/mountinfo').read_text().splitlines():
   fields=line.split();need(len(fields)>=10,'host_archive_adopt_mount_inventory_unknown')
   unescape=lambda value:re.sub(r'\\([0-7]{3})',lambda match:chr(int(match[1],8)),value)
   point,fsroot=unescape(fields[4]),unescape(fields[3])
   need(not any(point==v or point.startswith(v+'/') for v in forbidden),'host_archive_adopt_target_mounted')
   if fields[2]==str(os.major(disk))+':'+str(os.minor(disk)) and not (point=='/sdc-disk' and fsroot=='/'):
    need(not any(fsroot==v or fsroot.startswith(v+'/') or v.startswith(fsroot.rstrip('/')+'/') for v in fsroots),'host_archive_adopt_target_referenced')
  ids=run(['/usr/bin/docker','container','ls','--all','--quiet','--no-trunc']).decode().splitlines()
  if ids:
   need(all(re.fullmatch('[a-f0-9]{64}',v) for v in ids),'host_archive_adopt_container_inventory_unknown')
   rows=json.loads(run(['/usr/bin/docker','container','inspect',*ids]));need(isinstance(rows,list) and {r.get('Id') for r in rows}==set(ids),'host_archive_adopt_container_inventory_unknown')
   for row in rows:
    for mount in row.get('Mounts',[]):
     if mount.get('Type') not in ['bind','volume']:continue
     source=mount.get('Source');need(isinstance(source,str) and source.startswith('/'),'host_archive_adopt_mount_source_unknown');need(not any(source==v or source.startswith(v+'/') or v.startswith(source.rstrip('/')+'/') for v in forbidden),'host_archive_adopt_target_referenced')
 def adopt_archive(self):
  need(self.j['phase'] in ['planned','archive-adopting','archive-prepared'],'host_archive_adopt_phase_invalid')
  approved,purpose,proof=self.archive_adopt_facts();self.archive_adopt_unmounted(purpose)
  old=purpose/'archive';target=self.core.TARGET;new=target/'archive';claim=self.j.get('archiveAdopt')
  scope={'version':1,'planSha256':self.planSha256,'directory':str(purpose),'proofSha256':approved['proofSha256'],'archiveRoot':proof['targetRoot']}
  if claim is None:
   need(self.j['phase']=='planned' and not os.path.lexists(target) and not os.path.lexists(PROOF),'host_archive_adopt_foreign_target')
   self.verify_archive_adopt_target(old,proof)
   staging=ROOT/('.pg-storage-adopt-'+uuid.uuid4().hex);staging.mkdir(mode=0o700);(staging/'db').mkdir(mode=0o700);os.chown(staging/'db',999,999);self.core.sync_dir(staging/'db');self.core.sync_dir(staging);self.core.sync_dir(ROOT)
   claim={**scope,'staging':str(staging),'targetRoot':self.core.identity(staging.lstat())[:5],'dbRoot':self.core.identity((staging/'db').lstat())[:5]}
   self.j.update(phase='archive-adopting',archiveAdopt=claim)
   try:self.save(self.j)
   except BaseException:
    # Only a confirmed old journal permits reclaiming this still-empty stage.
    # A lost write ACK keeps the inode-bound stage for the next locked retry.
    try:
     durable=json.loads(private(STATE))
     if durable.get('scope')==self.plan['scope'] and durable.get('runId')==self.plan['runId'] and durable.get('phase')=='planned' and 'archiveAdopt' not in durable and self.core.identity(staging.lstat())[:5]==claim['targetRoot'] and self.core.identity((staging/'db').lstat())[:5]==claim['dbRoot'] and {p.name for p in staging.iterdir()}=={'db'} and not any((staging/'db').iterdir()):
      (staging/'db').rmdir();staging.rmdir();self.core.sync_dir(ROOT)
    except BaseException:pass # Unknown durability never authorizes deletion.
    raise
  need(isinstance(claim,dict) and all(claim.get(k)==v for k,v in scope.items()),'host_archive_adopt_journal_changed')
  staging=Path(claim.get('staging',''));need(staging.parent==ROOT and re.fullmatch(r'\.pg-storage-adopt-[a-f0-9]{32}',staging.name),'host_archive_adopt_staging_invalid')
  stage_exists=os.path.lexists(staging);target_exists=os.path.lexists(target);need(stage_exists!=target_exists,'host_archive_adopt_target_ambiguous')
  current=staging if stage_exists else target
  need(self.core.identity(current.lstat())[:5]==claim.get('targetRoot') and stat.S_ISDIR(current.lstat().st_mode),'host_archive_adopt_claim_changed')
  need(self.core.identity((current/'db').lstat())[:5]==claim.get('dbRoot') and not any((current/'db').iterdir()),'host_archive_adopt_db_changed')
  need(set(p.name for p in current.iterdir())<=({'db'} if stage_exists else {'db','archive'}),'host_archive_adopt_foreign_target')
  if stage_exists:rename_noreplace(staging,target);self.core.sync_dir(ROOT)
  old_exists=os.path.lexists(old);new_exists=os.path.lexists(new);need(old_exists!=new_exists,'host_archive_adopt_archive_ambiguous')
  self.verify_archive_adopt_target(old if old_exists else new,proof)
  if old_exists:rename_noreplace(old,new)
  # Fsync both parents even when a prior rename completed but its ACK was lost.
  self.core.sync_dir(purpose);self.core.sync_dir(target);self.core.sync_dir(ROOT)
  canonical=json.dumps(proof,sort_keys=True,separators=(',',':')).encode()
  if os.path.lexists(PROOF):need(private(PROOF)==canonical,'host_archive_adopt_foreign_proof')
  else:save(PROOF,proof)
  self.j.update(phase='archive-prepared',archiveProofSha256=sha(private(PROOF)),createdTargetRoots={'db':claim['dbRoot'],'archive':proof['targetRoot']});self.save(self.j)
  return {'prepared':True,'adopted':True,'eligible':False,'databaseCopied':False,'servicesChanged':False}
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
def archive_copy_lock():
 fd=os.open(ARCHIVE_COPY_LOCK,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK)
 try:
  info=os.fstat(fd);need(stat.S_ISREG(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600 and info.st_nlink==1,'host_archive_adopt_lock_untrusted');fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);yield
 finally:os.close(fd)

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
 return {'scope':core.SCOPE,'approved':False,'readOnly':True,'runId':uuid.uuid4().hex,'systemId':ticket['database']['cluster'],'tlsVerifyFull':True,'activeIdentityDigest':upgrade.digest(upgrade.active_identity(active)),'materialsDirectory':ticket['materialsDirectory'],'containers':rows,'sourceRoots':{k:core.identity(v.lstat())[:5] for k,v in [('db',core.SOURCE_DB),('archive',core.SOURCE_ARCHIVE)]},'targetFilesystemUuid':disk[0]['uuid'],'networkPersistentDigest':sha(json.dumps({k:network.get(k) for k in ['Name','Id','Driver','Internal','IPAM','Options','Labels']},sort_keys=True).encode()),'toolSha256':{str(v):sha(v.read_bytes()) for v in [CORE,Path(__file__),DEPLOY,ENTRY,UPGRADE,COMPOSE,RUNTIME,MATERIAL]},'noLargeDirectoryScan':True,'noCredentialValuesRetained':True}

def verify_factory_guard(config):
 host=Host();need(config==host.config and config.get('postgres_storage_startup_guard') is True,'host_factory_root_configuration_changed');need(host.j.get('phase')=='complete' and host.j.get('oldDataRollbackForbidden') is True,'host_factory_move_not_complete');host.data_mounts();host.immutable(PG);host.require_docker_restart_disabled();host.verify_installed_unit_files();need(host.row(PG)['HostConfig']['RestartPolicy']['Name']=='no','host_factory_pg_autostart_enabled')
 for name in [host.core.STORAGE_START,host.core.STORAGE_ONLINE]:
  row=host.state_unit(name);need(row.get('UnitFileState')=='enabled','host_factory_boot_guard_not_enabled')
  if name==host.core.STORAGE_ONLINE:need(row.get('ActiveState')=='active' and row.get('SubState')=='running' and int(row.get('MainPID','0'))>0,'host_factory_supervisor_not_running')
  if name==host.core.STORAGE_ONLINE:host.verify_supervisor_tools(row)
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
  else:
   with locks(host):host.reload_locked_state();host.signal_online_ready()
 while not stopped:
  try:
   with locks(host):
    host.reload_locked_state()
    # Requires=online writers must not be stopped by an implicit supervisor
    # exit. The explicit bridge refresh snapshots/restores that dependency set.
    need(host.runtimeTools is None or verified_runtime_tools(host.plan,host.j)==host.runtimeTools,'host_approved_tools_refresh_required')
    host.runtime_step()
  except BlockingIOError:pass
  except (RuntimeError,OSError,ValueError,subprocess.TimeoutExpired):print(json.dumps({'supervision':'protected','error':'host_runtime_reconcile_failed'}),flush=True)
  if not stopped:time.sleep(host.poll_interval_seconds)
 return {'supervisorStopped':True}

def main():
 global _EMIT_PROGRESS
 actions=['collect-readonly','adopt-archive','precopy','launch-switch','launch-recover-source','launch-recover-target','switch','recover-source','recover-target','boot-start','boot-online','pause','resume','reconcile','status']
 need(len(sys.argv) in [2,3] and sys.argv[1] in actions and (len(sys.argv)==3)==(sys.argv[1] in ['recover-source','recover-target']),'host_action_invalid')
 action=sys.argv[1]
 if action=='collect-readonly':return collect_readonly()
 host=Host();_EMIT_PROGRESS=action in ['adopt-archive','precopy','switch','recover-source','recover-target','boot-start','boot-online','pause','resume']
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
  if action=='adopt-archive':
   with archive_copy_lock():return host.adopt_archive()
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
  print(json.dumps({'failed':True,**failure_summary(error),'inspectOriginalUnitBeforeRetry':True}));raise SystemExit(1)
