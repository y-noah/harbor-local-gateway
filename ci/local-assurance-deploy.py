"""8092 本机保障发布；独立合同，不签发或冒充 offhost/严格 activation 证明。"""
from pathlib import Path
from types import SimpleNamespace
import fcntl,hashlib,importlib.util,json,os,re,signal,stat,subprocess,time
PROFILE=Path('/etc/harbor-local-assurance-release.json')
RELEASE='4ad63d04-7c44-43d8-b747-8a7dab78e1ec'
REVISION='26f91a202dcd258b38d4721a4b029822a4f0da92'
CONTRACT='harbor-local-assurance-deploy-v1'
RUN=Path('/opt/product-development/harbor-local-gateway/local-assurance')/RELEASE

def private_json(path):
 p=Path(path);info=p.lstat()
 if not stat.S_ISREG(info.st_mode) or info.st_uid!=0 or stat.S_IMODE(info.st_mode)!=0o600 or info.st_nlink!=1:raise ValueError('Local assurance configuration must be root-owned 0600')
 fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW)
 with os.fdopen(fd,'rb') as f:raw=f.read(1048577)
 if len(raw)>1048576:raise ValueError('Local assurance configuration is too large')
 return json.loads(raw)

def validate_profile(profile,image,config):
 if profile.get('enabled') is not True:raise ValueError('Local assurance release is disabled')
 if type(profile.get('allowDynamicBridgePeers',False)) is not bool:raise ValueError('Dynamic peer flag must be boolean')
 if profile.get('version')!=1 or profile.get('mode')!='local-assurance' or profile.get('releaseId')!=RELEASE or profile.get('businessRevision')!=REVISION:raise ValueError('Local assurance release identity changed')
 if not isinstance(profile.get('businessConfigDigest'),str) or not re.fullmatch(r'sha256:[a-f0-9]{64}',profile['businessConfigDigest']):raise ValueError('Business config digest is required')
 if config.get('port')!=8092 or config.get('storage_engine')!='postgres' or image!=config['image_prefix']+':'+REVISION or str(RUN)!=profile.get('runDirectory'):raise ValueError('Local assurance target changed')
 return profile

class LocalOperationUnknown(RuntimeError):pass

def helper(action,timeout=1200):
 try:result=subprocess.run(['/opt/harbor-storage-tools/node/bin/node','/usr/local/lib/harbor/local-assurance-migrate.mjs',action],capture_output=True,timeout=timeout,env={'PATH':'/usr/sbin:/usr/bin:/sbin:/bin','LANG':'C','HOME':'/root'})
 except subprocess.TimeoutExpired as error:raise LocalOperationUnknown('Helper outcome requires inspection') from error
 if result.returncode!=0:raise RuntimeError('Local assurance migration failed; material retained')
 value=json.loads(result.stdout)
 if value.get('contract')!='harbor-local-assurance-migration-v1' or value.get('releaseId')!=RELEASE or value.get('offhostVerified') is not False or value.get('deploymentAllowed') is not False:raise ValueError('Local assurance helper contract changed')
 return value

STATE=RUN/'release.json'
PUBLISHER=Path('/opt/product-development/harbor-local-gateway/postgres-infra/control/publisher.json')
LOCAL_HELPER='/usr/local/lib/harbor/local-assurance-deploy.py'

def load_file(path,name):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

def journal(entry,value):
 entry.save_json(STATE,value)

def profile_identity(profile):return hashlib.sha256(json.dumps({k:v for k,v in profile.items() if k not in ['enabled','migrationTimeoutSeconds','recoveryTimeoutSeconds']},sort_keys=True,separators=(',',':')).encode()).hexdigest()

def binding(value):return {k:value[k] for k in ['contract','deploymentId','receiptDigest','configDigest','manifestDigest','revision','sourceFence']}

def validate_authorization(image,approval,active=False):
 profile=private_json(PROFILE);saved=private_json(STATE)
 return bool((active or profile.get('enabled') is True) and profile.get('releaseId')==RELEASE and image.endswith(':'+REVISION) and saved.get('image')==image and approval.get('contract')==CONTRACT and binding(saved['approval'])==binding(approval) and saved.get('offhostVerified') is False and saved.get('profileIdentityDigest')==profile_identity(profile))

def allow_dynamic_bridge_peers(config,state):
 if state.get('allowDynamicBridgePeers') is not True:return False
 profile=private_json(PROFILE);root=private_json('/etc/harbor-gateway-deploy.json')
 if profile.get('allowDynamicBridgePeers') is not True or profile.get('enabled') is not True or root.get('local_assurance_release') is not True:return False
 validate_profile(profile,root['image_prefix']+':'+REVISION,root)
 if state.get('localAssuranceReleaseId')!=RELEASE or state.get('localAssuranceProfileIdentityDigest')!=profile_identity(profile):raise ValueError('Dynamic peer policy identity changed')
 original=state.get('source',{});expected=profile['source']
 if any(original.get(k)!=expected.get(v) for k,v in [('containerId','containerId'),('imageId','imageId'),('startedAt','startedAt')]):raise ValueError('Dynamic peer source identity changed')
 identity=state.get('dynamicBridgeIdentity',{})
 if config['bridge']['name']!=identity.get('name') or config['source'].get('containerId')!=expected['containerId']:return False
 if config['bridge']['mac']!=identity.get('mac') or type(identity.get('ifindex')) is not int or not isinstance(state.get('dynamicPeerDeviceIndices'),dict) or not isinstance(state.get('dynamicSourceNamespace'),list):raise ValueError('Dynamic peer bridge identity changed')
 return True


def candidate_network(verify_live=False):
 profile=private_json(PROFILE);root=private_json('/etc/harbor-gateway-deploy.json');operator=private_json('/etc/harbor-storage-operator.json')
 validate_profile(profile,root['image_prefix']+':'+REVISION,root)
 expected=profile.get('candidateNetwork',{});name=operator.get('candidate',{}).get('network')
 if not isinstance(name,str) or name=='bridge' or root.get('postgres_network')!=name or expected.get('name')!=name or not re.fullmatch('[a-f0-9]{64}',expected.get('id','')):raise ValueError('Local candidate requires the approved isolated PostgreSQL network')
 if verify_live:
  result=subprocess.run(['/usr/bin/docker','network','inspect',name],capture_output=True,text=True,timeout=15,check=True);rows=json.loads(result.stdout)
  if len(rows)!=1 or rows[0].get('Id')!=expected['id'] or rows[0].get('Name')!=name or rows[0].get('Driver')!='bridge' or rows[0].get('Internal') is not False or set(rows[0].get('Containers',{}))!=set(expected['infrastructureContainerIds']):raise ValueError('Approved PostgreSQL network has changed or has foreign members')
  links=json.loads(subprocess.run(['/usr/sbin/ip','-j','link','show','dev',expected['bridge']],capture_output=True,text=True,timeout=10,check=True).stdout)
  if len(links)!=1 or links[0]['ifindex']!=expected['bridgeIfindex'] or links[0]['address']!=expected['bridgeMac']:raise ValueError('Approved PostgreSQL bridge identity changed')
 return expected

def verify_candidate_network(row):
 expected=candidate_network();networks=row.get('NetworkSettings',{}).get('Networks',{})
 if set(networks)!={expected['name']} or networks[expected['name']].get('NetworkID')!=expected['id']:raise ValueError('Candidate isolated network identity changed')
 return expected


def active_authorization(image):
 saved=private_json(STATE)
 if saved.get('phase') not in ['verified','committed','commit-unknown-or-active'] or not validate_authorization(image,saved['approval'],active=True):raise RuntimeError('Local assurance release is not committed')
 actual=helper('check',180)
 if actual.get('identityAndDataVerified') is not True:raise RuntimeError('Local PostgreSQL identity is unavailable')
 return {**saved['approval'],'state':'active'}

class LocalActivation:
 def __init__(self,entry,approval,proof):self.entry=entry;self.approval=approval;self.proof=proof;self.state='pending'
 def __call__(self,action,image):
  if not validate_authorization(image,self.approval):raise RuntimeError('Local release binding changed')
  if action=='preflight':return {**self.approval,'state':self.state,'proof':self.proof}
  if action=='consume':self.state='active'
  elif action=='revoke':helper('close',180);self.state='failed'
  elif action not in ['check','status']:raise ValueError('Unknown local assurance action')
  return {**self.approval,'state':self.state}

def restore_publisher(database):
 source=RUN/'backup/publisher-before.json'
 if not source.exists():return
 before=private_json(source);path=PUBLISHER;current=json.loads(path.read_text())
 if current.get('database') not in [before.get('database'),database] or {k:v for k,v in current.items() if k!='database'}!={k:v for k,v in before.items() if k!='database'}:raise RuntimeError('Publisher ownership changed')
 if current==before:return
 temporary=path.with_name('.local-assurance-restore');fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o644)
 with os.fdopen(fd,'w') as file:os.fchmod(file.fileno(),0o644);json.dump(before,file);file.write('\n');file.flush();os.fsync(file.fileno())
 os.replace(temporary,path)
 directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
 try:os.fsync(directory)
 finally:os.close(directory)

DEVICE_TABLE='harbor_local_device_release'

def device_gate_snapshot(entry):
 value=json.loads(entry.execute(['/usr/sbin/nft','--json','list','table','inet',DEVICE_TABLE]))
 def stable(v):
  if isinstance(v,dict):return {k:stable(x) for k,x in v.items() if k not in ['handle','metainfo']}
  if isinstance(v,list):return [stable(x) for x in v if not isinstance(x,dict) or 'metainfo' not in x]
  return v
 return stable(value)

def device_gate_install(entry,port):
 if type(port)!=int or not 1<=port<=65535 or port==8092:raise ValueError('Device gate port is invalid')
 bridge=candidate_network()['bridge']
 if not isinstance(bridge,str) or not re.fullmatch('[a-zA-Z0-9_-]{1,15}',bridge):raise ValueError('Candidate bridge name is invalid')
 entry.require(not entry.execute(['/usr/bin/ss','-lntH','sport = :'+str(port)]).strip(),'local_device_port_occupied')
 # An existing table belongs to an earlier/unknown attempt and is never adopted.
 tables=json.loads(entry.execute(['/usr/sbin/nft','--json','list','tables']))
 entry.require(not any(v.get('table',{}).get('name')==DEVICE_TABLE for v in tables.get('nftables',[])),'local_device_gate_exists')
 entry.execute(['/usr/sbin/nft','-f','-'],body=f'add table inet {DEVICE_TABLE}\nadd chain inet {DEVICE_TABLE} ingress {{ type filter hook prerouting priority -110; policy accept; }}\nadd rule inet {DEVICE_TABLE} ingress iifname != "lo" tcp dport {port} drop comment "{RELEASE}"\nadd chain inet {DEVICE_TABLE} candidate_backend {{ type filter hook forward priority -110; policy accept; }}\nadd rule inet {DEVICE_TABLE} candidate_backend oifname "{bridge}" tcp dport 8092 drop comment "{RELEASE}"\n')
 snapshot=device_gate_snapshot(entry);entry.save_json(RUN/'device-gate.json',{'releaseId':RELEASE,'port':port,'snapshot':snapshot})
 return snapshot

def device_gate_remove(entry,snapshot):
 entry.require(snapshot==device_gate_snapshot(entry),'local_device_gate_changed')
 entry.execute(['/usr/sbin/nft','delete','table','inet',DEVICE_TABLE])
 tables=json.loads(entry.execute(['/usr/sbin/nft','--json','list','tables']))
 entry.require(not any(v.get('table',{}).get('name')==DEVICE_TABLE for v in tables.get('nftables',[])),'local_device_gate_removal_unknown')


# 同一CI进程承担完整停源、迁移、候选验收与恢复；无后续模型依赖。
def run_release(image,config,archive,api):
 if archive is not None:raise ValueError('Local assurance requires the already installed image')
 profile=validate_profile(private_json(PROFILE),image,config)
 if config.get('local_assurance_release') is not True:raise ValueError('Local assurance root dispatch is disabled')
 entry=load_file(api['ENTRY_HELPER'],'local_entry_controller')
 composition=load_file(Path(__file__).with_name('postgres-deploy.py'),'local_postgres_composition')
 entry_config=entry.validate(entry.private_json(entry.PROFILE));original=entry.verify_source(entry_config)
 initial=entry.private_json(entry.STATE)
 if initial.get('phase') not in ['prepared','withdrawn','routed']:raise ValueError('Entry topology must be prepared before local release')
 operator=private_json('/etc/harbor-storage-operator.json');candidate_network(verify_live=True)
 if operator['source'].get('supervisorKind')!='docker-restart':raise ValueError('This local release only supports the existing Docker supervisor')
 if original['Id']!=profile['source']['containerId'] or original['Image']!=profile['source']['imageId']:raise ValueError('Original source identity changed')
 # Leave the SSH/logind session before the first source-affecting operation.
 entry.EntryDeployment.enter_probe_worker(SimpleNamespace(config=entry_config))
 helper('prepare',180)
 RUN.mkdir(mode=0o700,parents=True,exist_ok=True);(RUN/'backup').mkdir(mode=0o700,exist_ok=True)
 for name,path in [('entry-controller.json',entry.PROFILE),('gateway-deploy.json',Path(api['CONFIG']))]:
  target=RUN/'backup'/name
  if target.exists():raise RuntimeError('Local release backup already exists; inspect previous attempt')
  fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
  with os.fdopen(fd,'wb') as file:file.write(Path(path).read_bytes());file.flush();os.fsync(file.fileno())
 saved={'contract':CONTRACT,'releaseId':RELEASE,'image':image,'phase':'prepared','sourceContainerId':original['Id'],'sourceRestartPolicy':original['HostConfig']['RestartPolicy'],'sourceMounts':original.get('Mounts',[]),'profileIdentityDigest':profile_identity(profile),'offhostVerified':False}
 journal(entry,saved);context=None;migration=None;device_gate=None;preflight_attempted=False
 try:
  device_gate=device_gate_install(entry,config['device_tls_port'])
  state=entry.private_json(entry.STATE)
  if state['phase'] in ['prepared','withdrawn']:entry.run_action(entry_config,'arm')
  entry.run_action(entry_config,'hold-drain');saved['phase']='holding';journal(entry,saved)
  migration=helper('migrate',1200)
  if migration.get('identityAndDataVerified') is not True or migration.get('state') not in ['data-verified','verified']:raise RuntimeError('Local migration is not verified')
  common,_=composition.configuration(config,str(RUN));local_api={**api,'STACK':str(RUN)}
  def proof_docker(*args,**kwargs):
   nonlocal preflight_attempted
   if args and args[0]=='run':preflight_attempted=True
   return local_api['docker'](*args,**kwargs)
  proof=composition.image_proof({**local_api,'docker':proof_docker},image,common)
  fence=private_json(RUN/'source-fence.json');fence=fence.get('sourceFence',fence.get('fence',fence))
  receipt=hashlib.sha256(json.dumps({'migration':migration,'candidate':proof},sort_keys=True).encode()).hexdigest()
  approval={'contract':CONTRACT,'deploymentId':RELEASE,'receiptDigest':receipt,'configDigest':profile['businessConfigDigest'],'manifestDigest':profile['businessImage'].split('@')[1],'revision':REVISION,'sourceFence':fence,'state':'pending','offhostVerified':False}
  saved.update(phase='verified',approval=approval,migration=migration,candidateProof=proof);journal(entry,saved)
  context=entry.authorize_deployment(image=image,approval=approval,local_assurance=True)
  activation=LocalActivation(entry,approval,proof)
  local_api.update(local_assurance=True,activation=activation,entry_deployment=context,http_code=lambda port,path,headers=None:context.probe(port,path,headers))
  result=composition.deploy(image,config,None,local_api)
  # ACTIVE 是不可逆边界：其后只保留/修复PG，绝不自动启动旧SQLite。
  entry.STORAGE_MARKER=RUN/'postgres-storage.json'
  context.activate(activation('status',image));device_gate_remove(entry,device_gate);device_gate=None;saved['phase']='committed';saved['approval']=activation('status',image);journal(entry,saved)
  return {'mode':'local-assurance','offhostVerified':False,'sourcePreserved':True,'deploymentAllowed':True,'composition':result}
 except BaseException as error:
  if entry.ACTIVE.exists():
   saved.update(phase='commit-unknown-or-active',errorCategory=type(error).__name__,sqliteRollbackForbidden=True);journal(entry,saved);raise
  saved.update(phase='failed-before-public',errorCategory=type(error).__name__);journal(entry,saved)
  # 未确认候选退出时保留现场，不能把未知状态当成已回收。
  if isinstance(error,LocalOperationUnknown):raise
  if isinstance(error,composition.ShutdownUnconfirmed) and context is None:
   if preflight_attempted:raise LocalOperationUnknown('Preflight run requires exit inspection') from error
   # Proof rejection before candidate startup can be recovered only after real
   # inventory confirms there is neither candidate nor preflight process.
   cause=error
   while cause is not None:
    if isinstance(cause,subprocess.TimeoutExpired):raise LocalOperationUnknown('Candidate operation timed out') from error
    cause=cause.__cause__
   for target in [api['NAME'],api['NAME']+'-worker',api['NAME']+'-postgres-check']:
    row=composition.container_inspection(local_api,target,[])
    if row is not None and not (target==api['NAME'] and row['Id']==original['Id'] and row.get('State',{}).get('Running') is False):raise LocalOperationUnknown('Candidate absence is unconfirmed')
  if context is not None:reclaim_before_public(context,composition,local_api,activation,image,api['NAME'],original['Id'])
  fence_file=RUN/'source-fence.json'
  if fence_file.exists() and private_json(fence_file).get('state')!='fenced':raise LocalOperationUnknown('Source fence is unconfirmed')
  closed={}
  if fence_file.exists():
   closed=helper('close',180)
   if closed.get('state')!='closed' or not (closed.get('runtimeLoginRevoked') is True or closed.get('noRuntimeAllocated') is True) or closed.get('targetSessions')!=0:raise LocalOperationUnknown('New runtime closure is unconfirmed')
  database=migration['database']['name'] if migration is not None else closed.get('database')
  if database:restore_publisher(database)
  if device_gate is not None:device_gate_remove(entry,device_gate)
  recovery=load_file(Path(api['ENTRY_HELPER']).with_name('gateway-source-recovery.py'),'local_source_recovery')
  saved['recovery']=recover_local_source(entry,recovery,profile,saved,closed);saved['phase']='source-restored';journal(entry,saved)
  raise


def verified_marker_path(image,active):
 if not validate_authorization(image,private_json(STATE)['approval'],active=True) or active.get('storageMarkerPath')!=str(RUN/'postgres-storage.json'):raise ValueError('Local storage marker binding changed')
 return RUN/'postgres-storage.json'

def reclaim_before_public(context,composition,api,activation,image,name,original_id):
 if context.activated:raise LocalOperationUnknown('Candidate commit is unconfirmed')
 removed=[]
 for target,role in [(name,'gateway'),(name+'-worker','worker')]:
  row=composition.container_inspection(api,target,[])
  if row is None or row['Id']==original_id:continue
  labels=row['Config'].get('Labels',{})
  if labels.get('com.harbor.activation-deployment-id')!=RELEASE or labels.get('com.harbor.activation-receipt')!=activation.approval['receiptDigest']:raise LocalOperationUnknown('Candidate ownership changed')
  state=row.get('State',{});never_started=state.get('Running') is False and state.get('Status')=='created' and state.get('StartedAt')=='0001-01-01T00:00:00Z' and not any(state.get(key) for key in ('Paused','Restarting','Dead','OOMKilled'))
  if not never_started:composition.stop_confirmed(api,target,row,role,[],proof_verified=True)
  current=composition.container_inspection(api,target,[])
  if current is None or current['Id']!=row['Id'] or current['State']['Running']:raise LocalOperationUnknown('Candidate shutdown is unconfirmed')
  api['docker']('rm',row['Id'])
  if composition.container_inspection(api,target,[]) is not None:raise LocalOperationUnknown('Candidate removal is unconfirmed')
  removed.append(row['Id'])
 if any(context.state.get(key) for key in ['candidate','candidateIntent']):context.unbind_candidate(activation=activation('revoke',image),removed_container_ids=removed)
 current=composition.container_inspection(api,name,[])
 if current is None:api['docker']('rename',original_id,name)
 elif current['Id']!=original_id:raise LocalOperationUnknown('Original source name is occupied')

def recover_local_source(entry,recovery,profile,saved,closed):
 # This contract authorizes only the preserved SQLite generation before public ACTIVE.
 config=entry.validate(entry.private_json(entry.PROFILE));state=entry.private_json(entry.STATE)
 recovery.no_writer(entry,state);recovery.no_candidates(entry,config['source'])
 row=entry.inspect_container(config['source']['containerId'])
 recovery.JOURNAL=RUN/'source-recovery.json'
 if row['State']['Running']:
  return recovery.recover_running_source(entry,config,state,retire_operator=False)
 fence=private_json(RUN/'source-fence.json')
 actual=fence.get('sourceFence',{})
 entry.require(fence.get('state')=='fenced' and saved['phase']=='failed-before-public' and saved['sourceContainerId']==row['Id']==profile['source']['containerId'] and row['Image']==profile['source']['imageId'] and row.get('Mounts',[])==saved['sourceMounts'],'local_recovery_source_identity')
 entry.require(actual.get('sourceContainerId')==row['Id'] and actual.get('sourceImageId')==row['Image'] and actual.get('sourceStartedAt')==row['State']['StartedAt'] and actual.get('sourceFinishedAt')==row['State']['FinishedAt'] and all(actual.get(k) is True for k in ['sourceStopped','supervisorFenced','productionVerified']),'local_recovery_fence')
 entry.require(closed.get('state')=='closed' and closed.get('targetSessions')==0 and (closed.get('runtimeLoginRevoked') is True or closed.get('noRuntimeAllocated') is True),'local_recovery_runtime_not_closed')
 entry.verify_source(config,running=False);entry.verify_resources(state,config,fenced=True)
 bridge=json.loads(entry.execute(['/usr/sbin/ip','-j','link','show','dev',config['bridge']['name']]))
 ports=json.loads(entry.execute(['/usr/sbin/ip','-j','link','show','master',config['bridge']['name']]))
 entry.require(len(bridge)==1 and bridge[0]['ifindex']==state.get('bridgeIfindex') and {v['ifname'] for v in ports}==set(config['bridge']['ports'])-{state.get('sourceBridgePort')},'local_recovery_network_changed')
 entry.require(entry.process_cgroup(entry.socket_owner(Path(entry.CONTROL)))==config['entryCgroup'],'local_recovery_entry_identity')
 current=entry.entry_state();entry.require(state.get('phase')=='holding' and current.get('slot')=='legacy' and current.get('phase')=='drained' and current.get('inflight')==0 and current.get('epoch')==state['drainReceipt']['entryEpoch'],'local_recovery_entry')
 recovery.JOURNAL=RUN/'source-recovery.json'
 entry.require(not recovery.JOURNAL.exists(),'local_recovery_already_attempted')
 journal={'version':1,'contract':'harbor-local-assurance-source-recovery-v1','releaseId':RELEASE,'phase':'authorized','originalConfig':config,'originalState':state,'sourceMounts':saved['sourceMounts'],'sourceRestartPolicy':saved['sourceRestartPolicy'],'closed':closed,'sourceFence':actual}
 entry.save_json(recovery.JOURNAL,journal);state['phase']='recovery-starting';entry.save_state(state)
 fd=os.open(entry.STATE.with_suffix('.lock'),os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
 try:
  fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);entry._recovery_controller_fd=fd
  return recovery.finish_recovery(entry,config,state,journal,local_assurance=True)
 finally:os.close(fd)


def deploy(image,config,archive,api):
 # fork继承现有deploy flock；独立session保证SSH/CI断开不杀停源后的闭环。
 validate_profile(private_json(PROFILE),image,config)
 RUN.mkdir(mode=0o700,parents=True,exist_ok=True)
 result_file=RUN/'runner-result.json'
 if result_file.exists():raise RuntimeError('Previous local release requires inspection')
 pid=os.fork()
 if pid==0:
  os.setsid();signal.signal(signal.SIGHUP,signal.SIG_IGN)
  null=os.open('/dev/null',os.O_RDONLY);os.dup2(null,0);os.close(null)
  log=os.open(RUN/'runner.log',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.dup2(log,1);os.dup2(log,2);os.close(log)
  birth=Path('/proc',str(os.getpid()),'stat').read_text().rsplit(')',1)[1].split()[19]
  fd=os.open(RUN/'runner-process.pending',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
  with os.fdopen(fd,'w') as file:json.dump({'pid':os.getpid(),'startTicks':birth,'session':os.getsid(0),'releaseId':RELEASE},file);file.flush();os.fsync(file.fileno())
  os.replace(RUN/'runner-process.pending',RUN/'runner-process.json')
  value={'contract':CONTRACT,'releaseId':RELEASE,'offhostVerified':False};code=1
  try:value['result']=run_release(image,config,archive,api);value['state']='complete';code=0
  except BaseException as error:value.update(state='failed-retained',errorCategory=type(error).__name__)
  finally:
   temporary=result_file.with_suffix('.pending');fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
   with os.fdopen(fd,'w') as file:json.dump(value,file);file.write('\n');file.flush();os.fsync(file.fileno())
   os.replace(temporary,result_file)
   os._exit(code)
 _,status=os.waitpid(pid,0)
 result=private_json(result_file)
 if os.waitstatus_to_exitcode(status)!=0 or result.get('state')!='complete':raise RuntimeError('Local release failed; inspect durable journal and source recovery')
 return result['result']
