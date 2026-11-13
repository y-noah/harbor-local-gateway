"""Root-approved same-database PostgreSQL code upgrade; never migrates SQLite."""
from pathlib import Path
from types import SimpleNamespace
import copy,fcntl,hashlib,http.client,importlib.util,json,os,re,stat,time,traceback
from contextlib import contextmanager
CONTRACT='harbor-postgres-runtime-upgrade-v1'
TICKET=Path('/etc/harbor-postgres-runtime-upgrade.json')
TOOLS_MANIFEST=Path('/etc/harbor-postgres-runtime-tools.json')
HOST_FILES=('/usr/local/bin/harbor-gateway-deploy','/usr/local/lib/harbor/postgres-deploy.py','/usr/local/lib/harbor/gateway-entry-controller.py','/usr/local/lib/harbor/postgres-upgrade-deploy.py','/usr/local/lib/harbor/postgres-usage-cascade.py')
RELEASES=Path('/var/lib/harbor-postgres-upgrades')
ENTRY=Path('/usr/local/lib/harbor/gateway-entry-controller.py')
LOCAL=Path('/usr/local/lib/harbor/local-assurance-deploy.py')
STACK=Path('/opt/product-development/harbor-local-gateway')
GATE='harbor_postgres_upgrade_gate'
UUID=r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}'
DIGEST=r'sha256:[a-f0-9]{64}'

def need(ok,message):
 if not ok:raise RuntimeError('PostgreSQL runtime upgrade: '+message)

def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def private_json(path):
 path=Path(path)
 for parent in path.parents:
  info=parent.lstat();need(stat.S_ISDIR(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022,'untrusted configuration parent')
 fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
 with os.fdopen(fd,'rb') as f:
  info=os.fstat(f.fileno());need(stat.S_ISREG(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600 and info.st_nlink==1 and info.st_size<=262144,'untrusted configuration')
  raw=f.read(262145)
 need(len(raw)<=262144,'configuration budget');return json.loads(raw)

def load(path,name):
 path=Path(path)
 for parent in [path,*path.parents]:
  info=parent.lstat();need((stat.S_ISREG(info.st_mode) if parent==path else stat.S_ISDIR(info.st_mode)) and info.st_uid==0 and not info.st_mode&0o022,'untrusted installed helper')
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

def trusted_code_bytes(path):
 path=Path(path)
 for parent in path.parents:
  info=parent.lstat();need(stat.S_ISDIR(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022,'untrusted host tool parent')
 fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
 with os.fdopen(fd,'rb') as file:
  info=os.fstat(file.fileno());need(stat.S_ISREG(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022 and info.st_nlink==1 and info.st_size<=1048576,'untrusted host tool')
  raw=file.read(1048577);need(len(raw)<=1048576,'host tool budget');return raw

def verify_installed_tools(ticket):
 manifest=private_json(TOOLS_MANIFEST)
 need(isinstance(manifest,dict) and set(manifest)=={'version','contract','revision','files'} and type(manifest['version']) is int and manifest['version']==1 and manifest['contract']=='harbor-postgres-runtime-tools-v1','host installation manifest')
 need(manifest['revision']==ticket['hostToolsRevision'] and re.fullmatch('[a-f0-9]{40}',manifest['revision']),'installed host tools revision differs from approval')
 need(isinstance(manifest['files'],dict) and set(manifest['files'])==set(HOST_FILES) and all(isinstance(value,str) and re.fullmatch('[a-f0-9]{64}',value) for value in manifest['files'].values()),'fixed host installation files')
 for path,expected in manifest['files'].items():need(hashlib.sha256(trusted_code_bytes(path)).hexdigest()==expected,'installed host tool bytes differ from approval manifest')
 return manifest

def approval_binding(value):return {k:value.get(k) for k in ['contract','deploymentId','receiptDigest','configDigest','manifestDigest','revision','sourceFence']}

def active_identity(active):
 return {k:copy.deepcopy(active.get(k)) for k in ['version','image','approval','configDigest','mounts','storageMarkerDigest','storageMarkerPath']}|{'candidate':{k:active.get('candidate',{}).get(k) for k in ['containerId','imageId','port']}}

def validate_ticket(ticket,image,config):
 need(isinstance(ticket,dict) and ticket.get('version')==1 and ticket.get('contract')==CONTRACT and ticket.get('approved') is True,'one-time root approval required')
 need(type(ticket.get('interruptEntryClients',False)) is bool,'entry interruption approval must be boolean')
 need(re.fullmatch(UUID,ticket.get('deploymentId','')) and re.fullmatch('[a-f0-9]{40}',ticket.get('hostToolsRevision','')),'release identity')
 target=ticket.get('to',{});source=ticket.get('from',{});network=ticket.get('network',{});database=ticket.get('database',{})
 need(config.get('port')==8092 and config.get('storage_engine')=='postgres' and config.get('local_assurance_release') is True and config.get('entry_installed_release') is True,'installed preserved PostgreSQL entry required')
 need(isinstance(image,str) and re.fullmatch(re.escape(config['image_prefix'])+':[a-f0-9]{40}',image) and target.get('image')==image and target.get('revision')==image.rsplit(':',1)[1] and all(re.fullmatch(DIGEST,target.get(k,'')) for k in ['manifestDigest','configDigest']),'approved target image')
 need(all(re.fullmatch('[a-f0-9]{64}',source.get(k,'')) for k in ['containerId','workerId','activeIdentityDigest']) and re.fullmatch(DIGEST,source.get('imageId','')) and isinstance(source.get('image'),str) and source['image']!=image,'approved PG source')
 materials=ticket.get('materialsDirectory','');need(re.fullmatch(re.escape(str(STACK))+r'/local-assurance/'+UUID,materials),'material scope')
 need(isinstance(database,dict) and set(database)=={'name','oid','cluster'} and re.fullmatch('[a-zA-Z_][a-zA-Z0-9_]{0,62}',database.get('name','')) and all(re.fullmatch('[0-9]{1,20}',database.get(k,'')) for k in ['oid','cluster']),'database identity')
 need(all(re.fullmatch('[a-f0-9]{64}',ticket.get(k,'')) for k in ['databaseDigest','schemaDigest','deviceCaDigest','storageMarkerDigest']),'material proof identity')
 need(network.get('name')==config.get('postgres_network') and isinstance(network.get('name'),str) and network['name']!='bridge' and re.fullmatch('[-a-zA-Z0-9_]{1,63}',network['name']) and re.fullmatch('[a-f0-9]{64}',network.get('id','')) and re.fullmatch('[a-zA-Z0-9_-]{1,15}',network.get('bridge','')) and type(network.get('bridgeIfindex')) is int and network['bridgeIfindex']>0 and re.fullmatch('(?:[a-f0-9]{2}:){5}[a-f0-9]{2}',network.get('bridgeMac','')) and isinstance(network.get('infrastructureContainerIds'),list) and network['infrastructureContainerIds'] and all(re.fullmatch('[a-f0-9]{64}',v) for v in network['infrastructureContainerIds']),'isolated network identity')
 return ticket

def release_path(identifier):
 need(isinstance(identifier,str) and re.fullmatch(UUID,identifier),'release identifier');return RELEASES/identifier

def approval(ticket):
 target=ticket['to'];return {'contract':CONTRACT,'deploymentId':ticket['deploymentId'],'receiptDigest':digest(ticket),'configDigest':target['configDigest'],'manifestDigest':target['manifestDigest'],'revision':target['revision'],'sourceFence':{'scope':'postgres-runtime-upgrade','sourceContainerId':ticket['from']['containerId'],'sourceImageId':ticket['from']['imageId'],'sameDatabase':True},'offhostVerified':False,'state':'pending'}

def record_for(authorization):
 need(authorization.get('contract')==CONTRACT,'upgrade authorization contract');record=private_json(release_path(authorization.get('deploymentId'))/'release.json');ticket=record['ticket']
 need(record.get('contract')==CONTRACT and record.get('ticketDigest')==digest(ticket) and approval_binding(authorization)==approval_binding(approval(ticket)),'durable upgrade binding');return record

def materials_directory(authorization):return record_for(authorization)['ticket']['materialsDirectory']

def check_storage(ticket):
 local=load(LOCAL,'upgrade_storage_identity');actual=local.helper('check',180)
 need(actual.get('identityAndDataVerified') is True and actual.get('database')==ticket['database'] and actual.get('materialsDirectory')==ticket['materialsDirectory'] and actual.get('connectionEnvFile')==ticket['materialsDirectory']+'/secrets/postgres.env','same database and runtime materials')
 marker=private_json(Path(ticket['materialsDirectory'])/'postgres-storage.json')
 need(marker=={'contract':'postgres-conversation-v2','database_digest':ticket['databaseDigest']} and digest(marker)==ticket['storageMarkerDigest'],'same database marker');return actual

def verify_candidate_network(row,authorization):
 network=record_for(authorization)['ticket']['network'];attachments=row.get('NetworkSettings',{}).get('Networks',{})
 need(set(attachments)=={network['name']} and attachments[network['name']].get('NetworkID')==network['id'],'candidate isolated network')

def active_authorization(image):
 entry=load(ENTRY,'upgrade_active_entry');active=entry.private_json(entry.ACTIVE);record=record_for(active.get('approval',{}));ticket=record['ticket']
 need(record.get('phase') in ['committing','committed','complete'] and record.get('candidate',{}).get('containerId')==active.get('candidate',{}).get('containerId') and image==ticket['to']['image'] and active.get('image')==image,'current durable PG release')
 check_storage(ticket);return {**approval(ticket),'state':'active'}

def verified_marker_path(image,active):
 record=record_for(active.get('approval',{}));ticket=record['ticket'];path=Path(ticket['materialsDirectory'])/'postgres-storage.json'
 need(image==ticket['to']['image'] and active.get('storageMarkerPath')==str(path) and digest(private_json(path))==ticket['storageMarkerDigest'],'active storage marker');return path

def verify_source(ticket,active,gateway,worker):
 source=ticket['from'];need(digest(active_identity(active))==source['activeIdentityDigest'] and active['image']==source['image'] and active['candidate']['containerId']==source['containerId'] and active['candidate']['imageId']==source['imageId'],'from ACTIVE changed')
 for row,identifier,role in [(gateway,source['containerId'],'gateway'),(worker,source['workerId'],'worker')]:
  need(row and row.get('Id')==identifier and row.get('Image')==source['imageId'] and row.get('Config',{}).get('Image')==source['image'] and row['Config'].get('Cmd')==['node','/app/postgres-'+role+'.mjs'] and row['Config'].get('Labels',{}).get('com.aistrong.project')=='harbor-local-gateway' and row.get('State',{}).get('Running') is True,'from PostgreSQL composition changed')
 expected=sorted([{'source':ticket['materialsDirectory']+'/'+name,'destination':destination,'readWrite':True} for name,destination in [('pg-data','/app/data'),('runtime','/app/runtime')]],key=lambda v:v['destination'])
 need(active.get('mounts')==expected and active.get('storageMarkerDigest')==ticket['storageMarkerDigest'],'from material binding')

def verify_target(ticket,gateway,worker):
 target=approval(ticket)
 for row,role in [(gateway,'gateway'),(worker,'worker')]:
  config=(row or {}).get('Config',{});labels=config.get('Labels',{});state=(row or {}).get('State',{})
  need(row and re.fullmatch('[a-f0-9]{64}',row.get('Id','')) and row['Id'] not in [ticket['from']['containerId'],ticket['from']['workerId']] and row.get('Image') in [target['configDigest'],target['manifestDigest']] and config.get('Image')==ticket['to']['image'] and config.get('Cmd')==['node','/app/postgres-'+role+'.mjs'] and labels.get('com.aistrong.project')=='harbor-local-gateway' and labels.get('com.harbor.activation-deployment-id')==target['deploymentId'] and labels.get('com.harbor.activation-receipt')==target['receiptDigest'] and state.get('Running') is True and not state.get('Paused') and not state.get('Restarting') and state.get('Health',{}).get('Status')=='healthy','new owned PostgreSQL composition')
  mounts={v.get('Destination'):(v.get('Source'),v.get('RW')) for v in row.get('Mounts',[]) if v.get('Destination') in ['/app/data','/app/runtime']}
  need(mounts=={'/app/data':(ticket['materialsDirectory']+'/pg-data',True),'/app/runtime':(ticket['materialsDirectory']+'/runtime',True)},'new PG material mounts')
  attachments=row.get('NetworkSettings',{}).get('Networks',{});need(set(attachments)=={ticket['network']['name']} and attachments[ticket['network']['name']].get('NetworkID')==ticket['network']['id'],'new PG network')
 need(gateway['Id']!=worker['Id'],'distinct PostgreSQL composition members')

def retire_sqlite(api,ticket):
 name=api['NAME'];row=api['inspect_container'](name+'-previous')
 if not row:return None
 source=ticket.get('sqliteRetirement',{});need(re.fullmatch('[a-f0-9]{64}',source.get('containerId','')) and re.fullmatch(DIGEST,source.get('imageId','')),'previous requires exact SQLite retirement approval')
 state=row.get('State',{});need(row.get('Id')==source['containerId'] and row.get('Image')==source['imageId'] and row.get('Config',{}).get('Cmd') in [['node','server.mjs'],['node','/app/server.mjs']] and row['Config'].get('Labels',{}).get('com.aistrong.project')==name and state.get('Running') is False and state.get('Pid',0)==0 and state.get('Status')=='exited' and not any(state.get(k) for k in ['Paused','Restarting','Dead','OOMKilled','Error']) and row.get('HostConfig',{}).get('RestartPolicy',{}).get('Name')=='no','previous is not the approved stopped SQLite')
 target=name+'-sqlite-retired-'+source['containerId'][:12];need(api['inspect_container'](target) is None,'retired SQLite name occupied');api['docker']('rename',row['Id'],target);retired=api['inspect_container'](target);need(retired and retired['Id']==row['Id'] and retired['State']['Running'] is False and api['inspect_container'](name+'-previous') is None,'SQLite retirement unknown');return target

def network_snapshot(ticket,entry,gateway,worker):
 expected=ticket['network'];rows=json.loads(entry.execute(['/usr/bin/docker','network','inspect',expected['name']]))
 need(len(rows)==1 and rows[0].get('Id')==expected['id'] and rows[0].get('Driver')=='bridge' and rows[0].get('Internal') is False and set(rows[0].get('Containers',{}))==set(expected['infrastructureContainerIds']+[gateway['Id'],worker['Id']]),'isolated network membership changed')
 links=json.loads(entry.execute(['/usr/sbin/ip','-j','link','show','dev',expected['bridge']]))
 need(len(links)==1 and links[0]['ifindex']==expected['bridgeIfindex'] and links[0]['address']==expected['bridgeMac'],'isolated bridge changed')
 ports=json.loads(entry.execute(['/usr/sbin/ip','-j','link','show','master',expected['bridge']]))
 return {'candidateNetworkIdentity':{'id':expected['id'],'name':expected['name'],'bridge':expected['bridge'],'ifindex':expected['bridgeIfindex'],'mac':expected['bridgeMac'],'ports':{v['ifname']:v['ifindex'] for v in ports}},'candidateNetworkContainerIds':list(rows[0]['Containers'])}

def gate_snapshot(entry):
 value=json.loads(entry.execute(['/usr/sbin/nft','--json','list','table','inet',GATE]))
 def stable(v):
  if isinstance(v,dict):return {k:stable(x) for k,x in v.items() if k not in ['handle','metainfo']}
  if isinstance(v,list):return [stable(x) for x in v if not isinstance(x,dict) or 'metainfo' not in x]
  return v
 return stable(value)

def gate_install(entry,ticket,config):
 port=config['device_tls_port'];need(type(port) is int and 1<=port<=65535 and port!=8092,'device endpoint')
 tables=json.loads(entry.execute(['/usr/sbin/nft','--json','list','tables']));need(not any(v.get('table',{}).get('name')==GATE for v in tables.get('nftables',[])),'upgrade gate already exists')
 entry.execute(['/usr/sbin/nft','-f','-'],body=f'add table inet {GATE}\nadd chain inet {GATE} ingress {{ type filter hook prerouting priority -110; policy accept; }}\nadd rule inet {GATE} ingress iifname != "lo" ct state new tcp dport {port} drop\nadd chain inet {GATE} backend {{ type filter hook forward priority -110; policy accept; }}\nadd rule inet {GATE} backend oifname "{ticket["network"]["bridge"]}" ct state new tcp dport 8092 drop\n')
 return gate_snapshot(entry)

def gate_remove(entry,snapshot):
 need(gate_snapshot(entry)==snapshot,'upgrade gate changed');entry.execute(['/usr/sbin/nft','delete','table','inet',GATE])

def probe(entry,ticket,row,port,path,headers=None):
 need(port==8092 and path in ['/health','/ready','/api/admin/state'],'probe scope')
 networks=row.get('NetworkSettings',{}).get('Networks',{});need(set(networks)=={ticket['network']['name']} and networks[ticket['network']['name']].get('NetworkID')==ticket['network']['id'],'probe network identity')
 address=networks[ticket['network']['name']]['IPAddress'];client=http.client.HTTPConnection(address,port,timeout=5)
 try:client.request('GET',path,headers={'Host':'127.0.0.1:8092',**(headers or {})});response=client.getresponse();response.read(65536);return response.status
 except (OSError,http.client.HTTPException):return 0
 finally:client.close()

def publish(entry,ticket,record,gateway,worker,old_active):
 verify_target(ticket,gateway,worker);check_storage(ticket);network=network_snapshot(ticket,entry,gateway,worker);target=approval(ticket);profile=entry.private_json(entry.CANDIDATE_PROFILE);profile.update(sourceAddress=gateway['NetworkSettings']['Networks'][ticket['network']['name']]['IPAddress'],sourcePort=8092)
 candidate={'containerId':gateway['Id'],'imageId':gateway['Image'],'address':profile['sourceAddress'],'port':8092}
 record.update(phase='committing',candidate=candidate,workerId=worker['Id']);entry.save_json(release_path(ticket['deploymentId'])/'release.json',record)
 active={**copy.deepcopy(old_active),'image':ticket['to']['image'],'approval':approval_binding(target),'candidate':candidate,'candidateProfileDigest':entry.fingerprint(profile),'storageMarkerPath':ticket['materialsDirectory']+'/postgres-storage.json','networkSnapshot':{**copy.deepcopy(old_active.get('networkSnapshot',{})),**network}}
 # From here a restart must select this candidate, never automatically undo the
 # durable decision after it could have accepted new writes.
 entry.save_json(entry.ACTIVE,active);entry.save_json(entry.CANDIDATE_PROFILE,profile);entry.prepare_active_network()
 control=entry.entry_state();need(control.get('slot')=='candidate' and control.get('phase')=='drained' and control.get('inflight')==0,'entry no longer drained')
 entry.entry_state('activate',control['epoch'],'candidate');entry.restore_active();record['phase']='committed';entry.save_json(release_path(ticket['deploymentId'])/'release.json',record)
 return active

@contextmanager
def controller_lock(entry):
 fd=os.open(entry.STATE.with_suffix('.lock'),os.O_RDWR|os.O_NOFOLLOW)
 try:
  info=os.fstat(fd);need(stat.S_ISREG(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600 and info.st_nlink==1,'controller lock identity');fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);yield
 finally:os.close(fd)

def supervised_upgrade(entry,ticket,operation):
 # The child keeps both inherited deployment locks and its own session. An SSH
 # disconnect may stop the CI observer, but cannot abandon a held public entry.
 root=RELEASES/'runners';root.mkdir(mode=0o700,parents=True,exist_ok=True)
 run=root/ticket['deploymentId'];run.mkdir(mode=0o700)
 out=os.open(run/'runner.log',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
 pid=os.fork()
 if pid==0:
  try:
   os.setsid();null=os.open('/dev/null',os.O_RDONLY)
   os.dup2(null,0);os.close(null);os.dup2(out,1);os.dup2(out,2);os.close(out)
   entry.save_json(run/'process.json',{'pid':os.getpid(),'startTicks':entry.process_ticks(os.getpid()),'deploymentId':ticket['deploymentId']})
   try:
    result=operation();entry.save_json(run/'result.json',{'ok':True,'result':result});os._exit(0)
   except BaseException as error:
    traceback.print_exc();entry.save_json(run/'result.json',{'ok':False,'errorCategory':type(error).__name__});os._exit(1)
  except BaseException:traceback.print_exc();os._exit(2)
 os.close(out)
 while True:
  try:_,status=os.waitpid(pid,0);break
  except InterruptedError:continue
 need(os.WIFEXITED(status),'upgrade runner exited without a confirmed result; inspect retained release')
 result=entry.private_json(run/'result.json')
 need(os.WEXITSTATUS(status)==0 and result.get('ok') is True,'upgrade failed; inspect private runner log and retained release')
 return result['result']

METADATA_FILES=(Path('/var/lib/containerd/io.containerd.metadata.v1.bolt/meta.db'),Path('/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/metadata.db'))
METADATA_MAX_BYTES=512*1024*1024

@contextmanager
def metadata_residency(config):
 enabled=config.get('postgres_metadata_residency',False)
 need(type(enabled) is bool,'metadata residency must be a root boolean')
 if not enabled:
  yield;return
 import ctypes,resource
 lib=ctypes.CDLL(None,use_errno=True)
 lib.mmap.restype=ctypes.c_void_p;lib.mmap.argtypes=[ctypes.c_void_p,ctypes.c_size_t,ctypes.c_int,ctypes.c_int,ctypes.c_int,ctypes.c_long]
 lib.mlock.argtypes=[ctypes.c_void_p,ctypes.c_size_t];lib.munmap.argtypes=[ctypes.c_void_p,ctypes.c_size_t]
 files=[];sizes=[];mappings=[];previous=resource.getrlimit(resource.RLIMIT_MEMLOCK);changed=False
 try:
  total=0
  for path in METADATA_FILES:
   for parent in path.parents:
    info=parent.lstat();need(stat.S_ISDIR(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022,'metadata parent is not root controlled')
   fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK);files.append(fd);info=os.fstat(fd)
   need(stat.S_ISREG(info.st_mode) and info.st_uid==0 and info.st_nlink==1 and not info.st_mode&0o022 and info.st_size>0,'metadata file is not root controlled')
   total+=info.st_size;need(total<=METADATA_MAX_BYTES,'metadata residency exceeds memory budget');sizes.append(info.st_size)
  resource.setrlimit(resource.RLIMIT_MEMLOCK,(METADATA_MAX_BYTES,METADATA_MAX_BYTES));changed=True
  for fd,size in zip(files,sizes):
   need(os.fstat(fd).st_size==size,'metadata size changed before mapping')
   # This is a read-only performance aid, never a database snapshot or proof.
   # Kernel releases all mappings on process exit, including abnormal exit.
   ptr=lib.mmap(None,size,1,1,fd,0)
   if ptr==ctypes.c_void_p(-1).value:raise OSError(ctypes.get_errno(),'metadata mapping failed')
   mappings.append((ptr,size))
   if lib.mlock(ptr,size)!=0:raise OSError(ctypes.get_errno(),'metadata residency failed')
  yield
 finally:
  for ptr,size in reversed(mappings):lib.munmap(ptr,size)
  for fd in reversed(files):os.close(fd)
  if changed:resource.setrlimit(resource.RLIMIT_MEMLOCK,previous)

def run_with_metadata_residency(image,config,api,ticket,entry):
 with metadata_residency(config):return run_upgrade(image,config,api,ticket,entry)

def deploy(image,config,archive,api):
 need(archive is None,'installed CI image required');ticket=validate_ticket(private_json(TICKET),image,config);verify_installed_tools(ticket);entry=load(api['ENTRY_HELPER'],'runtime_upgrade_entry')
 with controller_lock(entry):return supervised_upgrade(entry,ticket,lambda:run_with_metadata_residency(image,config,api,ticket,entry))

def interrupt_entry_clients(entry,ticket,api,active,deadline,held_epoch):
 # Root ticket approval authorizes dropping only this entry's TCP clients.
 # The private Unix control socket and listening socket stay open.
 import subprocess
 need(ticket.get('interruptEntryClients') is True,'entry interruption not approved')
 runtime=load(Path(__file__).with_name('entry-runtime.py'),'upgrade_entry_interrupt_identity')
 profile=entry.validate(entry.private_json(entry.PROFILE));process_config=entry.private_json(entry.ENTRY_CONFIG);port=process_config.get('listenPort')
 need(type(port) is int and 1024<=port<=65535 and port==profile['entryPort'] and port!=8092,'entry interrupt listener configuration')
 row=runtime.inspect_entry(entry);pid=row.get('State',{}).get('Pid')
 need(type(pid) is int and pid>1 and row['State'].get('Running') is True and not any(row['State'].get(k) for k in ['Paused','Restarting','Dead']),'entry interrupt process state')
 ticks=entry.process_ticks(pid);namespace=Path('/proc',str(pid),'ns/net').stat();host_namespace=Path('/proc/self/ns/net').stat()
 need((namespace.st_dev,namespace.st_ino)==(host_namespace.st_dev,host_namespace.st_ino),'entry interrupt host namespace')
 def identity():
  need(entry.socket_owner(Path(entry.CONTROL))==pid and entry.process_ticks(pid)==ticks and entry.process_cgroup(pid)==profile['entryCgroup'],'entry interrupt process changed')
  current=runtime.inspect_entry(entry)
  need(current['Id']==row['Id'] and current['State'].get('Pid')==pid and current['State'].get('Running') is True and not any(current['State'].get(k) for k in ['Paused','Restarting','Dead']),'entry interrupt container changed')
  ns=Path('/proc',str(pid),'ns/net').stat();need((ns.st_dev,ns.st_ino)==(namespace.st_dev,namespace.st_ino),'entry interrupt namespace changed')
 def command(kill=False,listening=False):
  remaining=deadline-time.monotonic();need(remaining>0,'entry drain timeout')
  args=['/usr/bin/nsenter','-t',str(pid),'-n','/usr/bin/ss','-H','-O','-n','-t','-e','-p']
  if kill:args.append('-K')
  args+=['state','listening' if listening else 'established','( sport = :'+str(port)+' )']
  # ss can return zero with a SOCK_DESTROY error on stderr. Never interpret
  # unsupported/partial destruction as success, and never log endpoint output.
  value=subprocess.run(args,stdin=subprocess.DEVNULL,capture_output=True,text=True,timeout=min(15,remaining),env={'PATH':'/usr/sbin:/usr/bin:/sbin:/bin','LANG':'C'})
  need(value.returncode==0 and not value.stderr.strip() and len(value.stdout)<=1048576,'entry TCP destroy unsupported or unconfirmed' if kill else 'entry TCP inventory unavailable')
  return value.stdout.splitlines()
 for tool in ['/usr/bin/nsenter','/usr/bin/ss']:trusted_code_bytes(tool)
 identity()
 current_active=entry.private_json(entry.ACTIVE);verify_source(ticket,current_active,api['inspect_container'](api['NAME']),api['inspect_container'](api['NAME']+'-worker'))
 state=entry.private_json(entry.STATE);view=state.get('activeRuntimeConfig')
 need(state.get('phase')=='activated' and view and state.get('candidate')==active.get('candidate'),'entry interrupt active runtime changed')
 entry.verify_resources(state,view);network_snapshot(ticket,entry,api['inspect_container'](api['NAME']),api['inspect_container'](api['NAME']+'-worker'))
 def owned(lines):
  inodes=set()
  for fd in Path('/proc',str(pid),'fd').iterdir():
   try:target=os.readlink(fd)
   except FileNotFoundError:continue
   match=re.fullmatch(r'socket:\[([0-9]+)\]',target)
   if match:inodes.add(match[1])
  for line in lines:
   inode=re.findall(r'\bino:([0-9]+)\b',line);owners=re.findall(r'\bpid=([0-9]+),',line)
   need(len(inode)==1 and inode[0] in inodes and owners and set(owners)=={str(pid)},'entry TCP socket ownership unknown')
 listeners=command(listening=True);need(listeners,'entry TCP listener absent');owned(listeners)
 established=command();owned(established);identity()
 control=entry.entry_state(timeout=min(125,max(0.001,deadline-time.monotonic())))
 need(control.get('slot')=='candidate' and control.get('phase')=='holding' and control.get('epoch')==held_epoch,'entry interrupt hold changed')
 if not established:return {'tcpResetRequested':False,'observedClients':0}
 reported=command(kill=True);identity()
 return {'tcpResetRequested':True,'observedClients':len(established),'reportedSockets':len(reported)}

def drain_active_entry(entry,ticket=None,api=None,active=None):
 # Freeze new admission before observing inflight: continuous traffic cannot
 # prevent progress. Existing requests keep their original connections.
 config=entry.private_json(entry.ENTRY_CONFIG);budget=config.get('drainTimeoutMs',config.get('shutdownTimeoutMs'))
 need(type(budget) is int and 1<=budget<=2147483647,'entry drain budget')
 deadline=time.monotonic()+budget/1000
 def current(command=None,epoch=None):
  remaining=deadline-time.monotonic();need(remaining>0,'entry drain timeout')
  value=entry.entry_state(command,epoch,timeout=min(125,remaining))
  need(time.monotonic()<deadline,'entry drain timeout')
  return value
 control=current()
 need(control.get('slot')=='candidate' and control.get('phase')=='active' and type(control.get('epoch')) is int and control['epoch']>=0,'current entry is not active PG')
 epoch=control['epoch']+1;control=current('hold',control['epoch'])
 if ticket is not None and ticket.get('interruptEntryClients') is True:
  need(control.get('phase')=='holding' and control.get('slot')=='candidate' and control.get('epoch')==epoch,'entry interrupt hold CAS')
  interrupt_entry_clients(entry,ticket,api,active,deadline,epoch);control=current()
 while True:
  need(control.get('slot')=='candidate' and control.get('phase')=='holding' and control.get('epoch')==epoch,'held entry generation changed')
  count=control.get('inflight');need(type(count) is int and count>=0,'entry drain count')
  if count==0:break
  time.sleep(min(0.1,max(0,deadline-time.monotonic())))
  control=current()
 # Only issue drain once idle, so its long-running operation cannot lock the
 # control socket against a guarded resume while requests are still running.
 control=current('drain',epoch)
 need(control.get('slot')=='candidate' and control.get('phase')=='drained' and control.get('epoch')==epoch+1 and control.get('inflight')==0,'entry drain failed')
 return control

CONSUMPTION_POLICY_CONTRACT='v1'
SOURCE_CONSUMPTION_QUERY="""
WITH fields AS (
 SELECT a.attname,format_type(a.atttypid,a.atttypmod) kind,a.attnotnull,pg_get_expr(d.adbin,d.adrelid) default_value
 FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
 WHERE a.attrelid='harbor.keys'::regclass AND a.attname IN ('consumption_policy','consumption_policy_version') AND a.attnum>0 AND NOT a.attisdropped
), shape AS (
 SELECT count(*) total,count(*) FILTER(WHERE attnotnull AND (attname='consumption_policy' AND kind='text' AND default_value='''inherit''::text' OR attname='consumption_policy_version' AND kind='bigint' AND default_value='0')) valid FROM fields
), current_keys AS MATERIALIZED (SELECT to_jsonb(k) value FROM harbor.keys k WHERE k.removed_at IS NULL)
SELECT COALESCE(bool_or(value->>'consumption_policy' IN ('api_only','subscription_only','api_first')),false) explicit,
 (SELECT total NOT IN (0,2) OR total=2 AND valid<>2 FROM shape) OR COALESCE(bool_or(CASE
 WHEN NOT(value?'consumption_policy') AND NOT(value?'consumption_policy_version') THEN false
 WHEN jsonb_typeof(value->'consumption_policy') IS DISTINCT FROM 'string' OR jsonb_typeof(value->'consumption_policy_version') IS DISTINCT FROM 'number' THEN true
 WHEN value->>'consumption_policy' NOT IN ('inherit','api_only','subscription_only','api_first') THEN true
 WHEN value->>'consumption_policy_version' !~ '^(0|[1-9][0-9]*)$' THEN true
 ELSE (value->>'consumption_policy_version')::numeric>9007199254740991 END),false) invalid
FROM current_keys
"""

def consumption_contract(proof):
 need(type(proof) is dict,'consumption_policy_proof_unknown')
 if 'consumption_policy_contract' not in proof:return None
 need(type(proof['consumption_policy_contract']) is str and proof['consumption_policy_contract']==CONSUMPTION_POLICY_CONTRACT,'consumption_policy_contract_unknown')
 return CONSUMPTION_POLICY_CONTRACT

def source_consumption_snapshot(api,ticket,entry,instance):
 verify_source(ticket,entry.private_json(entry.ACTIVE),api['inspect_container'](api['NAME']),api['inspect_container'](api['NAME']+'-worker'))
 need(instance.get('Id')==ticket['from']['containerId'] and instance.get('Image')==ticket['from']['imageId'],'consumption_policy_source_identity_changed')
 timeout=int(api['postgres_preflight_timeout_seconds'])
 program="import {createHash} from 'node:crypto';import {createRecordingProbeStore} from '/app/postgres-recording-storage.mjs';import {recordingDatabaseIdentity} from '/app/postgres-storage-report.mjs';const store=createRecordingProbeStore({connectionString:process.env.HARBOR_DATABASE_URL});const deadline=setTimeout(()=>{console.error('Source consumption query timed out');process.exit(1);},"+str(timeout*1000)+");try{const value=await store.transaction(async()=>{const identity=await recordingDatabaseIdentity(store);const row=await store.get("+json.dumps(SOURCE_CONSUMPTION_QUERY)+");return {contract:'harbor-consumption-legacy-key-snapshot-v1',database_digest:createHash('sha256').update(identity.cluster+'\\0'+identity.database).digest('hex'),...row};},{level:'REPEATABLE READ',readOnly:true});console.log(JSON.stringify(value));}catch{console.error('Source consumption query unavailable');process.exitCode=1;}finally{clearTimeout(deadline);await store.close();}"
 try:
  result=api['docker']('exec',instance['Id'],'node','--input-type=module','-e',program,timeout=timeout)
  need(len(result.stdout)<=4096,'consumption_policy_query_budget');value=json.loads(result.stdout)
 except Exception as error:raise RuntimeError('PostgreSQL runtime upgrade: consumption_policy_query_unavailable') from error
 need(type(value) is dict and set(value)=={'contract','database_digest','explicit','invalid'} and value['contract']=='harbor-consumption-legacy-key-snapshot-v1' and value['database_digest']==ticket['databaseDigest'] and type(value['explicit']) is bool and type(value['invalid']) is bool,'consumption_policy_query_unknown')
 need(value['invalid'] is False,'consumption_policy_metadata_unknown')
 need(value['explicit'] is False,'consumption_policy_explicit_keys_require_v1_target')
 return value

def consumption_upgrade_guard(pg,api,ticket,entry,gateway,common,target_proof):
 if consumption_contract(target_proof)==CONSUMPTION_POLICY_CONTRACT:return
 original=api['source_image_preflight']
 def guarded(instance,requested):
  try:
   proof=original(instance,requested)
   # Holding 8092 does not freeze TLS keepalive/loopback management writes.
   # Refuse a contract downgrade independently of the current Key snapshot.
   need(consumption_contract(proof) is None,'consumption_policy_target_unsupported: 旧目标不支持来源策略，不能回退来源策略版本（全inherit也不例外）')
   if instance.get('Id')==ticket['from']['containerId']:source_consumption_snapshot(api,ticket,entry,instance)
   return proof
  except pg.SourcePreflightRejected:raise
  except Exception as error:raise pg.SourcePreflightRejected(str(error)) from error
 api['source_image_preflight']=guarded
 values=pg.container_shutdown(api,gateway,'gateway');need(type(values) is dict,'consumption_policy_source_shutdown_unknown')
 requested=[next((label+'='+str(values[key]) for key,label in pg.SHUTDOWN_ENV.items() if item.startswith(label+'=')),item) for item in common]
 guarded(gateway,requested)  # Reject before hold; the existing stop path repeats this proof.

def run_upgrade(image,config,api,ticket,entry):
 pg=load(Path(__file__).with_name('postgres-deploy.py'),'runtime_upgrade_composition')
 preflight_timeout=pg.preflight_timeout_seconds(config.get('postgres_preflight_timeout_seconds',30))
 active,original,gateway=entry.active_record();worker=api['inspect_container'](api['NAME']+'-worker');verify_source(ticket,active,gateway,worker);check_storage(ticket)
 pg_api={**api,'STACK':ticket['materialsDirectory'],'postgres_preflight_timeout_seconds':preflight_timeout};common,_=pg.configuration(config,pg_api['STACK']);pg.verify_candidate_image(api['inspect_image'](image),approval(ticket),image);proof=pg.image_proof(pg_api,image,common)
 need(all(proof[k]==ticket[v] for k,v in [('database_digest','databaseDigest'),('schema_digest','schemaDigest'),('device_ca_digest','deviceCaDigest')]),'new image changed database/schema/device identity')
 pg.container_shutdown(pg_api,gateway,'gateway');pg.container_shutdown(pg_api,worker,'worker')
 pg_api['source_image_preflight']=pg.prepare_source_image_preflights(pg_api,[(gateway,'gateway'),(worker,'worker')],common)
 consumption_upgrade_guard(pg,pg_api,ticket,entry,gateway,common,proof)
 network_snapshot(ticket,entry,gateway,worker)
 RELEASES.mkdir(mode=0o700,exist_ok=True);path=release_path(ticket['deploymentId']);path.mkdir(mode=0o700);record={'contract':CONTRACT,'ticket':ticket,'ticketDigest':digest(ticket),'phase':'prepared','offhostVerified':False};entry.save_json(path/'release.json',record);entry.save_json(path/'active.before.json',active)
 context=SimpleNamespace(config=original,state=entry.private_json(entry.STATE));entry.EntryDeployment.enter_probe_worker(context);entry.EntryDeployment.allow_probe_worker(context)
 active=entry.private_json(entry.ACTIVE);record['fromActive']=active;entry.save_json(path/'release.json',record);gate=None;gate_attempted=False;commit_started=False
 if proof.get('usage_rollup_cascade_contract') is not None:
  cascade=load(Path(__file__).with_name('postgres-usage-cascade.py'),'runtime_upgrade_usage_cascade')
  pg_api['usage_cascade_release']=cascade.UsageCascadeRelease(api=api,entry=entry,ticket=ticket,active=active,gateway=gateway,worker=worker,image=image,proof=proof,record=record,path=path)
 target=approval(ticket);activation_state='pending'
 def activation(action,requested):
  nonlocal activation_state
  need(requested==image and digest(private_json(TICKET))==record['ticketDigest'],'one-time ticket changed')
  if action=='consume':activation_state='active'
  elif action=='revoke':activation_state='failed' # Same DB: never revoke the shared runtime role.
  elif action not in ['check','preflight','status']:raise ValueError('Unknown PG upgrade action')
  return {**target,'state':activation_state,**({'proof':proof} if action=='preflight' else {})}
 def http_code(port,path,headers=None):
  row=api['inspect_container'](api['NAME']);need(row and row['Config'].get('Image')==image and row.get('Image') in [target['configDigest'],target['manifestDigest']] and row['Config'].get('Labels',{}).get('com.harbor.activation-deployment-id')==target['deploymentId'] and row['Config']['Labels'].get('com.harbor.activation-receipt')==target['receiptDigest'],'probe candidate identity')
  return probe(entry,ticket,row,port,path,headers)
 verify_installed_tools(ticket)
 try:
  record['phase']='draining-entry';entry.save_json(path/'release.json',record)
  control=drain_active_entry(entry,ticket,api,active)
  gate_attempted=True;gate=gate_install(entry,ticket,config);record.update(phase='holding',gate=gate);entry.save_json(path/'release.json',record);record['retiredSqlite']=retire_sqlite(api,ticket);entry.save_json(path/'release.json',record)
  pg_api.update(runtime_upgrade=True,retain_previous=True,activation=activation,http_code=http_code)
  result=pg.deploy(image,config,None,pg_api);new_gateway=api['inspect_container'](api['NAME']);new_worker=api['inspect_container'](api['NAME']+'-worker')
  commit_started=True;publish(entry,ticket,record,new_gateway,new_worker,active);gate_remove(entry,gate);gate=None
  # Keep the previous PG code as stopped, explicitly retired containers, freeing
  # the ordinary previous slots for the next independently approved upgrade.
  for name,identifier in [(api['NAME'],ticket['from']['containerId']),(api['NAME']+'-worker',ticket['from']['workerId'])]:
   previous=api['inspect_container'](name+'-previous');need(previous and previous['Id']==identifier and previous['State']['Running'] is False,'previous PG retirement identity');retired=name+'-retired-pg-'+identifier[:12];need(api['inspect_container'](retired) is None,'retired PG name occupied');api['docker']('rename',identifier,retired)
  record['phase']='complete';entry.save_json(path/'release.json',record);return {'contract':CONTRACT,'state':'active','sameDatabase':True,'offhostVerified':False,'composition':result}
 except Exception as error:
  if isinstance(error,pg.ShutdownUnconfirmed):
   record.update(phase='stop-unknown',errorCategory='shutdown_unconfirmed');entry.save_json(path/'release.json',record);raise
  if not commit_started and not (gate_attempted and gate is None):
   old_gateway=api['inspect_container'](api['NAME']);old_worker=api['inspect_container'](api['NAME']+'-worker')
   verify_source(ticket,entry.private_json(entry.ACTIVE),old_gateway,old_worker)
   if not gate_attempted or isinstance(error,pg.SourcePreflightRejected):
    # Before gate installation or a source preflight rejection, no source
    # member was changed. Do not require the rejected recording-storage fact
    # again to restore the unchanged, core-ready composition.
    # Readiness must not depend on the held public admission path.
    # Check the exact unchanged PG pair via its private candidate bridge, then
    # release admission without rebuilding networking or replaying deployment.
    state=entry.private_json(entry.STATE);runtime=state.get('activeRuntimeConfig')
    need(state.get('phase')=='activated' and runtime and state.get('candidate')==active.get('candidate'),'unchanged entry runtime required')
    entry.verify_resources(state,runtime);network_snapshot(ticket,entry,old_gateway,old_worker)
    if gate_attempted:
     need(all(row.get('State',{}).get('Running') is True and not any(row['State'].get(k) for k in ['Paused','Restarting','Dead','OOMKilled']) for row in [old_gateway,old_worker]),'unchanged source is not running')
     need(probe(entry,ticket,old_gateway,8092,'/ready')==200,'unchanged candidate is not core-ready')
     gate_remove(entry,gate);gate=None
    else:need(entry.candidate_slot_ready(runtime['bridge']['address'],8092),'unchanged candidate is not ready')
    control=entry.entry_state();need(control.get('slot')=='candidate' and control.get('phase') in ['active','holding','drained'],'unchanged entry control required')
    if control['phase']!='active':entry.entry_state('resume',control['epoch'])
    record.update(phase='rolled-back-pg',errorCategory='source_preflight_rejected' if gate_attempted else 'before_composition');entry.save_json(path/'release.json',record)
    raise
   old_approval=active['approval'];pg.verify_started_composition({**pg_api,'http_code':lambda port,p,headers=None:probe(entry,ticket,old_gateway,port,p,headers)},ticket['from']['image'],config,int(time.time()*1000),old_approval)
   # Restarted PG containers have new veths. Rebind only the exact approved
   # restored pair and infrastructure before the existing guarded rebuild.
   restored=entry.private_json(entry.ACTIVE);restored['networkSnapshot']={**restored.get('networkSnapshot',{}),**network_snapshot(ticket,entry,old_gateway,old_worker)};entry.save_json(entry.ACTIVE,restored)
   entry.refresh_active()
   if gate is not None:gate_remove(entry,gate)
   record['phase']='rolled-back-pg';entry.save_json(path/'release.json',record)
  raise
