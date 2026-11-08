#!/usr/bin/python3
"""Fixed boot ordering for ephemeral IPC, high-port guard and committed PG.

Only host boot recreates an absent /run state. Candidate restore uses the
reviewed controller and refuses to remove its temporary guard while the entry
container has a live/unknown resource state. No source is ever started here.
"""
import fcntl,importlib.util,json,os,re,signal,stat,sys,time
from pathlib import Path
sys.dont_write_bytecode=True
ROOT=Path('/usr/local/lib/harbor')
RUN=Path('/run/harbor-entry')
CONTAINER_CONTRACT=Path('/etc/harbor-entry/container.json')
CANDIDATE_WAIT_SECONDS=180
TRANSIENT_READINESS={'entry_controller_candidate_not_running','entry_controller_active_database_not_ready','entry_controller_command_failed_docker'}
def need(value,code):
 if not value:raise RuntimeError('entry_runtime_'+code)
def trusted(path,directory=False):
 for item in ([path] if directory else [path,*path.parents]):
  info=item.lstat();need((stat.S_ISDIR(info.st_mode) if item!=path or directory else stat.S_ISREG(info.st_mode)) and info.st_uid==0 and not info.st_mode&0o022 and (item!=path or directory or info.st_nlink==1),'untrusted_code')
def module(name,path):
 trusted(path);spec=importlib.util.spec_from_file_location(name,path);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value
def validate_container_contract(value):
 need(isinstance(value,dict) and set(value)=={'version','containerId','imageId','memoryBytes','pidsLimit'} and type(value['version']) is int and value['version']==1,'container_contract')
 need(isinstance(value['containerId'],str) and re.fullmatch('[a-f0-9]{64}',value['containerId']) and isinstance(value['imageId'],str) and re.fullmatch('sha256:[a-f0-9]{64}',value['imageId']),'container_identity')
 need(type(value['memoryBytes']) is int and 0<value['memoryBytes']<2**63 and type(value['pidsLimit']) is int and 0<value['pidsLimit']<2**31,'container_resources')
 return value

def verify_entry(row,contract):
 contract=validate_container_contract(contract)
 need(row.get('Id')==contract['containerId'] and row['Image']==contract['imageId'] and row['Config']['Image']==contract['imageId'] and row['Config']['Labels'].get('com.aistrong.project')=='harbor-entry','entry_image')
 host=row['HostConfig'];need(host.get('Privileged') is False and host.get('CapAdd') in [None,[]] and type(host.get('Memory')) is int and host['Memory']==contract['memoryBytes'] and type(host.get('PidsLimit')) is int and host['PidsLimit']==contract['pidsLimit'],'entry_privilege_and_resources')
 need(host['NetworkMode']=='host' and host['RestartPolicy']['Name']=='no' and host['ReadonlyRootfs'] is True and host['CapDrop']==['ALL'] and host['UsernsMode']=='host' and row['Config']['User']=='0:0' and host['SecurityOpt']==['no-new-privileges:true'],'entry_isolation')
 need(row['Config'].get('Healthcheck',{}).get('Test')==['NONE'] and row['Config']['Entrypoint']==['/usr/local/bin/node'] and row['Config']['Cmd']==['/app/gateway-entry-process.mjs'],'entry_startup')
 expected={'/etc/harbor-entry':('/etc/harbor-entry',False),'/run/harbor-entry':('/run/harbor-entry',True),'/app':('/usr/local/lib/harbor/entry-app',False)}
 need(len(row['Mounts'])==3 and {value['Destination']:(value['Source'],value['RW']) for value in row['Mounts']}==expected,'entry_mounts')

def inspect_entry(c):
 contract=validate_container_contract(c.private_json(CONTAINER_CONTRACT))
 rows=json.loads(c.execute(['/usr/bin/docker','container','inspect','harbor-gateway-entry']));need(len(rows)==1,'entry_inventory');verify_entry(rows[0],contract);return rows[0]

def verify_start_guard(c):
 config=c.validate(c.private_json(c.PROFILE));state=c.private_json(c.STATE)
 need(state.get('phase') in ['guarded','boot-guarded','prepared','routed','withdrawn','active-prepared','activated'],'guard_start_phase')
 if state['phase'] in ['guarded','boot-guarded']:
  need(state.get('guardFingerprint')==c.guard_fingerprint(config) and state.get('rulesDigest')==c.state_rules_digest(),'actual_guard_changed')
 else:
  need(state.get('configDigest')==c.fingerprint(config),'guard_configuration_changed')
  view=state.get('activeRuntimeConfig',config);fenced=False
  if state['phase'] in ['activated','active-prepared']:
   # The durable ACTIVE decision and live PG identity authorize every restart.
   # The retired SQLite container is never a startup dependency or target.
   active,approved,_=c.active_record()
   need(approved==config and active.get('candidate')==state.get('candidate'),'active_guard_binding')
   if state.get('activeRuntimeConfig'):
    candidate=active['candidate'];source=view.get('source',{})
    need(source.get('containerId')==candidate.get('containerId') and source.get('imageId')==candidate.get('imageId') and source.get('address')==candidate.get('address'),'active_runtime_candidate_binding')
   else:fenced=True
  if fenced:c.verify_resources(state,config,fenced=True)
  else:c.verify_resources(state,view)
 return state

def ensure_run(c):
 c.trusted_parents(RUN)
 if not RUN.exists():RUN.mkdir(mode=0o700)
 info=RUN.lstat();need(stat.S_ISDIR(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o700,'runtime_directory')
def boot_guard(c):
 config=c.validate(c.private_json(c.PROFILE))
 if c.STATE.exists():verify_start_guard(c);return {'guardVerified':True,'ephemeralStateCreated':False}
 # The old kernel and /run state are gone. Unknown tables are never adopted.
 tables=json.loads(c.execute(['/usr/sbin/nft','--json','list','tables']));need(not any(row.get('table',{}).get('name')==c.TABLE for row in tables['nftables']),'boot_table_collision')
 state={'version':1,'phase':'boot-guard-starting','configDigest':c.fingerprint(config),'guardFingerprint':c.guard_fingerprint(config)};c.save_state(state);c.nft(c.guard_rules(config));state['phase']='boot-guarded';state['rulesDigest']=c.state_rules_digest();c.save_state(state)
 return {'guardVerified':True,'ephemeralStateCreated':True,'sourceStarted':False}
def candidate_post_once(c):
 verify_start_guard(c)
 if not c.ACTIVE.exists():return {'candidateOnly':False,'sourceStarted':False}
 state=c.private_json(c.STATE)
 if state['phase']=='boot-guarded':
  # restart=no ensures a normal host boot cannot publish this listener before
  # guard creation. Independently confirm the exact owned container is stopped.
  row=inspect_entry(c)
  trusted(ROOT/'storage-recovery/install-tools.py')
  inventory=module('harbor_runtime_inventory',ROOT/'storage-recovery/rotate-tools.py');need(not inventory.container_holds_resources(row),'entry_not_stopped')
  # The controller keeps this verified guard through every authorization read
  # and atomically replaces it in the same nft transaction as the full rules.
 value=c.refresh_active();need(value.get('phase') in ['active-prepared','activated'] or value.get('candidateOnly') is True,'candidate_restore')
 return {**value,'sourceStarted':False}
def wait_candidate(c,operation):
 deadline=time.monotonic()+CANDIDATE_WAIT_SECONDS
 while True:
  try:return operation()
  except RuntimeError as error:
   if str(error) not in TRANSIENT_READINESS:raise
   # An authorization/identity mismatch is never a readiness retry. Only the
   # fixed read command failing or an observed nonrunning candidate may wait.
   # A retry cannot adopt an altered or partially installed network journal.
   verify_start_guard(c);need(time.monotonic()<deadline,'candidate_readiness_timeout');time.sleep(min(.1,max(0,deadline-time.monotonic())))

def candidate_pre(c):return wait_candidate(c,lambda:c.slot_start('candidate'))
def candidate_post(c):return wait_candidate(c,lambda:candidate_post_once(c))

def entry_listener_ready(c):
 config=c.validate(c.private_json(c.PROFILE));process_config=c.private_json(c.ENTRY_CONFIG);port=process_config.get('listenPort')
 need(type(port) is int and port==config['entryPort'],'entry_listener_configuration')
 try:pid=c.socket_owner(Path(c.CONTROL))
 except (FileNotFoundError,ConnectionRefusedError):return False
 need(c.process_cgroup(pid)==config['entryCgroup'],'entry_control_identity')
 state=c.entry_state();need(state.get('slot')=='candidate' and state.get('phase')=='active','entry_control_state')
 inodes=set()
 for fd in Path('/proc',str(pid),'fd').iterdir():
  try:value=os.readlink(fd)
  except FileNotFoundError:continue
  if value.startswith('socket:[') and value.endswith(']'):inodes.add(value[8:-1])
 for family in ['tcp','tcp6']:
  path=Path('/proc',str(pid),'net',family)
  if not path.exists():continue
  for row in path.read_text().splitlines()[1:]:
   fields=row.split()
   if len(fields)>9 and fields[3]=='0A' and int(fields[1].split(':')[1],16)==port and fields[9] in inodes:return True
 return False

def entry_post(c):
 if not c.ACTIVE.exists():return {'candidateOnly':False,'sourceStarted':False}
 deadline=time.monotonic()+CANDIDATE_WAIT_SECONDS
 while True:
  verify_start_guard(c);ready=entry_listener_ready(c)
  if ready:
   verify_start_guard(c);return c.restore_active()
  need(time.monotonic()<deadline,'entry_readiness_timeout');time.sleep(min(.1,max(0,deadline-time.monotonic())))


def entry_pre(c):
 verify_start_guard(c);inspect_entry(c);return c.assert_start()
def main():
 need(os.geteuid()==0 and len(sys.argv)==2 and sys.argv[1] in ['boot-guard','candidate-pre','candidate-post','entry-pre','entry-post'],'fixed_root_action')
 c=module('fixed_entry_controller',ROOT/'gateway-entry-controller.py');ensure_run(c)
 fd=os.open(c.STATE.with_suffix('.lock'),os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
 try:
  info=os.fstat(fd);need(stat.S_ISREG(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600 and info.st_nlink==1,'runtime_lock');fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
  action=sys.argv[1]
  if action in ['candidate-pre','candidate-post','entry-pre','entry-post']:
   def expired(*args):raise RuntimeError('entry_runtime_entry_readiness_timeout' if action.startswith('entry-') else 'entry_runtime_candidate_readiness_timeout')
   signal.signal(signal.SIGALRM,expired);signal.setitimer(signal.ITIMER_REAL,CANDIDATE_WAIT_SECONDS)
  value=boot_guard(c) if action=='boot-guard' else candidate_pre(c) if action=='candidate-pre' else candidate_post(c) if action=='candidate-post' else entry_pre(c) if action=='entry-pre' else entry_post(c)
  print(json.dumps(value,separators=(',',':')))
 finally:signal.setitimer(signal.ITIMER_REAL,0);os.close(fd)
if __name__=='__main__':
 try:main()
 except BaseException as error:
  code=str(error) if isinstance(error,RuntimeError) and re.fullmatch('(?:entry_runtime|entry_install|entry_controller)_[a-z_]+',str(error)) else 'entry_runtime_failed_closed'
  print(json.dumps({'error':{'code':code},'sourceStarted':False}));raise SystemExit(1)
