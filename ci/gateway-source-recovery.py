#!/usr/bin/python3
"""Fixed-root, pre-writer recovery of the exact fenced Harbor source.

No caller-supplied proof, source, command, or container is accepted. The formal
verifier releases its SQLite lease before Docker starts. A write-ahead journal
then permits only this same CID to complete network admission after interruption.
"""
import copy
import fcntl
import importlib.util
import http.client
import json
import os
from pathlib import Path
import re
import stat
import socket
import sys
import time

CONTROLLER=Path('/usr/local/lib/harbor/gateway-entry-controller.py')
VERIFIER=Path('/usr/local/lib/harbor/verify-source-recovery.mjs')
NODE=Path('/opt/harbor-storage-tools/node/bin/node')
DEPLOY_LOCK=Path('/run/harbor-gateway-deploy.lock')
JOURNAL=Path('/var/lib/harbor-entry/source-recovery.json')
OPERATOR_PROFILE=Path('/etc/harbor-storage-operator.json')


def load_controller():
 for path in [CONTROLLER,VERIFIER,NODE]:
  for parent in path.parents:
   info=parent.lstat()
   if not stat.S_ISDIR(info.st_mode) or info.st_uid!=0 or info.st_mode&0o022:raise RuntimeError('source_recovery_untrusted_directory')
  info=path.lstat()
  if not stat.S_ISREG(info.st_mode) or info.st_uid!=0 or info.st_mode&0o022:raise RuntimeError('source_recovery_untrusted_code')
 spec=importlib.util.spec_from_file_location('harbor_fixed_entry_controller',CONTROLLER)
 module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def no_writer(c,state):
 c.require(not c.ACTIVE.exists() and not c.WRITER_AUTHORIZED.exists() and not state.get('writerAuthorized'),'recovery_writer_authorized')
 c.require(not any(state.get(key) for key in ['candidate','candidateIntent','candidateOperations','activeRuntimeConfig']),'recovery_candidate_history')


def inventory_module(c):
 path=Path(c.__file__).parent/'storage-recovery/rotate-tools.py'
 for code in [path,path.with_name('install-tools.py')]:
  c.trusted_parents(code);info=code.lstat();c.require(stat.S_ISREG(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022 and info.st_nlink==1,'recovery_inventory_code_untrusted')
 spec=importlib.util.spec_from_file_location('harbor_recovery_inventory',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def network_snapshot(c,runtime):
 links={row['ifname']:row['ifindex'] for row in json.loads(c.execute(['/usr/sbin/ip','-j','link','show']))}
 return {'phase':'withdrawn','rulesDigest':c.state_rules_digest(),'mangleDigest':c.mangle_digest(),'tcDigests':{dev:c.tc_digest(dev) for dev in runtime['bridge']['ports']},'deviceIndices':{dev:links[dev] for dev in runtime['bridge']['ports']}}


def verify_network_snapshot(c,snapshot,runtime):
 c.require(isinstance(snapshot,dict) and set(snapshot.get('deviceIndices',{}))==set(runtime['bridge']['ports']),'recovery_network_snapshot')
 links={row['ifname']:row['ifindex'] for row in json.loads(c.execute(['/usr/sbin/ip','-j','link','show']))}
 c.require(all(links.get(dev)==index for dev,index in snapshot['deviceIndices'].items()),'recovery_network_device_changed');c.verify_resources(snapshot,runtime)


def runtime_binding(runtime):
 return {'source':runtime['source'],'bridge':runtime['bridge'],'bypassProcesses':[{**proc,'cgroupInode':(Path('/sys/fs/cgroup')/proc['cgroup'].lstrip('/')).stat().st_ino} for proc in runtime['bypassProcesses']]}


def no_candidates(c,source):
 module=inventory_module(c)
 for row in c.local_container_inventory():
  if row['Id']==source['containerId']:continue
  labels=row.get('Config',{}).get('Labels') or {};name=row.get('Name','').lstrip('/')
  c.require(name not in ['harbor-local-gateway','harbor-local-gateway-worker'],'recovery_foreign_composition')
  if labels.get('com.aistrong.project')=='harbor-local-gateway':
   c.require(not labels.get('com.harbor.activation-deployment-id') and not module.container_holds_resources(row),'recovery_candidate_exists')


def validate_proof(c,proof,config,row):
 c.require(isinstance(proof,dict) and proof.get('contract')=='harbor-source-recovery-v1' and re.fullmatch('[a-f0-9-]{36}',proof.get('runId','')) and re.fullmatch('[a-f0-9]{64}',proof.get('configurationDigest','')),'recovery_proof')
 c.require(proof.get('supervisorKind')=='docker-restart' and proof.get('activationState') in ['pending','not-created'] and proof.get('deploymentActivationAbsent') is True and proof.get('rolesNoLogin') is True and type(proof.get('targetSessionCount')) is int and proof['targetSessionCount']==0 and proof.get('sourceVerified') is True and proof.get('registryVerified') is True and proof.get('inventoryVerified') is True,'recovery_authority')
 c.require(proof.get('sourceVerificationMode') in ['authenticated-captured-source-match','fenced-current-capture'],'recovery_source_verification')
 roles=proof.get('roleStates');c.require(isinstance(roles,dict) and set(roles)=={'migration','owner','runtime'} and all(value in ['NOLOGIN','not-created'] for value in roles.values()) and (roles['migration']=='NOLOGIN' or proof['activationState']=='not-created' and all(value=='not-created' for value in roles.values())),'recovery_role_state')
 source=config['source'];fence=proof.get('sourceFence') or {}
 c.require(fence.get('scope')=='docker-systemd' and fence.get('sourceContainerId')==source['containerId'] and fence.get('sourceImageId')==source['imageId'] and fence.get('sourceStartedAt')==source['startedAt'] and fence.get('sourceFinishedAt')==row['State']['FinishedAt'] and all(fence.get(key) is True for key in ['sourceStopped','supervisorFenced','productionVerified']),'recovery_fence')
 mounts=[v for v in row.get('Mounts',[]) if v.get('Destination')=='/app/data']
 c.require(len(mounts)==1 and mounts[0].get('Type')=='bind' and mounts[0].get('RW') is True and mounts[0].get('Source')==proof.get('sourceDataDir') and str(Path(proof['sourceDataDir']).resolve())==proof['sourceDataDir'],'recovery_source_mount')


def recover(c):
 config=c.validate(c.private_json(c.PROFILE));state=c.private_json(c.STATE);no_writer(c,state)
 if state.get('phase')=='routed' and state.get('recoveryComplete') is True:
  journal=c.private_json(JOURNAL);c.require(journal.get('phase')=='complete' and journal.get('runtimeConfigDigest')==c.fingerprint(config),'recovery_completed_changed');c.verify_source(config)
  return {'phase':'recovery-complete','sourceRunning':True,'freshRunRequired':True}
 c.require(state.get('phase') in ['holding','recovery-starting'],'recovery_phase');entry=c.entry_state()
 c.require(entry.get('slot')=='legacy' and entry.get('phase') in ['drained','active'] and entry.get('inflight')==0,'recovery_entry_changed')
 if entry['phase']=='active':c.require(state['phase']=='recovery-starting' and c.private_json(JOURNAL).get('phase')=='network-ready','recovery_premature_resume')
 else:c.require(entry.get('epoch')==state.get('drainReceipt',{}).get('entryEpoch'),'recovery_entry_epoch')
 if state['phase']=='holding' or not JOURNAL.exists():
  c.require(not JOURNAL.exists(),'recovery_journal_exists');c.require(state.get('configDigest')==c.fingerprint(config),'recovery_configuration_changed')
  row=c.verify_source(config,running=False);no_candidates(c,config['source'])
  # Verify all owned rules and exact bridge inventory before asking for data
  # recovery permission. Dead bridge processes are the failure being recovered;
  # their original cgroup rules remain covered by the exact mangle digest.
  c.verify_resources({**state,'phase':'holding'},config,fenced=True)
  bridge=json.loads(c.execute(['/usr/sbin/ip','-j','link','show','dev',config['bridge']['name']]))
  ports=json.loads(c.execute(['/usr/sbin/ip','-j','link','show','master',config['bridge']['name']]))
  c.require(len(bridge)==1 and bridge[0]['ifindex']==state.get('bridgeIfindex') and {row['ifname'] for row in ports}==set(config['bridge']['ports'])-{state.get('sourceBridgePort')},'recovery_network_environment_changed')
  c.require(c.process_cgroup(c.socket_owner(Path(c.CONTROL)))==config['entryCgroup'],'recovery_entry_identity_changed')
  state['phase']='recovery-starting';c.save_state(state)
  operator_digest=c.fingerprint(c.private_json(OPERATOR_PROFILE))
  proof=json.loads(c.execute([str(NODE),str(VERIFIER)],timeout=600));validate_proof(c,proof,config,row)
  c.require(c.fingerprint(c.private_json(OPERATOR_PROFILE))==operator_digest,'recovery_operator_profile_changed')
  # Recheck current stopped generation after the formal verifier's leases release.
  c.require(c.verify_source(config,running=False)['State']['FinishedAt']==row['State']['FinishedAt'],'recovery_source_changed');no_candidates(c,config['source'])
  journal={'version':1,'phase':'authorized','originalConfig':config,'originalState':state,'proof':proof,'sourceMounts':row.get('Mounts',[]),'sourceRestartPolicy':state.get('sourceRestartPolicy'),'operatorProfileDigest':operator_digest};c.save_json(JOURNAL,journal)
 else:
  # Missing proof after a verifier failure stays closed; never infer safe from absence.
  journal=c.private_json(JOURNAL);c.require(journal.get('version')==1 and journal.get('phase') in ['authorized','source-started','network-ready'],'recovery_journal')
  config=journal['originalConfig'];c.validate(config);c.require(state.get('configDigest')==c.fingerprint(config),'recovery_journal_configuration')
 source=config['source'];no_candidates(c,source);no_writer(c,state)
 if journal['phase']=='network-ready':verify_network_snapshot(c,journal.get('networkSnapshot'),journal.get('runtimeConfig',{}))
 # Disable the fixed old-run entry before restarting SQLite. The run remains
 # archived for diagnosis; a later migration must explicitly provision a new run.
 retired=OPERATOR_PROFILE.with_name('harbor-storage-operator.retired-'+journal['proof']['runId']+'.json')
 if OPERATOR_PROFILE.exists():
  operator=c.private_json(OPERATOR_PROFILE);c.require(c.fingerprint(operator)==journal['operatorProfileDigest'],'recovery_operator_profile_changed')
  if retired.exists():c.require(c.private_json(retired)==operator,'recovery_retired_profile_changed')
  else:c.save_json(retired,operator)
  OPERATOR_PROFILE.unlink();directory=os.open(OPERATOR_PROFILE.parent,os.O_RDONLY|os.O_DIRECTORY)
  try:os.fsync(directory)
  finally:os.close(directory)
 else:c.require(c.fingerprint(c.private_json(retired))==journal['operatorProfileDigest'],'recovery_retired_profile_missing')
 marker=c.STORAGE_MARKER
 if marker.exists():
  expected=journal['proof'].get('storageMarker');c.require(expected is not None and c.private_json(marker)==expected,'recovery_storage_marker_unknown')
  archived=marker.with_name('postgres-storage.retired-'+journal['proof']['runId']+'.json')
  if archived.exists():c.require(c.private_json(archived)==expected,'recovery_storage_archive_changed')
  else:c.save_json(archived,expected)
  marker.unlink()
  directory=os.open(marker.parent,os.O_RDONLY|os.O_DIRECTORY)
  try:os.fsync(directory)
  finally:os.close(directory)
 row=c.inspect_container(source['containerId']);c.require(row['Image']==source['imageId'] and row.get('Mounts',[])==journal['sourceMounts'],'recovery_source_materials_changed')
 if not row['State']['Running']:
  c.verify_source(config,running=False);c.execute(['/usr/bin/docker','start',source['containerId']],timeout=60)
 deadline=time.monotonic()+10
 while True:
  row=c.inspect_container(source['containerId']);status=row['State']
  if status['Running'] and status['Pid']>1 and not any(status.get(key) for key in ['Restarting','Paused','Dead','OOMKilled','Error']):break
  c.require(time.monotonic()<deadline,'recovery_source_start_timeout');time.sleep(.05)
 c.require(row['Image']==source['imageId'] and row.get('Mounts',[])==journal['sourceMounts'],'recovery_started_materials_changed')
 if journal['phase']=='authorized':journal['phase']='source-started';c.save_json(JOURNAL,journal)
 attachments=[v for v in row['NetworkSettings']['Networks'].values() if v.get('IPAddress')];c.require(len(attachments)==1,'recovery_source_network')
 attachment=attachments[0];runtime=copy.deepcopy(config);runtime['source'].update(pid=status['Pid'],startedAt=status['StartedAt'],startTicks=c.process_ticks(status['Pid']),address=attachment['IPAddress'])
 bridge=json.loads(c.execute(['/usr/sbin/ip','-j','link','show','dev',config['bridge']['name']]))[0]
 c.require(bridge['ifindex']==state.get('bridgeIfindex'),'recovery_bridge_changed')
 ports=json.loads(c.execute(['/usr/sbin/ip','-j','link','show','master',config['bridge']['name']]))
 runtime['bridge']['mac']=bridge['address'];runtime['bridge']['ports']=[v['ifname'] for v in ports];runtime['candidate']=None
 c.require(set(runtime['bridge']['ports'])==set(config['bridge']['ports'])-{state['sourceBridgePort']}|{c.source_bridge_port(runtime)},'recovery_foreign_bridge_port')
 if journal['phase']=='network-ready':c.require(runtime['source']==journal['runtimeConfig']['source'] and runtime['bridge']==journal['runtimeConfig']['bridge'],'recovery_runtime_source_changed')
 profile=c.private_json(c.LEGACY_PROFILE);profile['sourceAddress']=runtime['source']['address'];c.save_json(c.LEGACY_PROFILE,profile)
 try:c.socket_owner(Path(profile['socketPath']))
 except (FileNotFoundError,ConnectionRefusedError):
  # A failed old bridge is recoverable only after the verified source is live.
  # Its fixed slot-start checks the newly pinned source generation. Deployment
  # remains excluded by the outer mutex while this unit takes the state lock.
  c.save_json(c.PROFILE,runtime);fcntl.flock(c._recovery_controller_fd,fcntl.LOCK_UN)
  try:c.execute(['/usr/bin/systemctl','restart','harbor-entry-legacy.service'],timeout=30)
  finally:fcntl.flock(c._recovery_controller_fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
  deadline=time.monotonic()+5
  while True:
   try:c.socket_owner(Path(profile['socketPath']));break
   except (FileNotFoundError,ConnectionRefusedError):c.require(time.monotonic()<deadline,'recovery_legacy_bridge_start_timeout');time.sleep(.05)
 runtime['bypassProcesses']=[{key:value for key,value in proc.items() if key!='cgroupInode'} for proc in c.refreshed_bypass(config,'legacy')]
 c.verify_environment(runtime)
 binding=runtime_binding(runtime)
 if journal['phase']!='network-ready' or binding!=journal.get('runtimeBinding'):
  c.install_owned_network(runtime,journal['networkSnapshot'] if journal['phase']=='network-ready' else state)
 else:verify_network_snapshot(c,journal['networkSnapshot'],runtime)
 mac=attachment.get('MacAddress','');c.require(re.fullmatch('(?:[a-f0-9]{2}:){5}[a-f0-9]{2}',mac),'recovery_source_mac');c.execute(['/usr/sbin/ip','neigh','replace',runtime['source']['address'],'lladdr',mac,'nud','reachable','dev',runtime['bridge']['name']])
 # The fixed bridge reloads its root profile for every new connection.
 journal['runtimeConfig']=runtime;journal['runtimeBinding']=binding;journal['networkSnapshot']=network_snapshot(c,runtime);journal['runtimeConfigDigest']=c.fingerprint(runtime);journal['phase']='network-ready';c.save_json(JOURNAL,journal);c.save_json(c.PROFILE,runtime)
 newstate={'version':1,'configDigest':c.fingerprint(runtime),'phase':'withdrawn','source':runtime['source'],'sourceBridgePort':c.source_bridge_port(runtime),'sourceRestartPolicy':journal['sourceRestartPolicy'],'bridgeIfindex':bridge['ifindex'],'rulesDigest':c.state_rules_digest(),'mangleDigest':c.mangle_digest(),'tcDigests':{dev:c.tc_digest(dev) for dev in runtime['bridge']['ports']}}
 # Probe through the exact root-only bridge while entry remains drained.
 connection=http.client.HTTPConnection('localhost',timeout=3);connection.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);connection.sock.settimeout(3);connection.sock.connect('/run/harbor-entry/legacy.sock')
 try:
  connection.request('GET','/health',headers={'Host':'127.0.0.1:8092','x-harbor-entry-client-address':'127.0.0.1','Connection':'close'})
  response=connection.getresponse();c.require(response.status==200 and len(response.read(65537))<=65536,'recovery_source_health')
 finally:connection.close()
 # Same held entry preserves all queued requests and dispatches them to exact source.
 entry=c.entry_state();c.require(entry.get('slot')=='legacy' and entry.get('phase') in ['drained','active'],'recovery_entry_changed')
 if entry['phase']=='drained':entry=c.entry_state('resume',entry['epoch'])
 c.require(entry.get('slot')=='legacy' and entry.get('phase')=='active','recovery_resume_failed')
 policy=journal.get('sourceRestartPolicy');c.require(isinstance(policy,dict) and policy.get('Name') in ['no','always','unless-stopped','on-failure'] and type(policy.get('MaximumRetryCount')) is int,'recovery_restart_policy')
 value=policy['Name']+(':'+str(policy['MaximumRetryCount']) if policy['Name']=='on-failure' and policy['MaximumRetryCount'] else '')
 c.execute(['/usr/bin/docker','update','--restart='+value,source['containerId']]);c.nft(f'add element inet {c.TABLE} enabled_ports {{ {runtime["source"]["port"]} }}');newstate['phase']='routed';newstate['recoveryComplete']=True;c.save_state(newstate);journal['phase']='complete';c.save_json(JOURNAL,journal)
 return {'phase':'recovery-complete','sourceRunning':True,'freshRunRequired':True,'pgDataPreserved':True}


def main():
 if os.geteuid()!=0 or len(sys.argv)!=1:raise RuntimeError('source_recovery_fixed_root_only')
 c=load_controller();descriptors=[]
 try:
  for path in [DEPLOY_LOCK,c.STATE.with_suffix('.lock')]:
   c.trusted_parents(path);fd=os.open(path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600);descriptors.append(fd);info=os.fstat(fd)
   c.require(stat.S_ISREG(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600 and info.st_nlink==1,'recovery_lock_untrusted');fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
  c._recovery_controller_fd=descriptors[-1]
  print(json.dumps(recover(c),separators=(',',':')))
 finally:
  for fd in reversed(descriptors):os.close(fd)


if __name__=='__main__':
 try:main()
 except BaseException:
  print(json.dumps({'error':{'code':'source_recovery_failed_closed'},'automaticSqliteRollback':False}));raise SystemExit(1)
