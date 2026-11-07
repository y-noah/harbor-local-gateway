#!/usr/bin/python3
"""Fixed-root Harbor network admission controller; never stops a source.

Deployment owns final capture/import and activation. This helper only prepares
and arms ingress, holds/drains entry plus the pinned source socket namespace,
and withdraws NEW traffic while retaining NAT dependencies for existing flows.
"""
import fcntl
import importlib.util
import copy
import hashlib
import http.client
import ipaddress
import json
import os
from pathlib import Path
import re
import socket
import stat
import subprocess
import sys
import time

PROFILE=Path('/etc/harbor-entry/controller.json')
STATE=Path('/run/harbor-entry/controller-state.json')
TABLE='harbor_entry'
REPLY_CHAIN='HARBOR_ENTRY_REPLY'
BYPASS_CHAIN='HARBOR_ENTRY_BYPASS'
CONTROL='/run/harbor-entry/control.sock'
CANDIDATE_PROFILE=Path('/etc/harbor-entry/candidate/harbor-gateway-legacy-transparent.json')
LEGACY_PROFILE=Path('/etc/harbor-entry/legacy/harbor-gateway-legacy-transparent.json')
ACTIVE=Path('/var/lib/harbor-entry/active.json')
WRITER_AUTHORIZED=Path('/var/lib/harbor-entry/writer-authorized.json')
ENTRY_CONFIG=Path('/etc/harbor-entry/config.json')
STORAGE_MARKER=Path('/opt/product-development/harbor-local-gateway/postgres-storage.json')
DEPLOY_CGROUP='/system.slice/harbor-entry-deploy.service'


def require(value,code):
 if not value:raise RuntimeError('entry_controller_'+code)


def execute(args,body=None,timeout=15):
 result=subprocess.run(args,input=body,capture_output=True,text=True,timeout=timeout,env={'PATH':'/usr/sbin:/usr/bin:/sbin:/bin','LANG':'C'})
 require(result.returncode==0,'command_failed_'+Path(args[0]).name.replace('-','_'))
 require(len(result.stdout)<8*1024*1024,'output_budget')
 return result.stdout


def trusted_parents(path):
 for parent in path.parents:
  info=parent.lstat();require(stat.S_ISDIR(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022,'directory_untrusted')


def private_json(path):
 trusted_parents(path);fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
 try:
  info=os.fstat(fd);require(stat.S_ISREG(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600 and info.st_nlink==1 and info.st_size<=65536,'file_untrusted')
  data=os.read(fd,65537);require(len(data)<=65536,'file_budget');return json.loads(data)
 finally:os.close(fd)


def fingerprint(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def source_bridge_port(config):
 source=config['source'];links=json.loads(execute(['/usr/bin/nsenter','--net=/proc/'+str(source['pid'])+'/ns/net','/usr/sbin/ip','-j','link','show']))
 peers={row.get('link_index') for row in links if row.get('ifname')!='lo'}
 host=json.loads(execute(['/usr/sbin/ip','-j','link','show','master',config['bridge']['name']]))
 matches=[row['ifname'] for row in host if row.get('ifindex') in peers]
 require(len(matches)==1,'source_bridge_port_ambiguous');return matches[0]


def validate(config):
 require(isinstance(config,dict) and set(config)=={'version','source','entryPort','bridge','entryCgroup','bypassProcesses','mark','bypassMark','routeTable','rulePriority','tcPreference','drainTimeoutMs','candidate'},'configuration')
 require(config['version']==1 and isinstance(config['source'],dict) and set(config['source'])=={'containerId','imageId','pid','startedAt','startTicks','address','port'},'source_configuration')
 source=config['source'];require(re.fullmatch('[a-f0-9]{64}',source['containerId']) and re.fullmatch('sha256:[a-f0-9]{64}',source['imageId']),'source_identity')
 require(type(source['pid']) is int and source['pid']>1 and re.fullmatch('[0-9]+',str(source['startTicks'])) and isinstance(source['startedAt'],str),'source_generation')
 require(str(ipaddress.IPv4Address(source['address']))==source['address'] and type(source['port']) is int and source['port']==8092,'source_endpoint')
 bridge=config['bridge'];require(isinstance(bridge,dict) and set(bridge)=={'name','address','mac','ports'},'bridge_configuration')
 require(re.fullmatch('[a-zA-Z0-9_-]{1,15}',bridge['name']) and re.fullmatch('(?:[a-f0-9]{2}:){5}[a-f0-9]{2}',bridge['mac']),'bridge_identity')
 require(str(ipaddress.IPv4Address(bridge['address']))==bridge['address'] and isinstance(bridge['ports'],list) and bridge['ports'] and len(set(bridge['ports']))==len(bridge['ports']) and all(re.fullmatch('[a-zA-Z0-9_-]{1,15}',p) for p in bridge['ports']),'bridge_ports')
 for key,lower,upper in [('entryPort',1024,65535),('mark',1,0xffffffff),('bypassMark',1,0xffffffff),('routeTable',1,0x7fffffff),('rulePriority',1,32765),('tcPreference',1,65000),('drainTimeoutMs',1,120000)]:require(type(config[key]) is int and lower<=config[key]<=upper,'numeric_configuration')
 require(config['mark']!=config['bypassMark'],'mark_collision')
 require(config['entryPort']!=8092,'entry_port')
 require(isinstance(config['bypassProcesses'],list),'bypass_configuration')
 paths=[]
 for proc in config['bypassProcesses']:
  require(isinstance(proc,dict) and set(proc)=={'pid','startTicks','cgroup','executableSha256'},'bypass_configuration')
  require(type(proc['pid']) is int and proc['pid']>1 and re.fullmatch('[0-9]+',str(proc['startTicks'])) and re.fullmatch('[a-f0-9]{64}',proc['executableSha256']),'bypass_identity')
  validate_cgroup(proc['cgroup']);paths.append(proc['cgroup'])
 validate_cgroup(config['entryCgroup']);require(config['entryCgroup'] not in paths,'entry_bypass_forbidden')
 require(config['candidate'] is None or isinstance(config['candidate'],dict) and set(config['candidate'])=={'containerId','imageId','address','port'},'candidate_configuration')
 if config['candidate'] is not None:
  candidate=config['candidate'];require(re.fullmatch('[a-f0-9]{64}',candidate['containerId']) and re.fullmatch('sha256:[a-f0-9]{64}',candidate['imageId']) and candidate['containerId']!=source['containerId'],'candidate_identity')
  require(str(ipaddress.IPv4Address(candidate['address']))==candidate['address'] and type(candidate['port']) is int and 1<=candidate['port']<=65535,'candidate_endpoint')
 return config


def validate_cgroup(value):
 require(isinstance(value,str) and value.startswith('/') and value!='/' and str(Path(value))==value and all(re.fullmatch('[a-zA-Z0-9_.:@-]+',part) and part not in ['.','..'] for part in value.split('/')[1:]),'cgroup_path')


def process_ticks(pid):
 data=Path('/proc/'+str(pid)+'/stat').read_text();return data[data.rfind(')')+2:].split()[19]


def process_cgroup(pid):
 rows=Path('/proc/'+str(pid)+'/cgroup').read_text().splitlines();require(len(rows)==1 and rows[0].startswith('0::'),'cgroup_v2_required');return rows[0][3:]


def inspect_container(cid):
 rows=json.loads(execute(['/usr/bin/docker','container','inspect',cid]));require(len(rows)==1 and rows[0]['Id']==cid,'container_identity');return rows[0]


def local_container_inventory():
 # Reuse the reviewed positive inventory: stable root-owned Docker disk ID set
 # plus full direct daemon inspect for every ID. Failed inspect is never absence.
 path=Path(__file__).parent/'storage-recovery/rotate-tools.py'
 for code in [path,path.with_name('install-tools.py')]:
  trusted_parents(code);info=code.lstat();require(stat.S_ISREG(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022 and info.st_nlink==1,'inventory_code_untrusted')
 spec=importlib.util.spec_from_file_location('harbor_entry_inventory',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 return module.local_containers()


def verify_source(config,running=True):
 expected=config['source'];row=inspect_container(expected['containerId']);state=row['State']
 require(row['Image']==expected['imageId'] and state['StartedAt']==expected['startedAt'],'source_generation_changed')
 if running:
  require(state['Running'] is True and not state['Restarting'] and not state['Paused'] and state['Pid']==expected['pid'] and process_ticks(expected['pid'])==str(expected['startTicks']),'source_not_running')
  addresses={v.get('IPAddress') for v in row.get('NetworkSettings',{}).get('Networks',{}).values()};require(expected['address'] in addresses,'source_address_changed')
 else:require(not state['Running'] and state['Pid']==0 and state.get('ExitCode')==0 and not any(state.get(key) for key in ['Restarting','Paused','Dead','OOMKilled','Error']) and row['HostConfig']['RestartPolicy']['Name']=='no','source_not_fenced')
 return row


CANDIDATE_NETWORK_KEYS=('candidateNetworkIdentity','candidateNetworkContainerIds')
DYNAMIC_KEYS=('allowDynamicBridgePeers','localAssuranceReleaseId','localAssuranceProfileIdentityDigest','dynamicBridgeIdentity','dynamicPeerDeviceIndices','dynamicSourceNamespace')

def dynamic_bridge_policy(config,state=None):
 if state is None:
  if not STATE.exists():return None
  state=private_json(STATE)
 if state.get('allowDynamicBridgePeers') is not True:return None
 return state if local_assurance_module().allow_dynamic_bridge_peers(config,state) else None

def verify_source_bridge_owners(config,policy):
 ids=execute(['/usr/bin/docker','container','ls','--all','--no-trunc','--filter','label=com.aistrong.project=harbor-local-gateway','--format','{{.ID}}']).splitlines()
 for cid in ids:
  require(re.fullmatch('[a-f0-9]{64}',cid),'dynamic_owned_inventory')
  row=inspect_container(cid);require(row.get('Config',{}).get('Labels',{}).get('com.aistrong.project')=='harbor-local-gateway','dynamic_owned_inventory_changed')
  attached=any(v.get('NetworkID')==policy['dynamicBridgeIdentity']['networkId'] and v.get('IPAddress') for v in row.get('NetworkSettings',{}).get('Networks',{}).values())
  require(not attached or cid==config['source']['containerId'],'unexpected_harbor_source_bridge_peer')


def verify_bridge_peers(config,ports,removed_ports=(),additional_ports=(),state=None):
 policy=dynamic_bridge_policy(config,state)
 if policy is None:
  require({v['ifname'] for v in ports}==set(config['bridge']['ports'])-set(removed_ports)|set(additional_ports),'bridge_ports_changed');return
 verify_source_bridge_owners(config,policy)
 live={v['ifname']:v['ifindex'] for v in ports};critical={}
 source=policy.get('source',{})
 if config['source']['containerId']==source.get('containerId') and policy.get('sourceBridgePort') not in removed_ports:
  dev=policy.get('sourceBridgePort')
  if dev:critical[dev]=policy['dynamicPeerDeviceIndices'].get(dev)
 for dev,index in critical.items():require(index is not None and live.get(dev)==index,'critical_bridge_port_changed')


def verify_candidate_bridge(state):
 identity=state.get('candidateNetworkIdentity')
 if identity is None:return
 rows=json.loads(execute(['/usr/sbin/ip','-j','link','show','dev',identity['bridge']]))
 require(len(rows)==1 and rows[0]['ifindex']==identity['ifindex'] and rows[0]['address']==identity['mac'],'candidate_bridge_identity_changed')
 ports=json.loads(execute(['/usr/sbin/ip','-j','link','show','master',identity['bridge']]))
 require({v['ifname']:v['ifindex'] for v in ports}==identity['ports'],'candidate_bridge_ports_changed')
 network=json.loads(execute(['/usr/bin/docker','network','inspect',identity['id']]))
 require(len(network)==1 and network[0]['Id']==identity['id'] and network[0]['Name']==identity['name'] and set(network[0].get('Containers',{}))==set(state['candidateNetworkContainerIds']),'candidate_network_members_changed')


def verify_environment(config,removed_ports=(),additional_ports=(),bridge_index=None):
 require(config['bypassProcesses'],'bypass_configuration')
 bridge=config['bridge'];rows=json.loads(execute(['/usr/sbin/ip','-j','link','show','dev',bridge['name']]))
 require(len(rows)==1 and (rows[0]['ifindex']==bridge_index if bridge_index is not None else rows[0]['address']==bridge['mac']),'bridge_changed')
 ports=json.loads(execute(['/usr/sbin/ip','-j','link','show','master',bridge['name']]));verify_bridge_peers(config,ports,removed_ports,additional_ports)
 policy=dynamic_bridge_policy(config)
 if policy:
  require(rows[0]['ifindex']==policy['dynamicBridgeIdentity']['ifindex'] and rows[0]['address']==policy['dynamicBridgeIdentity']['mac'],'dynamic_bridge_identity_changed')
  if policy.get('sourceBridgePort') not in removed_ports:
   info=Path('/proc',str(config['source']['pid']),'ns/net').stat();require([info.st_dev,info.st_ino]==policy['dynamicSourceNamespace'],'dynamic_source_namespace_changed')
 for proc in config['bypassProcesses']:
  require(process_ticks(proc['pid'])==str(proc['startTicks']) and process_cgroup(proc['pid'])==proc['cgroup'],'bypass_process_changed')
  status=Path('/proc/'+str(proc['pid'])+'/status').read_text();require(next(line.split()[1] for line in status.splitlines() if line.startswith('Uid:'))=='0','bypass_uid')
  digest=hashlib.sha256()
  with open('/proc/'+str(proc['pid'])+'/exe','rb') as file:
   for block in iter(lambda:file.read(1048576),b''):digest.update(block)
  require(digest.hexdigest()==proc['executableSha256'],'bypass_executable_changed')
  info=(Path('/sys/fs/cgroup')/proc['cgroup'].lstrip('/')).stat();require(stat.S_ISDIR(info.st_mode),'bypass_cgroup_missing')
 require(Path('/proc/sys/net/ipv4/ip_forward').read_text().strip()=='1','forwarding_required')
 # Never enable global br_netfilter. Dedicated TC routes only Harbor packets.
 return True


def cgroup_match(path):return 'socket cgroupv2 level '+str(len(path.split('/'))-1)+' '+json.dumps(path.lstrip('/'))


def network_rules(config):
 source=config['source'];bridge=config['bridge'];port=config['entryPort'];old=source['address'];app=source['port'];mark=config['mark']
 targets=[(old,app)]
 if config['candidate'] is not None:targets.append((config['candidate']['address'],config['candidate']['port']))
 target_elements=', '.join(address+' . '+str(target_port) for address,target_port in targets)
 bypass='  meta skuid 0 meta mark '+str(config['bypassMark'])+' return'
 # Keep the DNAT expressions registered when withdrawing NEW traffic. Removing
 # their last instance can break established translations despite intact CT.
 return f'''table inet {TABLE} {{
 set enabled_ports {{ type inet_service; }}
 set targets {{ type ipv4_addr . inet_service; elements = {{ {target_elements} }}; }}
 chain private_input {{ type filter hook input priority -10; policy accept;
  tcp dport {port} ct status dnat ct original proto-dst {app} accept
  tcp dport {port} reject with tcp reset
 }}
 chain prerouting {{ type nat hook prerouting priority -101; policy accept;
  meta nfproto ipv4 tcp dport @enabled_ports tcp flags & (fin | syn | rst | ack) == syn fib daddr type local dnat ip to {bridge['address']}:{port}
  ip daddr . tcp dport @targets tcp dport @enabled_ports tcp flags & (fin | syn | rst | ack) == syn dnat ip to {bridge['address']}:{port}
  meta nfproto ipv6 tcp dport @enabled_ports tcp flags & (fin | syn | rst | ack) == syn fib daddr type local redirect to :{port}
 }}
 chain output {{ type nat hook output priority -101; policy accept;
{bypass}
  meta nfproto ipv4 tcp dport @enabled_ports tcp flags & (fin | syn | rst | ack) == syn fib daddr type local redirect to :{port}
  ip daddr . tcp dport @targets tcp dport @enabled_ports tcp flags & (fin | syn | rst | ack) == syn dnat ip to {bridge['address']}:{port}
  meta nfproto ipv6 tcp dport @enabled_ports tcp flags & (fin | syn | rst | ack) == syn fib daddr type local redirect to :{port}
 }}
 chain private_output {{ type filter hook output priority 0; policy accept;
  tcp dport {port} ct status dnat ct original proto-dst {app} accept
  tcp dport {port} reject with tcp reset
 }}
}}'''


def mangle_commands(config):
 commands=[['/usr/sbin/iptables','-w','5','-t','mangle','-N',REPLY_CHAIN],['/usr/sbin/iptables','-w','5','-t','mangle','-N',BYPASS_CHAIN]]
 commands.append(['/usr/sbin/iptables','-w','5','-t','mangle','-A',BYPASS_CHAIN,'-m','mark','--mark',str(config['bypassMark'])+'/0xffffffff','-j','MARK','--set-xmark','0/0xffffffff'])
 for proc in config['bypassProcesses']:
  commands.append(['/usr/sbin/iptables','-w','5','-t','mangle','-A',BYPASS_CHAIN,'-m','owner','--uid-owner','0','-m','cgroup','--path',proc['cgroup'].lstrip('/'),'-j','MARK','--set-xmark',str(config['bypassMark'])+'/0xffffffff'])
 source=config['source']
 commands.append(['/usr/sbin/iptables','-w','5','-t','mangle','-A',REPLY_CHAIN,'-i',config['bridge']['name'],'-s',source['address'],'-p','tcp','--sport',str(source['port']),'-m','socket','--transparent','-j','MARK','--set-xmark',str(config['mark'])+'/0xffffffff'])
 commands.extend([['/usr/sbin/iptables','-w','5','-t','mangle','-I','PREROUTING','1','-j',REPLY_CHAIN],['/usr/sbin/iptables','-w','5','-t','mangle','-I','OUTPUT','1','-j',BYPASS_CHAIN]])
 # IPv6 local requests need the same exact kernel cgroup evidence before NAT.
 commands.extend([['/usr/sbin/ip6tables','-w','5','-t','mangle','-N',BYPASS_CHAIN]])
 commands.append(['/usr/sbin/ip6tables','-w','5','-t','mangle','-A',BYPASS_CHAIN,'-m','mark','--mark',str(config['bypassMark'])+'/0xffffffff','-j','MARK','--set-xmark','0/0xffffffff'])
 for proc in config['bypassProcesses']:
  commands.append(['/usr/sbin/ip6tables','-w','5','-t','mangle','-A',BYPASS_CHAIN,'-m','owner','--uid-owner','0','-m','cgroup','--path',proc['cgroup'].lstrip('/'),'-j','MARK','--set-xmark',str(config['bypassMark'])+'/0xffffffff'])
 commands.append(['/usr/sbin/ip6tables','-w','5','-t','mangle','-I','OUTPUT','1','-j',BYPASS_CHAIN])
 return commands


def resource_digest(value):
 if isinstance(value,dict):return {k:resource_digest(v) for k,v in value.items() if k not in ['handle','packets','bytes','used','lastuse','firstuse','installed','ref','bind','index','stats','stats2']}
 if isinstance(value,list):return [resource_digest(v) for v in value]
 return value


def tc_digest(dev):return fingerprint(resource_digest(json.loads(execute(['/usr/sbin/tc','-j','filter','show','dev',dev,'ingress']))))


def mangle_digest():
 rows={}
 for binary,chain,parent in [('/usr/sbin/iptables',REPLY_CHAIN,'PREROUTING'),('/usr/sbin/iptables',BYPASS_CHAIN,'OUTPUT'),('/usr/sbin/ip6tables',BYPASS_CHAIN,'OUTPUT')]:
  rules=execute([binary,'-t','mangle','-S',chain]).splitlines();hooks=execute([binary,'-t','mangle','-S',parent]).splitlines()
  require(hooks.count('-A '+parent+' -j '+chain)==1,'mangle_hook_changed')
  rows[binary+chain]=rules
 return fingerprint(rows)


def verify_resources(state,config,fenced=False):
 require(state.get('rulesDigest')==state_rules_digest() and state.get('mangleDigest')==mangle_digest(),'rules_changed')
 policy=dynamic_bridge_policy(config,state)
 live={v['ifname']:v['ifindex'] for v in json.loads(execute(['/usr/sbin/ip','-j','link','show']))} if policy else {}
 for dev,digest in state.get('tcDigests',{}).items():
  if fenced and dev==state.get('sourceBridgePort'):continue
  if policy:
   owned=state.get('candidateDeviceIndices',{});critical=dev in owned or dev not in state['dynamicPeerDeviceIndices'] or dev==state.get('sourceBridgePort')
   expected=owned.get(dev,state['dynamicPeerDeviceIndices'].get(dev))
   if not critical and live.get(dev)!=expected:continue
   require(expected is not None and live.get(dev)==expected,'tc_device_identity_changed')
  require(tc_digest(dev)==digest,'tc_rules_changed')
 sets=json.loads(execute(['/usr/sbin/nft','--json','list','set','inet',TABLE,'enabled_ports']))
 actual=next(item['set'].get('elem',[]) for item in sets['nftables'] if 'set' in item)
 expected=[config['source']['port']] if state['phase'] in ['routed','holding','candidate-binding','candidate-bound','activating','activated'] else []
 require(actual==expected or state['phase']=='withdrawing' and actual in [[],[config['source']['port']]],'enabled_ports_changed')


def guard_rules(config):
 port=config['entryPort']
 return f'''table inet {TABLE} {{
 chain private_input {{ type filter hook input priority -10; policy accept; tcp dport {port} reject with tcp reset; }}
 chain private_output {{ type filter hook output priority 0; policy accept; tcp dport {port} reject with tcp reset; }}
}}'''


def guard_fingerprint(config):return fingerprint({k:v for k,v in config.items() if k!='bypassProcesses'})


def tc_commands(config,remove=False):
 source=config['source'];bridge=config['bridge'];base=config['tcPreference'];commands=[]
 for dev in bridge['ports']:
  if remove:
   for priority in [base,base+1]:commands.append(['/usr/sbin/tc','filter','del','dev',dev,'ingress','protocol','ip','pref',str(priority)])
   commands.append(['/usr/sbin/tc','qdisc','del','dev',dev,'clsact']);continue
  commands.append(['/usr/sbin/tc','qdisc','add','dev',dev,'clsact'])
  for delta,fields in [(0,['dst_ip',source['address'],'dst_port',str(source['port'])]),(1,['src_ip',source['address'],'src_port',str(source['port'])])]:
   commands.append(['/usr/sbin/tc','filter','add','dev',dev,'ingress','protocol','ip','pref',str(base+delta),'flower','ip_proto','tcp',*fields,'action','pedit','ex','munge','eth','dst','set',bridge['mac'],'action','skbedit','ptype','host','action','mirred','ingress','redirect','dev',bridge['name']])
 return commands


class UnixHttp(http.client.HTTPConnection):
 def connect(self):
  self.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);self.sock.settimeout(self.timeout);self.sock.connect(CONTROL)


def entry_state(command=None,epoch=None,slot=None):
 info=Path(CONTROL).lstat();trusted_parents(Path(CONTROL));require(stat.S_ISSOCK(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600,'control_untrusted')
 client=UnixHttp('localhost',timeout=125)
 try:
  body=None if command is None else json.dumps({'command':command,'expectedEpoch':epoch,**({'slot':slot} if slot is not None else {})})
  client.request('GET' if command is None else 'POST','/state' if command is None else '/control',body=body,headers={} if body is None else {'Content-Type':'application/json'})
  response=client.getresponse();data=response.read(65537);require(response.status==200 and len(data)<=65536,'entry_control_failed');return json.loads(data)
 finally:client.close()


def old_socket_counts(pid,port):
 counts={};active=0
 for family in ['tcp','tcp6']:
  path=Path('/proc/'+str(pid)+'/net/'+family)
  if not path.exists():continue
  for row in path.read_text().splitlines()[1:]:
   fields=row.split()
   if len(fields)<4 or int(fields[1].split(':')[1],16)!=port:continue
   state=fields[3];counts[family+':'+state]=counts.get(family+':'+state,0)+1
   if state not in ['0A','06','07']:active+=1
 return active,counts


def save_state(state):
 save_json(STATE,state)


def save_json(path,value):
 trusted_parents(path)
 if path.exists():private_json(path)
 temporary=path.with_suffix('.pending');fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
 try:
  with os.fdopen(fd,'w') as file:json.dump(value,file);file.flush();os.fsync(file.fileno())
  os.replace(temporary,path)
  directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
  try:os.fsync(directory)
  finally:os.close(directory)
 finally:
  if temporary.exists():temporary.unlink()


def nft(command):return execute(['/usr/sbin/nft','-f','-'],command)


def state_rules_digest():
 value=json.loads(execute(['/usr/sbin/nft','--json','list','table','inet',TABLE]))
 # Runtime counters/handles are not authorization. The declarative rule shape is.
 def clean(value):
  if isinstance(value,dict):return {k:clean(v) for k,v in value.items() if k not in ['handle','packets','bytes','elem','elements']}
  if isinstance(value,list):return [clean(v) for v in value]
  return value
 return fingerprint(clean(value))


def run_action(config,action):
 if action=='guard':
  verify_source(config);require(not STATE.exists(),'state_collision')
  tables=json.loads(execute(['/usr/sbin/nft','--json','list','tables']));require(not any(v.get('table',{}).get('name')==TABLE for v in tables.get('nftables',[])),'table_collision')
  state={'version':1,'phase':'guard-starting','guardFingerprint':guard_fingerprint(config)};save_state(state);nft(guard_rules(config));state['phase']='guarded';state['rulesDigest']=state_rules_digest();save_state(state)
  return {'phase':'guarded','privateEntryPortDenied':True,'sourceStopped':False}
 if action=='prepare':
  source_row=verify_source(config);verify_environment(config)
  guarded=private_json(STATE);require(guarded.get('phase')=='guarded' and guarded.get('guardFingerprint')==guard_fingerprint(config) and guarded.get('rulesDigest')==state_rules_digest(),'guard_not_ready')
  rules=json.loads(execute(['/usr/sbin/ip','-j','rule','show']));require(not any(v.get('priority')==config['rulePriority'] or v.get('table')==config['routeTable'] for v in rules),'route_rule_collision')
  routes=json.loads(execute(['/usr/sbin/ip','-j','route','show','table','all']));require(not any(str(v.get('table'))==str(config['routeTable']) for v in routes),'route_table_collision')
  for dev in config['bridge']['ports']:
   qdiscs=json.loads(execute(['/usr/sbin/tc','-j','qdisc','show','dev',dev]));require(all(v.get('kind') not in ['clsact','ingress'] for v in qdiscs),'qdisc_collision')
  for binary in ['/usr/sbin/iptables','/usr/sbin/ip6tables']:
   existing=execute([binary,'-t','mangle','-S']);require(REPLY_CHAIN not in existing and BYPASS_CHAIN not in existing,'mangle_chain_collision')
  # Preserve a root journal before the first mutation. A partial failure is
  # retained for explicit inspection; never delete unknown resources to retry.
  bridge_index=json.loads(execute(['/usr/sbin/ip','-j','link','show','dev',config['bridge']['name']]))[0]['ifindex']
  state={'version':1,'configDigest':fingerprint(config),'phase':'preparing','source':config['source'],'sourceRestartPolicy':source_row['HostConfig']['RestartPolicy'],'sourceBridgePort':source_bridge_port(config),'bridgeIfindex':bridge_index,'completedTcCommands':[]};save_state(state)
  nft(f'delete table inet {TABLE}\n'+network_rules(config))
  for args in mangle_commands(config):execute(args)
  for args in tc_commands(config):execute(args);state['completedTcCommands'].append(args);save_state(state)
  execute(['/usr/sbin/ip','route','add','local','0.0.0.0/0','dev','lo','table',str(config['routeTable'])])
  execute(['/usr/sbin/ip','rule','add','fwmark',str(config['mark']),'lookup',str(config['routeTable']),'priority',str(config['rulePriority'])])
  state['phase']='prepared';state['rulesDigest']=state_rules_digest();state['mangleDigest']=mangle_digest();state['tcDigests']={dev:tc_digest(dev) for dev in config['bridge']['ports']};save_state(state);return {'phase':'prepared','newTrafficRouted':False}
 state=private_json(STATE);require(state.get('configDigest')==fingerprint(config),'configuration_changed')
 if action=='status':
  if 'activeRuntimeConfig' in state:
   rows={row['Id']:row for row in local_container_inventory()};running=rows.get(config['source']['containerId'],{}).get('State',{}).get('Running',False)
  else:running=inspect_container(config['source']['containerId'])['State']['Running']
  verify_resources(state,state.get('activeRuntimeConfig',config),fenced=not running and 'activeRuntimeConfig' not in state)
  if running:verify_source(config);active,counts=old_socket_counts(config['source']['pid'],config['source']['port'])
  else:active,counts=0,{}
  return {'phase':state['phase'],'entry':entry_state(),'oldSocketCounts':counts,'oldActiveConnections':active,'sourceStopped':not running,'candidate':state.get('candidate')}
 verify_resources(state,config)
 verify_environment(config)
 if action=='arm':
  require(state['phase'] in ['prepared','withdrawn'],'phase');verify_source(config);entry=entry_state();require(entry['slot']=='legacy' and entry['phase']=='active','entry_not_legacy_active')
  nft(f'add element inet {TABLE} enabled_ports {{ {config["source"]["port"]} }}');state['phase']='routed';save_state(state);return {'phase':'routed','entryEpoch':entry['epoch']}
 if action=='withdraw':
  require(state['phase'] in ['routed','holding','withdrawing','withdrawn'],'phase');verify_source(config)
  state['phase']='withdrawing';save_state(state)
  nft(f'flush set inet {TABLE} enabled_ports');entry=entry_state()
  if entry['phase'] in ['holding','drained']:entry=entry_state('resume',entry['epoch'])
  require(entry['slot']=='legacy' and entry['phase']=='active','legacy_resume_failed');state['phase']='withdrawn';save_state(state)
  return {'phase':'withdrawn','existingNatAndTcRetained':True,'entryEpoch':entry['epoch']}
 if action=='hold-drain':
  require(state['phase'] in ['routed','holding'],'phase');verify_source(config);entry=entry_state()
  if entry['phase']=='active':entry=entry_state('hold',entry['epoch'])
  require(entry['slot']=='legacy' and entry['phase'] in ['holding','drained'],'entry_phase');state['phase']='holding';save_state(state)
  if entry['phase']=='holding':entry=entry_state('drain',entry['epoch'])
  deadline=time.monotonic()+config['drainTimeoutMs']/1000
  while True:
   verify_source(config);verify_environment(config);verify_resources(state,config)
   active,counts=old_socket_counts(config['source']['pid'],config['source']['port'])
   if not active:break
   require(time.monotonic()<deadline,'old_connections_not_drained');time.sleep(0.05)
  current=entry_state();require(current['epoch']==entry['epoch'] and current['phase']=='drained' and current['inflight']==0,'entry_drain_changed')
  receipt={'phase':'holding','entryEpoch':current['epoch'],'sourceContainerId':config['source']['containerId'],'sourcePid':config['source']['pid'],'sourceStartTicks':config['source']['startTicks'],'oldSocketCounts':counts,'sourceStopped':False,'drained':True}
  state['drainReceipt']=receipt;save_state(state);return receipt
 raise RuntimeError('entry_controller_action')


class EntryDeployment:
 """Internal capability returned only after independently checking root facts."""
 def __init__(self,image,approval,config,state):
  self.image=image;self.approval=approval;self.config=config;self.state=state;self.activated=state.get('phase')=='activated';self.candidate=state.get('candidate');self.check(approval)

 def check(self,approval=None):
  require(os.geteuid()==0,'root_required');require(fingerprint(private_json(PROFILE))==self.state['configDigest'],'configuration_changed')
  current=private_json(STATE);require(current.get('configDigest')==self.state['configDigest'] and current.get('phase') in ['holding','candidate-binding','candidate-bound','activating','activated'],'deployment_phase')
  require(current.get('drainReceipt')==self.state.get('drainReceipt'),'deployment_guard_changed');verify_resources(current,current.get('activeRuntimeConfig',self.config),fenced='activeRuntimeConfig' not in current);verify_candidate_bridge(current)
  if approval is not None:
   for key in ['contract','deploymentId','receiptDigest','configDigest','manifestDigest','revision','sourceFence']:require(approval.get(key)==self.approval.get(key),'activation_changed')
   require(approval.get('state') in ['pending','active'] or approval.get('state')=='failed' and current.get('candidateCleanupVerified') is True and not current.get('candidate'),'activation_state')
  fence=self.approval.get('sourceFence') or {};expected=self.config['source']
  if current.get('activeRuntimeConfig'):
   inventory={row['Id'] for row in local_container_inventory()}
   if expected['containerId'] in inventory:verify_source(self.config,running=False)
   require(approval_binding(self.approval)==private_json(ACTIVE).get('approval'),'active_approval_changed')
   runtime_view=copy.deepcopy(current['activeRuntimeConfig']);runtime_view['bypassProcesses']=refreshed_bypass(self.config);verify_environment(runtime_view)
  else:
   source=verify_source(self.config,running=False);fence=self.approval.get('sourceFence') or {}
   expected=self.config['source']
   require(fence.get('scope')=='docker-systemd' and fence.get('sourceContainerId')==expected['containerId'] and fence.get('sourceImageId')==expected['imageId'] and fence.get('sourceStartedAt')==expected['startedAt'] and fence.get('sourceFinishedAt')==source['State']['FinishedAt'] and fence.get('sourceStopped') is True and fence.get('supervisorFenced') is True and fence.get('productionVerified') is True,'source_fence_changed')
   old_port=self.state.get('sourceBridgePort');ports=json.loads(execute(['/usr/sbin/ip','-j','link','show','master',self.config['bridge']['name']]))
   present={row['ifname'] for row in ports};base=set(self.config['bridge']['ports'])-{old_port};extras=present-base
   # Every extra interface must belong to the signed new gateway/worker, never
   # an unrelated new container on the shared Docker bridge.
   if extras and dynamic_bridge_policy(self.config,current) is None:
    expected_indices=set()
    for role in ['gateway','worker']:
     name='harbor-local-gateway'+('' if role=='gateway' else '-worker')
     rows=json.loads(execute(['/usr/bin/docker','container','inspect',name]));require(len(rows)==1,'candidate_composition')
     row=rows[0];self.verify_candidate_row(row)
     links=json.loads(execute(['/usr/bin/nsenter','--net=/proc/'+str(row['State']['Pid'])+'/ns/net','/usr/sbin/ip','-j','link','show']))
     expected_indices.update(v.get('link_index') for v in links)
    require(all(row['ifindex'] in expected_indices for row in ports if row['ifname'] in extras),'unowned_bridge_port')
   verify_environment(self.config,removed_ports=[old_port],additional_ports=extras,bridge_index=current.get('bridgeIfindex'))
  entry=entry_state();receipt=self.state.get('drainReceipt') or {}
  committed=current.get('phase') in ['activating','activated']
  if committed:
   active=private_json(ACTIVE);require(active.get('image')==self.image and active.get('candidate')==self.candidate and active.get('approval')==approval_binding(self.approval),'committed_binding_changed')
  if committed and entry.get('slot')=='candidate' and entry.get('phase')=='active' and entry.get('epoch')==receipt.get('entryEpoch')+1:
   self.activated=True
   if current['phase']!='activated':self.state['phase']='activated';self.state['activatedEpoch']=entry['epoch'];save_state(self.state)
  if not self.activated:require(entry.get('slot')=='legacy' and entry.get('phase')=='drained' and entry.get('inflight')==0 and entry.get('epoch')==receipt.get('entryEpoch'),'entry_drain_changed')
  else:require(entry.get('slot')=='candidate' and entry.get('phase')=='active' and entry.get('epoch')==self.state.get('activatedEpoch'),'entry_activation_changed')
  if self.candidate is not None:
   row=inspect_container(self.candidate['containerId']);self.verify_candidate_row(row);require(self.endpoint(row)==self.candidate['address'],'candidate_address_changed')
  if approval is not None and approval.get('state')=='active' and self.approval.get('contract')!='harbor-local-assurance-deploy-v1':
   authorized={'version':1,'image':self.image,'approval':approval_binding(self.approval),'configDigest':self.state['configDigest']}
   if WRITER_AUTHORIZED.exists():require(private_json(WRITER_AUTHORIZED)==authorized,'writer_authorization_changed')
   else:save_json(WRITER_AUTHORIZED,authorized)
   if not self.state.get('writerAuthorized'):self.state['writerAuthorized']=True;save_state(self.state)

 def verify_candidate_row(self,row):
  labels=row.get('Config',{}).get('Labels',{})
  require(row.get('Image') in [self.approval.get('configDigest'),self.approval.get('manifestDigest')] and row.get('Config',{}).get('Image')==self.image,'candidate_image_changed')
  require(labels.get('com.aistrong.project')=='harbor-local-gateway' and labels.get('com.harbor.activation-deployment-id')==self.approval.get('deploymentId') and labels.get('com.harbor.activation-receipt')==self.approval.get('receiptDigest'),'candidate_activation_changed')
  if self.approval.get('contract')=='harbor-local-assurance-deploy-v1':local_assurance_module().verify_candidate_network(row)
  state=row.get('State',{});require(state.get('Running') and not state.get('Restarting') and not state.get('Paused'),'candidate_not_running')

 def endpoint(self,row):
  addresses=[str(ipaddress.IPv4Address(v['IPAddress'])) for v in row.get('NetworkSettings',{}).get('Networks',{}).values() if v.get('IPAddress')]
  require(len(addresses)==1,'candidate_address_ambiguous');return addresses[0]

 def bind_candidate(self,candidate):
  self.check();row=inspect_container(candidate['Id']);require(row['Id']==candidate['Id'] and row['Image']==candidate.get('Image'),'candidate_changed');self.verify_candidate_row(row)
  address=self.endpoint(row);port=self.config['source']['port'];target={'containerId':row['Id'],'imageId':row['Image'],'address':address,'port':port}
  if self.candidate is not None:
   require(target==self.candidate and fingerprint(private_json(CANDIDATE_PROFILE))==self.state.get('candidateBridgeProfileDigest'),'candidate_rebind_changed')
   return {**target,'slotSocket':'/run/harbor-entry/candidate.sock'}
  require(self.state.get('phase')=='holding','candidate_binding_incomplete')
  profile=private_json(CANDIDATE_PROFILE);require(profile.get('socketPath')=='/run/harbor-entry/candidate.sock','candidate_slot_profile')
  self.state['candidateIntent']=target
  self.state['candidateBaseline']={'tcDigests':copy.deepcopy(self.state['tcDigests']),'rulesDigest':self.state['rulesDigest'],'mangleDigest':self.state['mangleDigest'],'profile':profile}
  self.state['candidateOperations']=[];self.state['candidateDeviceIndices']={};self.state['phase']='candidate-binding';save_state(self.state)
  def operation(args,kind,previous=None):
   step={'args':args,'kind':kind,'applied':False}
   if previous is not None:step['previousArgs']=previous
   self.state['candidateOperations'].append(step);save_state(self.state)
   execute(args);step['applied']=True
   if kind in ['tc','tc-replace']:self.state['tcDigests'][args[args.index('dev')+1]]=tc_digest(args[args.index('dev')+1])
   self.state['rulesDigest']=state_rules_digest();self.state['mangleDigest']=mangle_digest();save_state(self.state)
  # The restricted bridge reopens its root profile per accepted UDS request.
  # Its service mounts this DIRECTORY read-only, never a stale file inode.
  profile={**profile,'sourceAddress':address,'sourcePort':port};save_json(CANDIDATE_PROFILE,profile)
  networks=row['NetworkSettings']['Networks'];attachment=next(iter(networks.values()));network=json.loads(execute(['/usr/bin/docker','network','inspect',attachment['NetworkID']]))[0]
  require(network.get('Driver')=='bridge','candidate_network')
  bridge_name=(network.get('Options') or {}).get('com.docker.network.bridge.name') or ('docker0' if network['Name']=='bridge' else 'br-'+network['Id'][:12])
  bridge_rows=json.loads(execute(['/usr/sbin/ip','-j','link','show','dev',bridge_name]));require(len(bridge_rows)==1,'candidate_bridge')
  peer_mac=attachment.get('MacAddress','');require(re.fullmatch('(?:[a-f0-9]{2}:){5}[a-f0-9]{2}',peer_mac),'candidate_mac')
  # A new CID may reuse the stopped source IP while the host still caches its
  # old MAC. Replace only this verified candidate endpoint's neighbor entry.
  execute(['/usr/sbin/ip','neigh','replace',address,'lladdr',peer_mac,'nud','reachable','dev',bridge_name])
  ports=json.loads(execute(['/usr/sbin/ip','-j','link','show','master',bridge_name]));added=[]
  if self.approval.get('contract')=='harbor-local-assurance-deploy-v1':
   expected=local_assurance_module().candidate_network();worker=json.loads(execute(['/usr/bin/docker','container','inspect','harbor-local-gateway-worker']))[0];self.verify_candidate_row(worker)
   members={*expected['infrastructureContainerIds'],row['Id'],worker['Id']}
   require(network['Id']==expected['id'] and network['Name']==expected['name'] and bridge_name==expected['bridge'] and bridge_rows[0]['ifindex']==expected['bridgeIfindex'] and bridge_rows[0]['address']==expected['bridgeMac'] and set(network.get('Containers',{}))==members,'candidate_isolated_network_changed')
   self.state['candidateNetworkIdentity']={'id':network['Id'],'name':network['Name'],'bridge':bridge_name,'ifindex':bridge_rows[0]['ifindex'],'mac':bridge_rows[0]['address'],'ports':{v['ifname']:v['ifindex'] for v in ports}};self.state['candidateNetworkContainerIds']=sorted(members);save_state(self.state)
  # The old profile's bridge ports remain guarded. New candidate/worker ports
  # receive only the same two target-specific routing filters.
  for item in ports:
   dev=item['ifname'];existing=dev in self.config['bridge']['ports'] and dev!=self.state.get('sourceBridgePort') and address==self.config['source']['address']
   if existing:
    if bridge_rows[0]['address']!=self.config['bridge']['mac']:
     # Linux bridges may select a new MAC after the old source port vanishes.
     # Retained same-IP filters run before candidate additions and must target
     # the current L3 MAC. Journal exact prior owned expressions for reclaim.
     self.state['candidateDeviceIndices'][dev]=item['ifindex'];save_state(self.state)
     previous={**self.config,'bridge':{**self.config['bridge'],'ports':[dev]}}
     current={**previous,'bridge':{**previous['bridge'],'mac':bridge_rows[0]['address']}}
     for before,after in zip(tc_commands(previous)[1:],tc_commands(current)[1:]):
      filters=json.loads(execute(['/usr/sbin/tc','-j','filter','show','dev',dev,'ingress']))
      matches=[item for item in filters if item.get('pref')==int(before[before.index('pref')+1]) and item.get('options',{}).get('handle')]
      require(len(matches)==1,'candidate_retained_filter_identity');handle=matches[0]['options']['handle']
      for command in [before,after]:command[2]='replace';command[command.index('flower'):command.index('flower')]=['handle',str(handle)]
      operation(after,'tc-replace',before)
    continue
   self.state['candidateDeviceIndices'][dev]=item['ifindex'];save_state(self.state)
   qdiscs=json.loads(execute(['/usr/sbin/tc','-j','qdisc','show','dev',dev]));owns_clsact=dev in self.config['bridge']['ports'] and dev!=self.state.get('sourceBridgePort')
   require(owns_clsact or all(v.get('kind') not in ['clsact','ingress'] for v in qdiscs),'candidate_qdisc_collision')
   changed={**self.config,'source':{**self.config['source'],'address':address},'bridge':{'name':bridge_name,'address':next(v['Gateway'] for v in networks.values() if v.get('Gateway')),'mac':bridge_rows[0]['address'],'ports':[dev]},'tcPreference':self.config['tcPreference']+2}
   commands=tc_commands(changed)
   for args in (commands[1:] if owns_clsact else commands):operation(args,'tc')
   added.append(dev)
  if address!=self.config['source']['address']:
   self.state['candidateTargetAdded']=True;save_state(self.state);nft(f'add element inet {TABLE} targets {{ {address} . {port} }}');self.state['rulesDigest']=state_rules_digest();save_state(self.state)
  if address!=self.config['source']['address'] or bridge_name!=self.config['bridge']['name']:operation(['/usr/sbin/iptables','-w','5','-t','mangle','-A',REPLY_CHAIN,'-i',bridge_name,'-s',address,'-p','tcp','--sport',str(port),'-m','socket','--transparent','-j','MARK','--set-xmark',str(self.config['mark'])+'/0xffffffff'],'mangle')
  self.candidate=target;self.state['candidate']=target;self.state['candidateBridgeProfileDigest']=fingerprint(profile);self.state['candidateAddedPorts']=added;self.state['phase']='candidate-bound';self.state['rulesDigest']=state_rules_digest();self.state['mangleDigest']=mangle_digest();self.state['tcDigests'].update({dev:tc_digest(dev) for dev in added});save_state(self.state);self.check()
  return {**target,'slotSocket':'/run/harbor-entry/candidate.sock'}

 def unbind_candidate(self,*,activation,removed_container_ids):
  require(not self.activated and not ACTIVE.exists(),'candidate_cleanup_after_commit')
  require(isinstance(activation,dict) and activation.get('state')=='failed' and approval_binding(activation)==approval_binding(self.approval),'candidate_cleanup_activation')
  require(isinstance(removed_container_ids,list) and removed_container_ids and all(isinstance(cid,str) and re.fullmatch('[a-f0-9]{64}',cid) for cid in removed_container_ids),'candidate_cleanup_ids')
  state=private_json(STATE);target=state.get('candidate') or state.get('candidateIntent');require(state.get('configDigest')==self.state['configDigest'] and self.config['source']['containerId'] not in removed_container_ids,'candidate_cleanup_binding')
  inventory={row['Id'] for row in local_container_inventory()}
  require(not inventory.intersection(removed_container_ids),'candidate_cleanup_not_absent')
  verify_source(self.config,running=False);entry=entry_state();require(entry.get('slot')=='legacy' and entry.get('phase')=='drained' and entry.get('inflight')==0 and entry.get('epoch')==state['drainReceipt']['entryEpoch'],'candidate_cleanup_entry_changed')
  if target is None:
   require(state.get('phase')=='holding' and state.get('writerAuthorized') is True and not deployment_inventory(self.approval),'candidate_cleanup_unbound_generation')
   state['candidateCleanupVerified']=True;save_state(state);self.state=state;self.check(activation)
   return {'phase':'holding','candidateUnbound':True,'sourceStopped':True,'entryEpoch':entry['epoch']}
  require(target['containerId'] in removed_container_ids and not deployment_inventory(self.approval),'candidate_cleanup_binding')
  require(state.get('rulesDigest')==state_rules_digest() and state.get('mangleDigest')==mangle_digest(),'candidate_cleanup_rules_changed')
  live={v['ifname']:v['ifindex'] for v in json.loads(execute(['/usr/sbin/ip','-j','link','show']))}
  require(all(step.get('applied') is True for step in state.get('candidateOperations',[])),'candidate_partial_operation_unconfirmed')
  checked=set()
  for step in reversed(state.get('candidateOperations',[])):
   args=step['args']
   if step['kind']=='mangle':
    delete=list(args);delete[delete.index('-A')]='-D';execute(delete)
   elif step['kind'] in ['tc','tc-replace']:
    dev=args[args.index('dev')+1]
    if dev not in live:continue
    require(live[dev]==state['candidateDeviceIndices'][dev],'candidate_cleanup_device_reused')
    if dev not in checked:require(tc_digest(dev)==state['tcDigests'][dev],'candidate_cleanup_tc_changed');checked.add(dev)
    if step['kind']=='tc-replace':execute(step['previousArgs'])
    elif 'filter' in args:
     pref=args[args.index('pref')+1];execute(['/usr/sbin/tc','filter','del','dev',dev,'ingress','protocol','ip','pref',pref])
    else:
     require(not json.loads(execute(['/usr/sbin/tc','-j','filter','show','dev',dev,'ingress'])) and not json.loads(execute(['/usr/sbin/tc','-j','filter','show','dev',dev,'egress'])),'candidate_cleanup_foreign_filter')
     execute(['/usr/sbin/tc','qdisc','del','dev',dev,'clsact'])
  if state.get('candidateTargetAdded'):nft(f'delete element inet {TABLE} targets {{ {target["address"]} . {target["port"]} }}')
  baseline=state['candidateBaseline'];save_json(CANDIDATE_PROFILE,baseline['profile'])
  for key in ['candidate','candidateIntent','candidateOperations','candidateDeviceIndices','candidateTargetAdded','candidateBaseline','candidateBridgeProfileDigest','candidateAddedPorts',*CANDIDATE_NETWORK_KEYS]:state.pop(key,None)
  state['phase']='holding';state['candidateCleanupVerified']=True;state['tcDigests']=baseline['tcDigests'];state['rulesDigest']=state_rules_digest();state['mangleDigest']=mangle_digest();save_state(state)
  self.state=state;self.candidate=None;self.check(activation);return {'phase':'holding','candidateUnbound':True,'sourceStopped':True,'entryEpoch':entry['epoch']}

 def allow_probe_worker(self):
  runtime=self.state.get('activeRuntimeConfig')
  if runtime is None:return
  pid=os.getpid();group=process_cgroup(pid);digest=hashlib.sha256(Path('/proc/self/exe').read_bytes()).hexdigest()
  require(any(v['cgroup']==group and v['executableSha256']==digest for v in self.config['bypassProcesses']),'probe_process_not_bound')
  fresh=refreshed_bypass(self.config)
  if all(any(old['cgroup']==v['cgroup'] and old.get('cgroupInode')==v.get('cgroupInode') for old in runtime['bypassProcesses']) for v in fresh):
   runtime['bypassProcesses']=fresh;save_state(self.state);return
  # Only this root-profile-approved cgroup is refreshed. Other root groups
  # remain subject to normal ingress; the preceding mark scrub still applies.
  for bound in fresh:
   if any(old['cgroup']==bound['cgroup'] and old.get('cgroupInode')==bound.get('cgroupInode') for old in runtime['bypassProcesses']):continue
   for binary in ['/usr/sbin/iptables','/usr/sbin/ip6tables']:
    execute([binary,'-w','5','-t','mangle','-A',BYPASS_CHAIN,'-m','owner','--uid-owner','0','-m','cgroup','--path',bound['cgroup'].lstrip('/'),'-j','MARK','--set-xmark',str(self.config['bypassMark'])+'/0xffffffff'])
  runtime['bypassProcesses']=fresh;self.state['mangleDigest']=mangle_digest();save_state(self.state)
  active=private_json(ACTIVE);active['networkSnapshot']=network_snapshot(self.state);save_json(ACTIVE,active)

 def enter_probe_worker(self):
  current=process_cgroup(os.getpid());digest=hashlib.sha256(Path('/proc/self/exe').read_bytes()).hexdigest()
  if any(v['cgroup']==current and v['executableSha256']==digest for v in self.config['bypassProcesses']):return
  declared=[v for v in self.config['bypassProcesses'] if v['cgroup']==DEPLOY_CGROUP and v['executableSha256']==digest]
  require(declared,'dedicated_probe_cgroup_not_bound')
  # The root-owned idle service keeps this one cgroup alive. Only after all
  # release/fence/network checks pass do we move this root worker itself, never
  # its SSH parent or another process, into the already approved kernel scope.
  anchor=declared[0];require(process_ticks(anchor['pid'])==str(anchor['startTicks']) and process_cgroup(anchor['pid'])==DEPLOY_CGROUP,'probe_anchor_changed')
  target=Path('/sys/fs/cgroup')/DEPLOY_CGROUP.lstrip('/')/'cgroup.procs';trusted_parents(target);info=target.lstat();require(stat.S_ISREG(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022,'probe_scope_untrusted')
  target.write_text(str(os.getpid()));require(process_cgroup(os.getpid())==DEPLOY_CGROUP,'probe_scope_not_entered')

 def probe(self,port,path,headers=None):
  self.check();require(self.candidate is not None and port==self.config['source']['port'] and path in ['/health','/ready','/api/admin/state'],'probe_target')
  current=os.getpid();matches=[v for v in self.config['bypassProcesses'] if process_cgroup(current)==v['cgroup']]
  # The profile pins a live executable/generation anchoring the dedicated
  # deployment cgroup; its root worker children may issue these bounded probes.
  require(matches,'probe_process_not_bound')
  require(headers is None or isinstance(headers,dict) and set(headers)<= {'Host'},'probe_headers')
  client=http.client.HTTPConnection(self.candidate['address'],port,timeout=5)
  try:
   client.request('GET',path,headers={'Host':'127.0.0.1:'+str(port),**(headers or {})});response=client.getresponse();status=response.status;response.read(65536);return status
  except (OSError,http.client.HTTPException):return 0
  finally:client.close()

 def activate(self,activation):
  self.check(activation);require(activation.get('state')=='active' and self.candidate is not None,'activation_not_ready')
  if self.activated:return entry_state()
  require(self.probe(self.candidate['port'],'/ready')==200,'candidate_not_ready')
  require(fingerprint(private_json(CANDIDATE_PROFILE))==self.state['candidateBridgeProfileDigest'],'candidate_bridge_profile_changed')
  slot=private_json(CANDIDATE_PROFILE)['socketPath'];info=Path(slot).lstat();trusted_parents(Path(slot));require(stat.S_ISSOCK(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600,'candidate_bridge_socket_untrusted')
  client=UnixHttp('localhost',timeout=5)
  def connect_slot():
   client.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);client.sock.settimeout(5);client.sock.connect(slot)
  client.connect=connect_slot
  try:
   client.request('GET','/ready',headers={'Host':'127.0.0.1:'+str(self.candidate['port']),'x-harbor-entry-client-address':self.config['bridge']['address']});response=client.getresponse();require(response.status==200,'candidate_dispatch_not_ready');response.read(65536)
  finally:client.close()
  # This durable decision authorizes candidate-only startup even if CAS or its
  # response fails. No cleanup/revocation/SQLite fallback is allowed afterward.
  marker=private_json(STORAGE_MARKER);require(marker.get('contract')=='postgres-conversation-v2' and re.fullmatch('[a-f0-9]{64}',marker.get('database_digest','')),'storage_marker')
  row=inspect_container(self.candidate['containerId']);self.verify_candidate_row(row)
  active={'version':1,'image':self.image,'approval':approval_binding(self.approval),'candidate':self.candidate,'candidateProfileDigest':self.state['candidateBridgeProfileDigest'],'storageMarkerDigest':fingerprint(marker),'mounts':candidate_mounts(row,self.approval),'configDigest':self.state['configDigest']}
  if self.approval.get('contract')=='harbor-local-assurance-deploy-v1':active['storageMarkerPath']=str(STORAGE_MARKER)
  if ACTIVE.exists():require({k:v for k,v in private_json(ACTIVE).items() if k!='networkSnapshot'}==active,'active_binding_collision')
  else:active['networkSnapshot']=network_snapshot(self.state);save_json(ACTIVE,active)
  entry_config=private_json(ENTRY_CONFIG);require(entry_config.get('slots',{}).get('candidate')=='/run/harbor-entry/candidate.sock','entry_candidate_slot');entry_config['initialSlot']='candidate';save_json(ENTRY_CONFIG,entry_config)
  self.state['phase']='activating';save_state(self.state)
  epoch=self.state['drainReceipt']['entryEpoch'];entry=entry_state('activate',epoch,'candidate')
  require(entry.get('phase')=='active' and entry.get('slot')=='candidate' and entry.get('epoch')==epoch+1,'entry_activate_failed')
  self.activated=True;self.state['phase']='activated';self.state['activatedEpoch']=entry['epoch'];save_state(self.state)
  durable=private_json(ACTIVE);durable['networkSnapshot']=network_snapshot(self.state);save_json(ACTIVE,durable);return entry


def authorize_deployment(*,image,approval,local_assurance=False):
 require(os.geteuid()==0 and isinstance(image,str) and re.fullmatch('[a-zA-Z0-9_.:/-]+:[a-f0-9]{40}',image),'deployment_image')
 require(type(local_assurance) is bool,'local_assurance_mode')
 if local_assurance:
  require(local_assurance_module().validate_authorization(image,approval),'local_assurance_approval')
 else:
  require(isinstance(approval,dict) and approval.get('contract')=='harbor-storage-activation-deploy-v1' and approval.get('revision')==image.rsplit(':',1)[1],'deployment_approval')
 config=validate(private_json(PROFILE));state=private_json(STATE);require(state.get('configDigest')==fingerprint(config) and state.get('phase') in ['holding','candidate-binding','candidate-bound','activating','activated'],'deployment_state')
 binding={'image':image,'approval':approval_binding(approval)}
 require(state.get('deploymentBinding') in [None,binding],'deployment_binding_changed')
 if approval.get('state')=='failed' and state.get('phase')=='holding' and state.get('writerAuthorized') and not state.get('candidate') and not state.get('candidateIntent'):
  require(not deployment_inventory(approval),'failed_candidate_not_cleaned');state['candidateCleanupVerified']=True;save_state(state)
 context=EntryDeployment(image,approval,config,state)
 if not state.get('deploymentBinding'):state['deploymentBinding']=binding;save_state(state)
 context.enter_probe_worker()
 context.allow_probe_worker()
 return context


def approval_binding(approval):return {key:approval.get(key) for key in ['contract','deploymentId','receiptDigest','configDigest','manifestDigest','revision','sourceFence']}


def deployment_inventory(approval):
 values=execute(['/usr/bin/docker','container','ls','--all','--no-trunc','--filter','label=com.harbor.activation-deployment-id='+approval['deploymentId'],'--filter','label=com.harbor.activation-receipt='+approval['receiptDigest'],'--format','{{.ID}}']).splitlines()
 require(all(re.fullmatch('[a-f0-9]{64}',value) for value in values),'deployment_inventory_invalid');return values


def network_snapshot(state):return {key:copy.deepcopy(state.get(key)) for key in ['rulesDigest','mangleDigest','tcDigests','sourceBridgePort','configDigest',*DYNAMIC_KEYS,*CANDIDATE_NETWORK_KEYS] if key in state}


def candidate_mounts(row,approval=None):
 root='/opt/product-development/harbor-local-gateway'
 if (approval or {}).get('contract')=='harbor-local-assurance-deploy-v1':root=str(local_assurance_module().RUN)
 mounts=[{'source':v.get('Source'),'destination':v.get('Destination'),'readWrite':v.get('RW')} for v in row.get('Mounts',[]) if v.get('Destination') in ['/app/data','/app/runtime']]
 require({v['destination']:v['source'] for v in mounts}=={'/app/data':root+'/pg-data','/app/runtime':root+'/runtime'},'candidate_material_mounts')
 return sorted(mounts,key=lambda v:v['destination'])


def active_record():
 active=private_json(ACTIVE);config=validate(private_json(PROFILE));verify_candidate_bridge(active.get('networkSnapshot',{}))
 require(active.get('version')==1 and active.get('configDigest')==fingerprint(config),'active_configuration_changed')
 marker=STORAGE_MARKER
 if active.get('approval',{}).get('contract')=='harbor-local-assurance-deploy-v1':marker=local_assurance_module().verified_marker_path(active['image'],active)
 require(active.get('storageMarkerDigest')==fingerprint(private_json(marker)),'active_material_marker_changed')
 temporary=object.__new__(EntryDeployment);temporary.image=active['image'];temporary.approval=active['approval']
 row=inspect_container(active['candidate']['containerId']);temporary.verify_candidate_row(row)
 require(row['Image']==active['candidate']['imageId'] and candidate_mounts(row,active['approval'])==active['mounts'],'active_candidate_materials_changed')
 # StartedAt/PID/IP are runtime facts, not the permanent release identity.
 address=temporary.endpoint(row);profile=private_json(CANDIDATE_PROFILE)
 require(profile.get('socketPath')=='/run/harbor-entry/candidate.sock','active_slot_changed')
 profile['sourceAddress']=address;profile['sourcePort']=active['candidate']['port'];save_json(CANDIDATE_PROFILE,profile)
 active['candidate']['address']=address;active['candidateProfileDigest']=fingerprint(profile)
 status=read_active_authorization(active['image'])
 require(status.get('state')=='active' and approval_binding(status)==active['approval'],'active_database_authorization_changed')
 save_json(ACTIVE,active);return active,config,row


def local_assurance_module():
 path=Path('/usr/local/lib/harbor/local-assurance-deploy.py')
 trusted_parents(path);info=path.lstat();require(stat.S_ISREG(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022,'local_assurance_helper_untrusted')
 spec=importlib.util.spec_from_file_location('harbor_local_assurance',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def read_active_authorization(image):
 if ACTIVE.exists() and private_json(ACTIVE).get('approval',{}).get('contract')=='harbor-local-assurance-deploy-v1':return local_assurance_module().active_authorization(image)
 node=Path('/opt/harbor-storage-tools/node/bin/node');helper=Path('/usr/local/lib/harbor/gateway-entry-activation-status.mjs');business=Path('/opt/harbor-storage-tools/app/storage-activation-deploy.mjs')
 for path in [node,helper,business]:
  trusted_parents(path);info=path.lstat();require(stat.S_ISREG(info.st_mode) and info.st_uid==0 and not info.st_mode&0o022,'activation_helper_untrusted')
 try:result=subprocess.run([str(node),str(helper)],input=json.dumps({'image':image}),capture_output=True,text=True,timeout=150,env={'PATH':'/usr/sbin:/usr/bin:/sbin:/bin','LANG':'C'})
 except subprocess.TimeoutExpired:raise RuntimeError('entry_controller_active_database_not_ready') from None
 require(len(result.stdout)<65536 and len(result.stderr)<65536,'activation_output_budget')
 if result.returncode==75:
  try:unavailable=json.loads(result.stderr)=={'code':'entry_status_unavailable'}
  except (ValueError,TypeError):unavailable=False
  if unavailable:raise RuntimeError('entry_controller_active_database_not_ready')
 require(result.returncode==0,'active_database_status_denied');return json.loads(result.stdout)


def socket_owner(path):
 trusted_parents(path);info=path.lstat();require(stat.S_ISSOCK(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600,'socket_untrusted')
 with socket.socket(socket.AF_UNIX,socket.SOCK_STREAM) as peer:
  peer.settimeout(1);peer.connect(str(path));pid,uid,gid=__import__('struct').unpack('3i',peer.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))
 require(uid==0,'socket_uid');return pid


def refreshed_bypass(config,slot='candidate'):
 result=[];profile=private_json(CANDIDATE_PROFILE if slot=='candidate' else LEGACY_PROFILE);pid=socket_owner(Path(profile['socketPath']));group=process_cgroup(pid)
 candidates=[v for v in config['bypassProcesses'] if v['cgroup']==group];require(candidates,'candidate_bridge_cgroup_changed')
 digest=hashlib.sha256(Path('/proc/'+str(pid)+'/exe').read_bytes()).hexdigest();require(any(v['executableSha256']==digest for v in candidates),'candidate_bridge_executable_changed')
 result.append({'pid':pid,'startTicks':process_ticks(pid),'cgroup':group,'executableSha256':digest,'cgroupInode':(Path('/sys/fs/cgroup')/group.lstrip('/')).stat().st_ino})
 current=os.getpid();current_group=process_cgroup(current);current_digest=hashlib.sha256(Path('/proc/self/exe').read_bytes()).hexdigest()
 if any(v['cgroup']==current_group and v['executableSha256']==current_digest for v in config['bypassProcesses']) and current_group!=group:result.append({'pid':current,'startTicks':process_ticks(current),'cgroup':current_group,'executableSha256':current_digest,'cgroupInode':(Path('/sys/fs/cgroup')/current_group.lstrip('/')).stat().st_ino})
 return result



def install_owned_network(runtime,previous):
 tables=json.loads(execute(['/usr/sbin/nft','--json','list','tables']));exists=any(v.get('table',{}).get('name')==TABLE for v in tables.get('nftables',[]))
 boot_guard=exists and previous and previous.get('phase')=='boot-guarded'
 if boot_guard:
  approved=validate(private_json(PROFILE));require(previous.get('configDigest')==fingerprint(approved) and previous.get('guardFingerprint')==guard_fingerprint(approved) and previous.get('rulesDigest')==state_rules_digest(),'boot_guard_unowned')
 if exists and not boot_guard:
  require(previous and previous.get('rulesDigest')==state_rules_digest() and previous.get('mangleDigest')==mangle_digest(),'active_resources_unowned')
  live={v['ifname'] for v in json.loads(execute(['/usr/sbin/ip','-j','link','show']))}
  for dev,digest in previous.get('tcDigests',{}).items():
   if dev not in live:continue
   filters=json.loads(execute(['/usr/sbin/tc','-j','filter','show','dev',dev,'ingress']))
   if not filters:continue
   require(tc_digest(dev)==digest,'active_tc_changed')
   for preference in sorted({item['pref'] for item in filters}):execute(['/usr/sbin/tc','filter','del','dev',dev,'ingress','protocol','ip','pref',str(preference)])
   require(not json.loads(execute(['/usr/sbin/tc','-j','filter','show','dev',dev,'egress'])),'active_foreign_filter');execute(['/usr/sbin/tc','qdisc','del','dev',dev,'clsact'])
  for binary,chain,parent in [('/usr/sbin/iptables',REPLY_CHAIN,'PREROUTING'),('/usr/sbin/iptables',BYPASS_CHAIN,'OUTPUT'),('/usr/sbin/ip6tables',BYPASS_CHAIN,'OUTPUT')]:
   execute([binary,'-w','5','-t','mangle','-D',parent,'-j',chain]);execute([binary,'-w','5','-t','mangle','-F',chain]);execute([binary,'-w','5','-t','mangle','-X',chain])
  execute(['/usr/sbin/ip','rule','del','fwmark',str(runtime['mark']),'lookup',str(runtime['routeTable']),'priority',str(runtime['rulePriority'])]);execute(['/usr/sbin/ip','route','del','local','0.0.0.0/0','dev','lo','table',str(runtime['routeTable'])])
 else:
  require(not any(v.get('priority')==runtime['rulePriority'] or str(v.get('table'))==str(runtime['routeTable']) for v in json.loads(execute(['/usr/sbin/ip','-j','rule','show']))),'active_rule_collision')
  for binary in ['/usr/sbin/iptables','/usr/sbin/ip6tables']:require(REPLY_CHAIN not in execute([binary,'-t','mangle','-S']) and BYPASS_CHAIN not in execute([binary,'-t','mangle','-S']),'active_mangle_collision')
 for dev in runtime['bridge']['ports']:require(all(v.get('kind') not in ['clsact','ingress'] for v in json.loads(execute(['/usr/sbin/tc','-j','qdisc','show','dev',dev]))),'active_qdisc_collision')
 nft((f'delete table inet {TABLE}\n' if exists else '')+network_rules(runtime))
 for args in mangle_commands(runtime):execute(args)
 for args in tc_commands(runtime):execute(args)
 execute(['/usr/sbin/ip','route','add','local','0.0.0.0/0','dev','lo','table',str(runtime['routeTable'])]);execute(['/usr/sbin/ip','rule','add','fwmark',str(runtime['mark']),'lookup',str(runtime['routeTable']),'priority',str(runtime['rulePriority'])])

def prepare_active_network():
 active,original,row=active_record();runtime=copy.deepcopy(original);attachment=next(v for v in row['NetworkSettings']['Networks'].values() if v.get('IPAddress'))
 network=json.loads(execute(['/usr/bin/docker','network','inspect',attachment['NetworkID']]))[0];require(network.get('Driver')=='bridge','active_network')
 name=(network.get('Options') or {}).get('com.docker.network.bridge.name') or ('docker0' if network['Name']=='bridge' else 'br-'+network['Id'][:12])
 bridge=json.loads(execute(['/usr/sbin/ip','-j','link','show','dev',name]))[0];ports=json.loads(execute(['/usr/sbin/ip','-j','link','show','master',name]))
 runtime['source']={**runtime['source'],'containerId':row['Id'],'imageId':row['Image'],'pid':row['State']['Pid'],'startedAt':row['State']['StartedAt'],'startTicks':process_ticks(row['State']['Pid']),'address':active['candidate']['address']}
 runtime['bridge']={'name':name,'address':attachment['Gateway'],'mac':bridge['address'],'ports':[v['ifname'] for v in ports]};runtime['candidate']=None;runtime['bypassProcesses']=refreshed_bypass(original)
 verify_environment(runtime)
 previous=private_json(STATE) if STATE.exists() else active.get('networkSnapshot');install_owned_network(runtime,previous)
 peer_mac=attachment.get('MacAddress','');require(re.fullmatch('(?:[a-f0-9]{2}:){5}[a-f0-9]{2}',peer_mac),'active_candidate_mac');execute(['/usr/sbin/ip','neigh','replace',runtime['source']['address'],'lladdr',peer_mac,'nud','reachable','dev',name])
 entry_config=private_json(ENTRY_CONFIG);entry_config['initialSlot']='candidate';save_json(ENTRY_CONFIG,entry_config)
 state={'version':1,'configDigest':fingerprint(original),'phase':'active-prepared','source':original['source'],'sourceBridgePort':None,'candidate':active['candidate'],'candidateBridgeProfileDigest':active['candidateProfileDigest'],'activeRuntimeConfig':runtime,'drainReceipt':{'entryEpoch':0},'rulesDigest':state_rules_digest(),'mangleDigest':mangle_digest(),'tcDigests':{dev:tc_digest(dev) for dev in runtime['bridge']['ports']}}
 state.update({key:copy.deepcopy(active.get('networkSnapshot',{})[key]) for key in CANDIDATE_NETWORK_KEYS if key in active.get('networkSnapshot',{})})
 save_state(state);active['networkSnapshot']=copy.deepcopy(state);save_json(ACTIVE,active);return state


def assert_start():
 path=Path(CONTROL)
 if path.exists():
  info=path.lstat();trusted_parents(path);require(stat.S_ISSOCK(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600,'startup_control_untrusted')
  try:socket_owner(path)
  except ConnectionRefusedError:
   now=path.lstat();require((info.st_dev,info.st_ino)==(now.st_dev,now.st_ino),'startup_control_changed');path.unlink()
  else:raise RuntimeError('entry_controller_entry_already_running')
 if ACTIVE.exists():
  config=private_json(ENTRY_CONFIG);require(config.get('initialSlot')=='candidate','startup_slot_not_committed');prepare_active_network()
 else:
  config=validate(private_json(PROFILE));verify_source(config);state=private_json(STATE);require(state.get('phase') in ['guarded','prepared','routed','withdrawn'],'legacy_startup_not_safe');require(private_json(ENTRY_CONFIG).get('initialSlot')=='legacy','legacy_startup_slot')
 return {'candidateOnly':ACTIVE.exists(),'sourceRestarted':False}


def restore_active():
 active,config,row=active_record();state=private_json(STATE);require(state.get('phase')=='active-prepared' and state.get('candidate')==active['candidate'],'active_network_not_prepared')
 verify_resources(state,state['activeRuntimeConfig']);entry=entry_state();require(entry.get('slot')=='candidate' and entry.get('phase')=='active','active_entry_not_ready')
 require(process_cgroup(socket_owner(Path(CONTROL)))==config['entryCgroup'],'active_entry_cgroup_changed')
 deadline=time.monotonic()+config['drainTimeoutMs']/1000
 while not candidate_slot_ready(state['activeRuntimeConfig']['bridge']['address'],active['candidate']['port']):
  require(time.monotonic()<deadline,'active_candidate_not_ready');time.sleep(0.05)
 nft(f'add element inet {TABLE} enabled_ports {{ {config["source"]["port"]} }}');state['phase']='activated';state['activatedEpoch']=entry['epoch'];save_state(state)
 active['networkSnapshot']=copy.deepcopy(state);save_json(ACTIVE,active);return {'phase':'activated','candidateOnly':True,'entryEpoch':entry['epoch'],'sourceRestarted':False}


def candidate_slot_ready(peer_address,port):
 profile=private_json(CANDIDATE_PROFILE);path=Path(profile['socketPath']);socket_owner(path)
 client=UnixHttp('localhost',timeout=2)
 def connect():
  client.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);client.sock.settimeout(2);client.sock.connect(str(path))
 client.connect=connect
 try:
  client.request('GET','/ready',headers={'Host':'127.0.0.1:'+str(port),'x-harbor-entry-client-address':peer_address});reply=client.getresponse();reply.read(65536);return reply.status==200
 except (OSError,http.client.HTTPException):return False
 finally:client.close()


def slot_start(slot):
 require(slot in ['legacy','candidate'],'slot')
 if ACTIVE.exists():require(slot=='candidate','legacy_restart_after_commit');active_record()
 else:verify_source(validate(private_json(PROFILE)))
 path=Path('/run/harbor-entry/'+slot+'.sock')
 if path.exists():
  info=path.lstat();trusted_parents(path);require(stat.S_ISSOCK(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600,'slot_socket_untrusted')
  try:socket_owner(path)
  except ConnectionRefusedError:
   current=path.lstat();require((current.st_dev,current.st_ino)==(info.st_dev,info.st_ino),'slot_socket_changed');path.unlink()
  else:raise RuntimeError('entry_controller_slot_already_running')
 return {'slot':slot,'sourceRestarted':False}


def refresh_active():
 if not ACTIVE.exists():return {'committed':False,'sourceRestarted':False}
 active_record();resuming=False
 if Path(CONTROL).exists():
  try:entry=entry_state()
  except (OSError,http.client.HTTPException):entry=None
  if entry is not None:
   require(entry.get('slot')=='candidate' and entry.get('phase') in ['active','holding','drained'],'refresh_entry_slot')
   if entry['phase']=='active':entry=entry_state('hold',entry['epoch'])
   if entry['phase']=='holding':entry=entry_state('drain',entry['epoch'])
   resuming=True
 prepare_active_network()
 if resuming:
  entry_state('resume',entry['epoch']);return restore_active()
 return {'phase':'active-prepared','sourceRestarted':False}


def main():
 require(os.geteuid()==0,'root_required');require(len(sys.argv) in [2,3] and sys.argv[1] in ['guard','prepare','arm','withdraw','hold-drain','status','assert-start','restore-active','refresh-active','slot-start'] and (len(sys.argv)==3)==(sys.argv[1]=='slot-start'),'action')
 config=validate(private_json(PROFILE));trusted_parents(STATE)
 lock=STATE.with_suffix('.lock');fd=os.open(lock,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
 try:
  info=os.fstat(fd);require(stat.S_ISREG(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o600 and info.st_nlink==1,'lock_untrusted');fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
  action=sys.argv[1]
  if action=='assert-start':value=assert_start()
  elif action=='restore-active':value=restore_active()
  elif action=='refresh-active':value=refresh_active()
  elif action=='slot-start':value=slot_start(sys.argv[2])
  else:value=run_action(config,action)
  print(json.dumps(value,separators=(',',':')))
 finally:os.close(fd)


if __name__=='__main__':
 try:main()
 except BaseException as error:
  code=str(error) if isinstance(error,RuntimeError) and re.fullmatch('entry_controller_[a-z_]+',str(error)) else 'entry_controller_failed_closed'
  print(json.dumps({'error':{'code':code},'sourceStopped':False,'automaticSqliteRollback':False}));raise SystemExit(1)
