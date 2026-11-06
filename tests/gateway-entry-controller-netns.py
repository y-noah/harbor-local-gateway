#!/usr/bin/python3
"""Run only inside a fresh root network + private mount namespace.

Example: env HARBOR_ENTRY_TEST_NODE=/absolute/node unshare --net --mount \
  --propagation private python3 -I -B tests/gateway-legacy-transparent-netns.py
This verifies transport only; it never fences a source or activates PostgreSQL.
"""
import os,subprocess,sys,json,time,signal,uuid,importlib.util,hashlib
from pathlib import Path
def require(value,message="private network fixture check failed"):
 if not value:raise RuntimeError(message)
require(os.getuid()==0 and os.readlink('/proc/self/ns/net')!=os.readlink('/proc/1/ns/net'))
require(os.readlink('/proc/self/ns/mnt')!=os.readlink('/proc/1/ns/mnt'))
def expired(*args):raise TimeoutError('private network fixture deadline')
signal.signal(signal.SIGALRM,expired);signal.alarm(45)
SOURCE=Path(__file__).resolve().parents[1]
NODE=os.environ.get('HARBOR_ENTRY_TEST_NODE','/usr/bin/node')
children=[]
cgroup_base=None
original_cgroup=Path("/sys/fs/cgroup") / next(row[3:] for row in Path("/proc/self/cgroup").read_text().splitlines() if row.startswith("0::")).lstrip("/")
def run(*args):
 p=subprocess.run(args,capture_output=True,text=True,timeout=10)
 if p.returncode:raise RuntimeError('command_failed '+args[0]+' '+p.stderr[:500])
 return p.stdout
def child(code,net=True):
 p=subprocess.Popen((['/usr/bin/unshare','--net'] if net else [])+['/usr/bin/python3','-u','-c',code],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);children.append(p);ready(p);return p
def ready(p):
 if p.stdout.readline().strip()!='ready':raise RuntimeError('private fixture startup failed: '+p.stderr.read(4096))
def ip(p,*args):return run('/usr/bin/nsenter','--net=/proc/'+str(p.pid)+'/ns/net','/usr/sbin/ip',*args)
backend_code="""import socket,json,threading,http.server,signal
print('ready',flush=True)
input()
class Handler(http.server.BaseHTTPRequestHandler):
 protocol_version='HTTP/1.1'
 def do_GET(self):
  body=json.dumps({'generation':'old','peer':self.client_address[0],'body':self.path,'host':self.headers.get('Host'),'cookie':self.headers.get('Cookie'),'origin':self.headers.get('Origin'),'forwarded':self.headers.get('X-Forwarded-For')}).encode()
  self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def log_message(self,*args):pass
s=http.server.ThreadingHTTPServer(('0.0.0.0',8092),Handler);s.daemon_threads=True
signal.signal(signal.SIGTERM,lambda *args:threading.Thread(target=s.shutdown,daemon=True).start())
print('listening',flush=True);s.serve_forever();s.server_close()
"""
client_code="""import socket,json
print('ready',flush=True)
sockets={}
for line in __import__('sys').stdin:
 v=json.loads(line)
 if v['op']=='close_all':
  for s,f in sockets.values():f.close();s.close()
  sockets.clear();print('closed',flush=True);continue
 if v['op']=='open':
  s=socket.create_connection((v.get('host','10.62.0.2'),v.get('port',8092)),3);sockets[v['id']]=(s,s.makefile('rb'));print('opened',flush=True)
 else:
  s,f=sockets[v['id']];s.sendall(('GET /'+v['body']+' HTTP/1.1\\r\\nHost: public.example:8092\\r\\nOrigin: http://public.example:8092\\r\\nCookie: session=synthetic\\r\\nX-Forwarded-For: 203.0.113.9\\r\\nContent-Length: 0\\r\\nConnection: keep-alive\\r\\n\\r\\n').encode())
  status=f.readline();headers={}
  while True:
   row=f.readline()
   if row==b'\\r\\n':break
   name,value=row.split(b':',1);headers[name.lower()]=value.strip()
  body=f.read(int(headers[b'content-length']))
  data=json.loads(body);data['responseConnection']=headers.get(b'connection',b'').decode();print(json.dumps(data),flush=True)
"""
bridge_code="""import asyncio,importlib.util,json,sys,ctypes
from pathlib import Path
def require(value,message="bridge capability check failed"):
 if not value:raise RuntimeError(message)
class Header(ctypes.Structure):_fields_=[('version',ctypes.c_uint32),('pid',ctypes.c_int)]
class Data(ctypes.Structure):_fields_=[('effective',ctypes.c_uint32),('permitted',ctypes.c_uint32),('inheritable',ctypes.c_uint32)]
libc=ctypes.CDLL(None,use_errno=True)
for cap in range(41):
 if cap!=13:require(libc.prctl(24,cap,0,0,0)==0)
require(libc.prctl(38,1,0,0,0)==0)
header=Header(0x20080522,0);data=(Data*2)(Data(1<<13,1<<13,0),Data(0,0,0));require(libc.capset(ctypes.byref(header),ctypes.byref(data))==0)
require(int(next(line.split()[1] for line in open('/proc/self/status') if line.startswith('CapEff:')),16)==1<<13)
spec=importlib.util.spec_from_file_location('bridge',sys.argv[1]);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
async def run():
 module.PROFILE=Path('/run/harbor-entry/legacy-profile.json')
 bridge=await module.LegacyTransparentBridge(module.load_configuration(),reload_configuration=module.load_configuration).start()
 # Only connection establishment is controlled. The UDS and disconnect are
 # real, including a body large enough to pause StreamReader transport reads.
 from unittest.mock import patch
 loop=asyncio.get_running_loop();original=loop.sock_connect
 for body in [b'',b'x'*131072]:
  connecting=asyncio.Event();release=asyncio.Event();sockets=[];actually_connected=[]
  async def blocked(sock,address):
   if address!=('10.62.0.2',8092):return await original(sock,address)
   sockets.append(sock);connecting.set();await release.wait()
   actually_connected.append(True);await original(sock,address)
  with patch.object(loop,'sock_connect',blocked):
   reader,writer=await asyncio.wait_for(asyncio.open_unix_connection('/run/harbor-entry/legacy.sock'),1)
   writer.write(b'POST /cancelled-before-connect HTTP/1.1\\r\\nHost: synthetic\\r\\nx-harbor-entry-client-address: 10.61.0.2\\r\\nContent-Length: '+str(len(body)).encode()+b'\\r\\n\\r\\n'+body)
   await writer.drain();await asyncio.wait_for(connecting.wait(),1)
   writer.transport.abort();await asyncio.wait_for(writer.wait_closed(),1)
   deadline=loop.time()+0.5
   while bridge.tasks:
    require(loop.time()<deadline,'cancelled connect retained its admission slot');await asyncio.sleep(0.005)
   require(sockets and all(sock.fileno()==-1 for sock in sockets),'cancelled raw socket still open')
   require(not actually_connected,'cancelled request reached upstream connect')
   release.set();await asyncio.sleep(0)
   require(not actually_connected,'cancelled connect restarted after release')
 print('ready',flush=True)
 await asyncio.Event().wait()
asyncio.run(run())
"""
entry_code="""import {createGatewayEntryProcess} from '/run/harbor-entry/app/gateway-entry-process.mjs';import readline from 'node:readline';import http from 'node:http';import {readFileSync} from 'node:fs';const entry=await createGatewayEntryProcess({config:JSON.parse(readFileSync('/run/harbor-entry/entry-config.json','utf8'))});console.log('ready');for await(const cmd of readline.createInterface({input:process.stdin})){if(['hold','resume'].includes(cmd)){await new Promise((resolve,reject)=>{const req=http.request({socketPath:'/run/harbor-entry/control.sock',path:'/control',method:'POST',headers:{'Content-Type':'application/json'}},res=>{res.resume();res.once('end',resolve);});req.on('error',reject);req.end(JSON.stringify({command:cmd,expectedEpoch:entry.state().epoch}));});}console.log(JSON.stringify(entry.state()));}
"""
proxy_code="""import socket,threading,select
def serve(s):
 while True:
  c,a=s.accept();threading.Thread(target=copy,args=(c,),daemon=True).start()
def copy(c):
 with c,socket.create_connection(('10.62.0.2',8092)) as u:
  while True:
   for src in select.select([c,u],[],[])[0]:
    body=src.recv(65536)
    if not body:return
    (u if src is c else c).sendall(body)
for family,address in [(socket.AF_INET,'0.0.0.0'),(socket.AF_INET6,'::')]:
 s=socket.socket(family);s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
 if family==socket.AF_INET6:s.setsockopt(socket.IPPROTO_IPV6,socket.IPV6_V6ONLY,1)
 s.bind((address,8092));s.listen();threading.Thread(target=serve,args=(s,),daemon=True).start()
print('ready',flush=True);threading.Event().wait()
"""
def host_request(host,port=8092,outside=False,forged_mark=False):
 setup="import socket;s=socket.socket();s.settimeout(2);s.setsockopt(socket.SOL_SOCKET,socket.SO_MARK,98);s.connect(("+repr(host)+","+str(port)+"));h.sock=s;" if forged_mark else ''
 code="import http.client,json;h=http.client.HTTPConnection("+repr(host)+","+str(port)+",timeout=2);"+setup+"h.request('GET','/synthetic',headers={'Host':'public.example:8092','X-Forwarded-For':'203.0.113.9'});r=h.getresponse();print(r.read().decode())"
 def isolate():
  (cgroup_base/'entry/cgroup.procs').write_text(str(os.getpid()))
 p=subprocess.run(['/usr/bin/python3','-I','-B','-c',code],capture_output=True,text=True,timeout=4,preexec_fn=isolate if outside else None)
 require(p.returncode==0,'private probe failed '+p.stderr);return json.loads(p.stdout)
def ask(p,v):
 p.stdin.write(json.dumps(v)+'\n');p.stdin.flush();
 value=p.stdout.readline().strip()
 if not value:raise RuntimeError('private client failed: '+p.stderr.read(4096))
 return value
try:
 links=json.loads(run('/usr/sbin/ip','-j','link','show'));require(len(links)==1 and links[0]['ifname']=='lo','requires a fresh empty network namespace')
 require(all('shared:' not in row.split(' - ')[0] for row in Path('/proc/self/mountinfo').read_text().splitlines()),'requires private mount propagation')
 backend=child(backend_code);client=child(client_code)
 run('/usr/sbin/ip','link','set','lo','up')
 run('/usr/sbin/ip','link','add','hb0','type','bridge');run('/usr/sbin/ip','addr','add','10.62.0.1/24','dev','hb0');run('/usr/sbin/ip','link','set','hb0','up')
 for prefix,p,number in [('hc',client,'3'),('hb',backend,'2')]:
  run('/usr/sbin/ip','link','add',prefix+'r','type','veth','peer','name',prefix+'p');run('/usr/sbin/ip','link','set',prefix+'p','netns',str(p.pid));run('/usr/sbin/ip','link','set',prefix+'r','address','02:00:00:00:00:'+('01' if prefix=='hb' else '02'));run('/usr/sbin/ip','link','set',prefix+'r','master','hb0');run('/usr/sbin/ip','link','set',prefix+'r','up')
  ip(p,'link','set','lo','up');ip(p,'addr','add','10.62.0.'+number+'/24','dev',prefix+'p');ip(p,'link','set',prefix+'p','up');ip(p,'route','add','default','via','10.62.0.1')
 run('/usr/sbin/sysctl','-q','-w','net.ipv4.ip_forward=1')
 for interface in ['all','default','hcr','hbr','hb0','lo']:run('/usr/sbin/sysctl','-q','-w','net.ipv4.conf.'+interface+'.rp_filter='+('0' if interface=='lo' else '2'))
 brnf=Path('/proc/sys/net/bridge/bridge-nf-call-iptables')
 if brnf.exists():run('/usr/sbin/sysctl','-q','-w','net.bridge.bridge-nf-call-iptables=0')
 require(not brnf.exists() or brnf.read_text().strip()=='0','bridge netfilter unexpectedly active')
 backend.stdin.write('start\n');backend.stdin.flush();require(backend.stdout.readline().strip()=='listening')
 run('/usr/bin/mount','-t','tmpfs','-o','mode=755','tmpfs','/run')
 from pathlib import Path
 Path('/run/harbor-entry').mkdir(mode=0o755)
 module=Path('/run/harbor-entry/bridge.py');module.write_bytes((SOURCE/'ci/gateway-legacy-transparent.py').read_bytes());module.chmod(0o644)
 profile={'version':1,'socketPath':'/run/harbor-entry/legacy.sock','sourceAddress':'10.62.0.2','sourcePort':8092,'maxConnections':8,'maxHeaderBytes':32768,'headerTimeoutMs':1000,'connectTimeoutMs':1000}
 profile_path=Path('/run/harbor-entry/legacy-profile.json');profile_path.write_text(json.dumps(profile));profile_path.chmod(0o600)
 app=Path('/run/harbor-entry/app');app.mkdir(mode=0o755)
 for name in ['gateway-entry.mjs','gateway-entry-forward.mjs','gateway-transport.mjs','gateway-entry-process.mjs']:
  target=app/name;target.write_bytes((SOURCE/name).read_bytes());target.chmod(0o644)
 cgroup_base=Path('/sys/fs/cgroup')/('harbor-entry-proof-'+str(os.getpid())+'-'+uuid.uuid4().hex[:8]);cgroup_base.mkdir()
 for name in ['probe','legacy','entry','old-source','candidate','new-source','new-worker']:(cgroup_base/name).mkdir()
 (cgroup_base/'probe/cgroup.procs').write_text(str(os.getpid()))
 (cgroup_base/'old-source/cgroup.procs').write_text(str(backend.pid))
 proxy=subprocess.Popen(['/usr/bin/python3','-u','-c',bridge_code,str(module)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);children.append(proxy);ready(proxy);(cgroup_base/'legacy/cgroup.procs').write_text(str(proxy.pid))
 denied=run('/usr/bin/python3','-I','-B','-c',"import os,socket;os.setgroups([]);os.setgid(1000);os.setuid(1000);s=socket.socket(socket.AF_UNIX);\ntry:s.connect('/run/harbor-entry/legacy.sock')\nexcept PermissionError:raise SystemExit(0)\nraise SystemExit(2)")
 malformed=run('/usr/bin/python3','-I','-B','-c',"import socket;s=socket.socket(socket.AF_UNIX);s.settimeout(2);s.connect('/run/harbor-entry/legacy.sock');s.sendall(b'GET / HTTP/1.1\\r\\nHost: synthetic\\r\\nx-harbor-entry-client-address: 10.61.0.2\\r\\nx-harbor-entry-client-address: 10.61.0.3\\r\\n\\r\\n');raise SystemExit(0 if s.recv(1)==b'' else 2)")
 entry_configuration={'listenPort':8093,'slots':{'legacy':'/run/harbor-entry/legacy.sock','candidate':'/run/harbor-entry/candidate.sock'},'initialSlot':'legacy','limits':{'maxInflightRequests':4,'maxQueuedRequests':8,'maxQueuedBytes':65536,'maxRequestBytes':4096,'maxWaitMs':3000},'shutdownTimeoutMs':3000}
 entry_file=Path('/run/harbor-entry/entry-config.json');entry_file.write_text(json.dumps(entry_configuration));entry_file.chmod(0o600)
 entry=subprocess.Popen([NODE,'--input-type=module','-e',entry_code],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);children.append(entry);ready(entry);(cgroup_base/'entry/cgroup.procs').write_text(str(entry.pid))

 require(ask(client,{'op':'open','id':'old'})=='opened')
 before=json.loads(ask(client,{'op':'send','id':'old','body':'before'}));require(before['peer']=='10.62.0.3')
 docker_proxy=child(proxy_code,False)
 witnessed4=host_request('127.0.0.1');witnessed6=host_request('::1');require(witnessed4['peer']==witnessed6['peer'],'proxy family peers differ')
 witness_hash=hashlib.sha256(json.dumps({'proxyPid':docker_proxy.pid,'sourcePid':backend.pid,'v4':witnessed4,'v6':witnessed6},sort_keys=True).encode()).hexdigest()
 profile['legacyEffectivePeers']=[{'clientNetwork':network,'peerAddress':witnessed4['peer'],'witnessSha256':witness_hash} for network in ['127.0.0.0/8','::/0']]
 profile_path.write_text(json.dumps(profile))
 candidate_profile={**profile,'socketPath':'/run/harbor-entry/candidate.sock'};candidate_profile_path=Path('/run/harbor-entry/candidate-profile.json');candidate_profile_path.write_text(json.dumps(candidate_profile));candidate_profile_path.chmod(0o600)
 candidate_bridge_code=bridge_code.replace('legacy-profile.json','candidate-profile.json').replace('legacy.sock','candidate.sock')
 candidate_bridge=subprocess.Popen(['/usr/bin/python3','-u','-c',candidate_bridge_code,str(module)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);children.append(candidate_bridge);ready(candidate_bridge);(cgroup_base/'candidate/cgroup.procs').write_text(str(candidate_bridge.pid))
 cfg_module=Path('/run/harbor-entry/controller.py');cfg_module.write_bytes((SOURCE/'ci/gateway-entry-controller.py').read_bytes());cfg_module.chmod(0o644)
 spec=importlib.util.spec_from_file_location('controller',cfg_module);controller=importlib.util.module_from_spec(spec);spec.loader.exec_module(controller)
 def bound_process(pid,name):
  return {'pid':pid,'startTicks':controller.process_ticks(pid),'cgroup':'/'+cgroup_base.name+'/'+name,'executableSha256':hashlib.sha256(Path('/proc/'+str(pid)+'/exe').read_bytes()).hexdigest()}
 config={'version':1,'source':{'containerId':'a'*64,'imageId':'sha256:'+'b'*64,'pid':backend.pid,'startedAt':'private-fixture','startTicks':controller.process_ticks(backend.pid),'address':'10.62.0.2','port':8092},'entryPort':8093,'bridge':{'name':'hb0','address':'10.62.0.1','mac':json.loads(run('/usr/sbin/ip','-j','link','show','hb0'))[0]['address'],'ports':['hcr','hbr']},'entryCgroup':'/'+cgroup_base.name+'/entry','bypassProcesses':[bound_process(proxy.pid,'legacy'),bound_process(os.getpid(),'probe')],'mark':97,'routeTable':161,'rulePriority':161,'tcPreference':30000,'drainTimeoutMs':1000,'candidate':None}
 config['bypassMark']=98;config['bypassProcesses'].append(bound_process(candidate_bridge.pid,'candidate'));controller.validate(config);controller.verify_environment(config)
 commit=subprocess.run(['/usr/sbin/nft','-f','-'],input=controller.network_rules(config),capture_output=True,text=True,timeout=10);require(commit.returncode==0,'private controller rules failed: '+commit.stderr)
 for args in controller.mangle_commands(config):controller.execute(args)
 for args in controller.tc_commands(config):controller.execute(args)
 run('/usr/sbin/ip','route','add','local','0.0.0.0/0','dev','lo','table','161');run('/usr/sbin/ip','rule','add','fwmark','97','lookup','161','priority','161')
 controller.nft('add element inet harbor_entry enabled_ports { 8092 }')
 owned_state={'phase':'routed','rulesDigest':controller.state_rules_digest(),'mangleDigest':controller.mangle_digest(),'tcDigests':{dev:controller.tc_digest(dev) for dev in ['hcr','hbr']}}
 controller.verify_resources(owned_state,config)
 routed4=host_request('127.0.0.1',outside=True);routed6=host_request('::1',outside=True)
 require(routed4['peer']==witnessed4['peer'] and routed6['peer']==witnessed6['peer'] and routed4['forwarded'] is None and routed6['forwarded'] is None,'legacy effective peer changed')
 allowed_probe=host_request('10.62.0.2');other_root=host_request('10.62.0.2',outside=True)
 require(allowed_probe['forwarded']=='203.0.113.9' and other_root['forwarded'] is None,'cgroup probe exemption was not exact')
 require(host_request('10.62.0.2',outside=True,forged_mark=True)['forwarded'] is None,'caller socket mark bypassed cgroup evidence')
 blocked_code="import socket;\ntry:\n s=socket.create_connection(('10.62.0.1',8093),1);s.sendall(b'GET / HTTP/1.1\\r\\nHost: synthetic\\r\\n\\r\\n');body=s.recv(4096)\nexcept OSError:raise SystemExit(0)\nraise SystemExit(0 if not body else 2)"
 run('/usr/bin/nsenter','--net=/proc/'+str(client.pid)+'/ns/net','/usr/bin/python3','-I','-B','-c',blocked_code)
 persistent=json.loads(ask(client,{'op':'send','id':'old','body':'persistent'}));require(persistent['peer']=='10.62.0.3')
 require(ask(client,{'op':'open','id':'proxy'})=='opened')
 transparent=json.loads(ask(client,{'op':'send','id':'proxy','body':'transparent'}));require(transparent['peer']=='10.62.0.3')
 require(transparent['host']=='public.example:8092' and transparent['origin']=='http://public.example:8092' and transparent['cookie']=='session=synthetic' and transparent['forwarded'] is None)
 entry.stdin.write('hold\n');entry.stdin.flush();require(json.loads(entry.stdout.readline())['state']=='holding')
 require(ask(client,{'op':'open','id':'held'})=='opened')
 client.stdin.write(json.dumps({'op':'send','id':'held','body':'held'})+'\n');client.stdin.flush()
 deadline=time.monotonic()+2
 while True:
  entry.stdin.write('status\n');entry.stdin.flush();status=json.loads(entry.stdout.readline())
  if status['queued']==1:break
  require(time.monotonic()<deadline)
  time.sleep(.01)
 entry.stdin.write('resume\n');entry.stdin.flush();json.loads(entry.stdout.readline())
 held=json.loads(client.stdout.readline());require(held['peer']=='10.62.0.3' and held['body']=='/held')
 controller.verify_resources(owned_state,config);controller.nft('flush set inet harbor_entry enabled_ports');owned_state['phase']='withdrawn';controller.verify_resources(owned_state,config)
 require(ask(client,{'op':'open','id':'rollback'})=='opened')
 rollback=json.loads(ask(client,{'op':'send','id':'rollback','body':'rollback'}));require(rollback['peer']=='10.62.0.3')
 after_rollback=json.loads(ask(client,{'op':'send','id':'proxy','body':'existing-entry-after-rollback'}));require(after_rollback['peer']=='10.62.0.3' and after_rollback['forwarded'] is None,'rollback broke existing entry flow')
 # Exercise the production authorization/CAS API across a REAL source process,
 # netns/veth and cgroup lifetime. Only Docker's read-only metadata boundary is
 # adapted to these task-owned processes; this is not a Docker image proof.
 require(ask(client,{'op':'close_all'})=='closed','client sockets did not close')
 controller.PROFILE=Path('/run/harbor-entry/controller-profile.json');controller.STATE=Path('/run/harbor-entry/controller-state.json');controller.CANDIDATE_PROFILE=candidate_profile_path
 controller.ACTIVE=Path('/run/harbor-entry/active.json');controller.WRITER_AUTHORIZED=Path('/run/harbor-entry/writer-authorized.json');controller.ENTRY_CONFIG=Path('/run/harbor-entry/entry-config.json');controller.STORAGE_MARKER=Path('/run/harbor-entry/storage-marker.json')
 controller.save_json(controller.STORAGE_MARKER,{'contract':'postgres-conversation-v2','database_digest':'d'*64})
 approval={'contract':'harbor-storage-activation-deploy-v1','deploymentId':'private-fixture-activation','receiptDigest':'c'*64,'configDigest':'sha256:'+'d'*64,'manifestDigest':'sha256:'+'e'*64,'revision':'f'*40,'state':'pending'}
 image='registry.example/harbor:'+'f'*40;finished_at=None;new_backend=None;new_worker=None;candidate_ids={'gateway':'1'*64,'worker':'2'*64};candidate_address='10.62.0.2';candidate_started='private-new-started';candidate_link='hbp2'
 original_execute=controller.execute
 def owned_row(which):
  old=which=='old';p=backend if old else (new_backend if which=='gateway' else new_worker)
  require(p is not None,'private candidate not created');running=p.poll() is None
  return {'Id':config['source']['containerId'] if old else candidate_ids[which],'Image':config['source']['imageId'] if old else approval['configDigest'],'Name':'/harbor-local-gateway-previous' if old and not running else '/harbor-local-gateway'+('-worker' if which=='worker' else ''),'Config':{'Image':'private-old' if old else image,'Labels':{} if old else {'com.aistrong.project':'harbor-local-gateway','com.harbor.activation-deployment-id':approval['deploymentId'],'com.harbor.activation-receipt':approval['receiptDigest']}},'State':{'Running':running,'Restarting':False,'Paused':False,'Pid':p.pid if running else 0,'StartedAt':config['source']['startedAt'] if old else candidate_started,'FinishedAt':finished_at if old else None,'ExitCode':p.returncode if not running else 0,'Health':{'Status':'healthy'}},'HostConfig':{'RestartPolicy':{'Name':'no' if old and not running else 'unless-stopped'}},'Mounts':[{'Source':'/opt/product-development/harbor-local-gateway/pg-data','Destination':'/app/data','RW':True},{'Source':'/opt/product-development/harbor-local-gateway/runtime','Destination':'/app/runtime','RW':True}],'NetworkSettings':{'Networks':{'private-network':{'NetworkID':'private-network','IPAddress':('10.62.0.2' if old else candidate_address) if running and which!='worker' else '', 'Gateway':'10.62.0.1','MacAddress':json.loads(ip(new_backend,'-j','link','show',candidate_link))[0]['address'] if which=='gateway' and running else ''}}}}
 def fixture_execute(args,body=None,timeout=15):
  if args[:3]==['/usr/bin/docker','container','inspect']:
   ref=args[3]
   if ref==config['source']['containerId']:return json.dumps([owned_row('old')])
   if ref in ['harbor-local-gateway',candidate_ids['gateway']]:return json.dumps([owned_row('gateway')])
   if ref in ['harbor-local-gateway-worker',candidate_ids['worker']]:return json.dumps([owned_row('worker')])
   raise RuntimeError('private Docker identity unknown')
  if args[:3]==['/usr/bin/docker','container','ls']:
   if any(value.startswith('label=') for value in args):return '\n'.join(candidate_ids[role] for role,p in [('gateway',new_backend),('worker',new_worker)] if p is not None and p.poll() is None)
   cid=args[args.index('--filter')+1].split('=',1)[1]
   if cid==config['source']['containerId']:return cid+'\n'
   if cid==candidate_ids['gateway'] and new_backend is not None and new_backend.poll() is None:return cid+'\n'
   if cid==candidate_ids['worker'] and new_worker is not None and new_worker.poll() is None:return cid+'\n'
   return ''
  if args[:3]==['/usr/bin/docker','network','inspect']:return json.dumps([{'Id':'private-network','Name':'private-network','Driver':'bridge','Options':{'com.docker.network.bridge.name':'hb0'}}])
  if backend.poll() is not None and any('/proc/'+str(backend.pid)+'/' in arg for arg in args):raise RuntimeError('controller tried to reopen vanished source namespace')
  return original_execute(args,body,timeout)
 controller.execute=fixture_execute
 controller.local_container_inventory=lambda:[owned_row('old')]+[owned_row(role) for role,p in [('gateway',new_backend),('worker',new_worker)] if p is not None and p.poll() is None]
 controller.save_json(controller.PROFILE,config)
 controller.nft('add element inet harbor_entry enabled_ports { 8092 }');owned_state.update({'version':1,'phase':'routed','configDigest':controller.fingerprint(config),'source':config['source'],'sourceBridgePort':'hbr','bridgeIfindex':json.loads(run('/usr/sbin/ip','-j','link','show','hb0'))[0]['ifindex']})
 controller.save_state(owned_state);drain=controller.run_action(config,'hold-drain');require(drain['drained'] and drain['sourceStopped'] is False,'real source drain was not proved')
 backend.terminate();require(backend.wait(timeout=3)==0,'source shutdown not graceful');finished_at='private-finished-'+str(time.time_ns())
 require(not Path('/proc/'+str(backend.pid)+'/ns/net').exists(),'old netns still reachable')
 deadline=time.monotonic()+2
 while any(v['ifname']=='hbr' for v in json.loads(run('/usr/sbin/ip','-j','link','show'))):
  require(time.monotonic()<deadline,'old source veth survived');time.sleep(0.01)
 require(json.loads(run('/usr/sbin/ip','-j','link','show','hb0'))[0]['address']!=config['bridge']['mac'],'source removal did not exercise automatic bridge MAC change')
 (cgroup_base/'old-source').rmdir();require(not (cgroup_base/'old-source').exists(),'old source cgroup survived')
 approval['sourceFence']={'scope':'docker-systemd','sourceStopped':True,'supervisorFenced':True,'productionVerified':True,'sourceContainerId':config['source']['containerId'],'sourceImageId':config['source']['imageId'],'sourceStartedAt':config['source']['startedAt'],'sourceFinishedAt':finished_at}
 context=controller.authorize_deployment(image=image,approval=approval)
 new_backend=child(backend_code.replace("'generation':'old'","'generation':'candidate'"));(cgroup_base/'new-source/cgroup.procs').write_text(str(new_backend.pid))
 new_worker=child("import signal,sys;signal.signal(signal.SIGTERM,lambda *args:sys.exit(0));print('ready',flush=True);input()");(cgroup_base/'new-worker/cgroup.procs').write_text(str(new_worker.pid))
 run('/usr/sbin/ip','link','add','hbr2','type','veth','peer','name','hbp2');run('/usr/sbin/ip','link','set','hbp2','netns',str(new_backend.pid));run('/usr/sbin/ip','link','set','hbr2','master','hb0');run('/usr/sbin/ip','link','set','hbr2','up')
 ip(new_backend,'link','set','lo','up');ip(new_backend,'addr','add','10.62.0.2/24','dev','hbp2');ip(new_backend,'link','set','hbp2','up');ip(new_backend,'route','add','default','via','10.62.0.1')
 new_backend.stdin.write('start\n');new_backend.stdin.flush();require(new_backend.stdout.readline().strip()=='listening','new backend not listening')
 bound=context.bind_candidate(owned_row('gateway'));require(bound['containerId']=='1'*64 and bound['address']=='10.62.0.2','candidate was not bound from current inspection')
 require(context.probe(8092,'/ready')==200,'private candidate probe failed')
 # Force a post-bind failure/confirmed reclaim, then retry this same release.
 failed={**approval,'state':'failed'}
 new_backend.terminate();new_worker.terminate();require(new_backend.wait(timeout=3)==0 and new_worker.wait(timeout=3)==0,'candidate reclaim did not confirm exit')
 deadline=time.monotonic()+2
 while any(v['ifname']=='hbr2' for v in json.loads(run('/usr/sbin/ip','-j','link','show'))):
  require(time.monotonic()<deadline,'failed candidate veth survived');time.sleep(0.01)
 context.unbind_candidate(activation=failed,removed_container_ids=['1'*64,'2'*64])
 context=controller.authorize_deployment(image=image,approval=failed);context.check({**approval,'state':'pending'});context.check({**approval,'state':'active'})
 require(controller.WRITER_AUTHORIZED.exists(),'writer authorization was not durable')
 candidate_ids={'gateway':'3'*64,'worker':'4'*64};candidate_link='hbp3'
 new_backend=child(backend_code.replace("'generation':'old'","'generation':'candidate'"));(cgroup_base/'new-source/cgroup.procs').write_text(str(new_backend.pid))
 new_worker=child("import signal,sys;signal.signal(signal.SIGTERM,lambda *args:sys.exit(0));print('ready',flush=True);input()");(cgroup_base/'new-worker/cgroup.procs').write_text(str(new_worker.pid))
 run('/usr/sbin/ip','link','add','hbr3','type','veth','peer','name','hbp3');run('/usr/sbin/ip','link','set','hbp3','netns',str(new_backend.pid));run('/usr/sbin/ip','link','set','hbr3','master','hb0');run('/usr/sbin/ip','link','set','hbr3','up')
 ip(new_backend,'link','set','lo','up');ip(new_backend,'addr','add',candidate_address+'/24','dev','hbp3');ip(new_backend,'link','set','hbp3','up');ip(new_backend,'route','add','default','via','10.62.0.1')
 new_backend.stdin.write('start\n');new_backend.stdin.flush();require(new_backend.stdout.readline().strip()=='listening','retry backend not listening')
 context.bind_candidate(owned_row('gateway'));activation={**approval,'state':'active'}
 real_entry_state=controller.entry_state;cas_calls=[]
 def lose_cas_response(command=None,epoch=None,slot=None):
  result=real_entry_state(command,epoch,slot)
  if command=='activate':cas_calls.append(True);raise ConnectionResetError('private CAS response lost')
  return result
 controller.entry_state=lose_cas_response
 try:context.activate(activation)
 except ConnectionResetError:pass
 else:raise RuntimeError('CAS response loss was not exercised')
 controller.entry_state=real_entry_state
 context=controller.authorize_deployment(image=image,approval=activation);result=context.activate(activation);same=context.activate(activation)
 require(result['slot']=='candidate' and same['epoch']==result['epoch'] and len(cas_calls)==1 and context.activated,'same successful CAS was repeated')
 require(ask(client,{'op':'open','id':'after-pg'})=='opened');new_reply=json.loads(ask(client,{'op':'send','id':'after-pg','body':'new-backend'}));require(new_reply['generation']=='candidate' and new_reply['peer']=='10.62.0.3' and new_reply['forwarded'] is None,'public request did not reach new generation through entry')
 # A completed writer binding survives entry/runtime loss and a legitimate
 # same-CID candidate restart with a different PID, StartedAt, IP and veth.
 require(ask(client,{'op':'close_all'})=='closed','completed clients not closed')
 # Reuse the committed forward-only recovery with a real bridge process and
 # cgroup inode replacement; never restart the retired source.
 controller.read_active_authorization=lambda requested_image:activation if requested_image==image else {}
 candidate_bridge.terminate();candidate_bridge.wait(timeout=3);(cgroup_base/'candidate').rmdir();(cgroup_base/'candidate').mkdir()
 require(controller.slot_start('candidate')['slot']=='candidate','committed candidate slot restart denied')
 plain_bridge_code=candidate_bridge_code[:candidate_bridge_code.index(' # Only connection establishment')]+candidate_bridge_code[candidate_bridge_code.index(" print('ready',flush=True)"):]
 candidate_bridge=subprocess.Popen(['/usr/bin/python3','-u','-c',plain_bridge_code,str(module)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);children.append(candidate_bridge);ready(candidate_bridge);(cgroup_base/'candidate/cgroup.procs').write_text(str(candidate_bridge.pid))
 require(controller.refresh_active()['candidateOnly'],'committed bridge restart did not restore forward-only target')
 require(ask(client,{'op':'open','id':'bridge-restarted'})=='opened');bridge_restarted=json.loads(ask(client,{'op':'send','id':'bridge-restarted','body':'bridge-restarted'}));require(bridge_restarted['generation']=='candidate' and bridge_restarted['peer']=='10.62.0.3','new bridge cgroup did not retain transparent PG target')
 require(ask(client,{'op':'close_all'})=='closed','bridge restart client not closed')
 entry.terminate();entry.wait(timeout=3)
 new_backend.terminate();require(new_backend.wait(timeout=3)==0,'candidate restart shutdown failed')
 deadline=time.monotonic()+2
 while any(v['ifname']=='hbr3' for v in json.loads(run('/usr/sbin/ip','-j','link','show'))):
  require(time.monotonic()<deadline,'old candidate veth survived restart');time.sleep(0.01)
 candidate_address='10.62.0.4';candidate_link='hbp4';candidate_started='private-legitimate-restart'
 new_backend=child(backend_code.replace("'generation':'old'","'generation':'candidate-restarted'"));(cgroup_base/'new-source/cgroup.procs').write_text(str(new_backend.pid))
 run('/usr/sbin/ip','link','add','hbr4','type','veth','peer','name','hbp4');run('/usr/sbin/ip','link','set','hbp4','netns',str(new_backend.pid));run('/usr/sbin/ip','link','set','hbr4','master','hb0');run('/usr/sbin/ip','link','set','hbr4','up')
 ip(new_backend,'link','set','lo','up');ip(new_backend,'addr','add',candidate_address+'/24','dev','hbp4');ip(new_backend,'link','set','hbp4','up');ip(new_backend,'route','add','default','via','10.62.0.1')
 new_backend.stdin.write('start\n');new_backend.stdin.flush();require(new_backend.stdout.readline().strip()=='listening','restarted candidate not listening')
 snapshot=controller.private_json(controller.STATE);live={v['ifname'] for v in json.loads(run('/usr/sbin/ip','-j','link','show'))}
 for dev in snapshot['tcDigests']:
  if dev not in live:continue
  for preference in sorted({item['pref'] for item in json.loads(run('/usr/sbin/tc','-j','filter','show','dev',dev,'ingress'))}):run('/usr/sbin/tc','filter','del','dev',dev,'ingress','protocol','ip','pref',str(preference))
  run('/usr/sbin/tc','qdisc','del','dev',dev,'clsact')
 for binary,chain,parent in [('/usr/sbin/iptables',controller.REPLY_CHAIN,'PREROUTING'),('/usr/sbin/iptables',controller.BYPASS_CHAIN,'OUTPUT'),('/usr/sbin/ip6tables',controller.BYPASS_CHAIN,'OUTPUT')]:
  run(binary,'-t','mangle','-D',parent,'-j',chain);run(binary,'-t','mangle','-F',chain);run(binary,'-t','mangle','-X',chain)
 run('/usr/sbin/nft','delete','table','inet',controller.TABLE);run('/usr/sbin/ip','rule','del','fwmark','97','lookup','161','priority','161');run('/usr/sbin/ip','route','del','local','0.0.0.0/0','dev','lo','table','161');controller.STATE.unlink()
 controller.read_active_authorization=lambda requested_image:activation if requested_image==image else {}
 require(controller.assert_start()['candidateOnly'],'startup did not require committed candidate')
 entry=subprocess.Popen([NODE,'--input-type=module','-e',entry_code],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);children.append(entry);ready(entry);(cgroup_base/'entry/cgroup.procs').write_text(str(entry.pid))
 restored=controller.restore_active();require(restored['candidateOnly'] and not restored['sourceRestarted'],'startup restored legacy')
 require(ask(client,{'op':'open','id':'reboot','host':'10.62.0.1'})=='opened');after_restart=json.loads(ask(client,{'op':'send','id':'reboot','body':'after-entry-restart'}))
 require(after_restart['generation']=='candidate-restarted' and after_restart['peer']=='10.62.0.3','restart did not dispatch to current same-CID candidate')
 context=controller.authorize_deployment(image=image,approval=activation);require(context.activate(activation)['epoch']==restored['entryEpoch'],'completed deployment was switched again after restart')
 print(json.dumps({'scope':'task-owned-real-netns-with-Docker-read-boundary-adapter','sourceRealExitAndNetnsVethCgroupGone':True,'candidateNewPidSameIpBound':True,'actualEntryProcessCasAndCandidateDispatch':True,'confirmedCandidateCleanupFailedToForwardRetry':True,'lostCasResponseIdempotent':True,'durableCandidateOnlyEntryRestart':True,'sameCandidateCidNewPidAddressVethAccepted':True,'emptyRuntimeRestoredFromDurableBinding':True,'privateNetns':True,'bridgeNetfilterDisabled':True,'sameBridgeWithoutGlobalBrNf':True,'automaticBridgeMacChangeHandled':True,'committedBridgeNewPidCgroupRestored':True,'targetedTcIngress':True,'actualControllerRuleBuilders':True,'retainedNatExpressionsOnWithdraw':True,'witnessedLegacyLoopbackIPv6PeerPreserved':True,'exactProbeCgroupExemption':True,'otherRootCannotBypass':True,'privateEntryDirectAccessDenied':True,'existingEntryAfterRollback':after_rollback,'originalPersistentConnectionPreserved':persistent,'newTransparentConnection':transparent,'rollbackNewConnection':rollback,'hostNetworkChanged':False,'actualEntryUnixBridge':True,'bridgeOnlyCapNetRaw':True,'blockedConnectCancellationAndSlotRelease':True,'bufferedBodyCancellation':True,'nonRootSocketAccessDenied':True,'duplicateIdentityRejected':True,'hostOriginCookiePreserved':True,'forgedForwardedStripped':True,'holdingResumePreservedPeer':held}),flush=True)
finally:
 for p in children:
  if p.poll() is None:p.terminate()
 for p in children:
  try:p.wait(timeout=3)
  except subprocess.TimeoutExpired:p.kill();p.wait()
 if cgroup_base is not None:
  (original_cgroup/'cgroup.procs').write_text(str(os.getpid()))
  for name in ['probe','legacy','entry','old-source','candidate','new-source','new-worker']:
   if (cgroup_base/name).exists():(cgroup_base/name).rmdir()
  cgroup_base.rmdir()
 signal.alarm(0)
