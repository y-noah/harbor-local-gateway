#!/usr/bin/python3
"""Run only inside a fresh root network + private mount namespace.

Example: env HARBOR_ENTRY_TEST_NODE=/absolute/node unshare --net --mount \
  --propagation private python3 -I -B tests/gateway-legacy-transparent-netns.py
This verifies transport only; it never fences a source or activates PostgreSQL.
"""
import os,subprocess,sys,json,time,signal
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
def run(*args):
 p=subprocess.run(args,capture_output=True,text=True,timeout=10)
 if p.returncode:raise RuntimeError('command_failed '+args[0]+' '+p.stderr[:500])
 return p.stdout
def child(code,net=True):
 p=subprocess.Popen((['/usr/bin/unshare','--net'] if net else [])+['/usr/bin/python3','-u','-c',code],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);children.append(p);ready(p);return p
def ready(p):
 if p.stdout.readline().strip()!='ready':raise RuntimeError('private fixture startup failed: '+p.stderr.read(4096))
def ip(p,*args):return run('/usr/bin/nsenter','--net=/proc/'+str(p.pid)+'/ns/net','/usr/sbin/ip',*args)
backend_code="""import socket,json,threading,http.server
print('ready',flush=True)
input()
class Handler(http.server.BaseHTTPRequestHandler):
 protocol_version='HTTP/1.1'
 def do_GET(self):
  body=json.dumps({'peer':self.client_address[0],'body':self.path,'host':self.headers.get('Host'),'cookie':self.headers.get('Cookie'),'origin':self.headers.get('Origin'),'forwarded':self.headers.get('X-Forwarded-For')}).encode()
  self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def log_message(self,*args):pass
s=http.server.ThreadingHTTPServer(('0.0.0.0',8092),Handler);s.daemon_threads=True
print('listening',flush=True);s.serve_forever()
"""
client_code="""import socket,json
print('ready',flush=True)
sockets={}
for line in __import__('sys').stdin:
 v=json.loads(line)
 if v['op']=='open':
  s=socket.create_connection(('10.61.0.1',8092),3);sockets[v['id']]=(s,s.makefile('rb'));print('opened',flush=True)
 else:
  s,f=sockets[v['id']];s.sendall(('GET /'+v['body']+' HTTP/1.1\\r\\nHost: public.example:8092\\r\\nOrigin: http://public.example:8092\\r\\nCookie: session=synthetic\\r\\nX-Forwarded-For: 203.0.113.9\\r\\nContent-Length: 0\\r\\nConnection: keep-alive\\r\\n\\r\\n').encode())
  status=f.readline();headers={}
  while True:
   row=f.readline()
   if row==b'\\r\\n':break
   name,value=row.split(b':',1);headers[name.lower()]=value.strip()
  body=f.read(int(headers[b'content-length']))
  print(body.decode(),flush=True)
"""
bridge_code="""import asyncio,importlib.util,json,sys,ctypes
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
 bridge=await module.LegacyTransparentBridge({'version':1,'socketPath':'/run/harbor-entry/legacy.sock','sourceAddress':'10.62.0.2','sourcePort':8092,'maxConnections':8,'maxHeaderBytes':32768,'headerTimeoutMs':1000,'connectTimeoutMs':1000}).start()
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
entry_code="""import {createGatewayEntry} from '/run/harbor-entry/app/gateway-entry.mjs';import readline from 'node:readline';const entry=createGatewayEntry({route:{socketPath:'/run/harbor-entry/legacy.sock'},limits:{maxInflightRequests:4,maxQueuedRequests:8,maxQueuedBytes:65536,maxRequestBytes:4096,maxWaitMs:3000}});await new Promise(r=>entry.server.listen(8093,'0.0.0.0',r));console.log('ready');for await(const cmd of readline.createInterface({input:process.stdin})){if(cmd==='hold')entry.hold();if(cmd==='resume')entry.resume();console.log(JSON.stringify(entry.status()));}
"""
def ask(p,v):
 p.stdin.write(json.dumps(v)+'\n');p.stdin.flush();return p.stdout.readline().strip()
try:
 links=json.loads(run('/usr/sbin/ip','-j','link','show'));require(len(links)==1 and links[0]['ifname']=='lo','requires a fresh empty network namespace')
 require(all('shared:' not in row.split(' - ')[0] for row in Path('/proc/self/mountinfo').read_text().splitlines()),'requires private mount propagation')
 backend=child(backend_code);client=child(client_code)
 run('/usr/sbin/ip','link','set','lo','up')
 for prefix,p,network in [('hc',client,'10.61.0'),('hb',backend,'10.62.0')]:
  run('/usr/sbin/ip','link','add',prefix+'r','type','veth','peer','name',prefix+'p')
  run('/usr/sbin/ip','link','set',prefix+'p','netns',str(p.pid))
  run('/usr/sbin/ip','addr','add',network+'.1/24','dev',prefix+'r');run('/usr/sbin/ip','link','set',prefix+'r','up')
  ip(p,'link','set','lo','up');ip(p,'addr','add',network+'.2/24','dev',prefix+'p');ip(p,'link','set',prefix+'p','up');ip(p,'route','add','default','via',network+'.1')
 run('/usr/sbin/sysctl','-q','-w','net.ipv4.ip_forward=1')
 for interface in ['all','default','hcr','hbr','lo']:run('/usr/sbin/sysctl','-q','-w','net.ipv4.conf.'+interface+'.rp_filter=0')
 backend.stdin.write('start\n');backend.stdin.flush();require(backend.stdout.readline().strip()=='listening')
 run('/usr/bin/mount','-t','tmpfs','-o','mode=755','tmpfs','/run')
 from pathlib import Path
 Path('/run/harbor-entry').mkdir(mode=0o755)
 module=Path('/run/harbor-entry/bridge.py');module.write_bytes((SOURCE/'ci/gateway-legacy-transparent.py').read_bytes());module.chmod(0o644)
 app=Path('/run/harbor-entry/app');app.mkdir(mode=0o755)
 for name in ['gateway-entry.mjs','gateway-entry-forward.mjs','gateway-transport.mjs']:
  target=app/name;target.write_bytes((SOURCE/name).read_bytes());target.chmod(0o644)
 proxy=subprocess.Popen(['/usr/bin/python3','-u','-c',bridge_code,str(module)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);children.append(proxy);ready(proxy)
 denied=run('/usr/bin/python3','-I','-B','-c',"import os,socket;os.setgroups([]);os.setgid(1000);os.setuid(1000);s=socket.socket(socket.AF_UNIX);\ntry:s.connect('/run/harbor-entry/legacy.sock')\nexcept PermissionError:raise SystemExit(0)\nraise SystemExit(2)")
 malformed=run('/usr/bin/python3','-I','-B','-c',"import socket;s=socket.socket(socket.AF_UNIX);s.settimeout(2);s.connect('/run/harbor-entry/legacy.sock');s.sendall(b'GET / HTTP/1.1\\r\\nHost: synthetic\\r\\nx-harbor-entry-client-address: 10.61.0.2\\r\\nx-harbor-entry-client-address: 10.61.0.3\\r\\n\\r\\n');raise SystemExit(0 if s.recv(1)==b'' else 2)")
 entry=subprocess.Popen([NODE,'--input-type=module','-e',entry_code],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True);children.append(entry);ready(entry)
 run('/usr/sbin/iptables','-t','nat','-A','PREROUTING','-i','hcr','-p','tcp','--dport','8092','-j','DNAT','--to-destination','10.62.0.2:8092')
 require(ask(client,{'op':'open','id':'old'})=='opened')
 before=json.loads(ask(client,{'op':'send','id':'old','body':'before'}));require(before['peer']=='10.61.0.2')
 run('/usr/sbin/ip','rule','add','fwmark','0x61','lookup','161','priority','161')
 run('/usr/sbin/ip','route','add','local','0.0.0.0/0','dev','lo','table','161')
 run('/usr/sbin/iptables','-t','mangle','-A','PREROUTING','-i','hbr','-p','tcp','-m','socket','--transparent','-j','MARK','--set-mark','0x61')
 run('/usr/sbin/iptables','-t','nat','-I','PREROUTING','1','-i','hcr','-p','tcp','--dport','8092','-j','DNAT','--to-destination','10.61.0.1:8093')
 persistent=json.loads(ask(client,{'op':'send','id':'old','body':'persistent'}));require(persistent['peer']=='10.61.0.2')
 require(ask(client,{'op':'open','id':'proxy'})=='opened')
 transparent=json.loads(ask(client,{'op':'send','id':'proxy','body':'transparent'}));require(transparent['peer']=='10.61.0.2')
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
 held=json.loads(client.stdout.readline());require(held['peer']=='10.61.0.2' and held['body']=='/held')
 run('/usr/sbin/iptables','-t','nat','-D','PREROUTING','-i','hcr','-p','tcp','--dport','8092','-j','DNAT','--to-destination','10.61.0.1:8093')
 require(ask(client,{'op':'open','id':'rollback'})=='opened')
 rollback=json.loads(ask(client,{'op':'send','id':'rollback','body':'rollback'}));require(rollback['peer']=='10.61.0.2')
 print(json.dumps({'privateNetns':True,'originalPersistentConnectionPreserved':persistent,'newTransparentConnection':transparent,'rollbackNewConnection':rollback,'hostNetworkChanged':False,'actualEntryUnixBridge':True,'bridgeOnlyCapNetRaw':True,'blockedConnectCancellationAndSlotRelease':True,'bufferedBodyCancellation':True,'nonRootSocketAccessDenied':True,'duplicateIdentityRejected':True,'hostOriginCookiePreserved':True,'forgedForwardedStripped':True,'holdingResumePreservedPeer':held}),flush=True)
finally:
 for p in children:
  p.terminate()
 for p in children:
  try:p.wait(timeout=3)
  except subprocess.TimeoutExpired:p.kill();p.wait()
 signal.alarm(0)
