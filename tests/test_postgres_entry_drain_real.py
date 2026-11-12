"""Real entry HTTP/Unix control; no Docker, host configuration or live service."""
import concurrent.futures,http.client,importlib.util,json,select,subprocess,tempfile,time,unittest
from pathlib import Path
from types import SimpleNamespace
ROOT=Path(__file__).parents[1]
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,ROOT/path);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value
upgrade=load('real_drain_upgrade','ci/postgres-upgrade-deploy.py')
controller=load('real_drain_controller','ci/gateway-entry-controller.py')
NODE=r'''
import http from 'node:http';import readline from 'node:readline';import {once} from 'node:events';
import {createGatewayEntryProcess} from './gateway-entry-process.mjs';
const dir=process.argv[1],pending=new Set();
const upstream=http.createServer((req,res)=>{if(req.url==='/slow'){pending.add(res);res.writeHead(200);res.write('started');res.on('close',()=>pending.delete(res));}else res.end('queued-finished');});
upstream.listen(dir+'/candidate.sock');await once(upstream,'listening');
const reserve=http.createServer();reserve.listen(0,'127.0.0.1');await once(reserve,'listening');const port=reserve.address().port;await new Promise(r=>reserve.close(r));
const runtime=await createGatewayEntryProcess({controlPath:dir+'/control.sock',config:{listenPort:port,slots:{legacy:dir+'/legacy.sock',candidate:dir+'/candidate.sock'},initialSlot:'candidate',limits:{maxInflightRequests:8,maxQueuedRequests:8,maxQueuedBytes:8192,maxRequestBytes:1024,maxWaitMs:5000},shutdownTimeoutMs:500,drainTimeoutMs:2000}});
console.log(JSON.stringify({port}));
readline.createInterface({input:process.stdin}).on('line',async line=>{if(line==='release'){for(const res of pending)res.end('finished');}if(line==='close'){for(const res of pending)res.end('finished');await runtime.shutdown();upstream.close();}});
'''

class RealEntryDrain(unittest.TestCase):
 def fixture(self,budget):
  directory=tempfile.TemporaryDirectory(prefix='harbor-drain-');self.addCleanup(directory.cleanup)
  process=subprocess.Popen(['node','--input-type=module','-e',NODE,directory.name],cwd=ROOT,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  def cleanup():
   if process.poll() is None:
    try:process.stdin.write('close\n');process.stdin.flush();process.communicate(timeout=5)
    except (OSError,subprocess.TimeoutExpired):process.kill();process.communicate(timeout=5)
  self.addCleanup(lambda:[stream.close() for stream in [process.stdin,process.stdout,process.stderr]])
  self.addCleanup(cleanup)
  self.assertTrue(select.select([process.stdout],[],[],5)[0],'entry startup timed out')
  line=process.stdout.readline();self.assertTrue(line,process.stderr.read() if not line else line);port=json.loads(line)['port']
  def state(command=None,epoch=None,**kwargs):
   # Use the production Unix HTTP client and exact CAS protocol. The fixture
   # owns a user socket, so privileged filesystem admission is outside this test.
   client=controller.UnixHttp('localhost',timeout=kwargs.get('timeout',2));client.connect=lambda:None
   import socket
   client.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);client.sock.settimeout(kwargs.get('timeout',2));client.sock.connect(directory.name+'/control.sock')
   try:
    body=None if command is None else json.dumps({'command':command,'expectedEpoch':epoch});client.request('GET' if command is None else 'POST','/state' if command is None else '/control',body=body,headers={} if body is None else {'Content-Type':'application/json'});response=client.getresponse();value=json.loads(response.read());self.assertEqual(response.status,200,value);return value
   finally:client.close()
  entry=SimpleNamespace(ENTRY_CONFIG=Path(directory.name)/'config.json',private_json=lambda _: {'drainTimeoutMs':budget},entry_state=state)
  return process,port,entry
 def wait(self,predicate):
  deadline=time.monotonic()+3
  while True:
   value=predicate()
   if value:return value
   self.assertLess(time.monotonic(),deadline,'condition timed out');time.sleep(.01)
 def request(self,port,path):
  client=http.client.HTTPConnection('127.0.0.1',port,timeout=5)
  try:client.request('GET',path);res=client.getresponse();return res.status,res.read()
  finally:client.close()
 def test_busy_source_finishes_and_new_request_queues_until_resume(self):
  process,port,entry=self.fixture(2000)
  with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
   slow=pool.submit(self.request,port,'/slow');self.wait(lambda:entry.entry_state()['inflight']==1)
   drain=pool.submit(upgrade.drain_active_entry,entry);self.wait(lambda:entry.entry_state()['phase']=='holding')
   queued=pool.submit(self.request,port,'/new');self.wait(lambda:entry.entry_state()['queued']==1)
   self.assertFalse(slow.done());self.assertFalse(queued.done());process.stdin.write('release\n');process.stdin.flush()
   result=drain.result(timeout=3);self.assertEqual((result['phase'],result['inflight'],result['queued']),('drained',0,1));self.assertEqual(slow.result(timeout=3),(200,b'startedfinished'));self.assertFalse(queued.done())
   entry.entry_state('resume',result['epoch']);self.assertEqual(queued.result(timeout=3),(200,b'queued-finished'));self.assertEqual(entry.entry_state()['queued'],0)
 def test_reset_client_releases_forward_inflight_for_drain(self):
  _,port,entry=self.fixture(2000);client=http.client.HTTPConnection('127.0.0.1',port,timeout=3)
  try:
   client.request('GET','/slow');response=client.getresponse();self.assertEqual(response.status,200);self.wait(lambda:entry.entry_state()['inflight']==1)
   with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
    drain=pool.submit(upgrade.drain_active_entry,entry);self.wait(lambda:entry.entry_state()['phase']=='holding');response.close();client.close()
    result=drain.result(timeout=3);self.assertEqual((result['phase'],result['inflight']),('drained',0));entry.entry_state('resume',result['epoch'])
  finally:client.close()
 def test_budget_timeout_preserves_inflight_and_resume_releases_queue(self):
  process,port,entry=self.fixture(100)
  with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
   slow=pool.submit(self.request,port,'/slow');self.wait(lambda:entry.entry_state()['inflight']==1)
   drain=pool.submit(upgrade.drain_active_entry,entry);self.wait(lambda:entry.entry_state()['phase']=='holding')
   queued=pool.submit(self.request,port,'/new');self.wait(lambda:entry.entry_state()['queued']==1)
   with self.assertRaisesRegex(RuntimeError,'entry drain timeout'):drain.result(timeout=3)
   self.assertFalse(slow.done());held=entry.entry_state();self.assertEqual((held['phase'],held['inflight']),('holding',1))
   entry.entry_state('resume',held['epoch']);self.assertEqual(queued.result(timeout=3),(200,b'queued-finished'));process.stdin.write('release\n');process.stdin.flush();self.assertEqual(slow.result(timeout=3),(200,b'startedfinished'))

if __name__=='__main__':unittest.main()
