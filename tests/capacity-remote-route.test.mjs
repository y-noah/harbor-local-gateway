import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
const script=fileURLToPath(new URL('../tools/capacity/remote-route-proof.py',import.meta.url));
function python(args,input){return new Promise((resolve,reject)=>{const child=spawn('python3',['-I','-B',...args],{stdio:['pipe','pipe','pipe']}),out=[],err=[];const timer=setTimeout(()=>{child.kill('SIGKILL');reject(Error('route_test_timeout'));},10000);child.stdout.on('data',data=>out.push(data));child.stderr.on('data',data=>err.push(data));child.once('error',error=>{clearTimeout(timer);reject(error);});child.once('close',code=>{clearTimeout(timer);resolve({code,stdout:Buffer.concat(out).toString(),stderr:Buffer.concat(err).toString()});});child.stdin.on('error',()=>{});child.stdin.end(input);});}

test('fixed route observer rejects malformed attributes, mismatched kernel replies and caller onLink flags',{timeout:30000},async()=>{
 const source=`import importlib.util,struct\nspec=importlib.util.spec_from_file_location('route',${JSON.stringify(script)})\nr=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)\ndef rejected(call):\n try: call()\n except r.RouteError: return\n raise AssertionError('accepted malformed route')\nfor raw in [b'\\0',struct.pack('=HH',3,1),struct.pack('=HH',8,1),r.attribute(1,b'abcd')*2]: rejected(lambda:r.attributes(raw))\nassert r.attributes(r.attribute(1,b'abcd'))=={1:b'abcd'}\nclass Channel:\n def __init__(self,packet,peer=(0,0)):self.packet,self.peer=packet,peer\n def sendto(self,*args):pass\n def recvfrom(self,*args):return self.packet,self.peer\npayload=b'proof'\npacket=struct.pack('=IHHII',21,24,0,7,0)+payload\nassert r.exchange(Channel(packet),26,b'',0,7,24)==payload\nrejected(lambda:r.exchange(Channel(packet),26,b'',0,8,24))\nrejected(lambda:r.exchange(Channel(packet,(123,0)),26,b'',0,7,24))\nrejected(lambda:r.exchange(Channel(packet[:18]),26,b'',0,7,24))\nprint('route parser assertions passed')\n`;
 const result=await python(['-c',source],'');assert.equal(result.code,0,result.stderr);assert.match(result.stdout,/assertions passed/);
 for(const input of [{targetAddress:'127.0.0.1',onLink:true},{targetAddress:'not-an-address'},{targetAddress:'::1'}]){const row=await python([script],JSON.stringify(input));assert.equal(row.code,1);assert.deepEqual(JSON.parse(row.stdout),{ok:false,code:'capacity_remote_route_unproven'});}
});

test('actual RTNETLINK loopback observation cannot mint on-link LAN evidence',{timeout:15000},async()=>{
 const result=await python([script],JSON.stringify({targetAddress:'127.0.0.1'}));assert.equal(result.code,0,result.stderr||result.stdout);const {ok,network}=JSON.parse(result.stdout);assert.equal(ok,true);assert.equal(network.scope,'actual-rtnetlink-selected-ipv4-route');assert.equal(network.targetAddress,'127.0.0.1');assert.equal(network.onLink,false);assert.equal(typeof network.interface,'string');
});

test('route observer validates an optional actual socket UID without expanding the CLI input contract',async()=>{
 const source=`import importlib.util\nspec=importlib.util.spec_from_file_location('route',${JSON.stringify(script)})\nr=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)\nfor uid in [True,False,-1,4294967296,'0',0.5]:\n try: r.inspect('127.0.0.1',uid=uid)\n except r.RouteError: pass\n else: raise AssertionError('invalid UID accepted')\nprint('UID assertions passed')\n`;
 const result=await python(['-c',source],'');assert.equal(result.code,0,result.stderr);assert.match(result.stdout,/UID assertions passed/);
 const injected=await python([script],JSON.stringify({targetAddress:'127.0.0.1',uid:0}));assert.equal(injected.code,1);assert.equal(JSON.parse(injected.stdout).ok,false);
});

test('route lookup sends exact kernel socket UID and source address and accepts source-only replies',{timeout:15000},async()=>{
 const source=`import importlib.util,socket,struct\nspec=importlib.util.spec_from_file_location('route',${JSON.stringify(script)})\nr=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)\nclass Channel:\n def sendto(self,packet,*args): self.request=packet\n def recvfrom(self,*args):\n  payload=struct.pack('=BBBBBBBBI',socket.AF_INET,32,32,0,254,0,0,1,0)+r.attribute(4,struct.pack('=I',1))+r.attribute(2,socket.inet_pton(socket.AF_INET,'127.0.0.2'))\n  return struct.pack('=IHHII',len(payload)+16,24,0,7,0)+payload,(0,0)\nc=Channel();value=r.route(c,'127.0.0.1',7,uid=1001,source_address='127.0.0.2');assert value['source']=='127.0.0.2'\nassert struct.unpack_from('=BBBBBBBBI',c.request,16)[2]==32\nattrs=r.attributes(c.request[28:]);assert r.number(attrs[25])==1001;assert r.ipv4(attrs[2])=='127.0.0.2'\ntry: r.route(Channel(),'127.0.0.1',7,uid=1001,source_address='127.0.0.3')\nexcept r.RouteError: pass\nelse: raise AssertionError('different kernel source accepted')\nprint('source query assertions passed')\n`;
 const result=await python(['-c',source],'');assert.equal(result.code,0,result.stderr);assert.match(result.stdout,/source query assertions passed/);
 const actual=await python(['-c',`import importlib.util\ns=importlib.util.spec_from_file_location('route',${JSON.stringify(script)});r=importlib.util.module_from_spec(s);s.loader.exec_module(r);v=r.inspect('127.0.0.1',source_address='127.0.0.1');assert v['sourceAddress']=='127.0.0.1' and v['onLink'] is False\nprint('actual loopback source passed')`],'');assert.equal(actual.code,0,actual.stderr);
});

test('fixed Python loader executes the exact supplied route source bytes while retaining the bounded stdin contract',{timeout:15000},async()=>{
 const bytes=await readFile(script),loader="import base64,sys;exec(compile(base64.b64decode(sys.argv[1],validate=True),'<capacity-root-route>','exec'))";
 const result=await python(['-c',loader,bytes.toString('base64')],JSON.stringify({targetAddress:'127.0.0.1'}));assert.equal(result.code,0,result.stderr);const value=JSON.parse(result.stdout);assert.equal(value.ok,true);assert.equal(value.network.scope,'actual-rtnetlink-selected-ipv4-route');assert.equal(value.network.onLink,false);
});
