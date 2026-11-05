import test from 'node:test';
import assert from 'node:assert/strict';
import https from 'node:https';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
import {createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
import {createCapacityRequestDriver,capacityRequestDriverAgent} from '../tools/capacity/request-driver.mjs';
import {capacityAgent,requestCapacityTarget,sanitizeCapacityNetworkFailure} from '../tools/capacity/http-client.mjs';

async function fixture(mode,operation){
 const directory=await mkdtemp(join(tmpdir(),'harbor-network-failure-')),certificates=await syntheticCertificates(directory),runtime=createOwnedRuntime(),sockets=new Set(),requestSockets=new Set();let driver,agent,requests=0,connections=0;
 const server=https.createServer({...certificates.server,ca:certificates.ca,requestCert:true,rejectUnauthorized:true},(req,res)=>{requests++;requestSockets.add(req.socket);req.once('error',()=>{});if(req.url==='/drop'){req.socket.destroy();return;}req.resume();req.once('end',()=>res.end('{"ok":true}'));});
 server.on('connection',socket=>{connections++;sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});server.on('tlsClientError',()=>{});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='https://127.0.0.1:'+server.address().port;
 try{
  if(mode==='local'){driver=await createCapacityRequestDriver({runtime,agentPlans:[{id:'model',origin,tls:{caFile:join(directory,'ca.pem'),certFile:join(directory,'client.pem'),keyFile:join(directory,'client.key')}}]});agent=capacityRequestDriverAgent(driver,'model');}
  else agent=capacityAgent(origin,{ca:certificates.ca,...certificates.client});
  await operation({server,origin,agent,driver,runtime,directory,requests:()=>requests,connections:()=>connections,requestSockets:()=>requestSockets.size});
 }finally{agent?.destroy();await driver?.close();for(const socket of sockets)socket.destroy();await new Promise(resolve=>server.close(resolve));await runtime.close();await rm(directory,{recursive:true,force:true});}
}

async function nativeRefusalInOwnedChild(f){
 const code=`import {readFile} from 'node:fs/promises';import {join} from 'node:path';import {capacityAgent,requestCapacityTarget} from ${JSON.stringify(new URL('../tools/capacity/http-client.mjs',import.meta.url).href)};
 const {origin,directory}=JSON.parse(process.argv[1]),agent=capacityAgent(origin,{ca:await readFile(join(directory,'ca.pem')),cert:await readFile(join(directory,'client.pem')),key:await readFile(join(directory,'client.key'))});let attempts=0;const create=agent.createConnection.bind(agent);agent.createConnection=(...args)=>{attempts++;return create(...args);};
 try{const result=await requestCapacityTarget({origin,path:'/health',agent,timeoutMs:2000});await new Promise((resolve,reject)=>process.send({result:Object.fromEntries(['success','error','httpEnded','wireSent','networkFailure'].map(key=>[key,result[key]])),notSentPresent:Object.hasOwn(result,'notSent'),attempts},error=>error?reject(error):resolve()));}finally{agent.destroy();process.disconnect();}`;
 const child=f.runtime.start(process.execPath,['--input-type=module','-e',code,JSON.stringify({origin:f.origin,directory:f.directory})],{role:'helper',stdio:['ignore','ignore','ignore','ipc']});let timer;
 try{return await Promise.race([Promise.all([new Promise((resolve,reject)=>{child.once('message',resolve);child.once('error',reject);}),new Promise((resolve,reject)=>child.once('exit',code=>code===0?resolve():reject(Error('owned_native_probe_failed'))))]).then(([result])=>result),new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('owned_native_probe_timeout')),10000);})]);}finally{clearTimeout(timer);}
}

for(const mode of ['native','local']){
 test((mode==='native'?'native helper in an owned subprocess':'local driver')+' fresh HTTPS connection refusal retains fixed native evidence and does not retry', {timeout:30000},()=>fixture(mode,async f=>{
  await new Promise(resolve=>f.server.close(resolve));const owned=mode==='native'?await nativeRefusalInOwnedChild(f):null,result=owned?.result??await requestCapacityTarget({origin:f.origin,path:'/health',agent:f.agent,timeoutMs:2000});assert.equal(result.success,false);assert.equal(result.error,'connection_failed');assert.equal(result.httpEnded,false);assert.equal(result.wireSent,false);assert.deepEqual(result.networkFailure,{code:'ECONNREFUSED',reusedSocket:false,socketAssigned:true,tcpConnectObserved:false,tlsSecureConnectObserved:false});assert.equal(f.requests(),0);assert.equal(f.connections(),0);assert.equal(Object.hasOwn(result,'notSent'),false);if(mode==='native'){assert.equal(owned.attempts,1);assert.equal(owned.notSentPresent,false);}else{assert.equal(f.driver.status().executedRequestsByAgent.model,1);assert.equal(f.driver.status().pending,0);}
 }));

 test(mode+' fresh HTTPS failure after handshake records both actual connection events', {timeout:30000},()=>fixture(mode,async f=>{
  const result=await requestCapacityTarget({origin:f.origin,path:'/drop',agent:f.agent,timeoutMs:2000});assert.equal(result.success,false);assert.equal(result.error,'connection_failed');assert.equal(result.httpEnded,false);assert.deepEqual(result.networkFailure,{code:'ECONNRESET',reusedSocket:false,socketAssigned:true,tcpConnectObserved:true,tlsSecureConnectObserved:true});assert.equal(f.requests(),1);assert.equal(f.connections(),1);assert.equal(Object.hasOwn(result,'notSent'),false);
 }));

 // A deterministic reset after the reused request reaches our own server,
 // not a claim to reproduce the unobserved C6 idle-FIN/before-write race.
 test(mode+' reused HTTPS request closed before response headers retains actual reuse and never resends', {timeout:30000},()=>fixture(mode,async f=>{
  const first=await requestCapacityTarget({origin:f.origin,path:'/health',agent:f.agent,timeoutMs:2000});assert.equal(first.success,true);assert.equal(Object.hasOwn(first,'networkFailure'),false);const second=await requestCapacityTarget({origin:f.origin,path:'/drop',agent:f.agent,timeoutMs:2000});assert.equal(second.success,false);assert.equal(second.error,'connection_failed');assert.equal(second.httpEnded,false);assert.equal(second.wireSent,true);assert.deepEqual(second.networkFailure,{code:'ECONNRESET',reusedSocket:true,socketAssigned:true,tcpConnectObserved:false,tlsSecureConnectObserved:false});assert.equal(f.requests(),2);assert.equal(f.connections(),1);assert.equal(f.requestSockets(),1);assert.equal(Object.hasOwn(second,'notSent'),false);if(mode==='local'){assert.equal(f.driver.status().executedRequestsByAgent.model,2);assert.equal(f.driver.status().failures,0);}
 }));
}

test('network diagnostic sanitization has fixed labels and never copies arbitrary error or endpoint data',()=>{
 const raw={code:'private-unbounded-code',reusedSocket:true,socketAssigned:'private-value',tcpConnectObserved:false,tlsSecureConnectObserved:null,message:'private-message',address:'private-address',body:'private-body'},safe=sanitizeCapacityNetworkFailure(raw);assert.deepEqual(safe,{code:'OTHER',reusedSocket:true,socketAssigned:null,tcpConnectObserved:false,tlsSecureConnectObserved:null});assert.equal(JSON.stringify(safe).includes('private-'),false);assert.equal(Object.isFrozen(safe),true);assert.deepEqual(sanitizeCapacityNetworkFailure(null),{code:'NOT_REPORTED',reusedSocket:null,socketAssigned:null,tcpConnectObserved:null,tlsSecureConnectObserved:null});const hostile={get code(){throw Error('private-getter');}};assert.equal(sanitizeCapacityNetworkFailure(hostile).code,'UNKNOWN');
});
