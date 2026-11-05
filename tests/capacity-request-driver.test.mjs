import test from 'node:test';
import assert from 'node:assert/strict';
import https from 'node:https';
import net from 'node:net';
import {mkdtemp,rm,readFile,lstat,writeFile} from 'node:fs/promises';
import {readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {performance} from 'node:perf_hooks';
import {createHash,X509Certificate} from 'node:crypto';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
import {createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
import {createCapacityRequestDriver,capacityRequestDriverAgent,isOwnedCapacityRequestDriver,capacityRequestDriverBrowserBinding,capacityDriverAgentBinding} from '../tools/capacity/request-driver.mjs';
import {requestCapacityTarget,createCapacityClientStageObserver} from '../tools/capacity/http-client.mjs';

const until=async predicate=>{const end=performance.now()+10000;while(!predicate()){assert.ok(performance.now()<end,'driver condition timed out');await new Promise(resolve=>setTimeout(resolve,5));}};
async function fixture(t,driverOptions={}){
 const directory=await mkdtemp(join(tmpdir(),'harbor-request-driver-')),certificates=await syntheticCertificates(directory),runtime=createOwnedRuntime(),sockets=new Set(),seen=[],held=[];let driver,proxyConnections=0,aborted=0;if(driverOptions.onDriverChild){const start=runtime.start;runtime.start=(...args)=>{const child=start(...args);driverOptions.onDriverChild(child);return child;};}
 const server=https.createServer({...certificates.server,ca:certificates.ca,requestCert:true,rejectUnauthorized:true},(req,res)=>{let body=[];req.on('data',part=>body.push(part));req.on('end',()=>{seen.push({path:req.url,body:Buffer.concat(body),authorized:req.client.authorized,trace:req.headers['x-capacity-trace']});if(req.url==='/held'){held.push(res);return;}if(req.url==='/model'||req.url==='/slow'){res.writeHead(200,{'content-type':'text/event-stream'});res.write('data: '+JSON.stringify({type:'response.output_text.delta',delta:'synthetic driver content'})+'\n\n');if(req.url==='/slow'){res.once('close',()=>{aborted++;});return;}setTimeout(()=>res.end('data: '+JSON.stringify({type:'response.completed',response:{id:'driver-response',status:'completed'}})+'\n\n'),20);return;}res.setHeader('set-cookie','synthetic_driver=ok');res.end(JSON.stringify({ok:true,body:Buffer.concat(body).toString('utf8')}));});});
 server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 const proxy=net.createServer(client=>{proxyConnections++;const upstream=net.connect(server.address().port,'127.0.0.1');for(const socket of [client,upstream]){sockets.add(socket);socket.on('error',()=>{client.destroy();upstream.destroy();});socket.once('close',()=>sockets.delete(socket));}client.pipe(upstream);upstream.pipe(client);});await new Promise(resolve=>proxy.listen(0,'127.0.0.1',resolve));const origin='https://127.0.0.1:'+proxy.address().port;
 t.after(async()=>{let failure;try{await driver?.close();}catch(error){failure=error;}for(const socket of sockets)socket.destroy();await Promise.all([new Promise(resolve=>proxy.close(resolve)),new Promise(resolve=>server.close(resolve))]);await runtime.close();await rm(directory,{recursive:true,force:true});if(failure&&!(driverOptions.allowIncompleteBrowserCleanup&&failure.message==='capacity_driver_browser_start_cleanup_unverified'))throw failure;});
 const browser=driverOptions.browser?{...driverOptions.browser,serverSpkiHash:createHash('sha256').update(new X509Certificate(certificates.server.cert).publicKey.export({type:'spki',format:'der'})).digest('base64')}:undefined;
 driver=await createCapacityRequestDriver({runtime,...driverOptions,browser,agentPlans:[{id:'admin',origin,tls:{caFile:join(directory,'ca.pem'),certFile:join(directory,'client.pem'),keyFile:join(directory,'client.key')}},{id:'model',origin,tls:{caFile:join(directory,'ca.pem'),certFile:join(directory,'client.pem'),keyFile:join(directory,'client.key')}}]});
 return {driver,runtime,origin,seen,held,admin:capacityRequestDriverAgent(driver,'admin'),model:capacityRequestDriverAgent(driver,'model'),proxyConnections:()=>proxyConnections,aborted:()=>aborted,serverCertificate:createHash('sha256').update(new X509Certificate(certificates.server.cert).raw).digest('hex')};
}

test('owned child executes original JSON and SSE clients through actual mTLS passthrough with native socket evidence',{timeout:30000},async t=>{
 const f=await fixture(t),events=[],observer=createCapacityClientStageObserver({context:({trace})=>({trace}),observe:value=>events.push(value)});t.after(()=>observer.close());let headers;
 const admin=await requestCapacityTarget({origin:f.origin,path:'/health',agent:f.admin,headers:{'x-capacity-trace':'driver-admin'},onResponseHeaders:value=>{headers=value;}}),model=await requestCapacityTarget({origin:f.origin,path:'/model',agent:f.model,method:'POST',body:{driverBytes:'ordinary user field'},headers:{'x-capacity-trace':'driver-model'},mode:'sse',clientStageObserver:observer});
 assert.equal(admin.success,true);assert.equal(headers['set-cookie'][0],'synthetic_driver=ok');assert.equal(model.success,true);assert.equal(model.complete,true);assert.notEqual(model.driverProcess,process.pid);assert.equal(model.driverTiming.controllerReceiptUsedAsFinish,false);assert.ok(model.finishedAt<=performance.now());assert.ok(model.finishedAt>=model.startedAt);assert.ok(model.durationMs>=15);assert.ok(events.some(row=>row.type==='content'));assert.ok(events.some(row=>row.type==='end'));assert.equal(JSON.stringify(events).includes('synthetic driver content'),false);
 assert.equal(model.socketEvidence.source,'native-http-socket');assert.equal(model.socketEvidence.tlsAuthorized,true);assert.equal(model.socketEvidence.peerCertificateHash,f.serverCertificate);assert.ok(f.proxyConnections()>=2);assert.ok(f.seen.every(row=>row.authorized));assert.deepEqual(JSON.parse(f.seen[1].body),{driverBytes:'ordinary user field'});assert.ok(f.driver.status().missing.includes('actual_lan_path_unproven'));assert.equal(f.driver.status().pending,0);
});

test('origin and private driver identities cannot be replaced by options or cloned public objects',{timeout:30000},async t=>{
 const f=await fixture(t);assert.equal(isOwnedCapacityRequestDriver({...f.driver}),false);assert.throws(()=>capacityRequestDriverAgent({...f.driver},'admin'),/not_owned/);await assert.rejects(async()=>requestCapacityTarget({origin:'https://127.0.0.1:1',path:'/health',agent:f.admin}),/origin_mismatch/);await assert.rejects(requestCapacityTarget({origin:f.origin,path:'//other-target/health',agent:f.admin}),/driver_request_failed/);assert.equal(f.seen.length,0);
 const bytes=Buffer.from('compressed or binary test bytes\0');const result=await requestCapacityTarget({origin:f.origin,path:'/health',method:'POST',body:bytes,agent:f.admin});assert.equal(result.success,true);assert.deepEqual(f.seen[0].body,bytes);
});

test('actual cancellation and an owned child exit retire requests without pretending a successful terminal',{timeout:30000},async t=>{
 const f=await fixture(t),abort=new AbortController(),pending=requestCapacityTarget({origin:f.origin,path:'/slow',agent:f.model,mode:'sse',signal:abort.signal});await until(()=>f.seen.length===1);abort.abort();const result=await pending;assert.equal(result.success,false);assert.equal(result.error,'client_cancelled');await until(()=>f.aborted()===1);assert.equal(f.driver.status().pending,0);
 const second=requestCapacityTarget({origin:f.origin,path:'/slow',agent:f.model,mode:'sse'});await until(()=>f.seen.length===2);process.kill(f.driver.status().pid,'SIGKILL');await assert.rejects(second,/driver_exited/);await until(()=>f.aborted()===2);assert.equal(f.driver.status().closed,true);assert.equal(f.driver.status().pending,0);await f.driver.close();
});

test('remote claims cannot turn a local launcher or an arbitrary runtime into formal evidence',async()=>{
 await assert.rejects(createCapacityRequestDriver({remote:{lan:true,proxyPreserved:true}}),/remote_configuration/);await assert.rejects(createCapacityRequestDriver({runtime:{start:()=>{}},agentPlans:[]}),/driver_configuration/);
});

test('frame and inflight budgets reject before HTTP while agent destruction keeps other actors alive',{timeout:30000},async t=>{
 const f=await fixture(t,{maxInFlight:1,maxFrameBytes:4096,maxBufferedBytes:8192});await assert.rejects(requestCapacityTarget({origin:f.origin,path:'/health',agent:f.admin,body:'x'.repeat(5000)}),/frame_budget/);assert.equal(f.seen.length,0);
 const first=requestCapacityTarget({origin:f.origin,path:'/slow',agent:f.model,mode:'sse'});await until(()=>f.seen.length===1);await assert.rejects(requestCapacityTarget({origin:f.origin,path:'/health',agent:f.admin}),/inflight_budget/);f.model.destroy();assert.equal((await first).success,false);await until(()=>f.aborted()===1);assert.equal((await requestCapacityTarget({origin:f.origin,path:'/health',agent:f.admin})).success,true);assert.equal((await requestCapacityTarget({origin:f.origin,path:'/health',agent:f.model})).success,true,'native Agent.destroy permits later fresh connections');assert.equal(f.driver.status().pending,0);
});

test('unexpected driver exit also releases its actual Chromium process and private temporary profiles',{
 skip:!process.env.HARBOR_CAPACITY_BROWSER_MODULE||!process.env.HARBOR_CAPACITY_BROWSER_BINARY?'需真实Playwright/Chromium':false,timeout:30000
},async t=>{
 const f=await fixture(t,{browser:{module:process.env.HARBOR_CAPACITY_BROWSER_MODULE,binary:process.env.HARBOR_CAPACITY_BROWSER_BINARY}}),browser=await f.driver.launchBrowser(),binding=capacityRequestDriverBrowserBinding(f.driver.launchBrowser,browser);assert.ok(binding);assert.equal(capacityRequestDriverBrowserBinding(async()=>browser,browser),null);const context=await browser.newContext(),page=await context.newPage();await page.setContent('<p>owned driver</p>');const argumentsList=(await readFile('/proc/'+binding.browserPid+'/cmdline','utf8')).split('\0'),profile=/(?:^|\s)--user-data-dir=([^\s\0]+)/.exec(argumentsList.join(' '))?.[1];assert.ok(profile?.includes('/harbor-capacity-driver-'),'Chromium must use this driver private temporary root');assert.equal((await lstat(profile)).isDirectory(),true);process.kill(f.driver.status().pid,'SIGKILL');await until(()=>f.driver.status().closed);await f.driver.close();assert.equal(f.driver.status().resourcesReleased,true);await assert.rejects(lstat(profile),{code:'ENOENT'});let alive=false;try{const raw=await readFile('/proc/'+binding.browserPid+'/stat','utf8');alive=raw.slice(raw.lastIndexOf(')')+2).split(' ')[0]!=='Z';}catch{}assert.equal(alive,false);assert.ok(f.driver.status().failures>0);
});

for(const action of ['close','cancel','destroy'])test('saturated IPC plus a paused child has bounded '+action+' delivery and real termination',{timeout:30000},async t=>{
 const f=await fixture(t,{maxInFlight:256,maxFrameBytes:4096,maxBufferedBytes:8192,controlTimeoutMs:100,closeTimeoutMs:200}),abort=new AbortController(),requests=[];process.kill(f.driver.status().pid,'SIGSTOP');
 for(let index=0;index<100;index++){requests.push(requestCapacityTarget({origin:f.origin,path:'/slow',method:'POST',body:'x'.repeat(3000),agent:f.model,mode:'sse',...(index===0?{signal:abort.signal}:{})}).then(value=>({value}),error=>({error:error.message})));await new Promise(resolve=>setImmediate(resolve));}
 await until(()=>f.driver.status().bufferedBytes>0);const began=performance.now();if(action==='close')await f.driver.close();else{if(action==='cancel')abort.abort();else f.model.destroy();await until(()=>f.driver.status().closed);await f.driver.close();}
 assert.ok(performance.now()-began<5000,'stalled control delivery must reach actual termination');const results=await Promise.all(requests);assert.ok(results.every(row=>row.error||row.value?.success!==true));assert.equal(f.driver.status().pending,0);assert.equal(f.driver.status().bufferedBytes,0);assert.equal(f.driver.status().resourcesReleased,true);assert.ok(f.driver.status().controlFailures>0,'delivery failure must be recorded rather than swallowed');
});


for(const interruption of ['SIGKILL','command-timeout'])test('a real Chromium started before ready keeps honest cleanup after '+interruption,{
 skip:!process.env.HARBOR_CAPACITY_BROWSER_MODULE||!process.env.HARBOR_CAPACITY_BROWSER_BINARY?'需真实Playwright/Chromium':false,timeout:30000
},async t=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-driver-start-fault-')),marker=join(directory,'started.json'),module=join(directory,'browser.mjs');let proof,recoveryDirectory;
 await writeFile(module,`import {chromium as real} from ${JSON.stringify(process.env.HARBOR_CAPACITY_BROWSER_MODULE)};import {writeFile} from 'node:fs/promises';export const chromium={async launchServer(options){const server=await real.launchServer(options);await writeFile(${JSON.stringify(marker)},JSON.stringify({pid:server.process().pid}));await new Promise(()=>{});}};`);
 const f=await fixture(t,{allowIncompleteBrowserCleanup:true,browserLaunchTimeoutMs:interruption==='command-timeout'?3000:30000,controlTimeoutMs:100,browser:{module,binary:process.env.HARBOR_CAPACITY_BROWSER_BINARY}});
 try{
  const launch=f.driver.launchBrowser();const outcome=launch.then(()=>({ok:true}),error=>({error}));const deadline=performance.now()+10000;let started;
  while(!started){try{started=JSON.parse(await readFile(marker,'utf8'));}catch(error){if(error.code!=='ENOENT')throw error;}assert.ok(performance.now()<deadline);if(!started)await new Promise(resolve=>setTimeout(resolve,10));}
  const stat=await readFile('/proc/'+started.pid+'/stat','utf8'),parts=stat.slice(stat.lastIndexOf(')')+2).split(' ');assert.equal(Number(parts[1]),f.driver.status().pid);assert.equal(Number(parts[2]),started.pid);proof={pid:started.pid,started:parts[19]};
  if(interruption==='SIGKILL')process.kill(f.driver.status().pid,'SIGKILL');assert.match((await outcome).error.message,interruption==='SIGKILL'?/driver_exited/:/command_timeout/);await until(()=>f.driver.status().closed);if(interruption==='command-timeout')assert.equal(f.driver.status().commandFailures,1);
  await assert.rejects(f.driver.close(),error=>{assert.equal(error.message,'capacity_driver_browser_start_cleanup_unverified');assert.equal(typeof error.cleanup,'function');recoveryDirectory=error.recoveryDirectory;return true;});assert.equal(f.driver.status().resourcesReleased,false);assert.equal(f.driver.status().pendingBrowserStarts,1);assert.equal((await lstat(recoveryDirectory)).isDirectory(),true);
 }finally{
  if(proof){try{const raw=await readFile('/proc/'+proof.pid+'/stat','utf8'),parts=raw.slice(raw.lastIndexOf(')')+2).split(' ');if(parts[19]===proof.started&&Number(parts[2])===proof.pid)process.kill(-proof.pid,'SIGKILL');}catch(error){if(!['ESRCH','ENOENT'].includes(error.code))throw error;}}
  if(recoveryDirectory)await rm(recoveryDirectory,{recursive:true,force:true});await rm(directory,{recursive:true,force:true});
 }
});


test('browser ready keeps its launch pending until native process identity is actually registered',{
 skip:!process.env.HARBOR_CAPACITY_BROWSER_MODULE||!process.env.HARBOR_CAPACITY_BROWSER_BINARY?'需真实Playwright/Chromium':false,timeout:30000
},async t=>{
 let proof,recoveryDirectory,killed=false;const f=await fixture(t,{allowIncompleteBrowserCleanup:true,browser:{module:process.env.HARBOR_CAPACITY_BROWSER_MODULE,binary:process.env.HARBOR_CAPACITY_BROWSER_BINARY},onDriverChild:child=>child.prependListener('message',message=>{
  if(message?.kind!=='browser-ready'||!message.ok)return;const raw=readFileSync('/proc/'+message.browserPid+'/stat','utf8'),parts=raw.slice(raw.lastIndexOf(')')+2).split(' ');assert.equal(Number(parts[1]),child.pid);assert.equal(Number(parts[2]),message.browserPid);proof={pid:message.browserPid,started:parts[19]};process.kill(child.pid,'SIGKILL');killed=true;
  const deadline=performance.now()+2000;for(;;){const raw=readFileSync('/proc/'+proof.pid+'/stat','utf8'),fields=raw.slice(raw.lastIndexOf(')')+2).split(' ');if(Number(fields[1])!==child.pid)break;assert.ok(performance.now()<deadline,'actual Chrome must be reparented before driver identity registration');}
 })});
 try{await assert.rejects(f.driver.launchBrowser(),/browser_not_owned/);assert.equal(killed,true);await until(()=>f.driver.status().closed);await assert.rejects(f.driver.close(),error=>{assert.equal(error.message,'capacity_driver_browser_start_cleanup_unverified');recoveryDirectory=error.recoveryDirectory;return true;});assert.equal(f.driver.status().resourcesReleased,false);assert.equal(f.driver.status().pendingBrowserStarts,1);assert.equal((await lstat(recoveryDirectory)).isDirectory(),true);
 }finally{if(proof){try{const raw=await readFile('/proc/'+proof.pid+'/stat','utf8'),parts=raw.slice(raw.lastIndexOf(')')+2).split(' ');if(parts[19]===proof.started&&Number(parts[2])===proof.pid)process.kill(-proof.pid,'SIGKILL');}catch(error){if(!['ESRCH','ENOENT'].includes(error.code))throw error;}}if(recoveryDirectory)await rm(recoveryDirectory,{recursive:true,force:true});}
});

test('a browser launch definitely refused before IPC delivery does not leave an unresolvable cleanup claim',{
 skip:!process.env.HARBOR_CAPACITY_BROWSER_MODULE||!process.env.HARBOR_CAPACITY_BROWSER_BINARY?'需真实Playwright/Chromium':false,timeout:30000
},async t=>{
 let lastRequest;const f=await fixture(t,{maxInFlight:256,maxFrameBytes:4096,maxBufferedBytes:8192,browser:{module:process.env.HARBOR_CAPACITY_BROWSER_MODULE,binary:process.env.HARBOR_CAPACITY_BROWSER_BINARY},onDriverChild:child=>{const send=child.send.bind(child);child.send=(message,...args)=>{if(message.kind==='request')lastRequest=structuredClone(message);return send(message,...args);};}}),requests=[],binding=capacityDriverAgentBinding(f.admin);process.kill(f.driver.status().pid,'SIGSTOP');
 try{for(let index=0;index<100;index++){requests.push(binding.request({origin:f.origin,path:'/health',method:'POST',body:'x'.repeat(3000)}).then(value=>({value}),error=>({error})));await new Promise(setImmediate);}assert.ok(f.driver.status().bufferedBytes>0);
  const remaining=8192-2048-f.driver.status().bufferedBytes,base=Buffer.byteLength(JSON.stringify({...lastRequest,id:101,payload:{path:'/health',method:'POST',body:''}}));assert.ok(remaining>=base);requests.push(binding.request({origin:f.origin,path:'/health',method:'POST',body:'x'.repeat(remaining-base)}).then(value=>({value}),error=>({error})));await new Promise(setImmediate);assert.equal(f.driver.status().bufferedBytes,6144);
  await assert.rejects(f.driver.launchBrowser(),/transport_budget/);assert.equal(f.driver.status().pendingBrowserStarts,0);process.kill(f.driver.status().pid,'SIGCONT');await Promise.all(requests);await f.driver.close();assert.equal(f.driver.status().resourcesReleased,true);assert.equal(f.driver.status().controlFailures,0);
 }finally{try{process.kill(f.driver.status().pid,'SIGCONT');}catch(error){if(error.code!=='ESRCH')throw error;}await Promise.all(requests);}
});

for(const mode of ['json','sse','download'])test('original '+mode+' deadline retires a stopped executing driver without waiting for target close', {timeout:30000},async t=>{
 const f=await fixture(t,{controlTimeoutMs:100,closeTimeoutMs:200}),started=performance.now(),outcome=requestCapacityTarget({origin:f.origin,path:'/slow',agent:f.model,mode,timeoutMs:250}).then(value=>({value}),error=>({error}));await until(()=>f.seen.length===1);process.kill(f.driver.status().pid,'SIGSTOP');await until(()=>f.driver.status().closed);const result=await outcome;assert.ok(result.error||result.value.success===false);assert.notEqual(result.value?.httpEnded,true);assert.equal(f.driver.status().deadlineFailures,1);assert.ok(performance.now()-started<3000,'source liveness follows the original request deadline, not a new close timeout');await f.driver.close();assert.equal(f.driver.status().resourcesReleased,true);assert.equal(f.driver.status().pending,0);
});

test('consecutive native synchronous rejections never accumulate completed child active records', {timeout:30000},async t=>{
 const f=await fixture(t);for(let index=0;index<50;index++)await assert.rejects(requestCapacityTarget({origin:f.origin,path:index%2?'/health':'//forbidden',mode:index%2?'invalid':'json',agent:f.admin}),/driver_request_failed/);assert.equal(f.driver.status().pending,0);assert.equal(f.driver.status().childActiveAfterLastResult,0);assert.equal(f.seen.length,0);assert.equal((await requestCapacityTarget({origin:f.origin,path:'/health',agent:f.admin})).success,true);assert.equal(f.driver.status().childActiveAfterLastResult,0);
});

for(const scenario of ['unsent','paused-handshake','paused-backpressure'])test('initialization '+scenario+' observes both send and ready failures and releases the real child', {timeout:30000},async()=>{
 const runtime=createOwnedRuntime(),start=runtime.start,unhandled=[];let child,sendFinished=false;const observed=error=>unhandled.push(error.message);process.on('unhandledRejection',observed);runtime.start=(...args)=>{child=start(...args);if(scenario!=='unsent')process.kill(child.pid,'SIGSTOP');const send=child.send.bind(child);child.send=(message,callback)=>send(message,error=>{if(message.kind==='initialize')sendFinished=true;callback?.(error);});return child;};
 try{const big=scenario==='paused-backpressure',agentPlans=Array.from({length:scenario==='paused-handshake'?1:32},(_,index)=>({id:'agent-'+index,origin:'http://127.0.0.1:1',tls:big?{caFile:'/tmp/synthetic-'+ 'x'.repeat(10000)}:{}}));await assert.rejects(createCapacityRequestDriver({runtime,agentPlans,maxFrameBytes:big?1048576:1024,maxBufferedBytes:big?2097152:4096,readyTimeoutMs:100,controlTimeoutMs:100,closeTimeoutMs:200}),scenario==='unsent'?/transport_budget/:/ready_timeout/);assert.ok(child.exitCode!==null||child.signalCode!==null);await new Promise(setImmediate);assert.deepEqual(unhandled,[]);assert.equal(sendFinished,scenario!=='unsent');
 }finally{process.off('unhandledRejection',observed);await runtime.close();}
});

test('agent creation reply cannot wait forever while a real child is stopped', {timeout:30000},async t=>{
 let plan;const f=await fixture(t,{commandTimeoutMs:100,controlTimeoutMs:100,closeTimeoutMs:200,onDriverChild:child=>{const send=child.send.bind(child);child.send=(message,...args)=>{if(message.kind==='initialize')plan=structuredClone(message.agentPlans[0]);return send(message,...args);};}});process.kill(f.driver.status().pid,'SIGSTOP');const began=performance.now();await assert.rejects(f.driver.addAgent({...plan,id:'late-agent'}),/command_timeout/);await until(()=>f.driver.status().closed);await f.driver.close();assert.ok(performance.now()-began<3000);assert.equal(f.driver.status().commandFailures,1);assert.equal(f.driver.status().resourcesReleased,true);
});

test('a ready browser with a stopped driver cannot leave its real websocket connection pending forever',{
 skip:!process.env.HARBOR_CAPACITY_BROWSER_MODULE||!process.env.HARBOR_CAPACITY_BROWSER_BINARY?'需真实Playwright/Chromium':false,timeout:30000
},async t=>{
 let paused=false;const f=await fixture(t,{browserLaunchTimeoutMs:3000,controlTimeoutMs:100,browser:{module:process.env.HARBOR_CAPACITY_BROWSER_MODULE,binary:process.env.HARBOR_CAPACITY_BROWSER_BINARY},onDriverChild:child=>child.prependListener('message',message=>{if(message?.kind==='browser-ready'&&message.ok){process.kill(child.pid,'SIGSTOP');paused=true;}})});const began=performance.now();await assert.rejects(f.driver.launchBrowser(),/Timeout|timeout/);assert.equal(paused,true);await until(()=>f.driver.status().closed);await f.driver.close();assert.equal(f.driver.status().commandFailures,1);assert.equal(f.driver.status().resourcesReleased,true);assert.ok(performance.now()-began<7000);
});

test('invalid native timeout values are rejected before IPC even while the owned child is stopped', {timeout:30000},async t=>{
 let sent=0;const f=await fixture(t,{onDriverChild:child=>{const send=child.send.bind(child);child.send=(message,...args)=>{if(message.kind==='request')sent++;return send(message,...args);};}});process.kill(f.driver.status().pid,'SIGSTOP');try{for(const timeoutMs of [0,-1,1.5,NaN,Infinity,4294967296,'100',null])await assert.rejects(requestCapacityTarget({origin:f.origin,path:'/health',agent:f.admin,timeoutMs}),/capacity_http_configuration/);assert.equal(sent,0);assert.equal(f.driver.status().pending,0);assert.equal(f.seen.length,0);}finally{process.kill(f.driver.status().pid,'SIGCONT');}assert.equal((await requestCapacityTarget({origin:f.origin,path:'/health',agent:f.admin})).success,true);
});

test('actual IPC disconnection stops its owned executing source instead of leaving an unguarded model', {timeout:30000},async t=>{
 let child;const f=await fixture(t,{onDriverChild:value=>{child=value;}}),outcome=requestCapacityTarget({origin:f.origin,path:'/slow',agent:f.model,mode:'sse',timeoutMs:10000}).then(value=>({value}),error=>({error}));await until(()=>f.seen.length===1);child.disconnect();const result=await outcome;assert.ok(result.error||result.value.success===false);await until(()=>f.driver.status().closed);await f.driver.close();assert.ok(f.driver.status().failures>0);assert.equal(f.driver.status().resourcesReleased,true);assert.equal(f.driver.status().pending,0);assert.equal(f.seen.length,1);
});

test('real browser close uses reserved control budget and waits for actual Chromium exit',{
 skip:!process.env.HARBOR_CAPACITY_BROWSER_MODULE||!process.env.HARBOR_CAPACITY_BROWSER_BINARY?'需真实Playwright/Chromium':false,timeout:30000
},async t=>{
 let lastRequest;const f=await fixture(t,{maxInFlight:256,maxFrameBytes:4096,maxBufferedBytes:8192,controlTimeoutMs:5000,browser:{module:process.env.HARBOR_CAPACITY_BROWSER_MODULE,binary:process.env.HARBOR_CAPACITY_BROWSER_BINARY},onDriverChild:child=>{const send=child.send.bind(child);child.send=(message,...args)=>{if(message.kind==='request')lastRequest=structuredClone(message);return send(message,...args);};}}),browser=await f.driver.launchBrowser(),proof=capacityRequestDriverBrowserBinding(f.driver.launchBrowser,browser),requests=[],binding=capacityDriverAgentBinding(f.admin);process.kill(f.driver.status().pid,'SIGSTOP');try{
  for(let index=0;index<100;index++){requests.push(binding.request({origin:f.origin,path:'/held',method:'POST',body:'x'.repeat(3000)}).then(value=>({value}),error=>({error})));await new Promise(setImmediate);}const remaining=6144-f.driver.status().bufferedBytes,base=Buffer.byteLength(JSON.stringify({...lastRequest,id:102,payload:{path:'/held',method:'POST',body:''}}));assert.ok(remaining>=base);requests.push(binding.request({origin:f.origin,path:'/held',method:'POST',body:'x'.repeat(remaining-base)}).then(value=>({value}),error=>({error})));await new Promise(setImmediate);assert.equal(f.driver.status().bufferedBytes,6144);const closing=browser.close();await until(()=>f.driver.status().bufferedBytes>6144);process.kill(f.driver.status().pid,'SIGCONT');await closing;const drainDeadline=performance.now()+10000;while(f.driver.status().pending){assert.ok(performance.now()<drainDeadline,'held response drain must finish');if(!f.held.length){await new Promise(resolve=>setTimeout(resolve,5));continue;}const before=f.driver.status().pending;f.held.shift().end('{"ok":true}');await until(()=>f.driver.status().pending<before);}await Promise.all(requests);let live=false;try{const raw=await readFile('/proc/'+proof.browserPid+'/stat','utf8');live=raw.slice(raw.lastIndexOf(')')+2).split(' ')[0]!=='Z';}catch(error){if(error.code!=='ENOENT')throw error;}assert.equal(live,false,'close ACK follows the actual browser process exit');assert.equal(f.driver.status().controlFailures,0);assert.equal(f.driver.status().failures,0);
 }finally{try{process.kill(f.driver.status().pid,'SIGCONT');}catch(error){if(error.code!=='ESRCH')throw error;}await f.driver.close();await Promise.all(requests);}
});

test('paused-driver browser close reports failed confirmation and terminates the actual owned Chrome',{
 skip:!process.env.HARBOR_CAPACITY_BROWSER_MODULE||!process.env.HARBOR_CAPACITY_BROWSER_BINARY?'需真实Playwright/Chromium':false,timeout:30000
},async t=>{
 const f=await fixture(t,{controlTimeoutMs:100,closeTimeoutMs:200,browser:{module:process.env.HARBOR_CAPACITY_BROWSER_MODULE,binary:process.env.HARBOR_CAPACITY_BROWSER_BINARY}}),browser=await f.driver.launchBrowser(),proof=capacityRequestDriverBrowserBinding(f.driver.launchBrowser,browser);process.kill(f.driver.status().pid,'SIGSTOP');await assert.rejects(browser.close(),/control_timeout/);await until(()=>f.driver.status().closed);await f.driver.close();assert.ok(f.driver.status().controlFailures>0);assert.equal(f.driver.status().resourcesReleased,true);let live=false;try{const raw=await readFile('/proc/'+proof.browserPid+'/stat','utf8');live=raw.slice(raw.lastIndexOf(')')+2).split(' ')[0]!=='Z';}catch(error){if(error.code!=='ENOENT')throw error;}assert.equal(live,false);
});
