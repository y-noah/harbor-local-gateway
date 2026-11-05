import http from 'node:http';
import {EventEmitter} from 'node:events';
import test from 'node:test';
import assert from 'node:assert/strict';
import {createSseProbe,requestCapacityTarget,createRecordCounter} from '../tools/capacity/http-client.mjs';
import {createControlledUpstream} from '../tools/capacity/controlled-upstream.mjs';
import {runCapacitySelfcheck} from '../tools/capacity/selfcheck.mjs';

test('SSE probe ignores heartbeats and validates a complete UTF8/CRLF stream instead of response headers',()=>{
  let now=1;const probe=createSseProbe({clock:()=>now++});probe.write(Buffer.from(': heartbeat\r\n\r\n'));const bytes=Buffer.from('data: {"type":"response.output_text.delta","delta":"中文"}\r\n\r\ndata: {"type":"response.completed","response":{"status":"completed","id":"test"}}\r\n\r\n');for(const byte of bytes)probe.write(Buffer.from([byte]));const result=probe.finish();assert.equal(result.firstContentAt,1);assert.equal(result.completed,true);assert.equal(result.failed,false);assert.equal(result.events,2);
  const heartbeat=createSseProbe();heartbeat.write(Buffer.from(': heartbeat\n\n'));assert.equal(heartbeat.finish().firstContentAt,null);const onlyDone=createSseProbe();onlyDone.write(Buffer.from('data: [DONE]\n\n'));assert.equal(onlyDone.finish().completed,false);
  const invalid=createSseProbe({maxEventBytes:128});assert.throws(()=>invalid.write(Buffer.from('x'.repeat(129))),/event_budget/);const trailing=createSseProbe();trailing.write(Buffer.from('data: {}'));assert.throws(()=>trailing.finish(),/trailing_event/);
});

test('controlled upstream complete, 429, and HTTP200 disconnection remain separate outcomes',async()=>{
  const source=await createControlledUpstream({durationMs:30,firstContentMs:5,chunkIntervalMs:10});try{
    const call=()=>requestCapacityTarget({origin:source.origin,path:'/responses',method:'POST',headers:{'x-capacity-trace':'synthetic-trace','content-type':'application/json'},body:'{}',mode:'sse',timeoutMs:1000});const ok=await call();assert.equal(ok.success,true);assert.ok(ok.firstContentAt!==null);source.setFault('429');const quota=await call();assert.equal(quota.status,429);assert.equal(quota.success,false);assert.equal(quota.error,'http_429');source.setFault('disconnect');const cut=await call();assert.equal(cut.status,200);assert.equal(cut.success,false);assert.equal(cut.complete,false);
  }finally{await source.close();}
});

test('tiny real mutual-TLS selfcheck succeeds as a tool and explicitly remains incomplete for formal acceptance',async()=>{
  const report=await runCapacitySelfcheck();assert.equal(report.toolCheck.passed,true);assert.equal(report.metrics.arrivals.planned,15);assert.equal(report.metrics.arrivals.sent,15);assert.equal(report.metrics.arrivals.inflight,0);assert.notEqual(report.verdict.status,'passed');assert.equal(report.preflight.eligible,false);assert.ok(report.toolCheck.newConnections>0);assert.ok(report.toolCheck.reusedConnections>0);
});


test('export counters count CSV records with quoted newlines instead of equating raw lines with exported rows',()=>{
  const counter=createRecordCounter({format:'csv',header:true}),csv=Buffer.from('name,detail\r\na,"first\nsecond"\r\nb,"escaped ""quote"""\r\n');for(const value of csv)counter.write(Buffer.from([value]));assert.equal(counter.finish(),2);const malformed=createRecordCounter({format:'csv'});malformed.write(Buffer.from('"unfinished'));assert.throws(()=>malformed.finish(),/csv_incomplete/);
});


test('P2 repeated reuse and failed fresh connections leave no per-request connect listeners',async()=>{
  const original=http.request,socket=new EventEmitter();socket.connecting=false;socket.setMaxListeners(0);
  try{http.request=(_url,_options,callback)=>{const req=new EventEmitter();req.reusedSocket=true;req.end=()=>queueMicrotask(()=>{req.emit('socket',socket);req.emit('finish');const res=new EventEmitter();res.statusCode=200;callback(res);res.emit('data',Buffer.from('{}'));res.emit('end');});return req;};for(let index=0;index<100;index++)await requestCapacityTarget({origin:'http://127.0.0.1',path:'/'});assert.equal(socket.listenerCount('connect'),0);assert.equal(socket.listenerCount('secureConnect'),0);
    socket.connecting=true;http.request=()=>{const req=new EventEmitter();req.end=()=>queueMicrotask(()=>{req.emit('socket',socket);req.emit('error',new Error('synthetic-connect'));});return req;};await requestCapacityTarget({origin:'http://127.0.0.1',path:'/'});assert.equal(socket.listenerCount('connect'),0);assert.equal(socket.listenerCount('secureConnect'),0);
  }finally{http.request=original;}
});

test('private response header capture never becomes a result DTO and callback failure cannot reject a successful request',async()=>{
  const server=http.createServer((_request,response)=>{response.setHeader('set-cookie','harbor_session=synthetic-private-cookie; Max-Age=43200');response.end('{"ok":true}');});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));try{const origin='http://127.0.0.1:'+server.address().port;let cookie;const result=await requestCapacityTarget({origin,path:'/',onResponseHeaders:headers=>{cookie=headers['set-cookie'][0];headers['set-cookie'][0]='modified callback copy';}});assert.equal(result.success,true);assert.match(cookie,/synthetic-private-cookie/);assert.equal(JSON.stringify(result).includes('synthetic-private-cookie'),false);assert.equal((await requestCapacityTarget({origin,path:'/',onResponseHeaders:()=>{throw new Error('private callback failure');}})).success,true);assert.throws(()=>requestCapacityTarget({origin,path:'/',onResponseHeaders:'invalid'}),/http_configuration/);}finally{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
});

test('async private header callback rejection cannot change HTTP outcomes or resend requests',async()=>{
 let received=0;const server=http.createServer((request,response)=>{received++;response.writeHead(request.url==='/failure'?429:200,{'content-type':'application/json'});response.end('{}');});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));try{const origin='http://127.0.0.1:'+server.address().port,onResponseHeaders=async()=>{throw new Error('synthetic-private-header-callback');};const success=await requestCapacityTarget({origin,path:'/',onResponseHeaders}),failure=await requestCapacityTarget({origin,path:'/failure',onResponseHeaders});await new Promise(resolve=>setImmediate(resolve));assert.equal(success.success,true);assert.equal(failure.success,false);assert.equal(failure.status,429);assert.equal(received,2);assert.equal(JSON.stringify([success,failure]).includes('synthetic-private-header-callback'),false);}finally{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
});
