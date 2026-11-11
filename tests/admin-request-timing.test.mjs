import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createAdminRequestTiming} from '../admin-request-timing.mjs';
import {createGatewayEntry} from '../gateway-entry.mjs';

async function listen(server,address){await new Promise((resolve,reject)=>{server.once('error',reject);address?server.listen(address,resolve):server.listen(0,'127.0.0.1',resolve);});}
function request(server,{path='/api/admin/test',method='GET',headers={}}={}){return new Promise((resolve,reject)=>{const req=http.request({host:'127.0.0.1',port:server.address().port,path,method,headers,agent:false},res=>{const chunks=[];res.on('data',chunk=>chunks.push(chunk));res.once('end',()=>resolve({status:res.statusCode,headers:res.headers,body:Buffer.concat(chunks).toString()}));res.once('error',reject);});req.once('error',reject);req.end();});}
async function fixture(t,handler,{entry=false}={}){
 const directory=await mkdtemp(join(tmpdir(),'harbor-admin-timing-')),sockets=new Set(),backend=http.createServer(handler);
 const track=server=>server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});track(backend);
 await listen(backend,entry?join(directory,'backend.sock'):undefined);let proxy;
 if(entry){proxy=createGatewayEntry({route:{socketPath:join(directory,'backend.sock')},limits:{maxInflightRequests:2,maxQueuedRequests:2,maxQueuedBytes:32768,maxRequestBytes:4096,maxWaitMs:3000}});track(proxy.server);await listen(proxy.server);}
 t.after(async()=>{const closing=proxy?.close();for(const socket of sockets)socket.destroy();await closing;await new Promise(resolve=>backend.close(resolve));await rm(directory,{recursive:true,force:true});});return proxy?.server??backend;
}

test('nested begin preserves outer start and includes JSON serialization, appending only once',async t=>{
 let now=10;const timing=createAdminRequestTiming({clock:()=>now});
 const server=await fixture(t,(req,res)=>{
  assert.equal(timing.begin(req,res),true);now=20;assert.equal(timing.begin(req,res),false);
  res.setHeader('Server-Timing','cache;dur=1.00');res.setHeader('Cache-Control','no-store');
  const encoded=JSON.stringify({get value(){now=42.345;return 'synthetic';}});
  assert.equal(timing.finish(res,200,{authenticated:true}),true);now=50;assert.equal(timing.finish(res,200,{authenticated:true}),false);
  res.writeHead(200,{'Content-Type':'application/json; charset=utf-8','Content-Length':Buffer.byteLength(encoded)});res.end(encoded);
 });
 const reply=await request(server,{headers:{'X-Server-Timing':'app;dur=999','X-Harbor-Identity':'synthetic'}});
 assert.equal(reply.headers['server-timing'],'cache;dur=1.00, app;dur=32.34');assert.equal(reply.body,'{"value":"synthetic"}');assert.equal(Number(reply.headers['content-length']),Buffer.byteLength(reply.body));assert.equal(reply.headers['cache-control'],'no-store');
});

test('only explicitly authenticated administrator GET successful JSON gets app timing',async t=>{
 const timing=createAdminRequestTiming();const server=await fixture(t,(req,res)=>{
  timing.begin(req,res);const status=req.url.includes('error')?500:req.url.includes('unauthorized')?401:200;
  const encoded=JSON.stringify({ok:status===200});timing.finish(res,status,{authenticated:!req.url.includes('unauthorized')});res.writeHead(status,{'Content-Type':'application/json'});res.end(encoded);
 });
 for(const options of [{path:'/health'},{path:'/api/auth/session'},{path:'/app.js'},{path:'/v1/responses',method:'POST'},{method:'POST'},{path:'/api/admin/error'},{path:'/api/admin/unauthorized'}])assert.equal((await request(server,options)).headers['server-timing'],undefined);
 assert.match((await request(server)).headers['server-timing'],/^app;dur=\d+\.\d{2}$/);
});

test('closed or already-started responses and invalid clocks are no-ops',()=>{
 let now=1;const timing=createAdminRequestTiming({clock:()=>now}),headers=new Map();
 const res={getHeader:key=>headers.get(key),setHeader:(key,value)=>headers.set(key,value)};
 assert.equal(timing.begin({method:'GET',url:'/api/admin/test'},res),true);
 assert.equal(timing.finish(res,200),false);now=NaN;assert.equal(timing.finish(res,200,{authenticated:true}),false);now=0;assert.equal(timing.finish(res,200,{authenticated:true}),false);
 now=2;res.headersSent=true;assert.equal(timing.finish(res,200,{authenticated:true}),false);res.headersSent=false;res.destroyed=true;assert.equal(timing.finish(res,200,{authenticated:true}),false);res.destroyed=false;res.writableEnded=true;assert.equal(timing.finish(res,200,{authenticated:true}),false);assert.equal(headers.size,0);
});

test('an existing app metric is not duplicated or replaced',()=>{
 const timing=createAdminRequestTiming({clock:()=>1}),headers=new Map([['Server-Timing',['cache;dur=2','app;dur=3']]]),res={getHeader:key=>headers.get(key),setHeader:(key,value)=>headers.set(key,value)};
 timing.begin({method:'GET',url:'/api/admin/test'},res);assert.equal(timing.finish(res,200,{authenticated:true}),false);assert.deepEqual(headers.get('Server-Timing'),['cache;dur=2','app;dur=3']);
});

test('optional timing does not add requirements to lightweight response adapters',()=>{
 const timing=createAdminRequestTiming(),res={writeHead(){},end(){}};
 assert.equal(timing.begin({method:'GET',url:'/api/admin/test'},res),false);
 assert.equal(timing.finish(res,200,{authenticated:true}),false);
});

test('real Unix entry forwards app timing and response bytes without identity metadata',async t=>{
 const timing=createAdminRequestTiming(),server=await fixture(t,(req,res)=>{
  timing.begin(req,res);const encoded=JSON.stringify({ok:true});timing.finish(res,200,{authenticated:true});res.writeHead(200,{'Content-Type':'application/json','Content-Length':Buffer.byteLength(encoded)});res.end(encoded);
 },{entry:true});const reply=await request(server);assert.match(reply.headers['server-timing'],/^app;dur=\d+\.\d{2}$/);assert.equal(reply.body,'{"ok":true}');assert.equal(reply.headers['content-length'],'11');
});

test('two concurrent responses keep independent monotonic starts',{timeout:5000},async t=>{
 let now=0,release,firstEntered;const entered=new Promise(resolve=>release=resolve),ready=new Promise(resolve=>firstEntered=resolve),timing=createAdminRequestTiming({clock:()=>now});
 const server=await fixture(t,async(req,res)=>{
  if(req.url.endsWith('first')){timing.begin(req,res);firstEntered();await entered;now=30;}else{now=10;timing.begin(req,res);release();}
  const encoded='{}';timing.finish(res,200,{authenticated:true});res.writeHead(200,{'Content-Type':'application/json'});res.end(encoded);
 });const first=request(server,{path:'/api/admin/first'});await ready;const second=request(server,{path:'/api/admin/second'});const [a,b]=await Promise.all([first,second]);assert.equal(a.headers['server-timing'],'app;dur=30.00');assert.equal(b.headers['server-timing'],'app;dur=0.00');
});

test('client cancellation keeps close handling intact and cannot add late headers',async t=>{
 const timing=createAdminRequestTiming();let notify,observed;const entered=new Promise(resolve=>notify=resolve),closed=new Promise(resolve=>observed=resolve);
 const server=await fixture(t,(req,res)=>{
  const before=res.listenerCount('close');timing.begin(req,res);assert.equal(res.listenerCount('close'),before);
  res.once('close',()=>{assert.equal(timing.finish(res,200,{authenticated:true}),false);observed();});notify();
 });
 const req=http.request({host:'127.0.0.1',port:server.address().port,path:'/api/admin/test',agent:false});req.on('error',()=>{});req.end();await entered;req.destroy();await closed;
});
