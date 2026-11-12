import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {once} from 'node:events';
import {gzipSync,gunzipSync} from 'node:zlib';
import {createAdminJsonCompression} from '../admin-json-compression.mjs';
const encoded=JSON.stringify({synthetic:'合成内容'.repeat(2000)});
async function fixture(t,options={},sendOptions={}){
 const helper=createAdminJsonCompression(options);let last;
 const server=http.createServer((req,res)=>{res.setHeader('Vary','Origin');helper.send(req,res,encoded,{authenticated:true,beforeHeaders:()=>{assert.equal(res.headersSent,false);res.setHeader('Server-Timing','app;dur=1');},...sendOptions}).then(value=>last=value,error=>{res.statusCode=500;res.end('test-error');});});server.listen(0,'127.0.0.1');await once(server,'listening');
 t.after(async()=>{await helper.close();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));});
 const get=(encoding,method='GET')=>new Promise((resolve,reject)=>{const req=http.request({host:'127.0.0.1',port:server.address().port,method,headers:encoding===undefined?{}:{'Accept-Encoding':encoding}},res=>{const chunks=[];res.on('data',b=>chunks.push(b));res.on('end',()=>resolve({status:res.statusCode,headers:res.headers,body:Buffer.concat(chunks)}));});req.on('error',reject);req.end();});return {helper,get,last:()=>last,port:server.address().port};
}
test('真实HTTP异步gzip与identity保留UTF8内容和长度，完成编码后才记录timing',async t=>{
 const f=await fixture(t),compressed=await f.get('gzip');assert.equal(compressed.status,200);assert.equal(compressed.headers['content-encoding'],'gzip');assert.equal(Number(compressed.headers['content-length']),compressed.body.length);assert.equal(gunzipSync(compressed.body).toString(),encoded);assert.equal(compressed.headers.vary,'Origin, Accept-Encoding');assert.ok(compressed.headers['server-timing']);
 for(const header of [undefined,'identity','gzip;q=0','gzip;q=0, *;q=1','gzip ;q=0, *;q=1','gzip\t;q=0, *;q=1','gzip;q=0.5, identity;q=1']){const result=await f.get(header);assert.equal(result.status,200);assert.equal(result.headers['content-encoding'],undefined);assert.equal(result.body.toString(),encoded);assert.equal(Number(result.headers['content-length']),Buffer.byteLength(encoded));}
 for(const header of ['*;q=1','gzip;q=0.8,identity;q=0.2','identity;q=0,gzip','gzip ;q=1, identity;q=0','gzip\t;q=1, identity;q=0'])assert.equal((await f.get(header)).headers['content-encoding'],'gzip');
 for(const header of ['gzip;q=0,identity;q=0','*;q=0','gzip;q=0,*;q=0']){const result=await f.get(header);assert.equal(result.status,406);assert.equal(result.headers['server-timing'],undefined);assert.ok(!result.body.toString().includes('合成'));}
});
test('预算超限或gzip失败完整回退identity，identity禁用时有界503',async t=>{
 for(const options of [{maxInputBytes:1},{gzipImpl:(input,cb)=>cb(Error('synthetic'))},{gzipImpl:()=>{throw Error('synthetic');}}]){const f=await fixture(t,options);assert.equal((await f.get('gzip')).body.toString(),encoded);const reject=await f.get('gzip,identity;q=0');assert.equal(reject.status,503);assert.equal(reject.headers['server-timing'],undefined);}
});
test('压缩更大时可回退；身份禁止则发送标准gzip；重复Vary不扩增',async()=>{
 const headers=new Map([['Vary','Accept-Encoding']]),res={getHeader:n=>headers.get(n),setHeader:(n,v)=>headers.set(n,v),removeHeader:n=>headers.delete(n),writeHead(){},end(body){this.body=body;}};const h=createAdminJsonCompression();const req={method:'GET',headers:{'accept-encoding':'gzip'}};
 await h.send(req,res,'{}',{authenticated:true});assert.equal(res.body.toString(),'{}');assert.equal(headers.get('Vary'),'Accept-Encoding');req.headers['accept-encoding']='gzip,identity;q=0';await h.send(req,res,'{}',{authenticated:true});assert.equal(gunzipSync(res.body).toString(),'{}');await h.close();
});
test('单在途无排队，取消与关闭均等待真实压缩回调后释放槽',async t=>{
 let callback,entered;const started=new Promise(r=>entered=r);const f=await fixture(t,{gzipImpl:(input,cb)=>{callback=()=>cb(null,gzipSync(input));entered();}});
 const pending=f.get('gzip');await started;assert.equal(f.helper.status().active,true);assert.equal((await f.get('gzip')).body.toString(),encoded);assert.equal((await f.get('gzip,identity;q=0')).status,503);
 let closed=false;const closing=f.helper.close().then(()=>closed=true);await Promise.resolve();assert.equal(closed,false);assert.equal(f.helper.status().active,true);callback();await pending;await closing;assert.equal(f.helper.status().active,false);assert.equal((await f.get('gzip')).body.toString(),encoded);
});
test('执行中取消不发送、不提前释放槽；未开始和敏感调用拒绝不压缩',async()=>{
 let callback,writes=0;const headers=new Map(),res={getHeader:n=>headers.get(n),setHeader:(n,v)=>headers.set(n,v),removeHeader:n=>headers.delete(n),writeHead(){writes++;},end(){writes++;}};const c=new AbortController(),h=createAdminJsonCompression({gzipImpl:(input,cb)=>callback=()=>cb(null,gzipSync(input))}),req={method:'GET',headers:{'accept-encoding':'gzip'}};
 const active=h.send(req,res,encoded,{authenticated:true,signal:c.signal});c.abort();assert.equal(h.status().active,true);callback();assert.equal((await active).sent,false);assert.equal(writes,0);assert.equal(h.status().active,false);
 assert.equal((await h.send(req,res,encoded,{authenticated:true,signal:c.signal})).sent,false);
 for(const options of [{authenticated:false},{authenticated:true,status:401}])await assert.rejects(h.send(req,res,encoded,options),/invalid_admin_compression_response/);
 await assert.rejects(h.send({...req,method:'POST'},res,encoded,{authenticated:true}),/invalid_admin_compression_response/);await h.close();
});

test('真实HTTP断开后在途zlib仍保留槽直至回调且不发送响应',async t=>{
 let callback,entered;const start=new Promise(resolve=>entered=resolve);const f=await fixture(t,{gzipImpl:(input,cb)=>{callback=()=>cb(null,gzipSync(input));entered();}});
 const request=http.get({host:'127.0.0.1',port:f.port,headers:{'Accept-Encoding':'gzip'}});request.on('error',()=>{});await start;const closed=new Promise(resolve=>request.once('close',resolve));request.destroy();await closed;
 // A separate identity request proves the HTTP disconnect has propagated through
 // the event loop before the held compression callback is released.
 try{await f.get('identity');assert.equal(f.helper.status().active,true);}finally{callback();}
 const end=Date.now()+1000;while(f.last()?.sent!==false){if(Date.now()>end)throw Error('cancelled send did not settle');await new Promise(resolve=>setImmediate(resolve));}
 assert.equal(f.helper.status().active,false);
});
