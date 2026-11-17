import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {gzip,gunzipSync} from 'node:zlib';
import {readFile} from 'node:fs/promises';
import {createStaticCompression} from '../static-compression.mjs';
async function server(t,files,options){const assets=await createStaticCompression(files,options),app=http.createServer((req,res)=>{res.setHeader('Cache-Control','no-store');res.setHeader('Vary','Origin');res.setHeader('Content-Type','text/javascript; charset=utf-8');if(!assets.send(req,res,req.url.slice(1))){res.writeHead(404);res.end();}});await new Promise(resolve=>app.listen(0,'127.0.0.1',resolve));t.after(()=>new Promise(resolve=>app.close(resolve)));return encoding=>new Promise((resolve,reject)=>{const req=http.get({host:'127.0.0.1',port:app.address().port,path:'/app.js',headers:encoding===undefined?{}:{'accept-encoding':encoding}},res=>{const chunks=[];res.on('data',chunk=>chunks.push(chunk));res.on('end',()=>resolve({status:res.statusCode,headers:res.headers,body:Buffer.concat(chunks)}));});req.on('error',reject);});}
test('静态资源启动预压缩，真实HTTP协商保留原文/长度/安全缓存头',async t=>{
 const source=await readFile(new URL('../public/app.js',import.meta.url));let calls=0;const get=await server(t,new Map([['app.js',source]]),{gzipImpl:(bytes,callback)=>{calls++;gzip(bytes,callback);}});assert.equal(calls,1);
 for(const encoding of [undefined,'','br','gzip;q=0','gzip;q=0, *;q=1','gzip;q=0.3, identity;q=0.8','gzip;q=1, gzip;q=0']){const response=await get(encoding);assert.equal(response.status,200);assert.equal(response.headers['content-encoding'],undefined);assert.deepEqual(response.body,source);assert.equal(Number(response.headers['content-length']),source.length);assert.equal(response.headers['cache-control'],'no-store');assert.equal(response.headers.vary,'Origin, Accept-Encoding');}
 for(const encoding of ['gzip','GZip','*','gzip;q=1, identity;q=0','gzip;q=0.4, identity;q=0.2']){const response=await get(encoding);assert.equal(response.status,200);assert.equal(response.headers['content-encoding'],'gzip');assert.deepEqual(gunzipSync(response.body),source);assert.equal(Number(response.headers['content-length']),response.body.length);assert.ok(response.body.length<source.length);}
 for(const encoding of ['gzip;q=0,identity;q=0','*;q=0']){const response=await get(encoding);assert.equal(response.status,406);assert.equal(response.body.length,0);}
 assert.equal(calls,1);
});
test('启动压缩不可用时允许identity的客户端仍获得原文',async t=>{const source=Buffer.from('synthetic'),get=await server(t,new Map([['app.js',source]]),{gzipImpl:(_bytes,callback)=>callback(Error('synthetic'))});assert.deepEqual((await get('gzip')).body,source);assert.equal((await get('gzip,identity;q=0')).status,503);});
