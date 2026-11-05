import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {spawn} from 'node:child_process';
import {createWindowsDeviceBridge} from '../clients/harbor-device/wsl-bridge.mjs';

const localToken='synthetic-device-local-auth-'.repeat(2);
const wait=async predicate=>{const end=Date.now()+5000;while(!predicate()){assert.ok(Date.now()<end,'设备桥回收超时');await new Promise(resolve=>setTimeout(resolve,10));}};
test('WSL bridge rejects browser and unauthenticated requests and reclaims streaming relay after cancellation',{timeout:10000},async()=>{
  let created=0;const children=[];
  const bridge=createWindowsDeviceBridge({helper:'synthetic',stateFile:'synthetic',localToken,maxConcurrent:1,spawnProcess:()=>{
    created++;const child=spawn(process.execPath,['-e',`let done=false;process.stdin.on('data',chunk=>{if(done)return;done=true;process.stdout.write('HTTP/1.1 200 OK\\r\\nContent-Type: text/event-stream\\r\\nTransfer-Encoding: chunked\\r\\nConnection: close\\r\\n\\r\\n');const text='data: synthetic\\n\\n';process.stdout.write(Buffer.byteLength(text).toString(16)+'\\r\\n'+text+'\\r\\n');});setInterval(()=>{},1000);`],{stdio:['pipe','pipe','pipe']});children.push(child);return child;
  }});
  try{
    const address=await bridge.listen(),origin='http://127.0.0.1:'+address.port;
    for(const headers of [{},{'x-harbor-local-token':localToken,origin:'https://untrusted.invalid'},{'x-harbor-local-token':localToken,'sec-fetch-site':'same-origin'}]){const result=await fetch(origin+'/v1/responses',{method:'POST',headers:{connection:'close',...headers},body:'{}'});assert.equal(result.status,403);await result.text();}
    const forbidden=await fetch(origin+'/api/admin/state',{headers:{connection:'close','x-harbor-local-token':localToken}});assert.equal(forbidden.status,403);assert.equal(created,0);
    let stop;const first=new Promise((resolve,reject)=>{const req=http.request(origin+'/v1/responses',{method:'POST',headers:{'x-harbor-local-token':localToken,'content-type':'application/json','content-length':2}},res=>{res.once('data',()=>resolve(res));res.once('error',()=>{});});req.once('error',reject);req.end('{}');stop=()=>req.destroy();});const stream=await first;assert.equal(stream.statusCode,200);assert.equal(bridge.status().active,1);
    const busy=await fetch(origin+'/v1/responses',{method:'POST',headers:{'x-harbor-local-token':localToken},body:'{}'});assert.equal(busy.status,503);await busy.text();stop();await wait(()=>bridge.status().active===0);assert.equal(created,1);const second=await fetch(origin+'/v1/responses',{method:'POST',headers:{'x-harbor-local-token':localToken},body:'{}'});assert.equal(second.status,200);const closing=bridge.close();await bridge.close({force:true});await closing;await wait(()=>bridge.status().active===0);
  }finally{await bridge.close({force:true});for(const child of children)if(child.exitCode===null)child.kill();}
});
