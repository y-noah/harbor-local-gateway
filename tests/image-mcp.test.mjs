import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,rm,symlink,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {PassThrough} from 'node:stream';
import {createImageTool,serve} from '../clients/image-mcp.mjs';
const png='iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aX1cAAAAASUVORK5CYII=';
async function setup(t,fetchImpl){
 const root=await mkdtemp(join(tmpdir(),'harbor-image-'));await mkdir(join(root,'workspace'));
 t.after(()=>rm(root,{recursive:true,force:true}));
 const generate=await createImageTool({root:join(root,'workspace'),baseUrl:'http://127.0.0.1:1234/v1',key:'fixture-key',fetchImpl});
 return {root,workspace:join(root,'workspace'),generate};
}
test('MCP generation and editing save exact bytes and send only inline workspace references',async t=>{
 const calls=[],app=await setup(t,async(url,options)=>{calls.push({url,options});return Response.json({data:[{b64_json:png}]});});
 for(const args of [{prompt:'blue',output_path:'first.png',transparent_background:true},{prompt:'red',output_path:'second.png',reference_paths:['first.png'],quality:'high',size:'1024x1024'}]){
  const result=await app.generate(args);assert.equal(result.content[1].mimeType,'image/png');assert.deepEqual(await readFile(join(app.workspace,args.output_path)),Buffer.from(png,'base64'));
 }
 assert.match(calls[0].url,/\/images\/generations$/);assert.match(calls[1].url,/\/images\/edits$/);
 assert.equal(calls[0].options.headers.Authorization,'Bearer fixture-key');assert.equal(calls[0].options.headers['thread-id'],calls[1].options.headers['thread-id']);
 const payload=JSON.parse(calls[1].options.body);assert.equal(payload.model,'gpt-image-2');assert.deepEqual(payload.images,[{image_url:'data:image/png;base64,'+png}]);assert.equal(payload.quality,'high');assert.equal(payload.size,'1024x1024');
});
test('MCP rejects traversal, symlink escape, overwrite and non-images before any generation',async t=>{
 let calls=0;const app=await setup(t,async()=>{calls++;return Response.json({data:[{b64_json:png}]});});
 await writeFile(join(app.root,'outside.png'),Buffer.from(png,'base64'));await symlink(app.root,join(app.workspace,'escape'),'dir');await writeFile(join(app.workspace,'existing.png'),'preserve');
 for(const args of [{output_path:'../outside.png'},{output_path:'escape/new.png'},{output_path:'existing.png'},{output_path:'bad.png',reference_paths:['../outside.png']},{output_path:'bad.png',reference_paths:['escape/outside.png']},{output_path:'bad.png',reference_paths:['existing.png']},{output_path:'bad.png',reference_paths:'existing.png'}])await assert.rejects(app.generate({prompt:'fixture',...args}));
 assert.equal(calls,0);assert.equal(await readFile(join(app.workspace,'existing.png'),'utf8'),'preserve');assert.deepEqual((await readdir(app.workspace)).sort(),['escape','existing.png']);
});
test('MCP error, invalid image and cancellation remove reserved files without automatic retry',async t=>{
 for(const mode of ['429','json','image','cancel']){
  let calls=0;const controller=new AbortController();const app=await setup(t,async()=>{calls++;if(mode==='cancel'){controller.abort();throw Error('fixture-private-key');}if(mode==='429')return new Response('fixture-private-key',{status:429});return Response.json(mode==='json'?{}:{data:[{b64_json:'c2VjcmV0'}]});});
  await assert.rejects(app.generate({prompt:'fixture',output_path:'failed.png'},controller.signal),error=>!error.message.includes('fixture-private-key'));
  assert.equal(calls,1);assert.deepEqual(await readdir(app.workspace),[]);
 }
});
test('MCP concurrent calls cannot overwrite the same reserved output file',async t=>{
 let release,started;const began=new Promise(r=>started=r),gate=new Promise(r=>release=r);let calls=0;
 const app=await setup(t,async()=>{calls++;started();await gate;return Response.json({data:[{b64_json:png}]});});
 const first=app.generate({prompt:'fixture',output_path:'same.png'});await began;
 try{await assert.rejects(app.generate({prompt:'fixture',output_path:'same.png'}),/已存在/);}finally{release();}
 await first;assert.equal(calls,1);
});
test('MCP protocol reports malformed requests, lists tool and propagates cancellation',async()=>{
 const input=new PassThrough(),output=new PassThrough();let buffer='';output.on('data',b=>buffer+=b);let started;const began=new Promise(r=>started=r);
 await serve({input,output,generate:(_,signal)=>new Promise((resolve,reject)=>{started();signal.addEventListener('abort',()=>reject(Error('cancelled')),{once:true});})});
 for(const line of ['invalid','null',JSON.stringify({jsonrpc:'2.0',id:1,method:'initialize',params:{protocolVersion:'2024-11-05'}}),JSON.stringify({jsonrpc:'2.0',id:2,method:'tools/list'}),JSON.stringify({jsonrpc:'2.0',id:3,method:'tools/call',params:{name:'image_generate',arguments:{}}})])input.write(line+'\n');
 await began;input.write(JSON.stringify({jsonrpc:'2.0',method:'notifications/cancelled',params:{requestId:3}})+'\n');
 const deadline=Date.now()+2000;while(!buffer.includes('cancelled')){assert.ok(Date.now()<deadline);await new Promise(r=>setImmediate(r));}input.end();
 const messages=buffer.trim().split('\n').map(JSON.parse);assert.equal(messages[0].error.code,-32700);assert.equal(messages[1].error.code,-32600);assert.equal(messages.find(m=>m.id===2).result.tools[0].name,'image_generate');assert.equal(messages.find(m=>m.id===3).result.isError,true);
});
