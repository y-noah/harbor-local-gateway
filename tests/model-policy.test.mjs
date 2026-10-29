import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import http from 'node:http';
import {createGateway} from '../server.mjs';
import {Store,modelAllowlist} from '../store.mjs';
import {createResponsesTransport} from '../responses.mjs';
const headers=secret=>({Authorization:'Bearer '+secret,'Content-Type':'application/json'});
async function setup(t){
 const root=mkdtempSync(join(tmpdir(),'harbor-policy-')),calls=[];let holdModels=null,holdInference=null;
 const transport=createResponsesTransport({version:async()=>'fixture',credentials:async()=>({token:'synthetic-upstream',accountId:'fixture'}),fetchImpl:async(url,options)=>{
  if(url.includes('/models?')){if(holdModels)await holdModels();return Response.json({models:[{slug:'sol'},{slug:'astra'},{slug:'image'}]});}
  const body=JSON.parse(options.body);calls.push({url,body});if(holdInference)await holdInference();
  if(url.includes('/images/'))return Response.json({created:1,data:[{b64_json:'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aX1cAAAAASUVORK5CYII='}],usage:{input_tokens:1,output_tokens:1}});
  return new Response('data: '+JSON.stringify({type:'response.completed',response:{id:'fixture',status:'completed',model:body.model,output:[],usage:{input_tokens:1,output_tokens:1}}})+'\n\n',{headers:{'Content-Type':'text/event-stream'}});
 }});
 const app=createGateway({dataDir:join(root,'data'),runtimeDir:join(root,'runtime'),config:{adminToken:'fixture-admin',codexBinary:'fixture',reserveTokens:100},responsesTransport:transport,rateReader:async()=>null});
 app.store.run("INSERT INTO accounts(id,name,home,state) VALUES('a','fixture','fixture','ready')");
 await new Promise(r=>app.server.listen(0,'127.0.0.1',r));const base='http://127.0.0.1:'+app.server.address().port;
 t.after(async()=>{await app.close();rmSync(root,{recursive:true,force:true});});
 const call=async(path,method='GET',body,secret='fixture-admin')=>{const r=await fetch(base+path,{method,headers:headers(secret),...(body===undefined?{}:{body:JSON.stringify(body)})});return {status:r.status,body:await r.json()};};
 const member=app.store.createMember('fixture',null,null,['sol']);
 const policy=(allowed,key=member.keyId)=>call('/api/admin/keys/'+key,'PATCH',{allowed_models:allowed,expected_model_policy_version:app.store.get('SELECT model_policy_version FROM keys WHERE id=?',key).model_policy_version});
 const send=(model='sol',extra={},secret=member.secret)=>call('/v1/responses','POST',{model,input:'synthetic',stream:false,...extra},secret);
 return {...app,root,base,call,member,policy,send,calls,setModelWait:value=>holdModels=value,setInferenceWait:value=>holdInference=value};
}
test('model policies reject invalid configurations before creation and only administrators can change each Key',{timeout:10000},async t=>{
 const a=await setup(t),before=a.store.all('SELECT id FROM members').length;
 for(const value of [[],['codex'],['claude'],['*'],['sol',1],'sol',{},Array(51).fill('sol')]){
  const r=await a.call('/api/admin/members','POST',{name:'invalid',quota:null,expires:null,allowed_models:value});assert.equal(r.status,400);
 }
 assert.equal(a.store.all('SELECT id FROM members').length,before);
 assert.equal((await a.call('/api/admin/keys/'+a.member.keyId,'PATCH',{allowed_models:null,expected_model_policy_version:0},a.member.secret)).status,401);
 assert.deepEqual(modelAllowlist(['sol','sol']),['sol']);
 assert.equal((await a.policy(['astra'])).status,200);
 assert.equal((await a.call('/api/admin/keys/'+a.member.keyId,'PATCH',{allowed_models:null,expected_model_policy_version:0})).status,409);
 const snapshot=(await a.call('/api/admin/state')).body;assert.deepEqual(snapshot.keys[0].allowed_models,['astra']);assert.equal(snapshot.keys[0].model_policy_version,1);
 const history=(await a.call('/api/admin/history?kind=audit&action=key.models')).body;assert.ok(history.rows.some(r=>r.detail.allowed_models?.includes('astra')));
});
test('restricted requests reject before routing or inference, including existing sessions, spoofed fields, aliases and tool models',{timeout:10000},async t=>{
 const a=await setup(t);assert.equal((await a.send('sol')).status,200);assert.equal(a.calls.length,1);const sessions=a.store.all('SELECT id FROM sessions').length;
 for(const [model,extra] of [['astra',{}],['codex',{}],['sol-latest',{}],['SOL',{}],['astra',{allowed_models:null}],['sol',{tools:[{type:'image_generation',model:'sol'}]}],['sol',{tools:[{type:'namespace',tools:[{type:'image_generation'}]}]}],['sol',{tools:[{type:'hosted',model:'astra'}]}]])assert.equal((await a.send(model,extra)).status,403);
 assert.equal(a.calls.length,1);assert.equal(a.store.all('SELECT id FROM sessions').length,sessions);
 assert.equal((await a.send(undefined,{model:null})).status,400);
 assert.equal((await a.call('/v1/chat/completions','POST',{messages:[{role:'user',content:'synthetic'}]},a.member.secret)).status,403);
 for(const path of ['/v1/images/generations','/v1/images/edits']){
  const payload={model:'image',prompt:'synthetic',...(path.endsWith('edits')?{images:[{image_url:'data:image/png;base64,AAAA'}]}:{})};
  assert.equal((await a.call(path,'POST',payload,a.member.secret)).status,403);
 }
 assert.equal(a.calls.length,1);
 const other=a.store.createKey(a.member.memberId,null,null,['astra']);assert.equal((await a.send('astra',{},other.secret)).status,200);assert.equal((await a.send('sol',{},other.secret)).status,403);
 await a.policy(null);assert.equal((await a.send('astra')).status,200);assert.equal((await a.send('sol',{tools:[{type:'image_generation'}]})).status,200);
});
test('both model catalogs and employee metadata obey current Key policy, even across a policy change while catalog awaits',{timeout:10000},async t=>{
 const a=await setup(t);
 for(const path of ['/v1/models','/v1/codex/models']){const r=await a.call(path,'GET',undefined,a.member.secret);assert.equal(r.status,200);assert.deepEqual(r.body.models.map(x=>x.slug),['sol']);if(r.body.data)assert.deepEqual(r.body.data.map(x=>x.id),['sol']);}
 assert.deepEqual((await a.call('/api/me','GET',undefined,a.member.secret)).body.allowed_models,['sol']);
 a.store.run("UPDATE accounts SET home='uncached-fixture' WHERE id='a'");
 let entered,release;const ready=new Promise(r=>entered=r),held=new Promise(r=>release=r);a.setModelWait(()=>{entered();return held;});
 const pending=a.call('/v1/models','GET',undefined,a.member.secret);await ready;await a.policy(['astra']);release();assert.deepEqual((await pending).body.data.map(x=>x.id),['astra']);
});
test('policy is reread after a request body was admitted and a concurrent admin change blocks dispatch',{timeout:10000},async t=>{
 const a=await setup(t);let resolve;
 const finished=new Promise(r=>resolve=r);const req=http.request(a.base+'/v1/responses',{method:'POST',headers:headers(a.member.secret)},res=>{res.resume();res.on('end',()=>resolve(res.statusCode));});
 req.on('error',error=>resolve(error));req.write('{"model":"sol",');
 try{const deadline=Date.now()+3000;while((await a.call('/api/admin/state')).body.preparing!==1){assert.ok(Date.now()<deadline,'request body not admitted');await new Promise(r=>setTimeout(r,5));}
 await a.policy(['astra']);req.end('"input":"synthetic","stream":false}');assert.equal(await finished,403);assert.equal(a.calls.length,0);}finally{req.destroy();}
});
test('policy persistence preserves unrestricted legacy Keys and fails closed on invalid stored restrictions',t=>{
 const root=mkdtempSync(join(tmpdir(),'harbor-policy-store-')),path=join(root,'gateway.sqlite');let s=new Store(path);const legacy=s.createMember('legacy',null,null),limited=s.createMember('limited',null,null,['sol']);s.pricing.close();s.db.close();
 s=new Store(path);t.after(()=>{s.pricing.close();s.db.close();rmSync(root,{recursive:true,force:true});});
 assert.equal(s.allowedModels(s.authenticate(legacy.secret)),null);assert.deepEqual(s.allowedModels(s.authenticate(limited.secret)),['sol']);
 s.run('UPDATE keys SET allowed_models=? WHERE id=?','invalid-json',limited.keyId);assert.throws(()=>s.checkModel(s.authenticate(limited.secret),'sol'),e=>e.code==='model_policy_invalid');
});

test('model changes do not interrupt dispatched requests and existing session bindings cannot bypass later restrictions',{timeout:10000},async t=>{
 const a=await setup(t);let entered,release;const ready=new Promise(r=>entered=r),held=new Promise(r=>release=r);a.setInferenceWait(()=>{entered();return held;});
 const pending=a.send('sol',{prompt_cache_key:'same-client-session'});await ready;await a.policy(['astra']);release();assert.equal((await pending).status,200);
 assert.equal((await a.send('sol',{prompt_cache_key:'same-client-session'})).status,403);assert.equal(a.calls.length,1);
 assert.equal((await a.send('astra',{prompt_cache_key:'same-client-session'})).status,200);assert.equal(a.store.all('SELECT id FROM sessions').length,1);
});
test('explicitly allowed image models pass generation and editing while default unrestricted Keys remain compatible',{timeout:10000},async t=>{
 const a=await setup(t);await a.policy(['image']);
 for(const path of ['/v1/images/generations','/v1/images/edits']){const body={model:'image',prompt:'synthetic',...(path.endsWith('edits')?{images:[{image_url:'data:image/png;base64,AAAA'}]}:{})};assert.equal((await a.call(path,'POST',body,a.member.secret)).status,200);}
 assert.equal(a.calls.length,2);const created=await a.call('/api/admin/keys','POST',{memberId:a.member.memberId,expires:null,allowed_models:['sol']});assert.equal(created.status,201);assert.equal((await a.send('astra',{},created.body.secret)).status,403);
});
