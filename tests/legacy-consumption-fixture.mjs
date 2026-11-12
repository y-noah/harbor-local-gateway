import {randomUUID,randomBytes} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import http from 'node:http';
import {PostgresStore} from '../postgres-store.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateConsumptionPolicy} from '../postgres-consumption-policy.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';
import {createPostgresMemberLimits} from '../postgres-member-limits.mjs';
import {createPostgresResponsesHandler} from '../postgres-responses.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {RecordingBuffer} from '../recording-buffer.mjs';
import {MeteringRecovery} from '../metering-recovery.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL;
const options=(database='postgres')=>{const u=new URL(url);u.pathname='/'+database;return {connectionString:u.href,allowPlaintextLocalNetwork:true,poolSize:6,statementTimeoutMs:5000};};
export const legacyBoundaryIntegration={skip:!url?'需独立PG测试库':false,timeout:30000};
export async function withLegacyConsumption(run,{install=true,policy='inherit'}={}){
 const root=new PostgresStore(options()),database='harbor_legacy_'+randomUUID().replaceAll('-',''),directory=await mkdtemp(join(tmpdir(),'harbor-legacy-source-')),encryptionKey=randomBytes(32),calls=[];let store,admin,buffer,metering,handler,server;
 const gates=[];let modelGate=null,credentialGate=null;
 try{
  await root.query('CREATE DATABASE '+database);store=new PostgresStore(options(database));await store.migrate();await store.initializePricing();if(install){await createUsageRollupRuntime(store);await migrateConsumptionPolicy(store,{apply:true});}
  admin=createPostgresMemberAdmin(store,{encryptionKey});const member=(await admin.handle('/api/admin/members','POST',{name:'合成协议员工',quota:null,expires:null,...(install?{consumption_policy:policy}:{})})).value;
  const subscription=randomUUID(),platform=randomUUID(),at=Date.now(),quota=JSON.stringify({plan:'pro',checked:at,remaining:90,windows:[{used:10,minutes:10080,reset:at+3600000}]});
  await store.run("INSERT INTO harbor.accounts(id,name,home,state,quota_json,token_budget) VALUES($1,'合成订阅',$1,'ready',$2,1000000)",subscription,quota);
  await store.run("INSERT INTO harbor.accounts(id,name,home,state,quota_json,kind,auth_mode,platform_models) VALUES($1,'合成平台',$1,'ready',$2,'deepseek','platform','[\"deepseek-chat\"]')",platform,quota);
  const recordings=new RecordingStore(store,{encryptionKey});buffer=await RecordingBuffer.create(recordings,{directory:join(directory,'recording'),flushMs:10000});metering=await MeteringRecovery.create(store,{encryptionKey,directory:join(directory,'metering'),flushMs:10000});
  const router=createPostgresRouter(store),limits=createPostgresMemberLimits(store);
  handler=createPostgresResponsesHandler({store,recordings,buffer,meteringRecovery:metering,resolveRoute:router.resolve,onRouteFailure:router.block,checkLimits:(person,payload,args)=>limits.check(person.id,payload.model,args),transportForAccount:async()=>({}),adminAuthenticated:async()=>false,recordingEnabled:false,
   runChat:async args=>{calls.push({protocol:'chat',account:args.account.id});if(modelGate)await modelGate;return {text:'合成CLI回复',threadId:args.session.upstream_id||randomUUID(),input:10,output:2,cached:0,usageKnown:true,actualModel:'gpt-6.1-sol',cumulativeUsage:{input:args.session.cum_input+10,output:args.session.cum_output+2,cached:args.session.cum_cached}};},
   platformTransport:{run:async(account)=>{calls.push({protocol:'platform',account:account.id});if(modelGate)await modelGate;return {text:'合成平台回复',input:10,output:2,cached:0,usageKnown:true,actualModel:'deepseek-chat'};}},accountAdmin:{platformCredential:async()=>{if(credentialGate)await credentialGate;return {token:'synthetic-not-a-key'};}}
  });
  server=http.createServer((req,res)=>{void handler.handle(req,res).then(done=>{if(!done){res.statusCode=404;res.end('{}');}}).catch(error=>{if(res.headersSent){res.destroy();return;}res.statusCode=error.status||500;res.setHeader('content-type','application/json');res.end(JSON.stringify({error:{code:error.code||'internal'}}));});});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const origin='http://127.0.0.1:'+server.address().port;
  const call=async(path,{secret=member.secret,body,headers={}}={})=>{const response=await fetch(origin+path,{method:body===undefined?'GET':'POST',headers:{authorization:'Bearer '+secret,...(body===undefined?{}:{'content-type':'application/json'}),...headers},...(body===undefined?{}:{body:JSON.stringify(body)})});return {status:response.status,value:await response.json()};};
  const chatBody={model:'codex',messages:[{role:'user',content:'合成协议请求'}]},platformBody={model:'deepseek/deepseek-chat',messages:[{role:'user',content:'合成协议请求'}]};
  const gate=()=>{let resolve;const promise=new Promise(yes=>resolve=yes),value={promise,resolve};gates.push(value);return value;};
  await run({store,admin,member,handler,calls,subscription,platform,call,origin,chatBody,platformBody,gate,setModelGate:value=>modelGate=value,setCredentialGate:value=>credentialGate=value,setPolicy:(consumption_policy,version)=>admin.handle('/api/admin/keys/'+member.keyId,'PATCH',{consumption_policy,expected_consumption_policy_version:version})});
 }finally{for(const value of gates)value.resolve();handler?.abort();if(server){server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}await handler?.closeQueries();await metering?.close();await buffer?.close();await admin?.close();await store?.close();await root.query('DROP DATABASE IF EXISTS '+database);if(install)await root.query('DROP ROLE IF EXISTS '+database+'_runtime');await root.close();await rm(directory,{recursive:true,force:true});encryptionKey.fill(0);}
}
