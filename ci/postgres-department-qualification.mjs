import assert from 'node:assert/strict';
import http from 'node:http';
import {randomUUID} from 'node:crypto';
import {isAbsolute,resolve} from 'node:path';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {createResponsesTransport} from '../responses.mjs';
import {migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {passwordHash} from '../auth.mjs';
import {candidateCondition} from './postgres-recording-qualification.mjs';

export async function closeDepartmentQualification({app,source,store}){
 const errors=[];
 for(const close of [()=>app?.close(),()=>source?new Promise((resolve,reject)=>{source.closeAllConnections();source.close(error=>error?reject(error):resolve());}):undefined,()=>store.close()]){
  try{await close();}catch(error){errors.push(error);}
 }
 if(errors.length)throw new AggregateError(errors,'Department qualification cleanup failed');
}

// This runs last, after the candidate's legacy GW/Worker have closed. It uses
// the existing real socket/report storage proof; no test probe is accepted here.
export async function qualifyCandidateDepartments({env,directory}={}){
 const database=new URL(env.HARBOR_DATABASE_URL);assert.equal(env.HARBOR_CANDIDATE_TEST,'1');assert.match(database.pathname,/^\/harbor_candidate_[a-f0-9]{32}$/);assert.ok(isAbsolute(directory)&&isAbsolute(env.HARBOR_DATA_DIR)&&resolve(env.HARBOR_DATA_DIR).startsWith(resolve(directory)+'/'));
 if(env.CI==='true'){assert.equal(database.hostname,'harbor-pg');assert.equal(database.searchParams.get('sslmode'),'verify-full');assert.equal(env.HARBOR_RECORDING_STORAGE_MODE,'report');}
 else{assert.equal(database.hostname,'localhost');assert.equal(database.username,'capacity_owner');assert.match(database.searchParams.get('host')||'',/^\/tmp\/harbor-capacity-[a-zA-Z0-9_-]+\/socket$/);}
 const store=new PostgresStore({connectionString:database.href,poolSize:2});let app,source;
 try{
  await migrateDepartmentIsolation(store,{apply:true});
  source=http.createServer((req,res)=>{void(async()=>{const chunks=[];for await(const chunk of req)chunks.push(chunk);const value=JSON.parse(Buffer.concat(chunks));assert.equal(value.model,'gpt-6.1-sol');res.writeHead(200,{'Content-Type':'text/event-stream'});res.end('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic-department-proof',status:'completed',model:value.model,output:[{id:'message',type:'message',content:[{type:'output_text',text:'合成部门正文'}]}],usage:{input_tokens:10,output_tokens:3,input_tokens_details:{cached_tokens:0}}}})+'\n\n');})().catch(()=>res.destroy());});await new Promise(resolve=>source.listen(0,'127.0.0.1',resolve));
  const transport=createResponsesTransport({credentials:async(_binary,home)=>({token:'synthetic-subscription',accountId:home}),version:async()=>'0.160.1',fetchImpl:(_url,init)=>fetch('http://127.0.0.1:'+source.address().port,init)});
  app=await createPostgresGateway({env:{...env,HARBOR_DEVICE_TLS_ENABLED:'0'},database:{connectionString:database.href},dataDir:env.HARBOR_DATA_DIR,runtimeDir:env.HARBOR_RUNTIME_DIR,config:{port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-department-admin')},recording:{enabled:true},recordingBuffer:{flushMs:20},responsesTransport:transport});
  await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+app.server.address().port;
  await candidateCondition(async()=>{await app.recordingStorage.refresh();return app.status().recordingCapture.effective;},'Department recording storage was not verified');assert.equal(app.status().recordingStorage.binding.verified,true);
  const call=async(path,{cookie,secret,method='GET',body}={})=>{const response=await fetch(origin+path,{method,headers:{...(cookie?{cookie}:{}),...(secret?{Authorization:'Bearer '+secret}:{}),...(body?{'Content-Type':'application/json'}:{})},...(body?{body:JSON.stringify(body)}:{}),signal:AbortSignal.timeout(15000)});return {status:response.status,value:await response.json(),cookie:response.headers.get('set-cookie')?.split(';')[0],requestId:response.headers.get('x-harbor-request-id')};};
  const root=await call('/api/auth/login',{method:'POST',body:{password:'synthetic-department-admin'}});assert.equal(root.status,200);const entries=[];
  for(const label of ['A','B']){
   const department=await call('/api/admin/departments',{cookie:root.cookie,method:'POST',body:{name:'合成正文验收'+label}});assert.equal(department.status,201);
   const key=await call('/api/admin/departments/'+department.value.id+'/management-keys',{cookie:root.cookie,method:'POST',body:{label:'合成正文管理'}});assert.equal(key.status,201);
   const login=await call('/api/auth/login',{method:'POST',body:{password:key.value.secret}});assert.equal(login.status,200);
   const member=await call('/api/admin/members',{cookie:login.cookie,method:'POST',body:{name:'合成正文员工'+label,quota:null,expires:null}});assert.equal(member.status,201);
   const account=randomUUID(),quota={plan:'pro',checked:Date.now(),remaining:50,windows:[{used:50,minutes:10080,reset:Date.now()+86400000}]};await store.run("INSERT INTO harbor.accounts(id,department_id,name,home,state,quota_json) VALUES($1,$2,'合成正文账号',$1,'ready',$3)",account,department.value.id,JSON.stringify(quota));
   const request=await call('/v1/responses',{secret:member.value.secret,method:'POST',body:{model:'gpt-6.1-sol',input:'合成正文隔离'+label,stream:false}});assert.equal(request.status,200);entries.push({department:department.value.id,cookie:login.cookie,request:request.requestId});
  }
  await candidateCondition(async()=>{const rows=await store.all('SELECT recording_state FROM harbor.requests WHERE id=ANY($1::text[])',entries.map(e=>e.request));return rows.length===2&&rows.every(row=>row.recording_state==='complete');},'Department request bodies were not fully captured');
  for(let i=0;i<entries.length;i++){const own=entries[i],other=entries[1-i],row=await store.get('SELECT r.department_id,COUNT(e.sequence)::int events FROM harbor.requests r JOIN harbor.conversation_events e ON e.request_id=r.id WHERE r.id=$1 GROUP BY r.id',own.request);assert.equal(row.department_id,own.department);assert.ok(row.events>=2);assert.equal((await call('/api/admin/requests/'+own.request+'/recording',{cookie:own.cookie})).status,200);assert.equal((await call('/api/admin/requests/'+own.request+'/recording',{cookie:other.cookie})).status,404);}
  return {candidate_department_recording_real_storage:true,candidate_department_recording_cross_access_rejected:true};
 }finally{await closeDepartmentQualification({app,source,store});}
}
