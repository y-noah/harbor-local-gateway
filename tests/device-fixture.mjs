import assert from 'node:assert/strict';
import {randomUUID,randomBytes,sign} from 'node:crypto';
import {mkdtemp,readFile,writeFile,chmod,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import http from 'node:http';
import https from 'node:https';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresDeviceAccess} from '../postgres-device-access.mjs';
import {createDeviceCertificateAuthority} from '../device-certificates.mjs';
import {createDeviceTlsServer} from '../device-transport.mjs';
import {deviceEnrollmentMessage,deviceHardwareDigest} from '../device-proof.mjs';

const run=promisify(execFile),socket=process.env.HARBOR_TEST_PG_SOCKET,url=process.env.HARBOR_TEST_DATABASE_URL;
const integration={skip:!socket&&!url?'需隔离PostgreSQL，设备集成测试不使用线上库':false};
function databaseOptions(name){if(url){const value=new URL(url);if(name)value.pathname='/'+name;return {connectionString:value.toString(),allowPlaintextLocalNetwork:true};}return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_ci',database:name||'postgres'}};}
async function certificateFixture(root){
  const ca=join(root,'ca.pem'),caKey=join(root,'ca-key.pem'),serverKey=join(root,'server-key.pem'),serverCsr=join(root,'server.csr'),serverCert=join(root,'server.pem'),extensions=join(root,'server.cnf');
  await run('openssl',['req','-x509','-newkey','rsa:2048','-nodes','-keyout',caKey,'-out',ca,'-subj','/CN=Harbor Synthetic Device CA','-days','2','-addext','basicConstraints=critical,CA:TRUE'],{maxBuffer:32768});await chmod(caKey,0o600);
  await run('openssl',['req','-newkey','rsa:2048','-nodes','-keyout',serverKey,'-out',serverCsr,'-subj','/CN=localhost'],{maxBuffer:32768});await writeFile(extensions,'subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n');
  await run('openssl',['x509','-req','-in',serverCsr,'-CA',ca,'-CAkey',caKey,'-set_serial','1','-days','1','-extfile',extensions,'-out',serverCert],{maxBuffer:32768});
  return {ca:await readFile(ca),key:await readFile(serverKey),cert:await readFile(serverCert),issuer:createDeviceCertificateAuthority({certificateFile:ca,keyFile:caKey,days:1})};
}
async function clientKey(root,label){const key=join(root,label+'.key'),csr=join(root,label+'.csr');await run('openssl',['req','-newkey','rsa:2048','-nodes','-keyout',key,'-out',csr,'-subj','/CN=Untrusted Client Name'],{maxBuffer:32768});return {key:await readFile(key),csr:await readFile(csr,'utf8')};}
function request(origin,path,{body={},headers={},agent,raw}={}){return new Promise((resolve,reject)=>{
  const target=new URL(path,origin),transport=target.protocol==='https:'?https:http,payload=raw??Buffer.from(JSON.stringify(body));
  const req=transport.request(target,{method:'POST',headers:{'content-type':'application/json','content-length':payload.length,...headers},agent},res=>{const chunks=[];res.on('data',chunk=>chunks.push(chunk));res.on('end',()=>{const bytes=Buffer.concat(chunks);let value;try{value=JSON.parse(bytes.toString());}catch{}resolve({status:res.statusCode,value,raw:bytes,headers:res.headers});});});req.on('error',reject);req.end(payload);
});}
const wait=async predicate=>{const end=Date.now()+10000;while(!await predicate()){assert.ok(Date.now()<end,'设备测试条件等待超时');await new Promise(resolve=>setTimeout(resolve,10));}};
async function fixture(t,operation){
  const name='harbor_test_'+randomUUID().replaceAll('-',''),admin=new PostgresStore(databaseOptions()),root=await mkdtemp(join(tmpdir(),'harbor-device-test-'));let store,tlsServer,plain,upstream,ca;const agents=[];
  try{
    await admin.query('CREATE DATABASE '+name);store=new PostgresStore({...databaseOptions(name),poolSize:4});await store.migrate();if(!(await store.get("SELECT to_regclass('harbor.employee_devices') relation")).relation)await store.query(await readFile(new URL('../migrations/014-device-access.sql',import.meta.url),'utf8'));await store.initializePricing();ca=await certificateFixture(root);
    const members=createPostgresMemberAdmin(store,{encryptionKey:randomBytes(32)}),first=(await members.handle('/api/admin/members','POST',{name:'设备合成员工',expires:null,quota:null,allowed_models:['gpt-6.1-sol']})).value,second=(await members.handle('/api/admin/members','POST',{name:'另一设备合成员工',expires:null,quota:null,allowed_models:['gpt-6.1-sol']})).value,devices=createPostgresDeviceAccess(store,{issuer:ca.issuer});
    let dispatchGate=null,upstreamGate=null,streamGate=null,entered=0,dispatched=0,lastBody;
    upstream=http.createServer(async(req,res)=>{dispatched++;const chunks=[];for await(const chunk of req)chunks.push(chunk);lastBody=Buffer.concat(chunks);entered++;if(upstreamGate)await upstreamGate;res.writeHead(200,{'content-type':'text/event-stream'});res.write('event: response.output_text.delta\ndata: {"delta":"合成设备输出"}\n\n');if(streamGate)await streamGate;await new Promise(resolve=>setTimeout(resolve,10));res.end('event: response.completed\ndata: {"type":"response.completed"}\n\n');});await new Promise(resolve=>upstream.listen(0,'127.0.0.1',resolve));
    const handler=async(req,res)=>{try{
      const path=new URL(req.url,'https://localhost').pathname;
      if(path.startsWith('/api/device/enroll/')){const chunks=[];let bytes=0;for await(const chunk of req){bytes+=chunk.length;if(bytes>65536)throw Object.assign(Error('设备登记请求过大'),{status:413});chunks.push(chunk);}const result=await devices.publicHandle(path,req.method,JSON.parse(Buffer.concat(chunks).toString()),req);res.writeHead(result.status,{'content-type':'application/json'});res.end(JSON.stringify(result.value));return;}
      const key=String(req.headers.authorization||'').replace(/^Bearer /,''),identity=await store.authenticate(key);await devices.authorize(identity,req);const chunks=[];for await(const chunk of req)chunks.push(chunk);const body=Buffer.concat(chunks);
      const accepted=await store.transaction(async()=>{const current=await store.authenticate(key,{lock:true}),proof=await devices.authorize(current,req,{phase:'admission'}),requestId=await store.observe(current,'synthetic','device-test','gpt-6.1-sol');await store.run('UPDATE harbor.requests SET device_id=$1,device_name=$2,device_system=$3,device_generation=$4,device_auth_state=$5 WHERE id=$6',proof.deviceId,proof.deviceName,proof.deviceSystem,proof.generation,proof.authState,requestId);return {current,requestId};});
      if(dispatchGate)await dispatchGate;await devices.authorize(accepted.current,req,{phase:'dispatch'});
      const response=await fetch('http://127.0.0.1:'+upstream.address().port,{method:'POST',headers:{'content-type':'application/json',...(req.headers['content-encoding']?{'content-encoding':req.headers['content-encoding']}:{})},body});res.writeHead(response.status,{'content-type':response.headers.get('content-type')});for await(const chunk of response.body)if(!res.destroyed&&!res.write(chunk))await new Promise(resolve=>{const done=()=>{res.off('drain',done);res.off('close',done);resolve();};res.once('drain',done);res.once('close',done);});await store.finish(accepted.requestId,{input:20,output:5,cached:2});await devices.markVerified(accepted.requestId);res.end();
    }catch(error){if(!res.headersSent){res.writeHead(error.status||500,{'content-type':'application/json'});res.end(JSON.stringify({error:{code:error.code||'synthetic_error',message:error.message}}));}else res.destroy();}};
    tlsServer=createDeviceTlsServer({...ca,handler});const address=await tlsServer.listen(),origin='https://127.0.0.1:'+address.port;plain=http.createServer(handler);await new Promise(resolve=>plain.listen(0,'127.0.0.1',resolve));
    const anon=new https.Agent({ca:ca.ca,keepAlive:true});agents.push(anon);
    async function enroll(memberId,label,{fingerprint=true,replaces}={}){
      const key=await clientKey(root,label),invitation=await devices.invite({member_id:memberId,...(replaces?{replaces_device_id:replaces}:{})}),challenge=await request(origin,'/api/device/enroll/challenge',{agent:anon,body:{invitation:invitation.token}});assert.equal(challenge.status,200);
      const payload={invitation:invitation.token,...challenge.value,csr:key.csr,name:label,system:'合成Windows／WSL',fingerprint:fingerprint?deviceHardwareDigest(invitation.scope,'12345678-1234-1234-1234-123456789abc'):null,installation_fingerprint:fingerprint?deviceHardwareDigest(invitation.scope,'23456789-2345-2345-2345-23456789abcd'):null,fingerprint_source:fingerprint?'smbios_uuid':'unavailable'};delete payload.expires;payload.signature=sign('sha256',Buffer.from(deviceEnrollmentMessage(payload)),key.key).toString('base64');
      const registered=await request(origin,'/api/device/enroll/register',{agent:anon,body:payload});assert.equal(registered.status,201);return {...key,payload,registration:registered.value,invitation};
    }
    async function approve(enrollment,{credentialsOnly=false}={}){await devices.approve(enrollment.registration.device_id,{expected_version:0,credentials_only:credentialsOnly});const result=await request(origin,'/api/device/enroll/status',{agent:anon,body:{enrollment_token:enrollment.registration.enrollment_token}});assert.equal(result.status,200);const agent=new https.Agent({ca:ca.ca,key:enrollment.key,cert:result.value.certificate,keepAlive:true,maxSockets:1});agents.push(agent);return {agent,headers:{'x-harbor-hardware':enrollment.payload.fingerprint,'x-harbor-installation':enrollment.payload.installation_fingerprint}};}
    await operation({root,ca,store,devices,first,second,origin,plainOrigin:'http://127.0.0.1:'+plain.address().port,anon,enroll,approve,
      call:(key,client={},extra={})=>request(origin,'/v1/responses',{...extra,agent:client.agent??anon,headers:{authorization:'Bearer '+key,...client.headers,...extra.headers},body:{model:'gpt-6.1-sol',input:'合成设备提问'}}),
      closeTls:options=>tlsServer.close(options),setStreamGate:value=>{streamGate=value;},setDispatchGate:value=>{dispatchGate=value;},setUpstreamGate:value=>{upstreamGate=value;},counters:()=>({entered,dispatched,lastBody})});
  }finally{for(const agent of agents)agent.destroy();await tlsServer?.close({force:true});for(const server of [plain,upstream])if(server)await new Promise(resolve=>server.close(resolve));await ca?.issuer.close();await store?.close();try{await admin.query('DROP DATABASE '+name);}finally{await admin.close();await rm(root,{recursive:true,force:true});}}
}


export {fixture,request,wait,integration,certificateFixture,clientKey};
