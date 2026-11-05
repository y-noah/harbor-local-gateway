import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn,execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {randomUUID,randomBytes} from 'node:crypto';
import {mkdtemp,mkdir,writeFile,readFile,rm,chmod} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createResponsesTransport} from '../responses.mjs';
import {passwordHash} from '../auth.mjs';
import {certificateFixture,wait} from './device-fixture.mjs';
import {createWindowsDeviceBridge} from '../clients/harbor-device/wsl-bridge.mjs';
import {runAppServer} from './codex-client-fixture.mjs';

const binary=process.env.HARBOR_CODEX_BINARY,socket=process.env.HARBOR_TEST_PG_SOCKET,url=process.env.HARBOR_TEST_DATABASE_URL,execute=promisify(execFile);
const windows=process.env.HARBOR_TEST_WINDOWS_DEVICE==='1',powershell='/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe',compiler='/mnt/c/Windows/Microsoft.NET/Framework64/v4.0.30319/csc.exe';
const png='iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aX1cAAAAASUVORK5CYII=';
const connection=name=>url?{connectionString:(()=>{const value=new URL(url);value.pathname='/'+name;return value.toString();})(),allowPlaintextLocalNetwork:true}:{connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_scale',database:name}};
const psQuote=value=>"'"+value.replaceAll("'","''")+"'";
async function ps(source){return (await execute(powershell,['-NoProfile','-NonInteractive','-EncodedCommand',Buffer.from(source,'utf16le').toString('base64')],{timeout:15000,maxBuffer:65536})).stdout.trim();}
async function windowsPath(path){return (await execute('wslpath',['-w',path])).stdout.trim();}
async function invoke(helper,args,input=''){return new Promise((resolve,reject)=>{
 const child=spawn(helper,args,{stdio:['pipe','pipe','pipe']}),chunks=[];let bytes=0,failure;const timer=setTimeout(()=>{failure=Error('isolated_windows_helper_timeout');child.kill();},30000);
 child.stdout.on('data',chunk=>{bytes+=chunk.length;if(bytes>262144){failure=Error('isolated_windows_helper_output_limit');child.kill();}else chunks.push(chunk);});child.stderr.resume();child.once('error',()=>{clearTimeout(timer);reject(Error('isolated_windows_helper_start_failed'));});child.once('close',code=>{clearTimeout(timer);failure||code?reject(failure??Error('isolated_windows_helper_failed_'+code)):resolve(Buffer.concat(chunks).toString());});child.stdin.on('error',()=>{});child.stdin.end(input);
});}

for(const device of [false,true])test('official Codex runs multiple image tools through the shared PostgreSQL gateway'+(device?' and the actual Windows CNG / WSL device bridge':''),{skip:!binary||!socket&&!url?'需显式官方CLI及隔离PG；不调用真实模型':device&&!windows?'需显式Windows实机验收开关':false,timeout:120000},async()=>{
 const admin=new PostgresStore(connection('postgres')),name='harbor_test_'+randomUUID().replaceAll('-',''),root=await mkdtemp(join(tmpdir(),'harbor-pg-codex-')),data=join(root,'data'),client=join(root,'client'),master=randomBytes(32);let created=false,store,app,ca,bridge,serving,helper,state,windowsRoot,prepared=false;const calls=[];let turns=0;
 try{
  await admin.query('CREATE DATABASE '+name);created=true;store=new PostgresStore(connection(name));await store.migrate();await store.initializePricing();await mkdir(data,{mode:0o700});await mkdir(client,{mode:0o700});await writeFile(join(data,'key-encryption.key'),master,{mode:0o600});
  const members=createPostgresMemberAdmin(store,{encryptionKey:master}),member=(await members.handle('/api/admin/members','POST',{name:'官方客户端合成员工',quota:null,expires:null,allowed_models:['gpt-6.1-sol','gpt-image-2']})).value;
  await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-native-account','合成上游',$1,'ready')",join(root,'upstream-home'));
  const transport=createResponsesTransport({version:async()=>'0.166.0',credentials:async()=>({token:'synthetic-upstream-only',accountId:'synthetic-native-account'}),fetchImpl:async(target,options)=>{
   const payload=JSON.parse(options.body);calls.push({target,payload,headers:options.headers});assert.ok(!JSON.stringify(payload).includes(member.secret));
   if(target.endsWith('/images/generations')||target.endsWith('/images/edits'))return Response.json({data:[{b64_json:png}],usage:{input_tokens:20,output_tokens:30}});
   assert.ok(target.endsWith('/responses'));if(payload.input==='synthetic device qualification')return new Response('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic-device-proof',status:'completed',model:'gpt-6.1-sol',output:[],usage:{input_tokens:0,output_tokens:0,input_tokens_details:{cached_tokens:0}}}})+'\n\n',{headers:{'content-type':'text/event-stream'}});turns++;const code=[
    'const r=await tools.mcp__harbor_image__image_generate({prompt:"synthetic blue circle",output_path:"generated.png"}); for(const c of r.content)if(c.type==="image")image(c);else if(c.type==="text")text(c.text);',
    'const r=await tools.mcp__harbor_image__image_generate({prompt:"synthetic red circle",output_path:"edited.png",reference_paths:["generated.png"]}); for(const c of r.content)if(c.type==="image")image(c);else if(c.type==="text")text(c.text);'
   ],item=turns<=2?{type:'custom_tool_call',id:'tool_'+turns,call_id:'call_'+turns,name:'exec',input:code[turns-1]}:{type:'message',id:'synthetic-final',role:'assistant',content:[{type:'output_text',text:'Synthetic PostgreSQL complete'}]},response={id:'synthetic-response-'+turns,status:'completed',model:'gpt-6.1-sol',output:[item],usage:{input_tokens:10,output_tokens:5,total_tokens:15}};
   return new Response([{type:'response.created',response:{id:response.id}},{type:'response.output_item.done',output_index:0,item},{type:'response.completed',response}].map(event=>'data: '+JSON.stringify(event)+'\n\n').join(''),{headers:{'content-type':'text/event-stream'}});
  }});
  if(device)ca=await certificateFixture(root);
  app=await createPostgresGateway({database:connection(name),env:{},dataDir:data,runtimeDir:join(root,'runtime'),recording:{enabled:false},config:{port:43127,codexBinary:binary,adminPasswordHash:passwordHash('synthetic-native-password')},responsesTransport:{...transport,models:async()=>[{slug:'gpt-6.1-sol',display_name:'Synthetic Sol'}]},rateReader:async()=>null,...(device?{devices:{issuer:ca.issuer,tls:{key:ca.key,cert:ca.cert,ca:ca.ca,port:0}}}:{})});
  await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));let origin='http://127.0.0.1:'+app.server.address().port,localToken,deviceId;
  if(device){
   helper=join(root,'HarborDevice.exe');await execute(compiler,['/nologo','/target:exe','/out:'+await windowsPath(helper),'/r:System.Net.Http.dll','/r:System.Management.dll','/r:System.Web.Extensions.dll',await windowsPath(fileURLToPath(new URL('../clients/harbor-device/HarborDevice.cs',import.meta.url)))],{timeout:30000,maxBuffer:65536});await chmod(helper,0o700);
   windowsRoot=await ps('[IO.Path]::Combine([IO.Path]::GetTempPath(),'+psQuote('HarborNative-Test-'+randomUUID())+')');state=windowsRoot+'\\state.json';const tls=await app.deviceTls.listen(),gateway='https://127.0.0.1:'+tls.port,invitation=await app.deviceAccess.invite({member_id:member.memberId}),port=await ps('$s=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0);try{$s.Start();$s.LocalEndpoint.Port}finally{$s.Stop()}');
   const preparedValue=JSON.parse(await invoke(helper,['prepare','--state',state,'--gateway',gateway,'--ca-file',await windowsPath(join(root,'ca.pem')),'--port',port,'--allow-software-key','--name','官方客户端实机合成设备'],invitation.token+'\n'));assert.equal(preparedValue.state,'pending');prepared=true;
   const pending=await store.get('SELECT id,version,hardware_state FROM harbor.employee_devices WHERE member_id=$1',member.memberId);deviceId=pending.id;await app.deviceAccess.approve(deviceId,{expected_version:pending.version,credentials_only:pending.hardware_state==='unavailable'});assert.equal(JSON.parse(await invoke(helper,['complete','--state',state])).state,'approved');
   serving=spawn(helper,['serve','--state',state],{stdio:['ignore','pipe','pipe']});let ready=false,serveError;serving.once('error',error=>{serveError=error;});serving.stdout.on('data',chunk=>{if(chunk.toString().includes('"state":"running"'))ready=true;});serving.stderr.resume();await wait(()=>ready||serveError||serving.exitCode!==null);assert.equal(ready,true,'isolated helper did not listen');localToken=await invoke(helper,['local-token','--state',state]);bridge=createWindowsDeviceBridge({helper,stateFile:state,localToken});const address=await bridge.listen();origin='http://127.0.0.1:'+address.port;
   const proof=await fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'x-harbor-local-token':localToken,'content-type':'application/json'},body:JSON.stringify({model:'gpt-6.1-sol',input:'synthetic device qualification'})});assert.equal(proof.status,200);await proof.text();await wait(async()=>!!(await store.get('SELECT verified_request_at FROM harbor.employee_devices WHERE id=$1',deviceId)).verified_request_at);await app.deviceAccess.policy(member.memberId,{mode:'enforced',expected_version:0});
  }
  const headers={authorization:'Bearer '+member.secret,...(device?{'x-harbor-local-token':localToken}:{})},catalog=await fetch(origin+'/v1/codex/models',{headers});assert.equal(catalog.status,200);assert.equal((await catalog.json()).models[0].slug,'gpt-6.1-sol');const script=await fetch(origin+'/v1/codex/image-mcp.mjs',{headers});assert.equal(script.status,200);const downloaded=await script.text();assert.equal(downloaded,await readFile(new URL('../clients/image-mcp.mjs',import.meta.url),'utf8'));assert.ok(!downloaded.includes(member.secret));const mcp=join(client,'harbor-image-mcp.mjs');await writeFile(mcp,downloaded,{mode:0o600});
  let config=(await readFile(new URL('../clients/harbor.example.toml',import.meta.url),'utf8')).replace(/base_url = "[^"]+"/,'base_url = "'+origin+'/v1"');if(device)config=config.replace('\n[features]','\n[model_providers.harbor.env_http_headers]\n"X-Harbor-Local-Token" = "HARBOR_DEVICE_LOCAL_TOKEN"\n\n[features]');
  const variables=['HARBOR_API_KEY','HARBOR_BASE_URL',...(device?['HARBOR_DEVICE_LOCAL_TOKEN']:[])];await writeFile(join(client,'config.toml'),config+'\n[mcp_servers.harbor_image]\ncommand = '+JSON.stringify(process.execPath)+'\nargs = '+JSON.stringify([mcp,root])+'\nenv_vars = '+JSON.stringify(variables)+'\ndefault_tools_approval_mode = "approve"\n',{mode:0o600});
  const env={...process.env,CODEX_HOME:client,RUST_LOG:'off',HARBOR_API_KEY:member.secret,HARBOR_BASE_URL:origin+'/v1'};delete env.OPENAI_API_KEY;delete env.HARBOR_DEVICE_LOCAL_TOKEN;if(device)env.HARBOR_DEVICE_LOCAL_TOKEN=localToken;
  const result=await runAppServer({binary,cwd:root,env});assert.equal(result.code,0,result.stderr);assert.equal(turns,3);for(const path of ['/images/generations','/images/edits'])assert.equal(calls.filter(call=>call.target.endsWith(path)).length,1);for(const file of ['generated.png','edited.png'])assert.deepEqual(await readFile(join(root,file)),Buffer.from(png,'base64'));assert.equal((await store.get('SELECT used FROM harbor.members WHERE id=$1',member.memberId)).used,145);const rows=await store.all('SELECT device_id,device_auth_state,device_generation FROM harbor.requests WHERE member_id=$1',member.memberId);assert.equal(rows.length,device?6:5);if(device){assert.ok(rows.every(row=>row.device_id===deviceId&&row.device_generation===1&&['authenticated','unverified_hardware'].includes(row.device_auth_state)));assert.ok(calls.every(call=>!Object.keys(call.headers).some(name=>name.toLowerCase()==='x-harbor-local-token')));}assert.match(result.stdout,/Synthetic PostgreSQL complete/);
 }finally{
  const errors=[],cleanup=async(label,operation)=>{try{await operation();}catch{errors.push(label);}};
  await cleanup('bridge',()=>bridge?.close({force:true}));await cleanup('helper',async()=>{if(serving?.pid&&serving.exitCode===null&&serving.signalCode===null){serving.kill();await new Promise(resolve=>serving.once('close',resolve));}});
  if(prepared)await cleanup('device-key',()=>invoke(helper,['remove','--state',state]));if(windowsRoot)await cleanup('windows-directory',()=>ps('if(Test-Path -LiteralPath '+psQuote(windowsRoot)+'){Remove-Item -LiteralPath '+psQuote(windowsRoot)+' -Recurse -Force}'));
  await cleanup('gateway',()=>app?.close());await cleanup('issuer',()=>ca?.issuer.close());await cleanup('store',()=>store?.close());if(created)await cleanup('database',()=>admin.query('DROP DATABASE '+name));await cleanup('admin',()=>admin.close());await cleanup('directory',()=>rm(root,{recursive:true,force:true}));assert.deepEqual(errors,[],'isolated client cleanup did not finish');
 }
});
