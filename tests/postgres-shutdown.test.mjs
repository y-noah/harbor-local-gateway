import test from 'node:test';
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {spawn} from 'node:child_process';
import {mkdtemp,mkdir,writeFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {randomBytes} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {postgresShutdownConfiguration,postgresGatewayShutdownExitCode,installPostgresGatewayShutdown} from '../postgres-shutdown.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {Store} from '../store.mjs';
const normal={forced:false,ownershipLost:false,metering:{queued:0,files:0},recordingStorage:{pending:0}};
test('shutdown values, normal/interrupted/unknown exit status and signal single-flight use one contract',async()=>{
 assert.deepEqual(postgresShutdownConfiguration(),{contract:'postgres-shutdown-v1',gateway_drain_ms:300000,cleanup_grace_ms:30000,worker_stop_ms:30000});
 for(const [name,raw] of [['HARBOR_GATEWAY_DRAIN_TIMEOUT_MS','99'],['HARBOR_GATEWAY_DRAIN_TIMEOUT_MS','3600001'],['HARBOR_SHUTDOWN_CLEANUP_GRACE_MS','0'],['HARBOR_WORKER_STOP_TIMEOUT_MS','no'],['HARBOR_GATEWAY_DRAIN_TIMEOUT_MS','1e3']])assert.throws(()=>postgresShutdownConfiguration({[name]:raw}));
 assert.equal(postgresGatewayShutdownExitCode(normal),0);assert.equal(postgresGatewayShutdownExitCode({...normal,forced:true}),2);
 for(const result of [null,{}, {...normal,ownershipLost:true},{...normal,forced:true,metering:{queued:1}},{...normal,metering:{files:1}},{...normal,recordingStorage:{pending:1}}])assert.equal(postgresGatewayShutdownExitCode(result),1);
 const runtime=new EventEmitter();let closes=0;const remove=installPostgresGatewayShutdown({close:async()=>{closes++;return {...normal,forced:true};}},{runtime});runtime.emit('SIGTERM');runtime.emit('SIGINT');await new Promise(resolve=>setImmediate(resolve));assert.equal(closes,1);assert.equal(runtime.exitCode,2);remove();assert.equal(runtime.listenerCount('SIGTERM'),0);
});
const childSource=String.raw`
import {createPostgresGateway} from ${JSON.stringify(new URL('../postgres-gateway.mjs',import.meta.url).href)};
import {createResponsesTransport} from ${JSON.stringify(new URL('../responses.mjs',import.meta.url).href)};
import {postgresShutdownConfiguration,installPostgresGatewayShutdown} from ${JSON.stringify(new URL('../postgres-shutdown.mjs',import.meta.url).href)};
import {passwordHash} from ${JSON.stringify(new URL('../auth.mjs',import.meta.url).href)};
import {setTimeout as delay} from 'node:timers/promises';
import {join} from 'node:path';
const send=value=>process.send?.(value),shutdown=postgresShutdownConfiguration(process.env),hold=Number(process.env.HARBOR_SHUTDOWN_TEST_HOLD_MS);let calls=0;
const transport=createResponsesTransport({version:async()=> '0.160.1',credentials:async()=>({token:'synthetic',accountId:'synthetic'}),fetchImpl:async(_url,{signal})=>{calls++;return new Response(new ReadableStream({start(controller){controller.enqueue(Buffer.from('data: '+JSON.stringify({type:'response.output_text.delta',item_id:'synthetic',content_index:0,delta:'合成流开始'})+'\n\n'));send({kind:'dispatched'});void delay(hold,undefined,{signal}).then(()=>{controller.enqueue(Buffer.from('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic',model:'gpt-6.1-sol',output:[{id:'synthetic',type:'message',content:[{type:'output_text',text:'合成流开始'}]}],usage:{input_tokens:10,output_tokens:1,input_tokens_details:{cached_tokens:0}}}})+'\n\n'));controller.close();},error=>controller.error(error));}}),{headers:{'content-type':'text/event-stream'}});}});
const probe=async()=>[{total:1000000000000,free:900000000000}];probe.status=()=>({source:'controlled_test_measurement'});
const app=await createPostgresGateway({database:{connection:JSON.parse(process.env.HARBOR_SHUTDOWN_TEST_CONNECTION)},dataDir:process.env.HARBOR_SHUTDOWN_TEST_DATA,runtimeDir:join(process.env.HARBOR_SHUTDOWN_TEST_DATA,'runtime'),env:{},config:{port:43127,codexBinary:'synthetic',adminPasswordHash:passwordHash('synthetic')},drainTimeoutMs:shutdown.gateway_drain_ms,responsesTransport:transport,rateReader:async()=>{throw Error('synthetic quota unavailable');},recording:{enabled:true,storage:{probe}}});
installPostgresGatewayShutdown(app);
process.on('SIGTERM',()=>{void app.close().then(result=>{send({kind:'closed',forced:result.forced,calls});process.disconnect();},()=>process.disconnect());});
await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));send({kind:'ready',port:app.server.address().port});
`;
async function until(condition,ms=10000){const end=Date.now()+ms;for(;;){if(await condition())return;if(Date.now()>=end)throw Error('shutdown condition timed out');await delay(10);}}
for(const mode of ['long-complete','deadline-interrupted'])test('real isolated PG and signalled gateway '+mode,{skip:!process.env.HARBOR_CAPACITY_PG_BIN?'需独占PG18工具链':false,timeout:90000},async t=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-shutdown-')),data=join(root,'data');await mkdir(data,{mode:0o700});let lab,store,legacy,child,ended;const messages=[];let stderr='';
 t.after(async()=>{if(child&&child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');await ended?.catch(()=>{});legacy?.close();await store?.close();await lab?.close();await rm(root,{recursive:true,force:true});});
 lab=await createPrivatePostgres({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB});store=new PostgresStore({connection:lab.connection});await store.migrate();await store.initializePricing();const key=randomBytes(32);legacy=new Store(join(root,'legacy.sqlite'),{encryptionKey:key});const member=legacy.createMember('合成关闭验证员工',null,Date.now()+600000,['gpt-6.1-sol']);
 for(const [table,id] of [['members',member.memberId],['keys',member.keyId]]){const row=legacy.get('SELECT * FROM '+table+' WHERE id=?',id),columns=Object.keys(row);await store.run('INSERT INTO harbor.'+table+'('+columns.join(',')+') VALUES('+columns.map((_,i)=>'$'+(i+1)).join(',')+')',...columns.map(name=>row[name]));}
 await writeFile(join(data,'key-encryption.key'),key,{mode:0o600});await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-shutdown','合成关闭账号',$1,'ready')",join(root,'runtime','accounts','synthetic'));
 child=spawn(process.execPath,['--input-type=module','-'],{env:{...process.env,HARBOR_SHUTDOWN_TEST_DATA:data,HARBOR_SHUTDOWN_TEST_CONNECTION:JSON.stringify(lab.connection),HARBOR_SHUTDOWN_TEST_HOLD_MS:mode==='long-complete'?'32000':'60000',HARBOR_GATEWAY_DRAIN_TIMEOUT_MS:mode==='long-complete'?'40000':'150'},stdio:['pipe','ignore','pipe','ipc']});child.stderr.on('data',value=>stderr+=value);child.on('message',value=>messages.push(value));ended=new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',(code,signal)=>resolve({code,signal}));});child.stdin.end(childSource);
 await until(()=>messages.some(row=>row.kind==='ready'));const {port}=messages.find(row=>row.kind==='ready');const responsePromise=fetch('http://127.0.0.1:'+port+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','thread-id':'synthetic-shutdown'},body:JSON.stringify({model:'gpt-6.1-sol',input:'合成关闭请求',stream:true})}).then(async response=>({status:response.status,text:await response.text()})).catch(()=>null);
 await until(()=>messages.some(row=>row.kind==='dispatched'));child.kill('SIGTERM');let result;await until(()=>child.exitCode!==null||child.signalCode!==null,55000);result=await ended;const response=await responsePromise;
 assert.equal(result.signal,null);assert.equal(result.code,mode==='long-complete'?0:2,stderr);assert.equal(messages.find(row=>row.kind==='closed')?.calls,1);assert.equal(messages.find(row=>row.kind==='closed')?.forced,mode!=='long-complete');
 const rows=await store.all('SELECT status,usage_unknown,recording_state,recording_reason FROM harbor.requests');assert.equal(rows.length,1);
 if(mode==='long-complete'){assert.equal(response.status,200);assert.match(response.text,/response.completed/);assert.equal(rows[0].status,'ok');assert.equal(rows[0].usage_unknown,0);}
 else{assert.equal(rows[0].status,'interrupted');assert.equal(rows[0].usage_unknown,1);assert.equal(rows[0].recording_state,'partial');assert.equal(rows[0].recording_reason,'process_interrupted');}
});
