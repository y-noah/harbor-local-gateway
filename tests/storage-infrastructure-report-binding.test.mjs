import test from 'node:test';
import {once} from 'node:events';
import assert from 'node:assert/strict';
import {createHash,createHmac,randomBytes} from 'node:crypto';
import {performance} from 'node:perf_hooks';
import {spawn,spawnSync} from 'node:child_process';
import {mkdtemp,writeFile,readFile,mkdir,rm,chmod,lstat,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {infrastructurePublisherOption,infrastructurePublisherPlan,infrastructureBootstrapFingerprint,infrastructureContainerIdentity,infrastructureTargetIdentity,waitInfrastructureTargetReport,bindInstalledInfrastructureReport} from '../storage-infrastructure-report-binding.mjs';
const root='/opt/product-development/harbor-local-gateway/postgres-infra',hash=value=>createHash('sha256').update(value).digest('hex'),schema=[{version:1,checksum:'a'.repeat(64)}],target={database:'harbor_private_target',databaseOid:'99',cluster:'12345',schemaDigest:hash(JSON.stringify(schema))};
const control=()=>({version:1,database:null,pgDataDir:root+'/db/18/docker',socketDirectory:'/var/run/postgresql',port:5432,keyFile:'/run/harbor-report.key'});
test('bootstrap root permits private or traversal-only mode while child permissions remain exact',async()=>{
 const source=(await readFile(new URL('../storage-infrastructure-report-binding.mjs',import.meta.url),'utf8')).replace(/from '(\.\/[^']+)'/g,(_,path)=>"from '"+new URL('../'+path.slice(2),import.meta.url).href+"'")+'\nexport {directory};';
 const {directory}=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64')),path=await mkdtemp(join(tmpdir(),'harbor-infra-permissions-')),link=path+'-link';
 try{
  for(const mode of [0o700,0o711]){await chmod(path,mode);const identity=await directory(path,process.getuid(),0o700,0o711);assert.equal(identity.ino,(await lstat(path)).ino);}
  await assert.rejects(directory(path,process.getuid(),0o700),{code:'storage_infrastructure_report_directory_identity'});
  for(const mode of [0o710,0o750,0o755,0o770,0o777]){await chmod(path,mode);await assert.rejects(directory(path,process.getuid(),0o700,0o711),{code:'storage_infrastructure_report_directory_identity'});}
  await chmod(path,0o711);await assert.rejects(directory(path,process.getuid()+1,0o700,0o711),{code:'storage_infrastructure_report_directory_identity'});
  await symlink(path,link);await assert.rejects(directory(link,process.getuid(),0o700,0o711),{code:'storage_infrastructure_report_directory_identity'});
 }finally{await rm(link,{force:true});await rm(path,{recursive:true,force:true});}
});
test('admin endpoint keeps the shared TLS hostname but requires the owned loopback port',async()=>{
 const source=(await readFile(new URL('../storage-infrastructure-report-binding.mjs',import.meta.url),'utf8')).replace(/from '(\.\/[^']+)'/g,(_,path)=>"from '"+new URL('../'+path.slice(2),import.meta.url).href+"'")+'\nexport {infrastructureAdminEndpoint};';
 const {infrastructureAdminEndpoint}=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64')),local=async()=>[{address:'127.0.0.1'}];
 for(const host of ['127.0.0.1','localhost','harbor-local-gateway-postgres-infra'])assert.equal((await infrastructureAdminEndpoint('postgresql://'+host+'/postgres',5432,local)).hostname,host);
 const uri='postgresql://harbor-local-gateway-postgres-infra:55432/postgres';assert.equal((await infrastructureAdminEndpoint(uri,55432,local)).port,'55432');
 for(const addresses of [[],[{address:'192.0.2.1'}],[{address:'127.0.0.1'},{address:'192.0.2.1'}]])await assert.rejects(infrastructureAdminEndpoint(uri,55432,async()=>addresses),{code:'storage_infrastructure_report_endpoint_identity'});
 await assert.rejects(infrastructureAdminEndpoint(uri,5432,local),{code:'storage_infrastructure_report_endpoint_identity'});
 await assert.rejects(infrastructureAdminEndpoint('postgresql://foreign.invalid/postgres',5432,local),{code:'storage_infrastructure_report_endpoint_identity'});
 await assert.rejects(infrastructureAdminEndpoint(uri,55432,async()=>{throw Object.assign(Error('lookup failed'),{code:'ENOTFOUND'});}),{code:'ENOTFOUND'});
});
const bootstrap=()=>({version:1,businessImage:'registry.invalid/business@sha256:'+'a'.repeat(64),infrastructureImage:'registry.invalid/infra@sha256:'+'b'.repeat(64),sourceImageId:'sha256:'+'c'.repeat(64),publicOrigin:'http://harbor.invalid:8092',deviceOrigin:'https://device.invalid:8093',databasePort:55432,publisherDatabase:null});
function container(){const b=bootstrap();return {Id:'d'.repeat(64),Image:'sha256:'+'e'.repeat(64),Config:{Image:b.infrastructureImage,Entrypoint:['/usr/local/bin/harbor-postgres-infra'],Cmd:[],Labels:{'com.harbor.bootstrap-owner':'f'.repeat(32),'com.aistrong.project':'harbor-local-gateway-infra'}},State:{Running:true,Restarting:false,Paused:false,StartedAt:'2026-11-05T00:00:00Z'},HostConfig:{NetworkMode:'harbor-local-gateway-postgres-infra',PortBindings:{'5432/tcp':[{HostIp:'127.0.0.1',HostPort:'55432'}]}},Mounts:[[root+'/db',root+'/db',true],[root+'/image-volume','/var/lib/postgresql',false],[root+'/pg-tls','/run/harbor-pg-tls',false],[root+'/control','/run/harbor-infra-control',false],[root+'/report-pg.key','/run/harbor-report.key',false],[root+'/postgres-password','/run/harbor-postgres-password',false]].map(([Source,Destination,RW])=>({Type:'bind',Source,Destination,RW}))};}
function signed(key,at=1000,changes={}){const payload={version:1,source:'harbor.postgres.filesystem.v1',database:target.database,cluster:target.cluster,sampled_at:at,expires_at:at+15000,coverage:{data:true,wal:true,relations:1},samples:[{total:100,free:90}],...changes};return {payload,signature:createHmac('sha256',key).update('harbor-recording-storage-report-v1\0').update(JSON.stringify(payload)).digest('hex')};}
function storeFixture(records=[]){let reads=0;const state={identity:{database:target.database,database_oid:target.databaseOid,cluster:target.cluster,at:1000,data_directory:control().pgDataDir,postmaster:'epoch'},activation:{database_oid:target.databaseOid,state:'pending'},schema};return {state,verifySchema:async()=>{},all:async()=>state.schema,get:async sql=>{if(sql.startsWith('SELECT current_database()'))return {...state.identity};if(sql.includes('harbor_operator.activation'))return state.activation;if(sql.includes('recording_storage_reports'))return records[Math.min(reads++,records.length-1)];assert.fail('unexpected query');}};}

test('only explicit exact installed mode enables management and copied JSON is never a root capability',async()=>{
 assert.equal(infrastructurePublisherOption(undefined),undefined);assert.deepEqual(infrastructurePublisherOption({mode:'installed-bootstrap-v1'}),{mode:'installed-bootstrap-v1'});
 for(const value of [null,true,{}, {mode:'guess'},{mode:'installed-bootstrap-v1',path:'/tmp/arbitrary'}])assert.throws(()=>infrastructurePublisherOption(value),{code:'storage_infrastructure_report_configuration'});
 await assert.rejects(bindInstalledInfrastructureReport({context:{configuration:{infrastructurePublisher:{mode:'installed-bootstrap-v1'}}},target,deadline:performance.now()+1000}),{code:'storage_operator_context_required'});
});
test('control plan changes only database and never replaces a different binding',()=>{
 const original=control(),next=infrastructurePublisherPlan(original,target);assert.deepEqual(next,{...original,database:target.database});assert.equal(original.database,null);assert.deepEqual(infrastructurePublisherPlan(next,target),next);
 for(const value of [{...original,database:'other'}, {...original,port:5433},{...original,keyFile:'/other'},{...original,pgDataDir:'/other'},{...original,socketDirectory:'/other'},{...original,extra:true}])assert.throws(()=>infrastructurePublisherPlan(value,target),{code:'storage_infrastructure_report_control_configuration'});
});
test('bootstrap owner fingerprint matches the actual Python producer and excludes only database binding',()=>{
 const b=bootstrap(),result=spawnSync('python3',['-I','-c',"import sys,json,hashlib;v=json.load(sys.stdin);print(hashlib.sha256(json.dumps({k:x for k,x in v.items() if k!='publisherDatabase'},sort_keys=True).encode()).hexdigest())"],{input:JSON.stringify(b),encoding:'utf8'});assert.equal(result.status,0);assert.equal(infrastructureBootstrapFingerprint(b),result.stdout.trim());assert.equal(infrastructureBootstrapFingerprint({...b,publisherDatabase:'another'}),infrastructureBootstrapFingerprint(b));assert.notEqual(infrastructureBootstrapFingerprint({...b,databasePort:55433}),infrastructureBootstrapFingerprint(b));
});
test('actual infra identity requires owner image network and mandatory source mounts while allowing archive additions',()=>{
 const b=bootstrap(),row=container(),state={owner:'f'.repeat(32)};assert.equal(infrastructureContainerIdentity(row,b,state,row.Image).id,row.Id);
 row.Mounts.push(...[['/etc/harbor-storage-archive.json',false],[root+'/archive.key',false],[root+'/archive',true]].map(([path,RW])=>({Type:'bind',Source:path,Destination:path,RW})));infrastructureContainerIdentity(row,b,state,row.Image);
 for(const mutate of [v=>{v.Config.Labels['com.harbor.bootstrap-owner']='other';},v=>{v.State.Running=false;},v=>{v.Mounts[0].Source='/other';},v=>{v.Mounts[3].RW=true;},v=>{v.Mounts.push({...v.Mounts[0]});},v=>{v.Mounts.push({Type:'bind',Source:'/outside',Destination:'/opt/harbor-storage-report',RW:false});},v=>{v.Mounts.push({Type:'bind',Source:'/outside',Destination:'/run/harbor-infra-control/publisher.json',RW:false});},v=>{v.Config.Env=['NODE_OPTIONS=--import=/tmp/untrusted.mjs'];},v=>{v.Mounts.at(-3).RW=true;}]){const changed=structuredClone(row);mutate(changed);assert.throws(()=>infrastructureContainerIdentity(changed,b,state,row.Image));}
});
test('target verifies actual cluster, database OID, pending activation and full schema before binding',async()=>{
 const store=storeFixture();assert.equal((await infrastructureTargetIdentity(store,target)).database_oid,target.databaseOid);
 for(const [key,value] of [['cluster','other'],['database_oid','100'],['database','other']]){const original=store.state.identity[key];store.state.identity[key]=value;await assert.rejects(infrastructureTargetIdentity(store,target),{code:'storage_infrastructure_report_target_identity'});store.state.identity[key]=original;}
 store.state.activation.state='active';await assert.rejects(infrastructureTargetIdentity(store,target),{code:'storage_infrastructure_report_target_activation'});store.state.activation.state='pending';store.state.schema=[];await assert.rejects(infrastructureTargetIdentity(store,target),{code:'storage_infrastructure_report_target_schema'});
});
test('fresh same-target HMAC report is required and an older sample cannot satisfy the new binding',async()=>{
 const key=randomBytes(32),store=storeFixture([signed(key,999),signed(key)]);const result=await waitInfrastructureTargetReport({store,key,target,minimumSample:1000,deadline:performance.now()+2000});assert.equal(result.sampledAt,1000);
});
test('forged or foreign reports never pass; cancellation exits pending wait and expired deadline does not query',async()=>{
 const key=randomBytes(32),abort=new AbortController(),bad=signed(key);bad.signature='0'.repeat(64);const store=storeFixture([bad]),get=store.get;store.get=async sql=>{const value=await get(sql);if(sql.includes('recording_storage_reports'))abort.abort(Error('synthetic-cancel'));return value;};await assert.rejects(waitInfrastructureTargetReport({store,key,target,minimumSample:1000,deadline:performance.now()+1000,signal:abort.signal}),/synthetic-cancel/);
 await assert.rejects(waitInfrastructureTargetReport({store:{get:()=>assert.fail('must not query')},key,target,minimumSample:1000,deadline:performance.now()-1}),{code:'storage_infrastructure_report_timeout'});
 const foreign=storeFixture([signed(key,1000,{database:'other'})]),controller=new AbortController(),foreignGet=foreign.get;foreign.get=async sql=>{const result=await foreignGet(sql);if(sql.includes('recording_storage_reports'))controller.abort(Error('foreign-not-accepted'));return result;};await assert.rejects(waitInfrastructureTargetReport({store:foreign,key,target,minimumSample:1000,deadline:performance.now()+1000,signal:controller.signal}),/foreign-not-accepted/);
});
test('OID replacement during final report verification is rejected',async()=>{
 const key=randomBytes(32),store=storeFixture([signed(key)]),get=store.get;let calls=0;store.get=async sql=>{if(sql.startsWith('SELECT current_database()')&&++calls===2)store.state.identity.database_oid='100';return get(sql);};await assert.rejects(waitInfrastructureTargetReport({store,key,target,minimumSample:1000,deadline:performance.now()+1000}),{code:'storage_infrastructure_report_target_identity'});
});

test('root atomic control replacement preserves fields, rejects symlinks and detects concurrent rewrite',{skip:process.getuid()!==0?'requires isolated root file mechanism':false},async t=>{
 // 只给本模块私有原子函数增加测试导出；源码语义及生产 authority 入口不改、不绕过。
 const source=(await readFile(new URL('../storage-infrastructure-report-binding.mjs',import.meta.url),'utf8')).replace(/from '(\.\/[^']+)'/g,(_,path)=>"from '"+new URL('../'+path.slice(2),import.meta.url).href+"'")+ '\nexport {replaceControl};';
 const {replaceControl}=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64')),directory=await mkdtemp(join(tmpdir(),'harbor-publisher-control-'));t.after(()=>rm(directory,{recursive:true,force:true}));const path=join(directory,'publisher.json'),original=control(),next=infrastructurePublisherPlan(original,target);await writeFile(path,JSON.stringify(original),{mode:0o644});await chmod(path,0o644);await replaceControl(directory,original,next,async()=>{});assert.deepEqual(JSON.parse(await readFile(path)),next);assert.equal((await lstat(path)).mode&0o777,0o644);
 await assert.rejects(replaceControl(directory,original,next,async()=>{}),{code:'storage_infrastructure_report_control_changed'});
 await writeFile(path,JSON.stringify(original));const concurrent={...original,database:'other'};await assert.rejects(replaceControl(directory,original,next,async()=>{await writeFile(path,JSON.stringify(concurrent));}),{code:'storage_infrastructure_report_control_changed'});assert.deepEqual(JSON.parse(await readFile(path)),concurrent);await rm(path);const victim=join(directory,'victim');await writeFile(victim,'preserved');await symlink(victim,path);await assert.rejects(replaceControl(directory,original,next,async()=>{}));assert.equal(await readFile(victim,'utf8'),'preserved');
});

test('real private PG18 target OID and schema wait for its actual publisher report',{skip:process.getuid()===0||!process.env.HARBOR_TEST_PG_BIN_DIR?'requires unprivileged private PG18 laboratory tools':false,timeout:30000},async()=>{
 const {createPitrRehearsal}=await import('../postgres-recovery.mjs'),{PostgresStore}=await import('../postgres-store.mjs'),{publishRecordingStorageReport,recordingDatabaseIdentity}=await import('../postgres-storage-report.mjs');
 const directory=await mkdtemp(join(tmpdir(),'harbor-infra-target-report-')),key=randomBytes(32),keyFile=join(directory,'report.key'),controller=new AbortController();let rehearsal,store,pending;
 try{
  rehearsal=await createPitrRehearsal({directory,key:randomBytes(32),pgBinDir:process.env.HARBOR_TEST_PG_BIN_DIR,libraryDirectory:process.env.HARBOR_TEST_PG_LIBRARY_DIR,port:55471});store=new PostgresStore({...rehearsal.adminOptions,poolSize:2});await store.migrate();const identity=await recordingDatabaseIdentity(store),oid=await store.get('SELECT oid::text oid FROM pg_database WHERE datname=current_database()'),realTarget={database:identity.database,databaseOid:oid.oid,cluster:identity.cluster,schemaDigest:hash(JSON.stringify(await store.all('SELECT version,checksum FROM harbor.schema_migrations ORDER BY version')))};
  await store.query('CREATE SCHEMA harbor_operator');await store.query('CREATE TABLE harbor_operator.activation(singleton boolean PRIMARY KEY,database_oid text,state text)');await store.run("INSERT INTO harbor_operator.activation VALUES(true,$1,'pending')",oid.oid);await writeFile(keyFile,key,{mode:0o600});
  await infrastructureTargetIdentity(store,realTarget);await assert.rejects(infrastructureTargetIdentity(store,{...realTarget,databaseOid:String(Number(oid.oid)+1)}),{code:'storage_infrastructure_report_target_identity'});
  let reportRead;const emptyRead=new Promise(resolve=>reportRead=resolve),get=store.get.bind(store);store.get=async(sql,...args)=>{const value=await get(sql,...args);if(sql.includes('FROM harbor.recording_storage_reports')&&!value)reportRead();return value;};
  pending=waitInfrastructureTargetReport({store,key,target:realTarget,minimumSample:identity.at,deadline:performance.now()+10000,signal:controller.signal});void pending.catch(()=>{});await emptyRead;await publishRecordingStorageReport(store,{pgDataDir:rehearsal.primary,keyFile});const proof=await pending;assert.ok(proof.sampledAt>=identity.at);assert.equal(proof.identity.database_oid,oid.oid);store.get=get;
 }finally{controller.abort();await pending?.catch(()=>{});await store?.close();await rehearsal?.close();await rm(directory,{recursive:true,force:true});}
});


test('binding uses a kernel lock: concurrent publisher mutation is refused and SIGKILL permits the same target retry',{skip:process.platform!=='linux',timeout:10000},async t=>{
 const source=(await readFile(new URL('../storage-infrastructure-report-binding.mjs',import.meta.url),'utf8')).replace(/from '(\.\/[^']+)'/g,(_,path)=>"from '"+new URL('../'+path.slice(2),import.meta.url).href+"'")+ '\nexport {acquirePublisherBindingLock};';
 const module='data:text/javascript;base64,'+Buffer.from(source).toString('base64'),{acquirePublisherBindingLock}=await import(module),directory=await mkdtemp(join(tmpdir(),'harbor-publisher-lock-'));
 const code=`const {acquirePublisherBindingLock}=await import(${JSON.stringify(module)});await acquirePublisherBindingLock(${JSON.stringify(directory)});process.send('locked');`;
 const child=spawn(process.execPath,['--input-type=module','-e',code],{stdio:['ignore','ignore','ignore','ipc']});t.after(async()=>{if(child.exitCode===null&&child.signalCode===null){child.kill('SIGKILL');await once(child,'exit');}await rm(directory,{recursive:true,force:true});});
 const [ready]=await once(child,'message',{signal:AbortSignal.timeout(5000)});assert.equal(ready,'locked');await assert.rejects(acquirePublisherBindingLock(directory),{code:'storage_infrastructure_report_lock_busy'});
 const exited=once(child,'exit');child.kill('SIGKILL');await exited;const recovered=await acquirePublisherBindingLock(directory);await recovered.release();const repeated=await acquirePublisherBindingLock(directory);await repeated.release();
});


test('source observer rejects expired or cancelled calls before spawning and bounds a real slow child by shared remaining time',{timeout:5000},async()=>{
 const source=(await readFile(new URL('../storage-operator-context.mjs',import.meta.url),'utf8')).replace(/from '(\.\/[^']+)'/g,(_,path)=>"from '"+new URL('../'+path.slice(2),import.meta.url).href+"'")+ '\nexport {observeCommand};';
 const {observeCommand}=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
 const command=observeCommand({deadline:performance.now()-1});assert.throws(()=>command('/must-not-spawn',[]),{code:'storage_operator_private_preflight_timeout'});
 const controller=new AbortController();controller.abort(Error('observer-cancelled'));assert.throws(()=>observeCommand({signal:controller.signal,deadline:performance.now()+5000})('/must-not-spawn',[]),/observer-cancelled/);
 const began=performance.now();await assert.rejects(observeCommand({deadline:began+80})('/usr/bin/python3',['-I','-c','import time;time.sleep(5)'],{timeout:30000,maxBuffer:1024}),error=>error.killed===true);assert.ok(performance.now()-began<2000);
});
