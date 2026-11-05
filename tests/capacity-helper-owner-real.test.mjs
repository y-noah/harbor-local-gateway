import test from 'node:test';
import assert from 'node:assert/strict';
import {createTarget} from '../tools/capacity/gateway-adapter.mjs';
import * as helper from '../tools/capacity/formal-helper-proof.mjs';
import * as clock from '../tools/capacity/formal-helper-clock-proof.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('helper and helper-clock producers reject fake factory bindings and copied capabilities',async()=>{
 const {runCapacityFactoryInternalObservation}=await import('../tools/capacity/observation-adapter.mjs');for(const authority of [{},Object.freeze({phase:'restore'}),true,null])await assert.rejects(runCapacityFactoryInternalObservation(authority,()=>{}),/internal_observation_not_owned/);
 for(const producer of [helper,clock]){await assert.rejects(producer.prepareCapacityProof(Object.freeze({phase:'pre-seed'}),{phase:'pre-seed'}),/proof_not_owned/);await assert.rejects(producer.inspectCapacityProof(Object.freeze({}),Object.freeze({phase:'final'}),{phase:'final'}),/capability_not_owned/);}
});
test('actual pre-seed CNG catalog challenge leaves no model rows or device restriction before final model enrollment',{skip:process.env.HARBOR_TEST_WINDOWS_DEVICE!=='1'||!binDirectory?'需真实Windows/CNG/WSL和独立PG18':false,timeout:180000},async()=>{
 let target,seen=false,primary;
 try{
  target=await createTarget({employeeCount:1,windowsHelper:true,binDirectory,libraryDirectory,onSetup:async({store})=>{
   const devices=await store.get('SELECT (SELECT COUNT(*) FROM harbor.employee_devices) devices,(SELECT COUNT(*) FROM harbor.device_credentials) credentials,(SELECT COUNT(*) FROM harbor.device_invitations) invitations'),marker=await store.get('SELECT metadata_cursor,body_cursor,(SELECT COUNT(*) FROM harbor.requests) requests,(SELECT COUNT(*) FROM harbor.recordings) recordings FROM harbor_capacity.fixture WHERE singleton=1');assert.deepEqual(devices,{devices:0,credentials:0,invitations:0});assert.equal(marker.requests,marker.metadata_cursor);assert.equal(marker.recordings,marker.body_cursor);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.keys WHERE device_ids IS NOT NULL')).n,0);seen=true;
  }});
  assert.equal(seen,true);const evidence=(await target.collectEvidence()).adapter.factory.helper;assert.equal(evidence.formalQualified,false);assert.ok(evidence.phases.some(row=>row.phase==='pre-seed'&&row.ready===true&&row.actors===1));assert.ok(evidence.phases.some(row=>row.phase==='pre-seed'&&row.retired===true&&row.preseedModelRows===0));assert.ok(evidence.phases.some(row=>row.phase==='final'&&row.ready===true&&row.actors===1));const actor=target.employeeActors[0],device=await target.store.get('SELECT state,verified_request_at FROM harbor.employee_devices WHERE id=$1 AND member_id=$2',actor.deviceId,actor.memberId);assert.equal(device.state,'approved');assert.ok(device.verified_request_at);assert.equal((await target.store.get('SELECT device_ids FROM harbor.keys WHERE id=$1',actor.keyId)).device_ids,null);
 }catch(error){primary=error;throw error;}finally{try{await target?.close();}catch(error){if(!primary)throw error;}}
});

test('a failed pre-seed CNG removal can retry while its private database remains available',{skip:process.env.HARBOR_TEST_WINDOWS_DEVICE!=='1'||!binDirectory?'需真实Windows/CNG/WSL和独立PG18':false,timeout:180000},async()=>{
 const {mkdtemp,readFile,writeFile,readdir,rm}=await import('node:fs/promises'),{join}=await import('node:path'),{tmpdir}=await import('node:os'),{execFile}=await import('node:child_process'),{promisify}=await import('node:util'),execute=promisify(execFile),root=await mkdtemp(join(tmpdir(),'harbor-helper-cleanup-retry-'));let state,original,failure,retried=false;
 try{
  await assert.rejects(createTarget({employeeCount:1,windowsHelper:true,binDirectory,libraryDirectory,storageRoot:root,onProgress:async event=>{
   if(event.kind!=='preparation-ready')return;for(const pid of (await readFile('/proc/self/task/'+process.pid+'/children','utf8')).trim().split(/\s+/).filter(Boolean)){let args;try{args=(await readFile('/proc/'+pid+'/cmdline')).toString().split('\0');}catch{continue;}if(!args.includes('serve')||!args.some(value=>value.startsWith(root+'/')&&value.endsWith('/helper-proof/HarborDevice.exe')))continue;state=(await execute('wslpath',['-u',args[args.indexOf('--state')+1]])).stdout.trim();original=await readFile(state);await writeFile(state,'{}');throw Error('owned_helper_cleanup_fault');}throw Error('owned_helper_test_source_missing');
  }}),error=>{failure=error;return error.message==='capacity_adapter_cleanup_failed'&&typeof error.cleanup==='function';});
  assert.ok(state&&original);assert.ok((await readdir(root)).length>0,'cleanup failure must preserve its private database');await writeFile(state,original);await failure.cleanup();retried=true;assert.deepEqual(await readdir(root),[]);await assert.rejects(readFile(state),error=>error.code==='ENOENT');
 }finally{if(!retried&&state&&original)try{await writeFile(state,original);}catch{}if(failure?.cleanup)await failure.cleanup();if((await readdir(root)).length===0)await rm(root,{recursive:true,force:true});}
});

test('pre-seed cleanup confirms exact device deletion after a lost successful commit receipt',{skip:process.env.HARBOR_TEST_WINDOWS_DEVICE!=='1'||!binDirectory?'需真实Windows/CNG/WSL和独立PG18':false,timeout:180000},async()=>{
 const {PostgresStore}=await import('../postgres-store.mjs'),{mkdtemp,readdir,rm}=await import('node:fs/promises'),{join}=await import('node:path'),{tmpdir}=await import('node:os'),root=await mkdtemp(join(tmpdir(),'harbor-helper-commit-receipt-')),previousRun=PostgresStore.prototype.run,previousTransaction=PostgresStore.prototype.transaction,deleted=new WeakSet();let injected=false,failure;
 PostgresStore.prototype.run=async function(sql,...args){const result=await previousRun.call(this,sql,...args);if(sql==='DELETE FROM harbor.employee_devices WHERE id=ANY($1::text[])')deleted.add(this);return result;};
 PostgresStore.prototype.transaction=async function(...args){const result=await previousTransaction.apply(this,args);if(!injected&&deleted.has(this)){injected=true;throw Error('synthetic_successful_commit_receipt_lost');}return result;};
 try{
  await assert.rejects(createTarget({employeeCount:1,windowsHelper:true,binDirectory,libraryDirectory,storageRoot:root,onProgress:event=>{if(event.kind==='preparation-ready')throw Error('synthetic_cleanup_after_preseed');}}),error=>{failure=error;return error.message==='capacity_adapter_cleanup_failed'&&typeof error.cleanup==='function';});assert.equal(injected,true);assert.ok((await readdir(root)).length>0);await failure.cleanup();assert.deepEqual(await readdir(root),[]);
 }finally{PostgresStore.prototype.run=previousRun;PostgresStore.prototype.transaction=previousTransaction;if(failure?.cleanup)await failure.cleanup();if((await readdir(root)).length===0)await rm(root,{recursive:true,force:true});}
});

test('paused private PostgreSQL cannot block physical CNG cleanup and SQL cleanup remains retryable',{skip:process.env.HARBOR_TEST_WINDOWS_DEVICE!=='1'||!binDirectory?'需真实Windows/CNG/WSL和独立PG18':false,timeout:180000},async()=>{
 const {mkdtemp,readFile,readdir,rm}=await import('node:fs/promises'),{join}=await import('node:path'),{tmpdir}=await import('node:os'),{execFile}=await import('node:child_process'),{promisify}=await import('node:util'),execute=promisify(execFile),root=await mkdtemp(join(tmpdir(),'harbor-helper-paused-pg-'));let state,failure,started,resumed=false;const paused=[];
 const identity=async pid=>{const raw=await readFile('/proc/'+pid+'/stat','utf8');return raw.slice(raw.lastIndexOf(')')+2).trim().split(/\s+/)[19];};
 const resume=async()=>{for(const entry of paused)try{if(await identity(entry.pid)===entry.started)process.kill(entry.pid,'SIGCONT');}catch(error){if(error.code!=='ENOENT'&&error.code!=='ESRCH')throw error;}resumed=true;};
 try{
  await assert.rejects(createTarget({employeeCount:1,windowsHelper:true,binDirectory,libraryDirectory,storageRoot:root,onProgress:async event=>{
   if(event.kind!=='preparation-ready')return;let database;
   for(const value of (await readFile('/proc/self/task/'+process.pid+'/children','utf8')).trim().split(/\s+/).filter(Boolean)){const pid=Number(value);let args;try{args=(await readFile('/proc/'+pid+'/cmdline')).toString().split('\0');}catch{continue;}if(args[0]===join(binDirectory,'postgres')&&args[args.indexOf('-D')+1]?.startsWith(root+'/'))database=pid;if(args.includes('serve')&&args.some(value=>value.startsWith(root+'/')&&value.endsWith('/helper-proof/HarborDevice.exe')))state=(await execute('wslpath',['-u',args[args.indexOf('--state')+1]])).stdout.trim();}
   assert.ok(database&&state,'only this test owns the database and helper');await readFile(state);for(const pid of [database,...(await readFile('/proc/'+database+'/task/'+database+'/children','utf8')).trim().split(/\s+/).filter(Boolean).map(Number)]){paused.push({pid,started:await identity(pid)});process.kill(pid,'SIGSTOP');}started=performance.now();throw Error('owned_helper_paused_database');
  }}),error=>{failure=error;return error.message==='capacity_adapter_cleanup_failed'&&typeof error.cleanup==='function';});
  assert.ok(performance.now()-started<60000,'existing owner close budget bounds SQL failure without server recovery');assert.ok(paused.length>1);assert.equal(resumed,false);await assert.rejects(readFile(state),error=>error.code==='ENOENT');assert.ok((await readdir(root)).length>0,'database stays available for exact cleanup retry');await resume();await failure.cleanup();assert.deepEqual(await readdir(root),[]);
 }finally{await resume();if(failure?.cleanup)await failure.cleanup();if((await readdir(root)).length===0)await rm(root,{recursive:true,force:true});}
});

test('compiler registration budget tracks live ownership rather than 128 historical builds',{skip:process.env.HARBOR_TEST_WINDOWS_DEVICE!=='1'?'需实际Windows编译器':false,timeout:180000},async()=>{
 const {compileCapacityWindowsHelper,inspectCompiledCapacityWindowsHelper,releaseCompiledCapacityWindowsHelper}=await import('../tools/capacity/device-adapter.mjs'),{mkdtemp,rm}=await import('node:fs/promises'),{join}=await import('node:path'),{tmpdir}=await import('node:os'),root=await mkdtemp(join(tmpdir(),'harbor-helper-compile-lifecycle-'));let path;
 try{for(let index=0;index<129;index++){path=await compileCapacityWindowsHelper(root);assert.ok((await inspectCompiledCapacityWindowsHelper(path)).binarySha256);releaseCompiledCapacityWindowsHelper(path);await assert.rejects(inspectCompiledCapacityWindowsHelper(path),/binary_changed/);}}finally{if(path)releaseCompiledCapacityWindowsHelper(path);await rm(root,{recursive:true,force:true});}
});

test('actual Windows CNG and catalog access run directly from a root-owned readonly ordinary WSL path',{skip:process.env.HARBOR_TEST_WINDOWS_DEVICE!=='1'||!binDirectory?'需WSL root诊断入口、Windows/CNG和独立PG18':false,timeout:180000},async()=>{
 const {compileCapacityWindowsHelper,enrollCapacityWindowsDevice,inspectCompiledCapacityWindowsHelper,capacityDeviceDeliveryBinding,releaseCompiledCapacityWindowsHelper}=await import('../tools/capacity/device-adapter.mjs'),{capacityAgent}=await import('../tools/capacity/http-client.mjs'),{mkdtemp,readFile,writeFile,lstat,chmod}=await import('node:fs/promises'),{join}=await import('node:path'),{tmpdir}=await import('node:os'),{execFile}=await import('node:child_process'),{promisify}=await import('node:util'),{createHash}=await import('node:crypto'),execute=promisify(execFile),root=await mkdtemp(join(tmpdir(),'harbor-helper-root-image-')),wsl='/mnt/c/Windows/System32/wsl.exe';let target,actor,agent,helper,hashes,externalCleanup,sealed=false,actorClosed=false,primary;
 try{
  assert.equal((await execute(wsl,['--user','root','--exec','/usr/bin/id','-u'])).stdout.trim(),'0');target=await createTarget({employeeCount:1,binDirectory,libraryDirectory});const origin=target.gateway.ready.tlsOrigin;helper=await compileCapacityWindowsHelper(root);hashes={'HarborDevice.exe':createHash('sha256').update(await readFile(helper)).digest('hex')};const revision=(await execute('git',['rev-parse','HEAD'])).stdout.trim(),compiled=await inspectCompiledCapacityWindowsHelper(helper);
  for(const name of ['HarborDevice.cs','CapacityDeliveryObservation.cs']){const relative='clients/harbor-device/'+name,source=(await execute('git',['show',revision+':'+relative],{maxBuffer:1048576})).stdout;hashes[name]=createHash('sha256').update(source).digest('hex');assert.equal(compiled.sources[relative],hashes[name]);await writeFile(join(root,name),source,{mode:0o600});}await writeFile(join(root,'ca.pem'),target.certificateAuthority,{mode:0o600});hashes['ca.pem']=createHash('sha256').update(await readFile(join(root,'ca.pem'))).digest('hex');
  const seal=`import pathlib,os,sys,json,hashlib,stat
p=pathlib.Path(sys.argv[1]);expected=json.loads(sys.argv[3]);uid=int(sys.argv[2])
assert p.parent==pathlib.Path('/tmp') and p.name.startswith('harbor-helper-root-image-') and not p.is_symlink()
d=os.open(p,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
try:
 info=os.fstat(d);assert info.st_uid==uid and set(os.listdir(d))==set(expected)
 os.fchown(d,0,0);os.fchmod(d,0o555)
 assert (p.lstat().st_dev,p.lstat().st_ino)==(info.st_dev,info.st_ino)
 for name,digest in expected.items():
  f=os.open(name,os.O_RDONLY|os.O_NOFOLLOW,dir_fd=d)
  try:
   s=os.fstat(f);assert stat.S_ISREG(s.st_mode) and s.st_uid==uid and s.st_size<=16*1024**2
   data=b''
   while True:
    chunk=os.read(f,65536)
    if not chunk:break
    data+=chunk;assert len(data)<=16*1024**2
   assert hashlib.sha256(data).hexdigest()==digest
  finally:os.close(f)
  output=os.open(name+'.sealed',os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=d)
  try:
   with os.fdopen(os.dup(output),'wb') as copy:copy.write(data)
   os.fchmod(output,0o555 if name=='HarborDevice.exe' else 0o444)
  finally:os.close(output)
  os.rename(name+'.sealed',name,src_dir_fd=d,dst_dir_fd=d)
finally:os.close(d)
print('sealed')`;
  await execute(wsl,['--user','root','--exec','/usr/bin/python3','-c',seal,root,String(process.getuid()),JSON.stringify(hashes)],{timeout:15000,maxBuffer:4096});sealed=true;assert.equal((await lstat(helper)).uid,0);assert.equal((await lstat(root)).uid,0);assert.equal((await lstat(root)).mode&0o222,0);await assert.rejects(writeFile(helper,Buffer.from('replacement')),error=>['EACCES','EPERM','EROFS'].includes(error.code));await assert.rejects(chmod(helper,0o755),error=>error.code==='EPERM');
  agent=capacityAgent(origin,{ca:target.certificateAuthority});const before=(await target.store.get('SELECT COUNT(*) n FROM harbor.requests')).n;actor=await enrollCapacityWindowsDevice({directory:root,origin,ca:target.certificateAuthority,adminHeaders:target.adminActors[0].headers,adminAgent:agent,actor:target.employeeActors[0],runtime:target.laboratory.runtime,helper,observeDelivery:true,healthProof:'catalog',registerCleanup:cleanup=>externalCleanup=cleanup});assert.equal(actor.realHelper,true);assert.equal((await capacityDeviceDeliveryBinding(actor).inspectCredential()).ready,true);const result=await fetch(actor.origin+'/v1/models',{headers:actor.headers});assert.equal(result.status,200);assert.ok((await result.json()).models.length>0);assert.equal((await target.store.get('SELECT COUNT(*) n FROM harbor.requests')).n,before);
  const expected=(await execute('wslpath',['-w',helper])).stdout.trim(),quote=value=>"'"+value.replaceAll("'","''")+"'",script='$rows=@(Get-CimInstance Win32_Process -Filter "Name=\'HarborDevice.exe\'"|Where-Object{$_.ExecutablePath -ieq '+quote(expected)+'});[pscustomobject]@{count=$rows.Count;exactImage=($rows.Count -eq 1 -and $rows[0].ExecutablePath -ieq '+quote(expected)+')}|ConvertTo-Json -Compress',image=JSON.parse((await execute('/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe',['-NoProfile','-NonInteractive','-EncodedCommand',Buffer.from(script,'utf16le').toString('base64')],{timeout:15000,maxBuffer:4096})).stdout.trim());assert.deepEqual(image,{count:1,exactImage:true});assert.equal((await inspectCompiledCapacityWindowsHelper(helper)).sourceArtifact,null,'temporary root image is diagnostic, never the fixed formal artifact');
 }catch(error){primary=error;throw error;}finally{
  let cleanupError;try{await (actor?.close??externalCleanup)?.();actorClosed=true;}catch(error){cleanupError=error;}agent?.destroy();try{await target?.close();}catch(error){cleanupError??=error;}if(helper)releaseCompiledCapacityWindowsHelper(helper);
  if(actorClosed){const cleanup=`import pathlib,os,sys,json,hashlib,stat,shutil
p=pathlib.Path(sys.argv[1]);expected=json.loads(sys.argv[2]);assert p.parent==pathlib.Path('/tmp') and p.name.startswith('harbor-helper-root-image-') and not p.is_symlink()
assert set(x.name for x in p.iterdir())==set(expected)
for name,digest in expected.items():
 q=p/name;s=q.lstat();assert stat.S_ISREG(s.st_mode) and s.st_uid==0 and hashlib.sha256(q.read_bytes()).hexdigest()==digest
shutil.rmtree(p)
print('removed')`;if(sealed)await execute(wsl,['--user','root','--exec','/usr/bin/python3','-c',cleanup,root,JSON.stringify(hashes)],{timeout:15000,maxBuffer:4096});else await (await import('node:fs/promises')).rm(root,{recursive:true,force:true});}if(cleanupError&&!primary)throw cleanupError;
 }
});
