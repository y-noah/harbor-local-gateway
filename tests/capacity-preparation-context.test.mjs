import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {readFile,lstat,mkdtemp,rm,writeFile,chmod,cp,symlink} from 'node:fs/promises';
import {dirname,join} from 'node:path';
import {tmpdir} from 'node:os';
import {setTimeout as delay} from 'node:timers/promises';
import {createCapacityPreparationContext,inspectCapacityPreparationReceipt,buildCapacityPreparationEntry} from '../tools/capacity/preparation-context.mjs';

const configuration={binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB},real={skip:!configuration.binDirectory?'需隔离PG18工具路径':false,timeout:120000};
const execute=promisify(execFile);
let artifactDirectory;
before(async()=>{if(configuration.binDirectory){artifactDirectory=await mkdtemp(join(tmpdir(),'harbor-preparation-tools-'));await buildCapacityPreparationEntry({directory:artifactDirectory});configuration.entryDirectory=artifactDirectory;}});
after(async()=>{if(artifactDirectory)await rm(artifactDirectory,{recursive:true});});
// Artifact validation precedes spawning. Observe the fixed exec role rather
// than assuming the first child already exists.
const observeOwner=`let ownerPid;const ownerDeadline=Date.now()+10000;while(!ownerPid){const children=(await readFile('/proc/self/task/'+process.pid+'/children','utf8')).trim().split(/\\s+/).filter(Boolean).map(Number);for(const pid of children)try{if((await readFile('/proc/'+pid+'/cmdline','utf8')).includes('--owned-capacity-preparation'))ownerPid=pid;}catch{}if(!ownerPid){if(Date.now()>ownerDeadline)throw Error('owner timeout');await new Promise(resolve=>setTimeout(resolve,5));}}process.send({ownerPid});`;
async function until(read,predicate=value=>!!value,timeoutMs=15000){const deadline=Date.now()+timeoutMs;for(;;){const value=await read();if(predicate(value))return value;if(Date.now()>deadline)throw Error('condition timeout');await delay(10);}}
async function alive(pid){try{const raw=await readFile('/proc/'+pid+'/stat','utf8');return raw.slice(raw.lastIndexOf(')')+2).split(' ')[0]!=='Z';}catch(error){if(error.code==='ENOENT')return false;throw error;}}

test('preparation rejects caller manifests, injected targets and formal claims before spawning',async()=>{
 for(const extra of [{manifest:{profile:'formal',verified:true}},{profile:'formal'},{gateway:{}},{sink:()=>{}},{formalDataAllowed:true}])await assert.rejects(createCapacityPreparationContext({...configuration,...extra}),/configuration/);
 await assert.rejects(inspectCapacityPreparationReceipt({},{received:true,formalDataAllowed:true}),/receipt_not_owned/);
});

test('real minimal PG/TLS context creates no request/body rows and issues only a bounded local receipt',real,async()=>{
 let context,initial;try{
  context=await createCapacityPreparationContext(configuration);initial=await context.inspect();assert.equal(initial.profile,'selfcheck');assert.equal(initial.requests,0);assert.equal(initial.recordings,0);assert.equal(initial.metadata_cursor,0);assert.equal(initial.body_cursor,0);assert.equal(initial.observedRequests,0);assert.equal(initial.formalDataAllowed,false);
  const receipt=await context.challenge(),proof=await inspectCapacityPreparationReceipt(context,receipt);assert.equal(proof.scope,'owned-loopback-preparation-only');assert.equal(proof.manifestHash,initial.manifestHash);assert.equal(proof.epoch,initial.epoch);assert.equal(proof.clientReady,true);assert.deepEqual(proof.wire,{started:1,ended:1,settled:1});assert.equal(proof.formalDataAllowed,false);assert.ok(proof.missing.includes('actual_lan_path_unproven'));assert.ok(proof.missing.includes('immutable_runtime_unproven'));
  for(const candidate of [{...receipt},{...proof},Object.freeze({})])await assert.rejects(inspectCapacityPreparationReceipt(context,candidate),/receipt_not_owned/);await assert.rejects(inspectCapacityPreparationReceipt({...context},receipt),/receipt_not_owned/);
  const final=await context.inspect();assert.equal(final.requests,0);assert.equal(final.recordings,0);assert.equal(final.observedRequests,1);assert.equal(final.metadata_cursor,0);assert.equal(final.body_cursor,0);
  await inspectCapacityPreparationReceipt(context,receipt,{consume:true});await assert.rejects(inspectCapacityPreparationReceipt(context,receipt,{consume:true}),/receipt_not_owned/);await assert.rejects(context.challenge(),/already_used/);
 }finally{await context?.close();}
 if(initial){for(const pid of Object.values(initial.pids))assert.equal(await alive(pid),false);await assert.rejects(lstat(initial.directory),error=>error.code==='ENOENT');assert.equal((await lstat(initial.entryDirectory)).isDirectory(),true,'prebuilt artifact belongs to its independent installation lifecycle');}
});

test('only one simultaneous challenge is sent and a receipt cannot move to another context',real,async()=>{
 let a,b;try{
  a=await createCapacityPreparationContext(configuration);b=await createCapacityPreparationContext(configuration);const first=a.challenge();await assert.rejects(a.challenge(),/already_used/);const receipt=await first;await assert.rejects(inspectCapacityPreparationReceipt(b,receipt),/receipt_not_owned/);assert.equal((await a.inspect()).observedRequests,1);assert.equal((await b.inspect()).observedRequests,0);
 }finally{await a?.close();await b?.close();}
});

for(const cancelled of [true,false])test(cancelled?'cancellation cannot issue a receipt and cannot retry':'missing gateway terminals time out without a receipt or retry',real,async()=>{
 let context,pid;try{
  context=await createCapacityPreparationContext(configuration);pid=(await context.inspect()).pids.gateway;process.kill(pid,'SIGSTOP');const controller=new AbortController(),pending=context.challenge({signal:controller.signal,timeoutMs:cancelled?10000:200});if(cancelled)controller.abort();await assert.rejects(pending);assert.equal(context.status().phase,'failed');await assert.rejects(context.challenge(),/already_used/);assert.equal((await context.inspect()).observedRequests,0);
 }finally{if(pid&&await alive(pid))process.kill(pid,'SIGCONT');await context?.close();}
});

test('gateway epoch loss invalidates a previously genuine receipt',real,async()=>{
 let context;try{context=await createCapacityPreparationContext(configuration);const receipt=await context.challenge(),initial=await context.inspect();process.kill(initial.pids.gateway,'SIGKILL');await until(async()=>!await alive(initial.pids.gateway));await assert.rejects(inspectCapacityPreparationReceipt(context,receipt));}
 finally{await context?.close();}
});

test('controller SIGKILL leaves owner long enough to stop actual PG and all observed descendants',real,async()=>{
 let controller,state;try{
  const source=`import{createCapacityPreparationContext}from'./tools/capacity/preparation-context.mjs';process.once('message',async configuration=>{try{const context=await createCapacityPreparationContext(configuration);process.send(await context.inspect());}catch{process.exitCode=1;process.disconnect();}});`;
  controller=spawn(process.execPath,['--input-type=module','-e',source],{cwd:process.cwd(),stdio:['ignore','ignore','ignore','ipc'],env:{...process.env,NODE_OPTIONS:''}});
  const ready=new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('controller readiness timeout')),60000);controller.once('message',value=>{clearTimeout(timer);resolve(value);});controller.once('error',reject);});controller.send(configuration);state=await ready;for(const pid of Object.values(state.pids))assert.equal(await alive(pid),true);
  controller.kill('SIGKILL');await new Promise(resolve=>controller.once('exit',resolve));await until(async()=>{for(const pid of Object.values(state.pids))if(await alive(pid))return false;return true;});await assert.rejects(lstat(state.directory),error=>error.code==='ENOENT');assert.equal((await lstat(state.entryDirectory)).isDirectory(),true);
 }finally{if(controller?.exitCode===null&&controller?.signalCode===null)controller.kill('SIGKILL');}
});

test('close aborts an unfinished challenge, resumes its owned source and waits actual PG exit',real,async()=>{
 let context,initial;try{
  context=await createCapacityPreparationContext(configuration);initial=await context.inspect();process.kill(initial.pids.gateway,'SIGSTOP');const challenge=context.challenge({timeoutMs:10000}),rejected=assert.rejects(challenge);await context.close();await rejected;assert.equal(context.status().phase,'closed');for(const pid of Object.values(initial.pids))assert.equal(await alive(pid),false);await assert.rejects(lstat(initial.directory),error=>error.code==='ENOENT');
 }finally{await context?.close();}
});

test('controller loss after real PG startup cleans a partially initialized context',real,async()=>{
 let controller,ownerPid,postgresPid,directory;try{
  const source=`import{readFile}from'node:fs/promises';import{createCapacityPreparationContext}from'./tools/capacity/preparation-context.mjs';process.once('message',async configuration=>{const pending=createCapacityPreparationContext(configuration);pending.catch(()=>{});${observeOwner}try{await pending;}catch{process.exitCode=1;process.disconnect();}});`;
  controller=spawn(process.execPath,['--input-type=module','-e',source],{cwd:process.cwd(),stdio:['ignore','ignore','ignore','ipc'],env:{...process.env,NODE_OPTIONS:''}});
  const observed=new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('owner readiness timeout')),10000);controller.once('message',value=>{clearTimeout(timer);resolve(value);});controller.once('error',reject);});controller.send(configuration);ownerPid=(await observed).ownerPid;assert.ok(Number.isSafeInteger(ownerPid));
  postgresPid=await until(async()=>{let children;try{children=(await readFile('/proc/'+ownerPid+'/task/'+ownerPid+'/children','utf8')).trim().split(/\s+/).filter(Boolean).map(Number);}catch{return null;}for(const pid of children)try{if((await readFile('/proc/'+pid+'/comm','utf8')).trim()==='postgres')return pid;}catch{}return null;});
  const args=(await readFile('/proc/'+postgresPid+'/cmdline','utf8')).split('\0'),dataIndex=args.indexOf('-D');assert.ok(dataIndex>=0);directory=dirname(args[dataIndex+1]);assert.match(directory,/\/harbor-capacity-[^/]+$/);
  controller.kill('SIGKILL');await new Promise(resolve=>controller.once('exit',resolve));await until(async()=>!await alive(ownerPid)&&!await alive(postgresPid));await assert.rejects(lstat(directory),error=>error.code==='ENOENT');
 }finally{if(controller?.exitCode===null&&controller?.signalCode===null)controller.kill('SIGKILL');}
});

test('receipt validation concurrent with close cannot publish a live receipt afterward',real,async()=>{
 let context;try{context=await createCapacityPreparationContext(configuration);const receipt=await context.challenge(),checking=inspectCapacityPreparationReceipt(context,receipt),rejected=assert.rejects(checking);await context.close();await rejected;}
 finally{await context?.close();}
});

for(const role of ['gateway','worker','database'])test('receipt inspection and consumption require responsive '+role,real,async()=>{
 let context,pid;try{
  context=await createCapacityPreparationContext({...configuration,livenessTimeoutMs:200});const receipt=await context.challenge(),state=await context.inspect();pid=state.pids[role];process.kill(role==='database'?-pid:pid,'SIGSTOP');await assert.rejects(inspectCapacityPreparationReceipt(context,receipt),/liveness_unproven/);await assert.rejects(inspectCapacityPreparationReceipt(context,receipt,{consume:true}),/liveness_unproven/);process.kill(role==='database'?-pid:pid,'SIGCONT');pid=null;assert.equal((await context.inspect()).observedRequests,1,'liveness must not replay the /ready challenge');
 }finally{if(pid&&await alive(pid))process.kill(role==='database'?-pid:pid,'SIGCONT');await context?.close();}
});

test('stopped PG process group and pending query cannot block close before actual PG cleanup',real,async()=>{
 let context,state;try{
  context=await createCapacityPreparationContext(configuration);state=await context.inspect();process.kill(-state.pids.database,'SIGSTOP');const reading=context.inspect();reading.catch(()=>{});await context.close();await reading.catch(()=>{});for(const pid of Object.values(state.pids))assert.equal(await alive(pid),false);await assert.rejects(lstat(state.directory),error=>error.code==='ENOENT');
 }finally{await context?.close();}
});

test('controller death with stopped PG group and pending query still removes PG and lab',real,async()=>{
 let controller,state;try{
  const source=`import{createCapacityPreparationContext}from'./tools/capacity/preparation-context.mjs';process.once('message',async configuration=>{try{const context=await createCapacityPreparationContext(configuration),state=await context.inspect();process.kill(-state.pids.database,'SIGSTOP');context.inspect().catch(()=>{});setImmediate(()=>process.send(state));}catch{process.exitCode=1;process.disconnect();}});`;
  controller=spawn(process.execPath,['--input-type=module','-e',source],{cwd:process.cwd(),stdio:['ignore','ignore','ignore','ipc'],env:{...process.env,NODE_OPTIONS:''}});const observed=new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('frozen PG readiness timeout')),60000);controller.once('message',value=>{clearTimeout(timer);resolve(value);});controller.once('error',reject);});controller.send(configuration);state=await observed;controller.kill('SIGKILL');await new Promise(resolve=>controller.once('exit',resolve));await until(async()=>{for(const pid of Object.values(state.pids))if(await alive(pid))return false;return true;});await assert.rejects(lstat(state.directory),error=>error.code==='ENOENT');
 }finally{if(controller?.exitCode===null&&controller?.signalCode===null)controller.kill('SIGKILL');}
});

for(const killController of [false,true])test('initialization with a stopped real PG backend cleans after '+(killController?'controller SIGKILL':'cancellation'),real,async()=>{
 let controller,ownerPid,postgresPid,backendPid,directory;const messages=[];try{
  const source=`import{readFile}from'node:fs/promises';import{createCapacityPreparationContext}from'./tools/capacity/preparation-context.mjs';const abort=new AbortController();process.on('message',async message=>{if(message.cancel){abort.abort();return;}const pending=createCapacityPreparationContext({...message,signal:abort.signal});pending.catch(()=>{});${observeOwner}try{await pending;process.send({ready:true});}catch{if(process.connected){process.send({cancelled:true});process.disconnect();}}});`;
  controller=spawn(process.execPath,['--input-type=module','-e',source],{cwd:process.cwd(),stdio:['ignore','ignore','ignore','ipc'],env:{...process.env,NODE_OPTIONS:''}});controller.on('message',value=>messages.push(value));controller.send(configuration);ownerPid=(await until(()=>messages.find(row=>row.ownerPid))).ownerPid;
  postgresPid=await until(async()=>{let children;try{children=(await readFile('/proc/'+ownerPid+'/task/'+ownerPid+'/children','utf8')).trim().split(/\s+/).filter(Boolean).map(Number);}catch{return null;}for(const pid of children)try{if((await readFile('/proc/'+pid+'/comm','utf8')).trim()==='postgres')return pid;}catch{}return null;});
  const args=(await readFile('/proc/'+postgresPid+'/cmdline','utf8')).split('\0');directory=dirname(args[args.indexOf('-D')+1]);
  backendPid=await until(async()=>{let children;try{children=(await readFile('/proc/'+postgresPid+'/task/'+postgresPid+'/children','utf8')).trim().split(/\s+/).filter(Boolean).map(Number);}catch{return null;}for(const pid of children)try{if((await readFile('/proc/'+pid+'/cmdline','utf8')).includes('harbor_capacity_'))return pid;}catch{}return null;});
  assert.equal(messages.some(row=>row.ready),false,'fixture backend exists while owner initialization is still unfinished');process.kill(backendPid,'SIGSTOP');
  if(killController)controller.kill('SIGKILL');else controller.send({cancel:true});await until(async()=>!await alive(ownerPid)&&!await alive(postgresPid)&&!await alive(backendPid));await assert.rejects(lstat(directory),error=>error.code==='ENOENT');if(!killController)await until(()=>messages.some(row=>row.cancelled));
 }finally{if(backendPid&&await alive(backendPid))process.kill(backendPid,'SIGCONT');if(controller?.exitCode===null&&controller?.signalCode===null)controller.kill('SIGKILL');}
});

test('native owner entry rejects arguments and keeps its binary private',real,async()=>{
 let context;try{context=await createCapacityPreparationContext(configuration);const directory=context.status().entryDirectory,binary=directory+'/entry';assert.equal((await lstat(directory)).mode&0o777,0o700);assert.equal((await lstat(binary)).mode&0o777,0o700);await assert.rejects(execute(binary,['/bin/sh']),error=>error.code===126);assert.equal((await context.inspect()).observedRequests,0);}
 finally{await context?.close();}
});

for(const killController of [false,true])test('subreaper owns stopped PG backends after postmaster dies, then '+(killController?'controller dies':'close runs'),real,async()=>{
 let context,controller,state;const backends=[];let reading;try{
  if(killController){
   const source=`import{readFile}from'node:fs/promises';import{createCapacityPreparationContext}from'./tools/capacity/preparation-context.mjs';process.once('message',async configuration=>{try{const context=await createCapacityPreparationContext(configuration),state=await context.inspect(),children=(await readFile('/proc/'+state.pids.database+'/task/'+state.pids.database+'/children','utf8')).trim().split(/\\s+/).filter(Boolean).map(Number),backends=[];for(const pid of children)try{if((await readFile('/proc/'+pid+'/cmdline','utf8')).includes('harbor_capacity_'))backends.push(pid);}catch{}for(const pid of backends)process.kill(pid,'SIGSTOP');context.inspect().catch(()=>{});process.kill(state.pids.database,'SIGKILL');process.send({...state,backends});}catch{process.exitCode=1;process.disconnect();}});`;
   controller=spawn(process.execPath,['--input-type=module','-e',source],{cwd:process.cwd(),stdio:['ignore','ignore','ignore','ipc'],env:{...process.env,NODE_OPTIONS:''}});const observed=new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('adoption readiness timeout')),60000);controller.once('message',value=>{clearTimeout(timer);resolve(value);});controller.once('error',reject);});controller.send(configuration);state=await observed;backends.push(...state.backends);
  }else{
   context=await createCapacityPreparationContext(configuration);state=await context.inspect();const children=(await readFile('/proc/'+state.pids.database+'/task/'+state.pids.database+'/children','utf8')).trim().split(/\s+/).filter(Boolean).map(Number);for(const pid of children)try{if((await readFile('/proc/'+pid+'/cmdline','utf8')).includes('harbor_capacity_'))backends.push(pid);}catch{}for(const pid of backends)process.kill(pid,'SIGSTOP');reading=context.inspect();reading.catch(()=>{});process.kill(state.pids.database,'SIGKILL');
  }
  assert.ok(backends.length>0);await until(async()=>{for(const pid of backends){const raw=await readFile('/proc/'+pid+'/stat','utf8'),fields=raw.slice(raw.lastIndexOf(')')+2).split(' ');if(Number(fields[1])!==state.pids.owner||fields[0]!=='T')return false;}return true;});
  if(killController){controller.kill('SIGKILL');await new Promise(resolve=>controller.once('exit',resolve));}else{await context.close();await reading.catch(()=>{});}
  await until(async()=>{for(const pid of [...Object.values(state.pids),...backends])if(await alive(pid))return false;return true;});await assert.rejects(lstat(state.directory),error=>error.code==='ENOENT');assert.equal((await lstat(state.entryDirectory)).isDirectory(),true);
 }finally{for(const pid of backends)if(await alive(pid))process.kill(pid,'SIGCONT');await context?.close();if(controller?.exitCode===null&&controller?.signalCode===null)controller.kill('SIGKILL');}
});

test('runtime refuses missing, partial, modified or unsafe prebuilt entry without compiling',real,async()=>{
 await assert.rejects(createCapacityPreparationContext({...configuration,entryDirectory:undefined}),/entry_unproven/);
 for(const mutation of ['empty','symlink','mode','directory-mode','binary','manifest']){
  const directory=await mkdtemp(join(tmpdir(),'harbor-preparation-invalid-'));try{
   if(mutation!=='empty')await cp(artifactDirectory,directory,{recursive:true});
   if(mutation==='symlink'){await rm(join(directory,'entry'));await symlink(join(artifactDirectory,'entry'),join(directory,'entry'));}
   if(mutation==='mode')await chmod(join(directory,'entry'),0o755);if(mutation==='directory-mode')await chmod(directory,0o755);
   if(mutation==='binary')await writeFile(join(directory,'entry'),'partial binary');if(mutation==='manifest'){const manifest=JSON.parse(await readFile(join(directory,'manifest.json'),'utf8'));manifest.moduleSha256='0'.repeat(64);await writeFile(join(directory,'manifest.json'),JSON.stringify(manifest));}
   await assert.rejects(createCapacityPreparationContext({...configuration,entryDirectory:directory}),/entry_unproven/);
  }finally{await rm(directory,{recursive:true});}
 }
});

for(const [role,consume] of [['gateway',false],['worker',true]])test('final kernel recheck rejects '+role+' stopped after its status reply',real,async()=>{
 let context,endpointPid,pid;try{
  context=await createCapacityPreparationContext(configuration);const receipt=await context.challenge(),state=await context.inspect();endpointPid=state.pids.endpoint;process.kill(endpointPid,'SIGSTOP');const checking=inspectCapacityPreparationReceipt(context,receipt,{consume}),rejected=assert.rejects(checking,/liveness_unproven/);await until(()=>context.inspect(),value=>value.livenessProbe.id>state.livenessProbe.id&&value.livenessProbe.gatewayReplied&&value.livenessProbe.workerReplied);pid=state.pids[role];process.kill(pid,'SIGSTOP');process.kill(endpointPid,'SIGCONT');endpointPid=null;await rejected;process.kill(pid,'SIGCONT');pid=null;assert.equal((await context.inspect()).observedRequests,1);
 }finally{if(endpointPid&&await alive(endpointPid))process.kill(endpointPid,'SIGCONT');if(pid&&await alive(pid))process.kill(pid,'SIGCONT');await context?.close();}
});

test('interrupted explicit build cannot become a runtime entry',real,async()=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-preparation-interrupted-build-'));let builder,compilerPid;try{
  const source=`import{buildCapacityPreparationEntry}from'./tools/capacity/preparation-context.mjs';process.once('message',async directory=>{try{await buildCapacityPreparationEntry({directory});process.send({done:true});}catch{process.exitCode=1;}process.disconnect();});`;builder=spawn(process.execPath,['--input-type=module','-e',source],{cwd:process.cwd(),stdio:['ignore','ignore','ignore','ipc'],env:{...process.env,NODE_OPTIONS:''}});builder.send(directory);
  compilerPid=await until(async()=>{let children;try{children=(await readFile('/proc/'+builder.pid+'/task/'+builder.pid+'/children','utf8')).trim().split(/\s+/).filter(Boolean).map(Number);}catch{return null;}for(const pid of children)try{if((await readFile('/proc/'+pid+'/comm','utf8')).trim()==='cc')return pid;}catch{}return null;});process.kill(compilerPid,'SIGSTOP');builder.kill('SIGKILL');await new Promise(resolve=>builder.once('exit',resolve));process.kill(compilerPid,'SIGCONT');await until(async()=>!await alive(compilerPid));await assert.rejects(lstat(join(directory,'manifest.json')),error=>error.code==='ENOENT');await assert.rejects(createCapacityPreparationContext({...configuration,entryDirectory:directory}),/entry_unproven/);
 }finally{if(compilerPid&&await alive(compilerPid))process.kill(compilerPid,'SIGCONT');if(builder?.exitCode===null&&builder?.signalCode===null)builder.kill('SIGKILL');await rm(directory,{recursive:true});}
});
