import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {readFile,rm,lstat} from 'node:fs/promises';
import {setTimeout as delay} from 'node:timers/promises';
import {once} from 'node:events';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
const real={skip:!binDirectory?'需独占PG18完整工具链':false,timeout:60000};
async function row(pid){try{const text=await readFile('/proc/'+pid+'/stat','utf8'),fields=text.slice(text.lastIndexOf(')')+2).split(' ');return {state:fields[0],start:fields[19]};}catch(error){if(error.code==='ENOENT')return null;throw error;}}
async function waitStopped(pid){const deadline=Date.now()+5000;for(;;){if((await row(pid))?.state==='T')return;assert.ok(Date.now()<deadline,'actual backend did not enter SIGSTOP');await delay(5);}}
async function laboratory(){return createPrivatePostgres({binDirectory,libraryDirectory,recovery:{backupKey:randomBytes(32)}});}

test('source fault drains a real stopped PostgreSQL backend outside the postmaster session before one-shot source deletion',real,async()=>{
 const lab=await laboratory(),store=new PostgresStore({connection:lab.connection});let backend,original;
 try{backend=(await store.get('SELECT pg_backend_pid() pid')).pid;original=await row(backend);assert.ok(original);assert.notEqual(backend,lab.process.pid);process.kill(backend,'SIGSTOP');await waitStopped(backend);const fault=await lab.recovery.faultSource();assert.equal(fault.processTreeDrained,true);const after=await row(backend);assert.ok(!after||after.start!==original.start||after.state==='Z','a live stopped backend cannot outlast the fault acknowledgement');await rm(lab.recovery.originalDataDirectory,{recursive:true});await assert.rejects(lstat(lab.recovery.originalDataDirectory),{code:'ENOENT'});
 }finally{if(backend&&original){const current=await row(backend);if(current?.start===original.start&&current.state!=='Z')process.kill(backend,'SIGCONT');}await store.close();await lab.close();}
});

test('an unknown source-directory occupant receives no fault signal and refusal leaves the real source running for a safe retry',real,async()=>{
 const lab=await laboratory();let stranger,store;const signals=[];
 try{stranger=lab.runtime.start(process.execPath,['-e',"for(const s of ['SIGCONT','SIGTERM','SIGINT'])process.on(s,()=>process.send({signal:s}));process.send({ready:true});setInterval(()=>{},1000)"],{role:'helper',cwd:lab.dataDirectory,stdio:['ignore','ignore','ignore','ipc']});stranger.on('message',message=>{if(message.signal)signals.push(message.signal);});await once(stranger,'message',{signal:AbortSignal.timeout(5000)});await assert.rejects(lab.recovery.faultSource(),{code:'backup_restore_process_identity_unknown'});assert.deepEqual(signals,[]);assert.equal(stranger.exitCode,null);assert.equal(lab.process.exitCode,null);assert.notEqual((await row(lab.process.pid))?.state,'T');store=new PostgresStore({connection:{...lab.connection,host:lab.socketDirectory,port:lab.databasePort}});assert.equal((await store.get('SELECT 1 n')).n,1);await store.close();store=null;const exited=once(stranger,'exit');stranger.kill('SIGKILL');await exited;stranger=null;assert.equal((await lab.recovery.faultSource()).processTreeDrained,true);
 }finally{if(stranger&&stranger.exitCode===null){const exited=once(stranger,'exit');stranger.kill('SIGKILL');await exited;}await store?.close();await lab.close();}
});

test('pidfd signalling rejects a substituted process identity before any signal and preserves a different live child',{skip:process.platform!=='linux',timeout:15000},async()=>{
 const {execFile}=await import('node:child_process'),{promisify}=await import('node:util'),execute=promisify(execFile),runtime=(await import('../tools/capacity/owned-runtime.mjs')).createOwnedRuntime(),child=runtime.start(process.execPath,['-e',"process.send({ready:true});setInterval(()=>{},1000)"],{role:'helper',stdio:['ignore','ignore','ignore','ipc']});
 try{await once(child,'message',{signal:AbortSignal.timeout(5000)});const observed=await row(child.pid),send=async frame=>{const operation=execute('/usr/bin/python3',['-I','-B',new URL('../tools/capacity/process-signal.py',import.meta.url).pathname],{timeout:5000,maxBuffer:4096});operation.child.stdin.on('error',()=>{});operation.child.stdin.end(JSON.stringify(frame));return operation;},identity={pid:child.pid,uid:process.getuid(),start:observed.start};for(const rows of [[{...identity,start:identity.start+'0'}],[identity,{...identity}]]){await assert.rejects(send({signal:'SIGSTOP',rows}));assert.notEqual((await row(child.pid))?.state,'T');assert.equal(child.exitCode,null);}await assert.rejects(send({signal:'SIGTERM',rows:[identity]}));assert.equal(child.exitCode,null);await send({signal:'SIGSTOP',rows:[identity]});await waitStopped(child.pid);await send({signal:'SIGCONT',rows:[identity]});const deadline=Date.now()+5000;while((await row(child.pid))?.state==='T'){assert.ok(Date.now()<deadline);await delay(5);}
 }finally{await runtime.close();}
});

test('a pidfd pinned before process exit never signals a later unrelated process',{skip:process.platform!=='linux',timeout:15000},async()=>{
 const {execFile}=await import('node:child_process'),{promisify}=await import('node:util'),execute=promisify(execFile),program=`import importlib.util,os,subprocess,sys,signal,json
spec=importlib.util.spec_from_file_location('owned_signal',sys.argv[1]);helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
a=subprocess.Popen(['/usr/bin/sleep','30']);b=None;handles=[]
try:
 with open('/proc/'+str(a.pid)+'/stat') as f:text=f.read()
 start=text[text.rfind(')')+2:].split()[19]
 handles=helper.open_targets([{'pid':a.pid,'uid':os.getuid(),'start':start}])
 a.terminate();a.wait(timeout=5)
 b=subprocess.Popen(['/usr/bin/sleep','30'])
 helper.send_targets(handles,signal.SIGSTOP)
 with open('/proc/'+str(b.pid)+'/stat') as f:text=f.read()
 assert b.poll() is None and text[text.rfind(')')+2:].split()[0]!='T'
 print(json.dumps({'replacementUntouched':True}))
finally:
 for fd in handles:os.close(fd)
 for p in [a,b]:
  if p is not None and p.poll() is None:p.kill();p.wait(timeout=5)
`;
 const result=await execute('/usr/bin/python3',['-I','-B','-c',program,new URL('../tools/capacity/process-signal.py',import.meta.url).pathname],{timeout:10000,maxBuffer:4096});assert.deepEqual(JSON.parse(result.stdout),{replacementUntouched:true});
});
