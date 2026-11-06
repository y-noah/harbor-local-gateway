import {spawn,execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {realpath} from 'node:fs/promises';
import {join,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash,randomBytes} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {postgresBackupContext} from './postgres-backup-context.mjs';
import {restoreOwnedProcesses} from './postgres-backup-processes.mjs';
import {verifyReceiverCandidateKernelCredentials} from './storage-receiver-candidate-processes.mjs';
import {receiverPublisherSourceManifest,verifyReceiverPublisherSource} from './storage-receiver-candidate.mjs';
import {readCapacityRuntimeRegular} from './tools/capacity/runtime-artifact.mjs';
import {canonical,transferError} from './migration-artifacts.mjs';

const fail=code=>{throw transferError(code);},hash=value=>createHash('sha256').update(value).digest('hex');

// A thin adapter for the fixed installed application policy. It deliberately
// cannot create receiver/OCI/activation authority or accept arbitrary commands.
export async function createInstalledPeriodicApplicationProcesses(context,configuration,{protectedRoot,signal,archivedRestore}={}){
  const {verifyInstalledPeriodicApplicationPolicy}=await import('./postgres-periodic-application-policy.mjs');
  const admission=async()=>{if(archivedRestore){const {revalidateArchivedPeriodicApplicationRestore}=await import('./postgres-periodic-application-restore.mjs');await revalidateArchivedPeriodicApplicationRestore(archivedRestore.token,archivedRestore.session,{signal});}else await verifyInstalledPeriodicApplicationPolicy(configuration,{signal});};
  await admission();
  const record=postgresBackupContext(context);
  if(process.getuid()!==0||record.closed||record.scope!=='existing-cluster'||record.restoreUid<1||record.restoreGid<1||typeof protectedRoot!=='string'||!protectedRoot.startsWith(record.restoreDirectory+'/'))fail('periodic_application_installed_scope');
  const sourceDirectory=dirname(fileURLToPath(import.meta.url)),nodeFile=await realpath(process.execPath),nodeDigest=hash(await readCapacityRuntimeRegular(nodeFile,128*1024**2,{trusted:true,signal})),pythonFile=await realpath('/usr/bin/python3'),pythonDigest=hash(await readCapacityRuntimeRegular(pythonFile,128*1024**2,{trusted:true,signal}));
  const sources=await receiverPublisherSourceManifest(sourceDirectory,{signal}),sourceDigest=hash(canonical(sources));await verifyReceiverPublisherSource(sourceDirectory,sources,{signal});
  if(!sources.files['postgres-periodic-application-runtime-child.mjs'])fail('periodic_application_installed_source');
  const children=new Map();let closed=false,closing;
  async function revalidate(){
    signal?.throwIfAborted();await admission();
    if(record.closed||hash(await readCapacityRuntimeRegular(nodeFile,128*1024**2,{trusted:true,signal}))!==nodeDigest||hash(await readCapacityRuntimeRegular(pythonFile,128*1024**2,{trusted:true,signal}))!==pythonDigest)fail('periodic_application_installed_tool_changed');
    await verifyReceiverPublisherSource(sourceDirectory,sources,{signal});
  }
  const owns=child=>{const row=children.get(child);if(!row||row.owner.pid!==child.pid)fail('periodic_application_process_required');return row;};
  async function send(row,selected,{leader=false}={}){
    const members=(await restoreOwnedProcesses(row.owner,{checkUnknown:false})).filter(value=>!leader||value.pid===row.owner.pid);if(!members.length)return;
    // Cleanup remains possible after cancellation or configuration changes.
    // Authenticate the fixed pidfd helper independently, without re-opening
    // admission or requiring the cancelled business policy to be current.
    for(const name of ['storage-receiver-candidate-signal.py','tools/capacity/process-signal.py'])if(hash(await readCapacityRuntimeRegular(join(sourceDirectory,name),1048576,{trusted:true}))!==sources.files[name])fail('periodic_application_signal_untrusted');
    if(hash(await readCapacityRuntimeRegular(pythonFile,128*1024**2,{trusted:true}))!==pythonDigest)fail('periodic_application_signal_untrusted');
    const frame=JSON.stringify({signal:selected,rows:members.map(({pid,uid,start})=>({pid,uid,start})),expectedUid:record.restoreUid});if(Buffer.byteLength(frame)>524288)fail('periodic_application_signal_capacity');
    const operation=promisify(execFile)(pythonFile,['-I','-B',join(sourceDirectory,'storage-receiver-candidate-signal.py')],{env:{PATH:'/usr/bin:/bin'},timeout:5000,maxBuffer:4096});operation.child.stdin.on('error',()=>{});operation.child.stdin.end(frame);try{await operation;}catch{fail('periodic_application_signal_unverified');}
  }
  async function stop(child){
    const row=owns(child);return row.stopping??=(async()=>{
      await send(row,'SIGCONT');await send(row,'SIGTERM',{leader:true});const deadline=Date.now()+5000;
      while(!row.settled||(await restoreOwnedProcesses(row.owner,{checkUnknown:false})).length){
        if(Date.now()>=deadline){await send(row,'SIGKILL');const final=Date.now()+2000;while(!row.settled||(await restoreOwnedProcesses(row.owner,{checkUnknown:false})).length){if(Date.now()>=final)fail('periodic_application_cleanup_unknown');await delay(10);}break;}await delay(10);
      }
      await row.done;
    })();
  }
  async function start(role){
    if(closed||!['gateway','worker','publisher'].includes(role))fail('periodic_application_process_role');await revalidate();
    const env={PATH:'/usr/bin:/bin',HARBOR_BACKUP_RESTORE_TOKEN:randomBytes(32).toString('hex'),HARBOR_RESTORED_RUNTIME_ROLE:role},entry=role==='publisher'?'storage-receiver-candidate-publisher.mjs':'postgres-periodic-application-runtime-child.mjs',flag=role==='publisher'?'--owned-receiver-report':'--periodic-restored-runtime';
    const child=spawn(nodeFile,[join(sourceDirectory,entry),flag],{uid:record.restoreUid,gid:record.restoreGid,detached:true,env,stdio:['ignore','ignore','ignore','ipc']}),owner={pid:child.pid,nonce:env.HARBOR_BACKUP_RESTORE_TOKEN,uid:record.restoreUid,data:protectedRoot,protectedRoot,members:new Map(),start:null};
    let resolveDone,rejectDone;const row={owner,settled:false,done:new Promise((resolve,reject)=>{resolveDone=resolve;rejectDone=reject;})};children.set(child,row);void row.done.catch(()=>{});
    child.once('error',()=>{row.settled=true;rejectDone(transferError('periodic_application_process_start'));});child.once('close',(code,signal)=>{row.settled=true;resolveDone({code,signal});});
    try{
      await new Promise((resolve,reject)=>{child.once('spawn',resolve);child.once('error',reject);});
      if(!(await restoreOwnedProcesses(owner,{checkUnknown:false})).some(value=>value.pid===child.pid&&value.uid===record.restoreUid))fail('periodic_application_process_uid');
      await verifyReceiverCandidateKernelCredentials(child,{uid:record.restoreUid,gid:record.restoreGid});signal?.throwIfAborted();return child;
    }catch(error){await stop(child);throw error;}
  }
  return Object.freeze({start,resume:child=>send(owns(child),'SIGCONT'),stop,proof:child=>({pid:owns(child).owner.pid,uid:record.restoreUid}),sourceDigest,nodeDigest,uid:record.restoreUid,gid:record.restoreGid,revalidate,
    close:()=>closing??=(async()=>{closed=true;const results=await Promise.allSettled([...children.keys()].map(stop));if(results.some(value=>value.status==='rejected'))fail('periodic_application_cleanup_unknown');})()});
}
