import {randomUUID} from 'node:crypto';

export const workerStartupKind='worker.startup';
export const workerStartupResult=Object.freeze({contract:'harbor-worker-startup-v1'});

// Exercise the ordinary claim/execute/complete path once per Worker instance.
// A fixed per-instance id also makes a lost enqueue acknowledgement retry safe.
export function createWorkerStartupProbe(jobs){
 if(typeof jobs?.enqueue!=='function')throw TypeError('Worker startup jobs are required');
 const id=workerStartupKind+':'+randomUUID();let scheduled=false,pending;
 const schedule=()=>{
  if(scheduled)return Promise.resolve();
  if(!pending)pending=Promise.resolve().then(()=>jobs.enqueue(workerStartupKind,{id})).then(()=>{scheduled=true;}).finally(()=>{pending=undefined;});
  return pending;
 };
 return {schedule,handlers:{[workerStartupKind]:async()=>workerStartupResult}};
}

export async function hasWorkerProgress(store,since){
 if(!Number.isSafeInteger(since)||since<0)throw TypeError('Progress timestamp invalid');
 const row=await store.get("SELECT EXISTS(SELECT 1 FROM harbor.background_jobs WHERE ((status='running' AND claimed_at>=$1 OR status='completed' AND updated>=$1) AND kind IN ('recording.purge','routing.expiry','recording.index','recording.export')) OR (kind=$2 AND status='completed' AND created>=$1 AND claimed_at>=$1 AND updated>=$1 AND result=$3::jsonb)) progress",since,workerStartupKind,JSON.stringify(workerStartupResult));
 return row.progress===true;
}
