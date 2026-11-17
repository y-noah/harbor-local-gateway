import {AppError} from './store.mjs';

const conflicts=new WeakMap();
// This marker is private to a pre-admission routing conflict, never supplied
// by an upstream error or by a caller-controlled response body.
export function routeAdmissionBusy(requestId,owner){
 const error=new AppError(409,'会话仍有请求处理中，暂不能切换','session_busy');conflicts.set(error,{requestId,owner});return error;
}
export function createRouteAdmissionWait(){
 const owners=new Map();
 function own(requestId){
  if(typeof requestId!=='string'||!requestId||owners.has(requestId))throw new AppError(503,'请求生命周期登记冲突','routing_owner_conflict');
  const owner={waiters:new Set(),closed:false};owners.set(requestId,owner);
  return ()=>{if(owners.get(requestId)!==owner)return;owners.delete(requestId);owner.closed=true;for(const resume of [...owner.waiters])resume();};
 }
 async function run(operation,{signal,inTransaction=()=>false}={}){
  for(;;){
   signal?.throwIfAborted();
   try{return await operation();}catch(error){
    if(!conflicts.has(error)||inTransaction())throw error;
    signal?.throwIfAborted();const conflict=conflicts.get(error),owner=conflict.owner??owners.get(conflict.requestId);
    // A persisted queued/running row without a local lifecycle is not proof
    // that replay or deletion is safe. Leave it to the recovery mechanism.
    if(!owner)throw new AppError(503,'会话仍有待核实请求，暂不能安全切换账号','session_route_owner_missing');
    if(owner.closed)continue;
    await new Promise((resolve,reject)=>{
     const cleanup=()=>{owner.waiters.delete(resume);signal?.removeEventListener('abort',abort);},resume=()=>{cleanup();resolve();},abort=()=>{cleanup();reject(signal.reason??new AppError(499,'请求已取消','request_cancelled'));};
     owner.waiters.add(resume);signal?.addEventListener('abort',abort,{once:true});if(signal?.aborted)abort();
    });
   }
  }
 }
 // Capture before the database lookup, while the caller holds its routing
 // lock. Completion during that lookup must not turn a live request into an
 // apparent orphan. The snapshot is bounded by admitted request capacity.
 const capture=()=>{const snapshot=new Map(owners);return requestId=>routeAdmissionBusy(requestId,snapshot.get(requestId));};
 return {own,run,capture,status:()=>({owners:owners.size})};
}
