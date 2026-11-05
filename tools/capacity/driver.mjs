import {performance} from 'node:perf_hooks';
import {setTimeout as delay} from 'node:timers/promises';
import {sanitizeCapacityArrivalDiagnostic} from './metrics.mjs';
const driverErrorCodes=new Set(['ECONNRESET','ECONNREFUSED','ETIMEDOUT','EPIPE','ENOTFOUND','EAI_AGAIN','ENETUNREACH','EHOSTUNREACH']),driverErrorCode=error=>driverErrorCodes.has(error?.code)?error.code:'driver_error';

export async function runOpenLoop({durationMs,segments,send,metrics,maxInFlight=10000,scenario='mixed',diagnosticScenario='other',signal,maxArrivalLagMs=null,now=()=>performance.now(),sleep=ms=>delay(ms,undefined,{signal})}){
  if(!Number.isSafeInteger(durationMs)||durationMs<1||!Array.isArray(segments)||!segments.length||typeof send!=='function'||!Number.isInteger(maxInFlight)||maxInFlight<1)throw new Error('capacity_arrival_configuration');
  let edge=0;for(const part of segments){if(part.startMs!==edge||!Number.isFinite(part.qps)||part.qps<=0||part.endMs<=part.startMs||part.endMs>durationMs)throw new Error('capacity_rate_segment_invalid');edge=part.endMs;}if(edge!==durationMs)throw new Error('capacity_rate_gap');
  if(maxArrivalLagMs!==null&&(!Number.isFinite(maxArrivalLagMs)||maxArrivalLagMs<0||maxArrivalLagMs>1000))throw new Error('capacity_arrival_lag_invalid');
  const beginning=now(),pending=new Set();let sequence=0,slotSequence=0;
  try{for(const part of segments)for(let index=0;index*1000/part.qps<part.endMs-part.startMs;index++){
    signal?.throwIfAborted();const scheduled=beginning+part.startMs+index*1000/part.qps;while(now()<scheduled){await sleep(Math.max(1,scheduled-now()));signal?.throwIfAborted();}signal?.throwIfAborted();const actual=now(),slot=slotSequence++,limit=maxArrivalLagMs??1000/part.qps;
    const evidence=reason=>sanitizeCapacityArrivalDiagnostic({scenario:diagnosticScenario,slotSequence:slot,scheduledAt:scheduled,actualAt:actual,lagMs:actual-scheduled,lagLimitMs:limit,reason});
    if(actual>=beginning+durationMs||actual-scheduled>limit){const detail=evidence(actual>=beginning+durationMs?'duration_exhausted':'arrival_lag_exceeded'),retained=metrics.arrival({dispatched:false,diagnostic:detail});metrics.violation('capacity_arrival_late',retained??detail);continue;}
    if(pending.size>=maxInFlight){metrics.arrival({dispatched:false,diagnostic:evidence('inflight_limit')});continue;}metrics.arrival({dispatched:true});const number=sequence++;
    const task=Promise.resolve().then(()=>send({sequence:number,scheduledAt:scheduled,startedAt:actual,arrivalLagMs:actual-scheduled,scenario})).catch(error=>{metrics.violation('capacity_driver_request_failed',{code:driverErrorCode(error)});}).finally(()=>{pending.delete(task);metrics.finished();});pending.add(task);
  }while(now()<beginning+durationMs){await sleep(Math.max(1,beginning+durationMs-now()));signal?.throwIfAborted();}}finally{await Promise.allSettled([...pending]);}
  return {durationMs:now()-beginning,scheduledDurationMs:durationMs};
}
export async function runConcurrent({durationMs,concurrency,send,metrics,scenario='long-stream',signal,now=()=>performance.now()}){
  if(!Number.isSafeInteger(durationMs)||durationMs<1||!Number.isInteger(concurrency)||concurrency<1||concurrency>10000||typeof send!=='function')throw new Error('capacity_concurrent_configuration');const start=now();let sequence=0;
  await Promise.all(Array.from({length:concurrency},async(_,slot)=>{while(now()-start<durationMs&&!signal?.aborted){metrics.arrival({dispatched:true});try{await send({sequence:sequence++,slot,startedAt:now(),scenario});}catch(error){metrics.violation('capacity_driver_request_failed',{code:driverErrorCode(error)});}finally{metrics.finished();}}}));signal?.throwIfAborted();return {durationMs:now()-start};
}
