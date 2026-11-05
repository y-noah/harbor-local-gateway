import {AppError} from './store.mjs';

export function createPostgresWorkerRuntime({jobs,handlers,concurrency=2,pollMs=1000,schedule}={}){
  const kinds=Object.keys(handlers||{});
  if(!jobs||['claim','heartbeat','complete','fail'].some(name=>typeof jobs[name]!=='function')||!Number.isInteger(jobs.leaseMs)||jobs.leaseMs<1000||!kinds.length||kinds.length>32||kinds.some(kind=>!/^[a-z][a-z0-9_.-]{0,79}$/.test(kind)||typeof handlers[kind]!=='function')||!Number.isInteger(concurrency)||concurrency<1||concurrency>8||!Number.isInteger(pollMs)||pollMs<100||pollMs>60000||schedule!==undefined&&typeof schedule!=='function')throw new AppError(503,'Worker资源或处理器配置无效','worker_configuration');
  const active=new Map(),counts={completed:0,failed:0,lost:0};let stopping=false,started=false,timer,pumping,refillImmediate,refillRequested=false,scheduleRequested=false,backingOff=false,retryTimer,closePromise,lastError=null;
  function launch(job){
    const execution=Symbol(),controller=new AbortController();let renewing=null;
    const heartbeat=setInterval(()=>{
      if(renewing)return;
      renewing=jobs.heartbeat(job).then(owned=>{if(!owned)controller.abort();},()=>controller.abort()).finally(()=>{renewing=null;});
    },Math.max(100,Math.floor(jobs.leaseMs/3)));heartbeat.unref();
    const task=(async()=>{
      try{
        const result=await handlers[job.kind](job,{signal:controller.signal});
        if(controller.signal.aborted)throw new AppError(503,'Worker任务已停止','worker_stopping');
        await jobs.complete(job,result??{});counts.completed++;
      }catch(error){
        try{const terminal=await jobs.fail(job,{retry:true,errorCode:controller.signal.aborted?'worker_interrupted':'job_failed'});if(terminal==='completed')counts.completed++;else counts.failed++;}catch{counts.lost++;}
        lastError=error?.code==='job_lease_lost'?'job_lease_lost':controller.signal.aborted?'worker_interrupted':'job_failed';
      }finally{clearInterval(heartbeat);await renewing;active.delete(execution);requestRefill();}
    })();active.set(execution,{controller,task});
  }
  function requestRefill(){
    if(!started||stopping)return;
    refillRequested=true;
    if(pumping||refillImmediate||backingOff)return;
    // One bounded pump per event-loop turn; fast completions cannot starve I/O.
    refillImmediate=setImmediate(()=>{refillImmediate=null;if(!stopping&&started)void pump(false);});
  }
  function pump(withSchedule){
    if(stopping)return Promise.resolve();
    if(withSchedule)scheduleRequested=true;
    if(pumping)return pumping;
    if(backingOff)return Promise.resolve();
    pumping=(async()=>{
      refillRequested=false;
      try{
        if(scheduleRequested){scheduleRequested=false;if(schedule)try{await schedule();}catch{lastError='worker_schedule_failed';}}
        const slots=concurrency-active.size;
        for(let index=0;index<slots&&!stopping&&active.size<concurrency;index++){
          const job=await jobs.claim(kinds);if(!job)break;
          if(stopping){await jobs.fail(job,{errorCode:'worker_interrupted',retryDelayMs:0}).catch(()=>{});break;}
          launch(job);
        }
      }catch{lastError='worker_poll_failed';backingOff=true;retryTimer=setTimeout(()=>{retryTimer=null;backingOff=false;if(started&&!stopping)requestRefill();},pollMs);retryTimer.unref?.();}
    })().finally(()=>{pumping=null;if(!stopping&&started&&(refillRequested||scheduleRequested))requestRefill();});
    return pumping;
  }
  const runOnce=()=>pump(true);
  const start=()=>{if(stopping)throw new AppError(503,'Worker已关闭','worker_stopping');if(started)return;started=true;timer=setInterval(()=>{void runOnce();},pollMs);void runOnce();};
  function close(){return closePromise??=(async()=>{stopping=true;clearInterval(timer);clearTimeout(retryTimer);retryTimer=null;clearImmediate(refillImmediate);refillImmediate=null;refillRequested=false;scheduleRequested=false;for(const {controller} of active.values())controller.abort();await pumping;for(const {controller} of active.values())controller.abort();await Promise.allSettled([...active.values()].map(value=>value.task));})();}
  return {start,runOnce,close,status:()=>({stopping,active:active.size,polling:!!pumping,...counts,lastError})};
}
