import {performance} from 'node:perf_hooks';

// Response-local timing contains no request, route, identity or payload data.
export function createAdminRequestTiming({clock=()=>performance.now()}={}){
  const timings=new WeakMap(),signals=new WeakMap();
  const now=()=>{try{const value=clock();return Number.isFinite(value)?value:null;}catch{return null;}};
  // Each phase is the union of its active intervals; phases may overlap.
  const span=()=>({active:0,started:null,total:0,valid:true,seen:false});
  function enter(value){
    const at=now();value.seen=true;if(at===null)value.valid=false;
    if(value.active++===0)value.started=at;
  }
  function leave(value){
    if(value.active<=0)return;
    if(--value.active===0){const at=now();if(at===null||value.started===null||at<value.started)value.valid=false;else value.total+=at-value.started;}
  }
  function begin(req,res){
    try{
      if(typeof res.getHeader!=='function'||typeof res.setHeader!=='function'||timings.has(res)||req.method!=='GET'||res.destroyed||res.writableEnded)return false;
      const path=new URL(req.url,'http://localhost').pathname;if(!path.startsWith('/api/admin/'))return false;
      const started=now();if(started===null)return false;
      timings.set(res,{started,done:false,qwait:span(),qwork:span()});return true;
    }catch{return false;}
  }
  function registerSignal(res,signal){
    try{
      const timing=timings.get(res);
      if(!timing||timing.done||!signal||typeof signal.addEventListener!=='function'||typeof signal.removeEventListener!=='function'||typeof signal.aborted!=='boolean'||signals.has(signal))return false;
      signals.set(signal,timing);return true;
    }catch{return false;}
  }
  function wrapQueue(queue){
    return {
      run(operation,options){
        let timing;try{timing=signals.get(options?.signal);}catch{}
        if(!timing||timing.done)return queue.run(operation,options);
        enter(timing.qwait);let waiting=true;
        const admitted=()=>{if(waiting){waiting=false;leave(timing.qwait);}};
        let result;
        try{result=queue.run(async()=>{
          admitted();enter(timing.qwork);
          try{return await operation();}finally{leave(timing.qwork);}
        },options);}catch(error){admitted();throw error;}
        return Promise.resolve(result).then(value=>{admitted();return value;},error=>{admitted();throw error;});
      },
      close(...args){return queue.close(...args);},
      status(...args){return queue.status(...args);},
    };
  }
  // Call only after successful authentication and JSON serialization, before headers.
  function finish(res,status,{authenticated=false}={}){
    try{
      const timing=timings.get(res);
      if(!timing||timing.done||authenticated!==true||status!==200||res.headersSent||res.destroyed||res.writableEnded)return false;
      const at=now(),elapsed=at===null?null:at-timing.started;if(elapsed===null||!Number.isFinite(elapsed)||elapsed<0)return false;
      const previous=res.getHeader('Server-Timing'),existing=Array.isArray(previous)?previous.join(', '):previous===undefined?'':String(previous),metrics=[];
      const append=(name,value)=>{if(!new RegExp('(?:^|,)\\s*'+name+'\\s*(?:;|,|$)','i').test(existing))metrics.push(name+';dur='+value.toFixed(2));};
      append('app',elapsed);
      for(const name of ['qwait','qwork']){const value=timing[name];if(value.seen&&value.valid&&value.active===0&&Number.isFinite(value.total))append(name,value.total);}
      timing.done=true;if(!metrics.length)return false;
      res.setHeader('Server-Timing',(existing?existing+', ':'')+metrics.join(', '));return true;
    }catch{return false;}
  }
  return {begin,finish,registerSignal,wrapQueue};
}

const timing=createAdminRequestTiming();
export const beginAdminRequestTiming=timing.begin;
export const finishAdminRequestTiming=timing.finish;
export const registerAdminRequestTimingSignal=timing.registerSignal;
export const wrapAdminQueryTimingQueue=timing.wrapQueue;
