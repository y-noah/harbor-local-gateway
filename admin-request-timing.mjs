import {performance} from 'node:perf_hooks';

// Response-local timing contains no request, route, identity or payload data.
export function createAdminRequestTiming({clock=()=>performance.now()}={}){
  const timings=new WeakMap();
  function begin(req,res){
    if(typeof res.getHeader!=='function'||typeof res.setHeader!=='function'||timings.has(res)||req.method!=='GET'||res.destroyed||res.writableEnded)return false;
    let path;try{path=new URL(req.url,'http://localhost').pathname;}catch{return false;}
    if(!path.startsWith('/api/admin/'))return false;
    const started=clock();if(!Number.isFinite(started))return false;
    timings.set(res,{started,done:false});return true;
  }
  // Call only after successful authentication and JSON serialization, before headers.
  function finish(res,status,{authenticated=false}={}){
    const timing=timings.get(res);
    if(!timing||timing.done||authenticated!==true||status!==200||res.headersSent||res.destroyed||res.writableEnded)return false;
    const elapsed=clock()-timing.started;
    if(!Number.isFinite(elapsed)||elapsed<0)return false;
    const previous=res.getHeader('Server-Timing'),existing=Array.isArray(previous)?previous.join(', '):previous===undefined?'':String(previous);
    timing.done=true;
    if(/(?:^|,)\s*app\s*(?:;|,|$)/i.test(existing))return false;
    res.setHeader('Server-Timing',(existing?existing+', ':'')+'app;dur='+elapsed.toFixed(2));return true;
  }
  return {begin,finish};
}

const timing=createAdminRequestTiming();
export const beginAdminRequestTiming=timing.begin;
export const finishAdminRequestTiming=timing.finish;
