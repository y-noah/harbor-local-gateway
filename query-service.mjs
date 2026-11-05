import {Worker} from 'node:worker_threads';
import {usageWindow} from './store.mjs';

function createLane(filename,kind,{maxPending=8,timeoutMs=15000}={}){
  let worker=null,closed=false,sequence=0;
  const pending=new Map(),shared=new Map();
  const unavailable=()=>Object.assign(new Error('统计服务忙，请稍后重试'),{status:503,code:'statistics_unavailable'});
  function stop(){
    const active=worker;worker=null;
    for(const job of pending.values()){clearTimeout(job.timer);job.reject(unavailable());}pending.clear();shared.clear();
    return active?.terminate();
  }
  function connect(){
    if(worker)return worker;
    const current=new Worker(new URL('./query-worker.mjs',import.meta.url),{workerData:{filename},resourceLimits:{maxOldGenerationSizeMb:128}});worker=current;
    current.on('message',({id,result,error})=>{
      if(worker!==current)return;const job=pending.get(id);if(!job)return;
      clearTimeout(job.timer);pending.delete(id);if(!pending.size)current.unref();
      if(error)job.reject(Object.assign(new Error(error.message),{status:error.status,code:error.code}));else job.resolve(result);
    });
    current.on('error',()=>{if(worker===current)void stop();});
    current.on('exit',()=>{if(worker===current)void stop();});
    current.unref();return current;
  }
  function execute(query,at=Date.now()){
    if(closed)return Promise.reject(unavailable());
    const window=usageWindow(at),key=JSON.stringify([query.member,query.model,query.status,query.snapshot,window.start,window.end,...(kind==='list'?[query.page,query.pageSize,query.cursor]:[])]);
    if(shared.has(key))return shared.get(key);
    if(pending.size>=maxPending)return Promise.reject(unavailable());
    const id=++sequence;
    const result=new Promise((resolve,reject)=>{
      const active=connect(),timer=setTimeout(()=>{if(worker===active)void stop();},timeoutMs);
      pending.set(id,{resolve,reject,timer});active.ref();active.postMessage({id,query,window,at,kind});
    });
    shared.set(key,result);const clear=()=>{if(shared.get(key)===result)shared.delete(key);};result.then(clear,clear);return result;
  }
  return {execute,pending:()=>pending.size,close:()=>{closed=true;return stop();}};
}
export function createQueryService(filename,options){
  const lists=createLane(filename,'list',options),stats=createLane(filename,'summary',options);
  return {list:lists.execute,statistics:stats.execute,pending:()=>lists.pending()+stats.pending(),close:()=>Promise.all([lists.close(),stats.close()])};
}
