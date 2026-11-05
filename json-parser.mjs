import {Worker} from 'node:worker_threads';
import {AppError} from './store.mjs';
import {responseBodyBytes} from './request-body.mjs';
import {rebuildJson} from './json-graph.mjs';

export function createJsonParser({maxWorkers=2,maxQueued=16,waitMs=15000,timeoutMs=30000,heapMb=512,rebuildMs=5}={}, {workerFactory=(url,options)=>new Worker(url,options)}={}){
  if(!Number.isInteger(maxWorkers)||maxWorkers<1||maxWorkers>8||!Number.isInteger(maxQueued)||maxQueued<0||maxQueued>128||!Number.isSafeInteger(waitMs)||waitMs<100||waitMs>60000||!Number.isSafeInteger(timeoutMs)||timeoutMs<100||timeoutMs>300000||!Number.isInteger(heapMb)||heapMb<64||heapMb>2048||!Number.isInteger(rebuildMs)||rebuildMs<1||rebuildMs>25||typeof workerFactory!=='function')throw new AppError(503,'JSON解析资源配置无效','json_parser_configuration');
  const slots=new Set(),queue=[];let stopped=false,sequence=0,spawned=0,completed=0,failed=0,closePromise;
  const unavailable=()=>new AppError(503,'JSON解析暂不可用，请稍后重试','json_parser_failed');
  const cancelled=signal=>signal?.reason instanceof AppError?signal.reason:new AppError(499,'请求解析已取消','request_cancelled');
  function finish(job,error,value){if(job.done)return;job.done=true;clearTimeout(job.timer);job.signal?.removeEventListener('abort',job.abort);job.buffer=null;job.accounting=null;if(error){failed++;job.reject(error);}else{completed++;job.resolve(value);}}
  function retire(slot,error){
    if(slot.retiring)return;slot.retiring=true;slot.failure=error;clearTimeout(slot.job?.timer);slot.worker.ref();
    // The slot remains occupied until the actual exit event; no overlap with a
    // replacement while the cancelled CPU job or its transferred buffer lives.
    void slot.worker.terminate().catch(()=>{});
  }
  function connect(){
    const worker=workerFactory(new URL('./json-parser-worker.mjs',import.meta.url),{env:{},execArgv:[],stdout:true,stderr:true,resourceLimits:{maxOldGenerationSizeMb:heapMb}}),slot={worker,job:null,retiring:false};slots.add(slot);spawned++;worker.stdout?.resume();worker.stderr?.resume();
    slot.exited=new Promise(resolve=>{slot.resolveExit=resolve;});
    const reap=()=>{if(slot.delivery)return;slots.delete(slot);if(slot.job){const job=slot.job;slot.job=null;finish(job,slot.failure||unavailable());}slot.resolveExit();pump();};
    worker.on('message',message=>{
      if(slot.retiring||!slot.job)return;if(slot.delivery||message?.id!==slot.job.id){retire(slot,unavailable());return;}
      const job=slot.job;
      if(message.error){slot.job=null;finish(job,message.error==='invalid_json'?new AppError(400,'请求必须是 JSON 对象','invalid_json'):unavailable());worker.unref();pump();return;}
      const delivery=rebuildJson(message.graph,{sliceMs:rebuildMs,cancelled:()=>slot.retiring||job.done});slot.delivery=delivery;
      void delivery.then(value=>{if(slot.retiring||job.done)return;slot.delivery=null;slot.job=null;finish(job,null,value);worker.unref();pump();},()=>{if(!slot.retiring)retire(slot,unavailable());}).finally(()=>{if(slot.delivery===delivery)slot.delivery=null;if(slot.threadExited)reap();else pump();});
    });
    worker.on('messageerror',()=>retire(slot,unavailable()));worker.on('error',()=>retire(slot,unavailable()));
    worker.on('exit',()=>{slot.threadExited=true;slot.retiring=true;reap();});worker.unref();return slot;
  }
  function start(slot,job){
    clearTimeout(job.timer);slot.job=job;job.slot=slot;slot.worker.ref();job.timer=setTimeout(()=>retire(slot,new AppError(503,'JSON解析超时，请稍后重试','json_parser_timeout')),timeoutMs);job.timer.unref();
    try{
      let buffer=job.buffer.buffer;
      if(job.buffer.byteOffset!==0||job.buffer.byteLength!==buffer.byteLength){job.accounting?.resize('transfer',job.buffer.byteLength);buffer=buffer.slice(job.buffer.byteOffset,job.buffer.byteOffset+job.buffer.byteLength);}
      slot.worker.postMessage({id:job.id,buffer},[buffer]);job.buffer=null;
    }catch(error){slot.job=null;finish(job,error instanceof AppError?error:unavailable());slot.worker.unref();}
  }
  function pump(){
    if(stopped)return;
    while(queue.length){let slot=[...slots].find(slot=>!slot.job&&!slot.retiring&&!slot.delivery);if(!slot){if(slots.size>=maxWorkers)return;try{slot=connect();}catch{finish(queue.shift(),unavailable());continue;}}const job=queue.shift();if(job.signal?.aborted){finish(job,cancelled(job.signal));continue;}start(slot,job);}
  }
  function parse(buffer,{signal,accounting}={}){
    if(stopped)return Promise.reject(new AppError(503,'JSON解析服务已关闭','json_parser_closed'));
    if(!Buffer.isBuffer(buffer)||buffer.length>responseBodyBytes)return Promise.reject(new AppError(413,'请求体超过解析传输上限','request_too_large'));
    if(signal?.aborted)return Promise.reject(cancelled(signal));
    const room=[...slots].some(slot=>!slot.job&&!slot.retiring&&!slot.delivery)||slots.size<maxWorkers;
    if(!room&&queue.length>=maxQueued)return Promise.reject(new AppError(503,'JSON解析排队已满，请稍后重试','json_parser_busy'));
    return new Promise((resolve,reject)=>{
      const job={id:++sequence,buffer,signal,accounting,resolve,reject,done:false};
      job.abort=()=>{if(job.slot)retire(job.slot,cancelled(signal));else{const index=queue.indexOf(job);if(index!==-1)queue.splice(index,1);finish(job,cancelled(signal));}};
      job.timer=setTimeout(()=>{const index=queue.indexOf(job);if(index<0)return;queue.splice(index,1);finish(job,new AppError(503,'JSON解析等待超时，请稍后重试','json_parser_queue_timeout'));},waitMs);job.timer.unref();queue.push(job);signal?.addEventListener('abort',job.abort,{once:true});if(signal?.aborted)job.abort();pump();
    });
  }
  function close(){if(closePromise)return closePromise;stopped=true;const error=new AppError(503,'JSON解析服务已关闭','json_parser_closed');for(const job of queue.splice(0))finish(job,error);for(const slot of slots)retire(slot,error);closePromise=Promise.all([...slots].map(slot=>slot.exited)).then(()=>{});return closePromise;}
  return {parse,close,status:()=>({workers:slots.size,active:[...slots].filter(slot=>slot.job).length,rebuilding:[...slots].filter(slot=>slot.delivery).length,retiring:[...slots].filter(slot=>slot.retiring).length,queued:queue.length,spawned,completed,failed,stopped,maxWorkers,maxQueued,waitMs,timeoutMs,heapMb,rebuildMs})};
}
