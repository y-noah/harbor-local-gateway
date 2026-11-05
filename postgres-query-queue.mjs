import {AppError} from './store.mjs';

export function createPostgresQueryQueue({maxConcurrent=2,maxQueued=8,queueTimeoutMs=15000}={}){
  if(!Number.isInteger(maxConcurrent)||maxConcurrent<1||maxConcurrent>8||!Number.isInteger(maxQueued)||maxQueued<0||maxQueued>64||!Number.isInteger(queueTimeoutMs)||queueTimeoutMs<100||queueTimeoutMs>60000)throw new AppError(503,'管理查询资源配置无效','query_configuration');
  let active=0,closed=false;const queue=[],idle=[];
  function pump(){
    while(!closed&&active<maxConcurrent&&queue.length){const task=queue.shift();task.cleanup();active++;void (async()=>task.operation())().then(task.resolve,error=>task.reject(error.code==='57014'?new AppError(503,'管理查询执行超时，请缩小筛选范围后重试','query_timeout'):error)).finally(()=>{active--;pump();if(!active&&!queue.length)for(const done of idle.splice(0))done();});}
  }
  function run(operation,{signal}={}){
    if(signal!==undefined&&(!signal||typeof signal.addEventListener!=='function'||typeof signal.removeEventListener!=='function'||typeof signal.aborted!=='boolean'))return Promise.reject(new AppError(503,'管理查询取消信号无效','query_configuration'));
    if(signal?.aborted)return Promise.reject(signal.reason??new AppError(499,'管理查询已取消','request_cancelled'));
    if(closed)return Promise.reject(new AppError(503,'管理查询已关闭','query_closed'));
    if(active>=maxConcurrent&&queue.length>=maxQueued)return Promise.reject(new AppError(503,'管理查询繁忙，请稍后重试','query_queue_full'));
    return new Promise((resolve,reject)=>{
      const task={operation,resolve,reject},remove=error=>{const i=queue.indexOf(task);if(i<0)return;queue.splice(i,1);task.cleanup();reject(error);},abort=()=>remove(signal.reason??new AppError(499,'管理查询已取消','request_cancelled'));
      task.cleanup=()=>{clearTimeout(task.timer);signal?.removeEventListener('abort',abort);};
      task.timer=setTimeout(()=>remove(new AppError(503,'管理查询等待超时，请重试','query_timeout')),queueTimeoutMs);task.timer.unref();signal?.addEventListener('abort',abort,{once:true});queue.push(task);
      // Cancellation removes only waiting work. Once admitted, the operation
      // retains its real execution slot until its own promise settles.
      if(signal?.aborted)abort();else pump();
    });
  }
  async function close(){closed=true;for(const task of queue.splice(0)){task.cleanup();task.reject(new AppError(503,'管理查询已关闭','query_closed'));}if(active)await new Promise(resolve=>idle.push(resolve));}
  return {run,close,status:()=>({active,queued:queue.length,closed})};
}
