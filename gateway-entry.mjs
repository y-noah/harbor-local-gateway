import http from 'node:http';
import {forwardGatewayRequest,gatewayEntryRoute,gatewayEntryRequestBytes} from './gateway-entry-forward.mjs';

const fail=code=>Object.assign(new Error('网关准入状态无效'),{code});
function policy(value){
 const names=['maxInflightRequests','maxQueuedRequests','maxQueuedBytes','maxRequestBytes','maxWaitMs'];
 if(!value||Object.keys(value).some(name=>!names.includes(name))||names.some(name=>!Number.isSafeInteger(value[name])||value[name]<1)||value.maxRequestBytes>value.maxQueuedBytes||value.maxWaitMs>2147483647)throw fail('gateway_entry_limits_invalid');
 return Object.freeze({...value});
}
function unavailable(res,status=503,code='service_handover_busy'){
 if(res.destroyed)return;
 if(res.headersSent){res.destroy();return;}
 res.writeHead(status,{'Content-Type':'application/json','Connection':'close',...(status===503?{'Retry-After':'1'}:{})});res.end(JSON.stringify({error:{code,message:'服务繁忙，请稍后重试'}}));
}

// This is transport admission only. It neither grants a database writer lease
// nor claims that an existing application has quiesced its background work.
export function createGatewayEntry({route,limits}={}){
 let active=gatewayEntryRoute(route),state='active',inflight=0,queuedBytes=0,closing;
 const config=policy(limits),queue=[],forwarded=new Set(),waiters=new Set();
 const wake=()=>{if(inflight===0)for(const waiter of [...waiters])waiter.resolve();};
 const detach=item=>{clearTimeout(item.timer);item.req.off('aborted',item.cancel);item.res.off('close',item.cancel);};
 function remove(item){const index=queue.indexOf(item);if(index<0)return false;queue.splice(index,1);queuedBytes-=item.bytes;detach(item);return true;}
 function dispatch(req,res,selected){
  if(req.aborted||res.destroyed)return;
  inflight++;
  const task=Promise.resolve().then(()=>forwardGatewayRequest(req,res,{route:selected,maxRequestBytes:config.maxRequestBytes})).catch(error=>unavailable(res,error.status??502,error.code??'gateway_entry_upstream_failed')).finally(()=>{forwarded.delete(task);inflight--;release();wake();});
  forwarded.add(task);
 }
 const release=()=>{while(queue.length&&state==='active'&&inflight<config.maxInflightRequests){const item=queue[0];remove(item);dispatch(item.req,item.res,active);}};
 function handle(req,res){
  if(state==='closed'){unavailable(res);return;}
  let bytes;try{bytes=gatewayEntryRequestBytes(req,config.maxRequestBytes);}catch(error){unavailable(res,error.status,error.code);return;}
  if(state==='active'&&inflight<config.maxInflightRequests){dispatch(req,res,active);return;}
  if(queue.length>=config.maxQueuedRequests||queuedBytes+bytes>config.maxQueuedBytes){unavailable(res);return;}
  req.pause();
  const item={req,res,bytes,cancel:()=>remove(item)};item.timer=setTimeout(()=>{if(remove(item))unavailable(res);},config.maxWaitMs);item.timer.unref();
  queue.push(item);queuedBytes+=bytes;req.once('aborted',item.cancel);res.once('close',item.cancel);
 }
 // CONNECT and Upgrade detach sockets from Node's ordinary HTTP lifecycle.
 // A peer reset while writing the rejection must only close that connection.
 function rejectProtocol(socket,status){
  socket.on('error',()=>socket.destroy());
  socket.end(`HTTP/1.1 ${status}\r\nConnection: close\r\n\r\n`);
 }
 const server=http.createServer(handle);server.on('checkContinue',handle);
 server.on('connect',(_req,socket)=>rejectProtocol(socket,'405 Method Not Allowed'));
 server.on('upgrade',(_req,socket)=>rejectProtocol(socket,'426 Upgrade Required'));
 function hold(){if(state==='closed')throw fail('gateway_entry_closed');state='holding';return status();}
 function activate(route){if(state!=='holding'||inflight!==0)throw fail('gateway_entry_not_drained');active=gatewayEntryRoute(route);state='active';release();return status();}
 function resume(){if(state!=='holding')throw fail('gateway_entry_not_holding');state='active';release();return status();}
 function status(){return {state,inflight,queued:queue.length,reservedQueuedBytes:queuedBytes};}
 function waitForDrain({signal,timeoutMs}={}){
  if(state!=='holding'||!Number.isSafeInteger(timeoutMs)||timeoutMs<1||timeoutMs>2147483647)return Promise.reject(fail('gateway_entry_drain_invalid'));
  if(signal?.aborted)return Promise.reject(signal.reason);
  if(inflight===0)return Promise.resolve();
  return new Promise((resolve,reject)=>{
   const done=error=>{if(!waiters.delete(waiter))return;clearTimeout(timer);signal?.removeEventListener('abort',abort);error?reject(error):resolve();},waiter={resolve:()=>done(),reject:error=>done(error)},abort=()=>done(signal.reason),timer=setTimeout(()=>done(fail('gateway_entry_drain_timeout')),timeoutMs);
   waiters.add(waiter);signal?.addEventListener('abort',abort,{once:true});
  });
 }
 function close(){return closing??=(async()=>{state='closed';for(const item of [...queue]){remove(item);unavailable(item.res);}for(const waiter of [...waiters])waiter.reject(fail('gateway_entry_closed'));const stopped=new Promise((resolve,reject)=>server.close(error=>error&&error.code!=='ERR_SERVER_NOT_RUNNING'?reject(error):resolve()));server.closeIdleConnections();await Promise.all([stopped,...forwarded]);})();}
 return {server,hold,activate,resume,status,waitForDrain,close};
}
