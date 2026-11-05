import {createStreamingStageObserver} from './streaming-stage-observer.mjs';
import {AppError} from './store.mjs';
import {readJsonBody,responseBodyBytes} from './request-body.mjs';

// Byte accounting covers retained payload sizes and explicitly allocated body
// buffers. It is not an RSS/heap meter: JSON object overhead and GC are separate.
export function createRequestBodyBudget({maxBufferedBytes=512*1024*1024,maxReaders=16,maxBufferedBytesPerOwner=Math.min(3*responseBodyBytes,maxBufferedBytes),maxReadersPerOwner=Math.min(8,maxReaders),parse,stageObserver}={}){
  if(!Number.isSafeInteger(maxBufferedBytes)||maxBufferedBytes<256||maxBufferedBytes>8*1024*1024*1024||!Number.isInteger(maxReaders)||maxReaders<1||maxReaders>128||!Number.isSafeInteger(maxBufferedBytesPerOwner)||maxBufferedBytesPerOwner<256||maxBufferedBytesPerOwner>maxBufferedBytes||!Number.isInteger(maxReadersPerOwner)||maxReadersPerOwner<1||maxReadersPerOwner>maxReaders||parse!==undefined&&typeof parse!=='function')throw new AppError(503,'请求体资源配置无效','body_configuration');
  const stages=createStreamingStageObserver(stageObserver);
  const requests=new WeakMap(),owners=new Map();let readers=0,leases=0,bytes=0,peak=0,rejections=0;
  const busy=()=>{rejections++;return new AppError(503,'请求体缓冲繁忙，请稍后重试','request_body_busy');};
  function scope(owner,stage){
    const buckets=new Map();let open=false,reading=false,released=false;
    const resize=(name,size)=>{if(!open||released)throw new AppError(503,'请求体资源已释放','body_scope_closed');if(!Number.isSafeInteger(size)||size<0)throw new AppError(503,'请求体字节计量无效','body_configuration');const change=size-(buckets.get(name)||0),group=owners.get(owner),next=bytes+change;if(next>maxBufferedBytes||group.bytes+change>maxBufferedBytesPerOwner)throw busy();bytes=next;group.bytes+=change;peak=Math.max(peak,bytes);buckets.set(name,size);};
    return {
      start(length){if(open||released)throw new AppError(503,'请求体资源重复准入','body_scope_closed');const group=owners.get(owner)||{readers:0,leases:0,bytes:0};if(readers>=maxReaders||group.readers>=maxReadersPerOwner||bytes+length>maxBufferedBytes||group.bytes+length>maxBufferedBytesPerOwner)throw busy();owners.set(owner,group);open=true;reading=true;readers++;leases++;group.readers++;group.leases++;stage.emit('lease',{lease:'body',action:'acquire'});resize('wire',length);},
      resize,
      complete(size){const own=[...buckets.values()].reduce((sum,n)=>sum+n,0),group=owners.get(owner),next=bytes-own+size;if(next>maxBufferedBytes||group.bytes-own+size>maxBufferedBytesPerOwner)throw busy();bytes=next;group.bytes+=size-own;buckets.clear();buckets.set('payload',size);if(reading){reading=false;readers--;group.readers--;}},
      release(){if(released)return;released=true;if(!open)return;const own=[...buckets.values()].reduce((sum,n)=>sum+n,0),group=owners.get(owner);bytes-=own;group.bytes-=own;buckets.clear();leases--;group.leases--;if(reading){reading=false;readers--;group.readers--;}if(!group.leases)owners.delete(owner);stage.emit('lease',{lease:'body',action:'release'});}
    };
  }
  function release(req){const entry=requests.get(req);if(!entry)return;requests.delete(req);entry.release();}
  async function read(req,options={}){
    if(requests.has(req))throw new AppError(400,'请求体已读取','request_body_consumed');const owner=options.owner??'shared';if(typeof owner!=='string'||!owner||owner.length>256)throw new AppError(503,'请求体资源归属无效','body_configuration');const entry=scope(owner,stages.capture());requests.set(req,entry);
    try{return await readJsonBody(req,{...options,accounting:entry,parse});}
    catch(error){release(req);req.resume();throw error;}
  }
  return {read,release,stageObserverStatus:stages.status,status:()=>({readers,leases,bufferedBytes:bytes,peakBufferedBytes:peak,rejections,maxBufferedBytes,maxReaders,maxBufferedBytesPerOwner,maxReadersPerOwner,owners:owners.size})};
}
