import {AppError} from './store.mjs';

// Counts retained encoded payload sizes and explicit byte/string buffers, not
// V8 object overhead, GC or the network stack's internal receive buffers.
export function createOutputBudget({maxBufferedBytes=512*1024*1024,maxBytesPerOwner=Math.min(384*1024*1024,maxBufferedBytes),maxScopes=256,maxScopesPerOwner=Math.min(32,maxScopes)}={}){
  if(![maxBufferedBytes,maxBytesPerOwner].every(n=>Number.isSafeInteger(n)&&n>=256&&n<=8*1024**3)||maxBytesPerOwner>maxBufferedBytes||![maxScopes,maxScopesPerOwner].every(n=>Number.isInteger(n)&&n>=1&&n<=4096)||maxScopesPerOwner>maxScopes)throw new AppError(503,'上游输出资源配置无效','output_configuration');
  const owners=new Map();let bytes=0,scopes=0,peak=0,rejected=0;
  const busy=()=>{rejected++;return new AppError(503,'上游输出缓冲繁忙，本次请求未自动重试','upstream_buffer_busy');};
  function open(owner){
    if(typeof owner!=='string'||!owner||owner.length>256)throw new AppError(503,'上游输出归属无效','output_configuration');
    const group=owners.get(owner)||{bytes:0,scopes:0};if(scopes>=maxScopes||group.scopes>=maxScopesPerOwner||bytes>=maxBufferedBytes||group.bytes>=maxBytesPerOwner)throw busy();owners.set(owner,group);group.scopes++;scopes++;
    const buckets=new Map();let closed=false;
    const resize=(name,size)=>{if(closed)throw new AppError(503,'上游输出资源已释放','upstream_buffer_closed');if(typeof name!=='string'||!name||!Number.isSafeInteger(size)||size<0)throw new AppError(503,'上游输出字节计量无效','output_configuration');const delta=size-(buckets.get(name)||0);if(bytes+delta>maxBufferedBytes||group.bytes+delta>maxBytesPerOwner)throw busy();bytes+=delta;group.bytes+=delta;peak=Math.max(peak,bytes);if(size)buckets.set(name,size);else buckets.delete(name);};
    return {resize,move(from,to){if(closed)throw new AppError(503,'上游输出资源已释放','upstream_buffer_closed');if(from===to)return;const value=buckets.get(from)||0;if(value)buckets.set(to,(buckets.get(to)||0)+value);buckets.delete(from);},close(){if(closed)return;closed=true;const own=[...buckets.values()].reduce((sum,n)=>sum+n,0);bytes-=own;group.bytes-=own;buckets.clear();scopes--;group.scopes--;if(!group.scopes)owners.delete(owner);}};
  }
  return {open,status:()=>({bufferedBytes:bytes,scopes,owners:owners.size,peakBufferedBytes:peak,rejected,maxBufferedBytes,maxBytesPerOwner,maxScopes,maxScopesPerOwner})};
}

export async function readOutputText(response,{maxBytes,output,prefix='body'}={}){
  if(!Number.isSafeInteger(maxBytes)||maxBytes<1||maxBytes>8*1024**3)throw new AppError(503,'上游读取预算无效','output_configuration');
  const tooLarge=()=>new AppError(502,'上游响应超过读取上限，未重发请求','upstream_response_too_large');
  if(Number(response.headers.get('content-length'))>maxBytes){await response.body?.cancel().catch(()=>{});throw tooLarge();}
  if(!response.body)return {text:'',bytes:0};
  const reader=response.body.getReader();let bytes=0,chunks=[];
  try{
    for(;;){const item=await reader.read();if(item.done)break;const next=bytes+item.value.byteLength;if(next>maxBytes)throw tooLarge();output?.resize(prefix+':raw',next);bytes=next;chunks.push(item.value);}
    output?.resize(prefix+':concat',bytes);const joined=Buffer.concat(chunks,bytes);chunks=[];output?.resize(prefix+':raw',0);output?.resize(prefix+':text',bytes*2);const text=joined.toString('utf8');output?.resize(prefix+':text',text.length*2);output?.resize(prefix+':concat',0);return {text,bytes};
  }catch(error){await reader.cancel().catch(()=>{});throw error;}
  finally{chunks=[];output?.resize(prefix+':raw',0);output?.resize(prefix+':concat',0);reader.releaseLock();}
}
export async function readOutputJson(response,options){
  const {text,bytes}=await readOutputText(response,options);const {output,prefix='body',allowBom=false}=options;
  try{const bom=allowBom&&text.charCodeAt(0)===0xfeff;if(bom)output?.resize(prefix+':source',text.length*2);output?.resize(prefix+':payload',bytes);return JSON.parse(bom?text.slice(1):text);}finally{output?.resize(prefix+':text',0);output?.resize(prefix+':source',0);}
}

function quotedLength(text){let n=2;for(let i=0;i<text.length;i++){const c=text.charCodeAt(i);if(c===34||c===92||(c===8||c===9||c===10||c===12||c===13))n+=2;else if(c<32)n+=6;else if(c>=0xd800&&c<=0xdbff){const next=text.charCodeAt(i+1);if(next>=0xdc00&&next<=0xdfff){n+=2;i++;}else n+=6;}else if(c>=0xdc00&&c<=0xdfff)n+=6;else n++;}return n;}
// Exact UTF-16 JSON length for JSON data; no large string is allocated before
// its reservation. Object graph overhead is deliberately not called heap size.
export function jsonOutputLength(value){
  if(value===undefined||typeof value==='function'||typeof value==='symbol')throw new AppError(502,'上游返回了无法编码的JSON数据','upstream_response_encoding');
  const path=new Set(),stack=[{value}],skip=value=>value===undefined||typeof value==='function'||typeof value==='symbol';let size=0;
  while(stack.length){const frame=stack.at(-1);
    if(frame.container){const v=frame.container,array=Array.isArray(v),keys=frame.keys;if(frame.index>=(array?v.length:keys.length)){path.delete(v);stack.pop();continue;}const key=array?frame.index:keys[frame.index];frame.index++;const next=v[key];if(!array&&skip(next))continue;if(frame.count++)size++;if(!array)size+=quotedLength(key)+1;stack.push({value:skip(next)?null:next});continue;}
    stack.pop();const v=frame.value;if(v===null){size+=4;continue;}if(typeof v==='string'){size+=quotedLength(v);continue;}if(typeof v==='number'){size+=Number.isFinite(v)?String(v).length:4;continue;}if(typeof v==='boolean'){size+=v?4:5;continue;}if(skip(v)){size+=4;continue;}if(typeof v!=='object'||path.has(v)||typeof v.toJSON==='function')throw new AppError(502,'上游返回了无法编码的JSON数据','upstream_response_encoding');path.add(v);size+=2;stack.push({container:v,keys:Array.isArray(v)?null:Object.keys(v),index:0,count:0});
  }
  if(!Number.isSafeInteger(size))throw new AppError(503,'上游输出过大','upstream_buffer_busy');return size;
}

export async function writeOutput(res,value,{output,signal,prefix='',suffix='',status=200,headers={},timing,contentToken}={}){
  if(signal?.aborted||res.destroyed)throw new AppError(499,'客户端已断开','request_cancelled');
  const chars=prefix.length+jsonOutputLength(value)+suffix.length;output?.resize('send:text',chars*2);let text=prefix+JSON.stringify(value)+suffix;output?.resize('send:wire',Buffer.byteLength(text));const wire=Buffer.from(text);text=null;output?.resize('send:text',0);
  let timingToken,backpressured=false,writeSucceeded=false;
  try{timingToken=timing?.writeStart({bytes:wire.length,contentToken});if(timingToken&&typeof timingToken.then==='function'){void Promise.resolve(timingToken).catch(()=>{});timingToken=null;}}catch{}
  try{
    if(!res.headersSent)res.writeHead(status,headers);
    await new Promise((resolve,reject)=>{
      let settled=false;const cleanup=()=>{res.off('close',closed);res.off('error',failed);signal?.removeEventListener('abort',abort);};
      const done=error=>{if(settled)return;settled=true;cleanup();error?reject(error):resolve();};
      const closed=()=>done(new AppError(499,'客户端已断开','request_cancelled'));
      const abort=()=>{if(res.closed)closed();else res.destroy();},failed=()=>abort();
      res.once('close',closed);res.once('error',failed);signal?.addEventListener('abort',abort,{once:true});
      // A write error can precede close. Wait for the real close notification
      // so request cancellation and byte release share the same boundary.
      backpressured=!res.write(wire,error=>{if(error)failed();else{writeSucceeded=true;done();}});if(signal?.aborted||res.destroyed)abort();
    });
  }finally{try{const value=timing?.writeEnd(timingToken,{ok:writeSucceeded,backpressured});if(value&&typeof value.then==='function')void Promise.resolve(value).catch(()=>{});}catch{}output?.resize('send:wire',0);output?.resize('send:text',0);}
}
