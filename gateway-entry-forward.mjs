import http from 'node:http';
import {Transform} from 'node:stream';
import {resolve} from 'node:path';
import {gatewayClientAddress,gatewayClientAddressHeader} from './gateway-transport.mjs';

const hop=new Set(['connection','keep-alive','proxy-authenticate','proxy-authorization','te','trailer','transfer-encoding','upgrade']);
const fail=(code,status=502)=>Object.assign(new Error('请求转发未完成'),{code,status});
export function gatewayEntryRoute(value){
 if(!value||Object.keys(value).some(key=>!['socketPath'].includes(key))||typeof value.socketPath!=='string'||value.socketPath!==resolve(value.socketPath)||value.socketPath.includes('\0')||Buffer.byteLength(value.socketPath)>=108)throw fail('gateway_entry_route_invalid',503);
 return Object.freeze({socketPath:value.socketPath});
}
function headers(source,{request=false}={}){
 const connection=String(source.connection??'').toLowerCase().split(',').map(value=>value.trim()).filter(Boolean),drop=new Set([...hop,...connection]),result={};
 if(request&&connection.some(name=>['host','origin','cookie','authorization'].includes(name)))throw fail('gateway_entry_hop_header_invalid',400);
 for(const [name,value] of Object.entries(source)){
  const lower=name.toLowerCase();
  if(drop.has(lower)||request&&(lower==='forwarded'||lower.startsWith('x-forwarded-')||lower==='x-real-ip'||lower.startsWith('x-harbor-entry-')))continue;
  if(value!==undefined)result[lower]=value;
 }
 return result;
}
export function gatewayEntryRequestBytes(req,maxRequestBytes){
 const length=req.headers['content-length'];
 if(length!==undefined&&(typeof length!=='string'||!/^\d+$/.test(length)||!Number.isSafeInteger(Number(length))))throw fail('gateway_entry_length_invalid',400);
 if(length!==undefined&&Number(length)>maxRequestBytes)throw fail('gateway_entry_body_limit',413);
 // Include both CRLFs around the header block and the retained raw target.
 const head=Buffer.byteLength(`${req.method} ${req.url} HTTP/${req.httpVersion}\r\n`)+req.rawHeaders.reduce((sum,value)=>sum+Buffer.byteLength(value)+2,0)+2;
 return head+(length===undefined?maxRequestBytes:Number(length));
}
export function forwardGatewayRequest(req,res,{route,maxRequestBytes}={}){
 const selected=gatewayEntryRoute(route),address=gatewayClientAddress(req);
 if(!address)throw fail('gateway_entry_peer_unknown',400);
 if(!Number.isSafeInteger(maxRequestBytes)||maxRequestBytes<1)throw fail('gateway_entry_body_limit_invalid',503);
 if(typeof req.url!=='string'||!req.url.startsWith('/')||req.url.startsWith('//')||req.method==='CONNECT')throw fail('gateway_entry_target_invalid',400);
 gatewayEntryRequestBytes(req,maxRequestBytes);
 const outgoing=headers(req.headers,{request:true});outgoing[gatewayClientAddressHeader]=address;
 // IncomingMessage has removed chunk framing. Rebuild it for this hop rather
 // than relying on ClientRequest defaults for POST versus GET/DELETE.
 const encoding=req.headers['transfer-encoding'],length=req.headers['content-length'];
 if(encoding!==undefined){
  if(length!==undefined||typeof encoding!=='string'||!/(?:^|,)\s*chunked\s*$/i.test(encoding))throw fail('gateway_entry_framing_invalid',400);
  outgoing['transfer-encoding']=encoding;
 }else if(length!==undefined)outgoing['content-length']=length;
 return new Promise((resolve,reject)=>{
  let upstream,response,settled=false,bytes=0;
  const cleanup=()=>{req.off('aborted',cancel);res.off('close',closed);res.off('finish',finished);};
  const finish=error=>{if(settled)return;settled=true;cleanup();req.unpipe(counter);counter.unpipe();counter.destroy();upstream?.destroy();response?.destroy();if(error)reject(error);else resolve({forwarded:true});};
  const cancel=()=>finish(fail('gateway_entry_client_closed',499)),closed=()=>{if(!res.writableFinished)cancel();},finished=()=>finish();
  const counter=new Transform({transform(chunk,encoding,callback){bytes+=chunk.length;if(bytes>maxRequestBytes)callback(fail('gateway_entry_body_limit',413));else callback(null,chunk);}});
  counter.on('error',error=>finish(error));req.once('aborted',cancel);res.once('close',closed);res.once('finish',finished);
  if(req.aborted||req.destroyed&&!req.complete||res.destroyed){cancel();return;}
  upstream=http.request({socketPath:selected.socketPath,path:req.url,method:req.method,headers:outgoing,setHost:false,agent:false},incoming=>{
   response=incoming;
   if(settled){incoming.destroy();return;}
   incoming.once('error',()=>finish(fail('gateway_entry_upstream_failed')));
   incoming.once('aborted',()=>finish(fail('gateway_entry_upstream_failed')));
   const outgoing=headers(incoming.headers),encoding=incoming.headers['transfer-encoding'];
   // Node removes only chunk framing, not preceding transfer codings. Keep
   // those codings and let ServerResponse frame this hop without buffering.
   if(encoding!==undefined)outgoing['transfer-encoding']=/(?:^|,)\s*chunked\s*$/i.test(encoding)?encoding:`${encoding}, chunked`;
   if(!upstream.writableFinished)outgoing.connection='close';res.writeHead(incoming.statusCode??502,outgoing);res.flushHeaders();incoming.pipe(res);
  });
  upstream.once('error',()=>finish(fail('gateway_entry_upstream_failed')));
  upstream.on('continue',()=>{if(!settled&&!res.headersSent)res.writeContinue();});
  // Every request is sent to exactly one immutable route. No retry or replay
  // follows an error, even when the upstream may already have committed it.
  req.pipe(counter).pipe(upstream);upstream.flushHeaders();
 });
}
