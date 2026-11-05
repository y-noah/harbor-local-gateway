import {classifyStageContent,streamingStageClock} from '../../streaming-stage-observer.mjs';
import http from 'node:http';
import https from 'node:https';
import {createHash} from 'node:crypto';
import {performance} from 'node:perf_hooks';
import {currentCapacityObservation,observeCapacityHttp} from './observation-adapter.mjs';
import {capacityRemoteStageSourceBinding} from './remote-stage-source.mjs';
import {capacityDriverAgentBinding} from './request-driver.mjs';

const contentTypes=new Set(['response.output_text.delta','response.function_call_arguments.delta']);
const networkFailureCodes=new Set(['ECONNRESET','ECONNREFUSED','ETIMEDOUT','EPIPE','ENOTFOUND','EAI_AGAIN','ENETUNREACH','EHOSTUNREACH','ERR_TLS_CERT_ALTNAME_INVALID','CERT_HAS_EXPIRED','DEPTH_ZERO_SELF_SIGNED_CERT','SELF_SIGNED_CERT_IN_CHAIN','UNABLE_TO_VERIFY_LEAF_SIGNATURE','UNABLE_TO_GET_ISSUER_CERT_LOCALLY','ERR_SSL_WRONG_VERSION_NUMBER','UNKNOWN','OTHER','NOT_REPORTED']);
export const capacityNetworkFailureFlags=Object.freeze(['reusedSocket','socketAssigned','tcpConnectObserved','tlsSecureConnectObserved']);
// Diagnostic labels only: never a notSent proof or an instruction to retry.
export function sanitizeCapacityNetworkFailure(value){
 const result={code:'NOT_REPORTED',...Object.fromEntries(capacityNetworkFailureFlags.map(key=>[key,null]))};
 if(!value||typeof value!=='object')return Object.freeze(result);
 try{const code=value.code;result.code=networkFailureCodes.has(code)?code:typeof code==='string'?'OTHER':'UNKNOWN';for(const key of capacityNetworkFailureFlags){const flag=value[key];result[key]=typeof flag==='boolean'?flag:null;}}catch{result.code='UNKNOWN';for(const key of capacityNetworkFailureFlags)result[key]=null;}
 return Object.freeze(result);
}
const clientObservers=new WeakMap(),noClientStage=Object.freeze({content:()=>{},emit:()=>{},missing:()=>{}});
const stageTrace=value=>typeof value==='string'&&/^[a-zA-Z0-9._:-]{1,200}$/.test(value);
// This object is a private caller capability, never an HTTP payload setting.
// The selector receives only the final transport trace candidate, so an owned
// factory can verify its registry after the HTTP observer has rebound a trace.
export function createCapacityClientStageObserver(configuration){
 if(!configuration||Object.keys(configuration).some(key=>!['context','observe','remoteSource'].includes(key))||typeof configuration.context!=='function'||typeof configuration.observe!=='function')throw new Error('capacity_client_stage_configuration');
 const remoteSource=configuration.remoteSource;if(remoteSource!==undefined&&!capacityRemoteStageSourceBinding(remoteSource))throw Error('capacity_client_remote_source');
 const select=configuration.context,observe=configuration.observe;let failures=0,unavailable=0,selected=0,pendingCallbacks=0,closed=false,clock=null;try{clock=streamingStageClock();}catch{unavailable++;}
 const absorb=result=>{if(result&&typeof result.then==='function'){pendingCallbacks++;void Promise.resolve(result).then(value=>{pendingCallbacks--;if(value===false)failures++;},()=>{pendingCallbacks--;failures++;});return true;}return false;};
 const capture=candidate=>{
  if(closed){unavailable++;return noClientStage;}let trace;
  try{const context=select(Object.freeze({trace:candidate}));if(absorb(context)){failures++;return noClientStage;}if(context===null)return noClientStage;trace=context?.trace;}catch{failures++;return noClientStage;}
  if(!clock||!stageTrace(trace)||candidate!==undefined&&trace!==candidate){unavailable++;return noClientStage;}selected++;
  const emit=(type,details={},atNs=process.hrtime.bigint().toString())=>{if(closed){unavailable++;return false;}try{const result=observe(Object.freeze({trace,type,atNs,clock:remoteSource?.clock??clock,bound:'upper',...details}));if(result===false)failures++;else absorb(result);return result;}catch{failures++;return false;}};
  const content=(value,atNs)=>{try{const identity=classifyStageContent(value);if(!identity)return;if(identity.incomplete){unavailable++;emit('unavailable',{reason:'content_budget'},atNs);return;}emit('content',{ordinal:ordinal++,...identity},atNs);}catch{unavailable++;emit('unavailable',{reason:'content_invalid'},atNs);}};let ordinal=0;
  return Object.freeze({emit,content,...(remoteSource?{remoteSource}:{}),missing:reason=>{unavailable++;emit('unavailable',{reason});}});
 };
 const source=remoteSource??Object.freeze({role:'client',bound:'upper',clock,isOpen:()=>!closed&&clock!==null});
 const capability=Object.freeze({source,status:()=>({failures,unavailable,selected,pendingCallbacks,closed,bound:'upper'}),close:()=>{closed=true;}});clientObservers.set(capability,capture);return capability;
}
const captureClientStage=(capability,trace)=>{if(capability===undefined)return noClientStage;const capture=clientObservers.get(capability);if(!capture)throw new Error('capacity_client_stage_capability');return capture(trace);};
export function createSseProbe({clientStageObserver,...options}={}){return sseProbe({...options,stage:captureClientStage(clientStageObserver)});}
function sseProbe({maxEventBytes=1048576,clock=()=>performance.now(),stage=noClientStage}={}){
  if(!Number.isSafeInteger(maxEventBytes)||maxEventBytes<128||maxEventBytes>16777216)throw new Error('capacity_sse_budget_invalid');
  const decoder=new TextDecoder('utf-8',{fatal:true});let pending='',firstContentAt=null,completed=false,failed=false,events=0,completedId=null;
  function consume(){for(;;){const match=/\r?\n\r?\n/.exec(pending);if(!match)break;const event=pending.slice(0,match.index);pending=pending.slice(match.index+match[0].length);if(Buffer.byteLength(event)>maxEventBytes)throw new Error('capacity_sse_event_budget');const lines=event.split(/\r?\n/),body=lines.filter(line=>line.startsWith('data:')).map(line=>line.slice(5).trimStart()).join('\n');if(!body||body==='[DONE]')continue;let value;try{value=JSON.parse(body);}catch{throw new Error('capacity_sse_json_invalid');}events++;const type=value.type??lines.find(line=>line.startsWith('event:'))?.slice(6).trim(),parsedAtNs=process.hrtime.bigint().toString();
      const output=value.response?.output??[],fullContent=output.some(item=>item?.type==='message'&&item.content?.some(part=>typeof part.text==='string'&&part.text.length)||item?.type==='function_call'&&typeof item.name==='string'&&item.name.length);
      if(firstContentAt===null&&(contentTypes.has(type)&&typeof value.delta==='string'&&value.delta.length||type==='response.completed'&&fullContent))firstContentAt=clock();
      if(type==='response.completed'&&value.response?.status==='completed'){completed=true;completedId=typeof value.response.id==='string'?value.response.id:null;}if(['error','response.failed','response.incomplete'].includes(type))failed=true;stage.content(value.type===undefined?{...value,type}:value,parsedAtNs);
    }if(Buffer.byteLength(pending)>maxEventBytes)throw new Error('capacity_sse_event_budget');}
  return {write:chunk=>{pending+=decoder.decode(chunk,{stream:true});consume();},finish:()=>{pending+=decoder.decode();consume();if(pending.trim())throw new Error('capacity_sse_trailing_event');return {events,firstContentAt,completed,failed,completedId};}};
}
export function createRecordCounter({format='jsonl',header=false}={}){
  if(!['jsonl','csv'].includes(format)||typeof header!=='boolean')throw new Error('capacity_record_counter_invalid');let count=0,inQuotes=false,pendingQuote=false,content=false;
  return {write:chunk=>{for(const value of chunk){if(format==='csv'&&inQuotes){if(pendingQuote){if(value===34){pendingQuote=false;continue;}inQuotes=false;pendingQuote=false;}else{if(value===34)pendingQuote=true;continue;}}if(format==='csv'&&value===34){inQuotes=true;content=true;}else if(value===10){if(content||format==='csv')count++;content=false;}else if(value!==13&&value!==32&&value!==9)content=true;}},finish:()=>{if(inQuotes&&!pendingQuote)throw new Error('capacity_export_csv_incomplete');return Math.max(0,count+(content?1:0)-(header?1:0));}};
}
export function credentialFingerprints(headers={}){
  const result={authorization:null,cookie:null},seen=new Set();for(const [key,value] of Object.entries(headers)){const name=key.toLowerCase();if(!Object.hasOwn(result,name))continue;if(seen.has(name)||typeof value!=='string')throw new Error('capacity_credential_header_invalid');seen.add(name);result[name]=createHash('sha256').update(value).digest('hex');}return result;
}
export function requestSignature(method,path){const url=new URL(path,'http://capacity.invalid'),route=url.pathname.replace(/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}/gi,':id');return method+' '+route+(route==='/api/admin/requests'?'?view='+(url.searchParams.get('view')||'all'):'');}
// A positive socket ceiling lets Node honor the peer's keep-alive hint; it is
// separate from each request's AbortSignal deadline and does not add retries.
export function capacityAgent(origin,{connection='reuse-tls',timeout=capacityHttpDefaultTimeoutMs,...tls}={}){const target=new URL(origin);if(!['http:','https:'].includes(target.protocol)||target.username||target.password)throw new Error('capacity_origin_invalid');const Agent=target.protocol==='https:'?https.Agent:http.Agent;return new Agent({...tls,timeout,keepAlive:connection==='reuse-tls',maxCachedSessions:connection==='new-tls'?0:100,maxSockets:10000,rejectUnauthorized:true});}
export const capacityHttpDefaultTimeoutMs=600000;
export function requestCapacityTarget(options={}){
  const active=currentCapacityObservation();return active&&!active.httpObserved?observeCapacityHttp(options,dispatchCapacityTarget):dispatchCapacityTarget(options);
}
function dispatchCapacityTarget(options={}){
 const driver=capacityDriverAgentBinding(options.agent);if(!driver)return dispatchNativeCapacityTarget(options);
 if(options.origin!==driver.origin)throw Error('capacity_driver_origin_mismatch');
 const captured=Object.fromEntries(Object.entries(options.headers??{}).map(([key,value])=>[key,Array.isArray(value)?Object.freeze([...value]):value]));credentialFingerprints(captured);const headers=Object.freeze(Object.fromEntries(Object.entries(captured).map(([key,value])=>[key.toLowerCase(),value]))),requestOptions={...options,headers},stage=captureClientStage(options.clientStageObserver,headers['x-capacity-trace']);
 const receive=value=>{if(!['content','end','cancel','timeout','failed','unavailable'].includes(value.type))return stage.missing('driver_stage_invalid');const {type,atNs,...details}=value;return stage.emit(type,details,atNs);};
 return driver.request(requestOptions,{onHeaders:options.onResponseHeaders,onStage:receive,...(stage.remoteSource?{onMappedStage:receive,mappedStageSource:stage.remoteSource}:{})}).catch(error=>{const emit=()=>stage.emit('failed',{outcome:'failed',reason:'driver_source_unavailable'});if(driver.observeDriverFailure)driver.observeDriverFailure(emit);else emit();throw error;});
}
function nativeSocketEvidence(req){
 try{const socket=req?.socket;if(!socket)return null;const hash=value=>typeof value==='string'?createHash('sha256').update(value).digest('hex'):null,peer=socket.getPeerCertificate?.();return {source:'native-http-socket',localAddressHash:hash(socket.localAddress),peerAddressHash:hash(socket.remoteAddress),localPort:socket.localPort??null,peerPort:socket.remotePort??null,loopbackPeer:['127.0.0.1','::1','::ffff:127.0.0.1'].includes(socket.remoteAddress),encrypted:socket.encrypted===true,tlsAuthorized:socket.authorized===true,peerCertificateHash:peer?.raw?createHash('sha256').update(peer.raw).digest('hex'):null};}catch{return null;}
}
function dispatchNativeCapacityTarget({origin,path,method='GET',headers={},body,mode='json',rowFormat=null,csvHeader=false,agent,signal,timeoutMs=capacityHttpDefaultTimeoutMs,maxResponseBytes=2097152,onResponseHeaders,clientStageObserver,clock=()=>performance.now()}={}){
  const capturedHeaders=Object.fromEntries(Object.entries(headers).map(([key,value])=>[key,Array.isArray(value)?Object.freeze([...value]):value])),credentials=credentialFingerprints(capturedHeaders),wireHeaders=Object.freeze(Object.fromEntries(Object.entries(capturedHeaders).map(([key,value])=>[key.toLowerCase(),value]))),signature=requestSignature(method,path);const base=new URL(origin);if(!['http:','https:'].includes(base.protocol)||base.username||base.password||typeof path!=='string'||!path.startsWith('/')||path.startsWith('//')||/[\r\n]/.test(path)||!['json','sse','download'].includes(mode)||!Number.isSafeInteger(timeoutMs)||timeoutMs<1||!Number.isSafeInteger(maxResponseBytes)||maxResponseBytes<128||onResponseHeaders!==undefined&&typeof onResponseHeaders!=='function')throw new Error('capacity_http_configuration');const target=new URL(path,base);if(target.origin!==base.origin)throw new Error('capacity_redirect_forbidden');const bytes=body===undefined?null:Buffer.isBuffer(body)?body:Buffer.from(typeof body==='string'?body:JSON.stringify(body)),transport=target.protocol==='https:'?https:http,start=clock(),deadline=AbortSignal.any([AbortSignal.timeout(timeoutMs),signal].filter(Boolean));
  const traceCandidate=typeof wireHeaders['x-capacity-trace']==='string'&&/^[a-zA-Z0-9-]{1,100}$/.test(wireHeaders['x-capacity-trace'])?wireHeaders['x-capacity-trace']:null,stage=captureClientStage(clientStageObserver,traceCandidate);if(stage.remoteSource)throw Error('capacity_client_remote_source_native_mismatch');
  return new Promise((resolve,reject)=>{let settled=false,httpEnded=false,cancelObserved=false,stageFailure='connection_failed',received=0,newlineRows=0,previousByte=null,firstByteAt=null,headersAt=null,connectedAt=null,tlsAt=null,reused=false,wireSent=false,socketAssigned=false;const listeners=[];const chunks=[],counter=rowFormat?createRecordCounter({format:rowFormat,header:csvHeader}):null,digest=createHash('sha256'),probe=mode==='sse'?sseProbe({clock,stage}):null;
    const networkFailure=error=>{try{return sanitizeCapacityNetworkFailure({code:error?.code??'UNKNOWN',reusedSocket:req?.reusedSocket===true,socketAssigned,tcpConnectObserved:connectedAt!==null,tlsSecureConnectObserved:tlsAt!==null});}catch{return sanitizeCapacityNetworkFailure({code:'UNKNOWN'});}};
    const observeAbort=()=>{if(settled||httpEnded||cancelObserved)return;cancelObserved=true;stage.emit(signal?.aborted?'cancel':'timeout',{outcome:'cancelled'});};deadline.addEventListener('abort',observeAbort,{once:true});if(deadline.aborted){cancelObserved=true;stage.missing('cancel_before_observation');}
    const clean=()=>{deadline.removeEventListener('abort',observeAbort);for(const [socket,event,listener] of listeners)socket.off(event,listener);};
    const finish=(value,ended)=>{if(settled)return;settled=true;clean();const result={startedAt:start,finishedAt:clock(),durationMs:clock()-start,receivedBytes:received,lineCount:newlineRows+(received&&previousByte!==10?1:0),wireSent,firstByteAt,headersAt,connectedAt,tlsAt,reusedSocket:reused,socketEvidence:!socketChanged&&req?.socket===evidenceSocket?socketSnapshot:null,credentials,requestSignature:signature,trace:traceCandidate,httpEnded:false,...value};if(ended)stage.emit('end',{outcome:ended.outcome},ended.atNs);else if(!httpEnded&&!cancelObserved)stage.emit('failed',{outcome:'failed',reason:stageFailure});resolve(result);};
    let evidenceSocket=null,socketSnapshot=null,socketChanged=false;
    const captureSocket=()=>{const socket=req?.socket;if(!socket)return;if(evidenceSocket&&evidenceSocket!==socket){socketChanged=true;return;}evidenceSocket=socket;const value=nativeSocketEvidence(req);if(!value||['localAddressHash','peerAddressHash','localPort','peerPort'].some(key=>value[key]===null))return;if(socketSnapshot&&['localAddressHash','peerAddressHash','localPort','peerPort','peerCertificateHash','encrypted','tlsAuthorized'].some(key=>socketSnapshot[key]!==value[key])){socketChanged=true;return;}socketSnapshot=value;};
    let req;try{req=transport.request(target,{method,headers:{...(bytes?{'content-length':bytes.length}:{}),...wireHeaders},agent,signal:deadline},res=>{captureSocket();const okStatus=res.statusCode>=200&&res.statusCode<300;headersAt=clock();reused=!!req.reusedSocket;if(onResponseHeaders)try{Promise.resolve(onResponseHeaders(Object.fromEntries(Object.entries(res.headers).map(([key,value])=>[key,Array.isArray(value)?[...value]:value])))).catch(()=>{});}catch{}res.on('data',chunk=>{try{if(firstByteAt===null)firstByteAt=clock();received+=chunk.length;digest.update(chunk);counter?.write(chunk);for(const value of chunk)if(value===10)newlineRows++;previousByte=chunk.at(-1);if(mode==='sse'&&okStatus)probe.write(chunk);else if(mode==='json'){if(received>maxResponseBytes)throw new Error('capacity_http_response_budget');chunks.push(chunk);}}catch(error){stageFailure='response_invalid';res.destroy(error);}});
      res.once('end',()=>{
        if(settled)return;httpEnded=true;const endNs=process.hrtime.bigint().toString();let value,outcome='failed';
        try{const stream=okStatus?probe?.finish():null;let json;if(mode==='json')try{json=JSON.parse(Buffer.concat(chunks).toString('utf8'));}catch{if(okStatus)throw new Error('capacity_http_json_invalid');}
          const status=res.statusCode,ok=status>=200&&status<300,complete=mode!=='sse'||!!stream?.completed&&!stream.failed&&stream.firstContentAt!==null,records=counter?.finish()??null,bodyHash=digest.digest('hex');
          value={status,success:ok&&complete,complete,httpEnded:true,records,firstContentAt:stream?.firstContentAt??null,completedId:stream?.completedId??null,bodyHash,...(json!==undefined?{json}:{}),error:!ok?'http_'+status:!complete?'stream_incomplete':null};outcome=ok&&complete?'ok':status>=400&&status<500?'rejected':'failed';
        }catch(error){value={status:res.statusCode,success:false,complete:false,error:error.message};}
        finish(value,{outcome,atNs:endNs});
      });
      res.once('aborted',()=>{if(stageFailure==='connection_failed')stageFailure='response_aborted';finish({status:res.statusCode,success:false,complete:false,error:'response_aborted',networkFailure:networkFailure()});});res.once('error',error=>{if(stageFailure==='connection_failed')stageFailure='response_stream_error';finish({status:res.statusCode,success:false,complete:false,error:'response_stream_error',networkFailure:networkFailure(error)});});
    });
    req.once('finish',()=>{wireSent=true;});req.on('socket',socket=>{socketAssigned=true;if(!socket.connecting){reused=true;captureSocket();return;}const connected=()=>{connectedAt=clock();if(target.protocol==='http:')captureSocket();},secure=()=>{tlsAt=clock();captureSocket();};socket.once('connect',connected);listeners.push([socket,'connect',connected]);if(target.protocol==='https:'){socket.once('secureConnect',secure);listeners.push([socket,'secureConnect',secure]);}});req.once('error',error=>finish({status:null,success:false,complete:false,error:deadline.aborted?(signal?.aborted?'client_cancelled':'client_deadline'):'connection_failed',networkFailure:networkFailure(error)}));req.end(bytes??undefined);
    }catch(error){if(!settled){settled=true;clean();stage.emit('failed',{outcome:'failed',reason:'request_not_sent'});}try{req?.destroy();}catch{}reject(error);}
  });
}
