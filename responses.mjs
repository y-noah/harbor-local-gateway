import {readFileSync,lstatSync} from 'node:fs';
import {join} from 'node:path';
import {spawn,execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {StringDecoder} from 'node:string_decoder';
import {randomUUID} from 'node:crypto';
import {runnerEnv,codexAuthArgs} from './codex.mjs';
import {AppError} from './store.mjs';

const upstreamOrigin='https://chatgpt.com/backend-api/codex';
const authError=(confirmed=false)=>Object.assign(new AppError(503,'订阅凭据不可用，请检查账号登录','upstream_authentication'),confirmed?{routeFailure:'login_expired'}:{});
const refreshError=()=>new AppError(503,'订阅凭据刷新暂未完成，请稍后重试','upstream_auth_refresh');
export function structuredFailure(value){
  const error=value?.error||value,code=error?.code||error?.type;
  if(code==='usage_limit_reached')return Object.assign(new AppError(429,'上游订阅额度已耗尽','upstream_quota_exhausted'),{routeFailure:'quota_exhausted'});
  if(['refresh_token_expired','refresh_token_revoked','refresh_token_reused','refresh_token_invalidated'].includes(code))return authError(true);
  return null;
}
async function errorPayload(response){
  if(!response.body)return null;
  const reader=response.body.getReader(),chunks=[];let size=0;
  let timer;const timeout=new Promise(resolve=>{timer=setTimeout(()=>{void reader.cancel().catch(()=>{});resolve(null);},2000);});
  try{return await Promise.race([timeout,(async()=>{while(true){const {done,value}=await reader.read();if(done)break;size+=value.length;if(size>65536)return null;chunks.push(value);}try{return JSON.parse(Buffer.concat(chunks).toString('utf8'));}catch{return null;}})()]);}
  catch{return null;}finally{clearTimeout(timer);void reader.cancel().catch(()=>{});}
}
const versions=new Map();
async function installedVersion(binary){
  if(!versions.has(binary)){
    let stdout;try{({stdout}=await promisify(execFile)(binary,['--version'],{timeout:5000,maxBuffer:4096,windowsHide:true}));}catch{throw new AppError(503,'无法读取官方 CLI 版本','upstream_configuration');}
    const value=stdout.match(/codex-cli (\d+\.\d+\.\d+(?:[-.][a-zA-Z0-9.-]+)?)/)?.[1];
    if(!value)throw new AppError(503,'无法读取官方 CLI 版本','upstream_configuration');versions.set(binary,value);
  }
  return versions.get(binary);
}
export function refreshCredentials(binary,home,{spawnProcess=spawn,timeoutMs=15000}={}){
  return new Promise((resolve,reject)=>{
    const child=spawnProcess(binary,['app-server','--listen','stdio://','--disable','plugins','--disable','apps',...codexAuthArgs()],{env:runnerEnv(home),windowsHide:true});
    let settled=false,buffer='',bytes=0;const decoder=new StringDecoder('utf8');
    const finish=error=>{if(settled)return;settled=true;clearTimeout(timer);child.kill();error?reject(error):resolve();};
    const timer=setTimeout(()=>finish(refreshError()),timeoutMs);
    const send=value=>child.stdin.write(JSON.stringify(value)+'\n');
    child.on('error',()=>finish(refreshError()));child.on('close',()=>finish(refreshError()));child.stderr.on('data',()=>{});child.stdin.on('error',()=>{});
    child.stdout.on('data',chunk=>{
      if(settled)return;bytes+=chunk.length;if(bytes>1024*1024)return finish(refreshError());buffer+=decoder.write(chunk);
      let at;while((at=buffer.indexOf('\n'))!==-1){const line=buffer.slice(0,at);buffer=buffer.slice(at+1);let event;try{event=JSON.parse(line);}catch{continue;}
        if(event.id===1){if(event.error)return finish(refreshError());send({method:'initialized',params:{}});send({method:'account/read',id:2,params:{refreshToken:true}});}
        if(event.id===2)finish(event.error?(structuredFailure(event.error.data)||refreshError()):event.result?.account===null?authError(true):event.result?.account?.type!=='chatgpt'?refreshError():null);
      }
    });
    send({method:'initialize',id:1,params:{clientInfo:{name:'harbor_local_gateway',version:'1.0.0'}}});
  });
}
const refreshing=new Map();
export async function accountCredentials(binary,home,{force=false}={}){
  const load=()=>{
    try{
      const file=join(home,'auth.json'),stat=lstatSync(file);
      if(!stat.isFile()||stat.size>1024*1024)throw authError();
      const value=JSON.parse(readFileSync(file,'utf8')).tokens;
      if(typeof value?.access_token!=='string'||!value.access_token||typeof value.account_id!=='string'||!value.account_id)throw authError();
      return {token:value.access_token,accountId:value.account_id};
    }catch{throw authError();}
  };
  let credentials=load(),expires=Infinity;
  try{const value=JSON.parse(Buffer.from(credentials.token.split('.')[1],'base64url'));if(Number.isFinite(value.exp))expires=value.exp*1000;}catch{}
  if(force||expires<Date.now()+60000){
    if(!refreshing.has(home)){const pending=refreshCredentials(binary,home).finally(()=>refreshing.delete(home));refreshing.set(home,pending);}
    await refreshing.get(home);credentials=load();
  }
  return credentials;
}
export function validateResponses(value){
  if(typeof value.model!=='string'||!value.model)throw new AppError(400,'请指定有效模型名称','invalid_model');
  if(value.stream!==undefined&&typeof value.stream!=='boolean')throw new AppError(400,'stream 必须为布尔值');
  if(value.previous_response_id)throw new AppError(400,'请使用 HTTP Responses 完整历史重放，不支持 previous_response_id','unsupported_continuation');
  if(value.background===true)throw new AppError(400,'不支持后台响应','unsupported_background');
  if(value.store===true)throw new AppError(400,'网关仅支持 store=false','unsupported_storage');
  return {...value,instructions:value.instructions??'',store:false,stream:true};
}
// Only Codex inference/tool routes belong here, never account, chat or memory APIs.
export const codexJsonRoutes=new Set(['/images/generations','/images/edits']);
export function validateCodexJson(value,path){
  if(!codexJsonRoutes.has(path))throw new AppError(404,'接口不存在');
  if(typeof value.model!=='string'||!value.model)throw new AppError(400,'请指定有效模型名称','invalid_model');
  if(value.stream!==undefined&&value.stream!==false)throw new AppError(400,'此 Codex 工具接口使用 JSON 响应','unsupported_stream');
  if(value.store===true||value.background===true||value.previous_response_id)throw new AppError(400,'订阅工具不支持保存响应或后台续传','unsupported_storage');
  if(typeof value.prompt!=='string'||!value.prompt.trim())throw new AppError(400,'图片请求缺少 prompt','invalid_image_prompt');
  if(path==='/images/edits'&&(!Array.isArray(value.images)||!value.images.length||value.images.some(image=>!image||typeof image.image_url!=='string'||!/^data:image\/(png|jpeg|webp);base64,[A-Za-z0-9+/]+={0,2}$/.test(image.image_url)||Object.keys(image).some(k=>k!=='image_url'))))throw new AppError(400,'改图仅支持内联 PNG、JPEG 或 WebP 参考图','invalid_image_reference');
  if(path==='/images/generations'&&value.images!==undefined)throw new AppError(400,'参考图请使用改图接口','invalid_image_reference');
  return {...value};
}
function failure(status){
  if(status===401)return authError(true);
  if(status===403)return new AppError(403,'上游拒绝访问，不能据此判定登录失效','upstream_forbidden');
  if(status===429)return new AppError(429,'上游订阅限流或额度不足，请稍后重试','upstream_rate_limit');
  if(status===400||status===422)return new AppError(400,'上游不接受当前模型参数或工具协议，请检查客户端配置','upstream_invalid_request');
  if(status===413)return new AppError(413,'官方服务拒绝了过大的请求体','upstream_payload_too_large');
  if(status===404)return new AppError(404,'上游模型或接口不存在','upstream_not_found');
  return new AppError(502,'上游服务暂不可用','upstream_error');
}
export function responseUsage(response){
  const usage=response?.usage;
  if(!usage||!['input_tokens','output_tokens'].every(k=>Number.isSafeInteger(usage[k])&&usage[k]>=0))return null;
  const cached=usage.input_tokens_details?.cached_tokens??0;
  if(!Number.isSafeInteger(cached)||cached<0||cached>usage.input_tokens)return null;
  return {input:usage.input_tokens,output:usage.output_tokens,cached};
}
export function createResponsesTransport({fetchImpl=fetch,credentials=accountCredentials,version=installedVersion}={}){
  const catalogs=new Map();
  async function request(binary,home,path,{payload,signal,sessionId,protocolHeaders={},accept='text/event-stream'}={}){
    let auth;try{auth=await credentials(binary,home);}catch(error){throw Object.assign(error,{usageKnown:true});}
    const headers={'Authorization':'Bearer '+auth.token,'ChatGPT-Account-ID':auth.accountId,'Accept':accept,'Content-Type':'application/json'};
    if(sessionId){headers['session-id']=sessionId;headers['thread-id']=sessionId;}
    if(path.startsWith('/images/'))headers['x-codex-image-turn-id']=randomUUID();
    for(const name of ['originator','openai-beta','x-codex-beta-features','x-openai-internal-codex-responses-lite']){
      const value=protocolHeaders[name];if(typeof value==='string'&&/^[\x20-\x7e]*$/.test(value))headers[name]=value;
    }
    let result;
    try{result=await fetchImpl(upstreamOrigin+path,{method:payload?'POST':'GET',headers,body:payload?JSON.stringify(payload):undefined,signal,redirect:'error'});}
    catch{throw Object.assign(new AppError(502,'连接上游失败或请求已取消','upstream_connection'),{usageKnown:!payload});}
    if(!result.ok){const details=await errorPayload(result);const classified=[401,403,429].includes(result.status)?structuredFailure(details):null;throw Object.assign(classified||failure(result.status),{usageKnown:!payload||(result.status>=400&&result.status<500&&result.status!==408)});}
    return result;
  }
  async function runJson({binary,home,path,payload,sessionId,signal,protocolHeaders}){
    if(!codexJsonRoutes.has(path))throw new AppError(404,'接口不存在');
    let usage=null;
    try{
      const result=await request(binary,home,path,{payload,signal,sessionId,protocolHeaders,accept:'application/json'});
      let response;try{response=await result.json();}catch{throw new AppError(502,'上游工具返回了无效 JSON','upstream_protocol');}
      if(!response||typeof response!=='object'||Array.isArray(response))throw new AppError(502,'上游工具响应无效','upstream_protocol');
      usage=responseUsage(response);
      if(!Array.isArray(response.data)||!response.data.length||response.data.some(image=>typeof image?.b64_json!=='string'||!image.b64_json))throw new AppError(502,'上游图片响应缺少图片数据','upstream_protocol');
      return {input:0,output:0,cached:0,...usage,usageKnown:!!usage,response};
    }catch(error){
      if(signal?.aborted)error=new AppError(499,'客户端已取消或服务停止','request_cancelled');
      if(!(error instanceof AppError))error=new AppError(502,'上游工具连接中断','upstream_connection');
      throw Object.assign(error,usage||{},{usageKnown:!!usage||error.usageKnown===true});
    }
  }
  async function models({binary,home,signal,force=false}){
    const cached=catalogs.get(home);if(!force&&cached?.expires>Date.now())return cached.models;
    const result=await request(binary,home,'/models?client_version='+encodeURIComponent(await version(binary)),{signal:signal||AbortSignal.timeout(15000)});
    let value;try{value=await result.json();}catch{throw new AppError(502,'模型目录响应无效','upstream_protocol');}
    if(!Array.isArray(value.models)||!value.models.length||value.models.some(m=>typeof m.slug!=='string'))throw new AppError(502,'模型目录响应无效','upstream_protocol');
    catalogs.set(home,{expires:Date.now()+60000,models:value.models});return value.models;
  }
  async function run({binary,home,payload,sessionId,signal,protocolHeaders,collectOutput=true,onEvent=async()=>{}}){
    const controller=new AbortController(),abort=()=>controller.abort();signal?.addEventListener('abort',abort,{once:true});if(signal?.aborted)abort();
    let completed=null,knownUsage=null,dispatched=false;
    const finishedItems=new Map();
    try{
      dispatched=true;const response=await request(binary,home,'/responses',{payload,sessionId,protocolHeaders,signal:controller.signal});
      // Some Codex subscription responses omit Content-Type; validate their SSE frames below.
      const contentType=response.headers.get('content-type');
      if(contentType&&!contentType.includes('text/event-stream')){await response.body?.cancel();throw new AppError(502,'上游未返回 Responses 事件流','upstream_protocol');}
      const decoder=new TextDecoder();let buffer='';
      const consume=async frame=>{
        const data=frame.split('\n').filter(l=>l.startsWith('data:')).map(l=>l.slice(5).trimStart()).join('\n');if(!data||data==='[DONE]')return;
        let event;try{event=JSON.parse(data);}catch{throw new AppError(502,'上游事件格式无效','upstream_protocol');}
        if(typeof event.type!=='string'||!/^[-a-zA-Z0-9_.]+$/.test(event.type))throw new AppError(502,'上游事件缺少类型','upstream_protocol');
        if(event.type==='response.completed'){
          if(completed)throw new AppError(502,'上游重复结束事件','upstream_protocol');
          knownUsage=responseUsage(event.response);
          completed=event;return;
        }
        if(['response.failed','response.incomplete','error'].includes(event.type)){
          knownUsage=responseUsage(event.response);throw structuredFailure(event.response?.error||event.error||event)||new AppError(502,'上游响应失败或未完整结束','upstream_incomplete');
        }
        if(completed)throw new AppError(502,'上游结束后仍返回事件','upstream_protocol');
        if(collectOutput&&event.type==='response.output_item.done'&&Number.isSafeInteger(event.output_index)&&event.output_index>=0&&event.item)finishedItems.set(event.output_index,event.item);
        await onEvent(event,controller.signal);
      };
      for await(const chunk of response.body){
        buffer=(buffer+decoder.decode(chunk,{stream:true})).replaceAll('\r\n','\n');
        let at;while((at=buffer.indexOf('\n\n'))!==-1){await consume(buffer.slice(0,at));buffer=buffer.slice(at+2);}
      }
      buffer+=decoder.decode();if(buffer.trim())await consume(buffer);
      if(!completed)throw new AppError(502,'上游连接提前结束，本次用量可能未计入累计','upstream_incomplete');
      // Keep streamed events intact; non-streaming clients also need items from done events.
      const responseBody=(!completed.response?.output?.length&&finishedItems.size)
        ?{...completed.response,output:[...finishedItems].sort(([a],[b])=>a-b).map(([,item])=>item)}:completed.response;
      return {input:0,output:0,cached:0,...knownUsage,usageKnown:!!knownUsage,terminal:completed,response:responseBody};
    }catch(error){
      if(controller.signal.aborted)error=signal?.aborted?new AppError(499,'客户端已取消或服务停止','request_cancelled'):controller.signal.reason;
      if(!(error instanceof AppError))error=new AppError(502,'上游连接中断','upstream_connection');
      throw Object.assign(error,knownUsage||{},{usageKnown:!!knownUsage||error.usageKnown===true||!dispatched});
    }finally{signal?.removeEventListener('abort',abort);}
  }
  return {run,runJson,models};
}
