import {AppError} from './store.mjs';
import {spawn} from 'node:child_process';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';

// Keep platform endpoints server-owned: an employee cannot redirect credentials.
export const platformProviders={
  workbuddy:{label:'WorkBuddy',authentication:'browser',origin:'https://copilot.tencent.com'},
  qwen:{label:'Qwen',authentication:'key',regions:{china:'https://maas.qianwenaiapi.com/compatible-mode/v1',international:'https://maas.qwencloudapi.com/compatible-mode/v1',china_plan:'https://token-plan.maas.qianwenaiapi.com/compatible-mode/v1',international_plan:'https://token-plan.maas.qwencloudapi.com/compatible-mode/v1',dashscope:'https://dashscope.aliyuncs.com/compatible-mode/v1'}},
  deepseek:{label:'DeepSeek',authentication:'key',origin:'https://api.deepseek.com/v1'},
  qoder:{label:'Qoder',authentication:'pat'}
};
const fail=(status,message,code='platform_upstream_error')=>new AppError(status,message,code);
const modelId=value=>typeof value==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,199}$/.test(value);
export function platformConfiguration(provider,region='china'){
  if(!Object.hasOwn(platformProviders,provider))throw fail(400,'不支持的平台','invalid_platform');
  if(provider==='qwen'&&!Object.hasOwn(platformProviders.qwen.regions,region))throw fail(400,'请选择有效的Qwen区域与套餐入口','invalid_platform');
  return {region:provider==='qwen'?region:null};
}
export function platformModel(value){
  if(typeof value!=='string')throw fail(400,'请指定平台/实际模型ID');
  const at=value.indexOf('/'),provider=value.slice(0,at),model=value.slice(at+1);
  if(at<1||!Object.hasOwn(platformProviders,provider)||!modelId(model))throw fail(400,'模型格式为平台/实际模型ID','invalid_platform_model');
  return {provider,model};
}
export function qoderCatalog(raw){
  const lines=raw.trim().split(/\r?\n/);
  if(lines.shift()!=='MODEL'||!lines.length||lines.length>1000)throw fail(502,'Qoder CLI模型目录格式无效，请核对CLI版本');
  return [...new Set(lines.map(line=>line.replace(/ \([^()]+\)$/,'').trim()).filter(Boolean).map(name=>'name-'+Buffer.from(name).toString('base64url')))];
}
function platformParameters(provider,body){
  if(provider==='qoder'&&(body.temperature!==undefined||body.max_tokens!==undefined))throw fail(400,'Qoder CLI不支持本入口的temperature或max_tokens参数');
}
export function validatePlatformChat(body){
  if(!body||typeof body!=='object'||Array.isArray(body)||Object.keys(body).some(k=>!['model','messages','stream','session_id','max_tokens','temperature'].includes(k)))throw fail(400,'平台通道仅支持纯文本消息、模型、会话、stream、max_tokens和temperature');
  platformParameters(platformModel(body.model).provider,body);
  if(!Array.isArray(body.messages)||!body.messages.length||body.messages.length>100||body.messages.some(m=>!m||!['system','user','assistant','developer'].includes(m.role)||typeof m.content!=='string'||Object.keys(m).some(k=>!['role','content'].includes(k))))throw fail(400,'仅支持1至100条纯文本消息，不支持附件或工具');
  if(body.stream!==undefined&&typeof body.stream!=='boolean')throw fail(400,'stream必须为布尔值');
  if(body.session_id!==undefined&&(typeof body.session_id!=='string'||!/^[a-zA-Z0-9-]{1,80}$/.test(body.session_id)))throw fail(400,'会话标识无效');
  if(body.max_tokens!==undefined&&(!Number.isSafeInteger(body.max_tokens)||body.max_tokens<1||body.max_tokens>1000000))throw fail(400,'max_tokens无效');
  if(body.temperature!==undefined&&(!Number.isFinite(body.temperature)||body.temperature<0||body.temperature>2))throw fail(400,'temperature无效');
  return body;
}
function counts(usage){
  const input=usage?.prompt_tokens??usage?.input_tokens,output=usage?.completion_tokens??usage?.output_tokens,cached=usage?.prompt_cache_hit_tokens??usage?.prompt_tokens_details?.cached_tokens??usage?.cache_read_input_tokens??0;
  if([input,output,cached].some(v=>!Number.isSafeInteger(v)||v<0)||cached>input)return {input:0,output:0,cached:0,usageKnown:false};
  return {input,output,cached,usageKnown:true};
}
export function qoderCounts(usage){
  const values=['input_tokens','output_tokens','cache_read_input_tokens','cache_creation_input_tokens'].map((key,index)=>usage?.[key]??(index>1?0:undefined));
  if(values.some(v=>!Number.isSafeInteger(v)||v<0))return {input:0,output:0,cached:0,usageKnown:false};
  const [plain,output,cached,created]=values,input=plain+cached+created;
  // Qoder's default serializer masks counters as zero. Never turn those into free requests.
  if(!Number.isSafeInteger(input)||input+output===0)return {input:0,output:0,cached:0,usageKnown:false};
  return {input,output,cached,usageKnown:true};
}
async function boundedText(response,max=4*1024*1024){
  let size=0;const chunks=[];
  if(!response.body)return '';
  for await(const chunk of response.body){size+=chunk.length;if(size>max)throw fail(502,'平台响应过大；用量需核对');chunks.push(chunk);}
  return Buffer.concat(chunks).toString('utf8');
}
function catalog(values){
  if(!Array.isArray(values))throw fail(502,'平台没有返回有效模型目录');
  const models=[...new Set(values.map(m=>typeof m==='string'?m:m?.id??m?.modelId??m?.model).filter(modelId))];
  if(!models.length||models.length>1000)throw fail(502,'平台模型目录为空或过大');return models;
}
export function createPlatformTransport({fetchImpl=fetch,qoderBinary='qodercli',spawnProcess=spawn,clock=Date.now,pollMs=3000,timeoutMs=120000,killGraceMs=2000}={}){
  async function request(url,{signal,...options}={}){
    try{
      const response=await fetchImpl(url,{...options,redirect:'error',signal:AbortSignal.any([AbortSignal.timeout(120000),...(signal?[signal]:[])])});
      if(!response.ok){await response.body?.cancel();throw fail(response.status===401||response.status===403?401:response.status===429?429:502,'平台请求未通过（HTTP '+response.status+'），请检查认证、额度或稍后重试');}
      return response;
    }catch(error){if(error instanceof AppError)throw error;throw fail(signal?.aborted?499:502,signal?.aborted?'平台请求已取消；用量需核对':'平台连接失败；请检查网络，用量需核对');}
  }
  async function wb(path,{credential,signal,method='GET',body,refresh}={}){
    const headers={Accept:'application/json','User-Agent':'Harbor/1',...(body?{'Content-Type':'application/json'}:{}),...(credential?workbuddyHeaders(credential):{}),...(refresh?{'X-Refresh-Token':refresh,'X-Auth-Refresh-Source':'plugin'}:{})};
    const response=await request(platformProviders.workbuddy.origin+path,{method,headers,signal,...(body?{body:JSON.stringify(body)}:{})});
    let value;try{value=JSON.parse(await boundedText(response,1024*1024));}catch{throw fail(502,'WorkBuddy返回无效数据');}
    return value;
  }
  const data=value=>{if(value.code!==0&&value.code!==undefined)throw fail(401,'WorkBuddy授权未通过，请重新登录');if(!value.data||typeof value.data!=='object')throw fail(502,'WorkBuddy返回无效数据');return value.data;};
  function workbuddyHeaders(credential){
    const headers={Authorization:'Bearer '+credential.accessToken,Accept:'text/event-stream','Content-Type':'application/json'};
    for(const [field,header] of [['uid','X-User-Id'],['enterpriseId','X-Enterprise-Id'],['enterpriseId','X-Tenant-Id'],['departmentFullName','X-Department-Info']])if(typeof credential.account?.[field]==='string'&&credential.account[field])headers[header]=credential.account[field];
    if(typeof credential.domain==='string'&&credential.domain)headers['X-Domain']=credential.domain;
    return headers;
  }
  function qoder(credential,args,prompt,signal){
    if(signal?.aborted)return Promise.reject(fail(499,'请求已取消'));
    return new Promise((resolve,reject)=>{
      const home=mkdtempSync(join(tmpdir(),'harbor-qoder-')),env={};
      for(const key of ['PATH','Path','SystemRoot','WINDIR','COMSPEC','PATHEXT','HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','NO_PROXY'])if(process.env[key])env[key]=process.env[key];
      Object.assign(env,{HOME:home,USERPROFILE:home,QODER_CONFIG_DIR:home,QODER_PERSONAL_ACCESS_TOKEN:credential.token,QODER_EXPOSE_TOKEN_USAGE:'1'});
      let child;try{child=spawnProcess(qoderBinary,[...args,'--config-dir',home,'--setting-sources','','--settings','{"hooks":{},"general":{"maxAttempts":1}}','--strict-mcp-config'],{cwd:home,env,detached:process.platform!=='win32',stdio:['pipe','pipe','pipe'],windowsHide:true});}catch{rmSync(home,{recursive:true,force:true});reject(fail(503,'Qoder CLI未安装或无法启动'));return;}
      let bytes=0,output=[],failure=null,killTimer;
      const terminateTree=signal=>{
        if(!child.pid){child.kill(signal);return;}
        if(process.platform==='win32'){const killer=spawn('taskkill',['/PID',String(child.pid),'/T','/F'],{stdio:'ignore',windowsHide:true});killer.on('error',()=>child.kill(signal));return;}
        try{process.kill(-child.pid,signal);}catch(error){if(error.code!=='ESRCH')child.kill(signal);}
      };
      const stop=message=>{if(failure)return;failure=message;terminateTree('SIGTERM');killTimer=setTimeout(()=>terminateTree('SIGKILL'),killGraceMs);killTimer.unref();};
      const timer=setTimeout(()=>stop('Qoder请求超时；用量需核对'),timeoutMs),abort=()=>stop('Qoder请求已取消；用量需核对');
      signal?.addEventListener('abort',abort,{once:true});
      const cleanup=()=>{clearTimeout(timer);clearTimeout(killTimer);signal?.removeEventListener('abort',abort);rmSync(home,{recursive:true,force:true});};
      // A dispatcher can exit before its descendants close inherited pipes.
      child.on('exit',()=>terminateTree('SIGKILL'));
      child.stdout.on('data',chunk=>{bytes+=chunk.length;if(bytes>4*1024*1024)stop('Qoder响应过大');else output.push(chunk);});child.stderr.on('data',()=>{});child.stdin.on('error',()=>{});
      child.on('error',()=>{cleanup();reject(fail(503,'Qoder CLI未安装或无法启动'));});
      child.on('close',code=>{cleanup();if(failure||code!==0){reject(fail(signal?.aborted?499:502,failure||'Qoder请求未通过，请检查访问令牌与CLI版本'));return;}resolve(Buffer.concat(output).toString('utf8'));});
      child.stdin.end(prompt||'');if(signal?.aborted)abort();
    });
  }
  function origin(account){return account.kind==='qwen'?platformProviders.qwen.regions[JSON.parse(account.platform_config).region]:platformProviders[account.kind].origin;}
  async function models(account,credential,signal){
    if(account.kind==='qoder'){
      const raw=await qoder(credential,['--list-models'],null,signal);
      // Qoder 1.1.81 prints a MODEL table even with JSON output selected.
      return qoderCatalog(raw);
    }
    if(account.kind==='workbuddy')return catalog(data(await wb('/v3/config',{credential,signal})).models);
    const response=await request(origin(account)+'/models',{headers:{Authorization:'Bearer '+credential.token},signal});
    let value;try{value=JSON.parse(await boundedText(response,1024*1024));}catch{throw fail(502,'平台模型目录格式无效');}return catalog(value.data);
  }
  async function run(account,credential,payload,signal){
    const {model}=platformModel(payload.model);
    platformParameters(account.kind,payload);
    if(account.kind==='qoder'){
      if(!model.startsWith('name-'))throw fail(400,'请使用Qoder目录返回的模型ID');
      const selected=Buffer.from(model.slice(5),'base64url').toString('utf8');
      let value;try{value=JSON.parse(await qoder(credential,['--print','--output-format','json','--model',selected,'--tools','','--no-session-persistence','--max-model-request-retries','0'],JSON.stringify(payload.messages),signal));}catch(error){if(error instanceof AppError)throw error;throw fail(502,'Qoder未返回有效结果；用量需核对');}
      if(value.is_error||typeof value.result!=='string')throw fail(502,'Qoder未返回成功的文本结果；用量需核对');
      return {text:value.result,...qoderCounts(value.usage),actualModel:typeof value.model==='string'?'qoder/'+value.model:payload.model};
    }
    const isWB=account.kind==='workbuddy',messages=payload.messages.map(m=>({role:m.role==='developer'?'system':m.role,content:m.content}));
    const body={model,messages,stream:isWB,...(isWB?{stream_options:{include_usage:true}}:{}),...(payload.max_tokens===undefined?{}:{max_tokens:payload.max_tokens}),...(payload.temperature===undefined?{}:{temperature:payload.temperature})};
    const response=await request(isWB?platformProviders.workbuddy.origin+'/v2/chat/completions':origin(account)+'/chat/completions',{method:'POST',headers:isWB?workbuddyHeaders(credential):{Authorization:'Bearer '+credential.token,'Content-Type':'application/json'},body:JSON.stringify(body),signal});
    const raw=await boundedText(response);let text='',usage,actualModel,finish;
    if(isWB){
      let done=false;
      for(const line of raw.split(/\r?\n/)){
        if(!line.startsWith('data:'))continue;const rawData=line.slice(5).trim();if(rawData==='[DONE]'){done=true;continue;}
        let event;try{event=JSON.parse(rawData);}catch{throw fail(502,'WorkBuddy流格式无效；用量需核对');}
        if(event.error)throw fail(502,'WorkBuddy返回错误；用量需核对');
        if(event.usage)usage=event.usage;if(event.model)actualModel=event.model;
        for(const choice of event.choices||[]){if(choice.index!==0)continue;if(typeof choice.delta?.content==='string')text+=choice.delta.content;if(choice.finish_reason)finish=choice.finish_reason;}
      }
      if(!done||!finish)throw fail(502,'WorkBuddy响应中断；用量需核对');
    }else{
      let value;try{value=JSON.parse(raw);}catch{throw fail(502,'平台响应无效；用量需核对');}
      const choice=value.choices?.[0];if(typeof choice?.message?.content!=='string'||!choice.finish_reason||choice.message.tool_calls?.length)throw fail(502,'平台未返回纯文本完成结果；用量需核对');
      text=choice.message.content;usage=value.usage;actualModel=value.model;finish=choice.finish_reason;
    }
    return {text,...counts(usage),actualModel:modelId(actualModel)?account.kind+'/'+actualModel:payload.model,finishReason:finish};
  }
  async function login({signal,onChallenge}){
    const start=data(await wb('/v2/plugin/auth/state?platform=workbuddy',{method:'POST',body:{},signal}));
    let url;try{url=new URL(start.authUrl);}catch{throw fail(502,'WorkBuddy授权地址无效');}
    if(url.protocol!=='https:'||url.hostname!=='copilot.tencent.com'||typeof start.state!=='string'||!start.state)throw fail(502,'WorkBuddy授权响应无效');
    onChallenge({type:'browser',verificationUrl:url.href});
    while(!signal.aborted){
      const result=await wb('/v2/plugin/auth/token?state='+encodeURIComponent(start.state),{signal});
      if(result.code===11217){await new Promise((resolve,reject)=>{const abort=()=>{clearTimeout(timer);reject(fail(499,'登录已取消'));};const timer=setTimeout(()=>{signal.removeEventListener('abort',abort);resolve();},pollMs);signal.addEventListener('abort',abort,{once:true});if(signal.aborted)abort();});continue;}
      const token=data(result);
      if(typeof token.accessToken!=='string'||!token.accessToken||typeof token.refreshToken!=='string'||!Number.isFinite(token.expiresIn)||token.expiresIn<=0)throw fail(502,'WorkBuddy没有返回完整授权');
      const credential={accessToken:token.accessToken,refreshToken:token.refreshToken,expiresAt:clock()+token.expiresIn*1000,...(typeof token.domain==='string'?{domain:token.domain}:{})};
      credential.account=data(await wb('/v2/plugin/login/account?state='+encodeURIComponent(start.state),{credential,signal}));
      return credential;
    }
    throw fail(499,'登录已取消');
  }
  async function refresh(credential,signal){
    const token=data(await wb('/v2/plugin/auth/token/refresh',{method:'POST',body:{},credential,refresh:credential.refreshToken,signal}));
    if(typeof token.accessToken!=='string'||!token.accessToken||!Number.isFinite(token.expiresIn)||token.expiresIn<=0)throw fail(401,'WorkBuddy授权刷新失败，请重新登录');
    return {...credential,accessToken:token.accessToken,refreshToken:token.refreshToken||credential.refreshToken,expiresAt:clock()+token.expiresIn*1000};
  }
  return {models,run,login,refresh};
}
