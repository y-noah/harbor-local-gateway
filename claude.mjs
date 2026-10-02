import {spawn} from 'node:child_process';
import {mkdirSync} from 'node:fs';
import {StringDecoder} from 'node:string_decoder';
import {runnerEnv,usageDelta} from './codex.mjs';

export function claudeEnv(home){const env=runnerEnv(home);delete env.CODEX_HOME;env.CLAUDE_CONFIG_DIR=home;env.DISABLE_AUTOUPDATER='1';return env;}
export function claudeArgs(sessionId){
  const args=['--print','--output-format','json','--safe-mode','--restricted','--tools','','--disallowedTools','*','--strict-mcp-config','--mcp-config','{"mcpServers":{}}','--settings','{"disableAllHooks":true}','--disable-slash-commands','--no-chrome'];
  if(sessionId)args.push('--resume',sessionId);return args;
}
export function checkClaudeLogin(binary,home){return new Promise(resolve=>{
  const child=spawn(binary,['auth','status'],{env:claudeEnv(home),windowsHide:true});let output='';
  const timer=setTimeout(()=>child.kill(),10000);
  child.stdout.on('data',b=>{if(output.length<32768)output+=b.toString();});child.stderr.on('data',()=>{});
  child.on('error',()=>{clearTimeout(timer);resolve(false);});child.on('close',code=>{clearTimeout(timer);try{const info=JSON.parse(output);resolve(code===0&&info.loggedIn===true&&info.authMethod!=='api_key');}catch{resolve(false);}});
});}
export function parseClaudeResult(value,previousUsage={input:0,output:0,cached:0}){
  if(!value||value.type!=='result')throw Object.assign(Error('Claude 未返回有效结果'),{usageKnown:false});
  // modelUsage represents the conversation-wide per-model cost breakdown. Count
  // cache reads/writes as input, then subtract our persisted cumulative cursor.
  const models=Object.values(value.modelUsage||{});
  if(!models.length)throw Object.assign(Error('Claude 缺少会话用量，需人工核对'),{usageKnown:false,threadId:value.session_id});
  const totals={input_tokens:0,output_tokens:0,cached_input_tokens:0};
  for(const m of models){
    const fields=['inputTokens','outputTokens','cacheReadInputTokens','cacheCreationInputTokens'];
    if(!m||!Number.isSafeInteger(m.inputTokens)||!Number.isSafeInteger(m.outputTokens)||fields.some(k=>!Number.isSafeInteger(m[k]??0)||(m[k]??0)<0))throw Object.assign(Error('Claude 用量格式无效'),{usageKnown:false,threadId:value.session_id});
    totals.input_tokens+=(m.inputTokens||0)+(m.cacheReadInputTokens||0)+(m.cacheCreationInputTokens||0);
    totals.output_tokens+=m.outputTokens||0;totals.cached_input_tokens+=m.cacheReadInputTokens||0;
  }
  let counts;try{counts=usageDelta(totals,previousUsage);}catch{throw Object.assign(Error('Claude 累计用量不一致，需核对'),{usageKnown:false,threadId:value.session_id});}
  if(value.is_error||value.subtype!=='success')throw Object.assign(Error('Claude 请求失败，请检查订阅状态'),{...counts,usageKnown:true,threadId:value.session_id});
  if(typeof value.result!=='string'||typeof value.session_id!=='string')throw Object.assign(Error('Claude 响应缺少文本或 session'),{...counts,usageKnown:true});
  return {text:value.result,threadId:value.session_id,...counts,usageKnown:true};
}
export function runClaude({binary,home,workspace,messages,sessionId,previousUsage,signal,timeoutMs=120000,spawnProcess=spawn}){
  mkdirSync(workspace,{recursive:true});
  return new Promise((resolve,reject)=>{
    const child=spawnProcess(binary,claudeArgs(sessionId),{cwd:workspace,env:claudeEnv(home),windowsHide:true,stdio:['pipe','pipe','pipe']});
    const decoder=new StringDecoder('utf8');let output='',failure='',bytes=0;
    const stop=message=>{failure=message;child.kill();};const timer=setTimeout(()=>stop('Claude 请求超时；用量待核对'),timeoutMs);
    const abort=()=>stop('Claude 请求已取消；用量待核对');signal?.addEventListener('abort',abort,{once:true});
    const cleanup=()=>{clearTimeout(timer);signal?.removeEventListener('abort',abort);};
    child.stdout.on('data',b=>{bytes+=b.length;if(bytes>2*1024*1024)return stop('Claude 响应过大');output+=decoder.write(b);});
    child.stderr.on('data',()=>{});child.stdin.on('error',()=>{});
    child.on('error',()=>{cleanup();reject(Object.assign(Error('Claude CLI 无法启动'),{usageKnown:true}));});
    child.on('close',code=>{cleanup();output+=decoder.end();if(failure)return reject(Object.assign(Error(failure),{usageKnown:false}));let value;try{value=JSON.parse(output);}catch{return reject(Object.assign(Error('Claude 未返回有效 JSON；用量待核对'),{usageKnown:false}));}if(sessionId&&value.session_id!==sessionId)return reject(Object.assign(Error('Claude 返回了不同的会话，已停止续聊并保留用量待核对'),{usageKnown:false}));try{const result=parseClaudeResult(value,previousUsage);if(code!==0)return reject(Object.assign(Error('Claude 进程异常退出；请检查订阅状态'),result));resolve(result);}catch(e){reject(Object.assign(e,{usageKnown:e.usageKnown===true}));}});
    child.stdin.end('Respond to the following '+(sessionId?'NEW messages in the existing conversation':'conversation')+'. Do not use tools.\n'+JSON.stringify(messages));
    if(signal?.aborted)abort();
  });
}
