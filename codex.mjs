import { spawn } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { StringDecoder } from 'node:string_decoder';
import {AppError} from './store.mjs';

export function runnerEnv(home) {
  const env={};
  for(const name of ['PATH','Path','SystemRoot','SYSTEMROOT','WINDIR','COMSPEC','PATHEXT','TEMP','TMP','USERPROFILE','APPDATA','LOCALAPPDATA','HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','NO_PROXY']) if(process.env[name]) env[name]=process.env[name];
  env.CODEX_HOME=home;
  return env;
}
const disabled=['shell_tool','unified_exec','apps','plugins','hooks','multi_agent','multi_agent_v2','browser_use','browser_use_external','computer_use','image_generation','view_image','sleep_tool','workspace_dependencies','code_mode_host','memories'];
export const codexAuthArgs=()=>['-c','cli_auth_credentials_store="file"'];
export function argsFor(workspace,model,sessionId) {
  const args=['exec','--ignore-user-config','--ignore-rules','--skip-git-repo-check','--sandbox','read-only','--json','--color','never','-C',workspace];
  args.push(...codexAuthArgs());
  for(const feature of disabled)args.push('--disable',feature);
  args.push('--enable','skip_host_skill_discovery','-c','web_search="disabled"','-c','project_doc_max_bytes=0','-c','model_reasoning_effort="low"','-c','developer_instructions="You are a text-only assistant. Answer the supplied conversation. Do not access files, execute commands, call tools or delegate tasks."');
  if(model && model!=='codex')args.push('-m',model);
  if(sessionId)args.push('resume',sessionId);
  args.push('-'); return args;
}
export function checkLogin(binary,home,{spawnProcess=spawn,maxOutputBytes=2*1024*1024}={}) {
  if(!Number.isSafeInteger(maxOutputBytes)||maxOutputBytes<256||maxOutputBytes>128*1024*1024)throw new AppError(503,'订阅状态响应上限无效','upstream_configuration');
  return new Promise((resolve,reject)=>{
    const child=spawnProcess(binary,['login','status',...codexAuthArgs()],{env:runnerEnv(home),windowsHide:true});
    let out='',bytes=0,overflow,ended=false;const timer=setTimeout(()=>child.kill(),10000);
    const read=chunk=>{if(ended||overflow)return;bytes+=chunk.length;if(bytes>maxOutputBytes){out='';overflow=new AppError(502,'订阅状态响应超过读取上限','upstream_response_too_large');child.kill();return;}out+=chunk;};
    child.stdout.on('data',read);child.stderr.on('data',read);
    child.on('error',()=>{ended=true;clearTimeout(timer);resolve(false);});
    child.on('close',code=>{if(ended)return;ended=true;clearTimeout(timer);if(overflow)reject(overflow);else resolve(code===0 && /ChatGPT/i.test(out));});
  });
}
export function usageDelta(usage,previous={input:0,output:0,cached:0}){
  const current={input:usage.input_tokens,output:usage.output_tokens,cached:usage.cached_input_tokens||0};
  const counts={input:current.input-(previous.input||0),output:current.output-(previous.output||0),cached:current.cached-(previous.cached||0)};
  if(Object.values(counts).some(v=>!Number.isSafeInteger(v)||v<0)||current.cached>current.input||counts.cached>counts.input)throw Error('上游累计用量发生变化，需要核对');
  return {...counts,cumulativeUsage:current};
}
export function runCodex({binary,home,workspace,messages,model,sessionId,previousUsage,timeoutMs=120000,signal,onText=()=>{},output,spawnProcess=spawn}) {
  mkdirSync(workspace,{recursive:true});
  return new Promise((resolve,reject)=>{
    const child=spawnProcess(binary,argsFor(workspace,model,sessionId),{env:runnerEnv(home),windowsHide:true,stdio:['pipe','pipe','pipe']});
    let buffer='',text='',usage=null,reason='',size=0,done=false,forced=false,threadId=sessionId||null,actualModel,bufferFailure,spawnFailed=false;
    const decoder=new StringDecoder('utf8');
    const fail=message=>{reason=message;forced=true;child.kill();};
    const timer=setTimeout(()=>fail('请求执行超时；上游用量可能已产生'),timeoutMs);
    const abort=()=>fail('客户端断开或服务停止；上游用量可能已产生');
    signal?.addEventListener('abort',abort,{once:true});
    const cleanup=()=>{clearTimeout(timer);signal?.removeEventListener('abort',abort);};
    function event(line){
      output?.resize('codex:event',Buffer.byteLength(line));let e;try{e=JSON.parse(line);}catch{return;}
      if(['thread.started','turn.started','turn.completed'].includes(e.type)&&typeof e.model==='string')actualModel=e.model;
      if(e.type==='thread.started'&&typeof e.thread_id==='string')threadId=e.thread_id;
      if(e.type==='item.completed' && e.item?.type==='agent_message'){const part=e.item.text||'';output?.resize('codex:text-copy',(text.length+part.length)*2);text+=part;output?.resize('codex:text',text.length*2);output?.resize('codex:text-copy',0);try{onText(part);}catch{}}
      if(e.type==='turn.completed') { usage=e.usage; done=true; }
      if(e.type==='turn.failed'||e.type==='error')reason='Codex 请求失败，请检查账号登录状态或订阅额度';
      if(e.item && ['command_execution','file_change','mcp_tool_call','web_search'].includes(e.item.type))fail('已阻止非文本工具调用');
    }
    child.stdout.on('data',b=>{if(bufferFailure||spawnFailed)return;size+=b.length;if(size>2*1024*1024)return fail('上游输出超过限制');try{output?.resize('codex:scratch',b.length*3+(buffer.length+b.length)*2);buffer+=decoder.write(b);output?.resize('codex:buffer',buffer.length*2);output?.resize('codex:scratch',0);let p;while((p=buffer.indexOf('\n'))!==-1){event(buffer.slice(0,p));output?.resize('codex:event',0);buffer=buffer.slice(p+1);output?.resize('codex:buffer',buffer.length*2);}}catch(error){bufferFailure=error;fail('上游输出缓冲繁忙；用量需核对');}});
    child.stderr.on('data',()=>{}); // Never expose credential-bearing diagnostics to clients.
    child.stdin.on('error',()=>{});
    child.on('error',()=>{spawnFailed=true;cleanup();reject(Object.assign(new Error('无法启动 Codex CLI'),{usageKnown:true}));});
    child.on('close',code=>{
      cleanup();if(spawnFailed)return;if(!bufferFailure)try{output?.resize('codex:scratch',buffer.length*2+8);buffer+=decoder.end();output?.resize('codex:buffer',buffer.length*2);output?.resize('codex:scratch',0);if(buffer)event(buffer);buffer='';output?.resize('codex:event',0);output?.resize('codex:buffer',0);}catch(error){bufferFailure=error;forced=true;}
      let valid=usage && ['input_tokens','output_tokens'].every(k=>Number.isSafeInteger(usage[k])&&usage[k]>=0),counts={};
      if(valid){try{counts=usageDelta(usage,previousUsage);}catch(e){valid=false;reason=e.message;}}
      if(sessionId&&threadId!==sessionId){valid=false;counts={};reason='上游返回了不同的会话，已停止续聊并保留用量待核对';}
      if(code===0&&done&&valid&&!forced&&!reason&&threadId)resolve({text,...counts,usageKnown:true,threadId,...(actualModel?{actualModel}:{})});
      else reject(Object.assign(bufferFailure||new Error(reason||'Codex 未返回完整结果；请检查账号连接'),{...counts,usageKnown:!!valid,threadId,...(actualModel?{actualModel}:{})}));
    });
    child.stdin.end((sessionId?'Continue the existing conversation with these NEW messages only.':'Respond to this conversation.')+' Return only the assistant response.\n'+JSON.stringify(messages));
    if(signal?.aborted)abort();
  });
}
