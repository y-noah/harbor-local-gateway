import { spawn } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { StringDecoder } from 'node:string_decoder';

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
export function checkLogin(binary,home) {
  return new Promise(resolve=>{
    const child=spawn(binary,['login','status',...codexAuthArgs()],{env:runnerEnv(home),windowsHide:true});
    let out='';const timer=setTimeout(()=>child.kill(),10000);
    child.stdout.on('data',b=>out+=b);child.stderr.on('data',b=>out+=b);
    child.on('error',()=>{clearTimeout(timer);resolve(false);});
    child.on('close',code=>{clearTimeout(timer);resolve(code===0 && /ChatGPT/i.test(out));});
  });
}
export function usageDelta(usage,previous={input:0,output:0,cached:0}){
  const current={input:usage.input_tokens,output:usage.output_tokens,cached:usage.cached_input_tokens||0};
  const counts={input:current.input-(previous.input||0),output:current.output-(previous.output||0),cached:current.cached-(previous.cached||0)};
  if(Object.values(counts).some(v=>!Number.isSafeInteger(v)||v<0)||current.cached>current.input||counts.cached>counts.input)throw Error('上游累计用量发生变化，需要核对');
  return {...counts,cumulativeUsage:current};
}
export function runCodex({binary,home,workspace,messages,model,sessionId,previousUsage,timeoutMs=120000,signal,spawnProcess=spawn}) {
  mkdirSync(workspace,{recursive:true});
  return new Promise((resolve,reject)=>{
    const child=spawnProcess(binary,argsFor(workspace,model,sessionId),{env:runnerEnv(home),windowsHide:true,stdio:['pipe','pipe','pipe']});
    let buffer='',text='',usage=null,reason='',size=0,done=false,forced=false,threadId=sessionId||null;
    const decoder=new StringDecoder('utf8');
    const fail=message=>{reason=message;forced=true;child.kill();};
    const timer=setTimeout(()=>fail('请求超过 120 秒；上游用量可能已产生'),timeoutMs);
    const abort=()=>fail('客户端断开或服务停止；上游用量可能已产生');
    signal?.addEventListener('abort',abort,{once:true});
    const cleanup=()=>{clearTimeout(timer);signal?.removeEventListener('abort',abort);};
    function event(line){
      let e;try{e=JSON.parse(line);}catch{return;}
      if(e.type==='thread.started'&&typeof e.thread_id==='string')threadId=e.thread_id;
      if(e.type==='item.completed' && e.item?.type==='agent_message')text+=e.item.text||'';
      if(e.type==='turn.completed') { usage=e.usage; done=true; }
      if(e.type==='turn.failed'||e.type==='error')reason='Codex 请求失败，请检查账号登录状态或订阅额度';
      if(e.item && ['command_execution','file_change','mcp_tool_call','web_search'].includes(e.item.type))fail('已阻止非文本工具调用');
    }
    child.stdout.on('data',b=>{size+=b.length;if(size>2*1024*1024)return fail('上游输出超过限制');buffer+=decoder.write(b);let p;while((p=buffer.indexOf('\n'))!==-1){event(buffer.slice(0,p));buffer=buffer.slice(p+1);}});
    child.stderr.on('data',()=>{}); // Never expose credential-bearing diagnostics to clients.
    child.stdin.on('error',()=>{});
    child.on('error',()=>{cleanup();reject(Object.assign(new Error('无法启动 Codex CLI'),{usageKnown:true}));});
    child.on('close',code=>{
      cleanup();buffer+=decoder.end();if(buffer)event(buffer);
      let valid=usage && ['input_tokens','output_tokens'].every(k=>Number.isSafeInteger(usage[k])&&usage[k]>=0),counts={};
      if(valid){try{counts=usageDelta(usage,previousUsage);}catch(e){valid=false;reason=e.message;}}
      if(sessionId&&threadId!==sessionId){valid=false;counts={};reason='上游返回了不同的会话，已停止续聊并保留用量待核对';}
      if(code===0&&done&&valid&&!forced&&!reason&&threadId)resolve({text,...counts,usageKnown:true,threadId});
      else reject(Object.assign(new Error(reason||'Codex 未返回完整结果；请检查账号连接'),{...counts,usageKnown:!!valid,threadId}));
    });
    child.stdin.end((sessionId?'Continue the existing conversation with these NEW messages only.':'Respond to this conversation.')+' Return only the assistant response.\n'+JSON.stringify(messages));
    if(signal?.aborted)abort();
  });
}
