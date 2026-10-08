import {spawn} from 'node:child_process';
import {StringDecoder} from 'node:string_decoder';
import {runnerEnv,codexAuthArgs} from './codex.mjs';

// Only the challenge reaches the authenticated UI. Raw CLI output is never logged.
export function startCodexLogin(binary,home,{spawnProcess=spawn,startTimeoutMs=20000,timeoutMs=600000}={}) {
  let resolveChallenge,rejectChallenge,resolveCompleted;
  const challenge=new Promise((resolve,reject)=>{resolveChallenge=resolve;rejectChallenge=reject;});
  const completed=new Promise(resolve=>{resolveCompleted=resolve;});
  let child,ended=false,started=false,loginId='',buffer='',size=0;
  const decoder=new StringDecoder('utf8');
  const finish=(success,message)=>{
    if(ended)return;ended=true;clearTimeout(startTimer);clearTimeout(timer);
    if(!started)rejectChallenge(Error(message||'官方登录未能启动'));
    resolveCompleted({success,message});child?.kill();
  };
  const startTimer=setTimeout(()=>finish(false,'获取官方授权链接超时，请检查服务器网络后重试'),startTimeoutMs);
  const timer=setTimeout(()=>finish(false,'官方登录等待已超过 10 分钟，请重新登录'),timeoutMs);
  const send=value=>child.stdin.write(JSON.stringify(value)+'\n');
  try {
    child=spawnProcess(binary,['app-server','--listen','stdio://','--disable','plugins','--disable','apps',...codexAuthArgs()],{env:runnerEnv(home),windowsHide:true,stdio:['pipe','pipe','pipe']});
    child.on('error',()=>finish(false,'无法启动官方登录，请检查 Codex CLI 配置'));
    child.on('close',()=>finish(false,'官方登录进程已退出，请重试'));
    child.stdin.on('error',()=>finish(false,'官方登录连接已关闭，请重试'));
    child.stderr.on('data',()=>{});
    child.stdout.on('data',bytes=>{
      if(ended)return;size+=bytes.length;if(size>1024*1024)return finish(false,'官方登录响应超过限制');
      buffer+=decoder.write(bytes);let at;
      while(!ended&&(at=buffer.indexOf('\n'))!==-1){
        const line=buffer.slice(0,at);buffer=buffer.slice(at+1);let event;try{event=JSON.parse(line);}catch{continue;}
        if(event.id===1){
          if(event.error)return finish(false,'官方登录初始化失败，请检查 Codex CLI 版本');
          send({method:'initialized',params:{}});send({method:'account/login/start',id:2,params:{type:'chatgptDeviceCode'}});
        }
        if(event.id===2){
          if(event.error){
            // Preserve only the HTTP status; upstream diagnostics may contain secrets.
            const status=String(event.error.message||'').match(/\bstatus(?: code)?\s*[:=]?\s*([45]\d{2})\b/i)?.[1];
            return finish(false,'无法获取设备授权码'+(status?'（官方服务返回 HTTP '+status+'）':'')+'，请检查服务器网络，并确认 ChatGPT 安全设置或工作区权限已启用设备码登录');
          }
          const result=event.result;let url;try{url=new URL(result?.verificationUrl);}catch{}
          if(result?.type!=='chatgptDeviceCode'||!url||url.origin!=='https://auth.openai.com'||url.pathname!=='/codex/device'||url.search||url.hash||url.username||url.password||typeof result.loginId!=='string'||!result.loginId||typeof result.userCode!=='string'||! /^[A-Z0-9-]{4,32}$/.test(result.userCode))return finish(false,'官方授权响应无效，请更新 Codex CLI 后重试');
          loginId=result.loginId;started=true;clearTimeout(startTimer);
          resolveChallenge({type:'device',verificationUrl:url.href,userCode:result.userCode});
        }
        if(event.method==='account/login/completed'&&started&&event.params?.loginId===loginId)finish(event.params.success===true,event.params.success===true?'':'官方授权未完成，请检查设备码登录权限后重试');
      }
    });
    send({method:'initialize',id:1,params:{clientInfo:{name:'harbor_local_gateway',title:'Harbor Local Gateway',version:'1.0.0'}}});
  }catch{finish(false,'无法启动官方登录，请检查 Codex CLI 配置');}
  return {challenge,completed,cancel:()=>finish(false,'登录已取消')};
}

export function startLocalLogin(binary,args,env) {
  let child,resolveCompleted;
  const completed=new Promise(resolve=>{resolveCompleted=resolve;});
  const challenge=new Promise((resolve,reject)=>{
    child=spawn(binary,args,{env,windowsHide:true,stdio:'ignore'});
    child.once('spawn',()=>resolve({type:'browser'}));
    child.once('error',()=>{reject(Error('无法启动官方登录，请检查 CLI 配置'));resolveCompleted({success:false,message:'无法启动官方登录'});});
    child.once('close',code=>resolveCompleted({success:code===0,message:code===0?'':'官方登录未完成，请重试'}));
  });
  const timer=setTimeout(()=>child.kill(),600000);completed.then(()=>clearTimeout(timer));
  return {challenge,completed,cancel:()=>child.kill()};
}
