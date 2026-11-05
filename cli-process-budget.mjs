import {spawn} from 'node:child_process';
import {AppError} from './store.mjs';

const rejected=(message,code)=>Object.assign(new AppError(503,message,code),{usageKnown:true,notDispatched:true});
// Reservations cover the entire child lifetime, including asynchronous shutdown
// after an RPC has already returned. No stdout, command, owner or env is exposed.
export function createCliProcessBudget({maxConcurrent=8,maxPerAccount=Math.min(2,maxConcurrent),maxMaintenance=Math.min(4,maxConcurrent),killGraceMs=2000}={}, {spawnProcess=spawn}={}){
  if(![maxConcurrent,maxPerAccount,maxMaintenance].every(n=>Number.isInteger(n)&&n>=1&&n<=256)||maxPerAccount>maxConcurrent||maxMaintenance>maxConcurrent||!Number.isInteger(killGraceMs)||killGraceMs<10||killGraceMs>30000||typeof spawnProcess!=='function')throw rejected('CLI进程资源配置无效','cli_configuration');
  const leases=new Set(),accounts=new Map(),listeners=new Set();let maintenance=0,running=0,peak=0,rejections=0,stopping=false;
  function reserve(accountId,{kind='maintenance',signal}={}){
    if(typeof accountId!=='string'||!accountId||accountId.length>4096||!['maintenance','model'].includes(kind))throw rejected('CLI进程归属无效','cli_configuration');
    if(stopping)throw rejected('CLI进程正在关闭','service_stopping');
    if(signal?.aborted)throw rejected('CLI操作已取消','request_cancelled');
    if(leases.size>=maxConcurrent||(accounts.get(accountId)||0)>=maxPerAccount||kind==='maintenance'&&maintenance>=maxMaintenance){rejections++;throw rejected('CLI进程容量已满，请稍后重试','cli_capacity_busy');}
    let child,closed=false,started=false,killTimer,resolveDone;const done=new Promise(resolve=>{resolveDone=resolve;});
    const release=()=>{if(closed)return;closed=true;clearTimeout(killTimer);signal?.removeEventListener('abort',abort);leases.delete(lease);const count=accounts.get(accountId)-1;if(count)accounts.set(accountId,count);else accounts.delete(accountId);if(kind==='maintenance')maintenance--;if(started)running--;resolveDone();for(const listener of [...listeners])try{listener();}catch{}};
    const abort=()=>{if(child)child.kill();else release();};
    const lease={done,release:()=>{if(!started)release();},stop:abort,spawn(binary,args,options){
      if(started||closed||stopping)throw rejected('CLI进程预约已结束','service_stopping');
      if(signal?.aborted){release();throw rejected('CLI操作已取消','request_cancelled');}
      try{child=spawnProcess(binary,args,{...options,detached:process.platform!=='win32'});}catch(error){release();throw error;}
      started=true;running++;const nativeKill=child.kill.bind(child);
      const killTree=signum=>{
        if(process.platform==='win32'){
          if(child.pid){const killer=spawn('taskkill',['/PID',String(child.pid),'/T','/F'],{windowsHide:true,stdio:'ignore'});const fallback=()=>{try{nativeKill('SIGKILL');}catch{}};killer.on('error',fallback);killer.on('close',code=>{if(code!==0)fallback();});return true;}
          try{return nativeKill(signum);}catch{return false;}
        }
        if(child.pid)try{process.kill(-child.pid,signum);}catch(error){if(error.code!=='ESRCH')try{nativeKill(signum);}catch{}}
        try{return nativeKill(signum);}catch{return false;}
      };
      child.kill=(signum='SIGTERM')=>{if(closed)return false;if(!['SIGTERM','SIGINT','SIGKILL'].includes(signum))return nativeKill(signum);const sent=killTree(signum);if(signum!=='SIGKILL'&&!killTimer){killTimer=setTimeout(()=>killTree('SIGKILL'),killGraceMs);killTimer.unref();}return sent;};
      child.once('exit',()=>killTree('SIGKILL'));
      child.once('close',release);
      // An unspawnable executable still emits close after error. Keep capacity
      // until that event; the caller owns its own sanitized error response.
      child.on('error',()=>{});
      if(signal?.aborted)abort();return child;
    }};
    leases.add(lease);accounts.set(accountId,(accounts.get(accountId)||0)+1);if(kind==='maintenance')maintenance++;peak=Math.max(peak,leases.size);signal?.addEventListener('abort',abort,{once:true});return lease;
  }
  return {reserve,available(accountId,{kind='maintenance'}={}){return !stopping&&leases.size<maxConcurrent&&(accounts.get(accountId)||0)<maxPerAccount&&(kind!=='maintenance'||maintenance<maxMaintenance);},onRelease(listener){listeners.add(listener);return ()=>listeners.delete(listener);},spawn(accountId,binary,args,options,context){const lease=reserve(accountId,context);try{return lease.spawn(binary,args,options);}catch(error){lease.release();throw error;}},status:()=>({inUse:leases.size,running,maintenance,accounts:accounts.size,peak,rejected:rejections,stopping,maxConcurrent,maxPerAccount,maxMaintenance,killGraceMs}),stop(){stopping=true;for(const lease of [...leases])lease.stop();},async close(){this.stop();await Promise.all([...leases].map(lease=>lease.done));}};
}
