import {AppError} from './store.mjs';

// Synchronous reservations can be made inside a short admission transaction.
// No database connection or model is held while waiting for another permit.
export function createInferenceBudget({maxConcurrent=120,maxPerMember=Math.min(8,maxConcurrent),maxPerAccount=Math.min(32,maxConcurrent),maxCli=Math.min(4,maxConcurrent),maxCliPerMember=Math.min(2,maxCli,maxPerMember),maxCliPerAccount=Math.min(2,maxCli,maxPerAccount)}={}){
  if(![maxConcurrent,maxPerMember,maxPerAccount,maxCli,maxCliPerMember,maxCliPerAccount].every(value=>Number.isInteger(value)&&value>=1&&value<=4096)||maxPerMember>maxConcurrent||maxPerAccount>maxConcurrent||maxCli>maxConcurrent||maxCliPerMember>Math.min(maxCli,maxPerMember)||maxCliPerAccount>Math.min(maxCli,maxPerAccount))throw new AppError(503,'模型执行资源配置无效','inference_configuration');
  let active=0,cliActive=0,rejected=0,peak=0,cliPeak=0;const members=new Map(),accounts=new Map(),listeners=new Set();
  function acquire({memberId,accountId,cli=false,requireIdle=false}){
    if([memberId,accountId].some(value=>typeof value!=='string'||!value||value.length>256)||typeof cli!=='boolean'||typeof requireIdle!=='boolean')throw new AppError(503,'模型执行资源归属无效','inference_configuration');
    const member=members.get(memberId)||{active:0,cli:0},account=accounts.get(accountId)||{active:0,cli:0};
    if(active>=maxConcurrent||member.active>=maxPerMember||account.active>=maxPerAccount||requireIdle&&account.active||cli&&(cliActive>=maxCli||member.cli>=maxCliPerMember||account.cli>=maxCliPerAccount)){rejected++;return null;}
    members.set(memberId,member);accounts.set(accountId,account);active++;member.active++;account.active++;if(cli){cliActive++;member.cli++;account.cli++;}peak=Math.max(peak,active);cliPeak=Math.max(cliPeak,cliActive);let released=false;
    return ()=>{if(released)return;released=true;active--;member.active--;account.active--;if(cli){cliActive--;member.cli--;account.cli--;}if(!member.active)members.delete(memberId);if(!account.active)accounts.delete(accountId);for(const listener of [...listeners])try{listener();}catch{}};
  }
  return {acquire,onRelease:listener=>{listeners.add(listener);return ()=>listeners.delete(listener);},status:()=>({active,cliActive,peak,cliPeak,rejected,members:members.size,accounts:accounts.size,maxConcurrent,maxPerMember,maxPerAccount,maxCli,maxCliPerMember,maxCliPerAccount})};
}
