import {AppError,hash} from './store.mjs';
import {sanitizeRecord} from './record-redaction.mjs';

const count=value=>Number.isSafeInteger(value)&&value>=0;
const thread=value=>typeof value==='string'&&/^[a-zA-Z0-9-]{1,128}$/.test(value)&&sanitizeRecord(value)===value?value:null;
export const digestChatMessages=(seed,messages)=>messages.reduce((value,message)=>hash(value+'\n'+JSON.stringify({role:message.role,content:message.content})),seed);
export function validateLegacyChat(value){
  if(!value||typeof value!=='object'||Array.isArray(value)||!Array.isArray(value.messages)||!value.messages.length||value.messages.length>100||value.messages.some(m=>!m||!['system','user','assistant','developer'].includes(m.role)||typeof m.content!=='string'))throw new AppError(400,'仅支持 1–100 条纯文本 messages');
  if(value.tools||value.functions)throw new AppError(400,'此版本是纯文本网关，不支持工具调用');
  if(value.model!==undefined&&!['codex','claude'].includes(value.model))throw new AppError(400,'本地模型名为 codex 或 claude');
  if(value.max_tokens!==undefined||value.max_completion_tokens!==undefined)throw new AppError(400,'订阅 CLI 无硬输出 Token 上限，暂不支持 max_tokens 参数');
  if(value.stream!==undefined&&typeof value.stream!=='boolean')throw new AppError(400,'stream 必须为布尔值');
  return {model:value.model||'codex',messages:value.messages.map(({role,content})=>({role,content})),session_id:value.session_id,stream:value.stream===true};
}
// Only this small, typed cursor may accompany critical billing recovery. It
// contains no conversation text, CLI diagnostics, paths or arbitrary objects.
export function canonicalChatFacts(value){
  if(value===undefined)return undefined;
  if(!value||typeof value!=='object'||Array.isArray(value)||!count(value.accountEpoch)||typeof value.dispatched!=='boolean')throw new AppError(503,'文本会话恢复事实无效','chat_recovery_invalid');
  const upstreamId=thread(value.upstreamId),candidate=value.cursor;
  const valid=upstreamId&&candidate&&count(candidate.messageCount)&&typeof candidate.historyDigest==='string'&&/^[a-f0-9]{64}$/.test(candidate.historyDigest)&&['input','output','cached'].every(name=>count(candidate.cumulative?.[name]))&&candidate.cumulative.cached<=candidate.cumulative.input;
  return {accountEpoch:value.accountEpoch,dispatched:value.dispatched,upstreamId,cursor:valid?{messageCount:candidate.messageCount,historyDigest:candidate.historyDigest,cumulative:{input:candidate.cumulative.input,output:candidate.cumulative.output,cached:candidate.cumulative.cached}}:null};
}
export function chatSettlementFacts(admission,result,{dispatched=true}={}){
  const session=admission.session,previous={input:session.cum_input,output:session.cum_output,cached:session.cum_cached},next=result.cumulativeUsage||{input:previous.input+(result.input||0),output:previous.output+(result.output||0),cached:previous.cached+(result.cached||0)};
  const valid=result.usageKnown!==false&&(result.status||'ok')==='ok'&&typeof result.text==='string'&&['input','output','cached'].every(name=>count(result[name]??0)&&count(next[name])&&next[name]===previous[name]+(result[name]||0))&&next.cached<=next.input;
  return canonicalChatFacts({accountEpoch:admission.account.route_epoch,dispatched,upstreamId:result.threadId||session.upstream_id,cursor:valid?{messageCount:session.message_count+admission.delta.length+1,historyDigest:digestChatMessages(session.history_digest,[...admission.delta,{role:'assistant',content:result.text}]),cumulative:next}:null});
}
