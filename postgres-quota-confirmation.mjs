import {AppError,usageWindow} from './store.mjs';
import {createAdminDepartments} from './postgres-admin-departments.mjs';
import {createPostgresAudit} from './postgres-audit.mjs';
import {lockPostgresRouting} from './postgres-routing.mjs';
import {quotaSubjectsReady,lockQuotaSubjectState} from './postgres-quota-subjects.mjs';
import {sanitizeRecord} from './record-redaction.mjs';
import {requireDepartmentScope} from './department-access.mjs';

const fail=(status,code,message)=>{throw new AppError(status,message,code);};
const eligible=row=>row&&Number.isSafeInteger(row.ended)&&!['queued','running'].includes(row.status)&&row.usage_unknown===1&&typeof row.quota_subject_ref==='string'&&['account_metered_bucket','same_credential'].includes(row.quota_subject_scope);
const publicFact=row=>({confirmed:true,at:row.confirmed_at,statement:row.statement,source:'administrator_confirmation'});

export async function quotaConfirmationView(store,request,{canManage=true}={}){
  if(!await quotaSubjectsReady(store)||!eligible(request))return {can_confirm_upstream:false};
  const previous=await store.get('SELECT confirmed_at,statement FROM harbor.quota_subject_confirmations WHERE request_id=$1 AND subject_ref=$2',request.id,request.quota_subject_ref);
  return {can_confirm_upstream:canManage&&!previous,...(previous?{upstream_confirmation:publicFact(previous)}:{})};
}

export function createPostgresQuotaConfirmation(store,{clock=Date.now}={}){
  if(!store||typeof store.transaction!=='function'||typeof clock!=='function')fail(503,'upstream_confirmation_configuration','上游核对依赖尚未准备好');
  const access=createAdminDepartments(store),audit=createPostgresAudit(store,{clock});
  async function confirm(requestId,payload,{scope,revalidateScope}={}){
    if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).sort().join(',')!=='confirmed,reason,statement'||payload.confirmed!==true||!['upstream_finished','not_executed'].includes(payload.statement)||typeof payload.reason!=='string'||!payload.reason.trim()||payload.reason.length>400)fail(400,'upstream_confirmation_invalid','请明确核对结论并填写理由');
    const reason=sanitizeRecord(payload.reason.trim(),{maxBytes:1024});
    if(!await quotaSubjectsReady(store))fail(503,'quota_subject_contract_required','共享额度核对尚未启用');
    return store.transaction(async()=>{
      const initial=await access.resolve(scope,{write:true});await lockPostgresRouting(store);
      const row=await store.get('SELECT * FROM harbor.requests WHERE id=$1'+(initial?.view==='department'?' AND department_id=$2':'')+' FOR UPDATE',requestId,...(initial?.view==='department'?[initial.department_id]:[]));
      const refreshed=typeof revalidateScope==='function'?await revalidateScope(scope):scope;
      requireDepartmentScope(refreshed,{allowGlobal:true});
      if(scope!==undefined&&['role','view','department_id','credential_id','credential_version'].some(key=>refreshed?.[key]!==scope[key]))fail(403,'department_forbidden','管理身份已变化');
      const authorization=await access.resolve(refreshed,{write:true});await access.current(authorization,{lock:true});access.same(authorization,row);
      const window=usageWindow(clock()),time=row.ended??row.started;
      if(time<window.start||time>=window.end)fail(404,'request_not_found','请求记录不存在或超出近7天可见范围');
      if(!eligible(row))fail(409,'upstream_confirmation_not_eligible','只能核对已终态且用量未知的已识别主体请求');
      await lockQuotaSubjectState(store);
      if(typeof revalidateScope==='function'){
        const latest=await revalidateScope(refreshed);requireDepartmentScope(latest,{allowGlobal:true});
        if(['role','view','department_id','credential_id','credential_version'].some(key=>latest[key]!==refreshed[key]))fail(403,'department_forbidden','管理身份已变化');
        await access.current(authorization,{lock:true});
      }
      const previous=await store.get('SELECT * FROM harbor.quota_subject_confirmations WHERE request_id=$1',row.id);
      if(previous){if(previous.subject_ref!==row.quota_subject_ref)fail(409,'upstream_confirmation_conflict','原核对关联已变化');return {...publicFact(previous),replayed:true};}
      const actor={kind:refreshed.role,id:refreshed.credential_id,version:refreshed.credential_version};
      const at=clock();
      await store.run('INSERT INTO harbor.quota_subject_confirmations(request_id,subject_ref,statement,confirmed_at,actor_kind,actor_id,actor_version,reason) VALUES($1,$2,$3,$4,$5,$6,$7,$8)',row.id,row.quota_subject_ref,payload.statement,at,actor.kind,actor.id,actor.version,reason);
      await audit.write(authorization??undefined,{action:'request.upstream.confirmed',detail:{request:row.id,statement:payload.statement,reason,actor_kind:actor.kind,confirmed_at:at},subject:{kind:'request',id:row.id},at});
      return {...publicFact({confirmed_at:at,statement:payload.statement}),replayed:false};
    }).catch(error=>{if(error instanceof AppError)throw error;fail(503,'upstream_confirmation_persistence','上游核对未保存，请稍后重试');});
  }
  return {confirm};
}
