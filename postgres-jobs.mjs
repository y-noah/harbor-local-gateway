import {randomUUID} from 'node:crypto';
import {AppError} from './store.mjs';

const validKind=value=>typeof value==='string'&&/^[a-z][a-z0-9_.-]{0,79}$/.test(value);
const encoded=value=>{let text;try{text=JSON.stringify(value);}catch{}if(!text||Buffer.byteLength(text)>65536)throw new AppError(400,'后台任务数据超过限制','job_payload_invalid');return text;};
const lost=()=>new AppError(409,'后台任务租约已失效','job_lease_lost');
export function createPostgresJobs(store,{clock,leaseMs=30000,maxPending=1000,reservedKinds=[]}={}){
  if(!store||clock!==undefined&&typeof clock!=='function'||!Number.isInteger(leaseMs)||leaseMs<1000||leaseMs>300000||!Number.isInteger(maxPending)||maxPending<1||maxPending>100000||!Array.isArray(reservedKinds)||reservedKinds.length>8||new Set(reservedKinds).size!==reservedKinds.length||reservedKinds.some(kind=>!validKind(kind)))throw new AppError(503,'后台任务资源配置无效','job_configuration');
  const now=async()=>{const at=clock?clock():(await store.get('SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint at')).at;if(!Number.isSafeInteger(at)||at<0)throw new AppError(503,'后台任务时钟无效','job_configuration');return at;};
  async function enqueue(kind,{id=randomUUID(),target=null,payload={},availableAt,maxAttempts=5,dependencyId=null}={}){
    if(!validKind(kind)||typeof id!=='string'||!id.length||id.length>200||target!==null&&(typeof target!=='string'||target.length>200)||!payload||typeof payload!=='object'||Array.isArray(payload)||!Number.isInteger(maxAttempts)||maxAttempts<1||maxAttempts>20||availableAt!==undefined&&(!Number.isSafeInteger(availableAt)||availableAt<0))throw new AppError(400,'后台任务参数无效','job_payload_invalid');
    if(dependencyId!==null&&(typeof dependencyId!=='string'||!dependencyId.length||dependencyId.length>200||dependencyId===id))throw new AppError(400,'后台任务依赖无效','job_dependency_invalid');
    const body=encoded(payload);
    return store.transaction(async()=>{
      await store.query("SELECT pg_advisory_xact_lock(hashtextextended('harbor-background-admission',0))");
      const existing=await store.get('SELECT id,kind,target,payload,max_attempts,dependency_id FROM harbor.background_jobs WHERE id=$1',id);
      if(existing){if(existing.kind!==kind||existing.target!==target||existing.max_attempts!==maxAttempts||existing.dependency_id!==dependencyId||!(await store.get('SELECT $1::jsonb=$2::jsonb equal',body,JSON.stringify(existing.payload))).equal)throw new AppError(409,'任务标识已用于其他内容','job_identity_conflict');return {id,created:false};}
      if(dependencyId!==null&&!await store.get('SELECT id FROM harbor.background_jobs WHERE id=$1',dependencyId))throw new AppError(400,'后台任务前置依赖不存在','job_dependency_invalid');
      const pending=(await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE status IN ('pending','running')")).n;
      const reserved=reservedKinds.includes(kind);
      if(pending>=maxPending+(reserved?reservedKinds.length:0)||reserved&&(await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE kind=$1 AND status IN ('pending','running')",kind)).n>=1)throw new AppError(503,'后台任务队列已满','job_queue_full');
      const at=await now();await store.run("INSERT INTO harbor.background_jobs(id,kind,target,payload,available_at,created,updated,max_attempts,dependency_id) VALUES($1,$2,$3,$4::jsonb,$5,$6,$6,$7,$8)",id,kind,target,body,availableAt??at,at,maxAttempts,dependencyId);return {id,created:true};
    });
  }
  async function claim(kinds){
    if(!Array.isArray(kinds)||!kinds.length||kinds.length>32||kinds.some(kind=>!validKind(kind)))throw new AppError(503,'后台任务类型配置无效','job_configuration');
    return store.transaction(async()=>{
      const at=await now();
      await store.run("WITH exhausted AS (SELECT id FROM harbor.background_jobs WHERE kind=ANY($2::text[]) AND attempts>=max_attempts AND (status='pending' OR status='running' AND lease_until<=$1) ORDER BY id LIMIT 256 FOR UPDATE SKIP LOCKED) UPDATE harbor.background_jobs j SET status=CASE WHEN j.result IS NULL THEN 'failed' ELSE 'completed' END,error_code=CASE WHEN j.result IS NULL THEN 'job_attempts_exhausted' ELSE NULL END,claim_token=NULL,lease_until=NULL,updated=$1 FROM exhausted e WHERE j.id=e.id",at,kinds);
      await store.run("WITH blocked AS (SELECT j.id FROM harbor.background_jobs j JOIN harbor.background_jobs d ON d.id=j.dependency_id WHERE j.kind=ANY($2::text[]) AND j.result IS NULL AND (j.status='pending' OR j.status='running' AND j.lease_until<=$1) AND d.status IN ('failed','cancelled') ORDER BY j.id LIMIT 256 FOR UPDATE OF j SKIP LOCKED) UPDATE harbor.background_jobs j SET status='failed',error_code='job_dependency_failed',claim_token=NULL,lease_until=NULL,updated=$1 FROM blocked b WHERE j.id=b.id",at,kinds);
      const row=await store.get(`SELECT id FROM harbor.background_jobs j WHERE kind=ANY($1::text[]) AND attempts<max_attempts
        AND ((status='pending' AND available_at<=$2) OR (status='running' AND lease_until<=$2))
        AND (result IS NOT NULL OR dependency_id IS NULL OR EXISTS(SELECT 1 FROM harbor.background_jobs d WHERE d.id=j.dependency_id AND d.status='completed'))
        ORDER BY available_at,created,id LIMIT 1 FOR UPDATE SKIP LOCKED`,kinds,at);if(!row)return null;
      return store.get("UPDATE harbor.background_jobs SET status='running',attempts=attempts+1,claimed_at=$1,lease_until=$2,claim_token=$3,updated=$1,error_code=NULL WHERE id=$4 RETURNING id,kind,target,payload,attempts,max_attempts,lease_until,claim_token,result,dependency_id",at,at+leaseMs,randomUUID(),row.id);
    });
  }
  async function heartbeat(job){return store.transaction(async()=>{
    const row=await store.get('SELECT status,claim_token,lease_until FROM harbor.background_jobs WHERE id=$1 FOR UPDATE',job.id),at=await now();
    if(!row||row.status!=='running'||row.claim_token!==job.claim_token||row.lease_until<=at)return false;
    await store.run('UPDATE harbor.background_jobs SET lease_until=$1,updated=$2 WHERE id=$3',at+leaseMs,at,job.id);return true;
  });}
  async function owned(job,operation){
    return store.transaction(async()=>{
      // Lease/status writers still serialize, while export metadata foreign-key
      // KEY SHARE checks can coexist with ownership and avoid reversing locks.
      const row=await store.get("SELECT status,claim_token,lease_until FROM harbor.background_jobs WHERE id=$1 FOR NO KEY UPDATE",job.id),at=await now();
      if(!row||row.status!=='running'||row.claim_token!==job.claim_token||row.lease_until<=at)throw lost();
      const result=await operation(store);
      if(row.lease_until<=await now())throw lost();return result;
    });
  }
  async function complete(job,result={}){
    const body=encoded(result);
    return store.transaction(async()=>{
      const row=await store.get('SELECT status,claim_token,result FROM harbor.background_jobs WHERE id=$1 FOR UPDATE',job.id);
      if(row?.status==='completed'&&row.claim_token===job.claim_token)return false;
      if(row?.result!==null&&row?.result!==undefined&&!(await store.get('SELECT $1::jsonb=$2::jsonb equal',body,JSON.stringify(row.result))).equal)throw new AppError(409,'后台任务结果已固定','job_result_conflict');
      return owned(job,async()=>{await store.run("UPDATE harbor.background_jobs SET status='completed',result=$1::jsonb,lease_until=NULL,updated=$2 WHERE id=$3",body,await now(),job.id);return true;});
    });
  }
  async function commitEffect(job,operation){
    return owned(job,async()=>{
      const row=await store.get('SELECT result FROM harbor.background_jobs WHERE id=$1',job.id);if(row.result!==null)return row.result;
      const result=await operation(store);await store.run('UPDATE harbor.background_jobs SET result=$1::jsonb,updated=$2 WHERE id=$3',encoded(result??{}),await now(),job.id);return result??{};
    });
  }
  async function fail(job,{retry=true,errorCode='job_failed',retryDelayMs=1000}={}){
    if(typeof retry!=='boolean'||typeof errorCode!=='string'||!/^[a-z][a-z0-9_]{0,79}$/.test(errorCode)||!Number.isInteger(retryDelayMs)||retryDelayMs<0||retryDelayMs>3600000)throw new AppError(400,'后台任务失败状态无效');
    return owned(job,async()=>{const at=await now();return (await store.get("UPDATE harbor.background_jobs SET status=CASE WHEN result IS NOT NULL THEN 'completed' WHEN $1 AND attempts<max_attempts THEN 'pending' ELSE 'failed' END,available_at=$2,lease_until=NULL,claim_token=CASE WHEN result IS NOT NULL THEN claim_token ELSE NULL END,error_code=CASE WHEN result IS NOT NULL THEN NULL ELSE $3 END,updated=$4 WHERE id=$5 RETURNING status",retry,at+retryDelayMs,errorCode,at,job.id)).status;});
  }
  async function cancel(id){return (await store.run("UPDATE harbor.background_jobs SET status='cancelled',claim_token=NULL,lease_until=NULL,updated=$1 WHERE id=$2 AND status IN ('pending','running') AND result IS NULL",await now(),id)).changes===1;}
  return {enqueue,claim,heartbeat,owned,commitEffect,complete,fail,cancel,now,leaseMs};
}
