import {createAdminDepartments} from './postgres-admin-departments.mjs';
import {createPostgresAudit} from './postgres-audit.mjs';
import {AppError,id} from './store.mjs';
import {createRoutingDepartments} from './postgres-routing-departments.mjs';
import {bool} from './admin-input.mjs';
import {lockPostgresRouting} from './postgres-routing.mjs';

export function createPostgresSessionAdmin(store,{clock=Date.now,isPreparing=()=>false,isStopping=()=>false,preparation}={}){
  if(preparation&&typeof preparation.reserve!=='function')throw new AppError(503,'会话交接保护未配置','postgres_configuration');
  const departments=createAdminDepartments(store);
  const running=()=>{if(isStopping())throw new AppError(503,'会话管理正在关闭','service_stopping');};
  const audit=(scope,action,detail,sessionId)=>createPostgresAudit(store,{clock}).write(scope,{action,detail,subject:{kind:'session',id:sessionId}});
  const member=async(memberId,missingStatus=404,scope=null)=>{await departments.current(scope,{lock:true,write:true});const row=await store.get('SELECT * FROM harbor.members WHERE id=$1 AND removed_at IS NULL FOR UPDATE',memberId);if(!row)throw new AppError(missingStatus,missingStatus===400?'请选择有效员工及已启用、检查通过的API账号':'会话或员工不存在');departments.same(scope,row);running();return row;};
  const transaction=fn=>store.transaction(async()=>{running();const result=await fn();running();return result;});
  async function handle(path,method,payload={},options={}){
    if(!(path==='/api/admin/sessions'&&method==='POST')&&!(/^\/api\/admin\/sessions\/[a-zA-Z0-9-]+$/.test(path)&&method==='PATCH'))return null;
    const scope=await departments.resolve(options.scope,{write:true,payload});
    if(path==='/api/admin/sessions'&&method==='POST')return {status:201,value:await transaction(async()=>{
      const owner=await member(payload.member_id,400,scope);await lockPostgresRouting(store);running();
      const account=await store.get("SELECT * FROM harbor.accounts WHERE id=$1 AND auth_mode='openai_api' AND enabled=1 AND state='ready' AND route_block IS NULL AND removed_at IS NULL FOR SHARE",payload.account_id);
      if(account)departments.same(scope,account);
      if(!owner.enabled||!account)throw new AppError(400,'请选择有效员工及已启用、检查通过的API账号');
      const sessionId=id(),at=clock();await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,nightly_enabled,created,updated"+(scope?',department_id':'')+") VALUES($1,$2,$3,'responses',0,$4,$4"+(scope?',$5':'')+")",sessionId,owner.id,account.id,at,...(scope?[scope.department_id]:[]));
      await audit(scope,'session.route',{sessionId,from:null,to:account.id,reason:'manual'},sessionId);return {session_id:sessionId,header:'X-Harbor-Session'};
    })};
    const match=path.match(/^\/api\/admin\/sessions\/([a-zA-Z0-9-]+)$/);if(!match||method!=='PATCH')return null;
    if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).some(k=>!['enabled','account_id','expected_route_version','nightly_enabled'].includes(k))||!['enabled','account_id','nightly_enabled'].some(k=>payload[k]!==undefined))throw new AppError(400,'会话修改参数无效');
    let releasePreparation;
    try{return {status:200,value:await transaction(async()=>{
      const identity=await store.get('SELECT member_id FROM harbor.sessions WHERE id=$1',match[1]);if(!identity)throw new AppError(404,'会话不存在');
      const owner=await member(identity.member_id,404,scope);await lockPostgresRouting(store);running();
      const session=await store.get('SELECT * FROM harbor.sessions WHERE id=$1 AND member_id=$2 FOR UPDATE',match[1],owner.id);if(!session)throw new AppError(404,'会话不存在');departments.same(scope,session);
      const enabled=payload.enabled===undefined?session.enabled:bool(payload.enabled);
      let nightly=payload.nightly_enabled===undefined?(payload.account_id!==undefined?0:session.nightly_enabled):bool(payload.nightly_enabled);
      let expires=nightly?null:payload.account_id!==undefined||payload.nightly_enabled!==undefined?await createRoutingDepartments(store).nextReset(scope,clock()):session.nightly_expires;
      let accountId=session.account_id,thread=session.upstream_thread,version=session.route_version;
      if(payload.account_id!==undefined){
        if(session.protocol!=='responses')throw new AppError(409,'旧CLI会话上下文不能人工切换账号','session_route_unsupported');
        if(!session.enabled||!owner.enabled)throw new AppError(409,'请先恢复会话及员工访问，再修改绑定账号','session_disabled');
        if(typeof payload.account_id!=='string'||!Number.isSafeInteger(payload.expected_route_version)||payload.expected_route_version<0)throw new AppError(400,'请选择有效账号并提供路由版本');
        if(payload.expected_route_version!==version)throw new AppError(409,'会话绑定已变化，请刷新后重试','session_route_conflict');
        if(payload.account_id!==accountId){
          if(preparation){releasePreparation=preparation.reserve([owner.id]);if(!releasePreparation)throw new AppError(409,'会话有在途请求或员工正在准备请求，请完成后再切换','session_busy');}
          if(isPreparing(owner.id)||await store.get("SELECT id FROM harbor.requests WHERE session_id=$1 AND status IN ('queued','running') LIMIT 1",session.id))throw new AppError(409,'会话有在途请求或员工正在准备请求，请完成后再切换','session_busy');
          const target=await store.get("SELECT * FROM harbor.accounts WHERE id=$1 AND kind='codex' AND enabled=1 AND removed_at IS NULL AND state='ready' AND route_block IS NULL FOR SHARE",payload.account_id);
          if(target)departments.same(scope,target);
          if(!target)throw new AppError(400,'目标账号必须是已启用且已连接的Codex账号','invalid_route_account');
          accountId=target.id;thread=id();version++;
        }
      }else if(payload.expected_route_version!==undefined)throw new AppError(400,'路由版本必须与目标账号一起提交');
      const account=await store.get('SELECT auth_mode FROM harbor.accounts WHERE id=$1',accountId);
      if(account?.auth_mode==='openai_api'){if(payload.nightly_enabled!==undefined)throw new AppError(400,'API会话由人工控制，不参与夜间调整');nightly=0;expires=null;}
      await store.run('UPDATE harbor.sessions SET enabled=$1,account_id=$2,upstream_thread=$3,route_version=$4,nightly_enabled=$5,nightly_expires=$6 WHERE id=$7',enabled,accountId,thread,version,nightly,expires,session.id);
      if(nightly!==session.nightly_enabled||expires!==session.nightly_expires)await audit(scope,'session.nightly',{session:session.id,enabled:!!nightly,expires},session.id);
      if(enabled!==session.enabled)await audit(scope,enabled?'session.enable':'session.disable',session.id,session.id);
      if(accountId!==session.account_id)await audit(scope,'session.route',{sessionId:session.id,from:session.account_id,to:accountId,version},session.id);
      return {ok:true,route_version:version};
    })};}finally{releasePreparation?.();}
  }
  return {handle};
}
