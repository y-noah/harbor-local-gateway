import {AppError,hash} from './store.mjs';
import {readConsumptionPolicy,consumptionSource,assertConsumptionSource,subscriptionFallbackDecision} from './consumption-policy.mjs';
import {createPostgresConsumptionPolicy,verifyConsumptionPolicyAvailability} from './postgres-consumption-policy.mjs';
import {findPostgresResponseSession,lockPostgresRouting,createPostgresConsumptionRoutingScope} from './postgres-routing.mjs';
import {replayableResponsesInput} from './responses-replay-context.mjs';

const fail=(code,message,status=409)=>{throw new AppError(status,message,code);};
// This adapter receives only the authenticated member and server-owned options.
// It chooses a route, never invokes a model or releases an execution lease.
export function createPostgresConsumptionRouting(store,{router,clock=Date.now,schemaAvailable}={}){
  if(typeof router?.resolve!=='function'||typeof router?.catalogCandidates!=='function'||schemaAvailable!==undefined&&typeof schemaAvailable!=='boolean'&&typeof schemaAvailable!=='function')throw new TypeError('consumption_routing_configuration');
  const policies=createPostgresConsumptionPolicy(store,{clock}),tokens=new WeakMap(),used=new WeakSet();let availability;
  const available=async()=>{
    if(typeof schemaAvailable==='function'){const value=await schemaAvailable();if(typeof value!=='boolean')throw new TypeError('consumption_schema_availability_invalid');return value;}
    if(typeof schemaAvailable==='boolean')return schemaAvailable;
    availability??=verifyConsumptionPolicyAvailability(store).catch(error=>{availability=undefined;throw error;});return availability;
  };
  const transaction=()=>{if(!store.context.getStore())fail('database_transaction_required','来源路由必须位于请求受理事务',503);};
  async function read(member){
    transaction();if(!member?.id||!member.key_id)fail('invalid_api_key','缺少已认证的Key身份',401);
    const schema=await available();
    if(!schema)return Object.freeze({...readConsumptionPolicy({}),schemaAvailable:false});
    return Object.freeze({...await policies.read(member.key_id,{memberId:member.id,lock:true}),schemaAvailable:true});
  }
  async function context(session,payload,{excludeRequestId=null}={}){
    if(!replayableResponsesInput(payload))fail('session_context_unsupported','改变消耗来源需要完整且可重放的上下文');
    const last=await store.get('SELECT key_id,model,handoff_context,handoff_input,status,started,query_sequence FROM harbor.requests WHERE session_id=$1 AND route_version=$2 AND ($3::text IS NULL OR id<>$3) ORDER BY query_sequence DESC LIMIT 1',session.id,session.route_version,excludeRequestId);
    if(last&&last.handoff_context!=='replayable')fail('session_context_unsupported','之前的上下文尚未验证可跨账号续接');
    if(last&&await store.get("SELECT id FROM harbor.requests WHERE session_id=$1 AND route_version=$2 AND query_sequence<$3 AND (ended IS NULL OR ended>=$4) AND handoff_context<>'replayable' AND ($5::text IS NULL OR id<>$5) LIMIT 1",session.id,session.route_version,last.query_sequence,last.started,excludeRequestId))fail('session_context_unsupported','并行旧请求存在未知工具或会话上下文');
    if(last){
      const key=await store.get('SELECT id,enabled,expires FROM harbor.keys WHERE id=$1 AND member_id=$2 AND removed_at IS NULL',last.key_id,session.member_id);
      if(!key?.enabled||key.expires!==null&&key.expires<=clock())fail('session_permissions_changed','最近请求的访问权限已变化');
      try{await store.checkModel({id:session.member_id,key_id:key.id},last.model);}catch{fail('session_permissions_changed','最近请求的模型权限已变化');}
    }
  }
  async function scoped(member,payload,headers,options,snapshot,{excludeRequestId=null,sourceOverride=null,expectedSession=null}={}){
    await lockPostgresRouting(store);
    const identity=await findPostgresResponseSession(store,member,payload,headers,{lock:true}),session=identity.session??(expectedSession&&!identity.fixed?await store.get("SELECT * FROM harbor.sessions WHERE id=$1 AND member_id=$2 AND protocol='responses' FOR UPDATE",expectedSession.id,member.id):null);
    const view=await router.catalogCandidates({at:clock(),policy:snapshot.consumption_policy,model:payload.model,protocol:options.protocol??'responses'});
    let source=sourceOverride;
    if(!source){
      if(snapshot.consumption_policy==='api_only')source='api';
      else if(snapshot.consumption_policy==='subscription_only')source='subscription';
      else{
        const bound=session&&await store.get('SELECT kind,auth_mode FROM harbor.accounts WHERE id=$1',session.account_id);
        if(consumptionSource(bound)==='subscription')source='subscription';
        else if(view.candidates.some(item=>item.source==='api'))source='api';
        else{
          const api=view.sources.api;
          if(!api.configuredIds.length||api.configuredIds.length!==api.quotaExhaustedIds.length)fail('consumption_api_unavailable','官方API配置不可用，不能当作额度耗尽使用订阅',503);
          source='subscription';
        }
      }
    }
    const accountIds=new Set(view.sources[source].accountIds),bound=session&&await store.get('SELECT kind,auth_mode FROM harbor.accounts WHERE id=$1',session.account_id),changing=!!session&&consumptionSource(bound)!==source;
    if(changing){if(identity.fixed)fail('consumption_source_conflict','固定会话来源与当前Key配置冲突');await context(session,payload,{excludeRequestId});}
    const scope=createPostgresConsumptionRoutingScope({accountIds,source,rebind:changing,expectedSession:expectedSession??session,excludeRequestId,resumeSession:!!expectedSession});
    const route=await router.resolve(member,payload,headers,{...options,consumptionScope:scope});assertConsumptionSource(snapshot.consumption_policy,route.account);
    if(consumptionSource(route.account)!==source)fail('consumption_source_conflict','返回账号不符合已选消耗来源');return route;
  }
  async function resolve(member,payload,headers={},options={}){
    const snapshot=await read(member);
    const route=snapshot.consumption_policy==='inherit'?await router.resolve(member,payload,headers,options):await scoped(member,payload,headers,options,snapshot);
    const consumption=Object.freeze({...snapshot,consumption_source:consumptionSource(route.account),fallback:false});
    tokens.set(consumption,{memberId:member.id,keyId:member.key_id,sessionId:route.session.id,accountId:route.account.id,routeVersion:route.session.route_version,snapshot,account:Object.freeze({kind:route.account.kind,auth_mode:route.account.auth_mode}),payloadDigest:hash(JSON.stringify(payload)),protocol:options.protocol??'responses'});
    return {...route,consumption};
  }
  async function fallback({member,payload,headers={},options={},requestId,route,snapshot=route?.consumption,error,attempts,outputStarted,contextReplayable}={}){
    transaction();const token=snapshot&&tokens.get(snapshot);
    if(!token||used.has(snapshot)||token.memberId!==member?.id||token.keyId!==member?.key_id||token.payloadDigest!==hash(JSON.stringify(payload))||token.protocol!==(options.protocol??'responses')||!token.snapshot.schemaAvailable)fail('consumption_fallback_conflict','备用来源缺少原受理身份');
    const decision=subscriptionFallbackDecision({policy:token.snapshot.consumption_policy,account:token.account,error,payload,attempts,outputStarted,contextReplayable});if(!decision.allowed)fail(decision.reason,'当前请求不满足安全备用条件');
    await lockPostgresRouting(store);
    const identity=await findPostgresResponseSession(store,member,payload,headers,{lock:true}),session=identity.session??await store.get("SELECT * FROM harbor.sessions WHERE id=$1 AND member_id=$2 AND protocol='responses' FOR UPDATE",token.sessionId,member.id);
    if(identity.fixed)fail('consumption_source_conflict','固定会话不能自动改变消耗来源');
    if(!session||session.id!==token.sessionId||session.account_id!==token.accountId||session.route_version!==token.routeVersion)fail('session_route_conflict','会话绑定已变化');
    const request=await store.get('SELECT * FROM harbor.requests WHERE id=$1 AND member_id=$2 AND key_id=$3 FOR UPDATE',requestId,member.id,member.key_id);
    const previous=request?.consumption_attempts,attempt=Array.isArray(previous)&&previous[0];
    if(!request||!['queued','running'].includes(request.status)||request.session_id!==token.sessionId||request.account_id!==token.accountId||request.route_version!==token.routeVersion||request.consumption_policy!==token.snapshot.consumption_policy||request.consumption_policy_version!==token.snapshot.consumption_policy_version||request.consumption_fallback!==false||!Array.isArray(previous)||previous.length!==1||attempt.source!=='api'||attempt.account_id!==token.accountId||attempt.state!=='rejected'||attempt.usage_known!==true||attempt.error_code!=='upstream_quota_exhausted'||['input','output','cached'].some(field=>attempt[field]!==0)||request.handoff_input!=='replayable')fail('consumption_fallback_conflict','备用来源与原请求派发证据不一致');
    const next=await scoped(member,payload,headers,options,token.snapshot,{excludeRequestId:requestId,sourceOverride:'subscription',expectedSession:session});
    used.add(snapshot);return {...next,consumption:Object.freeze({...token.snapshot,consumption_source:'subscription',fallback:true})};
  }
  return {read,resolve,fallback,catalogCandidates:options=>router.catalogCandidates(options)};
}
