import {createPostgresPricingAdmin} from './postgres-pricing-admin.mjs';
import {requireDepartmentScope} from './department-access.mjs';
import {beginAdminRequestTiming,finishAdminRequestTiming,registerAdminRequestTimingSignal,wrapAdminQueryTimingQueue} from './admin-request-timing.mjs';
import {createAdminJsonCompression} from './admin-json-compression.mjs';
import {createPostgresConsumptionPolicy} from './postgres-consumption-policy.mjs';
import {consumptionSource,isTrustedApiQuotaRejection,subscriptionFallbackDecision} from './consumption-policy.mjs';
import {createPostgresExtraRuntime} from './postgres-extra-runtime.mjs';
import {createRequestStreamTiming} from './request-stream-timing.mjs';
import {createStreamingStageObserver} from './streaming-stage-observer.mjs';
import {createPostgresTypeStatisticsAdmin} from './postgres-type-statistics-admin.mjs';
import {createPostgresRecordingJobs} from './postgres-recording-jobs.mjs';
import {createPostgresRecordingAdmin} from './postgres-recording-admin.mjs';
import {recordingCaptureDecision,recordingBufferWrite,recordingFinishReason} from './postgres-recording-storage.mjs';
import {createPostgresPlatformService} from './postgres-platform-service.mjs';
import {createPostgresChatService} from './postgres-chat-service.mjs';
import {createPostgresDeviceAccess} from './postgres-device-access.mjs';
import {createPostgresClientCatalog} from './postgres-client-catalog.mjs';
import {createPostgresSessionAdmin} from './postgres-session-admin.mjs';
import {createPostgresRouter,findPostgresResponseSession,lockPostgresRouting} from './postgres-routing.mjs';
import {realpathSync} from 'node:fs';
import {AppError} from './store.mjs';
import {responseBodyBytes} from './request-body.mjs';
import {validateResponses,validateCodexJson,codexJsonRoutes,isResponsesDispatchSource} from './responses.mjs';
import {captureCodexJsonInput,captureCodexJsonOutput} from './codex-json-recording.mjs';
import {ConversationCapture,currentInput} from './conversation-capture.mjs';
import {observeRequestContextAsync,incompleteRequestContext} from './recording-context-capture.mjs';
import {postgresRequestObservation} from './postgres-observability.mjs';
import {createPostgresRequestQueries} from './postgres-request-query.mjs';
import {createPostgresStateQueries,stateQuery} from './postgres-state-query.mjs';
import {createPostgresQueryQueue} from './postgres-query-queue.mjs';
import {createPostgresMemberAdmin} from './postgres-member-admin.mjs';
import {requestsQuery,historyQuery} from './observability.mjs';
import {createPostgresHistoryQueries} from './postgres-history-query.mjs';
import {createPostgresJobs} from './postgres-jobs.mjs';
import {createPostgresPlanningJobs} from './postgres-planning-jobs.mjs';
import {createRoutingPreparation} from './routing-preparation.mjs';
import {createPostgresSelfQuery} from './postgres-self-query.mjs';
import {createRequestBodyBudget} from './request-body-budget.mjs';
import {createInferenceBudget} from './inference-budget.mjs';
import {createOutputBudget,writeOutput} from './output-budget.mjs';
import {createSessionRequestHandoff} from './session-request-handoff.mjs';
import {createPostgresSessionHandoffs} from './postgres-session-handoffs.mjs';
import {replayableResponsesInput,replayableResponsesEvent} from './responses-replay-context.mjs';
import {createCodexQuotaSubjectIdentity} from './quota-subject.mjs';
import {createPostgresQuotaSubjects} from './postgres-quota-subjects.mjs';
import {createPostgresQuotaConfirmation} from './postgres-quota-confirmation.mjs';

const json=(res,status,value)=>{const encoded=JSON.stringify(value);finishAdminRequestTiming(res,status,{authenticated:true});res.writeHead(status,{'Content-Type':'application/json; charset=utf-8'});res.end(encoded);};
async function sse(res,event,signal,sessionId,output,timing,contentToken){
  if(output)return writeOutput(res,event,{output,signal,timing,contentToken,prefix:'event: '+event.type+'\ndata: ',suffix:'\n\n',headers:{'Content-Type':'text/event-stream; charset=utf-8','X-Accel-Buffering':'no','X-Harbor-Session':sessionId}});
  if(res.destroyed||signal.aborted)throw new AppError(499,'客户端已断开','request_cancelled');
  if(!res.headersSent)res.writeHead(200,{'Content-Type':'text/event-stream; charset=utf-8','X-Accel-Buffering':'no','X-Harbor-Session':sessionId});
  if(res.write('event: '+event.type+'\ndata: '+JSON.stringify(event)+'\n\n'))return;
  await new Promise((resolve,reject)=>{
    const clean=()=>{res.off('drain',drain);res.off('close',close);signal.removeEventListener('abort',close);};
    const drain=()=>{clean();resolve();},close=()=>{clean();reject(new AppError(499,'客户端已断开','request_cancelled'));};
    res.once('drain',drain);res.once('close',close);signal.addEventListener('abort',close,{once:true});if(signal.aborted||res.destroyed)close();
  });
}

// Composable asynchronous endpoint. Routing and shared-limit adapters are
// mandatory: this must never silently replace an existing policy with LRU or
// an unlimited default while the PostgreSQL migration is being developed.
export function createPostgresResponsesHandler({store,queryStore=store,recordings,recordingReader=recordings,buffer,meteringRecovery,resolveRoute,consumptionRouting,sourceAccounts,consumptionPolicyReady,checkLimits,transportForAccount,adminAuthenticated,adminContext,adminActorContext,revalidateAdminScope,departmentActors,onRouteFailure,requestQueries,keyEncryptionKey,adminStateContext,accountAdmin,deviceIssuer,platformTransport,runChat,chatOptions={},cliProcesses,outputBudget=createOutputBudget(),executionOptions={},pricingOptions={},stageObserver,requestBodies=createRequestBodyBudget({stageObserver}),codexBinary='codex',recordingEnabled=true,captureContext,contextReader,contextIndexEnabled=false,recordingPolicy=()=>true,recordingBackend,recordingKey,recordingStoragePolicy,recordingBackendOptions={},recordingAdminOptions={},maxConcurrent=120,maxPerMember=8,maxPendingHandoffs=20000+64,clock=Date.now,extraRuntime=createPostgresExtraRuntime(store,{clock})}={}){
  const quotaSubjects=createPostgresQuotaSubjects(store,{identity:Buffer.isBuffer(keyEncryptionKey)&&keyEncryptionKey.length===32?createCodexQuotaSubjectIdentity(keyEncryptionKey,{schemaVersion:'0.167.2'}):undefined,clock});
  if(adminActorContext!==undefined&&typeof adminActorContext!=='function')throw new AppError(503,'管理身份核对依赖无效','postgres_configuration');
  if(consumptionRouting!==undefined&&(!consumptionRouting||['read','resolve','fallback'].some(name=>typeof consumptionRouting[name]!=='function'))||sourceAccounts!==undefined&&typeof sourceAccounts!=='function'||consumptionPolicyReady!==undefined&&typeof consumptionPolicyReady!=='function')throw new AppError(503,'消耗来源请求依赖无效','postgres_configuration');
  if(typeof contextIndexEnabled!=='boolean'||captureContext!==undefined&&typeof captureContext!=='function'||contextReader!==undefined&&typeof contextReader?.read!=='function')throw new AppError(503,'上下文采集依赖无效','recording_configuration');
  if(!store||!recordings||!buffer||!meteringRecovery||['settle','flush','status','close'].some(name=>typeof meteringRecovery[name]!=='function')||[resolveRoute,checkLimits,transportForAccount,adminAuthenticated].some(fn=>typeof fn!=='function')||onRouteFailure!==undefined&&typeof onRouteFailure!=='function'||requestQueries!==undefined&&(!requestQueries||['read','close','status'].some(name=>typeof requestQueries[name]!=='function')||!requestQueries.queue||['run','close','status'].some(name=>typeof requestQueries.queue[name]!=='function'))||adminStateContext!==undefined&&typeof adminStateContext!=='function'||typeof recordingEnabled!=='boolean'||![maxConcurrent,maxPerMember].every(n=>Number.isInteger(n)&&n>=1))throw new AppError(503,'PostgreSQL请求处理依赖尚未配置完整','postgres_configuration');
  const quotaConfirmation=createPostgresQuotaConfirmation(store,{clock});
  if(!meteringRecovery.directory||!buffer.directory||realpathSync(meteringRecovery.directory)===realpathSync(buffer.directory))throw new AppError(503,'计量与正文须使用独立恢复目录','metering_configuration');
  if(adminContext!==undefined&&typeof adminContext!=='function'||revalidateAdminScope!==undefined&&typeof revalidateAdminScope!=='function')throw new AppError(503,'部门请求鉴权未配置完整','postgres_configuration');
  const authorizeAdmin=async req=>{
    if(adminContext){const scope=await adminContext(req);if(scope!==undefined)requireDepartmentScope(scope,{allowGlobal:true});return scope;}
    if(!await adminAuthenticated(req))throw new AppError(401,'请先输入管理密码登录','admin_required');
    return undefined;
  };
  const refreshAdmin=async(req,scope)=>{
    if(scope===undefined)return scope;
    if(typeof revalidateAdminScope!=='function')throw new AppError(503,'部门管理会话复验未配置','postgres_configuration');
    const current=await revalidateAdminScope(req,scope);requireDepartmentScope(current,{allowGlobal:true});return current;
  };
  const authorizeLegacyAdmin=async req=>{
    let scope=await authorizeAdmin(req);
    if(scope!==undefined)throw new AppError(503,'此入口尚未完成部门隔离适配','department_operation_unavailable');
    return true;
  };
  const consumptionPolicies=createPostgresConsumptionPolicy(store,{clock});
  const stages=createStreamingStageObserver(stageObserver);
  const timingPersistence={queued:0,rejected:0};
  const active=new Set(),members=new Map(),preparing=new Map(),accounts=new Map();let stopping=false;
  const execution=createInferenceBudget({...executionOptions,maxConcurrent,maxPerMember:Math.min(maxPerMember,maxConcurrent)});
  const preparation=createRoutingPreparation({maxWaiting:maxConcurrent,stageObserver});
  const sessionHandoffs=createSessionRequestHandoff({maxTickets:maxConcurrent,maxPending:maxPendingHandoffs,maxWaiting:Math.max(1,Math.floor(maxConcurrent/2)),maxWaitingPerMember:Math.max(1,Math.floor(maxPerMember/2)),stageObserver});
  const queryQueue=requestQueries?.queue||wrapAdminQueryTimingQueue(createPostgresQueryQueue());
  const adminJsonCompression=createAdminJsonCompression();
  const usageJson=async(req,res,value,signal)=>adminJsonCompression.send(req,res,JSON.stringify(value),{authenticated:true,signal,beforeHeaders:()=>finishAdminRequestTiming(res,200,{authenticated:true})});
  const handoffAdmin=createPostgresSessionHandoffs(store,{departmentActors,gate:sessionHandoffs,readStore:queryStore,queryQueue,clock,isStopping:()=>stopping,validateTarget:async(account,model,signal)=>{if(!model)return true;const transport=await transportForAccount(account);if(typeof transport.models!=='function')return false;const models=await transport.models({binary:codexBinary,home:account.auth_mode==='openai_api'?account.id:account.home,signal});return models.some(item=>item.slug===model);}});
  const queries=requestQueries||createPostgresRequestQueries(queryStore,{clock,queryQueue,businessOutcomes:true,readInputPreviews:typeof recordingReader?.previews==='function'?(ids,options)=>recordingReader.previews(ids,options):undefined});
  const adminRouter=createPostgresRouter(store,{clock,isStopping:()=>stopping,extraRuntime}),readRouter=createPostgresRouter(queryStore,{clock});
  const states=createPostgresStateQueries(queryStore,{clock,encryptionKey:keyEncryptionKey,queryQueue,balancingReader:(at,options)=>readRouter.readView({at,...options})});
  const readAdminState=async(query,{signal,scope}={})=>({...await states.read(query,{signal,scope}),...(adminStateContext?await adminStateContext({scope}):{}),...(scope?.view==='department'?{}:{busy:active.size,queue:chat?.status().queued??0,preparing:[...preparing.values()].reduce((sum,n)=>sum+n,0)})});
  const sessionAdmin=createPostgresSessionAdmin(store,{clock,preparation,isPreparing:memberId=>preparing.has(memberId),isStopping:()=>stopping});
  const planning=createPostgresPlanningJobs(store,{jobs:createPostgresJobs(store,{departmentActors,scopedKinds:['routing.preview']}),clock}),planningReader=createPostgresPlanningJobs(queryStore,{jobs:createPostgresJobs(queryStore),clock});
  const history=createPostgresHistoryQueries(queryStore,{clock});
  const pricingAdmin=createPostgresPricingAdmin(store,{...pricingOptions,clock,adminAuthenticated,adminContext:authorizeAdmin,readStore:queryStore,queryQueue,readBody:(req,{signal}={})=>requestBodies.read(req,{owner:'admin',maxBytes:65536,signal})});
  const memberAdmin=createPostgresMemberAdmin(store,{clock,readStore:queryStore,encryptionKey:keyEncryptionKey,queryQueue,isBusy:memberId=>members.has(memberId),consumptionReady:consumptionPolicyReady,departmentActors});
  const deviceAccess=createPostgresDeviceAccess(store,{issuer:deviceIssuer,clock,readStore:queryStore,queryQueue});
  const clientCatalog=createPostgresClientCatalog(store,{deviceAccess,transportForAccount,sourceAccounts,codexBinary,clock});
  const typeStatisticsAdmin=createPostgresTypeStatisticsAdmin(queryStore,{clock,queryQueue,adminAuthenticated,adminContext});
  const recordingJobs=recordingBackend??(recordingKey?createPostgresRecordingJobs(store,{...recordingBackendOptions,departmentActors,contextIndex:contextIndexEnabled,contextStorage:!!contextReader,recordingKey,clock,readStore:queryStore,queryQueue,storagePolicy:recordingStoragePolicy,devices:true}):null);
  const recordingAdmin=recordingJobs?createPostgresRecordingAdmin(recordingJobs,{...recordingAdminOptions,adminAuthenticated,adminContext,revalidateAdminScope,requestBodies}):null;
  const selfQuery=createPostgresSelfQuery(queryStore,{clock,queryQueue});
  const chat=runChat?createPostgresChatService({...chatOptions,quotaSubjects,extraRuntime,execution,cliProcesses,outputBudget,readBody:requestBodies.read,store,buffer,meteringRecovery,checkLimits,runChat,deviceAccess,recordingEnabled,captureContext,recordingPolicy,clock,maxConcurrent,maxPerMember,refreshQuotas:()=>accountAdmin?.refreshQuotas?.()||Promise.resolve(),activity:{active,members,preparing,preparation,accounts,isStopping:()=>stopping}}):null;
  const platform=platformTransport?createPostgresPlatformService({extraRuntime,execution,outputBudget,readBody:requestBodies.read,store,queryStore,queryQueue,buffer,meteringRecovery,checkLimits,transport:platformTransport,credential:(row,signal)=>accountAdmin.platformCredential(row,signal),deviceAccess,recordingEnabled,captureContext,recordingPolicy,clock,maxConcurrent,maxPerMember,activity:{active,members,preparing,preparation,accounts,isStopping:()=>stopping,wake:()=>void chat?.wake()}}):null;
  execution.onRelease(()=>void chat?.wake());cliProcesses?.onRelease(()=>void chat?.wake());
  async function handleRoute(req,res,bodySignal,stage,receipt){
    const url=new URL(req.url,'http://localhost'),path=url.pathname;
    if(recordingAdmin&&await recordingAdmin.handle(req,res))return true;
    if(await typeStatisticsAdmin.handle(req,res,{signal:bodySignal}))return true;
    if(path.startsWith('/api/device/enroll/')&&req.method==='POST'){
      const payload=await requestBodies.read(req,{owner:'device-enrollment',maxBytes:65536,signal:bodySignal});
      const result=await deviceAccess.publicHandle(path,req.method,payload,req);if(result){json(res,result.status,result.value);return true;}
    }
    if(/^\/api\/admin\/(devices|device-invitations)(?:\/|$)/.test(path)){
      let scope=await authorizeAdmin(req);
      const payload=['POST','PATCH'].includes(req.method)?await requestBodies.read(req,{owner:'admin',maxBytes:65536,signal:bodySignal}):{};
      if(['POST','PATCH'].includes(req.method))scope=await refreshAdmin(req,scope);
      const result=await deviceAccess.handle(path,req.method,payload,url.searchParams,{scope,...(scope!==undefined?{revalidateScope:original=>refreshAdmin(req,original)}:{}),...(req.method==='GET'?{signal:bodySignal}:{})});if(result){json(res,result.status,result.value);return true;}
    }
    if(path==='/api/me'&&req.method==='GET'){const secret=String(req.headers.authorization||'').replace(/^Bearer /,''),member=await store.authenticate(secret,{at:clock()});await deviceAccess.authorize(member,req);json(res,200,await selfQuery(secret,{scope:store.departmentScope?.(member)}));return true;}
    if(await clientCatalog.handle(req,res))return true;
    if(platform&&['/v1/platform/models','/v1/platform/chat/completions'].includes(path))return platform.handle(req,res);
    if(path==='/v1/chat/completions'&&req.method==='POST'&&chat)return chat.handle(req,res);
    if(/^\/api\/admin\/(sessions|balancing)(?:\/|$)/.test(path)){
      let scope=await authorizeAdmin(req);
      if(stopping)throw new AppError(503,'会话管理正在关闭','service_stopping');
      const payload=['POST','PATCH'].includes(req.method)?await requestBodies.read(req,{owner:'admin',signal:bodySignal}):{};
      if(['POST','PATCH'].includes(req.method))scope=await refreshAdmin(req,scope);
      if(!payload||typeof payload!=='object'||Array.isArray(payload))throw new AppError(400,'管理参数须为对象');
      if(stopping)throw new AppError(503,'会话管理正在关闭','service_stopping');
      const handoff=await handoffAdmin.handle(path,req.method,payload,{scope,...(scope!==undefined?{revalidateScope:original=>refreshAdmin(req,original)}:{}),...(req.method==='GET'?{signal:bodySignal}:{})});if(handoff){json(res,handoff.status,handoff.value);return true;}
      if(path==='/api/admin/balancing/plans'&&req.method==='POST'){
        if(Object.keys(payload).length)throw new AppError(400,'规划预览无需额外参数');
        const result=await store.transaction(async()=>{if(stopping)throw new AppError(503,'会话管理正在关闭','service_stopping');const result=await planning.enqueue({scope});if(stopping)throw new AppError(503,'会话管理正在关闭','service_stopping');return result;});
        json(res,202,result);return true;
      }
      const planPath=path.match(/^\/api\/admin\/balancing\/plans\/([a-zA-Z0-9-]+)(\/items)?$/);
      if(planPath&&req.method==='GET'){
        let result;
        if(planPath[2]){
          const params=url.searchParams;if([...params.keys()].some(key=>!['section','after','limit'].includes(key))||['after','limit'].some(key=>params.has(key)&&!/^\d+$/.test(params.get(key))))throw new AppError(400,'规划分页参数无效','planning_query_invalid');
          result=await queryQueue.run(()=>planningReader.page(planPath[1],{scope,section:params.get('section')??'moves',after:Number(params.get('after')??0),limit:Number(params.get('limit')??25)}),{signal:bodySignal});
        }else result=await queryQueue.run(()=>planningReader.status(planPath[1],{scope}),{signal:bodySignal});
        json(res,200,result);return true;
      }
      if(path==='/api/admin/balancing/explanation'&&req.method==='GET'){json(res,200,await queryQueue.run(()=>readRouter.readView({explanation:true,scope}),{signal:bodySignal}));return true;}
      if(path==='/api/admin/balancing'&&req.method==='PATCH'){json(res,200,await adminRouter.saveSettings(payload,{scope}));return true;}
      if(path==='/api/admin/balancing/schedule'&&req.method==='PATCH'){json(res,200,await adminRouter.saveTransferSchedule(payload,{scope}));return true;}
      if(path==='/api/admin/balancing/weights'&&req.method==='PATCH'){const result=await adminRouter.saveWeights(payload,{scope});if(scope===undefined)await handoffAdmin.reconcileAutomatic();json(res,200,result);return true;}
      const result=await sessionAdmin.handle(path,req.method,payload,{scope});if(result){json(res,result.status,result.value);return true;}
    }
    if(accountAdmin&&/^\/api\/admin\/(accounts|account-packages|quotas|platform)(?:\/|$)/.test(path)){
      let scope=await authorizeAdmin(req);
      const payload=['POST','PATCH'].includes(req.method)?await requestBodies.read(req,{owner:'admin',signal:bodySignal}):{};
      if(['POST','PATCH'].includes(req.method))scope=await refreshAdmin(req,scope);
      if(!payload||typeof payload!=='object'||Array.isArray(payload))throw new AppError(400,'管理参数须为对象');
      const result=await accountAdmin.handle(path,req.method,payload,{scope,...(scope!==undefined&&revalidateAdminScope?{revalidateScope:original=>revalidateAdminScope(req,original)}:{}),...(req.method==='GET'?{signal:bodySignal}:{})});if(result){void chat?.wake();json(res,result.status,result.value);return true;}
    }
    if(/^\/api\/admin\/(members|keys|member-types|member-config-batches|member-action-batches)(?:\/|$)/.test(path)){
      let scope=await authorizeAdmin(req);
      const payload=['POST','PATCH'].includes(req.method)?await requestBodies.read(req,{owner:'admin',signal:bodySignal}):{};
      if(['POST','PATCH'].includes(req.method))scope=await refreshAdmin(req,scope);
      if(!payload||typeof payload!=='object'||Array.isArray(payload))throw new AppError(400,'管理参数须为对象');
      const result=await deviceAccess.handle(path,req.method,payload,url.searchParams,{scope,...(scope!==undefined?{revalidateScope:original=>refreshAdmin(req,original)}:{}),...(req.method==='GET'?{signal:bodySignal}:{})})||await memberAdmin.handle(path,req.method,payload,url.searchParams,{scope,...(req.method==='GET'?{signal:bodySignal}:{})});if(result){if(path==='/api/admin/members'&&req.method==='GET'&&result.status===200)await usageJson(req,res,result.value,bodySignal);else json(res,result.status,result.value);return true;}
    }
    if((path==='/api/admin/history'||path.startsWith('/api/admin/history/'))&&req.method==='GET'){
      let scope=await authorizeAdmin(req);
      if(path==='/api/admin/history'){json(res,200,await queryQueue.run(()=>history.list(historyQuery(url.searchParams),{scope}),{signal:bodySignal}));return true;}
      const match=path.match(/^\/api\/admin\/history\/nightly\/(\d{4}-\d{2}-\d{2}(?:-\d{4})?)(\/(?:items|revisions))?$/);if(!match)throw new AppError(404,'历史查询入口不存在');
      const params=url.searchParams;
      const allowed=match[2]==='/items'?['section','after','limit','revision']:match[2]==='/revisions'?['after','limit']:['revision'];
      for(const name of params.keys())if(!allowed.includes(name)||params.getAll(name).length!==1)throw new AppError(400,'夜间历史查询参数无效');
      for(const name of ['after','limit','revision'])if(params.has(name)&&!/^\d+$/.test(params.get(name)))throw new AppError(400,'夜间历史分页无效');
      const revision=params.has('revision')?Number(params.get('revision')):undefined,paging={after:Number(params.get('after')??0),limit:Number(params.get('limit')??20),scope};
      const result=await queryQueue.run(()=>match[2]==='/revisions'?history.revisions(match[1],paging):match[2]?history.page(match[1],{...paging,revision,section:params.get('section')??'moves'}):history.nightly(match[1],{revision,scope}),{signal:bodySignal});json(res,200,result);return true;
    }
    if(await pricingAdmin.handle(req,res,{signal:bodySignal}))return true;
    if(path==='/api/admin/state'&&req.method==='GET'){
      let scope=await authorizeAdmin(req);
      json(res,200,await readAdminState(stateQuery(url.searchParams),{signal:bodySignal,scope}));return true;
    }
    if(path==='/api/admin/requests'&&req.method==='GET'){
      let scope=await authorizeAdmin(req);
      await usageJson(req,res,await queries.read(requestsQuery(url.searchParams,{usageTypes:true}),{signal:bodySignal,scope}),bodySignal);return true;
    }
    const contextDetail=path.match(/^\/api\/admin\/requests\/([a-zA-Z0-9-]{1,80})\/context$/);
    if(contextDetail&&req.method==='GET'){
      let scope=await authorizeAdmin(req);
      for(const key of url.searchParams.keys())if(!['after_component','after_part','limit'].includes(key)||url.searchParams.getAll(key).length!==1)throw new AppError(400,'上下文分页无效');
      const numeric=(name,fallback)=>url.searchParams.has(name)&&/^\d+$/.test(url.searchParams.get(name))?Number(url.searchParams.get(name)):url.searchParams.has(name)?NaN:fallback;
      const options={after:{component:numeric('after_component',0),part:numeric('after_part',0)},limit:numeric('limit',32),scope};
      if(!Object.values(options.after).every(value=>Number.isSafeInteger(value)&&value>=0)||!Number.isInteger(options.limit)||options.limit<1||options.limit>100)throw new AppError(400,'上下文分页无效');
      json(res,200,await queryQueue.run(async()=>{
        if(contextReader)return contextReader.read(contextDetail[1],options);
        await postgresRequestObservation(queryStore,contextDetail[1],clock(),{scope});
        return {state:'unavailable',parts:[],next:{component:0,part:0},hasMore:false};
      },{signal:bodySignal}));return true;
    }
    const confirmUpstream=path.match(/^\/api\/admin\/requests\/([a-zA-Z0-9-]{1,80})\/confirm-upstream-ended$/);
    if(confirmUpstream&&req.method==='POST'){
      const actor=async()=>{const scope=await authorizeAdmin(req);if(scope!==undefined)return scope;const identity=await adminActorContext?.(req);requireDepartmentScope(identity,{allowGlobal:true,roles:['super_admin']});return identity;};
      const scope=await actor(),payload=await requestBodies.read(req,{owner:'admin',maxBytes:65536,signal:bodySignal});
      json(res,200,await quotaConfirmation.confirm(confirmUpstream[1],payload,{scope,revalidateScope:original=>adminContext?refreshAdmin(req,original):actor()}));return true;
    }
    const detail=path.match(/^\/api\/admin\/requests\/([a-zA-Z0-9-]{1,80})(\/recording)?$/);
    if(detail&&req.method==='GET'){
      let scope=await authorizeAdmin(req);
      if(!detail[2]){const value=await queryQueue.run(()=>postgresRequestObservation(queryStore,detail[1],clock(),{scope}),{signal:bodySignal});if(scope===undefined&&!adminActorContext||scope!==undefined&&!revalidateAdminScope)value.can_confirm_upstream=false;json(res,200,value);return true;}
      for(const key of url.searchParams.keys())if(!['after','limit'].includes(key)||url.searchParams.getAll(key).length!==1)throw new AppError(400,'正文分页无效');
      const numeric=(name,fallback)=>url.searchParams.has(name)&&/^\d+$/.test(url.searchParams.get(name))?Number(url.searchParams.get(name)):url.searchParams.has(name)?NaN:fallback;
      json(res,200,await queryQueue.run(()=>recordingReader.read(detail[1],{after:numeric('after',0),limit:numeric('limit',32),scope}),{signal:bodySignal}));return true;
    }
    const jsonPath=path.startsWith('/v1/')&&codexJsonRoutes.has(path.slice(3))?path.slice(3):null;
    if((path!=='/v1/responses'&&!jsonPath)||req.method!=='POST')return false;
    const secret=String(req.headers.authorization||'').replace(/^Bearer /,''),member=await store.authenticate(secret,{at:clock()});
    const requestScope=store.departmentScope?.(member);
    const authenticatedScope=current=>{
      const scope=store.departmentScope?.(current);
      if(requestScope!==undefined){
        requireDepartmentScope(scope,{roles:['employee']});
        if(['department_id','member_id','member_version','credential_id','credential_version','department_version'].some(field=>scope[field]!==requestScope[field]))throw new AppError(403,'请求访问身份已变化','request_identity_changed');
      }else if(scope!==undefined)throw new AppError(503,'请求受理期间部门结构已变化','department_migration_conflict');
      return scope;
    };
    await deviceAccess.authorize(member,req);
    const captureDecision=await recordingCaptureDecision(recordingEnabled,recordingPolicy),captureEnabled=captureDecision.enabled;
    if(stopping)throw new AppError(503,'服务正在排空，请稍后重试','service_stopping');
    if(active.size>=maxConcurrent||(members.get(member.id)||0)>=maxPerMember)throw new AppError(429,'请求并发已满，请稍后重试','gateway_busy');
    const controller=new AbortController();active.add(controller);members.set(member.id,(members.get(member.id)||0)+1);stage.emit('lease',{lease:'member',action:'acquire'});const observeCancel=()=>stage.emit('cancel');controller.signal.addEventListener('abort',observeCancel,{once:true});
    const close=()=>{if(!res.writableEnded)controller.abort();};res.on('close',close);
    let output,ticket,releasePreparation,releaseExecution,requestId,routedAccount,contextReplayable=false,contextComplete=false,contextSaved=false,accountTracked=false,capture,requestContext,batch=[],batchBytes=0,flushTimer,recordFailed=false,recordFailureReason=null,recordStarted=false,settlementAttempted=false,dispatched=false,extraBoundary,consumption,attemptIndex=-1,attemptPersistence=Promise.resolve(),upstreamEventSeen=false,currentDispatched=false,actualConsumptionSource=null,actualFallback=false,currentReliable=false,consumptionSelection;
    const consumptionAttempts=[];
    const record=operation=>{try{return operation();}catch{capture=undefined;recordFailed=true;recordFailureReason??='capture_failed';return undefined;}};
    const writeRecording=(method,...args)=>record(()=>{const result=recordingBufferWrite(buffer,method,...args);if(!result.accepted){recordFailed=true;recordFailureReason??=result.reason;}return result.accepted;});
    const flush=()=>{if(!batch.length)return;const events=batch;batch=[];batchBytes=0;if(recordStarted&&!writeRecording('append',requestId,events))recordFailed=true;};
    const finishCapture=response=>{
      const result=record(()=>capture?.finish(response));flush();
      if(result?.coverage&&recordStarted)writeRecording('append',requestId,[{sourceKey:'gateway:coverage',kind:'boundary',provenance:'gateway',content:{section:'coverage',...result.coverage}}]);
      return result;
    };
    const ensureConnected=()=>{if(controller.signal.aborted||res.destroyed)throw controller.signal.reason instanceof AppError?controller.signal.reason:new AppError(499,'客户端已断开','request_cancelled');};
    const sourceHeaders=()=>{
      if(!requestId||res.headersSent||res.destroyed)return;
      res.setHeader('X-Harbor-Request',requestId);res.setHeader('X-Harbor-Request-Id',requestId);
      if(consumption?.schemaAvailable&&actualConsumptionSource){res.setHeader('X-Harbor-Consumption-Source',actualConsumptionSource);res.setHeader('X-Harbor-Fallback',String(actualFallback));}
    };
    const attemptSnapshot=()=>consumptionAttempts.map(attempt=>({...attempt}));
    const noteDispatch=(account,{confirmed=true}={})=>{
      dispatched=true;currentDispatched=true;if(!consumption?.schemaAvailable||attemptIndex>=0)return;
      const source=consumptionSource(account);if(!source)return;
      attemptIndex=consumptionAttempts.length;consumptionAttempts.push({source,account_id:account.id,state:confirmed?'dispatched':'unknown',usage_known:false,started:clock()});
      if(confirmed){actualConsumptionSource=source;actualFallback=consumptionAttempts.length===2;const snapshot=attemptSnapshot();attemptPersistence=attemptPersistence.then(()=>consumptionPolicies.recordAttempts(requestId,member.key_id,snapshot)).catch(()=>{});sourceHeaders();}
    };
    const finishAttempt=(value,{failed=false}={})=>{
      if(attemptIndex<0)return;const attempt=consumptionAttempts[attemptIndex];
      const counts={input:value.input??0,output:value.output??0,cached:value.cached??0},known=value.usageKnown===true&&Object.values(counts).every(number=>Number.isSafeInteger(number)&&number>=0)&&counts.cached<=counts.input;
      Object.assign(attempt,{state:failed?isTrustedApiQuotaRejection(value)?'rejected':known?'failed':'unknown':'completed',usage_known:known,ended:clock()});
      if(known)Object.assign(attempt,counts);else for(const field of ['input','output','cached'])delete attempt[field];
      if(failed&&typeof value.code==='string'&&/^[a-z][a-z0-9_]{0,79}$/.test(value.code))attempt.error_code=value.code;
    };
    const settle=async result=>{
      settlementAttempted=true;await attemptPersistence;if(consumption?.schemaAvailable)result={...result,consumptionAttempts:consumptionSelection?[{...consumptionSelection.first_attempt}]:attemptSnapshot(),...(consumptionSelection?{consumptionSelection}:{})};const at=clock();
      receipt.settlement={requestId,at,result:Object.fromEntries(['status','input','output','cached','usageKnown','actualModel','upstreamRequestId','consumptionAttempts','consumptionSelection','quotaSubjectDenial'].filter(key=>result[key]!==undefined).map(key=>[key,result[key]]))};
      try{await store.finish(requestId,result,at);await extraBoundary?.settle();return true;}
      catch{
        if(!meteringRecovery.settle(requestId,result,at))throw new AppError(503,'用量结算暂存已满，需管理员核对本次请求','metering_unavailable');
        const saved=await meteringRecovery.flush();if(saved.queued)throw new AppError(503,'用量尚未持久化，需管理员核对本次请求','metering_unavailable');
        await extraBoundary?.settle();return false;
      }
    };
    const saveContext=async()=>{
      if(!requestId||contextSaved)return;contextSaved=true;
      // Commit capability facts before the terminal response, so a client
      // immediately starting its next model call does not encounter a new
      // metadata-only tail holding admission/body capacity. A failed write
      // leaves the conservative admission default unknown.
      await store.run('UPDATE harbor.requests SET handoff_context=$1 WHERE id=$2',!contextReplayable?'opaque':contextComplete?'replayable':'unknown',requestId).catch(()=>{});
    };
    try{
      ticket=sessionHandoffs.arrive(member.id,controller.signal);
      output=outputBudget.open('member:'+member.id);stage.emit('lease',{lease:'output',action:'acquire'});
      releasePreparation=await preparation.enter(member.id,controller.signal);ensureConnected();
      if(stopping)throw new AppError(503,'服务正在排空，请稍后重试','service_stopping');
      let original;preparing.set(member.id,(preparing.get(member.id)||0)+1);
      try{original=await requestBodies.read(req,{maxBytes:responseBodyBytes,compressed:true,owner:'member:'+member.id,signal:controller.signal});}
      finally{const count=preparing.get(member.id)-1;if(count)preparing.set(member.id,count);else preparing.delete(member.id);}
      ensureConnected();const payload=jsonPath?validateCodexJson(original,jsonPath):validateResponses(original);
      receipt.timing?.preDispatch('validated');
      contextReplayable=!jsonPath&&replayableResponsesInput(payload);
      // Never wait with a database transaction or routing lock held. Arrival
      // already covers uploads whose client identity only appears in the body.
      const identified=await findPostgresResponseSession(store,member,payload,req.headers,{scope:requestScope});
      if(identified.session)await ticket.bind(identified.session.id);
      receipt.timing?.preDispatch('sessionReady');
      // This request already owned admission/upload capacity before drain.
      // Graceful shutdown lets it complete; forced stop/ownership loss aborts
      // its controller and is still enforced by ensureConnected.
      ensureConnected();
      // A released savepoint is not proof that its outer transaction committed.
      // This receipt changes observation only; routing and settlement stay intact.
      try{receipt.nested=!!store.context?.getStore?.();}catch{receipt.nested=true;}
      let route=await store.transaction(async()=>{
        await lockPostgresRouting(store);
        const current=await store.authenticate(secret,{at:clock(),lock:true}),scope=authenticatedScope(current),device=await deviceAccess.authorize(current,req,{phase:'admission'});await store.checkModel(current,payload.model,payload.tools);
        await checkLimits(current,payload,{at:clock()});
        const route=await (consumptionRouting?consumptionRouting.resolve(current,payload,req.headers,{preparing,protocol:jsonPath?'images':'responses',scope}):resolveRoute(current,payload,req.headers,{preparing,protocol:jsonPath?'images':'responses',scope}));
        if(!route?.session?.id||!route?.account?.id)throw new AppError(503,'会话路由未准备好','session_account_unavailable');
        if(identified.session&&identified.session.id!==route.session.id)throw new AppError(409,'会话关联已变化，请稍后重试','session_route_conflict');
        // Recheck trusted ownership/state, even when an adapter returns stale data.
        const session=await store.get("SELECT * FROM harbor.sessions WHERE id=$1 AND member_id=$2 AND protocol='responses' AND enabled=1 FOR SHARE",route.session.id,current.id);
        const account=session&&await store.get("SELECT * FROM harbor.accounts WHERE id=$1 AND enabled=1 AND removed_at IS NULL AND state IN ('ready','degraded') FOR SHARE",session.account_id);
        if(!account||account.id!==route.account.id)throw new AppError(503,'会话路由已变化或不可用','session_route_conflict');
        if(scope&&(session.department_id!==scope.department_id||account.department_id!==scope.department_id))throw new AppError(403,'会话和账号不属于当前部门','department_forbidden');
        if(session.handoff_requires_replay&&!contextReplayable)throw new AppError(409,'此会话已跨账号转移，当前上下文不支持重放；请提交可重放的完整上下文','session_context_unsupported');
        if(jsonPath&&account.auth_mode==='openai_api')throw new AppError(400,'官方Key接入暂仅支持Responses会话','unsupported_api_route');
        stage.emit('admission-attempt');releaseExecution=execution.acquire({memberId:current.id,accountId:account.id});if(releaseExecution){stage.emit('lease',{lease:'execution',action:'acquire'});stage.emit('admission-granted');}if(!releaseExecution)throw new AppError(503,'账号执行容量已满，请稍后重试','inference_capacity_busy');
        const requestId=await store.observe(current,req.socket.remoteAddress||'unknown',String(req.headers['user-agent']||'').slice(0,200),payload.model,{at:clock(),channel:jsonPath?jsonPath.slice(1):'responses',device,parameters:payload,scope});
        await store.run('UPDATE harbor.requests SET account_id=$1,session_id=$2,route_version=$3,recording_state=$4,handoff_input=$5,recording_reason=$7 WHERE id=$6',account.id,session.id,session.route_version,captureEnabled?'pending':'disabled',contextReplayable?'replayable':'opaque',requestId,captureDecision.reason);
        if(route.consumption?.schemaAvailable)await consumptionPolicies.recordAdmission(requestId,current.key_id,route.consumption);
        await store.run('UPDATE harbor.accounts SET last_used=$1 WHERE id=$2',clock(),account.id);
        const extraAdmission={...await extraRuntime.admit({requestId,account,model:payload.model,protocol:jsonPath?'images':'responses',scope}),fixed:!!identified.fixed};
        await quotaSubjects.admit(requestId,account,{mode:extraAdmission.mode});
        receipt.callbackCompleted=true;return {requestId,account,session,extraAdmission,scope,...(route.consumption?{consumption:route.consumption}:{})};
      });
      receipt.timing?.preDispatch('admissionEnd');
      requestId=route.requestId;consumption=route.consumption;if(consumption?.schemaAvailable)sourceHeaders();if(receipt.nested)receipt.publish('source-unavailable');else receipt.publish('accepted',{accepted:true,recordingExpected:captureEnabled});routedAccount=route.account;accounts.set(routedAccount.id,(accounts.get(routedAccount.id)||0)+1);accountTracked=true;
      const createBoundary=()=>extraRuntime.boundary({requestId,scope:route.scope,protocol:jsonPath?'images':'responses',admittedExtra:route.extraAdmission.extraReserved,fixed:route.extraAdmission.fixed,packagePreference:route.scope?adminRouter.dispatchPreferenceFor(route.scope):adminRouter.dispatchPreference,
        prepare:async request=>{ensureConnected();await lockPostgresRouting(store);const current=await store.authenticate(secret,{at:clock(),lock:true});authenticatedScope(current);if(current.id!==request.member_id||current.key_id!==request.key_id)throw new AppError(403,'请求访问身份已变化','request_identity_changed');await deviceAccess.authorize(current,req,{phase:'dispatch'});await store.checkModel(current,payload.model,payload.tools);await checkLimits(current,payload,{at:clock(),requestId});return ensureConnected;},
        validate:async(account,request)=>{const current=await store.get("SELECT * FROM harbor.sessions WHERE id=$1 FOR SHARE",request.session_id);if(account.id!==route.account.id||account.route_epoch!==route.account.route_epoch||!current||current.id!==route.session.id||current.member_id!==member.id||!current.enabled||current.protocol!=='responses'||current.account_id!==account.id||current.route_version!==request.route_version)throw new AppError(409,'会话或账号身份已变化，请重新请求','session_route_conflict');if(jsonPath&&account.auth_mode==='openai_api')throw new AppError(400,'官方Key接入暂仅支持Responses会话','unsupported_api_route');const selected=await extraRuntime.classify(account,{model:request.model,protocol:jsonPath?'images':'responses',scope:route.scope});await quotaSubjects.beforeDispatch(requestId,{mode:selected.mode});ensureConnected();}
      });
      extraBoundary=createBoundary();
      releasePreparation();releasePreparation=null;
      if(!identified.session)await ticket.bind(route.session.id);
      const current=captureEnabled?record(()=>jsonPath?{state:'known'}:currentInput(payload)):null;
      if(captureEnabled){recordStarted=current?!!writeRecording('start',requestId,current.state):false;if(!recordStarted)recordFailed=true;}
      if(captureEnabled&&recordStarted){
        if(!jsonPath)requestContext=await observeRequestContextAsync(captureContext,requestId,original,{signal:controller.signal});
        capture=record(()=>new ConversationCapture({emit:event=>{
          const bytes=Buffer.byteLength(JSON.stringify(event));if(batch.length>=128||batchBytes+bytes>64*1024)flush();
          batch.push(event);batchBytes+=bytes;return !recordFailed;
        }}));
        // Start must precede the buffered input; no model or raw payload is replayed.
        record(()=>{if(capture){if(jsonPath)captureCodexJsonInput(capture,jsonPath,payload);else capture.input(payload,current);}});flush();flushTimer=setInterval(flush,250);flushTimer.unref();
      }
      receipt.timing?.preDispatch('captureReady');
      const dispatch=async()=>{
        attemptIndex=-1;upstreamEventSeen=false;currentDispatched=false;currentReliable=false;
        const transport=await transportForAccount(route.account);
        ensureConnected();if(jsonPath&&typeof transport.runJson!=='function')throw Object.assign(new AppError(503,'上游JSON通道尚未配置','upstream_configuration'),{usageKnown:true});
        const reliable=isResponsesDispatchSource(transport),outbound={...payload};currentReliable=reliable;
        if(!jsonPath||payload.prompt_cache_key!==undefined)outbound.prompt_cache_key=route.session.upstream_thread||route.session.id;
        return extraBoundary.run(async({beforeDispatch,onDispatch})=>{
          if(!reliable)noteDispatch(route.account,{confirmed:false});
          return (jsonPath?transport.runJson:transport.run).call(transport,{path:jsonPath,binary:codexBinary,home:route.account.auth_mode==='openai_api'?route.account.id:route.account.home,payload:outbound,sessionId:route.session.upstream_thread||route.session.id,signal:controller.signal,protocolHeaders:req.headers,collectOutput:!original.stream,output,timing:receipt.timing,
            beforeDispatch,onDispatch:()=>{receipt.timing?.dispatch();onDispatch();noteDispatch(route.account);},
            onEvent:async event=>{upstreamEventSeen=true;if(consumption?.schemaAvailable){actualConsumptionSource=consumptionSource(route.account);actualFallback=consumptionAttempts.length===2;}sourceHeaders();const contentToken=receipt.timing?.upstreamEvent(event);if(!replayableResponsesEvent(event))contextReplayable=false;if(event.type==='response.completed')contextComplete=true;record(()=>capture?.event(event));if(!jsonPath&&original.stream)await sse(res,event,controller.signal,route.session.id,output,receipt.timing,contentToken);else receipt.timing?.retireContent(contentToken);}});
        },{source:transport});
      };
      let result;
      try{result=await dispatch();}
      catch(error){
        finishAttempt(error,{failed:true});
        const decision=currentReliable&&consumption?.schemaAvailable&&!jsonPath&&consumptionRouting&&subscriptionFallbackDecision({policy:consumption.consumption_policy,account:route.account,error,payload,attempts:consumptionAttempts.length===1?0:1,outputStarted:upstreamEventSeen||res.headersSent||controller.signal.aborted,contextReplayable});
        if(!decision?.allowed)throw error;
        await attemptPersistence;await consumptionPolicies.recordAttempts(requestId,member.key_id,attemptSnapshot());
        await extraBoundary.settle();await extraBoundary.releaseUnused();extraBoundary=undefined;currentDispatched=false;
        const previous=route.account;
        if(releaseExecution){releaseExecution();releaseExecution=null;stage.emit('lease',{lease:'execution',action:'release'});}
        ensureConnected();let nextRelease,preparedRoute,preparedAt;const previousRoute=route;
        try{
          const next=await store.transaction(async()=>{
            await lockPostgresRouting(store);
            const current=await store.authenticate(secret,{at:clock(),lock:true}),scope=authenticatedScope(current);if(current.id!==member.id||current.key_id!==member.key_id)throw new AppError(403,'请求访问身份已变化','request_identity_changed');
            await deviceAccess.authorize(current,req,{phase:'dispatch'});await store.checkModel(current,payload.model,payload.tools);await checkLimits(current,payload,{at:clock(),requestId});ensureConnected();
            const selected=await consumptionRouting.fallback({member:current,payload,headers:req.headers,options:{preparing,protocol:'responses',scope},requestId,route,error,attempts:0,outputStarted:false,contextReplayable});
            const session=await store.get("SELECT * FROM harbor.sessions WHERE id=$1 AND member_id=$2 AND enabled=1 FOR SHARE",selected.session.id,current.id),account=session&&await store.get("SELECT * FROM harbor.accounts WHERE id=$1 AND enabled=1 AND removed_at IS NULL AND state IN ('ready','degraded') FOR SHARE",session.account_id);
            if(!account||account.id!==selected.account.id||session.route_version!==selected.session.route_version||account.id===previous.id||session.route_version!==previousRoute.session.route_version+1||account.auth_mode!=='subscription')throw new AppError(409,'备用会话或账号已变化','session_route_conflict');
            if(scope&&(session.department_id!==scope.department_id||account.department_id!==scope.department_id))throw new AppError(403,'备用会话和账号不属于当前部门','department_forbidden');
            nextRelease=execution.acquire({memberId:current.id,accountId:account.id});if(!nextRelease)throw new AppError(503,'备用账号执行容量已满','inference_capacity_busy');stage.emit('lease',{lease:'execution',action:'acquire'});
            const updated=await store.run("UPDATE harbor.requests SET account_id=$1,session_id=$2,route_version=$3,status='queued',business_status='running' WHERE id=$4 AND key_id=$5 AND account_id=$6 AND route_version=$7 AND status IN ('queued','running') AND ended IS NULL",account.id,session.id,session.route_version,requestId,current.key_id,previous.id,route.session.route_version);if(updated.changes!==1)throw new AppError(409,'备用派发的原请求已变化','consumption_fallback_conflict');
            await store.run('UPDATE harbor.accounts SET last_used=$1 WHERE id=$2',clock(),account.id);
            const extraAdmission={...await extraRuntime.admit({requestId,account,model:payload.model,protocol:'responses',scope}),fixed:false};await quotaSubjects.admit(requestId,account,{mode:extraAdmission.mode,replaceConfirmedUnused:true});ensureConnected();
            preparedAt=clock();preparedRoute={...selected,requestId,account,session,extraAdmission,scope};return preparedRoute;
          });
          route=next;consumption=next.consumption;releaseExecution=nextRelease;nextRelease=null;attemptIndex=-1;
          if(accountTracked){const count=accounts.get(previous.id)-1;if(count)accounts.set(previous.id,count);else accounts.delete(previous.id);}routedAccount=route.account;accounts.set(routedAccount.id,(accounts.get(routedAccount.id)||0)+1);accountTracked=true;
        }catch(fallbackError){
          nextRelease?.();if(nextRelease)stage.emit('lease',{lease:'execution',action:'release'});
          if(preparedRoute){
            consumptionSelection={version:1,request_id:requestId,member_id:member.id,key_id:member.key_id,session_id:previousRoute.session.id,consumption_policy:'api_first',consumption_policy_version:previousRoute.consumption.consumption_policy_version,from:{account_id:previousRoute.account.id,route_version:previousRoute.session.route_version},to:{account_id:preparedRoute.account.id,route_version:preparedRoute.session.route_version},prepared_at:preparedAt,failed_at:clock(),error_code:'consumption_rebind_commit_unconfirmed',first_attempt:{...consumptionAttempts[0]}};
            // A failed COMMIT receipt is not a rollback proof. A fresh read can
            // update local cleanup facts, but settlement always retains the
            // strict resolution envelope for replay if the database stays down.
            try{
              const saved=await store.transaction(()=>store.get('SELECT member_id,key_id,session_id,account_id,route_version,consumption_policy,consumption_policy_version FROM harbor.requests WHERE id=$1',requestId),{level:'REPEATABLE READ',readOnly:true});
              if(saved&&saved.member_id===member.id&&saved.key_id===member.key_id&&saved.session_id===consumptionSelection.session_id&&saved.consumption_policy===consumptionSelection.consumption_policy&&saved.consumption_policy_version===consumptionSelection.consumption_policy_version&&saved.account_id===consumptionSelection.to.account_id&&saved.route_version===consumptionSelection.to.route_version){
                route=preparedRoute;consumption=preparedRoute.consumption;attemptIndex=-1;
                if(accountTracked){const count=accounts.get(previous.id)-1;if(count)accounts.set(previous.id,count);else accounts.delete(previous.id);}routedAccount=route.account;accounts.set(routedAccount.id,(accounts.get(routedAccount.id)||0)+1);accountTracked=true;
              }
            }catch{/* The encrypted settlement resolver owns the unknown branch. */}
            throw new AppError(503,'备用绑定提交结果待核对，本次不再派发模型','consumption_rebind_commit_unconfirmed');
          }
          throw fallbackError;
        }
        finally{if(onRouteFailure)try{await onRouteFailure(previous,error.routeFailure,{scope:requestScope});}catch{}}
        extraBoundary=createBoundary();ensureConnected();result=await dispatch();
      }
      finishAttempt(result);if(consumption?.schemaAvailable){actualConsumptionSource=consumptionSource(route.account);actualFallback=consumptionAttempts.length===2;}sourceHeaders();
      if(!jsonPath){contextComplete=true;if(!replayableResponsesEvent({type:'response.completed',response:result.response}))contextReplayable=false;}
      if(jsonPath)record(()=>{if(capture)captureCodexJsonOutput(capture,jsonPath,result.response);});
      const recordingResult=finishCapture(jsonPath?undefined:result.response);
      if(recordStarted&&!writeRecording('finish',requestId,{state:recordFailed||recordingResult?.failed||recordingResult?.incomplete||incompleteRequestContext(requestContext)?'partial':recordingResult?.truncated?'truncated':'complete',...recordingFinishReason(recordFailed||recordingResult?.failed?recordFailureReason??'capture_failed':incompleteRequestContext(requestContext)?requestContext.reason??'capture_failed':recordingResult?.truncated?'size_limit':recordingResult?.incomplete?'capture_failed':null)}))recordFailed=true;
      // A failure after dispatch must not cause a second model call.
      const settled=await settle(result);
      await deviceAccess.markVerified(requestId).catch(()=>{});
      await saveContext();
      if(!res.destroyed){if(!res.headersSent){res.setHeader('X-Harbor-Request',requestId);if(!settled)res.setHeader('X-Harbor-Usage','pending');}if(!jsonPath&&original.stream){await sse(res,result.terminal,controller.signal,route.session.id,output,receipt.timing,result.timingTerminalObserved?result.timingTerminalToken:receipt.timing?.upstreamEvent(result.terminal));res.end();}else{res.setHeader('X-Harbor-Session',route.session.id);await writeOutput(res,result.response,{output,signal:controller.signal,timing:receipt.timing,contentToken:result.timingTerminalObserved?result.timingTerminalToken:undefined,headers:{'Content-Type':'application/json; charset=utf-8'}});res.end();}}
      return true;
    }catch(error){
      if(consumption?.fallback&&attemptIndex<0&&consumptionAttempts.length===1){attemptIndex=consumptionAttempts.length;consumptionAttempts.push({source:'subscription',account_id:routedAccount.id,state:'failed',dispatched:false,usage_known:true,started:clock(),ended:clock(),input:0,output:0,cached:0,...(typeof error.code==='string'&&/^[a-z][a-z0-9_]{0,79}$/.test(error.code)?{error_code:error.code}:{})});}
      if(attemptIndex>=0&&['dispatched','unknown'].includes(consumptionAttempts[attemptIndex]?.state))finishAttempt(error,{failed:true});sourceHeaders();
      dispatched||=extraBoundary?.status().dispatched===true;
      if(requestId){
        if(onRouteFailure&&routedAccount&&error.routeFailure){try{await onRouteFailure(routedAccount,error.routeFailure,{scope:requestScope});}catch{}}
        finishCapture();
        if(recordStarted&&!writeRecording('finish',requestId,{state:'partial',...recordingFinishReason(recordFailed?recordFailureReason??'capture_failed':controller.signal.reason?.code==='service_shutdown'?'process_interrupted':'upstream_interrupted')}))recordFailed=true;
        if(!settlementAttempted)await settle({...error,quotaSubjectDenial:currentReliable&&error.status===429&&error.code==='upstream_quota_exhausted'&&error.routeFailure==='quota_exhausted',status:controller.signal.reason?.code==='service_shutdown'?'interrupted':controller.signal.aborted?'cancelled':error.status&&error.status<500?'rejected':'failed',usageKnown:consumption?.schemaAvailable?!extraBoundary?.status().uncertain&&(!currentDispatched||error.usageKnown===true):!dispatched||!extraBoundary?.status().uncertain&&error.usageKnown===true}).catch(()=>{});
        await saveContext();
      }
      throw controller.signal.reason?.code==='service_shutdown'?controller.signal.reason:error;
    }finally{
      await extraBoundary?.releaseUnused();
      releasePreparation?.();
      if(flushTimer)clearInterval(flushTimer);
      if(requestId&&!contextSaved)await saveContext();
      if(requestId&&captureEnabled&&(!recordStarted||recordFailed))await store.run("UPDATE harbor.requests SET recording_state='partial',recording_reason=$2 WHERE id=$1 AND recording_state IN ('disabled','pending','complete')",requestId,recordFailureReason??'capture_failed').catch(()=>{});
      if(accountTracked){const n=accounts.get(routedAccount.id)-1;if(n)accounts.set(routedAccount.id,n);else accounts.delete(routedAccount.id);void chat?.wake();}
      if(releaseExecution){releaseExecution();stage.emit('lease',{lease:'execution',action:'release'});}else stage.emit('lease',{lease:'execution',action:'not-acquired'});output?.close();if(output)stage.emit('lease',{lease:'output',action:'release'});res.off('close',close);controller.signal.removeEventListener('abort',observeCancel);active.delete(controller);const count=members.get(member.id)-1;if(count)members.set(member.id,count);else members.delete(member.id);stage.emit('lease',{lease:'member',action:'release'});ticket?.close();
    }
  }
  const handle=async(req,res)=>{
    beginAdminRequestTiming(req,res);
    const path=new URL(req.url,'http://localhost').pathname,modelRequest=req.method==='POST'&&(path==='/v1/responses'||path.startsWith('/v1/')&&codexJsonRoutes.has(path.slice(3))),stage=modelRequest?stages.capture():{emit:()=>false};
    const receipt={callbackCompleted:false,nested:false,published:false,publish(type,details){if(this.published)return false;this.published=true;return stage.emit(type,details);}},body=new AbortController();
    registerAdminRequestTimingSignal(res,body.signal);
    const bodyEnd=()=>{stage.emit('body-end');receipt.timing?.bodyEnd();},closed=()=>{if(!res.writableEnded)body.abort();};
    if(modelRequest){
      receipt.timing=createRequestStreamTiming();let responseEnded=false,routeEnded=false,finalized=false;
      const finalize=()=>{
        if(finalized||!responseEnded||!routeEnded)return;finalized=true;
        res.off('finish',responseEnd);res.off('close',responseEnd);
        const summary=receipt.timing.finish({outcome:receipt.outcome==='interrupted'?'interrupted':receipt.disconnected?'cancelled':receipt.outcome??'unknown'}),fact=receipt.settlement;
        // Reuse the bounded encrypted FIFO, after the actual HTTP completion.
        // No database wait or fsync is added to the downstream write path.
        if(fact){try{if(meteringRecovery.settle(fact.requestId,{...fact.result,streamTiming:summary},fact.at))timingPersistence.queued++;else timingPersistence.rejected++;}catch{timingPersistence.rejected++;}}
      };
      const responseEnd=()=>{if(responseEnded)return;responseEnded=true;receipt.disconnected=!res.writableFinished;receipt.timing.responseEnd();finalize();};
      receipt.routeEnd=()=>{routeEnded=true;finalize();};res.once('finish',responseEnd);res.once('close',responseEnd);req.once('end',bodyEnd);
    }
    res.on('close',closed);
    try{const result=await handleRoute(req,res,body.signal,stage,receipt);receipt.outcome=result?'ok':'unknown';return result;}
    catch(error){receipt.outcome=body.signal.aborted?'cancelled':error?.code==='service_shutdown'?'interrupted':'failed';throw error;}
    finally{
      receipt.routeEnd?.();req.off('end',bodyEnd);res.off('close',closed);
      if(modelRequest&&!receipt.published){if(receipt.callbackCompleted)receipt.publish('source-unavailable');else receipt.publish('accepted',{accepted:false,recordingExpected:false});}
      requestBodies.release(req);
    }
  };
  return {handle,initialOverview:({signal,scope}={})=>readAdminState(stateQuery(new URLSearchParams('view=overview&people=none')),{signal,scope}),stageObserverStatus:()=>({handler:stages.status(),requestBodies:requestBodies.stageObserverStatus?.()??{enabled:false},preparation:preparation.stageObserverStatus(),handoff:sessionHandoffs.stageObserverStatus()}),recordingBackend:recordingJobs,preparation,sessionHandoffs,handoffAdmin,deviceAccess,recoverHandoffs:handoffAdmin.recover,status:()=>({consumptionRouting:{configured:!!consumptionRouting,sourceAccounts:typeof sourceAccounts==='function'},streamTiming:{...timingPersistence},clientCatalog:clientCatalog.status(),recordingAdmin:recordingAdmin?.status()??null,typeStatistics:typeStatisticsAdmin.status(),handoffAdmin:handoffAdmin.status(),sessionHandoffs:sessionHandoffs.status(),outputBuffers:outputBudget.status(),execution:execution.status(),requestBodies:requestBodies.status(),active:active.size,preparing:[...preparing.values()].reduce((sum,n)=>sum+n,0),routingPreparation:preparation.status(),queries:queries.status(),chat:chat?.status()??null,meteringRecovery:meteringRecovery.status(),stopping}),closeRecording:options=>recordingAdmin?.close(options),tickPricing:()=>pricingAdmin.tick(),closeQueries:async()=>{await pricingAdmin.close();return Promise.all([adminJsonCompression.close(),typeStatisticsAdmin.close(),recordingAdmin?.close(),clientCatalog.close(),handoffAdmin.close(),queries.close(),states.close()]);},drain:()=>{stopping=true;void pricingAdmin.close().catch(()=>{});clientCatalog.stop();handoffAdmin.stop();chat?.drain();},abort:()=>{stopping=true;void pricingAdmin.close().catch(()=>{});void recordingAdmin?.close({force:true,timeoutMs:0});clientCatalog.stop();handoffAdmin.stop();chat?.drain();for(const controller of active)controller.abort(new AppError(503,'服务排空超时或实例失去所有权','service_shutdown'));}};
}
