import {createPostgresPricingAdmin} from './postgres-pricing-admin.mjs';
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

const json=(res,status,value)=>{res.writeHead(status,{'Content-Type':'application/json; charset=utf-8'});res.end(JSON.stringify(value));};
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
export function createPostgresResponsesHandler({store,queryStore=store,recordings,recordingReader=recordings,buffer,meteringRecovery,resolveRoute,checkLimits,transportForAccount,adminAuthenticated,onRouteFailure,requestQueries,keyEncryptionKey,adminStateContext,accountAdmin,deviceIssuer,platformTransport,runChat,chatOptions={},cliProcesses,outputBudget=createOutputBudget(),executionOptions={},pricingOptions={},stageObserver,requestBodies=createRequestBodyBudget({stageObserver}),codexBinary='codex',recordingEnabled=true,captureContext,contextReader,contextIndexEnabled=false,recordingPolicy=()=>true,recordingBackend,recordingKey,recordingStoragePolicy,recordingBackendOptions={},recordingAdminOptions={},maxConcurrent=120,maxPerMember=8,maxPendingHandoffs=20000+64,clock=Date.now,extraRuntime=createPostgresExtraRuntime(store,{clock})}={}){
  if(typeof contextIndexEnabled!=='boolean'||captureContext!==undefined&&typeof captureContext!=='function'||contextReader!==undefined&&typeof contextReader?.read!=='function')throw new AppError(503,'上下文采集依赖无效','recording_configuration');
  if(!store||!recordings||!buffer||!meteringRecovery||['settle','flush','status','close'].some(name=>typeof meteringRecovery[name]!=='function')||[resolveRoute,checkLimits,transportForAccount,adminAuthenticated].some(fn=>typeof fn!=='function')||onRouteFailure!==undefined&&typeof onRouteFailure!=='function'||requestQueries!==undefined&&(!requestQueries||['read','close','status'].some(name=>typeof requestQueries[name]!=='function')||!requestQueries.queue||['run','close','status'].some(name=>typeof requestQueries.queue[name]!=='function'))||adminStateContext!==undefined&&typeof adminStateContext!=='function'||typeof recordingEnabled!=='boolean'||![maxConcurrent,maxPerMember].every(n=>Number.isInteger(n)&&n>=1))throw new AppError(503,'PostgreSQL请求处理依赖尚未配置完整','postgres_configuration');
  if(!meteringRecovery.directory||!buffer.directory||realpathSync(meteringRecovery.directory)===realpathSync(buffer.directory))throw new AppError(503,'计量与正文须使用独立恢复目录','metering_configuration');
  const stages=createStreamingStageObserver(stageObserver);
  const timingPersistence={queued:0,rejected:0};
  const active=new Set(),members=new Map(),preparing=new Map(),accounts=new Map();let stopping=false;
  const execution=createInferenceBudget({...executionOptions,maxConcurrent,maxPerMember:Math.min(maxPerMember,maxConcurrent)});
  const preparation=createRoutingPreparation({maxWaiting:maxConcurrent,stageObserver});
  const sessionHandoffs=createSessionRequestHandoff({maxTickets:maxConcurrent,maxPending:maxPendingHandoffs,maxWaiting:Math.max(1,Math.floor(maxConcurrent/2)),maxWaitingPerMember:Math.max(1,Math.floor(maxPerMember/2)),stageObserver});
  const queryQueue=requestQueries?.queue||createPostgresQueryQueue();
  const handoffAdmin=createPostgresSessionHandoffs(store,{gate:sessionHandoffs,readStore:queryStore,queryQueue,clock,isStopping:()=>stopping,validateTarget:async(account,model,signal)=>{if(!model)return true;const transport=await transportForAccount(account);if(typeof transport.models!=='function')return false;const models=await transport.models({binary:codexBinary,home:account.auth_mode==='openai_api'?account.id:account.home,signal});return models.some(item=>item.slug===model);}});
  const queries=requestQueries||createPostgresRequestQueries(queryStore,{clock,queryQueue,businessOutcomes:true,readInputPreviews:typeof recordingReader?.previews==='function'?ids=>recordingReader.previews(ids):undefined});
  const adminRouter=createPostgresRouter(store,{clock,isStopping:()=>stopping,extraRuntime}),readRouter=createPostgresRouter(queryStore,{clock});
  const states=createPostgresStateQueries(queryStore,{clock,encryptionKey:keyEncryptionKey,queryQueue,balancingReader:at=>readRouter.readView({at})});
  const sessionAdmin=createPostgresSessionAdmin(store,{clock,preparation,isPreparing:memberId=>preparing.has(memberId),isStopping:()=>stopping});
  const planning=createPostgresPlanningJobs(store,{jobs:createPostgresJobs(store),clock}),planningReader=createPostgresPlanningJobs(queryStore,{jobs:createPostgresJobs(queryStore),clock});
  const history=createPostgresHistoryQueries(queryStore,{clock});
  const pricingAdmin=createPostgresPricingAdmin(store,{...pricingOptions,clock,adminAuthenticated,readStore:queryStore,queryQueue,readBody:(req,{signal}={})=>requestBodies.read(req,{owner:'admin',maxBytes:65536,signal})});
  const memberAdmin=createPostgresMemberAdmin(store,{clock,readStore:queryStore,encryptionKey:keyEncryptionKey,queryQueue,isBusy:memberId=>members.has(memberId)});
  const deviceAccess=createPostgresDeviceAccess(store,{issuer:deviceIssuer,clock,readStore:queryStore,queryQueue});
  const clientCatalog=createPostgresClientCatalog(store,{deviceAccess,transportForAccount,codexBinary,clock});
  const typeStatisticsAdmin=createPostgresTypeStatisticsAdmin(queryStore,{clock,queryQueue,adminAuthenticated});
  const recordingJobs=recordingBackend??(recordingKey?createPostgresRecordingJobs(store,{...recordingBackendOptions,contextIndex:contextIndexEnabled,contextStorage:!!contextReader,recordingKey,clock,readStore:queryStore,queryQueue,storagePolicy:recordingStoragePolicy,devices:true}):null);
  const recordingAdmin=recordingJobs?createPostgresRecordingAdmin(recordingJobs,{...recordingAdminOptions,adminAuthenticated,requestBodies}):null;
  const selfQuery=createPostgresSelfQuery(queryStore,{clock,queryQueue});
  const chat=runChat?createPostgresChatService({...chatOptions,extraRuntime,execution,cliProcesses,outputBudget,readBody:requestBodies.read,store,buffer,meteringRecovery,checkLimits,runChat,deviceAccess,recordingEnabled,captureContext,recordingPolicy,clock,maxConcurrent,maxPerMember,refreshQuotas:()=>accountAdmin?.refreshQuotas?.()||Promise.resolve(),activity:{active,members,preparing,preparation,accounts,isStopping:()=>stopping}}):null;
  const platform=platformTransport?createPostgresPlatformService({extraRuntime,execution,outputBudget,readBody:requestBodies.read,store,queryStore,queryQueue,buffer,meteringRecovery,checkLimits,transport:platformTransport,credential:(row,signal)=>accountAdmin.platformCredential(row,signal),deviceAccess,recordingEnabled,captureContext,recordingPolicy,clock,maxConcurrent,maxPerMember,activity:{active,members,preparing,preparation,accounts,isStopping:()=>stopping,wake:()=>void chat?.wake()}}):null;
  execution.onRelease(()=>void chat?.wake());cliProcesses?.onRelease(()=>void chat?.wake());
  async function handleRoute(req,res,bodySignal,stage,receipt){
    const url=new URL(req.url,'http://localhost'),path=url.pathname;
    if(recordingAdmin&&await recordingAdmin.handle(req,res))return true;
    if(await typeStatisticsAdmin.handle(req,res))return true;
    if(path.startsWith('/api/device/enroll/')&&req.method==='POST'){
      const payload=await requestBodies.read(req,{owner:'device-enrollment',maxBytes:65536,signal:bodySignal});
      const result=await deviceAccess.publicHandle(path,req.method,payload,req);if(result){json(res,result.status,result.value);return true;}
    }
    if(/^\/api\/admin\/(devices|device-invitations)(?:\/|$)/.test(path)){
      if(!await adminAuthenticated(req))throw new AppError(401,'请先输入管理密码登录','admin_required');
      const payload=['POST','PATCH'].includes(req.method)?await requestBodies.read(req,{owner:'admin',maxBytes:65536,signal:bodySignal}):{};
      const result=await deviceAccess.handle(path,req.method,payload,url.searchParams);if(result){json(res,result.status,result.value);return true;}
    }
    if(path==='/api/me'&&req.method==='GET'){const secret=String(req.headers.authorization||'').replace(/^Bearer /,''),member=await store.authenticate(secret,{at:clock()});await deviceAccess.authorize(member,req);json(res,200,await selfQuery(secret));return true;}
    if(await clientCatalog.handle(req,res))return true;
    if(platform&&['/v1/platform/models','/v1/platform/chat/completions'].includes(path))return platform.handle(req,res);
    if(path==='/v1/chat/completions'&&req.method==='POST'&&chat)return chat.handle(req,res);
    if(/^\/api\/admin\/(sessions|balancing)(?:\/|$)/.test(path)){
      if(!await adminAuthenticated(req))throw new AppError(401,'请先输入管理密码登录','admin_required');
      if(stopping)throw new AppError(503,'会话管理正在关闭','service_stopping');
      const payload=['POST','PATCH'].includes(req.method)?await requestBodies.read(req,{owner:'admin',signal:bodySignal}):{};
      if(!payload||typeof payload!=='object'||Array.isArray(payload))throw new AppError(400,'管理参数须为对象');
      if(stopping)throw new AppError(503,'会话管理正在关闭','service_stopping');
      const handoff=await handoffAdmin.handle(path,req.method,payload);if(handoff){json(res,handoff.status,handoff.value);return true;}
      if(path==='/api/admin/balancing/plans'&&req.method==='POST'){
        if(Object.keys(payload).length)throw new AppError(400,'规划预览无需额外参数');
        const result=await store.transaction(async()=>{if(stopping)throw new AppError(503,'会话管理正在关闭','service_stopping');const result=await planning.enqueue();if(stopping)throw new AppError(503,'会话管理正在关闭','service_stopping');return result;});
        json(res,202,result);return true;
      }
      const planPath=path.match(/^\/api\/admin\/balancing\/plans\/([a-zA-Z0-9-]+)(\/items)?$/);
      if(planPath&&req.method==='GET'){
        let result;
        if(planPath[2]){
          const params=url.searchParams;if([...params.keys()].some(key=>!['section','after','limit'].includes(key))||['after','limit'].some(key=>params.has(key)&&!/^\d+$/.test(params.get(key))))throw new AppError(400,'规划分页参数无效','planning_query_invalid');
          result=await queryQueue.run(()=>planningReader.page(planPath[1],{section:params.get('section')??'moves',after:Number(params.get('after')??0),limit:Number(params.get('limit')??25)}));
        }else result=await queryQueue.run(()=>planningReader.status(planPath[1]));
        json(res,200,result);return true;
      }
      if(path==='/api/admin/balancing/explanation'&&req.method==='GET'){json(res,200,await queryQueue.run(()=>readRouter.readView({explanation:true})));return true;}
      if(path==='/api/admin/balancing'&&req.method==='PATCH'){json(res,200,await adminRouter.saveSettings(payload));return true;}
      if(path==='/api/admin/balancing/schedule'&&req.method==='PATCH'){json(res,200,await adminRouter.saveTransferSchedule(payload));return true;}
      if(path==='/api/admin/balancing/weights'&&req.method==='PATCH'){const result=await adminRouter.saveWeights(payload);await handoffAdmin.reconcileAutomatic();json(res,200,result);return true;}
      const result=await sessionAdmin.handle(path,req.method,payload);if(result){json(res,result.status,result.value);return true;}
    }
    if(accountAdmin&&/^\/api\/admin\/(accounts|account-packages|quotas|platform)(?:\/|$)/.test(path)){
      if(!await adminAuthenticated(req))throw new AppError(401,'请先输入管理密码登录','admin_required');
      const payload=['POST','PATCH'].includes(req.method)?await requestBodies.read(req,{owner:'admin',signal:bodySignal}):{};
      if(!payload||typeof payload!=='object'||Array.isArray(payload))throw new AppError(400,'管理参数须为对象');
      const result=await accountAdmin.handle(path,req.method,payload);if(result){void chat?.wake();json(res,result.status,result.value);return true;}
    }
    if(/^\/api\/admin\/(members|keys|member-types|member-config-batches|member-action-batches)(?:\/|$)/.test(path)){
      if(!await adminAuthenticated(req))throw new AppError(401,'请先输入管理密码登录','admin_required');
      const payload=['POST','PATCH'].includes(req.method)?await requestBodies.read(req,{owner:'admin',signal:bodySignal}):{};
      if(!payload||typeof payload!=='object'||Array.isArray(payload))throw new AppError(400,'管理参数须为对象');
      const result=await deviceAccess.handle(path,req.method,payload,url.searchParams)||await memberAdmin.handle(path,req.method,payload,url.searchParams,req.method==='GET'?{signal:bodySignal}:{});if(result){json(res,result.status,result.value);return true;}
    }
    if((path==='/api/admin/history'||path.startsWith('/api/admin/history/'))&&req.method==='GET'){
      if(!await adminAuthenticated(req))throw new AppError(401,'请先输入管理密码登录','admin_required');
      if(path==='/api/admin/history'){json(res,200,await queryQueue.run(()=>history.list(historyQuery(url.searchParams)),{signal:bodySignal}));return true;}
      const match=path.match(/^\/api\/admin\/history\/nightly\/(\d{4}-\d{2}-\d{2}(?:-\d{4})?)(\/(?:items|revisions))?$/);if(!match)throw new AppError(404,'历史查询入口不存在');
      const params=url.searchParams;
      const allowed=match[2]==='/items'?['section','after','limit','revision']:match[2]==='/revisions'?['after','limit']:['revision'];
      for(const name of params.keys())if(!allowed.includes(name)||params.getAll(name).length!==1)throw new AppError(400,'夜间历史查询参数无效');
      for(const name of ['after','limit','revision'])if(params.has(name)&&!/^\d+$/.test(params.get(name)))throw new AppError(400,'夜间历史分页无效');
      const revision=params.has('revision')?Number(params.get('revision')):undefined,paging={after:Number(params.get('after')??0),limit:Number(params.get('limit')??25)};
      const result=await queryQueue.run(()=>match[2]==='/revisions'?history.revisions(match[1],paging):match[2]?history.page(match[1],{...paging,revision,section:params.get('section')??'moves'}):history.nightly(match[1],{revision}),{signal:bodySignal});json(res,200,result);return true;
    }
    if(await pricingAdmin.handle(req,res,{signal:bodySignal}))return true;
    if(path==='/api/admin/state'&&req.method==='GET'){
      if(!await adminAuthenticated(req))throw new AppError(401,'请先输入管理密码登录','admin_required');
      const value=await states.read(stateQuery(url.searchParams),{signal:bodySignal});
      json(res,200,{...value,...(adminStateContext?await adminStateContext():{}),busy:active.size,queue:chat?.status().queued??0,preparing:[...preparing.values()].reduce((sum,n)=>sum+n,0)});return true;
    }
    if(path==='/api/admin/requests'&&req.method==='GET'){
      if(!await adminAuthenticated(req))throw new AppError(401,'请先输入管理密码登录','admin_required');
      json(res,200,await queries.read(requestsQuery(url.searchParams,{usageTypes:true}),{signal:bodySignal}));return true;
    }
    const contextDetail=path.match(/^\/api\/admin\/requests\/([a-zA-Z0-9-]{1,80})\/context$/);
    if(contextDetail&&req.method==='GET'){
      if(!await adminAuthenticated(req))throw new AppError(401,'请先输入管理密码登录','admin_required');
      for(const key of url.searchParams.keys())if(!['after_component','after_part','limit'].includes(key)||url.searchParams.getAll(key).length!==1)throw new AppError(400,'上下文分页无效');
      const numeric=(name,fallback)=>url.searchParams.has(name)&&/^\d+$/.test(url.searchParams.get(name))?Number(url.searchParams.get(name)):url.searchParams.has(name)?NaN:fallback;
      const options={after:{component:numeric('after_component',0),part:numeric('after_part',0)},limit:numeric('limit',32)};
      if(!Object.values(options.after).every(value=>Number.isSafeInteger(value)&&value>=0)||!Number.isInteger(options.limit)||options.limit<1||options.limit>100)throw new AppError(400,'上下文分页无效');
      json(res,200,await queryQueue.run(async()=>{
        if(contextReader)return contextReader.read(contextDetail[1],options);
        if(!await queryStore.get('SELECT id FROM harbor.requests WHERE id=$1',contextDetail[1]))throw new AppError(404,'请求不存在');
        return {state:'unavailable',parts:[],next:{component:0,part:0},hasMore:false};
      }));return true;
    }
    const detail=path.match(/^\/api\/admin\/requests\/([a-zA-Z0-9-]{1,80})(\/recording)?$/);
    if(detail&&req.method==='GET'){
      if(!await adminAuthenticated(req))throw new AppError(401,'请先输入管理密码登录','admin_required');
      if(!detail[2]){json(res,200,await queryQueue.run(()=>postgresRequestObservation(queryStore,detail[1],clock())));return true;}
      for(const key of url.searchParams.keys())if(!['after','limit'].includes(key)||url.searchParams.getAll(key).length!==1)throw new AppError(400,'正文分页无效');
      const numeric=(name,fallback)=>url.searchParams.has(name)&&/^\d+$/.test(url.searchParams.get(name))?Number(url.searchParams.get(name)):url.searchParams.has(name)?NaN:fallback;
      json(res,200,await queryQueue.run(()=>recordingReader.read(detail[1],{after:numeric('after',0),limit:numeric('limit',32)})));return true;
    }
    const jsonPath=path.startsWith('/v1/')&&codexJsonRoutes.has(path.slice(3))?path.slice(3):null;
    if((path!=='/v1/responses'&&!jsonPath)||req.method!=='POST')return false;
    const secret=String(req.headers.authorization||'').replace(/^Bearer /,''),member=await store.authenticate(secret,{at:clock()});
    await deviceAccess.authorize(member,req);
    const captureDecision=await recordingCaptureDecision(recordingEnabled,recordingPolicy),captureEnabled=captureDecision.enabled;
    if(stopping)throw new AppError(503,'服务正在排空，请稍后重试','service_stopping');
    if(active.size>=maxConcurrent||(members.get(member.id)||0)>=maxPerMember)throw new AppError(429,'请求并发已满，请稍后重试','gateway_busy');
    const controller=new AbortController();active.add(controller);members.set(member.id,(members.get(member.id)||0)+1);stage.emit('lease',{lease:'member',action:'acquire'});const observeCancel=()=>stage.emit('cancel');controller.signal.addEventListener('abort',observeCancel,{once:true});
    const close=()=>{if(!res.writableEnded)controller.abort();};res.on('close',close);
    let output,ticket,releasePreparation,releaseExecution,requestId,routedAccount,contextReplayable=false,contextComplete=false,contextSaved=false,accountTracked=false,capture,requestContext,batch=[],batchBytes=0,flushTimer,recordFailed=false,recordFailureReason=null,recordStarted=false,settlementAttempted=false,dispatched=false,extraBoundary;
    const record=operation=>{try{return operation();}catch{capture=undefined;recordFailed=true;recordFailureReason??='capture_failed';return undefined;}};
    const writeRecording=(method,...args)=>record(()=>{const result=recordingBufferWrite(buffer,method,...args);if(!result.accepted){recordFailed=true;recordFailureReason??=result.reason;}return result.accepted;});
    const flush=()=>{if(!batch.length)return;const events=batch;batch=[];batchBytes=0;if(recordStarted&&!writeRecording('append',requestId,events))recordFailed=true;};
    const finishCapture=response=>{
      const result=record(()=>capture?.finish(response));flush();
      if(result?.coverage&&recordStarted)writeRecording('append',requestId,[{sourceKey:'gateway:coverage',kind:'boundary',provenance:'gateway',content:{section:'coverage',...result.coverage}}]);
      return result;
    };
    const ensureConnected=()=>{if(controller.signal.aborted||res.destroyed)throw controller.signal.reason instanceof AppError?controller.signal.reason:new AppError(499,'客户端已断开','request_cancelled');};
    const settle=async result=>{
      settlementAttempted=true;const at=clock();
      receipt.settlement={requestId,at,result:Object.fromEntries(['status','input','output','cached','usageKnown','actualModel','upstreamRequestId'].filter(key=>result[key]!==undefined).map(key=>[key,result[key]]))};
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
      const identified=await findPostgresResponseSession(store,member,payload,req.headers);
      if(identified.session)await ticket.bind(identified.session.id);
      receipt.timing?.preDispatch('sessionReady');
      // This request already owned admission/upload capacity before drain.
      // Graceful shutdown lets it complete; forced stop/ownership loss aborts
      // its controller and is still enforced by ensureConnected.
      ensureConnected();
      // A released savepoint is not proof that its outer transaction committed.
      // This receipt changes observation only; routing and settlement stay intact.
      try{receipt.nested=!!store.context?.getStore?.();}catch{receipt.nested=true;}
      const route=await store.transaction(async()=>{
        const current=await store.authenticate(secret,{at:clock(),lock:true}),device=await deviceAccess.authorize(current,req,{phase:'admission'});await store.checkModel(current,payload.model,payload.tools);
        await checkLimits(current,payload,{at:clock()});
        const route=await resolveRoute(current,payload,req.headers,{preparing,protocol:jsonPath?'images':'responses'});
        if(!route?.session?.id||!route?.account?.id)throw new AppError(503,'会话路由未准备好','session_account_unavailable');
        if(identified.session&&identified.session.id!==route.session.id)throw new AppError(409,'会话关联已变化，请稍后重试','session_route_conflict');
        // Recheck trusted ownership/state, even when an adapter returns stale data.
        const session=await store.get("SELECT * FROM harbor.sessions WHERE id=$1 AND member_id=$2 AND protocol='responses' AND enabled=1 FOR SHARE",route.session.id,current.id);
        const account=session&&await store.get("SELECT * FROM harbor.accounts WHERE id=$1 AND enabled=1 AND removed_at IS NULL AND state IN ('ready','degraded') FOR SHARE",session.account_id);
        if(!account||account.id!==route.account.id)throw new AppError(503,'会话路由已变化或不可用','session_route_conflict');
        if(session.handoff_requires_replay&&!contextReplayable)throw new AppError(409,'此会话已跨账号转移，当前上下文不支持重放；请提交可重放的完整上下文','session_context_unsupported');
        if(jsonPath&&account.auth_mode==='openai_api')throw new AppError(400,'官方Key接入暂仅支持Responses会话','unsupported_api_route');
        stage.emit('admission-attempt');releaseExecution=execution.acquire({memberId:current.id,accountId:account.id});if(releaseExecution){stage.emit('lease',{lease:'execution',action:'acquire'});stage.emit('admission-granted');}if(!releaseExecution)throw new AppError(503,'账号执行容量已满，请稍后重试','inference_capacity_busy');
        const requestId=await store.observe(current,req.socket.remoteAddress||'unknown',String(req.headers['user-agent']||'').slice(0,200),payload.model,{at:clock(),channel:jsonPath?jsonPath.slice(1):'responses',device,parameters:payload});
        await store.run('UPDATE harbor.requests SET account_id=$1,session_id=$2,route_version=$3,recording_state=$4,handoff_input=$5,recording_reason=$7 WHERE id=$6',account.id,session.id,session.route_version,captureEnabled?'pending':'disabled',contextReplayable?'replayable':'opaque',requestId,captureDecision.reason);
        await store.run('UPDATE harbor.accounts SET last_used=$1 WHERE id=$2',clock(),account.id);
        const extraAdmission={...await extraRuntime.admit({requestId,account,model:payload.model,protocol:jsonPath?'images':'responses'}),fixed:!!identified.fixed};
        receipt.callbackCompleted=true;return {requestId,account,session,extraAdmission};
      });
      receipt.timing?.preDispatch('admissionEnd');
      requestId=route.requestId;if(receipt.nested)receipt.publish('source-unavailable');else receipt.publish('accepted',{accepted:true,recordingExpected:captureEnabled});routedAccount=route.account;accounts.set(routedAccount.id,(accounts.get(routedAccount.id)||0)+1);accountTracked=true;
      extraBoundary=extraRuntime.boundary({requestId,protocol:jsonPath?'images':'responses',admittedExtra:route.extraAdmission.extraReserved,fixed:route.extraAdmission.fixed,packagePreference:adminRouter.dispatchPreference,
        prepare:async request=>{ensureConnected();const current=await store.authenticate(secret,{at:clock(),lock:true});if(current.id!==request.member_id||current.key_id!==request.key_id)throw new AppError(403,'请求访问身份已变化','request_identity_changed');await deviceAccess.authorize(current,req,{phase:'dispatch'});await store.checkModel(current,payload.model,payload.tools);await checkLimits(current,payload,{at:clock(),requestId});await lockPostgresRouting(store);return ensureConnected;},
        validate:async(account,request)=>{const current=await store.get("SELECT * FROM harbor.sessions WHERE id=$1 FOR SHARE",request.session_id);if(account.id!==route.account.id||account.route_epoch!==route.account.route_epoch||!current||current.id!==route.session.id||current.member_id!==member.id||!current.enabled||current.protocol!=='responses'||current.account_id!==account.id||current.route_version!==request.route_version)throw new AppError(409,'会话或账号身份已变化，请重新请求','session_route_conflict');if(jsonPath&&account.auth_mode==='openai_api')throw new AppError(400,'官方Key接入暂仅支持Responses会话','unsupported_api_route');ensureConnected();}
      });
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
      if(!jsonPath||payload.prompt_cache_key!==undefined)payload.prompt_cache_key=route.session.upstream_thread||route.session.id;
      const transport=await transportForAccount(route.account);
      ensureConnected();if(jsonPath&&typeof transport.runJson!=='function')throw Object.assign(new AppError(503,'上游JSON通道尚未配置','upstream_configuration'),{usageKnown:true});
      const reliable=isResponsesDispatchSource(transport);
      const result=await extraBoundary.run(async({beforeDispatch,onDispatch})=>{
        if(!reliable)dispatched=true;
        return (jsonPath?transport.runJson:transport.run).call(transport,{path:jsonPath,binary:codexBinary,home:route.account.auth_mode==='openai_api'?route.account.id:route.account.home,payload,sessionId:route.session.upstream_thread||route.session.id,signal:controller.signal,protocolHeaders:req.headers,collectOutput:!original.stream,output,timing:receipt.timing,
          beforeDispatch,onDispatch:()=>{receipt.timing?.dispatch();onDispatch();dispatched=true;},
          onEvent:async event=>{const contentToken=receipt.timing?.upstreamEvent(event);if(!replayableResponsesEvent(event))contextReplayable=false;if(event.type==='response.completed')contextComplete=true;record(()=>capture?.event(event));if(!jsonPath&&original.stream)await sse(res,event,controller.signal,route.session.id,output,receipt.timing,contentToken);else receipt.timing?.retireContent(contentToken);}});
      },{source:transport});
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
      dispatched||=extraBoundary?.status().dispatched===true;
      if(requestId){
        if(onRouteFailure&&routedAccount&&error.routeFailure){try{await onRouteFailure(routedAccount,error.routeFailure);}catch{}}
        finishCapture();
        if(recordStarted&&!writeRecording('finish',requestId,{state:'partial',...recordingFinishReason(recordFailed?recordFailureReason??'capture_failed':controller.signal.reason?.code==='service_shutdown'?'process_interrupted':'upstream_interrupted')}))recordFailed=true;
        if(!settlementAttempted)await settle({...error,status:controller.signal.reason?.code==='service_shutdown'?'interrupted':controller.signal.aborted?'cancelled':error.status&&error.status<500?'rejected':'failed',usageKnown:!dispatched||!extraBoundary?.status().uncertain&&error.usageKnown===true}).catch(()=>{});
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
    const path=new URL(req.url,'http://localhost').pathname,modelRequest=req.method==='POST'&&(path==='/v1/responses'||path.startsWith('/v1/')&&codexJsonRoutes.has(path.slice(3))),stage=modelRequest?stages.capture():{emit:()=>false};
    const receipt={callbackCompleted:false,nested:false,published:false,publish(type,details){if(this.published)return false;this.published=true;return stage.emit(type,details);}},body=new AbortController();
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
  return {handle,stageObserverStatus:()=>({handler:stages.status(),requestBodies:requestBodies.stageObserverStatus?.()??{enabled:false},preparation:preparation.stageObserverStatus(),handoff:sessionHandoffs.stageObserverStatus()}),recordingBackend:recordingJobs,preparation,sessionHandoffs,handoffAdmin,deviceAccess,recoverHandoffs:handoffAdmin.recover,status:()=>({streamTiming:{...timingPersistence},clientCatalog:clientCatalog.status(),recordingAdmin:recordingAdmin?.status()??null,typeStatistics:typeStatisticsAdmin.status(),handoffAdmin:handoffAdmin.status(),sessionHandoffs:sessionHandoffs.status(),outputBuffers:outputBudget.status(),execution:execution.status(),requestBodies:requestBodies.status(),active:active.size,preparing:[...preparing.values()].reduce((sum,n)=>sum+n,0),routingPreparation:preparation.status(),queries:queries.status(),chat:chat?.status()??null,meteringRecovery:meteringRecovery.status(),stopping}),closeRecording:options=>recordingAdmin?.close(options),tickPricing:()=>pricingAdmin.tick(),closeQueries:async()=>{await pricingAdmin.close();return Promise.all([typeStatisticsAdmin.close(),recordingAdmin?.close(),clientCatalog.close(),handoffAdmin.close(),queries.close(),states.close()]);},drain:()=>{stopping=true;void pricingAdmin.close().catch(()=>{});clientCatalog.stop();handoffAdmin.stop();chat?.drain();},abort:()=>{stopping=true;void pricingAdmin.close().catch(()=>{});void recordingAdmin?.close({force:true,timeoutMs:0});clientCatalog.stop();handoffAdmin.stop();chat?.drain();for(const controller of active)controller.abort(new AppError(503,'服务排空超时或实例失去所有权','service_shutdown'));}};
}
