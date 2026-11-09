import {postgresShutdownConfiguration,installPostgresGatewayShutdown} from './postgres-shutdown.mjs';
import {createPostgresExtraRuntime} from './postgres-extra-runtime.mjs';
import {RecordingContextStore} from './recording-context-store.mjs';
import {createRequestContextCapture} from './recording-context-capture.mjs';
import {createPostgresExtraUsageAdmin} from './postgres-extra-usage-admin.mjs';
import {createPostgresExtraUsageBatches} from './postgres-extra-usage-batches.mjs';
import {createPostgresAccountExtraUsage} from './postgres-account-extra-usage.mjs';
import {loadPostgresRecordingKey,createRecordingProbeStore,createRecordingStorageMonitor,recordingStorageConfiguration,recordingPolicyBuffer} from './postgres-recording-storage.mjs';
import http from 'node:http';
import {createDeviceTlsServer} from './device-transport.mjs';
import {createDeviceCertificateAuthority} from './device-certificates.mjs';
import {createHash,randomBytes} from 'node:crypto';
import {readFile,writeFile,mkdir,lstat,realpath,rename} from 'node:fs/promises';
import {resolve,join,dirname,isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';
import {monitorEventLoopDelay} from 'node:perf_hooks';
import {PostgresStore} from './postgres-store.mjs';
import {AppError,revealStoredKey} from './store.mjs';
import {createAdminAuth,passwordHash,adminCookieName} from './auth.mjs';
import {networkConfig,acceptsRequest} from './network.mjs';
import {acquireInstance} from './instance.mjs';
import {createRequestBodyBudget} from './request-body-budget.mjs';
import {createJsonParser} from './json-parser.mjs';
import {createCliProcessBudget} from './cli-process-budget.mjs';
import {createOutputBudget} from './output-budget.mjs';
import {createPostgresAccountAdmin} from './postgres-account-admin.mjs';
import {checkLogin,runCodex} from './codex.mjs';
import {checkClaudeLogin,claudeEnv,runClaude} from './claude.mjs';
import {readRateLimits} from './rates.mjs';
import {startCodexLogin,startLocalLogin} from './account-login.mjs';
import {createPostgresRouter} from './postgres-routing.mjs';
import {createPostgresMemberLimits} from './postgres-member-limits.mjs';
import {RecordingStore} from './recording-store.mjs';
import {RecordingBuffer} from './recording-buffer.mjs';
import {MeteringRecovery} from './metering-recovery.mjs';
import {createPostgresResponsesHandler} from './postgres-responses.mjs';
import {createResponsesTransport,accountCredentials,refreshCredentials,installedVersion} from './responses.mjs';
import {sanitizeRecord} from './record-redaction.mjs';
import {createPostgresNightly} from './postgres-nightly.mjs';
import {createPlatformTransport} from './platforms.mjs';

const root=dirname(fileURLToPath(import.meta.url));
const json=(res,status,value)=>{res.writeHead(status,{'content-type':'application/json; charset=utf-8'});res.end(JSON.stringify(value));};
const missing=async path=>{try{await lstat(path);return false;}catch(e){if(e.code==='ENOENT')return true;throw e;}};
async function privateFile(path){
  const info=await lstat(path);
  if(info.size>1024*1024||!info.isFile()||info.isSymbolicLink()||process.platform!=='win32'&&(info.mode&0o077||info.uid!==process.getuid()))throw new AppError(503,'服务配置或加密文件权限不安全','gateway_configuration');
  return readFile(path);
}
async function material(path,required){
  if(await missing(path)){
    if(required)throw new AppError(503,'已有加密数据的密钥缺失，禁止生成替代材料','encryption_material_missing');
    await writeFile(path,randomBytes(32),{mode:0o600,flag:'wx',flush:true});
  }
  const value=await privateFile(path);if(value.length!==32)throw new AppError(503,'服务加密材料格式无效','gateway_configuration');return value;
}
async function deviceConfiguration(env){
  if(env.HARBOR_DEVICE_TLS_ENABLED===undefined||env.HARBOR_DEVICE_TLS_ENABLED==='0')return null;
  if(env.HARBOR_DEVICE_TLS_ENABLED!=='1')throw new AppError(503,'设备TLS开关须为0或1','device_tls_configuration');
  const port=Number(env.HARBOR_DEVICE_TLS_PORT),paths=['HARBOR_DEVICE_TLS_KEY_FILE','HARBOR_DEVICE_TLS_CERT_FILE','HARBOR_DEVICE_CA_CERT_FILE'];
  if(!Number.isInteger(port)||port<1||port>65535||paths.some(name=>typeof env[name]!=='string'||!env[name]||!isAbsolute(env[name])))throw new AppError(503,'设备TLS须配置独立端口和受控证书绝对路径','device_tls_configuration');
  const publicFile=async path=>{const info=await lstat(path);if(!info.isFile()||info.isSymbolicLink()||info.size>1048576||process.platform!=='win32'&&(info.mode&0o022))throw new AppError(503,'设备证书文件权限不安全','device_tls_configuration');return readFile(path);};
  const tls={port,key:await privateFile(env.HARBOR_DEVICE_TLS_KEY_FILE),cert:await publicFile(env.HARBOR_DEVICE_TLS_CERT_FILE),ca:await publicFile(env.HARBOR_DEVICE_CA_CERT_FILE)};
  let issuer;if(env.HARBOR_DEVICE_CA_KEY_FILE){if(!isAbsolute(env.HARBOR_DEVICE_CA_KEY_FILE))throw new AppError(503,'设备CA私钥须为受控绝对路径','device_ca_configuration');await privateFile(env.HARBOR_DEVICE_CA_KEY_FILE);issuer=createDeviceCertificateAuthority({certificateFile:env.HARBOR_DEVICE_CA_CERT_FILE,keyFile:env.HARBOR_DEVICE_CA_KEY_FILE});}
  return {tls,issuer};
}
const validConfig=config=>config&&Number.isInteger(config.port)&&config.port>=1&&config.port<=65535&&typeof config.codexBinary==='string'&&config.codexBinary.length>0&&/^[a-f0-9]{32}$/i.test(config.adminPasswordHash?.salt)&&/^[a-f0-9]{64}$/i.test(config.adminPasswordHash?.digest);
async function initialConfig(data,configPath){
  const passwordPath=join(data,'管理密码.txt'),pending=configPath+'.pending';let password;
  if(await missing(passwordPath)){
    if(!await missing(pending))throw new AppError(503,'待完成启动配置缺少原密码副本，禁止生成替代密码','gateway_configuration');
    password='Hbr!'+randomBytes(24).toString('base64url');await writeFile(passwordPath,'Harbor 管理密码（请勿分享）\r\n\r\n'+password+'\r\n',{mode:0o600,flag:'wx',flush:true});
  }else{
    password=(await privateFile(passwordPath)).toString('utf8').match(/^Harbor 管理密码（请勿分享）\r?\n\r?\n([^\r\n]{8,256})\r?\n/)?.[1];
    if(!password)throw new AppError(503,'首次配置未完成且密码副本无效，请核对原文件','gateway_configuration');
  }
  let config;
  if(!await missing(pending)){
    config=JSON.parse((await privateFile(pending)).toString('utf8'));adminCookieName(config);
    if(!validConfig(config)||passwordHash(password,config.adminPasswordHash.salt).digest!==config.adminPasswordHash.digest)throw new AppError(503,'待完成启动配置与密码副本不一致','gateway_configuration');
  }else{
    config={port:43127,codexBinary:'codex',adminPasswordHash:passwordHash(password)};
    await writeFile(pending,JSON.stringify(config,null,2),{mode:0o600,flag:'wx',flush:true});
  }
  await rename(pending,configPath);return config;
}

const gatewayLock=createHash('sha256').update('harbor-active-postgres-gateway').digest().readInt32BE();

// Development application entry; SQLite is never opened here. The ordinary
// server entry stays unchanged until all protocols/management and migration
// acceptance are complete, so merely setting a URL cannot switch live storage.
export async function createPostgresGateway(options={}){
  const env=options.env||process.env,network=networkConfig(env),database=options.database||{connectionString:env.HARBOR_DATABASE_URL};
  const clock=options.clock||Date.now,data=resolve(options.dataDir||env.HARBOR_DATA_DIR||join(root,'data')),runtime=resolve(options.runtimeDir||env.HARBOR_RUNTIME_DIR||join(root,'runtime'));
  const resources={corePoolSize:12,queryPoolSize:3,recordingPoolSize:2,...options.resources};
  const cliProcesses=createCliProcessBudget(options.cliProcesses,{spawnProcess:options.cliSpawner});
  const spawnFor=(accountId,context)=>(binary,args,spawnOptions)=>cliProcesses.spawn(accountId,binary,args,spawnOptions,context);
  const jsonParser=createJsonParser(options.jsonParsing,{workerFactory:options.jsonParserWorker});
  const outputBudget=createOutputBudget(options.outputBuffers);
  const requestBodies=createRequestBodyBudget({...options.requestBodies,parse:jsonParser.parse,stageObserver:options.stageObserver});
  const drainTimeoutMs=options.drainTimeoutMs??300000;
  if(!Number.isInteger(drainTimeoutMs)||drainTimeoutMs<100||drainTimeoutMs>3600000)throw new AppError(503,'排空等待配置无效','gateway_configuration');
  let workspaceLock,guardStore,guardClient,store,queryStore,recordingStore,buffer,meteringRecovery,handler,accountAdmin,extraUsageAdmin,nightly,server,deviceTls,deviceSettings,ownedDeviceIssuer,config,closing=false,releasingOwnership=false,lostOwnership=false,closePromise,quotaTimer,nightlyTimer,recordingTimer,pricingTimer,recordingMonitor,recordingProbeStore;
  const inflight=new Set(),loop=monitorEventLoopDelay({resolution:20}),ownership=new AbortController();
  const requireOwnership=()=>{if(lostOwnership)throw new AppError(503,'数据库单实例锁已丢失','gateway_ownership_lost');};
  const loseOwnership=()=>{
    if(releasingOwnership||lostOwnership)return;
    lostOwnership=true;clearInterval(pricingTimer);clearInterval(quotaTimer);clearInterval(nightlyTimer);clearInterval(recordingTimer);void recordingMonitor?.close({timeoutMs:0});handler?.abort();accountAdmin?.stop();cliProcesses.stop();
    ownership.abort();
  };
  const cleanup=async()=>{
    clearInterval(pricingTimer);clearInterval(quotaTimer);clearInterval(recordingTimer);
    await deviceTls?.close({force:true});
    ownedDeviceIssuer?.close();
    clearInterval(nightlyTimer);await nightly?.close();
    const recordingStorage=await recordingMonitor?.close();await recordingProbeStore?.close();
    await extraUsageAdmin?.close();await accountAdmin?.close();await cliProcesses.close();await jsonParser.close();
    const metering=meteringRecovery?await meteringRecovery.close():null,recording=buffer?await buffer.close():null;
    await Promise.allSettled([store?.close(),queryStore?.close(),recordingStore?.close()]);
    releasingOwnership=true;
    if(guardClient){try{await guardClient.query('SELECT pg_advisory_unlock($1)',[gatewayLock]);}catch{}guardClient.release();guardClient=null;}
    await Promise.allSettled([guardStore?.close(),workspaceLock?.release()]);loop.disable();
    return {metering,recording,recordingStorage};
  };
  try{
    await mkdir(data,{recursive:true,mode:0o700});workspaceLock=await acquireInstance(await realpath(data));
    if(!await missing(join(data,'gateway.sqlite')))throw new AppError(503,'该目录仍有SQLite来源数据，请先完成正式迁移核验','database_migration_required');
    await mkdir(runtime,{recursive:true,mode:0o700});
    guardStore=new PostgresStore({...database,poolSize:1});guardClient=await guardStore.pool.connect();
    guardClient.on('error',loseOwnership);
    guardClient.on('end',loseOwnership);
    if(!(await guardClient.query('SELECT pg_try_advisory_lock($1) acquired',[gatewayLock])).rows[0].acquired)throw new AppError(503,'此数据库已有活动网关，暂不支持多实例','gateway_already_running');
    store=new PostgresStore({...database,poolSize:resources.corePoolSize});
    queryStore=new PostgresStore({...database,poolSize:resources.queryPoolSize});recordingStore=new PostgresStore({...database,poolSize:resources.recordingPoolSize});
    try{await store.verifySchema();}catch(e){if(e.code==='42P01')throw new AppError(503,'数据库尚未迁移，请先运行db:migrate','database_migration_required');throw e;}
    if(env.HARBOR_RECORDING_CONTEXT_ENABLED!==undefined&&!['0','1'].includes(env.HARBOR_RECORDING_CONTEXT_ENABLED))throw new AppError(503,'上下文采集开关须为0或1','recording_configuration');
    if(options.recording?.contextEnabled!==undefined&&typeof options.recording.contextEnabled!=='boolean')throw new AppError(503,'上下文采集开关须为布尔值','recording_configuration');
    const contextEnabled=options.recording?.contextEnabled??env.HARBOR_RECORDING_CONTEXT_ENABLED==='1',contextAvailable=(await store.get('SELECT MAX(version) version FROM harbor.schema_migrations')).version===23;
    if(typeof contextEnabled!=='boolean')throw new AppError(503,'上下文采集开关须为布尔值','recording_configuration');
    if(contextEnabled&&!contextAvailable)throw new AppError(503,'上下文结构尚未迁移','database_migration_required');
    await store.initializePricing();
    const configPath=join(data,'settings.json');
    config=options.config;
    if(!config){
      if(!await missing(configPath))config=JSON.parse((await privateFile(configPath)).toString('utf8'));
      else config=await initialConfig(data,configPath);
    }
    adminCookieName(config);
    if(!validConfig(config))throw new AppError(503,'服务启动配置无效','gateway_configuration');
    deviceSettings=options.devices??await deviceConfiguration(env);if(options.devices===undefined)ownedDeviceIssuer=deviceSettings?.issuer;
    const encrypted=(await store.get("SELECT EXISTS(SELECT 1 FROM harbor.keys WHERE secret_cipher IS NOT NULL) OR EXISTS(SELECT 1 FROM harbor.accounts WHERE api_secret_cipher IS NOT NULL) required")).required;
    const key=await material(join(data,'key-encryption.key'),encrypted||!await missing(join(data,'metering-spool')));
    const recordKey=await loadPostgresRecordingKey(store,{dataDir:data,createIfEmpty:true,spoolLimits:options.recordingBuffer});
    // Even with new capture disabled, a schema-23 bridge must replay existing
    // context commands and allow reading already persisted snapshots.
    const contexts=contextAvailable?new RecordingContextStore(recordingStore,{encryptionKey:recordKey,clock}):undefined,contextReader=contextAvailable?new RecordingContextStore(queryStore,{encryptionKey:recordKey,clock}):undefined;
    const recordings=new RecordingStore(recordingStore,{...options.recording,encryptionKey:recordKey,clock,contextRepository:contexts}),recordingReader=new RecordingStore(queryStore,{...options.recording,encryptionKey:recordKey,clock});
    buffer=await RecordingBuffer.create(recordings,{...options.recordingBuffer,directory:join(data,'recording-spool'),atomicCommands:commands=>recordings.writeAtomicCommands(commands),atomicBatch:operation=>recordingStore.transaction(operation)});
    recordingProbeStore=createRecordingProbeStore(database);
    recordingMonitor=createRecordingStorageMonitor(recordingProbeStore,{...recordingStorageConfiguration(options.recording?.storage,env),appPaths:[data,join(data,'recording-spool')],clock});await recordingMonitor.refresh();
    if(env.HARBOR_RECORDING_ENABLED!==undefined&&!['0','1'].includes(env.HARBOR_RECORDING_ENABLED))throw new AppError(503,'正文采集开关须为0或1','recording_configuration');
    const recordingEnabled=options.recording?.enabled??env.HARBOR_RECORDING_ENABLED!=='0',captureBuffer=recordingPolicyBuffer(buffer,recordingMonitor);
    const captureContext=contextEnabled?createRequestContextCapture(buffer,{policyBuffer:captureBuffer}):undefined;
    meteringRecovery=await MeteringRecovery.create(store,{...options.meteringBuffer,encryptionKey:key,directory:join(data,'metering-spool')});
    // Recovery order matters: persisted successful billing must win before stale
    // running rows are marked interrupted. A corrupt critical spool fails closed.
    requireOwnership();const startupVisibility=(await guardClient.query('SELECT pg_current_snapshot()::text visibility')).rows[0].visibility;requireOwnership();
    const recovered=await meteringRecovery.flush();requireOwnership();
    if(recovered.queued||recovered.files)throw new AppError(503,'关键计量暂存尚未恢复，禁止接纳新请求','metering_recovery_required');
    await buffer.flush();let interrupted=0;const recoveredAt=clock();
    for(;;){requireOwnership();const rows=await store.all("SELECT id FROM harbor.requests WHERE ended IS NULL AND status IN ('queued','running') AND pg_visible_in_snapshot(inserted_xid,$1::pg_snapshot) ORDER BY started,id LIMIT 100",startupVisibility);if(!rows.length)break;
      for(const row of rows){requireOwnership();await store.finish(row.id,{status:'interrupted',usageKnown:false},recoveredAt);interrupted++;}
    }
    for(;;){requireOwnership();const rows=await store.all("SELECT id FROM harbor.requests WHERE recording_state='pending' AND pg_visible_in_snapshot(inserted_xid,$1::pg_snapshot) ORDER BY id LIMIT 1000",startupVisibility);if(!rows.length)break;
      await store.run("UPDATE harbor.requests SET recording_state='partial',recording_reason=COALESCE(recording_reason,'process_interrupted') WHERE id=ANY($1::text[]) AND recording_state='pending'",rows.map(row=>row.id));
    }
    if(lostOwnership)throw new AppError(503,'数据库单实例锁已丢失','gateway_ownership_lost');
    const extraUsage=createPostgresAccountExtraUsage(store,{...options.extraUsage,clock}),extraRuntime=createPostgresExtraRuntime(store,{clock,service:extraUsage});await extraUsage.recover({limit:100});requireOwnership();
    const auth=createAdminAuth(config,{clock}),router=createPostgresRouter(store,{clock,extraRuntime}),limits=createPostgresMemberLimits(store,{clock});
    const responses=options.responsesTransport||createResponsesTransport({outputBudget,stageObserver:options.stageObserver,fetchImpl:options.responsesFetch,credentials:(binary,home)=>accountCredentials(binary,home,{refresh:(binary,home)=>refreshCredentials(binary,home,{spawnProcess:spawnFor(home)})}),version:binary=>installedVersion(binary,{spawnProcess:spawnFor('internal:cli-version')})}),apiResponses=options.apiResponsesTransport||createResponsesTransport({outputBudget,stageObserver:options.stageObserver,apiKeyMode:true,credentials:async(_binary,accountId)=>{
      const account=await store.get("SELECT * FROM harbor.accounts WHERE id=$1 AND removed_at IS NULL AND enabled=1 AND auth_mode='openai_api'",accountId);
      const secret=account&&revealStoredKey(key,{id:'upstream:'+account.id,secret_cipher:account.api_secret_cipher,digest:account.api_secret_digest});
      if(!secret)throw new AppError(503,'官方API Key不可用，请检查账号配置','upstream_authentication');return {token:secret};
    }});
    const platformTransport=options.platformTransport||createPlatformTransport({qoderBinary:config.qoderBinary||'qodercli',clock,spawnForAccount:(account,context)=>spawnFor(account.home,context)});
    accountAdmin=createPostgresAccountAdmin(store,{platformTransport,runtime,encryptionKey:key,router,clock,checks:options.accountChecks,readQuota:(account,{signal})=>(options.rateReader||readRateLimits)(config.codexBinary,account.home,{signal,spawnProcess:spawnFor(account.home,{signal})}),local:['127.0.0.1','::1'].includes(network.listenHost),
      reserveLogin:options.loginStarter?undefined:account=>cliProcesses.reserve(account.home),
      startLogin:(account,{lease})=>{const binary=account.kind==='claude'?(config.claudeBinary||join(root,'runtime','claude-cli','node_modules','@anthropic-ai','claude-code','bin','claude.exe')):config.codexBinary;if(options.loginStarter)return options.loginStarter(account,{binary});let flow;try{flow=account.kind==='codex'?startCodexLogin(binary,account.home,{spawnProcess:lease.spawn}):startLocalLogin(binary,['auth','login'],claudeEnv(account.home),{spawnProcess:lease.spawn});}catch(error){lease.release();throw error;}return {...flow,completed:Promise.resolve(flow.completed).then(async result=>{lease.release();await lease.done;return result;})};},
      checkApi:(account,{signal})=>apiResponses.models({binary:config.codexBinary,home:account.id,signal,force:true}),
      checkSubscription:(account,{signal})=>account.kind==='claude'?(options.claudeChecker||checkClaudeLogin)(config.claudeBinary||join(root,'runtime','claude-cli','node_modules','@anthropic-ai','claude-code','bin','claude.exe'),account.home,{signal,spawnProcess:spawnFor(account.home,{signal})}):(options.checker||checkLogin)(config.codexBinary,account.home,{signal,spawnProcess:spawnFor(account.home,{signal})})});
    await accountAdmin.recover();requireOwnership();
    const extraReader=createPostgresAccountExtraUsage(queryStore,{clock}),extraBatches=createPostgresExtraUsageBatches(store,{readStore:queryStore,service:extraUsage,clock});extraUsageAdmin=createPostgresExtraUsageAdmin({service:extraUsage,reader:extraReader,policyPreviews:extraBatches.policyPreviews,batches:extraBatches,refreshSubscriptionQuota:(id,options)=>accountAdmin.refreshQuota(id,options),adminAuthenticated:auth.authenticated,adminSession:auth.sessionReference,acceptsAdminRequest:req=>acceptsRequest(req.headers,network),requestBodies,clock,refreshSupported:typeof options.extraUsage?.readEntitlement==='function'});
    const files=new Map(await Promise.all(['index.html','app.js','style.css'].map(async name=>[name,await readFile(join(root,'public',name))])));
    handler=createPostgresResponsesHandler({extraRuntime,runChat:({account,processLease,output,session,messages,model,signal,timeoutMs,onText})=>(account.kind==='claude'?(options.claudeExecutor||runClaude):(options.executor||runCodex))({binary:account.kind==='claude'?(config.claudeBinary||join(root,'runtime','claude-cli','node_modules','@anthropic-ai','claude-code','bin','claude.exe')):config.codexBinary,home:account.home,workspace:join(runtime,'jobs',session.id),messages,model,sessionId:session.upstream_id,previousUsage:{input:session.cum_input,output:session.cum_output,cached:session.cum_cached},signal,timeoutMs,onText,output,spawnProcess:processLease?.spawn||spawnFor(account.home,{kind:'model',signal})}),cliProcesses,outputBudget,requestBodies,stageObserver:options.stageObserver,executionOptions:options.execution,pricingOptions:options.pricing,chatOptions:{reserveTokens:config.reserveTokens??32000,...options.chat},store,queryStore,recordings,recordingReader,buffer:captureBuffer,meteringRecovery,accountAdmin,deviceIssuer:deviceSettings?.issuer,platformTransport,keyEncryptionKey:key,clock,codexBinary:config.codexBinary,
      maxPendingHandoffs:options.handoffs?.maxPending,recordingEnabled,captureContext,contextReader,recordingPolicy:recordingMonitor.snapshot,recordingKey:recordKey,recordingStoragePolicy:recordingMonitor.policy,recordingBackendOptions:options.recording?.backend,recordingAdminOptions:options.recording?.http,maxConcurrent:options.maxConcurrent??120,maxPerMember:options.maxPerMember??8,
      resolveRoute:router.resolve,onRouteFailure:router.block,checkLimits:(m,p,o)=>limits.check(m.id,p.model,{...o,tools:p.tools}),adminAuthenticated:auth.authenticated,
      transportForAccount:async account=>{if(account.kind!=='codex')throw new AppError(503,'此开发入口尚未接通该平台通道','platform_unavailable');return account.auth_mode==='openai_api'?apiResponses:responses;},
      adminStateContext:()=>({endpoint:network.origins[0]||'http://'+(network.listenHost==='::1'?'[::1]':network.listenHost)+':'+(server?.address()?.port||network.port||config.port)+'/v1',mode:'PostgreSQL · 开发验证',capabilities:{accountStateView:true,extraUsage:true,accountPackages:true,asyncPlanning:true,pagedHistory:true,sessionHandoffs:true,memberTypes:true,pagedMembers:true,memberListMetadata:true,sessionFilters:true,deviceAccess:true,memberActions:true,typeStatistics:true,recordingHistory:true}})});
    await handler.recoverHandoffs();requireOwnership();
    nightly=createPostgresNightly(store,{preparation:handler.preparation,sessionHandoffs:handler.sessionHandoffs,handoffAdmin:handler.handoffAdmin,refreshQuotas:options=>accountAdmin.refreshQuotas(options),clock,isStopping:()=>closing||lostOwnership});
    await nightly.recover();requireOwnership();
    const status=()=>({...handler.status(),jsonParsing:jsonParser.status(),cliProcesses:cliProcesses.status(),nightly:nightly.status(),ownership:!lostOwnership,interruptedOnStartup:interrupted,accountChecks:accountAdmin.status(),pools:{core:store.status(),query:queryStore.status(),recording:recordingStore.status()},recording:buffer.status(),recordingStorage:recordingMonitor.status(),recordingCapture:{enabled:recordingEnabled,effective:recordingEnabled&&recordingMonitor.captureAllowed(),reason:!recordingEnabled?'capture_disabled':recordingMonitor.snapshot().status==='low'?'storage_low':recordingMonitor.captureAllowed()?null:'storage_unknown'},devices:{...handler.deviceAccess.status(),tls:deviceTls?.status()??null},eventLoop:{meanMs:Number.isFinite(loop.mean)?loop.mean/1e6:0,maxMs:loop.max/1e6}});
    async function handle(req,res){
      const body=new AbortController(),closed=()=>{if(!res.writableEnded)body.abort();};res.on('close',closed);
      res.setHeader('Cache-Control','no-store');res.setHeader('X-Content-Type-Options','nosniff');res.setHeader('Referrer-Policy','no-referrer');
      res.setHeader('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'");
      try{
        if(!acceptsRequest(req.headers,network))throw new AppError(403,'不允许此Host或跨站请求');
        const path=new URL(req.url,'http://localhost').pathname;
        if(req.method==='GET'&&path==='/health')return json(res,200,{ok:true,service:'harbor',storage:'postgres'});
        if(req.method==='GET'&&path==='/ready'){const databaseReady=!closing&&!lostOwnership&&await store.ready(),ready=databaseReady&&!closing&&!lostOwnership;return json(res,ready?200:503,{ready});}
        if(closing||lostOwnership)throw new AppError(503,closing?'服务正在排空':'服务实例所有权已丢失',closing?'service_stopping':'gateway_ownership_lost');
        if(await extraUsageAdmin.handle(req,res))return;
        if(req.method==='GET'&&['/','/app.js','/style.css'].includes(path)){res.setHeader('Content-Type',path==='/'?'text/html; charset=utf-8':path.endsWith('.js')?'text/javascript; charset=utf-8':'text/css; charset=utf-8');return res.end(files.get(path==='/'?'index.html':path.slice(1)));}
        if(path==='/api/auth/login'&&req.method==='POST'){await auth.loginAsync((await requestBodies.read(req,{owner:'login',signal:body.signal})).password,res);return json(res,200,{ok:true});}
        if(path==='/api/auth/logout'&&req.method==='POST'){auth.logout(req,res);return json(res,200,{ok:true});}
        if(path==='/api/auth/session'&&req.method==='GET')return json(res,200,{authenticated:auth.authenticated(req)});
        if(path==='/api/admin/status'&&req.method==='GET'){if(!auth.authenticated(req))throw new AppError(401,'请先输入管理密码登录','admin_required');return json(res,200,status());}
        if(await handler.handle(req,res))return;
        throw new AppError(404,'此入口尚未接入PostgreSQL开发服务','not_found');
      }catch(error){
        if(res.destroyed||res.writableEnded)return;
        const known=Number.isInteger(error.status)&&error.status>=400&&error.status<=599;
        const databaseUnavailable=['ECONNREFUSED','ECONNRESET','ETIMEDOUT','EPIPE','57P01','57P02','57P03'].includes(error.code)||String(error.code||'').startsWith('08');
        const code=known?error.code||'request_failed':databaseUnavailable?'database_unavailable':'internal_error',message=known?sanitizeRecord(error.message,{maxBytes:2048}):databaseUnavailable?'数据库暂不可用':'请求处理失败';
        if(res.headersSent){res.end('event: error\ndata: '+JSON.stringify({type:'error',error:{code,message}})+'\n\n');return;}
        json(res,known?error.status:databaseUnavailable?503:500,{error:{code,message}});
      }finally{res.off('close',closed);requestBodies.release(req);}
    }
    const dispatch=(req,res)=>{res.once('finish',()=>{if(closing)setImmediate(()=>{server.closeIdleConnections();deviceTls?.server.closeIdleConnections();});});const task=handle(req,res);inflight.add(task);return task.then(()=>inflight.delete(task),()=>{inflight.delete(task);res.destroy();});};
    server=http.createServer(dispatch);
    if(deviceSettings?.tls)deviceTls=createDeviceTlsServer({...deviceSettings.tls,host:network.listenHost,handler:dispatch});
    if(lostOwnership)throw new AppError(503,'数据库单实例锁已丢失','gateway_ownership_lost');loop.enable();
    pricingTimer=setInterval(()=>{void handler.tickPricing().catch(()=>{});},60000);pricingTimer.unref();
    quotaTimer=setInterval(()=>{void Promise.resolve().then(()=>accountAdmin.refreshQuotas()).catch(()=>{});},30000);quotaTimer.unref();
    nightlyTimer=setInterval(()=>{void nightly.runOnce();},1000);nightlyTimer.unref();
    recordingTimer=setInterval(()=>{void recordingMonitor.refresh();},1000);recordingTimer.unref();
    function close(){
      if(closePromise)return closePromise;closing=true;clearInterval(pricingTimer);clearInterval(quotaTimer);clearInterval(nightlyTimer);clearInterval(recordingTimer);const nightlyClosed=nightly.close();handler.drain();accountAdmin.stop();
      closePromise=(async()=>{
        let forced=false;const queryClosed=Promise.all([handler.closeQueries(),extraUsageAdmin.close()]);
        const serverClosed=new Promise((resolve,reject)=>server.close(error=>error&&error.code!=='ERR_SERVER_NOT_RUNNING'?reject(error):resolve()));
        const tlsClosed=deviceTls?.close();
        const timer=setTimeout(()=>{forced=true;handler.abort();cliProcesses.stop();server.closeAllConnections();void deviceTls?.close({force:true});},drainTimeoutMs);timer.unref();
        let result,error;
        try{await Promise.all([serverClosed,tlsClosed]);await Promise.allSettled([...inflight]);await queryClosed;await nightlyClosed;}catch(e){error=e;}
        finally{clearTimeout(timer);result=await cleanup();}
        if(error)throw error;return {forced,ownershipLost:lostOwnership,...result};
      })();return closePromise;
    }
    return {server,deviceTls,ownershipSignal:ownership.signal,stageObserverStatus:()=>({gateway:handler.stageObserverStatus(),subscription:responses.stageObserverStatus?.()??{enabled:false},api:apiResponses.stageObserverStatus?.()??{enabled:false}}),deviceAccess:handler.deviceAccess,network,config,store,queryStore,recordingStore,buffer,meteringRecovery,extraUsage,recordingBackend:handler.recordingBackend,recordingStorage:recordingMonitor,preparation:handler.preparation,sessionHandoffs:handler.sessionHandoffs,handoffAdmin:handler.handoffAdmin,nightly,status,close};
  }catch(error){closing=true;await handler?.closeQueries();await cleanup();throw error;}
}

export async function startPostgresGateway(options={}){
  const app=await createPostgresGateway(options);
  try{await new Promise((resolve,reject)=>{const failed=error=>reject(error);app.server.once('error',failed);app.server.listen(app.network.port||app.config.port,app.network.listenHost,()=>{app.server.off('error',failed);resolve();});});await app.deviceTls?.listen();return app;}
  catch{await app.close();throw new AppError(503,'服务监听失败，请检查端口占用及网络配置','gateway_listen_failed');}
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  try{
    const shutdown=postgresShutdownConfiguration(process.env);const app=await startPostgresGateway({drainTimeoutMs:shutdown.gateway_drain_ms});console.log('Harbor PostgreSQL development ready');
    installPostgresGatewayShutdown(app);
  }catch(error){console.error(error.status?error.message:'PostgreSQL服务未能启动，请核对受控配置及数据库连接');process.exitCode=1;}
}
