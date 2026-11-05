import {apiClasses,operations,targets,capacityOperationCanonical} from './scenarios.mjs';

// Conservative upper-bound quantiles: three significant digits, >=0.01ms.
// Finite bins prevent an eight-hour run retaining every latency in memory.
export class Histogram{
  constructor(){this.bins=new Map();this.count=0;this.max=0;}
  add(value){if(!Number.isFinite(value)||value<0||value>1e12)throw new Error('capacity_measurement_invalid');const step=10**Math.max(-2,Math.floor(Math.log10(Math.max(value,0.01)))-2),upper=Number((Math.ceil(value/step)*step).toPrecision(14));const added=!this.bins.has(upper);this.bins.set(upper,(this.bins.get(upper)||0)+1);this.count++;this.max=Math.max(this.max,value);return added;}
  quantile(fraction){if(!Number.isFinite(fraction)||fraction<0||fraction>1)throw new Error('capacity_quantile_invalid');if(!this.count)return null;let seen=0;const rank=Math.ceil(this.count*fraction);for(const [upper,count] of [...this.bins].sort((a,b)=>a[0]-b[0])){seen+=count;if(seen>=rank)return upper;}return this.max;}
  view(){return {samples:this.count,p50:this.quantile(0.5),p95:this.quantile(0.95),p99:this.quantile(0.99),max:this.count?this.max:null,quantiles:'conservative-histogram-upper-ms'};}
}
const label=value=>typeof value==='string'&&/^[a-zA-Z0-9._:/-]{1,180}$/.test(value);
// A shared route can serve a stricter purpose than its original class. Keep
// that tighter bound in both aggregate and window checks; never relax a class.
const operationLimits=operation=>{const baseline=apiClasses[operation.category],p95=operation.p95===undefined?baseline?.p95:operation.p95,p99=operation.p99===undefined?baseline?.p99:operation.p99,valid=!!baseline&&Number.isFinite(p95)&&Number.isFinite(p99)&&p95>0&&p99>=p95&&p95<=baseline.p95&&p99<=baseline.p99;return {...(valid?{p95,p99}:baseline??{p95:0,p99:0}),valid};};
// Miss diagnostics use the existing violation stream's 1000-record limit.
// They never raise histogram budgets or change qualification counts.
const diagnosticLimit=1000,arrivalScenarios=new Set(['api-coverage-model','api-coverage-admin','api-coverage-probe','other']),arrivalReasons=new Set(['arrival_lag_exceeded','duration_exhausted','inflight_limit']);
export function sanitizeCapacityArrivalDiagnostic(value){
 if(!value||!arrivalReasons.has(value.reason))return null;
 return Object.freeze({scenario:arrivalScenarios.has(value.scenario)?value.scenario:'other',slotSequence:Number.isSafeInteger(value.slotSequence)&&value.slotSequence>=0?value.slotSequence:null,clock:'controller-performance-now',...Object.fromEntries(['scheduledAt','actualAt','lagMs','lagLimitMs'].map(key=>[key,Number.isFinite(value[key])&&value[key]>=0?value[key]:null])),reason:value.reason});
}
export function createCapacityMetrics({maxSeries=10000,maxWindows=1024,maxBuckets=2000000,windowMs=targets.windowMs}={}){
  if(![maxSeries,maxWindows,maxBuckets,windowMs].every(n=>Number.isSafeInteger(n)&&n>0))throw new Error('capacity_metric_configuration');
  const series=new Map(),windows=new Map(),violations=[],missedArrivals=[];let arrivalDiagnosticsDropped=0,usedBuckets=0,dropped=0,planned=0,sent=0,missed=0,inflight=0,peak=0,wireSent=0,httpEnded=0,unknownTransport=0;
  function add(map,key,record){let row=map.get(key);if(!row){row={operation:record.operation,scenario:record.scenario,variant:record.variant,category:record.category,group:record.group,histogram:new Histogram(),errors:{},success:0};map.set(key,row);}if(row.histogram.add(record.durationMs))usedBuckets++;if(record.success)row.success++;else {const code=typeof record.error==='string'&&/^[a-zA-Z0-9_.:-]{1,100}$/.test(record.error)?record.error:'unspecified';row.errors[code]=(row.errors[code]||0)+1;};}
  function record(input){const value={variant:'steady',group:'normal',category:'read',...input};if(![value.operation,value.scenario,value.variant,value.group,value.category].every(label)||typeof value.success!=='boolean'||!Number.isFinite(value.at)||!Number.isFinite(value.durationMs)||value.durationMs<0)throw new Error('capacity_metric_invalid');const key=[value.scenario,value.operation,value.variant,value.group,value.category].join('/'),time=Math.floor(value.at/windowMs);
    if(!series.has(key)&&series.size>=maxSeries||!windows.has(time)&&windows.size>=maxWindows){dropped++;return;}
    if(!windows.has(time))windows.set(time,new Map());if(usedBuckets+2>maxBuckets){dropped++;return;}
    add(series,key,value);add(windows.get(time),key,value);
  }
  const rows=map=>[...map.values()].map(row=>({...Object.fromEntries(Object.entries(row).filter(([key])=>key!=='histogram')),...row.histogram.view(),successRate:row.histogram.count?row.success/row.histogram.count:null}));
  return {record,transport:value=>{if(typeof value.wireSent!=='boolean'||typeof value.httpEnded!=='boolean')unknownTransport++;else{if(value.wireSent)wireSent++;if(value.httpEnded)httpEnded++;}},violation:(code,detail=null)=>{if(violations.length<diagnosticLimit)violations.push({code,detail});else dropped++;},arrival:({dispatched,diagnostic})=>{planned++;if(dispatched){sent++;inflight++;peak=Math.max(peak,inflight);}else{missed++;const detail=sanitizeCapacityArrivalDiagnostic(diagnostic);if(detail){if(missedArrivals.length<diagnosticLimit)missedArrivals.push(detail);else arrivalDiagnosticsDropped++;return detail;}}},finished:()=>{inflight--;if(inflight<0)throw new Error('capacity_inflight_underflow');},view:()=>({series:rows(series),windows:[...windows].sort((a,b)=>a[0]-b[0]).map(([index,map])=>({start:index*windowMs,end:(index+1)*windowMs,series:rows(map)})),arrivals:{planned,sent,offered:sent,wireSent,httpEnded,unknownTransport,missed,inflight,peak},dropped,violations,arrivalDiagnostics:{limit:diagnosticLimit,retained:missedArrivals.length,dropped:arrivalDiagnosticsDropped,truncated:arrivalDiagnosticsDropped>0,missed:[...missedArrivals]}})};
}
export function evaluateCapacity({plan,preflight,metrics,evidence={}}){
  const failures=[],incomplete=[],observations=[];
  if(!preflight?.eligible)incomplete.push('资源、数据或入口前置证据不足');
  if(plan.profile!=='formal')incomplete.push('仅为小规模工具自验');
  if(metrics.arrivals.unknownTransport)incomplete.push('缺客户端实际发送/响应结束计数');
  if(metrics.dropped)incomplete.push('指标预算耗尽，存在未采样记录');if(metrics.arrivals.missed)failures.push('发压端未发送全部预定到达请求');if(metrics.arrivals.inflight)incomplete.push('结束时仍有未完成请求');
  for(const violation of metrics.violations)failures.push(violation.code);
  const expected=plan.operations??operations;
  const byId=new Map(expected.map(operation=>[operation.id,operation]));
  const canonical=new Map(expected.map(operation=>[operation.id,capacityOperationCanonical(operation)]));
  const matches=(row,operation)=>row.operation===operation.id||operation.method==='GET'&&typeof canonical.get(operation.id)==='string'&&canonical.get(operation.id)===canonical.get(row.operation);
  for(const operation of expected)if(!operationLimits(operation).valid)failures.push('接口预算无效:'+operation.id);
  for(const operation of expected)if(canonical.get(operation.id)===null)failures.push('接口路由身份无效:'+operation.id);
  for(const operation of expected){const found=metrics.series.filter(row=>matches(row,operation)&&row.group==='normal');if(!found.length){incomplete.push('接口未测:'+operation.id);continue;}const variants=new Map();for(const row of found){variants.set(row.variant,(variants.get(row.variant)??0)+row.samples);const limits=operationLimits(operation);if(row.p95>limits.p95||row.p99>limits.p99)failures.push('接口延迟:'+operation.id+'/'+row.variant);}for(const [variant,samples] of variants)if(samples<targets.minSamples)incomplete.push('样本不足:'+operation.id+'/'+variant);}
  // The prescribed four-way background mix is required in each scene. Other
  // endpoints retain their suite-wide per-purpose/variant sample gates above;
  // health, tasks and self-service must not be inserted into those four pools.
  for(const scene of plan.scenarios){const rows=metrics.series.filter(row=>row.scenario===scene.id),group=scene.fault?'fault':scene.burst?'stress':'normal',normal=rows.filter(row=>row.group===group);for(const operation of ['model','first-content']){const selected=rows.filter(row=>row.operation===operation&&row.group===(operation==='first-content'&&scene.burst?'normal':group)),samples=selected.reduce((sum,row)=>sum+row.samples,0);if(samples<targets.minSamples)incomplete.push('场景测量样本不足:'+scene.id+'/'+operation);}const backgroundClasses=['list','detail','summary','configuration'],admin=normal.filter(row=>{const binding=byId.get(row.operation);return binding&&backgroundClasses.includes(binding.workloadClass??binding.category);});if(admin.reduce((sum,row)=>sum+row.samples,0)<targets.minSamples)incomplete.push('场景管理请求样本不足:'+scene.id);for(const category of ['list','detail','summary','configuration'])if(!admin.some(row=>(byId.get(row.operation).workloadClass??byId.get(row.operation).category)===category&&row.samples>0))incomplete.push('场景管理类别未测:'+scene.id+'/'+category);}
  for(const row of metrics.series.filter(row=>row.group==='normal'&&['model','read','write'].includes(row.category)))if(row.successRate<targets.successRate)failures.push('正常组成功率:'+row.operation+'/'+row.variant);
  // Each five-minute interval is evaluated independently, never averaged away.
  for(const window of metrics.windows)for(const row of window.series.filter(row=>row.group==='normal'))for(const operation of expected.filter(operation=>matches(row,operation))){const limits=operationLimits(operation);if(row.p95>limits.p95||row.p99>limits.p99)failures.push('五分钟窗口超标:'+operation.id+':'+window.start);}
  for(const scene of plan.scenarios)if(!evidence.scenarios?.[scene.id]?.completed||evidence.scenarios[scene.id].durationMs<scene.durationMs)incomplete.push('场景未完整执行:'+scene.id);
  const required=['resources','metricIntegrity','actorCoverage','serverArrivals','sources','networkBaseline','defaultAndMaxPages','deepPagination','coldQueries','tlsNewAndReuse','deviceHelper','recordingComparison','requestStages','streamRelay','cancelRelease','bodyVisibility','batchCompletion','exportAmounts','burstRecovery','databaseRecovery','workerRecovery','gracefulDrain','crashRecovery','hostRestoreRpo','dataReconciliation','idleRecovery','browserLists','browserStatistics','slowReceiver','longContextCompressionLimits','deviceRevocation'];
  for(const key of required){if(evidence[key]?.passed===false)failures.push('验收失败:'+key);else if(evidence[key]?.passed!==true)incomplete.push('缺少验收证据:'+key);}
  if((evidence.realTransferSlots?.filter(row=>row.actual&&row.applied>0).length??0)<2)incomplete.push('未覆盖两次实际时点转移');
  observations.push('未知、故障、突发及主动取消分组均保留，不静默移出报告。');
  return {status:failures.length?'failed':incomplete.length?'incomplete':'passed',failures:[...new Set(failures)],incomplete:[...new Set(incomplete)],observations};
}
