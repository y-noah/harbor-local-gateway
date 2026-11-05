import {createHash} from 'node:crypto';
import {targets} from './scenarios.mjs';

const id=value=>typeof value==='string'&&/^[a-zA-Z0-9._:-]{1,200}$/.test(value);
const ns=value=>typeof value==='string'&&/^\d{1,30}$/.test(value);
const signedNs=value=>typeof value==='string'&&/^-?\d{1,30}$/.test(value);
const sha=value=>createHash('sha256').update(value).digest('hex');
const outcomes=new Set(['ok','cancelled','failed','rejected','unknown']);
const leases=new Set(['execution','output','body','member']);
const roles={gateway:new Set(['body-end','dispatch','admission-attempt','admission-granted','wait','content','cancel','lease','accepted']),upstream:new Set(['cancel','end']),client:new Set(['content','end','cancel']),helper:new Set(['content','end']),reader:new Set(['watch-start','visibility'])};
const allowed=new Set(['receipt','trace','type','atNs','ordinal','contentKind','hash','bytes','startNs','endNs','queued','queue','outcome','lease','action','projection','queryStartNs','ready','readVerified','accepted','recordingExpected']);
const invalidationReasons=new Set(['gateway_source_unavailable','client_source_unavailable','client_timeout','client_failed','observer_pending','observer_failed','observer_dropped','source_epoch_closed','reader_unavailable','reader_timeout','reader_budget','ledger_unavailable','admission_unconfirmed','handler_unavailable','active_trace_budget','helper_source_unavailable','helper_timeout']);
const widths={preDispatch:targets.preDispatch,admission:targets.admission,firstRelay:targets.relay,middleRelay:targets.relay,bodyVisibility:targets.bodyVisibility,usageVisibility:targets.bodyVisibility};
const metricNames=[...Object.keys(widths),'cancelPropagation','release'];
const diagnosticNames=new Set(['clientCancelEndToEnd','clientReceivePath']);

export {classifyStageContent} from '../../streaming-stage-observer.mjs';

// Source authentication, clocks and durable trace history are capabilities
// supplied by the private owned factory, never booleans supplied by a request.
// client bound defaults to upper: Node reception cannot prove when the helper
// delivered. Exact bounds require a verified helper-delivery endpoint.
// history: lookup(trace)->'absent'|'active'|'final'|'unavailable';
// begin/finalize(trace)->'stored'|'duplicate'|'unavailable'. No historical Map.
// begin never decides publication policy. Only the verified gateway's accepted
// receipt does: admitted requests always expose usage, independent of capture.
export function createStreamingStageAssessment({referenceClock,verifySource,verifyTopology,topologyProof,history,profile='formal',minSamples=targets.minSamples,maxActive=10000,maxPendingContent=256,maxWaitSpans=32,maxIssues=100,maxWindows=512,maxBuckets=200000,windowMs=targets.windowMs,helperRequired=false}={}){
  if(typeof helperRequired!=='boolean'||!referenceClock||!id(referenceClock.kernelId)||typeof verifySource!=='function'||typeof verifyTopology!=='function'||!history||['lookup','begin','finalize'].some(name=>typeof history[name]!=='function')||!['formal','selfcheck'].includes(profile)||![minSamples,maxActive,maxPendingContent,maxWaitSpans,maxIssues,maxWindows,maxBuckets,windowMs].every(value=>Number.isSafeInteger(value)&&value>0)||profile==='formal'&&(minSamples!==targets.minSamples||windowMs!==targets.windowMs))throw new Error('capacity_stage_assessment_configuration');
  const referenceKernelId=referenceClock.kernelId;const active=new Map(),windows=new Map(),aggregate=new Map(),missing=new Set(),issues=[],groups={normal:0,fault:0,cancel:0,stress:0,comparison:0};let issueCount=0,started=0,finished=0,peakActive=0,buckets=0,closed=false,observerFailures=0,finalizationFailures=0;const skipped={notDispatched:0,failedClients:0,cancelAfterEnd:0,bodyNotExpected:0,usageNotExpected:0,lateFirstVisibility:0,clientCancelAfterUpstream:0,cancelBeforeDispatch:0};
  const problem=(code,trace)=>{issueCount++;if(issues.length<maxIssues)issues.push({code,reference:sha(String(trace)).slice(0,16)});};
  const lack=code=>missing.add(code);
  const synchronous=value=>{if(value&&typeof value.then==='function'){void Promise.resolve(value).catch(()=>{});throw new Error('capacity_stage_async_capability');}return value;};
  const lookup=trace=>{try{const value=synchronous(history.lookup(trace));if(!['absent','active','final','unavailable'].includes(value))throw new Error();if(value==='unavailable')lack('history_unavailable');return value;}catch{observerFailures++;lack('history_unavailable');return 'unavailable';}};
  const historyWrite=(method,trace)=>{try{const result=synchronous(history[method](trace));if(!['stored','duplicate','unavailable'].includes(result))throw new Error();if(result==='unavailable')lack('history_unavailable');return result;}catch{observerFailures++;lack('history_unavailable');return 'unavailable';}};
  const histogram=round=>({count:0,observed:0,max:null,bins:new Map(),add(value){if(!Number.isFinite(value)||value<0||value>1e12){lack('duration_out_of_range');return;}this.observed++;this.max=this.max===null?value:Math.max(this.max,value);const step=10**Math.max(-2,Math.floor(Math.log10(Math.max(value,0.01)))-2),key=Number((round(value/step)*step).toPrecision(14));if(!this.bins.has(key)){if(buckets>=maxBuckets){lack('histogram_budget');return;}buckets++;}this.bins.set(key,(this.bins.get(key)??0)+1);this.count++;},view(){const rows=[...this.bins].sort((a,b)=>a[0]-b[0]);const percentile=fraction=>{let seen=0;for(const [bound,count] of rows){seen+=count;if(seen>=Math.ceil(this.count*fraction))return bound;}return null;};return {samples:this.count,observedSamples:this.observed,p50:percentile(0.5),p95:percentile(0.95),p99:percentile(0.99),max:this.max};}});
  const pair=()=>({lower:histogram(Math.floor),upper:histogram(Math.ceil)});
  const sample=(name,range,trace,group,anchor)=>{
    if(!range)return;if(range.upper<0){problem('endpoint_order',trace);return;}if(range.lower<0&&!diagnosticNames.has(name))lack('stage_endpoint_order_uncertain');const lower=Math.max(0,range.lower),upper=Math.max(0,range.upper),key=group+':'+name;
    let item=aggregate.get(key);if(!item){item=pair();aggregate.set(key,item);}item.lower.add(lower);item.upper.add(upper);
    const unit=BigInt(windowMs)*1000000n;if(anchor&&anchor.lower/unit!==anchor.upper/unit){lack('stage_window_clock_uncertain');return;}const index=anchor?.lower===undefined?null:Number(anchor.lower/unit);if(index===null||!Number.isSafeInteger(index)){lack('window_clock_unknown');return;}let window=windows.get(index);if(!window){if(windows.size>=maxWindows){lack('window_budget');return;}window=new Map();windows.set(index,window);}let row=window.get(key);if(!row){row=pair();window.set(key,row);}row.lower.add(lower);row.upper.add(upper);
  };
  const normalize=(clock,value)=>{
    if(!ns(value)){lack('invalid_clock_value');return null;}const raw=BigInt(value),common=clock.kernelId===referenceKernelId&&clock.kind==='linux-hrtime-ns';let lower=0n,upper=0n;
    if(!common){const map=clock.mapping;if(!map||map.referenceKernelId!==referenceKernelId||!signedNs(map.lowerOffsetNs)||!signedNs(map.upperOffsetNs)||!ns(map.validFromNs)||!ns(map.validUntilNs)||raw<BigInt(map.validFromNs)||raw>BigInt(map.validUntilNs)||BigInt(map.lowerOffsetNs)>BigInt(map.upperOffsetNs)){lack('clock_mapping_unproven');return null;}lower=BigInt(map.lowerOffsetNs);upper=BigInt(map.upperOffsetNs);}
    return {raw,lower:raw+lower,upper:raw+upper,domain:clock.kernelId+':'+clock.clockId+':'+clock.epoch,counterDomain:clock.kernelId+':'+clock.kind};
  };
  const diff=(end,start)=>{if(!end||!start){lack('missing_endpoint_clock');return null;}if(end.counterDomain&&end.counterDomain===start.counterDomain)return {lower:Number(end.raw-start.raw)/1e6,upper:Number(end.raw-start.raw)/1e6};return {lower:Number(end.lower-start.upper)/1e6,upper:Number(end.upper-start.lower)/1e6};};
  function begin({trace,group='normal',recordingExpected=null}={}){
    if(closed){lack('collector_closed');return false;}if(!id(trace)||!Object.hasOwn(groups,group)||recordingExpected!==null){lack('invalid_trace_plan');return false;}
    const prior=lookup(trace);if(prior==='active'||prior==='final'||active.has(trace)){problem('duplicate_stage_trace',trace);return false;}
    const result=historyWrite('begin',trace);if(result==='duplicate'){problem('duplicate_stage_trace',trace);return false;}started++;groups[group]++;
    if(active.size>=maxActive){lack('active_trace_budget');return false;}
    active.set(trace,{trace,group,policy:null,points:new Map(),waits:[],leases:new Map(),pending:new Map(),content:{gateway:0,client:0,helper:0},helperNoProbe:false,visibility:new Map(),incomplete:false,firstAnchor:null});peakActive=Math.max(peakActive,active.size);return true;
  }
  // This capability can only remove proof. It never clears a measured failure,
  // accepts arbitrary diagnostic text or turns a missing policy into an opt-out.
  function invalidate({trace,reason}={}){
    if(!invalidationReasons.has(reason)||trace!==undefined&&!id(trace)){lack('invalid_invalidation');return false;}
    lack('stage_'+reason);if(trace!==undefined){const state=active.get(trace);if(state)state.incomplete=true;else lack('stage_invalidation_without_active_trace');}return true;
  }
  function source({role,proof}={}){
    if(!Object.hasOwn(roles,role))throw new Error('capacity_stage_source_role');let descriptor;
    try{descriptor=synchronous(verifySource({role,proof}));}catch{lack('source_verification_failed');}
    if(!descriptor||descriptor.role!==role||descriptor.clock?.kind!=='linux-hrtime-ns'||!id(descriptor.clock.kernelId)||!id(descriptor.clock.clockId)||!id(descriptor.clock.epoch)||typeof descriptor.isOpen!=='function'){lack('source_unproven');return Object.freeze({observe:()=>false});}
    if(role==='client'&&descriptor.endpoint==='remote-http-receive'&&typeof descriptor.readReceipt!=='function'){lack('remote_receipt_unproven');return Object.freeze({observe:()=>false});}
    if(role==='helper'&&(descriptor.endpoint!=='helper-socket-write-complete'||typeof descriptor.readReceipt!=='function')){lack('helper_delivery_bound_unproven');return Object.freeze({observe:()=>false});}
    const deliveryBound=role==='client'?(descriptor.bound??'upper'):null;if(role==='client'&&(!['upper','exact'].includes(deliveryBound)||deliveryBound==='exact'&&descriptor.endpoint!=='helper-delivery')){lack('client_delivery_bound_unproven');return Object.freeze({observe:()=>false,bound:null});}
    const inputClock=descriptor.clock,clock={kind:inputClock.kind,kernelId:inputClock.kernelId,clockId:inputClock.clockId,epoch:inputClock.epoch,mapping:inputClock.mapping?Object.fromEntries(['referenceKernelId','lowerOffsetNs','upperOffsetNs','validFromNs','validUntilNs'].map(key=>[key,inputClock.mapping[key]])):null},isOpen=descriptor.isOpen;
    const observe=value=>{
      try{
        if(closed){lack('collector_closed');return false;}if(synchronous(isOpen())!==true){lack('source_epoch_closed');return false;}
        if(!value||Object.keys(value).some(key=>!allowed.has(key))||!id(value.trace)||!roles[role].has(value.type)){problem('source_endpoint_rejected',value?.trace);return false;}
        const state=active.get(value.trace);if(!state){if(lookup(value.trace)==='final')problem('late_stage_event',value.trace);else lack('stage_event_without_active_trace');return false;}
        let at;
        if(role==='helper'){
          const receipt=synchronous(descriptor.readReceipt(value.receipt)),event=receipt?.event,range=receipt?.range;
          if(!event||!range?.known||event.trace!==value.trace||event.type!==value.type||['ordinal','hash','bytes','contentKind'].some(key=>event[key]!==value[key])||!ns(range.lowerNs)||!ns(range.upperNs)||BigInt(range.lowerNs)>BigInt(range.upperNs)){lack('helper_receipt_unproven');state.incomplete=true;return false;}
          if(event.type==='end'&&event.probeCreated===false){state.helperNoProbe=true;if(state.content.helper||state.content.client)problem('helper_probe_contradiction',value.trace);}
          at={lower:BigInt(range.lowerNs),upper:BigInt(range.upperNs),domain:'qpc:'+event.clockEpoch,counterDomain:null,qpc:BigInt(event.atTicks)};
        }else if(role==='client'&&descriptor.endpoint==='remote-http-receive'){
          const receipt=synchronous(descriptor.readReceipt(value.receipt)),event=receipt?.event,range=receipt?.range;
          if(!event||!range?.known||receipt.trace!==value.trace||event.type!==value.type||['atNs','ordinal','hash','bytes','contentKind','outcome'].some(key=>event[key]!==value[key])||!ns(range.lowerNs)||!ns(range.upperNs)||BigInt(range.lowerNs)>BigInt(range.upperNs)){lack('remote_receipt_unproven');state.incomplete=true;return false;}
          at={raw:BigInt(event.atNs),lower:BigInt(range.lowerNs),upper:BigInt(range.upperNs),domain:'remote:'+receipt.clockEpoch,counterDomain:'remote:'+receipt.runId+':'+receipt.session+':'+receipt.clockEpoch};
        }else at=normalize(clock,value.atNs);if(!at)state.incomplete=true;state.firstAnchor??=at;
        const put=(key,point)=>{if(state.points.has(key)){problem('duplicate_stage_endpoint',value.trace);return false;}state.points.set(key,point);return true;};
        if(value.type==='accepted'){
          if(state.policy!==null){problem('duplicate_acceptance_policy',value.trace);return false;}
          if(typeof value.accepted!=='boolean'||typeof value.recordingExpected!=='boolean'){lack('invalid_acceptance_policy');state.incomplete=true;return false;}
          if(!value.accepted&&value.recordingExpected){problem('acceptance_policy_contradiction',value.trace);return false;}
          state.policy={accepted:value.accepted,bodyExpected:value.accepted&&value.recordingExpected,usageExpected:value.accepted};return true;
        }
        if(value.type==='content'){
          if(state.helperNoProbe&&['client','helper'].includes(role))problem('helper_probe_contradiction',value.trace);
          if(state.points.has(role+':end')){problem('content_after_terminal',value.trace);return false;}
          if(!Number.isSafeInteger(value.ordinal)||value.ordinal<0||!['text','arguments','terminal'].includes(value.contentKind)||typeof value.hash!=='string'||!/^[a-f0-9]{64}$/.test(value.hash)||!Number.isSafeInteger(value.bytes)||value.bytes<=0){lack('invalid_content_evidence');state.incomplete=true;return false;}
          if(value.contentKind==='terminal'&&state.content[role]>0)return true;
          if(value.ordinal<state.content[role]){problem('duplicate_content_ordinal',value.trace);return false;}if(value.ordinal!==state.content[role]){lack('content_sequence_gap');state.incomplete=true;}state.content[role]=value.ordinal+1;const previousContent=state.points.get(role+':last-content');if(previousContent){const ordered=role==='helper'&&at&&previousContent?{lower:at.qpc>=previousContent.qpc?0:-1,upper:at.qpc>=previousContent.qpc?0:-1}:diff(at,previousContent);if(ordered?.upper<0)problem('content_endpoint_order',value.trace);else if(ordered&&ordered.lower<0){lack('content_order_uncertain');state.incomplete=true;}}if(!state.points.has(role+':first-content'))state.points.set(role+':first-content',at);state.points.set(role+':last-content',at);
          let pending=state.pending.get(value.ordinal);if(!pending){if(state.pending.size>=maxPendingContent){lack('pending_content_budget');state.incomplete=true;return false;}pending={};state.pending.set(value.ordinal,pending);}if(pending[role]){problem('duplicate_content_ordinal',value.trace);return false;}pending[role]={at,hash:value.hash,bytes:value.bytes,kind:value.contentKind,bound:deliveryBound};
          if(pending.gateway&&pending.client&&!pending.clientMatched){
            pending.clientMatched=true;
            if(pending.gateway.hash!==pending.client.hash||pending.gateway.bytes!==pending.client.bytes||pending.gateway.kind!==pending.client.kind)problem('content_identity_mismatch',value.trace);
            else{const received=diff(pending.client.at,pending.gateway.at);if(received&&received.upper<0)problem('endpoint_order',value.trace);else{if(received&&received.lower<0){lack('content_receive_order_uncertain');state.incomplete=true;}sample('clientReceivePath',received,value.trace,state.group,pending.gateway.at);if(!helperRequired)sample(value.ordinal===0?'firstRelay':'middleRelay',received&&pending.client.bound==='upper'?{lower:0,upper:received.upper}:received,value.trace,state.group,pending.gateway.at);}}
          }
          if(pending.gateway&&pending.client&&(!helperRequired||pending.helper)){
            // The owned Responses hook stamps content before onEvent/terminal return;
            // three matching identities therefore prove a nonnegative relay.
            if(helperRequired){if(pending.gateway.hash!==pending.helper.hash||pending.gateway.bytes!==pending.helper.bytes||pending.gateway.kind!==pending.helper.kind)problem('helper_content_identity_mismatch',value.trace);else{const elapsed=diff(pending.helper.at,pending.gateway.at);if(elapsed&&elapsed.upper<0)problem('endpoint_order',value.trace);else sample(value.ordinal===0?'firstRelay':'middleRelay',elapsed?{lower:Math.max(0,elapsed.lower),upper:elapsed.upper}:null,value.trace,state.group,pending.gateway.at);}}
            state.pending.delete(value.ordinal);
          }return true;
        }
        if(value.type==='wait'){
          if(typeof value.queued!=='boolean'||!['capacity','preparation','handoff'].includes(value.queue)||!['granted','rejected'].includes(value.outcome)||!ns(value.startNs)||!ns(value.endNs)){lack('invalid_wait_evidence');state.incomplete=true;return false;}
          if(state.waits.length>=maxWaitSpans){lack('wait_span_budget');state.incomplete=true;return false;}const start=normalize(clock,value.startNs),end=normalize(clock,value.endNs);if(diff(end,start)?.upper<0)problem('wait_endpoint_order',value.trace);state.waits.push({start,end,queued:value.queued,queue:value.queue,outcome:value.outcome});return true;
        }
        if(value.type==='lease'){
          if(!leases.has(value.lease)||!['acquire','release','not-acquired'].includes(value.action)){lack('invalid_lease_evidence');state.incomplete=true;return false;}let lease=state.leases.get(value.lease);if(!lease){lease={};state.leases.set(value.lease,lease);}if(Object.hasOwn(lease,value.action)){problem('duplicate_lease_endpoint',value.trace);return false;}lease[value.action]=at;return true;
        }
        if(value.type==='watch-start'){
          if(!['body','usage'].includes(value.projection)){lack('invalid_visibility_evidence');return false;}let watched=state.visibility.get(value.projection);if(!watched){watched={};state.visibility.set(value.projection,watched);}if(watched.watch){problem('duplicate_visibility_watch',value.trace);return false;}watched.watch=at;return true;
        }
        if(value.type==='visibility'){
          if(!['body','usage'].includes(value.projection)||typeof value.ready!=='boolean'||typeof value.readVerified!=='boolean'||!ns(value.queryStartNs)){lack('invalid_visibility_evidence');state.incomplete=true;return false;}
          let watched=state.visibility.get(value.projection);if(!watched){watched={};state.visibility.set(value.projection,watched);}if(watched.ready){problem('visibility_after_final_read',value.trace);return false;}const start=normalize(clock,value.queryStartNs),elapsed=diff(at,start);if(elapsed&&elapsed.lower<0&&elapsed.upper>=0){lack('visibility_query_clock_uncertain');state.incomplete=true;}if(elapsed?.upper<0){problem('visibility_query_order',value.trace);return false;}
          if(!watched.watch){lack('visibility_watch_not_established');state.incomplete=true;}else{const order=diff(start,watched.watch);if(order?.upper<0){problem('visibility_query_before_watch',value.trace);return false;}if(order&&order.lower<0){lack('visibility_watch_order_uncertain');state.incomplete=true;}}const previous=watched.lastQueryEnd?diff(start,watched.lastQueryEnd):null;if(previous?.upper<0){problem('visibility_query_overlap_or_reorder',value.trace);return false;}if(previous&&previous.lower<0){lack('visibility_query_order_uncertain');state.incomplete=true;}watched.lastQueryEnd=at;watched.firstStart??=start;
          if(value.ready){if(!value.readVerified){lack('visibility_read_not_verified');state.incomplete=true;return false;}watched.ready={start,end:at};}else watched.notReady=start;return true;
        }
        if(value.type==='end'){if(!outcomes.has(value.outcome)){lack('invalid_terminal_outcome');state.incomplete=true;return false;}put(role+':outcome',value.outcome);return put(role+':end',at);}
        const stored=put(role+':'+value.type,at);if(stored&&role==='gateway'&&value.type==='body-end')state.firstAnchor=at;return stored;
      }catch{observerFailures++;lack('observer_unavailable');return false;}
    };
    return Object.freeze({observe,bound:deliveryBound});
  }
  function finalize(trace){
    if(!id(trace)){lack('invalid_trace_plan');return false;}const state=active.get(trace);if(!state){if(lookup(trace)==='final')return true;lack('finalize_without_active_trace');if(historyWrite('finalize',trace)==='unavailable')finalizationFailures++;return false;}
    const point=key=>state.points.get(key),miss=code=>{lack(code);state.incomplete=true;},dispatch=point('gateway:dispatch'),clientEnd=point('client:end'),upstreamEnd=point('upstream:end'),gatewayCancel=point('gateway:cancel'),clientCancel=point('client:cancel'),outcome=point('client:outcome');
    const checkOrder=(end,start,code)=>{if(!end||!start)return;const elapsed=diff(end,start);if(elapsed?.upper<0)problem(code,trace);else if(elapsed&&elapsed.lower<0)miss(code+'_uncertain');};
    checkOrder(point('gateway:first-content'),dispatch,'content_before_dispatch');checkOrder(clientEnd,point('client:last-content'),'content_after_client_end');
    if(!clientEnd||!outcome)miss('missing_client_terminal');if(state.pending.size||state.content.gateway!==state.content.client||helperRequired&&state.content.gateway!==state.content.helper)miss('unmatched_effective_content');
    if(helperRequired&&state.content.gateway>0&&!point('helper:end'))miss('missing_helper_terminal');
    for(const lease of state.leases.values()){const acquired=Object.hasOwn(lease,'acquire'),released=Object.hasOwn(lease,'release'),neverAcquired=Object.hasOwn(lease,'not-acquired');if(neverAcquired&&(acquired||released))problem('lease_acquisition_contradiction',trace);if(acquired&&!released||released&&!acquired&&!neverAcquired)miss('missing_lease_evidence');if(acquired&&released){const elapsed=diff(lease.release,lease.acquire);if(elapsed?.upper<0)problem('lease_release_before_acquire',trace);else if(elapsed&&elapsed.lower<0)miss('lease_release_order_uncertain');}}
    const executed=!!dispatch||!!upstreamEnd||state.content.gateway>0||outcome==='ok';if(state.policy===null)miss('missing_acceptance_policy');else if(!state.policy.accepted&&(executed||point('upstream:cancel')||state.content.client>0))problem('acceptance_policy_contradiction',trace);if(!dispatch&&executed)miss('missing_gateway_dispatch');if(executed){
      const bodyEnd=point('gateway:body-end');let processing=diff(dispatch,bodyEnd);if(processing?.upper<0)problem('dispatch_before_body_end',trace);else if(processing&&processing.lower<0)miss('body_dispatch_order_uncertain');
      if(processing&&bodyEnd){const intervals=[];for(const wait of state.waits){if(!wait.queued||wait.outcome!=='granted')continue;if(!wait.start||!wait.end||wait.start.counterDomain!==bodyEnd.counterDomain||wait.end.counterDomain!==dispatch.counterDomain){miss('wait_clock_unproven');continue;}const start=wait.start.raw>bodyEnd.raw?wait.start.raw:bodyEnd.raw,end=wait.end.raw<dispatch.raw?wait.end.raw:dispatch.raw;if(end>start)intervals.push([start,end]);}intervals.sort((a,b)=>a[0]<b[0]?-1:a[0]>b[0]?1:0);let amount=0n,start=null,end=null;for(const [a,b] of intervals){if(start===null){start=a;end=b;}else if(a<=end){if(b>end)end=b;}else{amount+=end-start;start=a;end=b;}}if(start!==null)amount+=end-start;const deducted=Number(amount)/1e6;processing={lower:processing.lower-deducted,upper:processing.upper-deducted};sample('preDispatch',processing,trace,state.group,dispatch);}
      const granted=point('gateway:admission-granted'),attempt=point('gateway:admission-attempt');if(!attempt||!granted)miss('missing_capacity_admission');else sample('admission',diff(granted,attempt),trace,state.group,granted);
      if(!upstreamEnd)miss('missing_upstream_end');else if(dispatch){const ended=diff(upstreamEnd,dispatch);if(ended?.upper<0)problem('upstream_ended_before_dispatch',trace);else if(ended&&ended.lower<0)miss('upstream_dispatch_order_uncertain');}let releaseLower=null,releaseUpper=null,releaseRange=null;
      for(const name of leases){const lease=state.leases.get(name);if(!lease){miss('missing_lease_evidence');continue;}if(Object.hasOwn(lease,'not-acquired')){problem('executed_without_required_lease',trace);continue;}if(!lease.acquire||!lease.release){miss('missing_lease_evidence');continue;}if(dispatch){const beforeDispatch=diff(dispatch,lease.acquire);if(beforeDispatch?.upper<0)problem('lease_acquired_after_dispatch',trace);else if(beforeDispatch&&beforeDispatch.lower<0)miss('lease_acquisition_order_uncertain');}if(upstreamEnd){const elapsed=diff(lease.release,upstreamEnd);if(elapsed){if(elapsed.lower<0&&elapsed.upper>=0)miss('resource_release_order_uncertain');const delay={lower:Math.max(0,elapsed.lower),upper:Math.max(0,elapsed.upper)};releaseRange=releaseRange?{lower:Math.max(releaseRange.lower,delay.lower),upper:Math.max(releaseRange.upper,delay.upper)}:delay;}}if(releaseLower===null||lease.release.lower>releaseLower)releaseLower=lease.release.lower;if(releaseUpper===null||lease.release.upper>releaseUpper)releaseUpper=lease.release.upper;}
      if(upstreamEnd&&releaseRange)sample('release',releaseRange,trace,state.group,{lower:releaseLower,upper:releaseUpper});
    }else skipped.notDispatched++;
    if((clientCancel||point('upstream:cancel')||state.group==='cancel'||outcome==='cancelled')&&!gatewayCancel)miss('missing_gateway_cancel');if(clientCancel&&point('upstream:cancel')){const elapsed=diff(point('upstream:cancel'),clientCancel);if(elapsed?.upper<0)skipped.clientCancelAfterUpstream++;else sample('clientCancelEndToEnd',elapsed,trace,state.group,point('upstream:cancel'));}if(gatewayCancel){const cancelled=point('upstream:cancel'),execution=state.leases.get('execution'),neverExecuted=!executed&&!cancelled&&execution?.['not-acquired']&&!execution.acquire&&!execution.release,afterEnd=upstreamEnd?diff(gatewayCancel,upstreamEnd):null;if(neverExecuted)skipped.cancelBeforeDispatch++;else if(!cancelled&&afterEnd&&afterEnd.lower>0)skipped.cancelAfterEnd++;else{if(afterEnd&&afterEnd.lower<0&&afterEnd.upper>0)miss('cancel_end_order_uncertain');if(!cancelled)miss('missing_upstream_cancel');else{const propagated=diff(cancelled,gatewayCancel);if(propagated&&propagated.lower<0&&propagated.upper>=0)miss('cancel_propagation_order_uncertain');sample('cancelPropagation',propagated,trace,state.group,cancelled);}}}
    if(outcome==='ok'&&(!state.content.gateway||!state.content.client))miss('missing_first_effective_content');if(outcome!=='ok')skipped.failedClients++;
    for(const projection of ['body','usage']){
      const expected=state.policy?.[projection+'Expected'];if(expected!==true){if(expected===false)skipped[projection+'NotExpected']++;continue;}const watched=state.visibility.get(projection);if(!watched?.watch||!watched.ready||!clientEnd){miss('missing_continuous_visibility');continue;}const started=diff(watched.firstStart,clientEnd),watchDelay=diff(watched.watch,clientEnd);if(started?.upper<0||watchDelay?.upper<0){problem('visibility_before_client_completion',trace);continue;}if(started&&started.lower<0||watchDelay&&watchDelay.lower<0)miss('visibility_completion_order_uncertain');const watchOrder=diff(watched.ready.start,watched.watch);if(watchOrder?.upper<0){problem('visibility_query_before_watch',trace);continue;}if(watchOrder&&watchOrder.lower<0)miss('visibility_watch_order_uncertain');const upper=diff(watched.ready.end,clientEnd),lower=watched.notReady?diff(watched.notReady,clientEnd):{lower:0};if(!upper||!lower)continue;if(!watched.notReady&&upper.upper>targets.bodyVisibility.p99)skipped.lateFirstVisibility++;sample(projection==='body'?'bodyVisibility':'usageVisibility',{lower:Math.max(0,lower.lower),upper:upper.upper},trace,state.group,watched.ready.end);}
    const stored=historyWrite('finalize',trace);if(stored!=='stored'){if(stored==='duplicate')problem('duplicate_stage_finalization',trace);else finalizationFailures++;}active.delete(trace);finished++;return stored==='stored';
  }
  const viewItem=(item,name,required=minSamples,group='normal')=>{
    const lower=item?.lower.view()??{samples:0,p50:null,p95:null,p99:null,max:null},upper=item?.upper.view()??{samples:0,p50:null,p95:null,p99:null,max:null};let passed=null;if(diagnosticNames.has(name)||group!=='normal'&&['bodyVisibility','usageVisibility'].includes(name))return {passed:null,lower,upper,diagnostic:true};
    if(widths[name]){const limit=widths[name];if(lower.samples===lower.observedSamples&&lower.samples>=required&&(lower.p95>limit.p95||lower.p99>limit.p99))passed=false;else if(upper.samples===upper.observedSamples&&upper.samples>=required&&upper.p95<=limit.p95&&upper.p99<=limit.p99)passed=true;}
    else{const limit=name==='release'?targets.releaseAfterUpstreamMs:targets.cancelPropagationMs;if(lower.observedSamples&&lower.max>limit)passed=false;else if(upper.observedSamples&&upper.max<=limit)passed=true;}
    return {passed,lower,upper,requiredSamples:widths[name]?required:1,threshold:widths[name]??{max:name==='release'?targets.releaseAfterUpstreamMs:targets.cancelPropagationMs}};
  };
  function collect(){
    let topology=null;try{topology=synchronous(verifyTopology(topologyProof));}catch{lack('topology_unavailable');}const topologyReady=!!topology&&['realHelper','tls','lan','proxyPreserved'].every(key=>topology[key]===true),rows={};
    for(const name of metricNames){if(name==='release'||name==='cancelPropagation'){const merged=pair();for(const group of Object.keys(groups)){const row=aggregate.get(group+':'+name);if(!row)continue;for(const direction of ['lower','upper']){merged[direction].count+=row[direction].count;merged[direction].observed+=row[direction].observed;merged[direction].max=Math.max(merged[direction].max??0,row[direction].max??0);for(const [key,count] of row[direction].bins)merged[direction].bins.set(key,(merged[direction].bins.get(key)??0)+count);}}rows[name]=viewItem(merged,name);}else rows[name]=viewItem(aggregate.get('normal:'+name),name);}
    for(const window of windows.values())for(const [key,item] of window){if(!key.startsWith('normal:'))continue;const name=key.slice(7);if(!Object.hasOwn(rows,name))continue;const result=viewItem(item,name,1).passed;if(result===false)rows[name].passed=false;else if(result===null&&rows[name].passed!==false){rows[name].passed=null;rows[name].windowIntervalUncertain=true;}}
    const incomplete=missing.size>0||active.size>0||started!==finished||!topologyReady;for(const row of Object.values(rows)){row.measurementPassed=row.passed;if(issueCount)row.passed=false;else if(!topologyReady)row.passed=null;else if(incomplete&&row.passed!==false)row.passed=null;}
    const components={requestStages:[rows.preDispatch,rows.admission],streamRelay:[rows.firstRelay,rows.middleRelay],cancelRelease:[rows.cancelPropagation,rows.release],bodyVisibility:[rows.bodyVisibility,rows.usageVisibility]};const combine=list=>list.some(row=>row.passed===false)?false:list.every(row=>row.passed===true)?true:null;
    return {profile,helperRequired,relayEndpoint:helperRequired?'helper-socket-write-complete':'node-client-upper-bound',formalEligible:profile==='formal',passed:combine(Object.values(rows)),...Object.fromEntries(Object.entries(components).map(([key,value])=>[key,{passed:combine(value)}])),metrics:rows,groups:{...groups},counts:{started,finished,active:active.size,peakActive,observerFailures,finalizationFailures},skipped:{...skipped},diagnostics:Object.fromEntries([...diagnosticNames].map(name=>[name,Object.fromEntries(Object.keys(groups).filter(group=>aggregate.has(group+':'+name)).map(group=>[group,viewItem(aggregate.get(group+':'+name),name)]))])),issues:[...issues],issueCount,incomplete,missing:[...missing,...(!topologyReady?['helper_tls_lan_proxy_unproven']:[])],bounds:{maxActive,maxPendingContent,maxWaitSpans,maxIssues,maxWindows,maxBuckets,usedBuckets:buckets},windows:[...windows].sort((a,b)=>a[0]-b[0]).map(([index,metrics])=>({index,metrics:Object.fromEntries([...metrics].map(([key,value])=>[key,viewItem(value,key.slice(key.indexOf(':')+1),1,key.slice(0,key.indexOf(':')))]))}))};
  }
  return Object.freeze({begin,source,invalidate,finalize,collect,close:()=>{if(closed)return;closed=true;if(active.size)lack('collector_closed_with_active_traces');}});
}
