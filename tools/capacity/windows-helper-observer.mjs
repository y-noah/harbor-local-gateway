import {randomUUID} from 'node:crypto';
import {StringDecoder} from 'node:string_decoder';
import {ownedProcessProof} from './owned-runtime.mjs';

const receipts=new WeakMap(),observations=new WeakMap(),observers=new WeakMap(),scope='owned-helper-stdout-causal-envelope';
const uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(value);
const integer=value=>typeof value==='string'&&/^[0-9]{1,20}$/.test(value);
const fail=code=>Error('capacity_windows_observer_'+code);
export function capacityWindowsHelperDeliveryReceipt(value){return receipts.get(value)??null;}
export function capacityWindowsHelperObserverBinding(value){const record=observers.get(value);return record?.isOpen()?record:null;}

// Only an owned process channel can publish events. Ownership alone does not
// authenticate its executable, the endpoint topology or an application read.
export function observeCapacityWindowsHelper({runtime,child,onEvent=()=>{},maxSamples=128,maxPendingEvents=128,sampleMaxAgeMs=30000,timeoutMs=5000}={}){
 const owner=ownedProcessProof(runtime,child);
 if(owner.role!=='helper'||!child.stdout||!child.stdin||typeof onEvent!=='function'||!Number.isSafeInteger(maxSamples)||maxSamples<2||maxSamples>4096||!Number.isSafeInteger(maxPendingEvents)||maxPendingEvents<1||maxPendingEvents>128||!Number.isSafeInteger(sampleMaxAgeMs)||sampleMaxAgeMs<100||sampleMaxAgeMs>600000||!Number.isSafeInteger(timeoutMs)||timeoutMs<100||timeoutMs>30000)throw fail('configuration');
 let buffered='',sequence=0n,epoch,frequency,terminal=false,closed=false,ended=false,fault=null,pending=null,pendingEvents=0,eventCount=0,evicted=0,samples=[],closePromise,stopPromise,sampleTimer=null,sampling=false;
 const decoder=new StringDecoder('utf8'),events=new Set();
 const unhealthy=code=>{fault??=code;if(pending){clearTimeout(pending.timer);pending.reject(fail(code));pending=null;}};
 const live=()=>{try{const current=ownedProcessProof(runtime,child);return !closed&&!ended&&!fault&&current.pid===owner.pid&&current.role===owner.role&&current.requestedAt===owner.requestedAt;}catch{return false;}};
 const prune=()=>{const cutoff=process.hrtime.bigint()-BigInt(sampleMaxAgeMs)*1000000n;const next=samples.filter(row=>row.controllerReceivedNs>=cutoff);evicted+=samples.length-next.length;samples=next;};
 const emit=row=>{
  if(pendingEvents>=maxPendingEvents){unhealthy('sink_budget');return;}
  const value=Object.freeze({...row,clockEpoch:epoch,frequency:frequency.toString(),scope,formalQualified:false});observations.set(value,{owner,observer:api});eventCount++;
  let release,settled=false;const task=new Promise(resolve=>{release=resolve;});pendingEvents++;events.add(task);
  const settle=()=>{if(settled)return;settled=true;pendingEvents--;events.delete(task);release();};
  try{const result=onEvent(value);if(result===false){unhealthy('sink_failed');settle();return;}
   if(result&&typeof result.then==='function')void Promise.resolve(result).then(accepted=>{if(accepted===false)unhealthy('sink_failed');},()=>unhealthy('sink_failed')).finally(settle);else settle();
  }catch{unhealthy('sink_failed');settle();}
 };
 const line=text=>{
  if(terminal){unhealthy('output_after_terminal');return;}
  let row;try{row=JSON.parse(text);}catch{unhealthy('protocol_invalid');return;}
  if(row?.capacity_delivery!=='helper-socket-write-complete-v1'){
   if(row?.state==='running'&&Object.keys(row).every(key=>['state','local_port','key_protection','hardware'].includes(key)))return;
   unhealthy('protocol_invalid');return;
  }
  if(terminal||!uuid(row.clockEpoch)||!integer(row.sequence)||!Number.isSafeInteger(row.dropped)||row.dropped!==0||row.fault!==false||row.formalQualified!==false){unhealthy('source_incomplete');return;}
  if(epoch!==undefined&&epoch!==row.clockEpoch){unhealthy('epoch_changed');return;}epoch=row.clockEpoch;
  if(row.type==='status'){
   if(BigInt(row.sequence)!==sequence){unhealthy('sequence_invalid');return;}terminal=true;return;
  }
  if(BigInt(row.sequence)!==sequence+1n||!integer(row.frequency)||BigInt(row.frequency)<1n||frequency!==undefined&&frequency!==BigInt(row.frequency)){unhealthy('sequence_invalid');return;}
  sequence=BigInt(row.sequence);frequency=BigInt(row.frequency);
  if(row.type==='clock'){
   if(!pending||row.requestId!==pending.requestId||!integer(row.receivedTicks)||!integer(row.sentTicks)||BigInt(row.receivedTicks)>BigInt(row.sentTicks)){unhealthy('clock_reply_invalid');return;}
   const previous=samples.at(-1),received=BigInt(row.receivedTicks),sent=BigInt(row.sentTicks),current=pending;pending=null;clearTimeout(current.timer);
   if(previous&&(received<previous.remoteSentTicks||current.controllerSentNs<previous.controllerReceivedNs)){unhealthy('clock_order_invalid');current.reject(fail('clock_order_invalid'));return;}
   prune();samples.push(Object.freeze({remoteReceivedTicks:received,remoteSentTicks:sent,controllerSentNs:current.controllerSentNs,controllerReceivedNs:process.hrtime.bigint()}));
   if(samples.length>maxSamples){evicted+=samples.length-maxSamples;samples=samples.slice(-maxSamples);}current.resolve(Object.freeze({clockEpoch:epoch,samples:samples.length,scope,formalQualified:false}));return;
  }
  if(!uuid(row.trace)||!integer(row.atTicks)){unhealthy('event_invalid');return;}
  const base={type:row.type,trace:row.trace,atTicks:row.atTicks};
  if(row.type==='content'){
   if(!['text','arguments','terminal'].includes(row.contentKind)||typeof row.hash!=='string'||!/^[a-f0-9]{64}$/.test(row.hash)||!Number.isSafeInteger(row.bytes)||row.bytes<1||row.bytes>1048576||!Number.isSafeInteger(row.ordinal)||row.ordinal<0){unhealthy('event_invalid');return;}
   emit({...base,contentKind:row.contentKind,hash:row.hash,bytes:row.bytes,ordinal:row.ordinal});
  }else if(row.type==='end'&&typeof row.complete==='boolean'&&(row.probeCreated===undefined||typeof row.probeCreated==='boolean')&&!(row.probeCreated===false&&row.complete))emit({...base,complete:row.complete,probeCreated:row.probeCreated??true});
  else if(row.type==='unavailable')emit(base);
  else unhealthy('event_invalid');
 };
 const data=chunk=>{
  if(fault||closed)return;const text=decoder.write(chunk);let start=0;
  for(;;){const end=text.indexOf('\n',start);if(end<0)break;const piece=text.slice(start,end);if(buffered.length+piece.length>8192){unhealthy('line_budget');return;}line((buffered+piece).replace(/\r$/,''));buffered='';start=end+1;if(fault||closed)return;}
  if(buffered.length+text.length-start>8192){unhealthy('line_budget');return;}buffered+=text.slice(start);
 };
 const finish=()=>{if(ended)return;ended=true;if(decoder.end()||buffered||!terminal)unhealthy('terminal_missing');else if(pending)unhealthy('clock_closed');};
 const streamError=()=>unhealthy('stream_failed');
 child.stdout.on('data',data);child.stdout.once('end',finish);child.stdout.once('error',streamError);child.stdin.on('error',streamError);child.once('error',streamError);child.once('close',finish);
 const sample=()=>{
  if(!live()||terminal)return Promise.reject(fail('closed'));if(pending)return pending.promise;
  const requestId=randomUUID(),controllerSentNs=process.hrtime.bigint();let resolve,reject;
  const promise=new Promise((yes,no)=>{resolve=yes;reject=no;}),timer=setTimeout(()=>unhealthy('clock_timeout'),timeoutMs);pending={requestId,controllerSentNs,resolve,reject,timer,promise};
  try{child.stdin.write('capacity-clock '+requestId+'\n',error=>{if(error)unhealthy('control_failed');});}catch{unhealthy('control_failed');}return promise;
 };
 const bounds=event=>{
  const binding=observations.get(event);if(!binding||binding.owner!==owner||binding.observer!==api)throw fail('event_not_owned');prune();
  const unknown=reason=>Object.freeze({known:false,reason,scope,formalQualified:false});if(!live())return unknown('source_closed_or_failed');
  let lower,upper;const at=BigInt(event.atTicks);for(const row of samples){if(row.remoteSentTicks<at&&(lower===undefined||row.controllerSentNs>lower))lower=row.controllerSentNs;if(row.remoteReceivedTicks>at&&(upper===undefined||row.controllerReceivedNs<upper))upper=row.controllerReceivedNs;}
  if(lower===undefined||upper===undefined)return unknown('timestamp_not_bracketed');if(lower>upper)return unknown('clock_order_invalid');
  return Object.freeze({known:true,clockEpoch:epoch,lowerNs:lower.toString(),upperNs:upper.toString(),widthNs:(upper-lower).toString(),scope,formalQualified:false});
 };
 const stopSampling=()=>{sampling=false;if(sampleTimer){clearTimeout(sampleTimer);sampleTimer=null;}};
 const startSampling=({intervalMs=1000}={})=>{
  if(!Number.isSafeInteger(intervalMs)||intervalMs<10||intervalMs>sampleMaxAgeMs/2||!live())throw fail('sampling_configuration');if(sampling)return;sampling=true;
  const next=()=>{if(!sampling||!live())return;sampleTimer=setTimeout(()=>{sampleTimer=null;if(!sampling||!live())return;void sample().catch(()=>{}).finally(next);},intervalMs);};next();
 };
 const resolveDelivery=async event=>{
  const binding=observations.get(event);if(!binding||binding.observer!==api)throw fail('event_not_owned');
  // An already pending sample may precede this event. A second actual round
  // brackets it; equal QPC ticks still remain unknown rather than guessed.
  let range=bounds(event);for(let attempt=0;!range.known&&attempt<2;attempt++){await sample();range=bounds(event);}
  if(!range.known)return null;
  const receipt=Object.freeze({});receipts.set(receipt,Object.freeze({observer:api,event,range}));return receipt;
 };
 const stop=()=>stopPromise??=(async()=>{
  stopSampling();if(closed)throw fail('closed');if(child.exitCode===null&&child.signalCode===null)await new Promise((resolve,reject)=>{
   const done=()=>{clearTimeout(timer);resolve();},timer=setTimeout(()=>{child.off('close',done);unhealthy('stop_timeout');reject(fail('stop_timeout'));},timeoutMs);child.once('close',done);
   try{child.stdin.end('capacity-stop\n');}catch{unhealthy('control_failed');}
  });if(fault||!terminal||child.exitCode!==0)throw fail('stop_incomplete');return {terminal:true,scope,formalQualified:false};
 })();
 const close=()=>closePromise??=(async()=>{
  stopSampling();closed=true;if(pending)unhealthy('closed');child.stdout.off('data',data);child.stdout.off('end',finish);child.stdout.off('error',streamError);child.off('error',streamError);child.off('close',finish);
  if(child.exitCode!==null||child.signalCode!==null)child.stdin.off('error',streamError);else child.once('close',()=>child.stdin.off('error',streamError));await Promise.allSettled([...events]);
 })();
 const closeWithin=async({timeoutMs=5000}={})=>{if(!Number.isSafeInteger(timeoutMs)||timeoutMs<100||timeoutMs>30000)throw fail('configuration');let timer;try{await Promise.race([close(),new Promise((resolve,reject)=>{timer=setTimeout(()=>{unhealthy('sink_drain_timeout');reject(fail('sink_drain_timeout'));},timeoutMs);})]);}finally{clearTimeout(timer);}};
 const api=Object.freeze({sample,bounds,resolveDelivery,startSampling,stop,close,closeWithin,status:()=>{prune();return {scope,formalQualified:false,sourceOpen:live(),closed,ended,terminal,fault,sequence:sequence.toString(),clockEpoch:epoch??null,frequency:frequency?.toString()??null,eventCount,pendingEvents,sampling,sampleTimer:sampleTimer!==null,samples:samples.length,evicted,maxSamples,maxPendingEvents};}});observers.set(api,Object.freeze({runtime,child,isOpen:live,scope,formalQualified:false}));return api;
}
