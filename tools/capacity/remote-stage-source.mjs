import {targets} from './scenarios.mjs';
import {capacityRemoteTransportBinding,capacityRemoteEventReceiptBinding} from './remote-transport.mjs';
import {capacityClockMappingBinding,capacityMappedRemoteEventBinding} from './clock-mapping.mjs';

const sources=new WeakMap(),receipts=new WeakMap();
const fail=code=>{throw Error('capacity_remote_stage_'+code);};
const fields=new Set(['type','atNs','ordinal','contentKind','hash','bytes','outcome','reason']);
export function capacityRemoteStageSourceBinding(source){const row=sources.get(source);return row&&row.isOpen()?row:null;}
export function capacityRemoteStageReceiptBinding(receipt){const row=receipts.get(receipt);return row&&capacityRemoteStageSourceBinding(row.source)&&capacityMappedRemoteEventBinding(row.mappedReceipt)===row.mapped?row:null;}

// Only authenticated transport events can enter this source. The driver still
// owns whole-driver callback count/byte reservations until deliver settles.
export function createCapacityRemoteStageSource({transport,mapping,maxPending=256,maxBytes=1048576,sampleIntervalMs=Math.max(1,Math.floor(targets.relay.p95/2))}={}){
 const owner=capacityRemoteTransportBinding(transport),clockOwner=capacityClockMappingBinding(mapping);
 if(!owner||clockOwner?.transport!==transport||!Number.isSafeInteger(maxPending)||maxPending<1||maxPending>10000||!Number.isSafeInteger(maxBytes)||maxBytes<1024||maxBytes>67108864||!Number.isSafeInteger(sampleIntervalMs)||sampleIntervalMs<1||sampleIntervalMs>1000)fail('configuration');
 let closed=false,pending=0,pendingBytes=0,peakPending=0,peakBytes=0,failures=0,sealedEvents=0;const tails=new Map(),tasks=new Set(),requests=new Set(),seenReceipts=new WeakSet();let sampleTimer=null,sampling=null;
 const isOpen=()=>!closed&&capacityRemoteTransportBinding(transport)===owner&&capacityClockMappingBinding(mapping)===clockOwner;
 const schedule=()=>{if(!isOpen()||!requests.size||sampleTimer)return;sampleTimer=setTimeout(()=>{sampleTimer=null;if(!isOpen()||!requests.size)return;sampling=mapping.sample().catch(()=>{failures++;closed=true;}).finally(()=>{sampling=null;schedule();});},sampleIntervalMs);sampleTimer.unref();};
 const begin=async()=>{if(!isOpen())fail('source_closed');const lease=Object.freeze(Object.create(null));requests.add(lease);try{await mapping.sample();if(!isOpen())fail('source_closed');schedule();return lease;}catch(error){requests.delete(lease);throw error;}};
 const end=lease=>{if(!requests.delete(lease))fail('request_lease');if(!requests.size){clearTimeout(sampleTimer);sampleTimer=null;}};
 const clock=Object.freeze({kind:'linux-hrtime-ns',kernelId:'remote:'+owner.runId+':'+owner.clockEpoch,clockId:'remote:'+owner.session,epoch:owner.clockEpoch});
 const deliver=(value,callback)=>{
  const event=capacityRemoteEventReceiptBinding(value?.receipt),bytes=Buffer.byteLength(JSON.stringify(value));
  if(!isOpen()||!event||seenReceipts.has(value.receipt)||event.transport!==transport||event.transportBinding!==owner||value.clockEpoch!==owner.clockEpoch||value.kind!=='stage'||value.sequence!==event.sequence||Object.keys(event.event).some(key=>!fields.has(key))||JSON.stringify(value.value)!==JSON.stringify(event.event)||typeof callback!=='function'){failures++;return Promise.reject(Error('capacity_remote_stage_event_unproven'));}
  if(pending>=maxPending||pendingBytes+bytes>maxBytes){failures++;return Promise.reject(Error('capacity_remote_stage_budget'));}
  seenReceipts.add(value.receipt);pending++;pendingBytes+=bytes;peakPending=Math.max(peakPending,pending);peakBytes=Math.max(peakBytes,pendingBytes);
  // Start sealing immediately; waiting for another trace's sink must never
  // allow an early stream event's preceding sample to age out first.
  const sealed=mapping.seal(value.receipt);sealed.catch(()=>{});const previous=tails.get(event.requestId)??Promise.resolve();
  const task=(async()=>{try{const mappedReceipt=await sealed;await previous;if(!isOpen())fail('source_closed');const mapped=capacityMappedRemoteEventBinding(mappedReceipt);if(!mapped||mapped.transportEvent!==event)fail('event_unproven');sealedEvents++;const receipt=Object.freeze(Object.create(null));receipts.set(receipt,Object.freeze({source,mappedReceipt,mapped,event:event.event,trace:event.trace,requestId:event.requestId,sequence:event.sequence,range:mapped.range,runId:owner.runId,session:owner.session,clockEpoch:owner.clockEpoch}));return await callback(Object.freeze({...event.event,trace:event.trace,receipt}));}catch(error){failures++;throw error;}finally{pending--;pendingBytes-=bytes;tasks.delete(task);if(tails.get(event.requestId)===task)tails.delete(event.requestId);}})();
  tails.set(event.requestId,task);tasks.add(task);task.catch(()=>{});return task;
 };
 const source=Object.freeze({role:'client',bound:'upper',endpoint:'remote-http-receive',clock,isOpen,readReceipt:receipt=>{const row=capacityRemoteStageReceiptBinding(receipt);return row?.source===source?row:null;}});
 sources.set(source,Object.freeze({transport,transportBinding:owner,mapping,clock,isOpen,deliver}));
 return Object.freeze({source,deliver,begin,end,status:()=>({closed,open:isOpen(),activeRequests:requests.size,sampleIntervalMs,sampling:!!sampling,pending,pendingBytes,peakPending,peakBytes,maxPending,maxBytes,failures,sealedEvents,scope:'authenticated-remote-event-clock-envelopes',formalQualified:false}),close:async()=>{closed=true;clearTimeout(sampleTimer);sampleTimer=null;await sampling?.catch(()=>{});}});
}
