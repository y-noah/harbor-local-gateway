import {capacityRemoteTransportBinding,capacityRemoteClockSampleBinding,capacityRemoteEventReceiptBinding} from './remote-transport.mjs';

const mappings=new WeakMap(),mappedEvents=new WeakMap(),fail=code=>{throw Error('capacity_clock_'+code);};
const validNs=value=>typeof value==='string'&&/^[0-9]{1,32}$/.test(value);
export function capacityMappedRemoteEventBinding(value){const row=mappedEvents.get(value);return row&&capacityClockMappingBinding(row.mapping)&&capacityRemoteEventReceiptBinding(row.transportReceipt)===row.transportEvent?row:null;}
export function capacityClockMappingBinding(value){const row=mappings.get(value);return row&&row.isOpen()?row:null;}

// This map gives conditional causal bounds for one endpoint clock epoch. It
// neither authenticates an event supplied by a caller nor proves a remote host,
// executable, LAN, frequency/drift bound or a formal measurement qualification.
export function createCapacityClockMapping(configuration={}){
 if(!configuration||typeof configuration!=='object'||Array.isArray(configuration)||Object.keys(configuration).some(key=>!['transport','maxSamples','sampleMaxAgeMs'].includes(key)))fail('configuration');
 const {transport,maxSamples=1024,sampleMaxAgeMs=30000}=configuration;
 const owner=capacityRemoteTransportBinding(transport);
 if(!owner||!Number.isSafeInteger(maxSamples)||maxSamples<2||maxSamples>100000||!Number.isSafeInteger(sampleMaxAgeMs)||sampleMaxAgeMs<100||sampleMaxAgeMs>600000)fail('configuration');
 let closed=false,epoch,pending,closePromise,samples=[],evicted=0;
 const isOpen=()=>!closed&&capacityRemoteTransportBinding(transport)===owner;
 const prune=()=>{if(!isOpen()){samples=[];return;}const cutoff=process.hrtime.bigint()-BigInt(sampleMaxAgeMs)*1000000n;const retained=samples.filter(row=>row.controllerReceivedNs>=cutoff);evicted+=samples.length-retained.length;samples=retained;};
 const sample=()=>{
  if(!isOpen())return Promise.reject(Error('capacity_clock_closed'));if(pending)return pending;
  pending=(async()=>{
   const handle=await transport.clockSample(),row=capacityRemoteClockSampleBinding(handle);
   if(!isOpen()||!row||row.transport!==transport||row.transportBinding!==owner||epoch!==undefined&&epoch!==row.clockEpoch)fail('sample_unproven');
   const previous=samples.at(-1);if(previous&&(row.controllerSentNs<previous.controllerReceivedNs||row.remoteReceivedNs<previous.remoteSentNs))fail('order_unproven');
   epoch=row.clockEpoch;prune();samples.push(row);if(samples.length>maxSamples){evicted+=samples.length-maxSamples;samples=samples.slice(-maxSamples);}
   return Object.freeze({clockEpoch:epoch,samples:samples.length,scope:'authenticated-endpoint-clock-envelope',formalQualified:false});
  })().catch(error=>{closed=true;samples=[];throw error;}).finally(()=>{pending=null;});return pending;
 };
 const bounds=({timestampNs,clockEpoch}={})=>{
  if(!validNs(timestampNs))fail('timestamp');prune();const unknown=reason=>Object.freeze({known:false,reason,scope:'authenticated-endpoint-clock-envelope',eventSourceVerified:false,formalQualified:false});
  if(!isOpen())return unknown('transport_or_mapping_closed');if(epoch===undefined||clockEpoch!==epoch)return unknown('clock_epoch_mismatch');const timestamp=BigInt(timestampNs);let lower,upper;
  // A monotonic clock can repeat a tick. Equality cannot establish which
  // capture came first, even when one exchange could supply both endpoints.
  for(const row of samples){if(row.remoteSentNs<timestamp&&(lower===undefined||row.controllerSentNs>lower))lower=row.controllerSentNs;if(row.remoteReceivedNs>timestamp&&(upper===undefined||row.controllerReceivedNs<upper))upper=row.controllerReceivedNs;}
  if(lower===undefined||upper===undefined)return unknown('timestamp_not_bracketed');if(lower>upper)return unknown('clock_order_unproven');
  return Object.freeze({known:true,clockEpoch:epoch,lowerNs:lower.toString(),upperNs:upper.toString(),widthNs:(upper-lower).toString(),scope:'authenticated-endpoint-clock-envelope',eventSourceVerified:false,formalQualified:false});
 };
 const seal=async receipt=>{
  const event=capacityRemoteEventReceiptBinding(receipt);if(!isOpen()||!event||event.transport!==transport||event.transportBinding!==owner||!validNs(event.event.atNs)||event.clockEpoch!==epoch)fail('event_unproven');
  const timestamp=BigInt(event.event.atNs);prune();let before;for(const row of samples)if(row.remoteSentNs<timestamp&&(!before||row.controllerSentNs>before.controllerSentNs))before=row;
  if(!before)fail('event_lower_unproven');
  // Preserve the preceding genuine sample now, before a long-lived stream can
  // evict it. A shared sample already pending at receipt may precede the event.
  await sample();let after=samples.find(row=>row.remoteReceivedNs>timestamp);if(!after){await sample();after=samples.find(row=>row.remoteReceivedNs>timestamp);}
  if(!isOpen()||capacityRemoteEventReceiptBinding(receipt)!==event||!after||before.controllerSentNs>after.controllerReceivedNs)fail('event_upper_unproven');
  const handle=Object.freeze(Object.create(null)),range=Object.freeze({known:true,lowerNs:before.controllerSentNs.toString(),upperNs:after.controllerReceivedNs.toString(),clockEpoch:epoch,eventSourceVerified:true,formalQualified:false});
  mappedEvents.set(handle,Object.freeze({mapping,transportReceipt:receipt,transportEvent:event,event:event.event,trace:event.trace,range,before,after}));return handle;
 };
 const mapping=Object.freeze({sample,bounds,seal,status:()=>{prune();return {closed,transportOpen:capacityRemoteTransportBinding(transport)===owner,samples:samples.length,evicted,clockEpoch:epoch??null,maxSamples,sampleMaxAgeMs,scope:'authenticated-endpoint-clock-envelope',formalQualified:false};},close:()=>closePromise??=(async()=>{closed=true;samples=[];await pending?.catch(()=>{});})()});
 mappings.set(mapping,Object.freeze({transport,transportBinding:owner,isOpen,scope:'authenticated-endpoint-clock-envelope',formalQualified:false}));return mapping;
}
