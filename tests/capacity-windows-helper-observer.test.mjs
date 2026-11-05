import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
import {observeCapacityWindowsHelper,capacityWindowsHelperObserverBinding} from '../tools/capacity/windows-helper-observer.mjs';

async function until(check){const deadline=performance.now()+3000;while(!check()){assert.ok(performance.now()<deadline,'helper observer condition timeout');await delay(5);}}
// A synthetic process exercises channel failure semantics only. It does not
// stand in for the separate actual Windows/CNG/WSL qualification.
const script=`
const {randomUUID}=require('node:crypto'),{createInterface}=require('node:readline');
let sequence=0,lastTick=0n;const epoch=randomUUID(),flat=process.argv[1]==='flat';
const encode=row=>JSON.stringify({capacity_delivery:'helper-socket-write-complete-v1',clockEpoch:epoch,sequence:String(sequence),frequency:'1000000000',dropped:0,fault:false,formalQualified:false,...row})+'\\n',output=row=>process.stdout.write(encode(row));
const content=(trace,at=process.hrtime.bigint())=>({type:'content',trace,atTicks:String(at),ordinal:0,contentKind:'text',hash:'a'.repeat(64),bytes:1});
process.stdout.write(JSON.stringify({state:'running',local_port:1,key_protection:'synthetic',hardware:'synthetic'})+'\\n');
createInterface({input:process.stdin}).on('line',line=>{
 if(line.startsWith('capacity-clock ')){const received=flat?100n:process.hrtime.bigint();lastTick=flat?100n:process.hrtime.bigint();sequence++;output({type:'clock',requestId:line.slice(15),receivedTicks:String(received),sentTicks:String(lastTick)});}
 else if(line==='capacity-stop'){output({type:'status'});process.exitCode=0;process.stdin.destroy();}
 else if(line.startsWith('emit ')){sequence++;output(content(line.slice(5)));}
 else if(line.startsWith('equal ')){sequence++;output(content(line.slice(6),lastTick));}
 else if(line.startsWith('pair ')){sequence++;const first=encode(content(line.slice(5)));sequence++;process.stdout.write(first+encode(content(line.slice(5))));}
 else if(line==='gap'){sequence+=2;output({type:'unavailable',trace:randomUUID(),atTicks:String(process.hrtime.bigint())});}
 else if(line==='drop'){sequence++;output({type:'unavailable',trace:randomUUID(),atTicks:String(process.hrtime.bigint()),dropped:1,fault:true});}
 else if(line==='oversize')process.stdout.write('x'.repeat(8193));
 else if(line==='truncated')process.stdout.end('{');
 else if(line==='no-status'){process.stdin.destroy();process.exitCode=0;}
});`;
async function fixture(operation,options={},mode='normal'){
 const runtime=createOwnedRuntime();let observer;try{const child=runtime.start(process.execPath,['-e',script,mode],{role:'helper',stdio:['pipe','pipe','pipe']});observer=observeCapacityWindowsHelper({runtime,child,...options});await operation({child,observer});}finally{await runtime.close();await observer?.close();}
}
test('owned helper events require two real channel clock samples and cannot be copied',{timeout:10000},async()=>{
 const rows=[];await fixture(async({child,observer})=>{
  assert.equal(capacityWindowsHelperObserverBinding({...observer}),null);assert.strictEqual(capacityWindowsHelperObserverBinding(observer).child,child);
  await observer.sample();const sent=process.hrtime.bigint();child.stdin.write('emit '+randomUUID()+'\n');await until(()=>rows.length===1);const received=process.hrtime.bigint();
  assert.equal(observer.bounds(rows[0]).known,false);await observer.sample();const bounds=observer.bounds(rows[0]);assert.equal(bounds.known,true);assert.ok(BigInt(bounds.lowerNs)<=sent);assert.ok(BigInt(bounds.upperNs)>=received);assert.equal(bounds.formalQualified,false);
  assert.throws(()=>observer.bounds({...rows[0]}),/event_not_owned/);
  for(let i=0;i<4;i++)await observer.sample();assert.equal(observer.status().samples,2);assert.ok(observer.status().evicted>=4);assert.equal(observer.bounds(rows[0]).known,false);
  assert.equal((await observer.stop()).terminal,true);assert.equal(observer.status().fault,null);assert.equal(observer.status().terminal,true);assert.equal(observer.bounds(rows[0]).known,false);assert.equal(capacityWindowsHelperObserverBinding(observer),null);
 },{onEvent:row=>rows.push(row),maxSamples:2});
});
test('helper corruption and missing output remain explicit measurement gaps',{timeout:20000},async t=>{
 for(const [command,expected] of [['gap','sequence_invalid'],['drop','source_incomplete'],['oversize','line_budget'],['truncated','terminal_missing'],['no-status','terminal_missing']])await t.test(command,async()=>fixture(async({child,observer})=>{
  await observer.sample();child.stdin.write(command+'\n');await until(()=>observer.status().fault!==null);assert.equal(observer.status().fault,expected);assert.equal(observer.status().sourceOpen,false);await assert.rejects(observer.sample(),/closed/);
 }));
});
test('bounded asynchronous sink reports loss and close waits for its actual completion',{timeout:10000},async()=>{
 let release,entered=false,closed=false;const held=new Promise(resolve=>{release=resolve;});await fixture(async({child,observer})=>{
  try{await observer.sample();child.stdin.write('emit '+randomUUID()+'\n');await until(()=>entered);child.stdin.write('emit '+randomUUID()+'\n');await until(()=>observer.status().fault==='sink_budget');const stopping=observer.close().then(()=>{closed=true;});await delay(10);assert.equal(closed,false);release();await stopping;assert.equal(closed,true);assert.equal(observer.status().pendingEvents,0);}finally{release();}
 },{maxPendingEvents:1,onEvent:()=>{entered=true;return held;}});
});
test('a close reentered by the sink waits for that same accepted event',{timeout:10000},async()=>{
 let observer,release,closing,closed=false;const held=new Promise(resolve=>{release=resolve;});await fixture(async({child,observer:source})=>{
  observer=source;try{await source.sample();child.stdin.write('emit '+randomUUID()+'\n');await until(()=>closing);await delay(10);assert.equal(closed,false);assert.equal(source.status().pendingEvents,1);release();await closing;assert.equal(closed,true);assert.equal(source.status().pendingEvents,0);}finally{release();}
 },{onEvent:()=>{closing=observer.close().then(()=>{closed=true;});return held;}});
});
test('reentrant close stops admitting later lines in the same actual stdout chunk',{timeout:10000},async()=>{
 let observer,release,closing,calls=0,paired=false,closed=false;const held=new Promise(resolve=>{release=resolve;});await fixture(async({child,observer:source})=>{
  observer=source;try{await source.sample();child.stdout.on('data',chunk=>{if((chunk.toString().match(/"type":"content"/g)??[]).length===2)paired=true;});child.stdin.write('pair '+randomUUID()+'\n');await until(()=>closing);assert.equal(paired,true);assert.equal(calls,1);await delay(10);assert.equal(closed,false);release();await closing;assert.equal(source.status().pendingEvents,0);assert.equal(calls,1);}finally{release();}
 },{onEvent:()=>{calls++;closing=observer.close().then(()=>{closed=true;});return held;}});
});
test('equal QPC ticks do not prove causal order even with two flat clock samples',{timeout:10000},async t=>{
 for(const mode of ['normal','flat'])await t.test(mode,async()=>{const rows=[];await fixture(async({child,observer})=>{await observer.sample();child.stdin.write('equal '+randomUUID()+'\n');await until(()=>rows.length===1);assert.equal(observer.bounds(rows[0]).known,false);await observer.sample();assert.equal(observer.bounds(rows[0]).known,false);await observer.stop();},{onEvent:row=>rows.push(row)},mode);});
});
test('valid uppercase and mixed UUID traces retain their exact association while invalid traces fail',{timeout:10000},async t=>{
 for(const trace of ['ABCDEFAB-ABCD-4ABC-8ABC-ABCDEFABCDEF','aBcDeFaB-aBcD-4aBc-8AbC-aBcDeFaBcDeF'])await t.test(trace===trace.toUpperCase()?'uppercase':'mixed',async()=>{const rows=[];await fixture(async({child,observer})=>{await observer.sample();child.stdin.write('emit '+trace+'\n');await until(()=>rows.length===1);await observer.sample();assert.equal(rows[0].trace,trace);assert.equal(observer.bounds(rows[0]).known,true);assert.equal(observer.status().fault,null);await observer.stop();},{onEvent:row=>rows.push(row)});});
 await t.test('invalid',async()=>fixture(async({child,observer})=>{await observer.sample();child.stdin.write('emit invalid-trace\n');await until(()=>observer.status().fault!==null);assert.equal(observer.status().fault,'event_invalid');}));
});
test('bounded outer drain reports pending work without resolving the underlying close',{timeout:10000},async()=>{
 let release,entered=false;const held=new Promise(resolve=>{release=resolve;});await fixture(async({child,observer})=>{
  try{await observer.sample();child.stdin.write('emit '+randomUUID()+'\n');await until(()=>entered);await assert.rejects(observer.closeWithin({timeoutMs:100}),/sink_drain_timeout/);assert.equal(observer.status().pendingEvents,1);assert.equal(observer.status().fault,'sink_drain_timeout');release();await observer.close();assert.equal(observer.status().pendingEvents,0);}finally{release();}
 },{onEvent:()=>{entered=true;return held;}});
});
test('clock timeout fails closed without sending another sample',{timeout:5000},async()=>{
 const runtime=createOwnedRuntime();let observer;try{const child=runtime.start(process.execPath,['-e',`process.stdin.resume();`],{role:'helper',stdio:['pipe','pipe','pipe']});observer=observeCapacityWindowsHelper({runtime,child,timeoutMs:100});await assert.rejects(observer.sample(),/clock_timeout/);await assert.rejects(observer.sample(),/closed/);assert.equal(observer.status().fault,'clock_timeout');}finally{await runtime.close();await observer?.close();}
});
test('rejected or throwing measurement sinks are never treated as accepted events',{timeout:10000},async t=>{
 for(const [name,onEvent] of [['false',()=>false],['async-false',async()=>false],['throw',()=>{throw Error('synthetic');}],['then-getter',()=>({get then(){throw Error('synthetic');}})]])await t.test(name,async()=>fixture(async({child,observer})=>{await observer.sample();child.stdin.write('emit '+randomUUID()+'\n');await until(()=>observer.status().fault!==null);assert.equal(observer.status().fault,'sink_failed');},{onEvent}));
});
test('copied runtime or process DTO cannot open a helper measurement channel',()=>{
 assert.throws(()=>observeCapacityWindowsHelper({runtime:{},child:{pid:1}}),/runtime_not_owned/);
});

test('resolved delivery receipt is branded to the actual observer and strict sample envelope',{timeout:10000},async()=>{
 const {capacityWindowsHelperDeliveryReceipt}=await import('../tools/capacity/windows-helper-observer.mjs'),rows=[];await fixture(async({child,observer})=>{
  await observer.sample();child.stdin.write('emit '+randomUUID()+'\n');await until(()=>rows.length===1);const receipt=await observer.resolveDelivery(rows[0]),binding=capacityWindowsHelperDeliveryReceipt(receipt);assert.ok(binding?.range.known);assert.strictEqual(binding.event,rows[0]);assert.strictEqual(binding.observer,observer);assert.equal(capacityWindowsHelperDeliveryReceipt({...receipt}),null);await assert.rejects(observer.resolveDelivery({...rows[0]}),/event_not_owned/);observer.startSampling({intervalMs:10});const before=observer.status().sequence;await until(()=>observer.status().sequence!==before);await observer.stop();assert.equal(observer.status().sampling,false);assert.equal(observer.status().sampleTimer,false);
 },{onEvent:row=>rows.push(row)});
});
