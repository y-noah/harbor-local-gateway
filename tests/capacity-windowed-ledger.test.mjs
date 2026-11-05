import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash,randomBytes,randomUUID} from 'node:crypto';
import {mkdtemp,rm,readdir,readFile,writeFile,lstat,symlink} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createWindowedCapacityLedger} from '../tools/capacity/windowed-ledger.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {quoteReference} from '../pricing.mjs';
import {createTarget} from '../tools/capacity/gateway-adapter.mjs';
import {runCapacitySuite} from '../tools/capacity/runner.mjs';
const hash=value=>createHash('sha256').update(value).digest('hex');
const book={rates:{'gpt-6.1-sol':{input:'2',cached:'0.1',output:'10'}},aliases:[]};
async function fixture(t,options={}){
  const directory=await mkdtemp(join(tmpdir(),'harbor-windowed-test-'));const records=new Map(),fragments=new Map(),key=randomBytes(32);let unavailable=false;
  const store={get:async(sql,id)=>{if(unavailable)throw new Error('database unavailable');if(sql.includes('pricing.state'))return {value:JSON.stringify({active:'synthetic'})};if(sql.includes('persisted_sequence'))return {persisted_sequence:fragments.get(id)?.length??0};throw new Error('unexpected query');},pricingBook:async()=>options.book??book,all:async(sql,id,after)=>{if(unavailable)throw new Error('database unavailable');if(sql.includes('recording_documents'))return (Array.isArray(id)?id:[id]).filter(value=>records.has(value)).map(value=>{const r=records.get(value),now=Date.now();return {...r,ended:now,recording_updated:now,expires_at:now+86400000,turn_state:'known',revision:1,indexed_revision:1,indexed_at:now,source_revision:1,source_sequence:2,persisted_sequence:2,document_state:r.recording_state,document_turn_state:'known',document_expires_at:now+86400000};});if(sql.includes('FROM harbor.requests WHERE id=ANY'))return id.filter(value=>records.has(value)).map(value=>records.get(value));if(sql.includes('LEFT JOIN harbor.request_prices'))return records.has(id)?[records.get(id)]:[];if(sql.includes('conversation_events'))return (fragments.get(id)??[]).filter(row=>row.sequence>after).slice(0,1);throw new Error('unexpected query');}};
  const ledger=await createWindowedCapacityLedger({store,recordingKey:key,directory,maxDiskBytes:32*1024*1024,pageSize:3,...options});t.after(async()=>{await ledger.close();await rm(directory,{recursive:true,force:true});});const repository=new RecordingStore(store,{encryptionKey:key});
  const add=async(index,{scope=ledger,outcome='ok',usageKnown=true,body=true,settlement=true,terminal=true,model='gpt-6.1-sol',startedAt=Date.now(),emptyOutput=false,emptyInput=false,unknownTokenNulls=false,admissionConfirmed=true,outputEvidenceComplete,clientCancelled=false,clientSuccess,wireAbort=false,actualOutcome=outcome,actualUsageKnown=usageKnown,captured=false}={})=>{
    const trace='trace-'+index,id='request-'+index,input=emptyInput?'':'synthetic private prompt '+index,output=emptyOutput?'':'合成输出 '+index,member='member-'+index,keyId='key-'+index,device='device-'+index;
    scope.planInput({trace,input});scope.registerTrace({trace,source:'model',actorIndex:index});ledger.recordEvent({kind:admissionConfirmed?'gateway-admitted':'gateway-admission-unconfirmed',trace,requestId:id,memberId:member,keyId,deviceId:device,startedAt,rawSecret:'NEVER_PERSIST_THIS_SECRET'});ledger.recordEvent({kind:'upstream-dispatch',trace});if(settlement)ledger.recordEvent({kind:'gateway-settled',trace,requestId:id,contextMatches:true,settled:true,usageKnown:actualUsageKnown});
    if(terminal)ledger.recordEvent({kind:'upstream-terminal',trace,outcome,usageKnown,model,inputTokens:!usageKnown&&unknownTokenNulls?null:1000,outputTokens:!usageKnown&&unknownTokenNulls?null:200,cachedTokens:!usageKnown&&unknownTokenNulls?null:500,outputHash:hash(output),outputBytes:Buffer.byteLength(output),outputComplete:body,...(outputEvidenceComplete!==undefined?{outputEvidenceComplete}:{}),raw:'NEVER_PERSIST_THIS_SECRET'});
    scope.claimResult({trace,source:'model',actorIndex:index});await scope.onResult({kind:'model',scenario:outcome==='ok'?'normal-scene':'fault-scene',event:{expectedMemberId:member,expectedKeyId:keyId,expectedDeviceId:device,actorIndex:index},result:{trace,finishedAt:performance.now(),status:outcome==='rejected'?429:200,validatedSuccess:clientSuccess??(outcome==='ok'&&!clientCancelled),error:clientCancelled||outcome==='cancelled'?'client_cancelled':null}});
    records.set(id,{request_id:id,member_id:member,key_id:keyId,device_id:device,status:actualOutcome,usage_unknown:!actualUsageKnown,input:actualUsageKnown?1000:0,output:actualUsageKnown?200:0,cached:actualUsageKnown?500:0,price_state:actualUsageKnown?'priced':'unmetered',total_atoms:actualUsageKnown?String(quoteReference({model,started:startedAt,input:1000,output:200,cached:500,status:'ok'},options.book??book).totalAtoms):null,recording_state:outcome==='ok'?'complete':'partial'});
    fragments.set(id,[['user',input],['assistant',output]].filter(([kind,text])=>kind!=='assistant'||text!=='').map(([kind,text],i)=>({sequence:i+1,source_key:'source-'+i,kind,content_cipher:repository.seal({text,field:kind,mode:'append'},id+':'+(i+1)+':source-'+i)})));
    if(wireAbort){const wireId=randomUUID();ledger.recordEvent({kind:'gateway-request-start',trace,wireId,signature:'POST /v1/responses'});ledger.recordEvent({kind:'gateway-response-abort',trace,wireId,signature:'POST /v1/responses'});}
    if(captured)ledger.recordEvent({kind:'gateway-captured-output',trace,requestId:id,verified:true,fields:output?[{field:hash('assistant'),hash:hash(output),bytes:Buffer.byteLength(output)}]:[]});
    return {trace,id,input,output};
  };
  return {ledger,store,directory,records,fragments,repository,add,setUnavailable:value=>unavailable=value};
}
test('windowed ledger scans all history over tiny pages, scopes precisely, and keeps only hashes on private disk',async t=>{
  const f=await fixture(t),scope=f.ledger.createScope('other');for(let i=0;i<240;i++)await f.add(i,{scope:i%2?scope:f.ledger});
  const all=await f.ledger.collect();assert.equal(all.passed,true,JSON.stringify(all));assert.deepEqual(all.counts,{accepted:240,dispatched:240,persisted:240});assert.equal(all.bounds.maxBatch,3);assert.equal((await scope.collect()).counts.accepted,120);
  assert.equal(f.ledger.readTrace('trace-0').resultSeen,true);assert.equal(f.ledger.registerTrace({trace:'trace-0',source:'model',actorIndex:0}),false);assert.equal((await f.ledger.collect()).passed,false);
  const [root]=await readdir(f.directory),privateRoot=join(f.directory,root);assert.equal((await lstat(privateRoot)).mode&0o777,0o700);const bytes=await readFile(join(privateRoot,'evidence.sqlite'));assert.equal((await lstat(join(privateRoot,'evidence.sqlite'))).mode&0o777,0o600);assert.equal(bytes.includes(Buffer.from('NEVER_PERSIST_THIS_SECRET')),false);assert.equal(bytes.includes(Buffer.from('synthetic private prompt')),false);assert.equal(bytes.includes(Buffer.from('合成输出')),false);
  await f.ledger.close();await f.ledger.close();assert.deepEqual(await readdir(f.directory),[]);
});
test('a witnessed client cancellation after upstream completion preserves genuinely unknown billing and captured output',async t=>{
  const f=await fixture(t),entry=await f.add(701,{clientCancelled:true,wireAbort:true,actualOutcome:'cancelled',actualUsageKnown:false,captured:true});
  const report=await f.ledger.collect();assert.equal(report.passed,null);assert.deepEqual(report.issues,[]);assert.equal(report.groups.cancelled,1);assert.equal(report.groupResults.fault.unknownAmounts,1);assert.ok(report.missing.includes('cancelled_usage_not_observed'));
  f.records.get(entry.id).total_atoms='0';let next=await f.ledger.collect();assert.equal(next.passed,false);assert.ok(next.issues.some(row=>row.code==='cancelled_unknown_accounting_mismatch'));
  f.records.get(entry.id).total_atoms=null;f.records.get(entry.id).status='ok';next=await f.ledger.collect();assert.equal(next.passed,false);assert.ok(next.issues.some(row=>row.code==='cancelled_unknown_accounting_mismatch'));
});
test('known usage on a genuinely cancelled request still verifies original upstream tokens and price',async t=>{
  const f=await fixture(t),entry=await f.add(702,{clientCancelled:true,wireAbort:true,actualOutcome:'cancelled',captured:true});
  assert.equal((await f.ledger.collect()).passed,true);f.records.get(entry.id).input=999;let report=await f.ledger.collect();assert.equal(report.passed,false);assert.ok(report.issues.some(row=>row.code==='amount_mismatch'));
  f.records.get(entry.id).input=1000;f.records.get(entry.id).total_atoms='1';report=await f.ledger.collect();assert.equal(report.passed,false);assert.ok(report.issues.some(row=>row.code==='amount_mismatch'));
});
test('a cancellation flag alone and contradictory successful-client evidence never excuse normal defects',async t=>{
  const a=await fixture(t);await a.add(703,{clientCancelled:true,actualOutcome:'cancelled',actualUsageKnown:false,captured:true});const unproven=await a.ledger.collect();assert.equal(unproven.passed,false);assert.ok(unproven.issues.some(row=>row.code==='usage_known_mismatch'));assert.ok(unproven.missing.includes('client_cancel_wire_unproven'));
  const b=await fixture(t);await b.add(704,{clientCancelled:true,clientSuccess:true,wireAbort:true,actualOutcome:'cancelled',actualUsageKnown:false,captured:true});const conflict=await b.ledger.collect();assert.equal(conflict.passed,false);assert.ok(conflict.issues.some(row=>row.code==='client_outcome_conflict'));assert.ok(conflict.issues.some(row=>row.code==='usage_known_mismatch'));
});
test('cancelled output uses capture provenance while still checking encrypted input and persisted fragments',async t=>{
  const f=await fixture(t),entry=await f.add(705,{clientCancelled:true,wireAbort:true,actualOutcome:'cancelled',actualUsageKnown:false,captured:true});
  const fragment=f.fragments.get(entry.id)[0];fragment.content_cipher=f.repository.seal({text:'wrong input',field:'user',mode:'append'},entry.id+':1:source-0');let report=await f.ledger.collect();assert.equal(report.passed,false);assert.ok(report.issues.some(row=>row.code==='recording_content_mismatch'));
  const g=await fixture(t),other=await g.add(706,{clientCancelled:true,wireAbort:true,actualOutcome:'cancelled',actualUsageKnown:false});report=await g.ledger.collect();assert.equal(report.passed,null);assert.ok(report.missing.includes('cancelled_capture_unproven'));
  const second=g.fragments.get(other.id)[0];second.content_cipher=g.repository.seal({text:'wrong input',field:'user',mode:'append'},other.id+':1:source-0');report=await g.ledger.collect();assert.equal(report.passed,false);assert.ok(report.issues.some(row=>row.code==='recording_content_mismatch'));
});
test('cross-request capture and caller-supplied normal capture cannot replace independent source expectations',async t=>{
  const f=await fixture(t),entry=await f.add(707,{clientCancelled:true,wireAbort:true,actualOutcome:'cancelled',actualUsageKnown:false});f.ledger.recordEvent({kind:'gateway-captured-output',trace:entry.trace,requestId:'another-request',verified:true,fields:[]});let report=await f.ledger.collect();assert.equal(report.passed,false);assert.ok(report.issues.some(row=>row.code==='capture_context_mismatch'));
  const g=await fixture(t),normal=await g.add(708,{captured:true});const row=g.fragments.get(normal.id)[1];row.content_cipher=g.repository.seal({text:'wrong output',field:'assistant',mode:'append'},normal.id+':2:source-1');report=await g.ledger.collect();assert.equal(report.passed,false);assert.ok(report.issues.some(row=>row.code==='recording_content_mismatch'));
});
test('a genuinely captured cancelled prefix is checked exactly instead of against unconsumed upstream suffixes',async t=>{
  const f=await fixture(t),entry=await f.add(709,{clientCancelled:true,wireAbort:true,actualOutcome:'cancelled',actualUsageKnown:false}),prefix=entry.output.slice(0,2),fragment=f.fragments.get(entry.id)[1];
  fragment.content_cipher=f.repository.seal({text:prefix,field:'assistant',mode:'append'},entry.id+':2:source-1');f.ledger.recordEvent({kind:'gateway-captured-output',trace:entry.trace,requestId:entry.id,verified:true,fields:[{field:hash('assistant'),hash:hash(prefix),bytes:Buffer.byteLength(prefix)}]});
  const correct=await f.ledger.collect();assert.equal(correct.passed,null);assert.deepEqual(correct.issues,[]);
  fragment.content_cipher=f.repository.seal({text:'wrong persisted prefix',field:'assistant',mode:'append'},entry.id+':2:source-1');const wrong=await f.ledger.collect();assert.equal(wrong.passed,false);assert.ok(wrong.issues.some(row=>row.code==='recording_content_mismatch'));
});
test('capture DTO storage strips every non-whitelisted property before writing its private ledger',async t=>{
  const f=await fixture(t),entry=await f.add(710,{clientCancelled:true,wireAbort:true,actualOutcome:'cancelled',actualUsageKnown:false});
  assert.equal(f.ledger.recordEvent({kind:'gateway-captured-output',trace:entry.trace,requestId:entry.id,verified:true,fields:[{field:hash('assistant'),hash:hash(entry.output),bytes:Buffer.byteLength(entry.output),text:'NEVER_PERSIST_CAPTURE_TEXT',credential:'NEVER_PERSIST_CAPTURE_CREDENTIAL',extra:{value:'NEVER_PERSIST_CAPTURE_NESTED',large:'x'.repeat(100000)}}]}),true);
  const report=await f.ledger.collect();assert.deepEqual(report.issues,[]);await f.ledger.flush();const [name]=await readdir(f.directory);for(const file of await readdir(join(f.directory,name)))assert.equal((await readFile(join(f.directory,name,file))).includes(Buffer.from('NEVER_PERSIST_CAPTURE_')),false);
});
test('late arrivals, request duplicates and repeated dispatches cannot disappear between collection windows',async t=>{
  const f=await fixture(t);assert.equal((await f.ledger.collect()).passed,null);const first=await f.add(1);assert.equal((await f.ledger.collect()).passed,true);await f.add(2);assert.equal((await f.ledger.collect()).counts.accepted,2);
  f.ledger.recordEvent({kind:'upstream-dispatch',trace:first.trace});let report=await f.ledger.collect();assert.equal(report.passed,false);assert.ok(report.issues.some(row=>row.code==='repeated_model_dispatch'));
  f.ledger.recordEvent({kind:'gateway-admitted',trace:'orphan',requestId:first.id,memberId:'member-1',keyId:'key-1',deviceId:'device-1'});report=await f.ledger.collect();assert.equal(report.passed,false);assert.equal(report.counts.accepted,3);assert.ok(report.issues.some(row=>row.code==='duplicate_admission'));assert.ok(report.missing.includes('missing_client_terminal'));
});
test('failed, rejected, cancelled and unknown usage remain in reconciliation; outage cannot hide an observed defect',async t=>{
  const f=await fixture(t);for(const [index,outcome] of ['ok','failed','rejected','cancelled'].entries())await f.add(index,{outcome});let report=await f.ledger.collect();assert.equal(report.passed,true,JSON.stringify(report));assert.deepEqual(report.groups,{ok:1,rejected:1,cancelled:1,failed:1,unknown:0});
  await f.add(4,{outcome:'cancelled',usageKnown:false,body:false});report=await f.ledger.collect();assert.equal(report.passed,null);assert.equal(report.counts.persisted,5);assert.equal(report.groups.cancelled,2);assert.equal(report.amountsVerified,null);assert.equal(report.groupResults.fault.unknownAmounts,1);
  f.setUnavailable(true);assert.equal((await f.ledger.collect()).passed,null);f.ledger.recordEvent({kind:'upstream-dispatch',trace:'trace-4'});assert.equal((await f.ledger.collect()).passed,false);f.setUnavailable(false);assert.equal((await f.ledger.collect()).passed,false);
});
test('every collection rechecks old atoms, authenticated content, sequence gaps and terminal status',async t=>{
  const f=await fixture(t),entry=await f.add(1),row=f.records.get(entry.id);assert.equal((await f.ledger.collect()).passed,true);
  row.total_atoms=String(BigInt(row.total_atoms)+1n);assert.ok((await f.ledger.collect()).issues.some(row=>row.code==='amount_mismatch'));row.total_atoms=String(BigInt(row.total_atoms)-1n);assert.equal((await f.ledger.collect()).passed,true);
  const fragment=f.fragments.get(entry.id)[0],old=fragment.content_cipher;fragment.content_cipher=f.repository.seal({text:'altered',field:'user',mode:'append'},entry.id+':1:source-0');assert.ok((await f.ledger.collect()).issues.some(row=>row.code==='recording_content_mismatch'));fragment.content_cipher=old;
  fragment.content_cipher=Buffer.from(old);fragment.content_cipher[fragment.content_cipher.length-1]^=1;assert.ok((await f.ledger.collect()).issues.some(row=>row.code==='recording_decryption_failed'));fragment.content_cipher=old;
  fragment.sequence=3;assert.ok((await f.ledger.collect()).issues.some(row=>row.code==='recording_fragment_gap'));fragment.sequence=1;row.status='failed';assert.ok((await f.ledger.collect()).issues.some(row=>row.code==='client_database_status_mismatch'));row.status='ok';assert.equal((await f.ledger.collect()).passed,true);
});
test('explicit disk ceiling, write after close and unsafe directory fail closed without throwing into model path',async t=>{
  const f=await fixture(t,{maxDiskBytes:262144,maxIssues:2});for(let i=0;i<500;i++)await f.add(i);const report=await f.ledger.collect();assert.notEqual(report.passed,true);assert.equal(report.incomplete,true);assert.ok(report.clientEvidenceDropped>0);assert.ok(report.storage.bytes<=262144);assert.ok(report.issues.length<=2);
  await f.ledger.close();assert.equal(f.ledger.planInput({trace:'closed',input:'text'}),false);assert.notEqual((await f.ledger.collect()).passed,true);
  const link=join(f.directory,'link');await symlink(tmpdir(),link);await assert.rejects(createWindowedCapacityLedger({store:{all(){},get(){}},recordingKey:randomBytes(32),directory:link,maxDiskBytes:262144}),/directory_not_private/);
});
test('unknown outcome stays incomplete and settlement evidence is independently mandatory',async t=>{
  const f=await fixture(t);await f.add(1,{outcome:'unknown'});let report=await f.ledger.collect();assert.equal(report.passed,null);assert.ok(report.missing.includes('upstream_outcome_unknown'));
  f.ledger.recordEvent({kind:'gateway-settled',trace:'trace-1',requestId:'request-1',contextMatches:false,settled:true,usageKnown:true});report=await f.ledger.collect();assert.equal(report.passed,false);assert.ok(report.issues.some(row=>row.code==='duplicate_gateway_settlement'));assert.ok(report.issues.some(row=>row.code==='settlement_context_mismatch'));
});
test('missing successful gateway settlement stays incomplete and a mismatched usage settlement fails',async t=>{
  const f=await fixture(t);const entry=await f.add(1,{settlement:false});let report=await f.ledger.collect();assert.equal(report.passed,null);assert.ok(report.missing.includes('missing_gateway_settlement'));
  f.ledger.recordEvent({kind:'gateway-settled',trace:entry.trace,requestId:entry.id,contextMatches:true,settled:true,usageKnown:false});report=await f.ledger.collect();assert.equal(report.passed,false);assert.ok(report.issues.some(row=>row.code==='settlement_usage_mismatch'));
});
test('all settlement associations survive later corrections and contradictions remain failures during database outage',async t=>{
  const f=await fixture(t);const entry=await f.add(1,{settlement:false});f.ledger.recordEvent({kind:'gateway-settled',trace:entry.trace,requestId:'wrong-request',contextMatches:true,settled:false,usageKnown:true});f.ledger.recordEvent({kind:'gateway-settled',trace:entry.trace,requestId:entry.id,contextMatches:true,settled:true,usageKnown:true});let result=await f.ledger.collect();assert.equal(result.passed,false);assert.ok(result.issues.some(row=>row.code==='settlement_context_mismatch'));
  const g=await fixture(t);const second=await g.add(2,{settlement:false});g.ledger.recordEvent({kind:'gateway-settled',trace:second.trace,requestId:second.id,contextMatches:true,settled:true,usageKnown:false});g.setUnavailable(true);result=await g.ledger.collect();assert.equal(result.passed,false);assert.ok(result.issues.some(row=>row.code==='settlement_usage_mismatch'));
});
test('complete and terminal summaries must independently agree before looking at PostgreSQL',async t=>{
  const f=await fixture(t),entry=await f.add(1);f.ledger.recordEvent({kind:'upstream-complete',trace:entry.trace,outputHash:hash('wrong-output'),outputBytes:12});let result=await f.ledger.collect();assert.equal(result.passed,false);assert.ok(result.issues.some(row=>row.code==='upstream_body_evidence_mismatch'));f.setUnavailable(true);result=await f.ledger.collect();assert.equal(result.passed,false);assert.ok(result.issues.some(row=>row.code==='upstream_body_evidence_mismatch'));
});
test('rebind cannot delete any early or delayed terminal evidence',async t=>{
  const f=await fixture(t);for(const [i,event] of [{kind:'upstream-complete',outputHash:hash('output'),outputBytes:6},{kind:'upstream-cancel'},{kind:'upstream-terminal',outcome:'unknown',usageKnown:false},{kind:'gateway-settled',requestId:'request-1',contextMatches:true,settled:false,usageKnown:false}].entries()){
    const trace='early-'+i;f.ledger.planInput({trace,input:'input'});f.ledger.recordEvent({...event,trace});f.ledger.rebindTrace(trace,'new-'+i);assert.ok(f.ledger.readTrace(trace));assert.ok((await f.ledger.collect()).issues.some(row=>row.code==='trace_rebind_invalid'));
  }
});
test('disk-backed readiness uses each real finish deadline and classifies failed clients outside success',async t=>{
  const f=await fixture(t);await f.add(1);await f.add(2,{outcome:'cancelled'});await f.add(3,{outcome:'failed'});let result=await f.ledger.waitForDataReadiness();assert.equal(result.passed,true);assert.equal(result.counts.ready,1);assert.equal(result.counts.cancelled,1);assert.equal(result.counts.failedClients,1);assert.equal(result.counts.pending,0);
  const cancelled=new AbortController();cancelled.abort();await assert.rejects(f.ledger.waitForDataReadiness({signal:cancelled.signal}),error=>error.name==='AbortError');
  const trace='unknown-finish';f.ledger.planInput({trace,input:'input'});f.ledger.recordEvent({kind:'upstream-dispatch',trace});f.ledger.recordEvent({kind:'gateway-admitted',trace,requestId:'unknown-request',memberId:'member',keyId:'key',deviceId:'device'});await f.ledger.onResult({kind:'model',scenario:'normal',event:{expectedMemberId:'member',expectedKeyId:'key',expectedDeviceId:'device'},result:{trace,status:200,validatedSuccess:true}});result=await f.ledger.waitForDataReadiness();assert.equal(result.passed,null);assert.equal(result.counts.unknownFinishedAt,1);
});
test('known fault conformance preserves unknown amounts while normal unknown usage and fabricated zero never pass',async t=>{
  const f=await fixture(t);await f.add(1,{outcome:'failed',usageKnown:false});let report=await f.ledger.collect();assert.equal(report.passed,true,JSON.stringify(report));assert.equal(report.groupResults.fault.conformance,true);assert.equal(report.groupResults.fault.amountsVerified,null);assert.equal(report.amountsVerified,null);assert.equal(report.groupResults.fault.unknownAmounts,1);assert.deepEqual(report.groupResults.fault.unknownReasons,['source_usage_unknown']);
  f.records.get('request-1').total_atoms='0';report=await f.ledger.collect();assert.equal(report.passed,false);assert.equal(report.amountsVerified,false);assert.ok(report.issues.some(row=>row.code==='unknown_accounting_fabricated'));
  const g=await fixture(t);await g.add(2,{usageKnown:false});report=await g.ledger.collect();assert.equal(report.passed,null);assert.equal(report.groupResults.normal.conformance,null);assert.ok(report.missing.includes('usage_source_unknown'));
  const h=await fixture(t);await h.add(3,{outcome:'failed',usageKnown:false,terminal:false});report=await h.ledger.collect();assert.equal(report.passed,null);assert.ok(report.missing.includes('missing_upstream_terminal'));assert.equal(report.groupResults.fault.requests,0);
});
test('a failed evidence write while a PG page is awaited cannot return complete success',async t=>{
  const f=await fixture(t);await f.add(1);const original=f.store.all;let entered,release;const enteredPromise=new Promise(resolve=>entered=resolve),gate=new Promise(resolve=>release=resolve);let first=true;f.store.all=async(...args)=>{if(first&&args[0].includes('LEFT JOIN harbor.request_prices')){first=false;entered();await gate;}return original(...args);};
  const collecting=f.ledger.collect();await enteredPromise;const [privateName]=await readdir(f.directory),unexpected=join(f.directory,privateName,'unexpected-io-obstruction');await writeFile(unexpected,'synthetic');assert.equal(f.ledger.recordEvent({kind:'upstream-dispatch',trace:'late'}),false);await rm(unexpected);release();const report=await collecting;assert.equal(report.passed,null);assert.equal(report.incomplete,true);assert.ok(report.clientEvidenceDropped>0);assert.ok(report.missing.includes('ledger_storage_unavailable_or_budget'));
});
test('a scope cannot take ownership of an already planned trace belonging to another scope',async t=>{
  const f=await fixture(t),first=f.ledger.createScope('scope-a'),second=f.ledger.createScope('scope-b');first.planInput({trace:'planned',input:'synthetic'});assert.equal(second.registerTrace({trace:'planned'}),false);assert.equal(f.ledger.readTrace('planned').scope,'scope-a');assert.equal((await first.collect()).passed,false);
});
test('unadmitted fault cannot hide missing source and unknown amount counts survive database failure',async t=>{
  const f=await fixture(t);const trace='unadmitted';await f.ledger.onResult({kind:'model',scenario:'fault-scene',event:{expectedMemberId:'member',expectedKeyId:'key',expectedDeviceId:'device'},result:{trace,status:429,validatedSuccess:false}});f.ledger.recordEvent({kind:'upstream-terminal',trace,outcome:'failed',usageKnown:false,outputComplete:false});let report=await f.ledger.collect();assert.equal(report.passed,null);assert.equal(report.groupResults.fault.conformance,null);assert.equal(report.groupResults.fault.unknownAmounts,1);assert.ok(report.missing.includes('missing_admission_source_evidence'));
  const g=await fixture(t);await g.add(1,{outcome:'failed',usageKnown:false});g.setUnavailable(true);report=await g.ledger.collect();assert.equal(report.passed,null);assert.equal(report.groupResults.fault.unknownAmounts,1);assert.deepEqual(report.groupResults.fault.unknownReasons,['source_usage_unknown']);g.setUnavailable(false);g.records.delete('request-1');report=await g.ledger.collect();assert.equal(report.passed,false);assert.equal(report.groupResults.fault.unknownAmounts,1);
});
test('historical pricing aliases use independently captured admission time across effective boundaries',async t=>{
  const aliasBook={rates:{low:{input:'2',cached:'0.1',output:'10'},high:{input:'10',cached:'0.5',output:'50'}},aliases:[{name:'dynamic-model',target:'low',from:1000,to:2000},{name:'dynamic-model',target:'high',from:2000,to:null}]};
  const f=await fixture(t,{book:aliasBook});await f.add(1,{model:'dynamic-model',startedAt:1500});await f.add(2,{model:'dynamic-model',startedAt:2500});let report=await f.ledger.collect();assert.equal(report.passed,true,JSON.stringify(report));assert.equal(report.amountsVerified,true);assert.notEqual(f.records.get('request-1').total_atoms,f.records.get('request-2').total_atoms);
  const g=await fixture(t,{book:aliasBook});await g.add(3,{model:'dynamic-model',startedAt:null});report=await g.ledger.collect();assert.equal(report.passed,null);assert.ok(report.missing.includes('missing_pricing_time_evidence'));
});
test('health distinguishes persistent completeness loss from availability and confirmed conflicts without leaking errors',async t=>{
  const f=await fixture(t);assert.deepEqual(f.ledger.health(),{available:true,incomplete:false,writeFailures:0,confirmedConflicts:0,reasonCodes:[]});f.ledger.registerTrace({trace:'duplicate'});f.ledger.registerTrace({trace:'duplicate'});assert.equal(f.ledger.health().confirmedConflicts,1);assert.equal(f.ledger.health().available,true);assert.equal(f.ledger.health().incomplete,false);
  const [privateName]=await readdir(f.directory),obstruction=join(f.directory,privateName,'unexpected');await writeFile(obstruction,'synthetic');assert.equal(f.ledger.health().available,false);assert.equal(f.ledger.recordEvent({kind:'upstream-dispatch',trace:'late'}),false);await rm(obstruction);const health=f.ledger.health();assert.equal(health.available,true);assert.equal(health.incomplete,true);assert.ok(health.writeFailures);assert.equal(health.confirmedConflicts,1);assert.ok(health.reasonCodes.every(code=>/^[a-z_]+$/.test(code)));
});
test('readiness query abort always throws safe AbortError even if a ready row was returned',async t=>{
  const f=await fixture(t);await f.add(1);const original=f.store.all,controller=new AbortController();f.store.all=async(...args)=>{const result=await original(...args);if(args[0].includes('recording_documents'))controller.abort(new Error('private raw reason'));return result;};await assert.rejects(f.ledger.waitForDataReadiness({signal:controller.signal}),error=>error.name==='AbortError'&&!error.message.includes('private'));
});
test('never-sent trace tombstones remain durable and any later real wire is a contradiction',async t=>{
  const f=await fixture(t),scope=f.ledger.createScope('browser');assert.equal(scope.registerTrace({trace:'never-sent',source:'baseline',actorIndex:0}),true);assert.equal(scope.markNotSent('never-sent'),true);assert.equal(scope.readTrace('never-sent').notSent,true);f.ledger.recordEvent({kind:'gateway-request-start',trace:'never-sent'});assert.equal((await f.ledger.collect()).passed,false);assert.ok((await f.ledger.collect()).issues.some(row=>row.code==='not_sent_wire_contradiction'));
  assert.equal(scope.registerTrace({trace:'already-sent',source:'baseline',actorIndex:0}),true);f.ledger.recordEvent({kind:'gateway-response-end',trace:'already-sent'});assert.equal(scope.markNotSent('already-sent'),false);assert.equal(scope.readTrace('already-sent').notSent,false);
});
test('confirmed conflicts arriving during a PG wait retain failure priority and bounded details',async t=>{
  const f=await fixture(t);await f.add(1);const browser=f.ledger.createScope('browser');browser.registerTrace({trace:'not-sent-during-collect',source:'baseline'});browser.markNotSent('not-sent-during-collect');const original=f.store.all;let entered,release;const enteredPromise=new Promise(resolve=>entered=resolve),gate=new Promise(resolve=>release=resolve);let first=true;f.store.all=async(...args)=>{if(first&&args[0].includes('LEFT JOIN harbor.request_prices')){first=false;entered();await gate;}return original(...args);};
  const collecting=f.ledger.collect();await enteredPromise;f.ledger.recordEvent({kind:'gateway-request-start',trace:'not-sent-during-collect'});release();const report=await collecting;assert.equal(report.passed,false);assert.equal(report.incomplete,true);assert.ok(report.issues.some(row=>row.code==='not_sent_wire_contradiction'));assert.ok(report.issueCount>0);
});
test('a proved rejected output with zero emitted text may have no assistant fragment',async t=>{
  const f=await fixture(t);await f.add(1,{outcome:'failed',usageKnown:false,emptyOutput:true});const report=await f.ledger.collect();assert.equal(report.passed,true,JSON.stringify(report));assert.equal(report.groupResults.fault.conformance,true);assert.equal(report.groupResults.fault.amountsVerified,null);assert.equal(f.fragments.get('request-1').length,1);
});
test('empty user input still requires its own authenticated user field',async t=>{
  const f=await fixture(t),entry=await f.add(1,{emptyInput:true});assert.equal((await f.ledger.collect()).passed,true);f.fragments.set(entry.id,[{sequence:1,source_key:'source-1',kind:'assistant',content_cipher:f.repository.seal({text:entry.output,field:'assistant',mode:'append'},entry.id+':1:source-1')}]);const report=await f.ledger.collect();assert.equal(report.passed,false);assert.ok(report.issues.some(row=>row.code==='recording_content_mismatch'));assert.equal(report.issues.some(row=>row.code==='recording_fragment_gap'),false);
});
test('explicit null usage facts from a real terminal remain unknown rather than invalid or zero',async t=>{
  const f=await fixture(t);await f.add(1,{outcome:'failed',usageKnown:false,unknownTokenNulls:true});const report=await f.ledger.collect();assert.equal(report.passed,true,JSON.stringify(report));assert.equal(report.groupResults.fault.unknownAmounts,1);assert.equal(report.amountsVerified,null);assert.equal(report.missing.includes('invalid_terminal_evidence'),false);
});
test('confirmed settlement contradictions fail even when admission and database evidence are missing',async t=>{
  const f=await fixture(t);f.setUnavailable(true);for(const [trace,events] of [['context-false',[{requestId:'request-1',contextMatches:false,settled:false}]],['different-requests',[{requestId:'request-1',contextMatches:true,settled:false},{requestId:'request-2',contextMatches:true,settled:true}]],['twice-settled',[{requestId:'request-1',contextMatches:true,settled:true},{requestId:'request-1',contextMatches:true,settled:true}]]]){
    for(const event of events)f.ledger.recordEvent({kind:'gateway-settled',trace,usageKnown:false,...event});
  }
  const result=await f.ledger.collect();assert.equal(result.passed,false);assert.equal(result.incomplete,true);assert.ok(result.issues.some(row=>row.code==='duplicate_gateway_settlement'));assert.ok(result.issues.some(row=>row.code==='settlement_context_mismatch'));
});
test('persistent trace source vocabulary matches the observation adapter browser and auxiliary classes',async t=>{
  const f=await fixture(t),scope=f.ledger.createScope('browser-vocabulary');assert.equal(scope.registerTrace({trace:'browser-trace',source:'browser'}),true);assert.equal(scope.registerTrace({trace:'auxiliary-trace',source:'auxiliary'}),true);assert.equal(scope.readTrace('browser-trace').source,'browser');assert.equal(scope.readTrace('auxiliary-trace').source,'auxiliary');assert.equal(scope.health().incomplete,false);
});
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('real private PostgreSQL request, price and authenticated recording reconcile through the disk-backed ledger',{skip:!binDirectory?'需独立私有PG工具路径':false,timeout:120000},async()=>{
  let owned,ledger;try{
    owned=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:1});ledger=await createWindowedCapacityLedger({store:owned.store,recordingKey:owned.recordingKey,maxDiskBytes:8*1024*1024,pageSize:1});const results=[],eventStart=owned.events.length;
    const scene={id:'windowed-actual',durationMs:250,modelQps:4,adminQps:4,employees:1,administrators:1,upstreamMs:[30,40],payloadBytes:1024};await runCapacitySuite({plan:{...owned.plan,scenarios:[scene]},target:{...owned,runQualificationProbes:undefined,onResult:async row=>{await owned.onResult(row);if(row.kind==='model')results.push(row);}},preflight:owned.preflight});
    for(const event of owned.events.slice(eventStart))if(results.some(row=>row.result.trace===event.trace))ledger.recordEvent(event);
    for(const row of results){const trace=row.result.trace,output=owned.events.find(event=>event.kind==='upstream-complete'&&event.trace===trace);ledger.planInput({trace,input:'容量合成请求 '+'.'.repeat(896)});ledger.registerTrace({trace,source:'model',actorIndex:row.event.actorIndex});await ledger.onResult(row);if(!owned.events.slice(eventStart).some(event=>event.kind==='upstream-terminal'&&event.trace===trace))ledger.recordEvent({kind:'upstream-terminal',trace,outcome:'ok',usageKnown:true,model:row.event.sequence%2?'gpt-6-astra':'gpt-6.1-sol',inputTokens:1000,outputTokens:200,cachedTokens:500,outputHash:output.outputHash,outputBytes:output.outputBytes,outputComplete:true});}
    const readiness=await ledger.waitForDataReadiness();assert.equal(readiness.passed,true,JSON.stringify(readiness));const report=await ledger.collect();assert.equal(report.passed,true,JSON.stringify(report));assert.deepEqual(report.counts,{accepted:1,dispatched:1,persisted:1});const admitted=owned.events.find(event=>event.kind==='gateway-admitted'&&event.trace===results[0].result.trace);assert.ok(Number.isFinite(admitted.startedAt));assert.equal((await owned.store.get('SELECT started FROM harbor.requests WHERE id=$1',admitted.requestId)).started,admitted.startedAt);
    await owned.store.run('UPDATE harbor.request_prices SET total_atoms=total_atoms+1 WHERE request_id=$1',admitted.requestId);assert.ok((await ledger.collect()).issues.some(row=>row.code==='amount_mismatch'));
  }finally{await ledger?.close();await owned?.close();}
});


test('unconfirmed admission candidates remain unknown with or without their current database row',async t=>{
  const f=await fixture(t),entry=await f.add(31,{admissionConfirmed:false});let report=await f.ledger.collect();assert.equal(report.passed,null);assert.equal(report.counts.accepted,0);assert.equal(report.counts.persisted,1);assert.ok(report.missing.includes('admission_commit_unconfirmed'));assert.equal(report.issues.some(row=>row.code==='missing_persisted_request'),false);assert.equal(f.ledger.sourceIntegrity().unconfirmed,1);assert.equal(f.ledger.readTraceEvidence(entry.trace).admissions[0].confirmed,0);
  f.records.delete(entry.id);report=await f.ledger.collect();assert.equal(report.passed,null);assert.ok(report.missing.includes('unconfirmed_candidate_not_found'));assert.equal(report.issues.some(row=>row.code==='missing_persisted_request'),false);
});

test('candidate identity contradictions are confirmed failures and a real commit receipt upgrades the candidate',async t=>{
  const f=await fixture(t),entry=await f.add(32,{admissionConfirmed:false}),row=f.records.get(entry.id);row.member_id='wrong-member';assert.equal((await f.ledger.collect()).passed,false);row.member_id='member-32';const candidate=f.ledger.readTraceEvidence(entry.trace).admissions[0];f.ledger.recordEvent({kind:'gateway-admitted',trace:entry.trace,requestId:entry.id,memberId:candidate.member_id,keyId:candidate.key_id,deviceId:candidate.device_id,startedAt:candidate.started_at});const report=await f.ledger.collect();assert.equal(report.passed,true,JSON.stringify(report));assert.equal(report.counts.accepted,1);assert.equal(f.ledger.sourceIntegrity().unconfirmed,0);assert.equal(f.ledger.sourceIntegrity().unconfirmedEvents,1);
});

test('failed responses can seal independent zero output evidence without pretending response completion',async t=>{
  const f=await fixture(t);await f.add(33,{outcome:'rejected',usageKnown:false,body:false,emptyOutput:true,unknownTokenNulls:true,outputEvidenceComplete:true});const report=await f.ledger.collect();assert.equal(report.groupResults.fault.conformance,true,JSON.stringify(report));assert.equal(report.amountsVerified,null);assert.equal(report.groupResults.fault.unknownAmounts,1);assert.equal(report.missing.includes('non_success_body_not_independently_verified'),false);
});

test('wire identity ownership and settled trace state survive beyond all live request metadata',async t=>{
  const {randomUUID}=await import('node:crypto'),f=await fixture(t),view=f.ledger.createScope('wire-history'),wireId=randomUUID();view.registerTrace({trace:'wire-one',source:'baseline',actorIndex:0});f.ledger.recordEvent({kind:'gateway-request-start',trace:'wire-one',wireId});f.ledger.recordEvent({kind:'gateway-response-end',trace:'wire-one',wireId});assert.equal(view.markWireSettled('wire-one'),true);assert.equal(view.readTrace('wire-one').wireSettled,true);assert.deepEqual(view.readWireIdentity(wireId),{scope:'wire-history',traceHash:hash('wire-one'),starts:1,terminal:true});view.registerTrace({trace:'wire-two',source:'baseline',actorIndex:0});f.ledger.recordEvent({kind:'gateway-request-start',trace:'wire-two',wireId:wireId.toUpperCase()});assert.ok(view.health().confirmedConflicts>0);assert.equal(view.sourceIntegrity().passed,false);
});

test('source audit preserves actual dispatch totals and only a credential digest',async t=>{
  const f=await fixture(t),view=f.ledger.createScope('auxiliary-proof'),proof=hash('synthetic-credential');view.registerTrace({trace:'auxiliary-dispatch',source:'auxiliary',actorIndex:0});f.ledger.recordEvent({kind:'upstream-dispatch',trace:'auxiliary-dispatch',credentialProof:proof});assert.equal(view.readTraceEvidence('auxiliary-dispatch').credentialProof,proof);assert.equal(view.sourceIntegrity().dispatches,1);assert.equal(view.sourceIntegrity().passed,true);f.ledger.recordEvent({kind:'upstream-dispatch',trace:'unregistered-real-load',credentialProof:'not-a-digest'});const audit=view.sourceIntegrity();assert.equal(audit.dispatches,2);assert.equal(audit.unattributed,1);assert.equal(audit.passed,null);
});


test('promoting an unconfirmed candidate cannot bypass cross-trace duplicate admission checks',async t=>{
  const f=await fixture(t),candidate={requestId:'shared-candidate',memberId:'member',keyId:'key',deviceId:'device',startedAt:Date.now()};f.ledger.registerTrace({trace:'candidate-a',source:'model',actorIndex:0});f.ledger.registerTrace({trace:'candidate-b',source:'model',actorIndex:1});f.ledger.recordEvent({kind:'gateway-admission-unconfirmed',trace:'candidate-a',...candidate});f.ledger.recordEvent({kind:'gateway-admitted',trace:'candidate-b',...candidate});f.ledger.recordEvent({kind:'gateway-admitted',trace:'candidate-a',...candidate});assert.ok(f.ledger.health().confirmedConflicts>0);f.setUnavailable(true);assert.equal((await f.ledger.collect()).passed,false);assert.equal(f.ledger.sourceIntegrity().passed,false);
});

test('every unconfirmed candidate is checked across pages while missing rows remain unknown',async t=>{
  const f=await fixture(t,{pageSize:1}),entry=await f.add(41,{admissionConfirmed:false});for(let index=0;index<3;index++){const requestId='additional-'+index;f.ledger.recordEvent({kind:'gateway-admission-unconfirmed',trace:entry.trace,requestId,memberId:'member-41',keyId:'key-41',deviceId:'device-41',startedAt:Date.now()});if(index===2)f.records.set(requestId,{request_id:requestId,member_id:'different-member',key_id:'key-41',device_id:'device-41'});}const report=await f.ledger.collect();assert.equal(report.passed,false);assert.equal(report.candidateFacts.audited,4);assert.equal(report.candidateFacts.rowsMissing,2);assert.equal(report.candidateFacts.rowsObserved,2);assert.ok(report.issues.some(row=>row.code==='identity_mismatch'));assert.ok(report.missing.includes('unconfirmed_candidate_not_found'));assert.equal(report.bounds.maxBatch,1);
});

test('missing planned evidence after IO loss is unknown while known target conflicts remain false',async t=>{
 const f=await fixture(t),[name]=await readdir(f.directory),obstruction=join(f.directory,name,'synthetic-obstruction');await writeFile(obstruction,'synthetic');assert.equal(f.ledger.planInput({trace:'missing-input',input:'synthetic'}),false);await rm(obstruction);f.ledger.rebindTrace('missing-input','new-trace');assert.equal(f.ledger.health().confirmedConflicts,0);assert.ok(f.ledger.health().reasonCodes.includes('trace_rebind_source_unavailable'));f.ledger.createScope('other').planInput({trace:'known-other',input:'synthetic'});f.ledger.rebindTrace('missing-input','known-other');assert.ok(f.ledger.health().confirmedConflicts>0);const before=f.ledger.health().confirmedConflicts;f.ledger.recordEvent({kind:'upstream-dispatch',trace:'known-active'});f.ledger.rebindTrace('missing-input','known-active');assert.ok(f.ledger.health().confirmedConflicts>before);assert.equal((await f.ledger.collect()).passed,false);
});
