import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,readdir,lstat,chmod,writeFile,rmdir} from 'node:fs/promises';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import {setTimeout as delay} from 'node:timers/promises';
import {createTarget} from '../tools/capacity/gateway-adapter.mjs';
import {ownedProcessProof} from '../tools/capacity/owned-runtime.mjs';
import {capacityDataReadinessSql,classifyCapacityDataReadiness} from '../tools/capacity/data-readiness.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,ref=value=>createHash('sha256').update(String(value)).digest('hex').slice(0,16);
const until=async(test,reason)=>{const end=Date.now()+10000;for(;;){const value=await test();if(value)return value;assert.ok(Date.now()<end,reason);await delay(10);}};
const recordResult=(target,scene,result)=>target.onResult({scenario:scene.id,kind:'model',event:{actorIndex:0,sequence:0},result:{...Object.fromEntries(['operation','method','status','success','complete','httpEnded','wireSent','startedAt','finishedAt','durationMs','receivedBytes','bodyHash','records','trace','error'].filter(k=>Object.hasOwn(result,k)).map(k=>[k,result[k]])),validatedSuccess:result.success===true}});
test('real upstream completion before actual client cancellation preserves unknown usage and strict normal accounting',{skip:!binDirectory?'requires private PG18':false,timeout:120000},async t=>{
  const storageRoot=await mkdtemp('/tmp/harbor-client-cancel-owned-'),artifact=await mkdtemp('/tmp/harbor-client-cancel-proof-');await chmod(storageRoot,0o700);await chmod(artifact,0o700);let target,stopped=false,ledger;
  const evidence={synthetic:true,formalPassed:false};
  try{
    target=await createTarget({profile:'selfcheck',employeeCount:1,storageRoot,binDirectory,libraryDirectory});target.upstream.setScenario({durationMs:500,firstContentMs:100,chunkIntervalMs:100});
    const seen=new Set(target.events.filter(row=>row.kind==='upstream-dispatch').map(row=>row.trace)),abort=new AbortController(),scene={id:'cancel-after-send',employees:1,payloadBytes:1024,streamMs:500};
    const pending=target.sendModel({actorIndex:0,sequence:0,scene,signal:abort.signal});pending.catch(()=>{});const dispatch=await until(()=>target.events.find(row=>row.kind==='upstream-dispatch'&&!seen.has(row.trace)),'new dispatch missing'),trace=dispatch.trace;
    assert.equal(ownedProcessProof(target.laboratory.runtime,target.gateway.child).role,'gateway');process.kill(target.gateway.child.pid,'SIGSTOP');stopped=true;
    await until(()=>target.events.find(row=>row.kind==='upstream-complete'&&row.trace===trace),'upstream complete missing');abort.abort(Error('synthetic client cancellation'));const result=await pending;assert.equal(result.wireSent,true);assert.equal(result.success,false);assert.equal(result.error,'client_cancelled');await recordResult(target,scene,result);
    process.kill(target.gateway.child.pid,'SIGCONT');stopped=false;await until(()=>target.events.find(row=>row.kind==='gateway-handler-settled'&&row.trace===trace),'handler not settled');
    const gateway=(await readdir(storageRoot)).find(name=>name.startsWith('harbor-capacity-gateway-')),parent=join(storageRoot,gateway,'ledger'),folder=(await readdir(parent)).find(name=>name.startsWith('harbor-capacity-ledger-')),file=join(parent,folder,'evidence.sqlite');assert.equal((await lstat(file)).uid,process.getuid());ledger=new DatabaseSync(file,{readOnly:true});
    const facts=ledger.prepare('SELECT client_success,client_cancelled,response_aborts,settled,settled_usage,terminal FROM traces WHERE trace=?').get(trace),admission=ledger.prepare('SELECT request_id FROM admissions WHERE trace=?').get(trace),terminal=JSON.parse(facts.terminal);assert.equal(terminal.outcome,'ok');assert.equal(terminal.usageKnown,true);assert.equal(facts.client_success,0);assert.equal(facts.client_cancelled,1);assert.equal(facts.response_aborts,1);assert.equal(facts.settled_usage,0);
    const actual=await target.store.get('SELECT status,usage_unknown,input,output,cached FROM harbor.requests WHERE id=$1',admission.request_id),price=await target.store.get('SELECT state,total_atoms::text total_atoms FROM harbor.request_prices WHERE request_id=$1',admission.request_id);assert.equal(actual.status,'cancelled');assert.equal(actual.usage_unknown,1);assert.deepEqual(price,{state:'unmetered',total_atoms:null});
    const cancelled=(await target.collectEvidence()).dataReconciliation;assert.equal(cancelled.passed,null,JSON.stringify(cancelled));assert.deepEqual(cancelled.issues,[]);assert.equal(cancelled.groups.cancelled,1);assert.ok(cancelled.missing.includes('cancelled_usage_not_observed'));
    evidence.cancelled={mapping:{traceRef:ref(trace),requestRef:ref(admission.request_id)},client:{success:result.success,wireSent:result.wireSent,error:result.error},gateway:{settledUsage:facts.settled_usage,responseAborts:facts.response_aborts},upstream:{outcome:terminal.outcome,usageKnown:terminal.usageKnown},actual,price,report:cancelled};
    target.upstream.setScenario({durationMs:40,firstContentMs:10,chunkIntervalMs:10});const success=await target.sendModel({actorIndex:0,sequence:1,scene});assert.equal(success.success,true);await recordResult(target,scene,success);await until(()=>target.events.find(row=>row.kind==='gateway-handler-settled'&&row.trace===success.trace),'normal handler not settled');
    const normalAdmission=ledger.prepare('SELECT request_id FROM admissions WHERE trace=?').get(success.trace);
    await until(async()=>{const row=(await target.store.all(capacityDataReadinessSql,[normalAdmission.request_id]))[0],ready=classifyCapacityDataReadiness({row,finishedAt:success.finishedAt});assert.ok(['pending','ready'].includes(ready.state),JSON.stringify(ready));return ready.state==='ready';},'normal data not ready');
    const normal=(await target.collectEvidence()).dataReconciliation;evidence.normalSnapshot=normal;assert.equal(normal.groupResults.normal.conformance,true,JSON.stringify(normal));assert.equal(normal.groupResults.normal.knownAmounts,1);assert.deepEqual(normal.issues,[]);
    await target.store.run('UPDATE harbor.request_prices SET total_atoms=total_atoms+1 WHERE request_id=$1',normalAdmission.request_id);const corrupted=(await target.collectEvidence()).dataReconciliation;evidence.corruptedSnapshot=corrupted;assert.equal(corrupted.passed,false);assert.ok(corrupted.issues.some(row=>row.code==='amount_mismatch'));evidence.normal={conformance:normal.groupResults.normal,corruptionRejected:corrupted.issues.some(row=>row.code==='amount_mismatch')};
  }finally{
    ledger?.close();if(stopped&&target?.gateway?.child?.exitCode===null)process.kill(target.gateway.child.pid,'SIGCONT');await target?.close();evidence.cleanupComplete=true;await rmdir(storageRoot);await writeFile(join(artifact,'evidence.json'),JSON.stringify(evidence,null,2)+'\n',{mode:0o600});t.diagnostic('Synthetic metadata evidence: '+artifact);
  }
});
