import test from 'node:test';
import assert from 'node:assert/strict';
import {createTarget} from '../tools/capacity/gateway-adapter.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {runCapacitySuite} from '../tools/capacity/runner.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('live runtime ledger independently reconciles exact atoms, identity and body, and detects one-atom corruption',{skip:!binDirectory?'需独立私有PG工具路径':false,timeout:120000},async()=>{
  let owned;try{
    owned=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:1});const scene={id:'ledger-actual',durationMs:250,modelQps:4,adminQps:4,employees:1,administrators:1,upstreamMs:[30,40],payloadBytes:1024},report=await runCapacitySuite({plan:{...owned.plan,scenarios:[scene]},target:{...owned,runQualificationProbes:undefined},preflight:owned.preflight});
    assert.equal(report.evidence.dataReconciliation.passed,true);assert.deepEqual(report.evidence.dataReconciliation.counts,{accepted:1,dispatched:1,persisted:1});assert.notEqual(report.verdict.status,'passed');const accepted=owned.events.find(row=>row.kind==='gateway-admitted'&&row.trace);assert.ok(accepted);
    await owned.store.run('UPDATE harbor.request_prices SET total_atoms=total_atoms+1 WHERE request_id=$1',accepted.requestId);const observed=await owned.collectEvidence();assert.equal(observed.dataReconciliation.passed,false);assert.ok(observed.dataReconciliation.issues.some(row=>row.code==='amount_mismatch'));
    await owned.store.run('UPDATE harbor.request_prices SET total_atoms=total_atoms-1 WHERE request_id=$1',accepted.requestId);assert.equal((await owned.collectEvidence()).dataReconciliation.passed,true);
    const fragments=await owned.store.all('SELECT * FROM harbor.conversation_events WHERE request_id=$1 ORDER BY sequence',accepted.requestId),repository=new RecordingStore(owned.store,{encryptionKey:owned.recordingKey});
    for(const kind of ['user','assistant']){
      const fragment=fragments.find(row=>row.kind===kind);assert.ok(fragment);const aad=accepted.requestId+':'+fragment.sequence+':'+fragment.source_key,content=repository.open(fragment.content_cipher,aad),changed=repository.seal({...content,text:content.text+' synthetic altered'},aad);
      await owned.store.run('UPDATE harbor.conversation_events SET content_cipher=$1 WHERE request_id=$2 AND sequence=$3',changed,accepted.requestId,fragment.sequence);const replaced=await owned.collectEvidence();assert.equal(replaced.dataReconciliation.passed,false);assert.ok(replaced.dataReconciliation.issues.some(row=>row.code==='recording_content_mismatch'));
      await owned.store.run('UPDATE harbor.conversation_events SET content_cipher=$1 WHERE request_id=$2 AND sequence=$3',fragment.content_cipher,accepted.requestId,fragment.sequence);assert.equal((await owned.collectEvidence()).dataReconciliation.passed,true);
    }
    const fragment=fragments[0],corrupt=Buffer.from(fragment.content_cipher);corrupt[corrupt.length-1]^=1;await owned.store.run('UPDATE harbor.conversation_events SET content_cipher=$1 WHERE request_id=$2 AND sequence=$3',corrupt,accepted.requestId,fragment.sequence);assert.ok((await owned.collectEvidence()).dataReconciliation.issues.some(row=>row.code==='recording_decryption_failed'));
    await owned.store.run('UPDATE harbor.conversation_events SET content_cipher=$1 WHERE request_id=$2 AND sequence=$3',fragment.content_cipher,accepted.requestId,fragment.sequence);
    await owned.store.run('DELETE FROM harbor.conversation_events WHERE request_id=$1 AND sequence=$2',accepted.requestId,fragment.sequence);assert.ok((await owned.collectEvidence()).dataReconciliation.issues.some(row=>row.code==='recording_fragment_gap'));
    await owned.store.run('INSERT INTO harbor.conversation_events(request_id,sequence,source_key,kind,provenance,created,content_cipher) VALUES($1,$2,$3,$4,$5,$6,$7)',fragment.request_id,fragment.sequence,fragment.source_key,fragment.kind,fragment.provenance,fragment.created,fragment.content_cipher);assert.equal((await owned.collectEvidence()).dataReconciliation.passed,true);
    await owned.store.run("UPDATE harbor.requests SET status='failed',business_status='failed' WHERE id=$1",accepted.requestId);const wrongStatus=await owned.collectEvidence();assert.equal(wrongStatus.dataReconciliation.passed,false);assert.ok(wrongStatus.dataReconciliation.issues.some(row=>row.code==='client_database_status_mismatch'));
    await owned.store.run("UPDATE harbor.requests SET status='ok',business_status='ok' WHERE id=$1",accepted.requestId);assert.equal((await owned.collectEvidence()).dataReconciliation.passed,true);

  }finally{await owned?.close();}
});
