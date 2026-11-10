import test from 'node:test';
import assert from 'node:assert/strict';
import {createTarget} from '../tools/capacity/gateway-adapter.mjs';
import {runCapacitySuite} from '../tools/capacity/runner.mjs';
import {RecordingContextStore} from '../recording-context-store.mjs';
import {requestCapacityTarget} from '../tools/capacity/http-client.mjs';
import {setTimeout as delay} from 'node:timers/promises';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
for(const windowsHelper of [false,true])test('owned capacity adapter uses actual PG / gateway / worker / enrollment'+(windowsHelper?' / CNG WSL helper':''),{skip:!binDirectory?'需显式独立 PostgreSQL18 工具路径':windowsHelper&&process.env.HARBOR_TEST_WINDOWS_DEVICE!=='1'?'需显式 Windows 实机开关':false,timeout:180000},async()=>{
  let owned,runAuxiliary,primary;try{
    owned=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:1,windowsHelper,onSetup:options=>{runAuxiliary=options.runAuxiliaryProbe;}});assert.equal(owned.dataset.verified,true);assert.equal(owned.preflight.eligible,false);assert.equal(owned.target.employeeActors[0].realHelper,windowsHelper);assert.equal(owned.target.employeeActors[0].deviceEnforced,true);
    assert.equal((await owned.store.get('SELECT MAX(version) version FROM harbor.schema_migrations')).version,24);
    const scene={id:'actual-gateway',durationMs:200,modelQps:5,adminQps:20,employees:1,administrators:1,upstreamMs:[30,40],payloadBytes:1024};
    const report=await runCapacitySuite({plan:{...owned.plan,scenarios:[scene]},target:owned,preflight:owned.preflight});assert.notEqual(report.verdict.status,'passed');assert.ok(report.metrics.series.filter(row=>row.operation==='model').every(row=>row.successRate===1));assert.equal(report.evidence.actorCoverage.passed,true);assert.equal(report.evidence.qualificationProbes.passed,true);assert.equal(report.evidence.dataReconciliation.passed,true);
    for(const category of ['list','detail','summary','configuration']){const response=await owned.sendAdmin({actor:owned.adminActors[0],category});assert.equal(response.success,true,category);if(response.method!=='GET')assert.equal(response.businessVerified,true);}
    const captured=await owned.store.get("SELECT COUNT(*) n FROM harbor.recordings WHERE request_id IN (SELECT id FROM harbor.requests WHERE device_id IS NOT NULL)");assert.ok(captured.n>=2,'actual runtime recording persisted');
    const concurrent=await Promise.all([0,1,2].map(index=>owned.sendAdmin({actor:owned.adminActors[index],category:'configuration'})));assert.ok(concurrent.every(row=>row.success&&row.businessVerified));
    const devices=await owned.store.get("SELECT COUNT(*) n FROM harbor.employee_devices WHERE state='approved' AND verified_request_at IS NOT NULL");assert.equal(devices.n,1);const live=await owned.store.all("SELECT device_id,status FROM harbor.requests WHERE id NOT IN (SELECT request_id FROM harbor_capacity.body_receipts) AND device_id IS NOT NULL");assert.ok(live.length>=2);assert.ok(live.every(row=>row.device_id===owned.employeeActors[0].deviceId&&row.status==='ok'));
    const snapshots=await owned.store.all('SELECT s.* FROM harbor.recording_context_snapshots s JOIN harbor.requests r ON r.id=s.request_id WHERE r.device_id IS NOT NULL');
    assert.equal(snapshots.length,live.length);assert.ok(snapshots.every(row=>row.state==='complete'),JSON.stringify(snapshots.map(({state,components,reason})=>({state,components,reason}))));
    const measured=snapshots.find(row=>row.components===5);assert.ok(measured,'the measured model request must include instructions, tools and history alongside qualification probes');
    const contexts=new RecordingContextStore(owned.store,{encryptionKey:owned.recordingKey}),view=await contexts.read(measured.request_id);
    assert.equal(view.state,'complete');assert.equal(view.hasMore,false);const text=view.parts.map(part=>part.text).join('');
    assert.match(text,/容量合成指令/);assert.match(text,/此前的合成问题/);assert.match(text,/capacity_probe/);
    const deadline=performance.now()+15000;for(;;){const state=await owned.store.get('SELECT COUNT(*) FILTER(WHERE revision>indexed_revision) pending,COUNT(*) FILTER(WHERE last_error IS NOT NULL) failed FROM harbor.recording_context_work');assert.equal(state.failed,0);if(!state.pending)break;assert.ok(performance.now()<deadline,'real Worker must finish the context indexes');await delay(20);}
    const search=await runAuxiliary(()=>requestCapacityTarget({origin:owned.gateway.ready.origin,path:'/api/admin/recordings/context/search?q=capacity_probe',headers:owned.adminActors[0].headers,mode:'json'}));
    assert.equal(search.success,true);assert.ok(search.json.items.length>0);assert.ok(search.json.items.every(item=>item.association==='shared_context'));
  }catch(error){primary=error;throw error;}finally{try{await owned?.close();}catch(error){if(!primary)throw error;primary.cleanupFailed=true;}}
});

test('dataset-only adapter exposes CLI ownership contract without starting runtime children',{skip:!binDirectory,timeout:60000},async()=>{
  const owned=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',purpose:'verify'});try{assert.ok(owned.datasetStore);assert.ok(owned.datasetManifest);assert.equal(owned.recordingKey.length,32);assert.equal(owned.sendModel,undefined);}finally{await owned.close();}
});

test('multiple concurrent slots per employee use distinct existing threads and reach the controlled upstream together',{skip:!binDirectory,timeout:120000},async()=>{
  let owned;try{
    owned=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:2});const began=owned.events.length,scene={id:'parallel-slots',employees:2,streamMs:300,payloadBytes:1024};await owned.onScenarioStart({scene});
    const results=await Promise.all([0,1,2,3].map(slot=>owned.sendModel({actor:owned.employeeActors[slot%2],actorIndex:slot%2,sequence:slot,slot,scene})));assert.ok(results.every(result=>result.success&&result.complete));const traces=new Set(results.map(result=>result.trace)),admitted=owned.events.slice(began).filter(row=>row.kind==='gateway-admitted'&&traces.has(row.trace));assert.equal(admitted.length,4);
    const sessions=await owned.store.all('SELECT DISTINCT session_id FROM harbor.requests WHERE id=ANY($1::text[])',admitted.map(row=>row.requestId));assert.equal(sessions.length,4);let active=0,peak=0;for(const row of owned.events.slice(began)){if(!traces.has(row.trace))continue;if(row.kind==='upstream-dispatch')peak=Math.max(peak,++active);if(row.kind==='upstream-complete')active--;}assert.equal(peak,4);assert.equal(active,0);
  }finally{await owned?.close();}
});
