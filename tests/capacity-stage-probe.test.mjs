import test from 'node:test';
import assert from 'node:assert/strict';
import {AsyncLocalStorage} from 'node:async_hooks';
import {setTimeout as delay} from 'node:timers/promises';
import {createRoutingPreparation} from '../routing-preparation.mjs';
import {createSessionRequestHandoff} from '../session-request-handoff.mjs';
import {createCapacityStageProbe} from '../tools/capacity/stage-probe.mjs';

function fixture(options={}){
  const context=new AsyncLocalStorage(),events=[],app={preparation:createRoutingPreparation(),sessionHandoffs:createSessionRequestHandoff()},original={enter:app.preparation.enter,arrive:app.sessionHandoffs.arrive},probe=createCapacityStageProbe({context:()=>context.getStore(),sink:event=>events.push(event),...options});probe.attach(app);
  return {app,events,probe,original,run:(trace,fn)=>context.run({trace},fn)};
}

test('real preparation distinguishes queued readers from immediate grants without changing release ownership',async()=>{
  const f=fixture(),writer=f.app.preparation.reserve(['member']),pending=f.run('waiting-request',()=>f.app.preparation.enter('member'));
  assert.equal(f.probe.status().pending,1);assert.equal(f.events.length,0);writer();const reader=await pending;
  assert.equal(f.app.preparation.isPreparing('member'),true);assert.equal(f.events[0].queued,true);assert.equal(f.events[0].category,'preparation');assert.equal(f.events[0].outcome,'granted');reader();
  const instant=await f.run('immediate-request',()=>f.app.preparation.enter('member'));assert.equal(f.events[1].queued,false);instant();f.probe.close();assert.equal(f.app.preparation.enter,f.original.enter);assert.equal(f.app.sessionHandoffs.arrive,f.original.arrive);
});

test('real handoff observes actual barrier and final ticket close once, including cross-request release',async()=>{
  const f=fixture(),old=f.run('old-request',()=>f.app.sessionHandoffs.arrive('member'));await old.bind('session');
  const intent=f.app.sessionHandoffs.register({memberId:'member',sessionId:'session',decisionId:'decision'});let commit;const committing=new Promise(resolve=>{commit=resolve;});intent.activate(()=>committing);
  const next=f.run('next-request',()=>f.app.sessionHandoffs.arrive('member')),waiting=next.bind('session');assert.equal(f.app.sessionHandoffs.status().waiting,1);old.close();assert.equal(f.events.filter(event=>event.kind==='gateway-ticket-closed').length,1);commit();await waiting;
  const span=f.events.find(event=>event.trace==='next-request'&&event.kind==='gateway-wait-span');assert.equal(span.queued,true);assert.equal(span.category,'handoff');assert.equal(span.outcome,'granted');next.close();next.close();assert.equal(f.events.filter(event=>event.kind==='gateway-ticket-closed'&&event.trace==='next-request').length,1);assert.equal(f.probe.status().pending,0);assert.equal(f.probe.status().tickets,0);f.probe.close();
});

test('cancelled preparation keeps its original error and never records a successful queue grant',async()=>{
  const f=fixture(),controller=new AbortController(),writer=f.app.preparation.reserve(['member']),pending=f.run('cancel-request',()=>f.app.preparation.enter('member',controller.signal));controller.abort();await assert.rejects(pending,{code:'request_cancelled'});assert.equal(f.events[0].outcome,'rejected');assert.equal(f.events[0].queued,true);assert.equal(f.probe.status().pending,0);writer();f.probe.close();
});

test('probe failure or budget exhaustion cannot change real gate behavior or leak arbitrary context',async()=>{
  const f=fixture({maxPending:1,sink:()=>{throw Error('sink failure');}}),writer=f.app.preparation.reserve(['a']),first=f.run('bounded-request',()=>f.app.preparation.enter('a'));
  const second=await f.run('unobserved-request',()=>f.app.preparation.enter('b'));second();writer();(await first)();assert.equal(f.probe.status().dropped,2);
  const secret=await f.run('not a trace: private text',()=>f.app.preparation.enter('c'));secret();assert.equal(f.probe.status().missingTrace,1);assert.equal(f.events.length,0);f.probe.close();f.probe.close();
});

test('closing observation restores open tickets without closing them or unblocking real handoffs',async()=>{
  const f=fixture(),ticket=f.run('live-ticket',()=>f.app.sessionHandoffs.arrive('member'));await ticket.bind('session');f.probe.close();assert.equal(f.app.sessionHandoffs.status().tickets,1);ticket.close();assert.equal(f.app.sessionHandoffs.status().tickets,0);assert.equal(f.events.filter(event=>event.kind==='gateway-ticket-closed').length,0);assert.throws(()=>f.probe.attach(f.app),/already_attached/);
});

test('owned real PG gateway emits queue and actual finally-ticket endpoints for a model request',{skip:!process.env.HARBOR_CAPACITY_PG_BIN,timeout:120000},async()=>{
  const {createTarget}=await import('../tools/capacity/gateway-adapter.mjs');let target;
  try{
    target=await createTarget({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB,employeeCount:1});
    const scene={id:'stage-endpoints',employees:1,upstreamMs:[30,40],payloadBytes:1024};await target.onScenarioStart({scene});
    const result=await target.sendModel({actor:target.employeeActors[0],actorIndex:0,sequence:0,scene});assert.equal(result.success,true);assert.equal(result.complete,true);
    const deadline=Date.now()+10000;while(!target.events.some(event=>event.trace===result.trace&&event.kind==='gateway-ticket-closed')&&Date.now()<deadline)await delay(10);
    const rows=target.events.filter(event=>event.trace===result.trace),spans=rows.filter(event=>event.kind==='gateway-wait-span'),release=rows.find(event=>event.kind==='gateway-ticket-closed'),settled=rows.find(event=>event.kind==='gateway-settled');
    assert.ok(spans.some(event=>event.category==='preparation'&&event.outcome==='granted'&&event.queued===false));assert.ok(spans.some(event=>event.category==='handoff'&&event.outcome==='granted'&&event.queued===false));assert.ok(release);assert.ok(settled);assert.ok(release.at>=settled.at);assert.equal(rows.filter(event=>event.kind==='gateway-ticket-closed').length,1);
    for(const row of [...spans,release])assert.ok(Object.keys(row).every(key=>['kind','trace','spanId','category','start','end','queued','outcome','at'].includes(key)));
    const status=await target.gateway.command('status');assert.equal(status.stageProbe.pending,0);assert.equal(status.stageProbe.tickets,0);assert.equal(status.stageProbe.dropped,0);
  }finally{await target?.close();}
});
