import test from 'node:test';
import assert from 'node:assert/strict';
import {createCapacityCoverageFactory} from '../tools/capacity/coverage-factory.mjs';

function fixture({witness=()=>({passed:null,settled:null}),registered=()=>null}={}){
 const scope={id:'witness-owned',kind:'normal'};let at=0,reads=0;
 const factory=createCapacityCoverageFactory({getBusiness:()=>({}),getAdminActors:()=>[],getCandidateActors:()=>[],getNormalActorCount:()=>8,prepareCandidates:async()=>{},ownsScope:value=>value===scope,runOwnedOperation:async(_scope,operation)=>operation(),validateOwnedOperation:()=>true,readHandlerWitness:async trace=>{reads++;return witness(trace,reads);},readTrace:registered,readTraceEvidence:()=>null,store:{get:async()=>null,all:async()=>[]},settlementTimeoutMs:100,clock:()=>at,sleep:async ms=>{at+=ms;}});
 return {scope,factory,reads:()=>reads,time:()=>at};
}

test('handler completion waits for actual private witness rather than client response closure',async()=>{
 const f=fixture({witness:(_trace,reads)=>reads<3?{passed:null,settled:null,scope:'obs:witness-owned'}:{passed:true,settled:true,scope:'obs:witness-owned',outcome:'resolved',reasonCodes:[]}});
 const result=await f.factory.awaitCoverageTransport({scope:f.scope,trace:'synthetic-trace'});assert.equal(result.passed,true);assert.equal(result.settled,true);assert.equal(f.reads(),3);assert.ok(f.time()>0);
});

test('unknown handler evidence times out incomplete without pretending settlement',async()=>{
 const f=fixture(),result=await f.factory.awaitCoverageTransport({scope:f.scope,trace:'synthetic-trace'});assert.equal(result.passed,null);assert.equal(result.settled,null);assert.ok(f.time()>=100);assert.deepEqual(result.reasonCodes,['coverage_handler_not_settled']);
});

test('known witness conflicts and cross-scope witnesses stay false even with not-sent claims',async()=>{
 for(const witness of [{passed:false,settled:null,scope:'obs:witness-owned',reasonCodes:['handler_source_conflict']},{passed:true,settled:true,scope:'obs:another-scope',outcome:'resolved'}]){const f=fixture({witness:()=>witness,registered:()=>({scope:'obs:witness-owned',registered:true,notSent:true})}),result=await f.factory.awaitCoverageTransport({scope:f.scope,trace:'synthetic-trace'});assert.equal(result.passed,false);assert.equal(result.settled,null);}
});

test('only a matching private registered not-sent fact can bypass an unnecessary handler wait',async()=>{
 const f=fixture({registered:()=>({scope:'obs:witness-owned',registered:true,notSent:true})}),result=await f.factory.awaitCoverageTransport({scope:f.scope,trace:'synthetic-trace'});assert.equal(result.passed,true);assert.equal(result.settled,true);assert.deepEqual(result.reasonCodes,['coverage_request_not_sent']);
 const foreign=fixture({registered:()=>({scope:'obs:other',registered:true,notSent:true})});assert.equal((await foreign.factory.awaitCoverageTransport({scope:foreign.scope,trace:'synthetic-trace'})).passed,null);
});

test('forged scopes and missing traces cannot acquire a handler completion proof',async()=>{
 const f=fixture();await assert.rejects(f.factory.awaitCoverageTransport({scope:{...f.scope},trace:'synthetic-trace'}),/scope_not_owned/);const result=await f.factory.awaitCoverageTransport({scope:f.scope});assert.equal(result.passed,null);assert.equal(f.reads(),0);
});
