import test from 'node:test';
import assert from 'node:assert/strict';
import {streamingStageReport,streamingStageQualification} from '../tools/capacity/streaming-stage-report.mjs';
const normal=()=>({passed:null,counts:{started:1,finished:1},groups:{normal:1},runtime:{hardFailures:0,missing:[]},missing:['helper_tls_lan_proxy_unproven'],requestStages:{passed:null},streamRelay:{passed:null},cancelRelease:{passed:null},bodyVisibility:{passed:null}});
test('selfcheck retains missing formal topology without relabeling it as a stage pass or failing old API qualification',()=>{
 const evidence=normal();assert.deepEqual(streamingStageQualification(evidence),{failed:false,missing:false});assert.deepEqual(streamingStageQualification(evidence,{profile:'formal'}),{failed:false,missing:true});const result=streamingStageReport([{id:'normal',evidence}]);assert.equal(result.passed,null);assert.equal(result.applicable,true);assert.deepEqual(result.scenarios[0].evidence.missing,['helper_tls_lan_proxy_unproven']);for(const name of ['requestStages','streamRelay','cancelRelease','bodyVisibility'])assert.equal(result[name].passed,null);
});
test('known runtime false and hardFailures dominate apparently green timing metrics in all reports',()=>{
 for(const value of [{passed:false},{runtime:{hardFailures:1}},{issueCount:1}]){const evidence={...normal(),...value,requestStages:{passed:true},streamRelay:{passed:true},cancelRelease:{passed:true},bodyVisibility:{passed:true}};assert.equal(streamingStageQualification(evidence).failed,true);assert.equal(streamingStageQualification(evidence,{profile:'formal'}).failed,true);const report=streamingStageReport([{id:'bad',evidence},{id:'good',evidence:{...normal(),passed:true}}]);assert.equal(report.passed,false);for(const name of ['requestStages','streamRelay','cancelRelease','bodyVisibility'])assert.equal(report[name].passed,false);}
});
test('admin-only scopes keep stage evidence separate and do not claim nonexistent model samples',()=>{
 const report=streamingStageReport([{id:'management',evidence:{...normal(),counts:{started:0}}}]);assert.equal(report.applicable,false);assert.equal(report.passed,null);assert.equal(report.requestStages.scenarios.length,0);assert.deepEqual(streamingStageQualification({...normal(),counts:{started:0}},{profile:'formal'}),{failed:false,missing:false});
});
test('known failures cannot disappear when no model reaches stage registration',()=>{
 for(const value of [{passed:false},{runtime:{hardFailures:1}},{issueCount:1}]){const evidence={...normal(),counts:{started:0},...value};assert.equal(streamingStageQualification(evidence).failed,true);assert.equal(streamingStageQualification(evidence,{profile:'formal'}).failed,true);const report=streamingStageReport([{id:'before-register',evidence}]);assert.equal(report.applicable,false);assert.equal(report.passed,false);for(const name of ['requestStages','streamRelay','cancelRelease','bodyVisibility'])assert.equal(report[name].passed,false);}
});
