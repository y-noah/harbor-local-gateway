import test from 'node:test';
import assert from 'node:assert/strict';
import {settlementMeteringDiagnostic} from '../request-metering-diagnostic.mjs';

const received={version:1,reason:'usage_received',terminal:'completed',interruption:'none',usage_known:true};
test('结算以实际计量纠正诊断，并保持回放后的原因',()=>{
  for(const usage of [{input:-1},{input:1,output:Infinity},{input:1,cached:2}]){
    const diagnostic=settlementMeteringDiagnostic(received,usage);
    assert.deepEqual(diagnostic,{...received,reason:'settlement_usage_invalid',usage_known:false});
    assert.deepEqual(settlementMeteringDiagnostic(diagnostic,{input:0,output:0,cached:0,usageKnown:false}),diagnostic);
  }
  assert.deepEqual(settlementMeteringDiagnostic(received,{input:5,output:1,usageKnown:false}),{...received,reason:'settlement_uncertain',usage_known:false});
  assert.deepEqual(settlementMeteringDiagnostic(received,{input:5,output:1}),received);
});
test('未知原因保留终态事实，缺失或带任意字段的旧诊断不补造',()=>{
  const missing={...received,reason:'terminal_usage_missing',usage_known:false};
  assert.deepEqual(settlementMeteringDiagnostic(missing,{usageKnown:false}),missing);
  for(const value of [undefined,null,{...received,raw:'private payload'}])assert.equal(settlementMeteringDiagnostic(value,{input:5,output:1}),null);
  const unsent={...received,reason:'not_dispatched',terminal:'none'};
  assert.deepEqual(settlementMeteringDiagnostic(unsent,{input:0,output:0,usageKnown:true}),unsent);
});
