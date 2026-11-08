import test from 'node:test';
import assert from 'node:assert/strict';
import {requestParameters} from '../request-parameters.mjs';

test('request parameters preserve explicit identifiers without changing the payload or guessing defaults',()=>{
  const payload={reasoning:{effort:'high'},service_tier:'auto',input:'not recorded',tools:[{name:'not recorded'}]};
  const before=structuredClone(payload);
  assert.deepEqual(requestParameters(payload),{effort:'high',service_tier:'auto'});
  assert.deepEqual(payload,before);
  assert.deepEqual(requestParameters({}),{effort:null,service_tier:null});
  assert.deepEqual(requestParameters(null),{effort:null,service_tier:null});
  assert.deepEqual(requestParameters({reasoning:{effort:'future-effort'},service_tier:'future_tier'}),{effort:'future-effort',service_tier:'future_tier'});
});
test('request parameters reject oversized, structured and credential-like values before persistence',()=>{
  for(const value of [0,{},[],true,'','a'.repeat(129),'high\nsecret','<script>','sk-synthetic-private-credential','hg_synthetic_private_credential']){
    assert.deepEqual(requestParameters({reasoning:{effort:value},service_tier:value}),{effort:null,service_tier:null});
  }
});
