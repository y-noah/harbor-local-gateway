import test from 'node:test';
import assert from 'node:assert/strict';
import {inspect} from 'node:util';
import {normalizeLimits,accountCapacity} from '../rates.mjs';
import {createCodexQuotaSubjectIdentity,codexQuotaSubject,apiCredentialQuotaSubject} from '../quota-subject.mjs';

const at=1800000000000,key=Buffer.alloc(32,7),identity=createCodexQuotaSubjectIdentity(key,{schemaVersion:'0.167.2'});
const rate=extra=>({limitId:'codex',primary:{usedPercent:35,windowDurationMins:300,resetsAt:1800000300},planType:'pro',...extra});
const input=extra=>({accountId:'fixture-backend-account',rateLimits:rate(),...extra});
const normalize=(value,subjectIdentity=identity)=>normalizeLimits(value,at,{subjectIdentity});

test('trusted backend account and explicit metered bucket correlate across independent reads',()=>{
  const single=normalize(input()),multi=normalize(input({rateLimitsByLimitId:{codex:rate()}}));
  assert.deepEqual(codexQuotaSubject(single),codexQuotaSubject(multi));
  const subject=codexQuotaSubject(single);assert.equal(subject.state,'known');assert.match(subject.ref,/^[a-f0-9]{64}$/);
  assert.equal(subject.ordinary_scope,'account_metered_bucket');assert.equal(subject.extra_scope,'unknown');assert.equal(subject.schema_version,'0.167.2');assert.equal(subject.checked,at);assert.ok(Object.isFrozen(subject));
});

test('nullable backend account or absent bucket stays unknown instead of using email or percentages',()=>{
  for(const value of [input({accountId:null}),input({accountId:undefined}),input({accountId:'',email:'fixture@example.invalid'}),input({rateLimits:rate({limitId:null})}),input({rateLimits:rate({limitId:undefined})})])assert.equal(codexQuotaSubject(normalize(value)).state,'unknown');
  const known=normalize(input({rateLimitsByLimitId:{codex:rate({limitId:null})}}));assert.equal(codexQuotaSubject(known).state,'known');
});

test('conflicting explicit bucket and map identity cannot establish a subject',()=>{
  for(const by of [{codex:rate({limitId:'other-bucket'})},{alias:rate()}]){
    const quota=normalize(input({rateLimitsByLimitId:by}));assert.equal(quota.remaining,65);
    assert.deepEqual(codexQuotaSubject(quota),{state:'unknown',reason:'metered_bucket_conflict',ordinary_scope:'unknown',extra_scope:'unknown'});
  }
});

test('untrusted supplied subject fields and lookalike identity capabilities cannot bind a quota',()=>{
  const value=input({subject_ref:'forged',subject:{state:'known',ref:'forged'}});
  for(const untrusted of [undefined,{},Object.freeze({...identity}),{key,schemaVersion:'0.167.2'},()=>({ref:'forged'})]){
    const quota=normalizeLimits(value,at,{subjectIdentity:untrusted});assert.equal(codexQuotaSubject(quota).state,'unknown');
  }
  assert.equal(codexQuotaSubject({...normalize(value)}).state,'unknown');
});

test('raw account and bucket identifiers never enter normalized DTOs or identity capability inspection',()=>{
  const quota=normalize(input()),publicText=JSON.stringify(quota),capabilityText=inspect(identity,{showHidden:true,depth:10});
  assert.doesNotMatch(publicText,/fixture-backend-account|subject|accountId|limitId/);
  assert.equal(capabilityText,'{}');assert.deepEqual(Object.keys(identity),[]);
  assert.equal(codexQuotaSubject(JSON.parse(publicText)).state,'unknown');
});

test('separate backend accounts, buckets and instance keys remain distinct',()=>{
  const first=codexQuotaSubject(normalize(input())).ref;
  const otherAccount=codexQuotaSubject(normalize(input({accountId:'fixture-other-account'}))).ref;
  const otherBucket=codexQuotaSubject(normalize(input({rateLimits:rate({limitId:'other-metered-bucket'})}))).ref;
  const otherInstance=codexQuotaSubject(normalize(input(),createCodexQuotaSubjectIdentity(Buffer.alloc(32,8),{schemaVersion:'0.167.2'}))).ref;
  assert.equal(new Set([first,otherAccount,otherBucket,otherInstance]).size,4);
});

test('opaque values are bounded and never silently trimmed into another identity',()=>{
  for(const accountId of [' fixture-backend-account','fixture-backend-account ', 'line\nbreak','x'.repeat(1025),123])assert.equal(codexQuotaSubject(normalize(input({accountId}))).state,'unknown');
  for(const limitId of ['', ' codex','codex\n','x'.repeat(257),123])assert.equal(codexQuotaSubject(normalize(input({rateLimits:rate({limitId})}))).state,'unknown');
});

test('credits and workspace spending refusal never expand the known ordinary bucket scope',()=>{
  const quota=normalize(input({ordinaryUsageAllowed:false,rateLimits:rate({spendControlReached:true,rateLimitReachedType:'workspace_owner_credits_depleted',credits:{balance:'100',hasCredits:true,unlimited:false}})}));
  assert.equal(codexQuotaSubject(quota).state,'known');assert.equal(codexQuotaSubject(quota).extra_scope,'unknown');
  assert.equal(quota.entitlements.credits.unit,null);assert.equal(quota.entitlements.credits.dispatch_permission_verified,false);
  assert.equal(accountCapacity({quota_json:JSON.stringify(quota),token_budget:100000,budget_used:0},1,0,at).eligible,false);
});

test('normalization and local capacity remain unchanged when identity capture is enabled',()=>{
  for(const value of [input(),input({ordinaryUsageAllowed:false}),input({rateLimits:rate({limitId:null})}),input({rateLimitsByLimitId:{codex:rate()}})]){
    const old=normalizeLimits(value,at),captured=normalize(value);assert.deepEqual(captured,old);
    const account={token_budget:100000,budget_used:1000,since_check_tokens:200,tokens_per_percent:10000};
    assert.deepEqual(accountCapacity({...account,quota_json:JSON.stringify(captured)},100,50,at),accountCapacity({...account,quota_json:JSON.stringify(old)},100,50,at));
  }
});

test('identity key is copied and invalid source configuration fails without echoing secrets',()=>{
  const mutable=Buffer.alloc(32,9),trusted=createCodexQuotaSubjectIdentity(mutable,{schemaVersion:'0.167.2'}),before=codexQuotaSubject(normalize(input(),trusted)).ref;
  mutable.fill(0);assert.equal(codexQuotaSubject(normalize(input(),trusted)).ref,before);
  for(const [invalid,options] of [[Buffer.alloc(31),{schemaVersion:'0.167.2'}],[key,{}],[key,{schemaVersion:'invalid-private-value'}]])assert.throws(()=>createCodexQuotaSubjectIdentity(invalid,options),{message:'供应方额度身份来源配置无效'});
});

test('both offline-checked schemas preserve the same bucket identity without claiming a runtime version',()=>{
  const older=createCodexQuotaSubjectIdentity(key,{schemaVersion:'0.160.1'}),a=codexQuotaSubject(normalize(input(),older)),b=codexQuotaSubject(normalize(input()));
  assert.equal(a.ref,b.ref);assert.equal(a.schema_version,'0.160.1');assert.equal(b.schema_version,'0.167.2');assert.equal(a.cli_version,undefined);
});

test('same API credential digest has a separate trusted scope and never implies a project balance',()=>{
  const a=apiCredentialQuotaSubject(identity,'a'.repeat(64)),b=apiCredentialQuotaSubject(identity,'a'.repeat(64)),other=apiCredentialQuotaSubject(identity,'b'.repeat(64));
  assert.deepEqual(a,b);assert.notEqual(a.ref,other.ref);assert.equal(a.ordinary_scope,'same_credential');assert.equal(a.extra_scope,'unknown');assert.doesNotMatch(JSON.stringify(a),/aaaaaaaa/);
  assert.equal(apiCredentialQuotaSubject({},'a'.repeat(64)).state,'unknown');assert.equal(apiCredentialQuotaSubject(identity,'not-a-digest').state,'unknown');
});
