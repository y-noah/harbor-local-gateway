import test from 'node:test';
import assert from 'node:assert/strict';
import {RecordRedactor,redactRecordText,sanitizeRecord,redacted} from '../record-redaction.mjs';

const samples=[
  ['sk-proj-'+('A'.repeat(60)),'A'.repeat(40)],
  ['GITHUB_TOKEN = github_pat_'+('x'.repeat(40)),'x'.repeat(30)],
  ['Authorization: Bearer opaque-credential-without-known-prefix\nvisible','opaque-credential'],
  ['{"api_key":"plain-opaque-value","safe":"visible"}','plain-opaque-value'],
  ['{"body":"{\\"password\\":\\"opaque-nested-value\\"}"}','opaque-nested-value'],
  ['-----BEGIN OPENSSH PRIVATE KEY-----\nPRIVATE_BASE64_MATERIAL\n-----END OPENSSH PRIVATE KEY-----\nvisible','PRIVATE_BASE64_MATERIAL'],
  ['data:image/png;base64,'+'A'.repeat(100),'A'.repeat(20)],
  ['Cookie: first=opaque-cookie; second=more-secret\nvisible','more-secret']
  ,['PASSWORD=opaque}value;still-secret\nvisible','still-secret']
  ,['sk-proj-opaque=value~with&punctuation visible','with&punctuation']
];
test('credential and attachment filtering is invariant under every two-fragment split',()=>{
  for(const [input,secret] of samples){
    const expected=redactRecordText(input);assert.ok(!expected.includes(secret),input.slice(0,25));
    for(let i=0;i<=input.length;i++){const f=new RecordRedactor(),actual=f.push(input.slice(0,i))+f.push(input.slice(i))+f.finish();assert.equal(actual,expected,'split '+i+' of '+input.slice(0,20));}
  }
});
test('one-character fragments hide tokens before persistence and retain harmless text and code formatting',()=>{
  const input='问题：保持代码\n```js\nconst hello = "world";\n```\nBearer opaque-value\n尾部😀';
  const f=new RecordRedactor();let output='';for(const c of input.split(''))output+=f.push(c);output+=f.finish();
  assert.ok(!output.includes('opaque-value'));assert.ok(output.includes('```js\nconst hello = "world";\n```'));assert.ok(output.endsWith('尾部😀'));assert.equal(f.finish(),'');assert.throws(()=>f.push('late'));
});
test('secret values with escaped quotes do not leak and long input cannot grow the raw token buffer',()=>{
  for(const input of ['{"password":"before\\\"middle-after","normal":"visible"}','password = "secret\\\\"\nvisible']){const actual=redactRecordText(input);assert.ok(!actual.includes('before'));assert.ok(!actual.includes('middle-after'));assert.ok(!actual.includes('secret'));assert.ok(actual.includes('visible'));}
  const f=new RecordRedactor();f.push('x'.repeat(100000));assert.ok(f.word.length<=8192);assert.ok(f.recent.length<=100);assert.equal(f.high.length,0);f.finish();
});
test('structured filtering does not mutate source data and masks sensitive descendants and arrays',()=>{
  const source={arguments:{apiKey:'fixture-secret',nested:[{password:'fixture-password',normal:'visible'}]},text:'user message',authorization:'Bearer opaque'};
  const result=sanitizeRecord(source);assert.equal(result.arguments.apiKey,redacted);assert.equal(result.arguments.nested[0].password,redacted);assert.equal(result.arguments.nested[0].normal,'visible');assert.equal(source.arguments.apiKey,'fixture-secret');assert.equal(result.authorization,redacted);
  assert.deepEqual(sanitizeRecord(Buffer.from('fixture-private-binary')),{type:'binary',bytes:22,content:'[附件数据已省略]'});
  assert.deepEqual(sanitizeRecord({type:'image',data:'PRIVATE_BASE64',url:'https://private.invalid/signed',size:22}),{type:'image',bytes:22,content:'[附件数据已省略]'});
  assert.ok(!redactRecordText('{"b64_json":"PRIVATE_BASE64","text":"visible"}').includes('PRIVATE_BASE64'));
  const fake={record_sanitized:true,text:'Authorization: opaque-fixture-secret'};const safe=sanitizeRecord(fake);assert.ok(!safe.text.includes('opaque-fixture-secret'));assert.ok(Object.isFrozen(safe));assert.throws(()=>{safe.text='unsafe';},TypeError);assert.ok(fake.text.includes('opaque-fixture-secret'));
  const partial=new RecordRedactor();partial.push('-----BEGIN PRIVATE KEY-----\nopaque-secret-tail');partial.finish();assert.ok(!JSON.stringify(partial).includes('opaque-secret-tail'));
});
test('plain JSON and ordinary code identifiers remain useful while embedded credentials are filtered',()=>{
  const raw=JSON.stringify({note:'Authorization: opaque-fixture-secret',path:'visible.txt'});
  for(let split=0;split<=raw.length;split++){const filter=new RecordRedactor();const safe=filter.push(raw.slice(0,split))+filter.push(raw.slice(split))+filter.finish();assert.equal(JSON.parse(safe).path,'visible.txt');assert.ok(!safe.includes('opaque-fixture-secret'));}
  const code='const task_result = "visible"; function ask_user() { return task_result; }';assert.equal(redactRecordText(code),code);
  assert.ok(!redactRecordText('url/ghp_fixture-secret-material').includes('fixture-secret-material'));
  assert.ok(!redactRecordText('labelsk-proj-'+('A'.repeat(40))).includes('A'.repeat(30)));
  assert.ok(!redactRecordText('labelsk-'+('A'.repeat(48))).includes('A'.repeat(30)));
});
test('R19 prose quotes never shorten ordinary Header filtering, including quoted Cookie values',()=>{
  const headers=["Cookie: session='fixture-secret'; other=another-secret",'Cookie: "fixture-secret"; other=another-secret','Authorization: "fixture-secret"; other=another-secret'];
  for(const prefix of ["Alice's configuration\n",'Unpaired "configuration\n','Unpaired "configuration '])for(const header of headers){
    const raw=prefix+header+'\nvisible tail';
    for(let split=0;split<=raw.length;split++){const filter=new RecordRedactor();const safe=filter.push(raw.slice(0,split))+filter.push(raw.slice(split))+filter.finish();assert.ok(!safe.includes('fixture-secret'));assert.ok(!safe.includes('another-secret'));assert.ok(safe.endsWith('visible tail'));}
  }
  const raw=JSON.stringify({note:'Cookie: session="fixture-secret"; other=another-secret',path:'visible.txt'});for(let split=0;split<=raw.length;split++){const filter=new RecordRedactor(),safe=filter.push(raw.slice(0,split))+filter.push(raw.slice(split))+filter.finish();assert.equal(JSON.parse(safe).path,'visible.txt');assert.ok(!safe.includes('fixture-secret'));assert.ok(!safe.includes('another-secret'));}
});
test('R20 real word boundaries survive window movement and long-word buffer clipping',()=>{
  for(const raw of ['task_result_identifier','ask_user_for_confirmation'])for(let split=0;split<=raw.length;split++){const filter=new RecordRedactor();assert.equal(filter.push(raw.slice(0,split))+filter.push(raw.slice(split))+filter.finish(),raw);}
  const long='a'.repeat(8200)+'sk_'+'b'.repeat(100);for(const split of [0,16,8191,8192,8200,8202,long.length]){const filter=new RecordRedactor();assert.equal(filter.push(long.slice(0,split))+filter.push(long.slice(split))+filter.finish(),long);}
  for(const raw of ['sk_'+'a'.repeat(60),'sk-proj-'+'a'.repeat(60),'hg_'+'a'.repeat(60)])for(let split=0;split<=raw.length;split++){const filter=new RecordRedactor(),safe=filter.push(raw.slice(0,split))+filter.push(raw.slice(split))+filter.finish();assert.ok(!safe.includes('a'.repeat(30)));}
});
