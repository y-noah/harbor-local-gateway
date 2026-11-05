import test from 'node:test';
import assert from 'node:assert/strict';
import {planDeviceConfig,restoreDeviceConfig,deviceConfigPath} from '../clients/harbor-device/configure.mjs';
import {deviceHardwareDigest,deviceEnrollmentMessage} from '../device-proof.mjs';

const original=`model = "gpt-6.1-sol"
model_provider = "harbor"
model_reasoning_effort = "high"
[model_providers.harbor]
name = "Harbor"
base_url = 'https://gateway.example.invalid/v1' # retain source comment
env_key = "HARBOR_API_KEY"
wire_api = "responses"
[model_providers.other]
base_url = "https://other.example.invalid/v1"
[mcp_servers.synthetic]
command = "node"
args = ["synthetic.mjs"]
`;
test('device client config only changes Harbor transport and adds environment-backed local authentication',()=>{
  const plan=planDeviceConfig(original,{baseUrl:'http://127.0.0.1:43128/v1'});assert.match(plan.text,/base_url = "http:\/\/127\.0\.0\.1:43128\/v1" # retain source comment/);for(const value of ['model = "gpt-6.1-sol"','model_reasoning_effort = "high"','env_key = "HARBOR_API_KEY"','https://other.example.invalid/v1','args = ["synthetic.mjs"]'])assert.ok(plan.text.includes(value));assert.match(plan.text,/"X-Harbor-Local-Token" = "HARBOR_DEVICE_LOCAL_TOKEN"/);assert.equal(plan.preview.containsEmployeeKey,false);const restored=restoreDeviceConfig(plan.text,plan.manifest);assert.deepEqual(restored.conflicts,[]);assert.equal(restored.text,original);
});
test('device config restores only its own still-matching fields and keeps later user changes',()=>{
  const plan=planDeviceConfig(original,{baseUrl:'http://127.0.0.1:43128/v1'}),changed=plan.text.replace('model_reasoning_effort = "high"','model_reasoning_effort = "low"').replace('http://127.0.0.1:43128/v1','http://127.0.0.1:49999/v1');const restored=restoreDeviceConfig(changed,plan.manifest);assert.deepEqual(restored.conflicts,['base_url']);assert.match(restored.text,/49999/);assert.match(restored.text,/model_reasoning_effort = "low"/);assert.ok(!restored.text.includes('X-Harbor-Local-Token'));
});
test('device config preserves other inline environment headers and their subsequent edits',()=>{
  const input=original.replace('wire_api = "responses"','wire_api = "responses"\nenv_http_headers = { Existing = "OLD_ENV", "X-Harbor-Local-Token" = "OLD_LOCAL_ENV" }'),plan=planDeviceConfig(input,{baseUrl:'http://127.0.0.1:43128/v1'});assert.match(plan.text,/Existing = "OLD_ENV"/);assert.match(plan.text,/"HARBOR_DEVICE_LOCAL_TOKEN"/);const restored=restoreDeviceConfig(plan.text.replace('Existing = "OLD_ENV"','Existing = "NEW_ENV"'),plan.manifest);assert.deepEqual(restored.conflicts,[]);assert.match(restored.text,/Existing = "NEW_ENV"/);assert.match(restored.text,/"OLD_LOCAL_ENV"/);
});
test('device config ignores fake tables inside multiline strings and refuses ambiguous or unsupported effective scopes',()=>{
  const multiline='instructions = """\n[model_providers.harbor]\nbase_url = "fake"\n"""\n'+original;const plan=planDeviceConfig(multiline,{baseUrl:'http://127.0.0.1:43128/v1'});assert.ok(plan.text.includes('base_url = "fake"'));assert.equal(restoreDeviceConfig(plan.text,plan.manifest).text,multiline);
  for(const input of [original.replace('model_provider = "harbor"','model_provider = "other"'),original+'[profiles.test.model_providers.harbor]\nbase_url="https://override.invalid/v1"\n',original.replace('base_url = ', 'base_url = "duplicate"\nbase_url = '),original.replace('env_key = "HARBOR_API_KEY"','env_http_headers = { nested = { value = "unsupported" } }')])assert.throws(()=>planDeviceConfig(input,{baseUrl:'http://127.0.0.1:43128/v1'}));
  assert.throws(()=>planDeviceConfig(original,{baseUrl:'http://0.0.0.0:43128/v1'}));assert.throws(()=>planDeviceConfig(original,{baseUrl:'https://gateway.invalid/v1'}));
});
test('device config resolves actual explicit user home and profile without touching project config',()=>{
  assert.equal(deviceConfigPath({env:{CODEX_HOME:'/isolated/custom'},userHome:'/isolated/user'}),'/isolated/custom/config.toml');assert.equal(deviceConfigPath({env:{},userHome:'/isolated/user',profile:'test-profile'}),'/isolated/user/.codex/test-profile.config.toml');assert.throws(()=>deviceConfigPath({env:{CODEX_HOME:'relative'},userHome:'/isolated/user'}));assert.throws(()=>deviceConfigPath({env:{},profile:'../other'}));
});
test('hardware digest is instance-scoped and rejects placeholders without retaining raw serials',()=>{
  const input='12345678-1234-1234-1234-123456789abc',a=deviceHardwareDigest('a'.repeat(64),input),b=deviceHardwareDigest('b'.repeat(64),input);assert.notEqual(a,b);assert.match(a,/^[a-f0-9]{64}$/);assert.throws(()=>deviceHardwareDigest('a'.repeat(64),'00000000-0000-0000-0000-000000000000'));assert.throws(()=>deviceHardwareDigest('a'.repeat(64),'FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF'));
  const message=deviceEnrollmentMessage({challenge:'synthetic',nonce:'synthetic',scope:'a'.repeat(64),csr:'synthetic',name:'测试',system:'合成',fingerprint:a,installation_fingerprint:null,fingerprint_source:'smbios_uuid'});assert.ok(!message.includes(input));
});

test('device configuration preserves existing dotted header tables and root provider layout',()=>{
  const dotted=original.replace('wire_api = "responses"','wire_api = "responses"\nenv_http_headers.Existing = "EXISTING_ENV"'),root='model_provider = "harbor"\nmodel_providers.harbor.base_url = "https://gateway.example.invalid/v1"\nmodel_providers.harbor.env_http_headers.Existing = "EXISTING_ENV"\n';
  for(const input of [dotted,root]){const plan=planDeviceConfig(input,{baseUrl:'http://127.0.0.1:43128/v1'});assert.match(plan.text,/Existing = "EXISTING_ENV"/);assert.equal(restoreDeviceConfig(plan.text,plan.manifest).text,input);assert.equal(plan.manifest.addedHeaderTable,false);}
});

test('local HTTP authentication header matching is case-insensitive and rejects conflicting static copies',()=>{
  for(const old of ['env_http_headers.x-harbor-local-token = "OLD_ENV"','env_http_headers = { "x-harbor-local-token" = "OLD_ENV" }']){const input=original.replace('wire_api = "responses"','wire_api = "responses"\n'+old),plan=planDeviceConfig(input,{baseUrl:'http://127.0.0.1:43128/v1'});assert.match(plan.text,/x-harbor-local-token.*HARBOR_DEVICE_LOCAL_TOKEN/);assert.ok(!plan.text.includes('"X-Harbor-Local-Token"'));assert.equal(restoreDeviceConfig(plan.text,plan.manifest).text,input);}
  for(const setting of ['http_headers.x-harbor-local-token = "old"','http_headers = { "x-harbor-local-token" = "old" }','env_http_headers = { "x-harbor-local-token" = "OLD_ENV", "X-Harbor-Local-Token" = "OTHER_ENV" }'])assert.throws(()=>planDeviceConfig(original.replace('wire_api = "responses"','wire_api = "responses"\n'+setting),{baseUrl:'http://127.0.0.1:43128/v1'}));
});
