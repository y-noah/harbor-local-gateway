import test from 'node:test';import assert from 'node:assert/strict';import {readFileSync} from 'node:fs';
import {resolveCodexModelMetadata,isCodexModelProfile,codexModelMetadataSource} from '../codex-model-metadata.mjs';
import {syntheticCodexProfile} from './codex-model-profile-fixture.mjs';
const publicModels=JSON.parse(readFileSync(new URL('../third-party/codex-models/models.json',import.meta.url))).models,known=publicModels.find(model=>model.supported_in_api),slug=known.slug;
test('known API metadata is the unchanged public supplier profile and never invents model availability',()=>{
 const value=resolveCodexModelMetadata([{source:'api',models:[{slug,display_name:'API identity only'},{slug:'unknown-future-model'}]}]);assert.deepEqual(value.models,[known]);assert.deepEqual(value.available,[slug,'unknown-future-model']);assert.deepEqual(value.status.missing,[{slug:'unknown-future-model',reason:'metadata_unavailable',consumption_sources:['api']}]);assert.equal(value.status.complete,false);assert.equal(codexModelMetadataSource.version,'0.167.2');assert.deepEqual(resolveCodexModelMetadata([]).models,[]);
 assert.deepEqual(resolveCodexModelMetadata([{source:'api',models:[{slug}]}],{allowed:[]}).available,[]);
});
test('same slug metadata is order-independent, source-scoped and never borrowed from a subscription',()=>{
 const sub={source:'subscription',models:[{...known,consumption_sources:['subscription'],protocol_compatibility:'unknown'}]},api={source:'api',models:[{slug}]};for(const entries of [[sub,api],[api,sub]])assert.deepEqual(resolveCodexModelMetadata(entries).models,[known]);
 const changed={source:'subscription',models:[{...known,model_messages:{instructions_template:'Different genuine source behavior'}}]};for(const entries of [[changed,api],[api,changed]]){const result=resolveCodexModelMetadata(entries);assert.deepEqual(result.models,[]);assert.equal(result.status.missing[0].reason,'metadata_conflict');}
 const unknown=syntheticCodexProfile({slug:'unknown-future-model'});assert.deepEqual(resolveCodexModelMetadata([{source:'subscription',models:[unknown]}]).models,[unknown]);const result=resolveCodexModelMetadata([{source:'subscription',models:[unknown]},{source:'api',models:[{slug:unknown.slug}]}]);assert.deepEqual(result.models,[]);assert.equal(result.status.missing[0].reason,'metadata_unavailable');
});
test('invalid incomplete selector metadata stays available but is explicitly omitted from the Codex export',()=>{const value=resolveCodexModelMetadata([{source:'subscription',models:[{slug:'incomplete'}]}]);assert.deepEqual(value.available,['incomplete']);assert.deepEqual(value.models,[]);assert.equal(value.status.missing[0].reason,'metadata_unavailable');assert.ok(publicModels.every(isCodexModelProfile));});

test('optional fields and nested model messages reject malformed selector JSON without changing data availability',()=>{
 for(const change of [model=>model.additional_speed_tiers=42,model=>model.model_messages.tools=42,model=>model.model_messages.tools={history:{read_item:{description:42}}},model=>model.service_tiers=[{id:'fast',name:'Fast'}],model=>model.model_messages.token_budget={enabled:true},model=>model.context_window='unknown',model=>model.guardian={initial_cua_call:'yes'},model=>model.model_messages.guardian_v2={review_threshold_basis_points:65536},model=>model.available_access_programs={cyber:[42]},model=>model.default_verbosity='invalid']){
  const bad=structuredClone(known);change(bad);assert.equal(isCodexModelProfile(bad),false);const result=resolveCodexModelMetadata([{source:'subscription',models:[bad]}]);assert.deepEqual(result.available,[slug]);assert.deepEqual(result.models,[]);assert.equal(result.status.missing[0].reason,'metadata_unavailable');
 }
 const future=structuredClone(known);future.supported_reasoning_levels=[{effort:'future-effort',description:'Future official effort'}];future.tool_mode='future-mode';future.extra_future_field={untouched:true};assert.equal(isCodexModelProfile(future),true);assert.deepEqual(resolveCodexModelMetadata([{source:'subscription',models:[future]}]).models,[future]);
});

test('empty reasoning effort is invalid at every nested location while unknown nonempty values remain intact',()=>{
 for(const change of [m=>m.default_reasoning_level='',m=>m.supported_reasoning_levels=[{effort:'',description:'Malformed'}],m=>m.model_messages.guardian_v2={reasoning_effort:''},m=>m.multi_agent_reasoning_effort='']){const bad=structuredClone(known);change(bad);assert.equal(isCodexModelProfile(bad),false);const result=resolveCodexModelMetadata([{source:'subscription',models:[bad]}]);assert.deepEqual(result.models,[]);assert.deepEqual(result.available,[slug]);assert.equal(result.status.complete,false);}
 const unknown=structuredClone(known);unknown.default_reasoning_level='future';unknown.multi_agent_reasoning_effort=' ';assert.equal(isCodexModelProfile(unknown),true);
});
