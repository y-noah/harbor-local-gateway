import {modelProfileContract} from './codex-model-profile-contract.mjs';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {isDeepStrictEqual} from 'node:util';
const source=JSON.parse(readFileSync(new URL('./third-party/codex-models/source.json',import.meta.url))),raw=readFileSync(new URL('./third-party/codex-models/models.json',import.meta.url));
if(source.provider!=='openai'||source.version!=='0.167.2'||createHash('sha256').update(raw).digest('hex')!==source.sha256)throw Error('codex_model_registry_unverified');
const freeze=value=>{if(value&&typeof value==='object'){Object.values(value).forEach(freeze);Object.freeze(value);}return value;};
const profiles=new Map(JSON.parse(raw).models.map(model=>[model.slug,freeze(model)]));
const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
// These are Harbor availability annotations, never model capabilities. All other
// fields, including instructions and provider fields unknown to Harbor, compare exactly.
const annotations=new Set(['consumption_sources','protocol_compatibility','fallback_compatibility']);
const clean=model=>Object.fromEntries(Object.entries(model).filter(([key])=>!annotations.has(key)));
const enums={ConfigShellToolType:['unified_exec','disabled','default','local','shell_command'],ModelVisibility:['list','hide','none'],ReasoningSummary:['auto','concise','detailed','none'],Verbosity:['low','medium','high'],ApplyPatchToolType:['freeform'],WebSearchToolType:['text','text_and_image'],InputModality:['text','image','audio'],TruncationMode:['bytes','tokens'],AsyncClassifierMode:['snapshot','conversation']};
const openStrings=new Set(['String','ToolMode','MultiAgentVersion','GuardianReviewMode','GuardianUnscoredAction','CyberAccessProgram']);
function validField(type,value){
 if(type.startsWith('Option<'))return value===null||validField(type.slice(7,-1),value);
 if(type.startsWith('Vec<'))return Array.isArray(value)&&value.every(item=>validField(type.slice(4,-1),item));
 if(type==='std::collections::BTreeMap<String,String>')return object(value)&&Object.values(value).every(v=>typeof v==='string');
 // The CLI's retirement_at deserializer accepts arbitrary JSON and converts
 // unknown/non-RFC3339 values to None; rejecting them would change its contract.
 if(type==='DateTime<Utc>')return true;
 if(type==='ReasoningEffort')return typeof value==='string'&&value.length>0;
 if(openStrings.has(type))return typeof value==='string';
 if(enums[type])return enums[type].includes(value);
 if(type==='bool')return typeof value==='boolean';
 if(['i64','i32','usize','u16'].includes(type))return Number.isSafeInteger(value)&&(type!=='i32'||value>=-2147483648&&value<=2147483647)&&(!['usize','u16'].includes(type)||value>=0)&&(type!=='u16'||value<=65535);
 const fields=modelProfileContract[type];if(!fields||!object(value))return false;
 return Object.entries(fields).every(([key,field])=>!Object.hasOwn(value,key)?!field.required:(!field.aliasFor||!Object.hasOwn(value,field.aliasFor))&&validField(field.type,value[key]));
}
export function isCodexModelProfile(model){
 return validField('ModelInfo',model)&&(typeof model.base_instructions==='string'||typeof model.model_messages?.instructions_template==='string');
}
for(const model of profiles.values())if(!isCodexModelProfile(model))throw Error('codex_model_registry_profile_invalid');
export const codexModelMetadataSource=Object.freeze({provider:source.provider,version:source.version,sha256:source.sha256});
// Caller supplies only currently authorized, revalidated account catalogs. The
// registry can enrich an observed OpenAI API slug, never invent availability.
export function resolveCodexModelMetadata(entries,{allowed=null}={}){
 const grouped=new Map();for(const {source,models} of entries)for(const model of models){if(allowed!==null&&!allowed.includes(model.slug))continue;if(!grouped.has(model.slug))grouped.set(model.slug,[]);grouped.get(model.slug).push({source,model});}
 const models=[],missing=[];
 for(const [slug,rows] of grouped){
  const candidates=rows.map(({source,model})=>{if(source==='api'){const profile=profiles.get(slug);return profile?.supported_in_api===true?profile:null;}return isCodexModelProfile(model)?clean(model):null;});
  const sources=[...new Set(rows.map(row=>row.source).filter(Boolean))].sort();
  if(candidates.some(model=>model===null)){missing.push({slug,reason:'metadata_unavailable',consumption_sources:sources});continue;}
  const first=candidates[0];if(candidates.some(model=>!isDeepStrictEqual(first,model))){missing.push({slug,reason:'metadata_conflict',consumption_sources:sources});continue;}
  models.push(first);
 }
 return {models,available:[...grouped.keys()],status:{complete:missing.length===0,missing,registry:codexModelMetadataSource}};
}
