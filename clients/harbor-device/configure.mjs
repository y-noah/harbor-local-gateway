import {createHash} from 'node:crypto';
import {readFile,writeFile,rename,chmod,stat} from 'node:fs/promises';
import {resolve,join,isAbsolute,dirname} from 'node:path';
import {homedir} from 'node:os';
import {fileURLToPath} from 'node:url';

const fail=code=>{throw new Error(code);},hash=value=>createHash('sha256').update(value).digest('hex');
const quote=value=>JSON.stringify(value);
function dotted(value){
  const keys=[];let at=0;
  while(at<value.length){while(/\s/.test(value[at]??''))at++;if(at>=value.length)break;let key;
    if(value[at]==='"'||value[at]==="'"){const delimiter=value[at++],start=at-1;let escaped=false;for(;at<value.length;at++){const c=value[at];if(c===delimiter&&!escaped){at++;break;}escaped=delimiter==='"'&&c==='\\'&&!escaped;}const raw=value.slice(start,at);if(!raw.endsWith(delimiter))fail('device_config_unsupported_key');try{key=delimiter==="'"?raw.slice(1,-1):JSON.parse(raw);}catch{fail('device_config_unsupported_key');}}
    else{const match=value.slice(at).match(/^[A-Za-z0-9_-]+/);if(!match)fail('device_config_unsupported_key');key=match[0];at+=key.length;}
    keys.push(key);while(/\s/.test(value[at]??''))at++;if(at===value.length)break;if(value[at++]!=='.')fail('device_config_unsupported_key');
  }
  if(!keys.length)fail('device_config_unsupported_key');return keys;
}
function codeAndComment(line){let delimiter=null,escaped=false;for(let at=0;at<line.length;at++){const c=line[at];if(delimiter){if(c===delimiter&&!escaped)delimiter=null;escaped=delimiter==='"'&&c==='\\'&&!escaped;}else if(c==='"'||c==="'")delimiter=c;else if(c==='#')return {code:line.slice(0,at),comment:line.slice(at)};}return {code:line,comment:''};}
function literal(value){const raw=value.trim();if(/^'(?:[^']*)'$/.test(raw))return raw.slice(1,-1);try{if(!/^"(?:[^"\\]|\\.)*"$/.test(raw))fail('device_config_unsupported_string');return JSON.parse(raw);}catch{fail('device_config_unsupported_string');}}
function scan(text){
  if(typeof text!=='string'||Buffer.byteLength(text)>2097152||text.includes('\0'))fail('device_config_invalid');const newline=text.includes('\r\n')?'\r\n':'\n';if(newline==='\r\n'&&text.replaceAll('\r\n','').includes('\n'))fail('device_config_mixed_newlines');const lines=text.split(newline),assignments=[],tables=[];let table=[],multiline=null;
  for(let line=0;line<lines.length;line++){
    const raw=lines[line];if(multiline){if(raw.includes(multiline))multiline=null;continue;}const {code,comment}=codeAndComment(raw),trimmed=code.trim();
    if(trimmed.startsWith('[[')){table=['array-table'];continue;}if(trimmed.startsWith('[')){if(!trimmed.endsWith(']'))fail('device_config_unsupported_table');table=dotted(trimmed.slice(1,-1));tables.push({path:table,line});continue;}
    if(!trimmed)continue;let delimiter=null,escaped=false,index=-1;for(let at=0;at<code.length;at++){const c=code[at];if(delimiter){if(c===delimiter&&!escaped)delimiter=null;escaped=delimiter==='"'&&c==='\\'&&!escaped;}else if(c==='"'||c==="'")delimiter=c;else if(c==='='){index=at;break;}}
    if(index<0)fail('device_config_unsupported_statement');const key=dotted(code.slice(0,index).trim()),value=code.slice(index+1),path=table.concat(key);assignments.push({path,line,prefix:code.slice(0,index+1),value,comment});
    for(const triple of ['"""',"'''"]){const start=value.indexOf(triple);if(start>=0&&value.indexOf(triple,start+3)<0){multiline=triple;break;}}
  }
  if(multiline)fail('device_config_incomplete_multiline');return {lines,newline,assignments,tables};
}
function target(parsed,path){const found=parsed.assignments.filter(item=>JSON.stringify(item.path)===JSON.stringify(path));if(found.length>1)fail('device_config_duplicate_managed_key');return found[0]??null;}
function inline(value){const trimmed=value.trim();if(!trimmed.startsWith('{')||!trimmed.endsWith('}'))fail('device_config_unsupported_headers');const content=trimmed.slice(1,-1),parts=[];let delimiter=null,escaped=false,start=0;for(let at=0;at<content.length;at++){const c=content[at];if(delimiter){if(c===delimiter&&!escaped)delimiter=null;escaped=delimiter==='"'&&c==='\\'&&!escaped;}else if(c==='"'||c==="'")delimiter=c;else if(c===','){parts.push(content.slice(start,at));start=at+1;}else if(c==='{'||c==='}')fail('device_config_unsupported_headers');}if(delimiter)fail('device_config_unsupported_headers');if(content.slice(start).trim())parts.push(content.slice(start));return parts.map(part=>{const index=part.indexOf('=');if(index<0)fail('device_config_unsupported_headers');const path=dotted(part.slice(0,index).trim());if(path.length!==1)fail('device_config_unsupported_headers');return {raw:part,key:path[0],prefix:part.slice(0,index+1),value:part.slice(index+1),decoded:literal(part.slice(index+1))};});}
function setValue(text,path,newLiteral){
  const parsed=scan(text),existing=target(parsed,path);if(existing){if(newLiteral===null)parsed.lines.splice(existing.line,1);else parsed.lines[existing.line]=existing.prefix+' '+newLiteral+(existing.comment?' '+existing.comment:'');return parsed.lines.join(parsed.newline);}
  if(newLiteral===null)return text;
  const parent=path.slice(0,-1),header=parsed.tables.filter(table=>table.path.length<=parent.length&&table.path.every((key,index)=>parent[index]===key)).sort((a,b)=>b.path.length-a.path.length)[0];
  const managed=path.slice(header?.path.length??0).map(quote).join('.')+' = '+newLiteral;parsed.lines.splice(header?header.line+1:0,0,managed);return parsed.lines.join(parsed.newline);
}
function localHeaderAssignments(parsed,provider,kind='env_http_headers'){return parsed.assignments.filter(item=>item.path.length===4&&item.path[0]==='model_providers'&&item.path[1]===provider&&item.path[2]===kind&&item.path[3].toLowerCase()==='x-harbor-local-token');}
function headerValue(text,provider){const parsed=scan(text),direct=localHeaderAssignments(parsed,provider),map=target(parsed,['model_providers',provider,'env_http_headers']);if(direct.length>1||direct.length&&map)fail('device_config_duplicate_managed_key');if(direct.length)return {raw:direct[0].value.trim(),decoded:literal(direct[0].value)};if(map){const values=inline(map.value).filter(item=>item.key.toLowerCase()==='x-harbor-local-token');if(values.length>1)fail('device_config_duplicate_managed_key');return values[0]?{raw:values[0].value.trim(),decoded:values[0].decoded}:null;}return null;}
function setHeader(text,provider,value){const parsed=scan(text),path=['model_providers',provider,'env_http_headers'],map=target(parsed,path),direct=localHeaderAssignments(parsed,provider);if(!map)return setValue(text,direct[0]?.path??path.concat('X-Harbor-Local-Token'),value);const parts=inline(map.value),found=parts.findIndex(item=>item.key.toLowerCase()==='x-harbor-local-token');if(found>=0){if(value===null)parts.splice(found,1);else parts[found]={raw:parts[found].prefix+' '+value};}else if(value!==null)parts.push({raw:' "X-Harbor-Local-Token" = '+value});parsed.lines[map.line]=map.prefix+' {'+parts.map(item=>item.raw).join(',')+' }'+(map.comment?' '+map.comment:'');return parsed.lines.join(parsed.newline);}
export function planDeviceConfig(text,{provider='harbor',baseUrl,tokenEnvironment='HARBOR_DEVICE_LOCAL_TOKEN'}={}){
  if(!/^[A-Za-z0-9_-]{1,80}$/.test(provider)||!/^http:\/\/127\.0\.0\.1:[1-9]\d{3,4}\/v1$/.test(baseUrl??'')||new URL(baseUrl).port>65535||!/^[_A-Z][_A-Z0-9]{1,79}$/.test(tokenEnvironment))fail('device_config_target_invalid');
  const parsed=scan(text),staticMap=target(parsed,['model_providers',provider,'http_headers']);if(localHeaderAssignments(parsed,provider,'http_headers').length||staticMap&&inline(staticMap.value).some(item=>item.key.toLowerCase()==='x-harbor-local-token'))fail('device_config_static_local_header_requires_explicit_cleanup');const path=['model_providers',provider,'base_url'],base=target(parsed,path);if(!base)fail('device_config_provider_base_url_missing');
  if(parsed.assignments.some(item=>item.path[0]==='profiles'&&item.path.includes('model_providers')&&item.path.includes(provider)))fail('device_config_profile_provider_override_requires_explicit_file');
  const currentProvider=target(parsed,['model_provider']);if(currentProvider&&literal(currentProvider.value)!==provider)fail('device_config_selected_provider_differs');const originalBase=base.value.trim();literal(originalBase);const previousHeader=headerValue(text,provider),managedBase=quote(baseUrl),managedHeader=quote(tokenEnvironment);
  let after=setValue(text,path,managedBase);after=setHeader(after,provider,managedHeader);
  const manifest={version:1,provider,beforeHash:hash(text),afterHash:hash(after),beforeBase:originalBase,beforeHeader:previousHeader?.raw??null,managedBase,managedHeader,addedHeaderTable:false};
  return {text:after,manifest,preview:{provider,keys:['base_url','env_http_headers.X-Harbor-Local-Token'],beforeHash:manifest.beforeHash,afterHash:manifest.afterHash,containsEmployeeKey:false}};
}
export function restoreDeviceConfig(text,manifest){
  if(!manifest||manifest.version!==1||!/^[A-Za-z0-9_-]{1,80}$/.test(manifest.provider))fail('device_config_backup_invalid');let result=text;const conflicts=[],basePath=['model_providers',manifest.provider,'base_url'],base=target(scan(result),basePath);
  if(base&&base.value.trim()===manifest.managedBase)result=setValue(result,basePath,manifest.beforeBase);else conflicts.push('base_url');
  const header=headerValue(result,manifest.provider);if(header&&header.raw===manifest.managedHeader)result=setHeader(result,manifest.provider,manifest.beforeHeader);else conflicts.push('env_http_headers.X-Harbor-Local-Token');
  if(manifest.addedHeaderTable){const parsed=scan(result),table=parsed.tables.find(item=>JSON.stringify(item.path)===JSON.stringify(['model_providers',manifest.provider,'env_http_headers']));if(table){const next=parsed.tables.find(item=>item.line>table.line)?.line??parsed.lines.length,content=parsed.lines.slice(table.line+1,next);if(content.every(line=>!line.trim())){let from=table.line;if(from>0&&parsed.lines[from-1]==='')from--;parsed.lines.splice(from,next-from);if(parsed.lines.at(-1)!=='')parsed.lines.push('');result=parsed.lines.join(parsed.newline);}}}
  return {text:result,conflicts};
}
export function deviceConfigPath({env=process.env,userHome=homedir(),file,profile}={}){
  if(file)return resolve(file);const root=env.CODEX_HOME||join(userHome,'.codex');if(!isAbsolute(root))fail('device_codex_home_must_be_absolute');if(profile&&!/^[A-Za-z0-9_-]{1,80}$/.test(profile))fail('device_profile_invalid');return join(root,profile?profile+'.config.toml':'config.toml');
}
async function atomic(path,text){const info=await stat(path);if(!info.isFile())fail('device_config_not_regular_file');const temp=join(dirname(path),'.harbor-device-'+process.pid+'-'+Date.now());await writeFile(temp,text,{mode:0o600,flag:'wx'});await chmod(temp,info.mode&0o777);await rename(temp,path);}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  const args=process.argv.slice(2),option=name=>{const at=args.indexOf(name);return at<0?undefined:args[at+1];};
  try{const path=deviceConfigPath({file:option('--file'),profile:option('--profile')}),text=await readFile(path,'utf8'),backup=option('--backup');
    if(args[0]==='restore'){if(!backup)fail('device_backup_path_required');const restored=restoreDeviceConfig(text,JSON.parse(await readFile(backup,'utf8')));await atomic(path,restored.text);console.log(JSON.stringify({state:restored.conflicts.length?'restored_with_conflicts':'restored',conflicts:restored.conflicts}));}
    else{const plan=planDeviceConfig(text,{provider:option('--provider')??'harbor',baseUrl:option('--base-url')});if(args[0]==='apply'){if(option('--expected-hash')!==plan.manifest.beforeHash||!backup)fail('device_config_preview_confirmation_required');await writeFile(backup,JSON.stringify(plan.manifest),{mode:0o600,flag:'wx'});await atomic(path,plan.text);console.log(JSON.stringify({state:'configured',config:path,...plan.preview}));}else if(args[0]==='plan')console.log(JSON.stringify({config:path,...plan.preview}));else fail('device_config_command_invalid');}
  }catch(error){console.error(/^[a-z0-9_]+$/.test(error.message)?error.message:'device_config_operation_failed_no_credentials_logged');process.exitCode=1;}
}
