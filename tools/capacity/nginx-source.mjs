import {readFile,readdir,readlink,lstat,realpath,open} from 'node:fs/promises';
import {constants} from 'node:fs';
import {createHash} from 'node:crypto';
import {isAbsolute,resolve,dirname} from 'node:path';
import {isIP} from 'node:net';
import {inspectCapacityNginxLoadedGeneration} from './nginx-generation-witness.mjs';
import {readCapacityRuntimeRegular,capacityRuntimeArtifactBinding,inspectCapacityRuntimeArtifact} from './runtime-artifact.mjs';

const sources=new WeakMap(),fail=code=>{throw Error('capacity_nginx_'+code);};
const sha=value=>createHash('sha256').update(value).digest('hex');
const fields=Object.freeze({connection:'$connection',remoteAddress:'$remote_addr',remotePort:'$remote_port',localAddress:'$server_addr',localPort:'$server_port',upstream:'$upstream_addr',status:'$status',sent:'$bytes_sent',received:'$bytes_received'});
const sameFile=(a,b)=>a.dev===b.dev&&a.ino===b.ino;
function target(value){let url;try{url=new URL(value);}catch{fail('origin');}if(url.protocol!=='https:'||url.username||url.password||url.origin!==value)fail('origin');return {hostname:url.hostname,port:url.port||'443',host:url.hostname+':'+(url.port||'443')};}
async function processIdentity(expected){
 if(!expected||!Number.isSafeInteger(expected.pid)||expected.pid<2||!/^\d+$/.test(expected.startTicks??'')||!Number.isSafeInteger(expected.uid)||expected.uid<0)fail('process_configuration');
 const path='/proc/'+expected.pid,raw=await readFile(path+'/stat','utf8'),parts=raw.slice(raw.lastIndexOf(')')+2).trim().split(/\s+/),status=await readFile(path+'/status','utf8'),uids=/^Uid:\s+([0-9]+)\s+([0-9]+)\s+([0-9]+)\s+([0-9]+)/m.exec(status);
 if(!uids||uids.slice(1).some(value=>Number(value)!==expected.uid)||parts[19]!==expected.startTicks||['T','t','Z','X','x'].includes(parts[0]))fail('process_changed');
 return {pid:expected.pid,startTicks:parts[19],uid:expected.uid,parent:Number(parts[1]),executable:await readlink(path+'/exe')};
}
// A bounded syntax reader, not nginx -T/-t: loading an arbitrary dynamic module
// or opening production log files is not a read-only configuration inspection.
function syntax(content){
 const tokens=[];let word='',quoted=null,escaped=false,comment=false;
 const flush=()=>{if(word){tokens.push(word);word='';if(tokens.length>65536)fail('configuration_budget');}};
 for(const ch of content){if(comment){if(ch==='\n')comment=false;continue;}if(escaped){word+=ch;escaped=false;continue;}if(ch==='\\'){escaped=true;continue;}if(quoted){if(ch===quoted)quoted=null;else word+=ch;continue;}if(ch==='"'||ch==="'"){quoted=ch;continue;}if(ch==='#'){flush();comment=true;continue;}if(/\s/.test(ch)){flush();continue;}if(['{','}',';'].includes(ch)){flush();tokens.push(ch);}else word+=ch;}
 if(quoted||escaped)fail('configuration_syntax');flush();let at=0,nodes=0;
 const parse=depth=>{if(depth>32)fail('configuration_budget');const result=[];while(at<tokens.length){if(tokens[at]==='}'){if(depth===0)fail('configuration_syntax');at++;return result;}const args=[];while(at<tokens.length&&!['{','}',';'].includes(tokens[at]))args.push(tokens[at++]);if(!args.length||tokens[at]==='}'||at===tokens.length)fail('configuration_syntax');const end=tokens[at++],row={name:args.shift(),args,children:end==='{'?parse(depth+1):null};result.push(row);if(++nodes>8192)fail('configuration_budget');}if(depth)fail('configuration_syntax');return result;};return parse(0);
}
function one(rows,name,{optional=false}={}){const selected=rows.filter(row=>row.name===name);if(selected.length>1||!optional&&selected.length!==1)fail('configuration_'+name);return selected[0];}
const integer=value=>typeof value==='string'&&/^[0-9]{1,15}$/.test(value)?Number(value):NaN;
async function regular(path,max,trusted){if(typeof path!=='string'||!isAbsolute(path))fail('material_path');return readCapacityRuntimeRegular(path,max,{trusted});}
function configuration(value){
 if(!value||value.schema!=='capacity-nginx-source-v1'||value.type!=='nginx'||value.mode!=='stream-tls-passthrough'||!isAbsolute(value.prefix??'')||!Array.isArray(value.materials)||value.materials.length<1||value.materials.length>256||!Array.isArray(value.workers)||value.workers.length<1||value.workers.length>128)fail('configuration');
 const publicUrl=target(value.publicOrigin),backend=target(value.backendOrigin);
 if(isIP(value.listenAddress)!==4||backend.hostname!=='127.0.0.1'||!isAbsolute(value.configurationEntry??'')||!value.binary||!isAbsolute(value.binary.file??'')||!/^[a-f0-9]{64}$/.test(value.binary.sha256??'')||!value.witness||!isAbsolute(value.witness.file??''))fail('configuration');
 const names=new Set();for(const row of value.materials){if(!row||!isAbsolute(row.file??'')||names.has(row.file)||!['configuration','module'].includes(row.role)||!/^[a-f0-9]{64}$/.test(row.sha256??''))fail('configuration');names.add(row.file);}if(!names.has(value.configurationEntry))fail('configuration');return {publicUrl,backend};
}
async function descriptors(pid){const values=[];for(const fd of await readdir('/proc/'+pid+'/fd'))try{const name='/proc/'+pid+'/fd/'+fd;values.push({target:await readlink(name),info:await lstat(name),fd:name});}catch{}return values;}
async function ownsPublicListener(processes,port,address){
 const inodes=new Set();for(const row of processes)for(const fd of await descriptors(row.pid)){const inode=/^socket:\[(\d+)\]$/.exec(fd.target)?.[1];if(inode)inodes.add(inode);}
 const expected=address.split('.').reverse().map(value=>Number(value).toString(16).padStart(2,'0')).join('').toUpperCase(),rows=(await readFile('/proc/'+processes[0].pid+'/net/tcp','utf8')).trim().split('\n').slice(1);
 const selected=rows.map(line=>line.trim().split(/\s+/)).filter(cells=>{const [ip,p]=cells[1].split(':');return cells[3]==='0A'&&parseInt(p,16)===port&&[expected,'00000000'].includes(ip)&&inodes.has(cells[9]);}),uids=new Set(selected.map(cells=>integer(cells[7])));if(!selected.length||uids.size!==1||![...uids].every(uid=>Number.isSafeInteger(uid)&&uid>=0&&uid<=0xffffffff))fail('listener_unowned');return [...uids][0];
}
async function inspect(record){
 if(record.recheck)await record.recheck();
 const {configuration:value,trusted}=record,{publicUrl,backend}=configuration(value),binary=await regular(value.binary.file,128*1024**2,trusted);if(sha(binary)!==value.binary.sha256)fail('binary_changed');
 const master=await processIdentity(value.master),workers=await Promise.all(value.workers.map(processIdentity));if(master.executable!==value.binary.file||workers.some(row=>row.parent!==master.pid||row.executable!==master.executable))fail('process_changed');
 const actualChildren=(await readFile('/proc/'+master.pid+'/task/'+master.pid+'/children','utf8')).trim().split(/\s+/).filter(Boolean).map(Number);if(actualChildren.length!==workers.length||actualChildren.some(pid=>!workers.some(row=>row.pid===pid)))fail('worker_generation_changed');
 const materials=new Map();let bytes=0;for(const row of value.materials){const body=await regular(row.file,row.role==='module'?128*1024**2:4*1024**2,trusted);bytes+=body.length;if(bytes>256*1024**2||sha(body)!==row.sha256)fail('material_changed');materials.set(row.file,{...row,body});if(row.role==='module'){const info=await lstat(row.file,{bigint:true});for(const worker of workers){const maps=(await readFile('/proc/'+worker.pid+'/maps','utf8')).split('\n'),mapped=maps.filter(line=>line.endsWith(' '+row.file));if(!mapped.length||mapped.some(line=>line.trim().split(/\s+/)[4]!==String(info.ino)))fail('loaded_module_unproven');}}}
 const visiting=new Set(),used=new Set();
 const expand=async file=>{if(visiting.has(file)||used.size>256)fail('configuration_include');const material=materials.get(file);if(material?.role!=='configuration')fail('configuration_include');visiting.add(file);used.add(file);const rows=syntax(material.body.toString('utf8')),result=[];
  for(const row of rows){if(row.name==='include'){if(row.args.length!==1||/[\*?\[\]$]/.test(row.args[0]))fail('configuration_include');result.push(...await expand(resolve(dirname(value.configurationEntry),row.args[0])));}else{if(row.name==='load_module'){if(row.args.length!==1)fail('configuration_module');const module=materials.get(resolve(value.prefix,row.args[0]));if(module?.role!=='module')fail('configuration_module');used.add(module.file);}if(row.children)row.children=await expandRows(row.children);result.push(row);}}
  visiting.delete(file);return result;
 };
 const expandRows=async rows=>{const result=[];for(const row of rows){if(row.name==='include'){if(row.args.length!==1||/[\*?\[\]$]/.test(row.args[0]))fail('configuration_include');result.push(...await expand(resolve(dirname(value.configurationEntry),row.args[0])));}else{if(row.children)row.children=await expandRows(row.children);result.push(row);}}return result;};
 const ast=await expand(value.configurationEntry);if(used.size!==materials.size)fail('material_not_loaded');const stream=one(ast,'stream'),servers=stream.children.filter(row=>row.name==='server'&&row.children?.some(child=>child.name==='listen'&&child.args.some(arg=>arg===publicUrl.port||arg.endsWith(':'+publicUrl.port))));if(servers.length!==1)fail('public_server');const server=servers[0],listen=one(server.children,'listen'),pass=one(server.children,'proxy_pass'),access=one(server.children,'access_log');
 if(listen.args.length!==1||![publicUrl.port,value.listenAddress+':'+publicUrl.port,'0.0.0.0:'+publicUrl.port].includes(listen.args[0])||pass.args.length!==1||pass.args[0]!==backend.host||['proxy_protocol','proxy_ssl'].some(name=>server.children.some(row=>row.name===name&&row.args[0]!=='off'))||access.args.length!==2||resolve(value.prefix,access.args[0])!==value.witness.file)fail('route_configuration');
 const formats=stream.children.filter(row=>row.name==='log_format'&&row.args[0]===access.args[1]);if(formats.length!==1||formats[0].args.length!==3||formats[0].args[1]!=='escape=json')fail('witness_format');let format;try{format=JSON.parse(formats[0].args[2]);}catch{fail('witness_format');}if(JSON.stringify(Object.entries(format).sort())!==JSON.stringify(Object.entries(fields).sort()))fail('witness_format');
 const commandBytes=await readFile('/proc/'+master.pid+'/cmdline');if(!/^[a-f0-9]{64}$/.test(value.master.commandSha256??'')||sha(commandBytes)!==value.master.commandSha256)fail('invocation_unproven');const cmd=commandBytes.toString().replaceAll('\0',' ');if(!cmd.startsWith('nginx: master process ')||!cmd.includes(value.binary.file)||!cmd.includes(value.configurationEntry))fail('invocation_unproven');
 await ownsPublicListener([master,...workers],Number(publicUrl.port),value.listenAddress);const witnessInfo=await lstat(value.witness.file,{bigint:true});if(trusted&&(witnessInfo.uid!==0n||witnessInfo.mode&0o022n))fail('witness_unprotected');if(!witnessInfo.isFile()||witnessInfo.isSymbolicLink()||await realpath(value.witness.file)!==value.witness.file)fail('witness_file');
 for(const worker of workers){let held=false;for(const fd of await readdir('/proc/'+worker.pid+'/fd'))try{const info=await (await import('node:fs/promises')).stat('/proc/'+worker.pid+'/fd/'+fd,{bigint:true});if(sameFile(info,witnessInfo))held=true;}catch{}if(!held)fail('witness_not_open');}
 let loadedConfigurationVerified=false;if(record.generation||trusted&&value.loadedGeneration){await inspectCapacityNginxLoadedGeneration(value,{generation:record.generation,trusted,artifact:record.artifact});loadedConfigurationVerified=true;}for(const row of [value.master,...value.workers])await processIdentity(row);
 return {scope:trusted?'root-approved-original-nginx-observation':'task-nginx-component-only',formalQualified:false,publicOrigin:value.publicOrigin,backendOrigin:value.backendOrigin,masterStartTicks:master.startTicks,workers:workers.length,diskConfigurationSha256:sha(Buffer.from(value.materials.map(row=>row.file+':'+row.sha256).join('\n'))),loadedConfigurationVerified,moduleMappingVerified:true,witnessBytes:Number(witnessInfo.size),witnessIdentity:{dev:String(witnessInfo.dev),ino:String(witnessInfo.ino)}};
}
const freeze=value=>{if(value&&typeof value==='object'){for(const child of Object.values(value))freeze(child);Object.freeze(value);}return value;};
function create(configurationValue,trusted,recheck,artifact,generation){configuration(configurationValue);const handle=Object.freeze(Object.create(null)),record=Object.freeze({configuration:freeze(structuredClone(configurationValue)),trusted,recheck,artifact,generation});sources.set(handle,record);return handle;}
// Diagnostic sources share the native read-only observer but can never acquire
// root provenance or a formal producer capability, even with identical JSON.
export function createCapacityNginxDiagnostic(configurationValue,generation){return create(configurationValue,false,null,null,generation);}
export async function inspectCapacityNginxSource(handle){const record=sources.get(handle);if(!record)fail('source_not_owned');return inspect(record);}
export function capacityNginxSourceBinding(handle){return sources.get(handle)??null;}
export async function readCapacityNginxWitness(handle,{offset,identity,maxBytes=1048576}={}){
 const record=sources.get(handle);if(!record||!Number.isSafeInteger(offset)||offset<0||!Number.isSafeInteger(maxBytes)||maxBytes<1||maxBytes>4*1024**2)fail('witness_configuration');const file=await open(record.configuration.witness.file,constants.O_RDONLY|constants.O_NOFOLLOW);try{const info=await file.stat({bigint:true});if(!identity||String(info.dev)!==identity.dev||String(info.ino)!==identity.ino||info.size<BigInt(offset)||info.size-BigInt(offset)>BigInt(maxBytes))fail('witness_changed');const buffer=Buffer.alloc(Number(info.size)-offset),read=await file.read(buffer,0,buffer.length,offset);if(read.bytesRead!==buffer.length)fail('witness_changed');const text=buffer.toString('utf8');if(text&&!text.endsWith('\n'))return {rows:[],pending:true};const rows=[];for(const line of text.split('\n').filter(Boolean)){let row;try{row=JSON.parse(line);}catch{fail('witness_invalid');}if(Object.keys(row).length!==Object.keys(fields).length||Object.keys(fields).some(key=>typeof row[key]!=='string')||!Number.isSafeInteger(integer(row.connection))||integer(row.remotePort)<1||integer(row.localPort)<1||!(integer(row.status)>=100&&integer(row.status)<=599)||!Number.isFinite(integer(row.sent))||!Number.isFinite(integer(row.received)))fail('witness_invalid');rows.push(Object.freeze(row));}return {rows,pending:false};}finally{await file.close();}
}
// Only an already authenticated runtime artifact can identify the fixed root
// topology file. A copied configuration or diagnostic source cannot enter here.
export async function loadCapacityTopologyConfiguration(artifact){
 await inspectCapacityRuntimeArtifact(artifact);const record=capacityRuntimeArtifactBinding(artifact),item=record.trust.topology;
 if(!item||!isAbsolute(item.file??'')||!/^[a-f0-9]{64}$/.test(item.sha256??''))fail('root_configuration_unavailable');
 const content=await regular(item.file,131072,true);if(sha(content)!==item.sha256)fail('root_configuration_changed');let value;try{value=JSON.parse(content);}catch{fail('root_configuration');}if(value?.schema!=='capacity-formal-topology-v1')fail('root_configuration');return freeze(value);
}
export async function loadCapacityApprovedNginxSource(artifact){const topology=await loadCapacityTopologyConfiguration(artifact),snapshot=JSON.stringify(topology.proxy);return create(topology.proxy,true,async()=>{const current=await loadCapacityTopologyConfiguration(artifact);if(JSON.stringify(current.proxy)!==snapshot)fail('root_configuration_changed');},artifact);}

// Native route inspection must run in the listener's actual network namespace;
// a /proc route table from a different namespace cannot qualify the public path.
export async function readCapacityNginxNetworkIdentity(handle){
 const record=sources.get(handle);if(!record)fail('source_not_owned');await inspect(record);const configurationValue=record.configuration,processes=await Promise.all([configurationValue.master,...configurationValue.workers].map(processIdentity)),namespace=await readlink('/proc/self/ns/net');for(const row of processes)if(await readlink('/proc/'+row.pid+'/ns/net')!==namespace)fail('network_namespace_unobserved');const socketUid=await ownsPublicListener(processes,Number(new URL(configurationValue.publicOrigin).port||443),configurationValue.listenAddress);for(const row of [configurationValue.master,...configurationValue.workers])await processIdentity(row);return Object.freeze({socketUid,namespace});
}
