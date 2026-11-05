import {readFile,readdir,statfs,realpath,stat} from 'node:fs/promises';
import {cpus,totalmem,freemem,hostname} from 'node:os';
import {createHash} from 'node:crypto';
import {isOwnedRuntime,ownedProcessProof} from './owned-runtime.mjs';
import {effectiveQuota,parseCpuSet} from './quota-graph.mjs';
import {targets} from './scenarios.mjs';
const observedRoles=new WeakSet();
export const capacityCpuPeriod=100000;
const defaultRole=Object.freeze({cpuQuota:4,memoryLimitBytes:8*1024**3});
const resourceFail=()=>{throw Error('capacity_resource_limits_invalid');};
export function capacityCpuQuotaUnits(value){const units=Math.round(value*capacityCpuPeriod);if(typeof value!=='number'||!Number.isFinite(value)||!Number.isSafeInteger(units)||units<1000||units/capacityCpuPeriod!==value)resourceFail();return units;}
// Only one relative IEEE-754 rounding unit is tolerated, never a CPU budget
// margin. Kernel quota deficits remain much larger than this machine precision.
export function capacityCpuQuotaAtLeast(actual,expected){return Number.isFinite(actual)&&actual>=0&&Number.isFinite(expected)&&expected>0&&(actual>=expected||expected-actual<=Number.EPSILON*Math.max(Math.abs(actual),Math.abs(expected)));}
export function normalizeCapacityResourceLimits(value){
 if(value===undefined)value={gatewayWorker:defaultRole,database:defaultRole};
 if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(key=>!['gatewayWorker','database','processLimits'].includes(key)))resourceFail();
 const role=input=>{if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).length!==2||!Object.hasOwn(input,'cpuQuota')||!Object.hasOwn(input,'memoryLimitBytes')||!Number.isSafeInteger(input.memoryLimitBytes)||input.memoryLimitBytes<=0)resourceFail();capacityCpuQuotaUnits(input.cpuQuota);return Object.freeze({cpuQuota:input.cpuQuota,memoryLimitBytes:input.memoryLimitBytes});};
 const gatewayWorker=role(value.gatewayWorker),database=role(value.database);let leaves=value.processLimits;
 if(leaves!==undefined&&(!leaves||typeof leaves!=='object'||Array.isArray(leaves)||Object.keys(leaves).some(key=>!['gateway','worker','database'].includes(key))||!Object.hasOwn(leaves,'gateway')||!Object.hasOwn(leaves,'worker')))resourceFail();
 const processLimits=Object.freeze({gateway:role(leaves===undefined?gatewayWorker:leaves.gateway),worker:role(leaves===undefined?gatewayWorker:leaves.worker),database:role(leaves===undefined||!Object.hasOwn(leaves,'database')?database:leaves.database)});
 for(const [name,leaf] of Object.entries(processLimits)){const parent=name==='database'?database:gatewayWorker;if(leaf.cpuQuota>parent.cpuQuota||leaf.memoryLimitBytes>parent.memoryLimitBytes)resourceFail();}
 if(!Number.isSafeInteger(gatewayWorker.memoryLimitBytes+database.memoryLimitBytes)||!Number.isSafeInteger(capacityCpuQuotaUnits(gatewayWorker.cpuQuota)+capacityCpuQuotaUnits(database.cpuQuota)))resourceFail();
 return Object.freeze({gatewayWorker,database,processLimits});
}
export function capacityResourceTotals(value){const limits=normalizeCapacityResourceLimits(value);return {cpuQuota:(capacityCpuQuotaUnits(limits.gatewayWorker.cpuQuota)+capacityCpuQuotaUnits(limits.database.cpuQuota))/capacityCpuPeriod,memoryLimitBytes:limits.gatewayWorker.memoryLimitBytes+limits.database.memoryLimitBytes};}
export function assessCapacityProcessLimits(roles,resourceLimits){
 const limits=normalizeCapacityResourceLimits(resourceLimits),missing=[];
 for(const name of ['gatewayWorker','database'])for(const process of roles[name]?.processes??[]){const expected=limits.processLimits[process.role],node=roles[name].nodes?.find(row=>row.identity===process.cgroupIdentity);if(!expected||!node||node.cpuQuota!==expected.cpuQuota||node.memoryLimitBytes!==expected.memoryLimitBytes)missing.push('实际进程叶子配额不匹配:'+process.role);}
 return missing;
}
const text=async path=>{try{return (await readFile(path,'utf8')).trim();}catch{return null;}};
export async function collectHostEvidence({directory='/tmp',cgroupRoot='/sys/fs/cgroup'}={}){
  const [quota,memory,affinity,fs]=await Promise.all([text(cgroupRoot+'/cpu.max'),text(cgroupRoot+'/memory.max'),text(cgroupRoot+'/cpuset.cpus.effective'),statfs(directory,{bigint:true})]);
  const memInfo=await text('/proc/meminfo'),available=/^MemAvailable:\s+(\d+) kB/m.exec(memInfo??'');
  return {observedAt:Date.now(),source:'local-os-read',hostHash:createHash('sha256').update(hostname()).digest('hex').slice(0,16),node:process.version,cpuLogical:cpus().length,memoryTotal:totalmem(),memoryAvailable:available?Number(available[1])*1024:freemem(),cgroup:{root:cgroupRoot,cpuMax:quota,memoryMax:memory,affinity},disk:{total:Number(fs.blocks*fs.bsize),free:Number(fs.bavail*fs.bsize)},formalIsolationVerified:false};
}
export async function collectProcessEvidence(pid,{procRoot='/proc'}={}){
  if(!Number.isSafeInteger(pid)||pid<1)throw new Error('capacity_pid_invalid');const root=procRoot+'/'+pid,[status,io,rawStat,cgroup,fds]=await Promise.all([text(root+'/status'),text(root+'/io'),text(root+'/stat'),text(root+'/cgroup'),readdir(root+'/fd').catch(()=>null)]);if(!status||!rawStat)return {pid,available:false,observedAt:Date.now()};
  const after=rawStat.slice(rawStat.lastIndexOf(')')+2).split(' '),field=name=>{const match=new RegExp('^'+name+':\\s+(\\d+)','m').exec(status);return match?Number(match[1]):null;};
  return {pid,available:true,observedAt:Date.now(),startTicks:after[19],userTicks:Number(after[11]),systemTicks:Number(after[12]),rssBytes:(field('VmRSS')??0)*1024,threads:field('Threads'),fdCount:fds?.length??null,cgroup,io:io?Object.fromEntries(io.split('\n').map(line=>{const [key,value]=line.split(':');return [key,Number(value.trim())];})):null};
}
export async function collectCgroupRole(path,{processes=[]}={}){
  const checked=processes.map(entry=>({runtime:entry.runtime,child:entry.child,proof:isOwnedRuntime(entry.runtime)?ownedProcessProof(entry.runtime,entry.child):null}));const root='/sys/fs/cgroup',location=await realpath(path),nodes=new Map(),roleProofs=[];let mappingVerified=processes.length>0;
  if(!(location===root||location.startsWith(root+'/')))throw new Error('capacity_cgroup_path_invalid');
  const identity=path=>createHash('sha256').update(path).digest('hex');
  async function chain(start){let current=start,first;const ancestors=[];for(let depth=0;;depth++){if(depth>=64||nodes.size>=256&&!nodes.has(current))throw new Error('capacity_resource_graph_budget');let row=nodes.get(current);if(!row){const [cpuMax,memoryMax,cpuset]=await Promise.all([text(current+'/cpu.max'),text(current+'/memory.max'),text(current+'/cpuset.cpus.effective')]);const parts=(cpuMax??'').split(/\s+/),quota=Number(parts[0]),period=Number(parts[1]);if(!(current===root&&cpuMax===null)&&(!(parts[0]==='max'||Number.isFinite(quota)&&quota>0)||!Number.isFinite(period)||period<=0)||!(current===root&&memoryMax===null||memoryMax==='max'||/^\d+$/.test(memoryMax??'')))throw new Error('capacity_cgroup_limit_missing');const parent=current===root?null:current.slice(0,current.lastIndexOf('/'));row=Object.freeze({identity:identity(current),parentIdentity:parent?identity(parent):null,cpuQuota:cpuMax===null||parts[0]==='max'?null:quota/period,memoryLimitBytes:memoryMax===null||memoryMax==='max'?null:Number(memoryMax),cpuIds:Object.freeze(parseCpuSet(cpuset))});nodes.set(current,row);}first??=row;ancestors.push(row);if(current===root)break;current=current.slice(0,current.lastIndexOf('/'));}return {first,ancestors};}
  const selected=await chain(location),[usage,memory,members]=await Promise.all([text(location+'/cpu.stat'),text(location+'/memory.current'),text(location+'/cgroup.procs')]);
  for(const entry of checked){if(!entry.proof){mappingVerified=false;continue;}const proof=entry.proof,[membership,status]=await Promise.all([text('/proc/'+proof.pid+'/cgroup'),text('/proc/'+proof.pid+'/status')]),actual=/^0::(.*)$/m.exec(membership??'')?.[1],relative=location.slice(root.length)||'/';if(!actual||!(relative==='/'||actual===relative||actual.startsWith(relative+'/'))){mappingVerified=false;continue;}const actualPath=await realpath(root+actual);if(!(actualPath===location||actualPath.startsWith(location+'/'))){mappingVerified=false;continue;}const measured=await chain(actualPath),affinity=/^Cpus_allowed_list:\s*(.*)$/m.exec(status??'')?.[1];ownedProcessProof(entry.runtime,entry.child);roleProofs.push(Object.freeze({pid:proof.pid,role:proof.role,cgroupIdentity:measured.first.identity,cpuIds:Object.freeze(parseCpuSet(affinity))}));}
  const proof={source:'observed-cgroup',cgroupIdentity:selected.first.identity,cpuQuota:selected.first.cpuQuota,memoryLimitBytes:selected.first.memoryLimitBytes,ancestors:Object.freeze(selected.ancestors),nodes:Object.freeze([...nodes.values()]),processes:Object.freeze(roleProofs),cpuUsageUs:Number(/^usage_usec (\d+)/m.exec(usage??'')?.[1]??NaN),memoryCurrentBytes:/^\d+$/.test(memory??'')?Number(memory):null,processCount:members?members.split('\n').length:0,mappingVerified,processRoles:Object.freeze(roleProofs.map(row=>row.role)),observedAt:Date.now()};Object.freeze(proof);observedRoles.add(proof);return proof;
}
export function assessCgroupHierarchy({host,roles={},resourceLimits}={}){
  const limits=normalizeCapacityResourceLimits(resourceLimits),total=capacityResourceTotals(limits),missing=[],names=['gatewayWorker','database'],cpuRequired=total.cpuQuota,memoryRequired=total.memoryLimitBytes;
  for(const name of names){const role=roles[name];if(!role)continue;for(const parent of role.ancestors??[])if(parent.cpuQuota!==null&&!capacityCpuQuotaAtLeast(parent.cpuQuota,limits[name].cpuQuota)||parent.memoryLimitBytes!==null&&parent.memoryLimitBytes<limits[name].memoryLimitBytes)missing.push('有效祖先配额不足:'+name);}
  const left=roles.gatewayWorker,right=roles.database;
  if(left&&right){if((left.ancestors??[]).some(row=>row.identity===right.cgroupIdentity)||(right.ancestors??[]).some(row=>row.identity===left.cgroupIdentity))missing.push('角色cgroup相同或祖孙重叠');const common=(left.ancestors??[]).filter(row=>(right.ancestors??[]).some(other=>other.identity===row.identity));for(const parent of common)if(parent.cpuQuota!==null&&!capacityCpuQuotaAtLeast(parent.cpuQuota,cpuRequired)||parent.memoryLimitBytes!==null&&parent.memoryLimitBytes<memoryRequired)missing.push('共同祖先不足以提供声明的角色合计配额');}
  if(!host||!capacityCpuQuotaAtLeast(host.cpuLogical,cpuRequired)||host.memoryTotal<memoryRequired)missing.push('宿主总容量不足以提供独立资源');
  const current=(left?.memoryCurrentBytes??0)+(right?.memoryCurrentBytes??0);if(!Number.isFinite(host?.memoryAvailable)||host.memoryAvailable+current<memoryRequired*targets.memoryFraction)missing.push('宿主可用内存及角色现有用量不足以覆盖正常80%预算');
  return missing;
}
export function resourcePreflight({host,roles={},dataset={},protocol={},operations=[],resourceLimits}={}){
  const limits=normalizeCapacityResourceLimits(resourceLimits),total=capacityResourceTotals(limits),missing=[];for(const role of ['gatewayWorker','database']){const proof=roles[role];if(!proof||!observedRoles.has(proof)||proof.source!=='observed-cgroup'||!proof.cgroupIdentity||proof.cpuQuota!==limits[role].cpuQuota||proof.memoryLimitBytes!==limits[role].memoryLimitBytes||!proof.mappingVerified||!proof.processRoles?.includes(role==='database'?'database':'gateway')||role==='gatewayWorker'&&!proof.processRoles?.includes('worker'))missing.push('缺少'+role+'声明配额的独立实测证据');}
  missing.push(...assessCgroupHierarchy({host,roles,resourceLimits:limits}),...assessCapacityProcessLimits(roles,limits));
  try{for(const name of ['gatewayWorker','database']){const quota=effectiveQuota({nodes:roles[name]?.nodes,processes:roles[name]?.processes,host});if(!capacityCpuQuotaAtLeast(quota.cpuQuota,limits[name].cpuQuota)||quota.memoryLimitBytes<limits[name].memoryLimitBytes)missing.push('实际进程子组/CPU亲和性资源不足:'+name);}const merged=new Map();for(const role of Object.values(roles))for(const node of role.nodes??[]){if(merged.has(node.identity)&&JSON.stringify(merged.get(node.identity))!==JSON.stringify(node))throw new Error('capacity_resource_graph_inconsistent');merged.set(node.identity,node);}const quota=effectiveQuota({nodes:[...merged.values()],processes:Object.values(roles).flatMap(role=>role.processes??[]),host});if(!capacityCpuQuotaAtLeast(quota.cpuQuota,total.cpuQuota)||quota.memoryLimitBytes<total.memoryLimitBytes)missing.push('实际进程资源共享后不足声明的角色合计配额');}catch{missing.push('缺实际进程完整cgroup/CPU亲和性证据');}

  if(!host||!/^v24\./.test(host.node??''))missing.push('未验证与交付一致的Node版本');if(!dataset.verified||dataset.metadata7d<targets.metadata7d||dataset.metadata30d<targets.metadata30d||!dataset.bodyRequests||!dataset.bodyBytes||!dataset.bodyDistributionVerified||!dataset.formalBodyFootprintVerified)missing.push('正式数据量或正文分布未验证');
  for(const key of ['tls','deviceEnforced','realHelper','proxyPreserved','metering','recording','worker','backgroundJobs','indexVersionVerified','databaseVersionVerified','loadClientLanBaseline'])if(protocol[key]!==true)missing.push('入口/功能证据缺失:'+key);
  for(const operation of operations)if(!operation.method||!operation.path||operation.path.includes('{'))missing.push('实际API尚未绑定:'+operation.id);
  return {eligible:missing.length===0,status:missing.length?'blocked':'ready',missing,host,roles,dataset,protocol,resourceLimits:limits};
}
export async function collectDatabaseEvidence(store){
  return store.transaction(async()=>{
    const version=await store.get('SELECT version() database_version,current_setting(\'server_version_num\') version_number'),activity=await store.get("SELECT COUNT(*) connections,COUNT(*) FILTER(WHERE wait_event_type='Lock') lock_waiters,COUNT(*) FILTER(WHERE state='active') active FROM pg_stat_activity WHERE datname=current_database()"),database=await store.get('SELECT numbackends,xact_commit,xact_rollback,blks_read,blks_hit,temp_bytes,deadlocks,blk_read_time,blk_write_time FROM pg_stat_database WHERE datname=current_database()'),size=await store.get('SELECT pg_database_size(current_database()) bytes'),indexes=await store.all("SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='harbor' ORDER BY indexname");
    return {at:Date.now(),version,activity,database,size,indexHash:createHash('sha256').update(JSON.stringify(indexes)).digest('hex'),indexCount:indexes.length};
  },{level:'REPEATABLE READ',readOnly:true});
}
