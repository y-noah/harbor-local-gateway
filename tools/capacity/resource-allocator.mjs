import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,mkdir,readFile,writeFile,lstat,statfs,realpath,rmdir,rm,chmod,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {isOwnedRuntime,ownedProcessProof,startOwnedCapacityChild,terminateOwnedCapacityChild} from './owned-runtime.mjs';
import {collectHostEvidence,collectCgroupRole,assessCgroupHierarchy} from './resources.mjs';
import {effectiveQuota} from './quota-graph.mjs';
import {targets} from './scenarios.mjs';

const exec=promisify(execFile),allocators=new WeakSet(),cgMagic=0x63677270n;
const roleNames={gateway:'gateway-worker',worker:'gateway-worker',database:'database'};
const limits=Object.freeze({cpuQuota:4,memoryLimitBytes:8*1024**3});
const fail=code=>{throw new Error(code);};
const text=async path=>(await readFile(path,'utf8')).trim();
const same=(left,right)=>left.dev===right.dev&&left.ino===right.ino;

export function assessCapacityAncestorHeadroom({limitBytes,currentBytes,ownedBytes=0n}={}){
 const valid=value=>typeof value==='bigint'&&value>=0n;if(![limitBytes,currentBytes,ownedBytes].every(valid)||limitBytes===0n||ownedBytes>currentBytes)return {passed:null,reason:'ancestor_memory_sample_unknown'};
 const other=currentBytes-ownedBytes,available=limitBytes>other?limitBytes-other:0n,required=BigInt(Math.ceil(2*limits.memoryLimitBytes*targets.memoryFraction));
 return {passed:available>=required,availableBytes:String(available),requiredBytes:String(required)};
}

async function ancestorHeadroom(delegation,ownedBytes){
 const rows=[];let path=delegation;
 for(;;){let limit;try{limit=await text(join(path,'memory.max'));}catch(error){if(path!=='/sys/fs/cgroup'||error.code!=='ENOENT')throw error;limit='max';}
  if(limit!=='max'){const current=await text(join(path,'memory.current'));if(!/^\d+$/.test(limit)||!/^\d+$/.test(current))fail('capacity_resource_memory_unavailable');const result=assessCapacityAncestorHeadroom({limitBytes:BigInt(limit),currentBytes:BigInt(current),ownedBytes});rows.push(result);}
  if(path==='/sys/fs/cgroup')return rows;path=path.slice(0,path.lastIndexOf('/'));
 }
}

export const isOwnedCapacityResourceAllocator=value=>allocators.has(value);

export async function buildCapacityResourceEntry({compiler='/usr/bin/cc',signal}={}){
 if(process.platform!=='linux'||!isAbsolute(compiler))fail('capacity_resource_platform_unsupported');
 const directory=await mkdtemp(join(tmpdir(),'harbor-capacity-resource-entry-')),binary=join(directory,'entry'),identity=await lstat(directory,{bigint:true});
 try{
  await chmod(directory,0o700);
  await exec(compiler,['-std=c11','-O2','-Wall','-Wextra','-Werror',fileURLToPath(new URL('./resource-entry.c',import.meta.url)),'-o',binary],{signal,maxBuffer:65536,timeout:30000});
  await chmod(binary,0o700);const binaryIdentity=await lstat(binary,{bigint:true});let closed=false;
  return Object.freeze({binary,close:async()=>{if(closed)return;const actual=await lstat(directory,{bigint:true}),file=await lstat(binary,{bigint:true});if(!actual.isDirectory()||!same(actual,identity)||!file.isFile()||!same(file,binaryIdentity)||(await readdir(directory)).some(name=>name!=='entry'))fail('capacity_resource_entry_cleanup_ownership');await rm(directory,{recursive:true});closed=true;}});
 }catch{await rm(directory,{recursive:true,force:true});fail('capacity_resource_entry_build_failed');}
}

// This allocator creates only nonce-named descendants of a delegated cgroup.
// It never changes the delegation's controllers, limits or existing processes.
export async function createCapacityResourceAllocator({delegatedRoot,compiler,signal,maxChildGroups=128}={}){
 if(process.platform!=='linux'||typeof delegatedRoot!=='string'||!isAbsolute(delegatedRoot)||!Number.isSafeInteger(maxChildGroups)||maxChildGroups<1||maxChildGroups>10000)fail('capacity_resource_delegation_invalid');
 signal?.throwIfAborted();
 const delegation=await realpath(delegatedRoot),identity=await lstat(delegatedRoot,{bigint:true}),filesystem=await statfs(delegation,{bigint:true});
 if(delegation!==delegatedRoot||!(delegation==='/sys/fs/cgroup'||delegation.startsWith('/sys/fs/cgroup/'))||identity.isSymbolicLink()||!identity.isDirectory()||filesystem.type!==cgMagic||identity.uid!==BigInt(process.getuid()))fail('capacity_resource_delegation_invalid');
 const controllers=new Set((await text(join(delegation,'cgroup.controllers'))).split(/\s+/)),enabled=new Set((await text(join(delegation,'cgroup.subtree_control'))).split(/\s+/));
 if(!['cpu','memory','cpuset'].every(name=>controllers.has(name)&&enabled.has(name)))fail('capacity_resource_controllers_not_delegated');
 const host=await collectHostEvidence();
 if(assessCgroupHierarchy({host}).length)fail('capacity_resource_host_insufficient');
 const parent=await collectCgroupRole(delegation);
 if(parent.ancestors.some(row=>row.cpuQuota!==null&&row.cpuQuota<8||row.memoryLimitBytes!==null&&row.memoryLimitBytes<2*limits.memoryLimitBytes||row.cpuIds.length<8))fail('capacity_resource_parent_insufficient');
 if((await ancestorHeadroom(delegation,0n)).some(row=>row.passed!==true))fail('capacity_resource_parent_headroom_insufficient');
 const root=join(delegation,'harbor-capacity-'+randomUUID()),groups=new Map(),children=[],created=[],pendingStarts=new Set();let entry,closing=false,closed=false,closePromise,childGroups=0;
 const check=async(path,expected)=>{const actual=await lstat(path,{bigint:true});if(!actual.isDirectory()||actual.isSymbolicLink()||!same(actual,expected)||(await statfs(path,{bigint:true})).type!==cgMagic)fail('capacity_resource_ownership_changed');};
 const stopAllocation=async row=>{
  await terminateOwnedCapacityChild(row.runtime,row.child);
  const group=row.group;if(group.removed)return;await check(group.path,group.identity);
  const populated=async()=>/^populated\s+1$/m.test(await text(join(group.path,'cgroup.events')));
  if(await populated())await writeFile(join(group.path,'cgroup.kill'),'1\n');
  const deadline=performance.now()+5000;while(await populated()){if(performance.now()>=deadline)fail('capacity_resource_descendant_exit_timeout');await delay(10);}
 };
 const cleanup=async()=>{
  const errors=[];
  await Promise.allSettled([...pendingStarts]);
  for(const row of children)try{await stopAllocation(row);}catch{errors.push('child');}
  for(const row of [...created].reverse())if(!row.removed)try{await check(row.path,row.identity);if((await text(join(row.path,'cgroup.events'))).split('\n').some(line=>/^populated\s+1$/.test(line)))fail('capacity_resource_group_populated');await rmdir(row.path);row.removed=true;}catch{errors.push('group');}
  if(!errors.length)await entry?.close();
  if(errors.length)fail('capacity_resource_cleanup_incomplete');closed=true;
 };
 try{
  await mkdir(root);created.push({path:root,identity:await lstat(root,{bigint:true})});
  await writeFile(join(root,'cpu.max'),'800000 100000\n');await writeFile(join(root,'memory.max'),String(2*limits.memoryLimitBytes)+'\n');
  await writeFile(join(root,'cgroup.subtree_control'),'+cpu +memory +cpuset\n');
  for(const name of ['gateway-worker','database']){
   signal?.throwIfAborted();const path=join(root,name);await mkdir(path);const group={path,identity:await lstat(path,{bigint:true})};created.push(group);groups.set(name,group);
   await writeFile(join(path,'cpu.max'),'400000 100000\n');await writeFile(join(path,'memory.max'),String(limits.memoryLimitBytes)+'\n');
   await writeFile(join(path,'cgroup.subtree_control'),'+cpu +memory +cpuset\n');
  }
  entry=await buildCapacityResourceEntry({compiler,signal});
 }catch(error){try{await cleanup();}catch{fail('capacity_resource_setup_cleanup_incomplete');}throw error;}
 const validate=async group=>{
  signal?.throwIfAborted();if(closing||closed)fail('capacity_resource_allocator_closed');await check(root,created[0].identity);await check(group.path,group.identity);
  if(await text(join(root,'cpu.max'))!=='800000 100000'||await text(join(root,'memory.max'))!==String(2*limits.memoryLimitBytes)||await text(join(group.path,'cpu.max'))!=='400000 100000'||await text(join(group.path,'memory.max'))!==String(limits.memoryLimitBytes))fail('capacity_resource_limits_changed');
 };
 const allocator=Object.freeze({limits,
  start:(runtime,command,args,options={})=>{
   const task=(async()=>{
   const group=groups.get(roleNames[options.role]);if(!isOwnedRuntime(runtime)||!group||!isAbsolute(command)||!Array.isArray(args)||args.some(arg=>typeof arg!=='string'))fail('capacity_resource_spawn_invalid');
   if(++childGroups>maxChildGroups)fail('capacity_resource_child_group_budget');await validate(group);if(closing||closed)fail('capacity_resource_allocator_closed');
   const path=join(group.path,'launch-'+randomUUID());await mkdir(path);const allocation={path,identity:await lstat(path,{bigint:true})};created.push(allocation);
   await writeFile(join(path,'cpu.max'),'400000 100000\n');await writeFile(join(path,'memory.max'),String(limits.memoryLimitBytes)+'\n');await lstat(join(path,'cgroup.kill'));
   if(closing||closed)fail('capacity_resource_allocator_closed');const child=startOwnedCapacityChild(runtime,entry.binary,[path,String(allocation.identity.dev),String(allocation.identity.ino),command,...args],options),record={runtime,child,role:options.role,group:allocation};children.push(record);
   const deadline=performance.now()+5000,expected=path.slice('/sys/fs/cgroup'.length)||'/';
   try{for(;;){signal?.throwIfAborted();if(closing)fail('capacity_resource_allocator_closed');ownedProcessProof(runtime,child);const membership=/^0::(.*)$/m.exec(await text('/proc/'+child.pid+'/cgroup'))?.[1];if(membership===expected)return child;if(performance.now()>=deadline)fail('capacity_resource_entry_timeout');await delay(10,undefined,{signal});}}
   catch{await stopAllocation(record);fail('capacity_resource_process_not_entered');}
   })();pendingStarts.add(task);task.finally(()=>pendingStarts.delete(task)).catch(()=>{});return task;
  },
  observe:async()=>{
   for(const group of groups.values())await validate(group);
   const processes=children.filter(row=>row.child.pid&&row.child.exitCode===null&&row.child.signalCode===null).map(row=>({...row,proof:ownedProcessProof(row.runtime,row.child)})),roles={};
   for(const [name,group] of groups)roles[name==='database'?'database':'gatewayWorker']=await collectCgroupRole(group.path,{processes:processes.filter(row=>roleNames[row.proof.role]===name).map(({runtime,child})=>({runtime,child}))});
   const observedHost=await collectHostEvidence(),missing=assessCgroupHierarchy({host:observedHost,roles});
   for(const [name,required] of [['database',['database']],['gatewayWorker',['gateway','worker']]])if(!roles[name].mappingVerified||required.some(role=>!roles[name].processRoles.includes(role)))missing.push('owned_role_process_missing:'+name);
   for(const [name,role] of Object.entries(roles)){if(!role.processes.length){missing.push('owned_role_effective_quota_unknown:'+name);continue;}const quota=effectiveQuota({...role,host:observedHost});if(quota.cpuQuota<4||quota.memoryLimitBytes<limits.memoryLimitBytes)missing.push('owned_role_effective_quota_insufficient:'+name);}
   const nodes=new Map();for(const role of Object.values(roles))for(const node of role.nodes){if(nodes.has(node.identity)&&JSON.stringify(nodes.get(node.identity))!==JSON.stringify(node))fail('capacity_resource_graph_changed');nodes.set(node.identity,node);}
   const combinedProcesses=Object.values(roles).flatMap(role=>role.processes);if(!combinedProcesses.length)missing.push('owned_combined_effective_quota_unknown');else{const combined=effectiveQuota({host:observedHost,nodes:[...nodes.values()],processes:combinedProcesses});if(combined.cpuQuota<8||combined.memoryLimitBytes<2*limits.memoryLimitBytes)missing.push('owned_combined_effective_quota_insufficient');}
   const ownCurrent=await text(join(root,'memory.current'));if(!/^\d+$/.test(ownCurrent))fail('capacity_resource_memory_unavailable');const headroom=await ancestorHeadroom(delegation,BigInt(ownCurrent));if(headroom.some(row=>row.passed!==true))missing.push('ancestor_memory_headroom_insufficient_or_unknown');
   return {passed:missing.length?null:true,roles,host:observedHost,headroom,missing,scope:'owned-role-allocation-only',formalCapacityProven:false};
  },
  close:()=>closePromise??=(async()=>{closing=true;try{await cleanup();}catch(error){closePromise=null;throw error;}})()
 });
 allocators.add(allocator);return allocator;
}
