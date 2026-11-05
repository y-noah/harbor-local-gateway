import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {fileURLToPath} from 'node:url';
import {mkdtemp,lstat,readFile,writeFile,rm,symlink,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {createCapacityResourceAllocator,isOwnedCapacityResourceAllocator,buildCapacityResourceEntry,assessCapacityAncestorHeadroom} from '../tools/capacity/resource-allocator.mjs';
import {normalizeCapacityResourceLimits,capacityResourceTotals,capacityCpuQuotaUnits,capacityCpuQuotaAtLeast} from '../tools/capacity/resources.mjs';
import {createOwnedRuntime,startOwnedCapacityChild,terminateOwnedCapacityChild,ownedProcessProof} from '../tools/capacity/owned-runtime.mjs';
const exec=promisify(execFile),linux={skip:process.platform!=='linux'?'需Linux真实内核文件系统校验':false,timeout:30000};

async function running(runtime,role='worker',{ignoreTerm=false}={}){
 const code=(ignoreTerm?'process.on("SIGTERM",()=>{});':'')+'process.send("ready");setInterval(()=>{},1000)',child=startOwnedCapacityChild(runtime,process.execPath,['-e',code],{role,stdio:['ignore','pipe','pipe','ipc']});
 await new Promise((resolve,reject)=>{const timer=setTimeout(()=>{done();reject(new Error('test_child_ready_timeout'));},5000),ready=()=>{done();resolve();},error=value=>{done();reject(value);},done=()=>{clearTimeout(timer);child.off('message',ready);child.off('error',error);};child.once('message',ready);child.once('error',error);});return child;
}

test('ancestor headroom deducts unrelated usage without double-counting the allocator own memory',()=>{
 const GiB=1024n**3n;
 assert.equal(assessCapacityAncestorHeadroom({limitBytes:16n*GiB,currentBytes:15n*GiB}).passed,false);
 assert.equal(assessCapacityAncestorHeadroom({limitBytes:16n*GiB,currentBytes:15n*GiB,ownedBytes:14n*GiB}).passed,true);
 assert.equal(assessCapacityAncestorHeadroom({limitBytes:16n*GiB,currentBytes:10n*GiB,ownedBytes:11n*GiB}).passed,null);
 assert.equal(assessCapacityAncestorHeadroom({limitBytes:16n*GiB,currentBytes:NaN}).passed,null);
});

test('private start and termination ignore replaced public methods and preserve unrelated children',linux,async()=>{
 const runtime=createOwnedRuntime();try{
  const unrelated=await running(runtime,'worker');let publicStartUsed=false,publicStopUsed=false;
  runtime.start=()=>{publicStartUsed=true;return unrelated;};runtime.stop=()=>{publicStopUsed=true;throw new Error('must_not_use_public_stop');};runtime.proof=()=>({pid:unrelated.pid,role:'database'});
  const own=await running(runtime,'gateway');assert.notEqual(own.pid,unrelated.pid);assert.equal(ownedProcessProof(runtime,own).role,'gateway');
  await terminateOwnedCapacityChild(runtime,own,{graceMs:50});assert.equal(publicStartUsed,false);assert.equal(publicStopUsed,false);assert.ok(own.exitCode!==null||own.signalCode!==null);assert.equal(unrelated.exitCode,null);assert.equal(unrelated.signalCode,null);
  await terminateOwnedCapacityChild(runtime,own);assert.throws(()=>startOwnedCapacityChild({...runtime},process.execPath,[],{role:'worker'}),/not_owned/);
 }finally{await runtime.close();}
});

test('bounded owned termination resumes a stopped worker and escalates an ignored TERM without stopping its peer',linux,async()=>{
 const runtime=createOwnedRuntime();try{
  const peer=await running(runtime),stopped=await running(runtime);process.kill(stopped.pid,'SIGSTOP');const deadline=performance.now()+5000;
  while(!/^State:\s+T/m.test(await readFile('/proc/'+stopped.pid+'/status','utf8'))){if(performance.now()>=deadline)throw new Error('test_worker_not_stopped');await delay(10);}
  await terminateOwnedCapacityChild(runtime,stopped,{graceMs:50});assert.ok(stopped.exitCode!==null||stopped.signalCode!==null);
  const stubborn=await running(runtime,'worker',{ignoreTerm:true});await terminateOwnedCapacityChild(runtime,stubborn,{graceMs:25});assert.equal(stubborn.signalCode,'SIGKILL');assert.equal(peer.exitCode,null);assert.equal(peer.signalCode,null);
  await assert.rejects(terminateOwnedCapacityChild(runtime,{...peer}),/not_owned/);
 }finally{await runtime.close();}
});

test('a writable ordinary directory or symlink cannot masquerade as delegated cgroup resources',linux,async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-capacity-delegation-test-'));try{
  await writeFile(join(root,'cgroup.controllers'),'cpu memory');await writeFile(join(root,'cgroup.subtree_control'),'cpu memory');
  const before=await readdir(root);await assert.rejects(createCapacityResourceAllocator({delegatedRoot:root}),/delegation_invalid/);assert.deepEqual(await readdir(root),before);
  await symlink(root,join(root,'alias'));await assert.rejects(createCapacityResourceAllocator({delegatedRoot:join(root,'alias')}),/delegation_invalid/);
  assert.equal(isOwnedCapacityResourceAllocator({start:()=>{},observe:()=>{}}),false);
 }finally{await rm(root,{recursive:true,force:true});}
});

test('native entry refuses wrong filesystems and identities before executing any model or database payload',linux,async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-capacity-entry-test-'));let entry;try{
  entry=await buildCapacityResourceEntry();const marker=join(root,'must-not-execute'),identity=await lstat(root,{bigint:true});await writeFile(join(root,'cgroup.procs'),'untouched');
  const args=[root,String(identity.dev),String(identity.ino),'100000','100000','2147483648',process.execPath,'-e','require("node:fs").writeFileSync(process.argv[1],"executed")',marker,'synthetic-private-argument'];
  for(const values of [args,[...args.slice(0,1),'bad-number',...args.slice(2)],...[['100000','100000','0'],['999','100000','1'],['1000','1','1'],[' 1000','100000','1']].map(limits=>[...args.slice(0,3),...limits,...args.slice(6)])])await assert.rejects(exec(entry.binary,values),error=>{assert.equal(error.code,126);assert.equal(error.stderr,'capacity_resource_entry_failed\n');assert.ok(!error.stderr.includes('synthetic-private-argument'));return true;});
  await assert.rejects(lstat(marker),{code:'ENOENT'});assert.equal(await readFile(join(root,'cgroup.procs'),'utf8'),'untouched');
  await assert.rejects(exec(entry.binary,[]),error=>error.code===126);
 }finally{await entry?.close();await rm(root,{recursive:true,force:true});}
});

test('native limit contract accepts declared default and fractional leaves and rejects changed values',linux,async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-capacity-native-contract-'));try{
  const source=join(root,'contract.c'),binary=join(root,'contract'),entry=fileURLToPath(new URL('../tools/capacity/resource-entry.c',import.meta.url));
  await writeFile(source,'#define main production_main\n#include '+JSON.stringify(entry)+'\n#undef main\nint main(int argc,char **argv){uintmax_t q,p,m;if(argc!=5||!number(argv[2],&q)||!number(argv[3],&p)||!number(argv[4],&m))return 2;int d=open(argv[1],O_RDONLY|O_DIRECTORY);return d<0||!limits_match(d,q,p,m); }\n');
  await exec('/usr/bin/cc',['-std=c11','-O2','-Wall','-Wextra','-Werror',source,'-o',binary]);
  await writeFile(join(root,'cgroup.procs'),'untouched');
  for(const [quota,memory] of [[400000,8*1024**3],[100000,2*1024**3],[50000,0.5*1024**3],[30000,2*1024**3],[60000,2*1024**3],[50000,1024**3]]){
   await writeFile(join(root,'cpu.max'),quota+' 100000\n');await writeFile(join(root,'memory.max'),memory+'\n');
   await exec(binary,[root,String(quota),'100000',String(memory)]);
   for(const limits of [[quota+1,100000,memory],[quota,100001,memory],[quota,100000,memory+1]])await assert.rejects(exec(binary,[root,...limits.map(String)]),error=>error.code===1);
  }
  assert.equal(await readFile(join(root,'cgroup.procs'),'utf8'),'untouched');
 }finally{await rm(root,{recursive:true,force:true});}
});

test('compiled native entry cleanup preserves unexpected files and remains retryable',linux,async()=>{
 const entry=await buildCapacityResourceEntry(),extra=join(dirname(entry.binary),'other-owner-material');try{
  await writeFile(extra,'preserve');await assert.rejects(entry.close(),/cleanup_ownership/);assert.equal(await readFile(extra,'utf8'),'preserve');await rm(extra);await entry.close();await entry.close();await assert.rejects(lstat(entry.binary),{code:'ENOENT'});
 }finally{await rm(extra,{force:true});await entry.close();}
});

test('compiler failure reports only the fixed setup code and no compiler output',linux,async()=>{
 await assert.rejects(buildCapacityResourceEntry({compiler:'/no-such-capacity-compiler'}),error=>error.message==='capacity_resource_entry_build_failed');
});

const delegatedRoot=process.env.HARBOR_CAPACITY_DELEGATED_CGROUP;
test('real delegated cgroup entry precedes payload execution and includes all three owned role children',{skip:!delegatedRoot?'需独立委派cgroup及足够资源；本机拒绝不冒充成功配额验收':false,timeout:30000},async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-capacity-role-proof-')),runtime=createOwnedRuntime();let allocator;try{
  allocator=await createCapacityResourceAllocator({delegatedRoot});assert.equal(isOwnedCapacityResourceAllocator(allocator),true);assert.equal(isOwnedCapacityResourceAllocator({...allocator}),false);
  const children=[];for(const role of ['gateway','worker','database']){
   const marker=join(root,role);children.push(await allocator.start(runtime,process.execPath,['-e','require("node:fs").writeFileSync(process.argv[1],require("node:fs").readFileSync("/proc/self/cgroup"));setInterval(()=>{},1000)',marker],{role}));
   const deadline=performance.now()+5000;for(;;){try{assert.match(await readFile(marker,'utf8'),new RegExp('/'+(role==='database'?'database':'gateway-worker')+'/launch-[a-f0-9-]+\\n'));break;}catch(error){if(performance.now()>=deadline)throw error;await delay(10);}}
  }
  const evidence=await allocator.observe();assert.equal(evidence.passed,true,JSON.stringify(evidence.missing));assert.equal(evidence.formalCapacityProven,false);assert.equal(evidence.roles.gatewayWorker.cpuQuota,4);assert.equal(evidence.roles.database.memoryLimitBytes,8*1024**3);
  await assert.rejects(allocator.start({...runtime},process.execPath,[],{role:'database'}),/spawn_invalid/);assert.ok(children.every(child=>child.exitCode===null));
  await terminateOwnedCapacityChild(runtime,children[2],{graceMs:50});const missingDb=await allocator.observe();assert.equal(missingDb.passed,null);assert.ok(missingDb.missing.includes('owned_role_process_missing:database'));
  await terminateOwnedCapacityChild(runtime,children[0],{graceMs:50});await terminateOwnedCapacityChild(runtime,children[1],{graceMs:50});const empty=await allocator.observe();assert.equal(empty.passed,null);assert.ok(empty.missing.includes('owned_combined_effective_quota_unknown'));
  for(const role of ['gateway','worker','database'])children.push(await allocator.start(runtime,process.execPath,['-e','setInterval(()=>{},1000)'],{role}));assert.equal((await allocator.observe()).passed,true);
  const peer=await running(runtime,'helper'),orphanFile=join(root,'orphan-pid');
  const orphanCode='const c=require("node:child_process").spawn(process.execPath,["-e",\'process.on("SIGTERM",()=>{});process.send("ready");setInterval(()=>{},1000)\'],{detached:true,stdio:["ignore","ignore","ignore","ipc"]});c.once("message",()=>require("node:fs").writeFileSync(process.argv[1],String(c.pid)));setInterval(()=>{},1000)';
  const leader=await allocator.start(runtime,process.execPath,['-e',orphanCode,orphanFile],{role:'worker'});children.push(leader);let orphanPid;const orphanDeadline=performance.now()+5000;
  for(;;){try{orphanPid=Number(await readFile(orphanFile,'utf8'));break;}catch(error){if(performance.now()>=orphanDeadline)throw error;await delay(10);}}
  await terminateOwnedCapacityChild(runtime,leader,{graceMs:50});assert.ok(!/^State:\s+Z/m.test(await readFile('/proc/'+orphanPid+'/status','utf8')),'separate-session descendant survived leader termination');
  const late=allocator.start(runtime,process.execPath,['-e','setInterval(()=>{},1000)'],{role:'worker'}),closing=allocator.close();await assert.rejects(late,/allocator_closed|process_not_entered/);await closing;await allocator.close();assert.ok(children.every(child=>child.exitCode!==null||child.signalCode!==null));
  try{assert.match(await readFile('/proc/'+orphanPid+'/status','utf8'),/^State:\s+Z/m);}catch(error){if(error.code!=='ENOENT')throw error;}assert.equal(peer.exitCode,null);assert.equal(peer.signalCode,null);
 }finally{try{await runtime.close();}finally{try{await allocator?.close();}finally{await rm(root,{recursive:true,force:true});}}}
});

test('real child allocation budget rejects extra launches without stopping the existing allocation',{skip:!delegatedRoot?'需真实独立委派cgroup':false,timeout:30000},async()=>{
 const runtime=createOwnedRuntime();let allocator;try{
  allocator=await createCapacityResourceAllocator({delegatedRoot,maxChildGroups:1});const child=await allocator.start(runtime,process.execPath,['-e','setInterval(()=>{},1000)'],{role:'gateway'});
  await assert.rejects(allocator.start(runtime,process.execPath,['-e','setInterval(()=>{},1000)'],{role:'worker'}),/child_group_budget/);assert.equal(child.exitCode,null);assert.equal(child.signalCode,null);
 }finally{try{await runtime.close();}finally{await allocator?.close();}}
});

const previewLimits=()=>({gatewayWorker:{cpuQuota:1.5,memoryLimitBytes:2.5*1024**3},database:{cpuQuota:1,memoryLimitBytes:2*1024**3},processLimits:{gateway:{cpuQuota:1,memoryLimitBytes:2*1024**3},worker:{cpuQuota:0.5,memoryLimitBytes:0.5*1024**3},database:{cpuQuota:1,memoryLimitBytes:2*1024**3}}});
test('resource declarations preserve default shared limits and explicitly bind fractional delivery leaves',()=>{
 const original=normalizeCapacityResourceLimits();assert.deepEqual(original.gatewayWorker,{cpuQuota:4,memoryLimitBytes:8*1024**3});assert.deepEqual(original.database,original.gatewayWorker);assert.deepEqual(original.processLimits.gateway,original.gatewayWorker);assert.deepEqual(original.processLimits.worker,original.gatewayWorker);assert.deepEqual(original.processLimits.database,original.database);
 const input=previewLimits(),limits=normalizeCapacityResourceLimits(input);assert.deepEqual(capacityResourceTotals(limits),{cpuQuota:2.5,memoryLimitBytes:4.5*1024**3});assert.equal(capacityCpuQuotaUnits(limits.processLimits.worker.cpuQuota),50000);input.processLimits.worker.cpuQuota=4;assert.equal(limits.processLimits.worker.cpuQuota,0.5);assert.throws(()=>{limits.database.cpuQuota=4;},TypeError);
 const inherited=previewLimits();delete inherited.processLimits.database;assert.deepEqual(normalizeCapacityResourceLimits(inherited).processLimits.database,inherited.database);const constrained=previewLimits();constrained.processLimits.database={cpuQuota:0.5,memoryLimitBytes:1024**3};assert.deepEqual(normalizeCapacityResourceLimits(constrained).processLimits.database,constrained.processLimits.database);
 assert.deepEqual(capacityResourceTotals({gatewayWorker:{cpuQuota:0.1,memoryLimitBytes:4096},database:{cpuQuota:0.2,memoryLimitBytes:4096}}),{cpuQuota:0.3,memoryLimitBytes:8192});
});
test('invalid or incomplete declarations fail before even checking a delegated filesystem',async()=>{
 const invalid=[null,{},[],{...previewLimits(),other:{}},{...previewLimits(),database:undefined},{...previewLimits(),processLimits:{gateway:previewLimits().processLimits.gateway}},...['cpuQuota','memoryLimitBytes'].flatMap(key=>[0,-1,NaN,Infinity,'1',true].map(value=>({...previewLimits(),database:{...previewLimits().database,[key]:value}}))),{...previewLimits(),processLimits:{gateway:null,worker:previewLimits().processLimits.worker}},{...previewLimits(),processLimits:{...previewLimits().processLimits,gateway:{cpuQuota:2,memoryLimitBytes:1024}}},{...previewLimits(),gatewayWorker:{cpuQuota:0.00001,memoryLimitBytes:4096}},...[{cpuQuota:1.5,memoryLimitBytes:1024**3},{cpuQuota:1,memoryLimitBytes:2.25*1024**3},null].map(database=>({...previewLimits(),processLimits:{...previewLimits().processLimits,database}}))];
 for(const resourceLimits of invalid){assert.throws(()=>normalizeCapacityResourceLimits(resourceLimits),/limits_invalid/);await assert.rejects(createCapacityResourceAllocator({delegatedRoot:'/does-not-exist',resourceLimits}),/limits_invalid/);}
});
test('ancestor memory headroom uses the declared role sum and still reserves the unchanged normal eighty percent',()=>{
 const GiB=1024n**3n,resourceLimits=previewLimits();assert.equal(assessCapacityAncestorHeadroom({limitBytes:8n*GiB,currentBytes:3n*GiB,resourceLimits}).passed,true);assert.equal(assessCapacityAncestorHeadroom({limitBytes:8n*GiB,currentBytes:5n*GiB,resourceLimits}).passed,false);assert.equal(assessCapacityAncestorHeadroom({limitBytes:8n*GiB,currentBytes:5n*GiB,ownedBytes:2n*GiB,resourceLimits}).passed,true);
});
test('real fractional delegated roles preserve shared and three leaf limits, decimal sums and genuine shortages',{skip:!delegatedRoot?'需真实独立委派cgroup，未伪装通过':false,timeout:30000},async()=>{
 const runtime=createOwnedRuntime();let allocator;try{
  for(const variant of ['preview','decimal','database-constrained']){
   const resourceLimits=previewLimits();if(variant==='decimal'){resourceLimits.gatewayWorker.cpuQuota=0.3;resourceLimits.database.cpuQuota=0.6;resourceLimits.processLimits.gateway.cpuQuota=0.3;resourceLimits.processLimits.worker.cpuQuota=0.15;resourceLimits.processLimits.database.cpuQuota=0.6;}if(variant==='database-constrained')resourceLimits.processLimits.database={cpuQuota:0.5,memoryLimitBytes:1024**3};
   allocator=await createCapacityResourceAllocator({delegatedRoot,resourceLimits});const children={};for(const role of ['gateway','worker','database'])children[role]=await allocator.start(runtime,process.execPath,['-e','setInterval(()=>{},1000)'],{role});
   const report=await allocator.observe();assert.deepEqual(report.resourceLimits,normalizeCapacityResourceLimits(resourceLimits));if(variant==='database-constrained'){assert.equal(report.passed,null);assert.ok(report.missing.includes('owned_role_effective_quota_insufficient:database'));}else assert.equal(report.passed,true,JSON.stringify(report.missing));
   for(const role of ['gateway','worker','database']){const member=(await readFile('/proc/'+children[role].pid+'/cgroup','utf8')).trim().split('::')[1],leaf='/sys/fs/cgroup'+member,expected=resourceLimits.processLimits[role];assert.equal((await readFile(leaf+'/cpu.max','utf8')).trim(),capacityCpuQuotaUnits(expected.cpuQuota)+' 100000');assert.equal((await readFile(leaf+'/memory.max','utf8')).trim(),String(expected.memoryLimitBytes));if(role==='gateway'&&variant==='preview'){await writeFile(leaf+'/cpu.max','50000 100000');await assert.rejects(allocator.observe(),/limits_changed/);await writeFile(leaf+'/cpu.max','100000 100000');assert.equal((await allocator.observe()).passed,true);}}
   await allocator.close();allocator=null;
  }
 }finally{try{await runtime.close();}finally{await allocator?.close();}}
});

test('CPU sufficiency removes only relative IEEE rounding and still rejects one real quota unit shortage',()=>{
 assert.equal(capacityCpuQuotaAtLeast(0.3+0.6,0.9),true);assert.equal(capacityCpuQuotaAtLeast(0.9-1e-6,0.9),false);assert.equal(capacityCpuQuotaAtLeast(0.89999,0.9),false);assert.equal(capacityCpuQuotaAtLeast(NaN,0.9),false);assert.equal(capacityCpuQuotaAtLeast(-0.1,0.9),false);
});
