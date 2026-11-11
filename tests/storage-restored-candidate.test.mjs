import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,rm,readFile,writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
import {buildCapacityRuntimeEntry,startCapacityRuntimeSandbox} from '../tools/capacity/runtime-entry.mjs';
import {createTaskRestoredCandidate,inspectTaskRestoredCandidate,startTaskRestoredCandidate} from '../storage-restored-candidate.mjs';
const exec=promisify(execFile);
test('restored candidate identities are opaque and cannot be copied or supplied as passed JSON',async()=>{
 for(const token of [undefined,{}, {scope:'task-restored-oci-runtime',actualOciBusinessRuntimeVerified:true}]){assert.throws(()=>inspectTaskRestoredCandidate(token),/storage_restored_candidate_binding/);await assert.rejects(startTaskRestoredCandidate(token),/storage_restored_candidate_binding/);}
});
test('the namespace launcher rejects unknown and caller-selected entries before starting a child',async()=>{
 const runtime=createOwnedRuntime();try{for(const entry of ['process-adapter','/tmp/entry.mjs','../entry',true,null])await assert.rejects(startCapacityRuntimeSandbox({runtime,role:'gateway',configuration:{entry}}),/capacity_runtime_launch_entry/);}finally{await runtime.close();}
});
test('native entry rejects unrecognized restored-report arguments without entering a namespace',{skip:process.platform!=='linux'},async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-restored-native-'));try{const entry=await buildCapacityRuntimeEntry({directory:root});for(const args of [['--execute','--other'],['--execute','/tmp/arbitrary.mjs'],['--execute','--restored-report','extra']])await assert.rejects(exec(entry.binary,args),error=>error.code===126);}finally{await rm(root,{recursive:true,force:true});}
});
test('actual CI candidate context rejects mismatched producer and OCI digest rather than trusting verified booleans',{skip:!process.env.HARBOR_CAPACITY_B_EVIDENCE||!process.env.HARBOR_RESTORED_CANDIDATE_CI},async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-restored-candidate-'));try{const base={layout:process.env.HARBOR_CAPACITY_B_EVIDENCE,revision:process.env.HARBOR_CAPACITY_B_REVISION,manifestDigest:process.env.HARBOR_CAPACITY_B_DIGEST,ci:JSON.parse(process.env.HARBOR_RESTORED_CANDIDATE_CI)};let n=0;for(const changes of [{ci:{...base.ci,producer:Number(base.ci.producer)+1}},{manifestDigest:'sha256:'+'0'.repeat(64)},{ci:{...base.ci,verificationJob:Number(base.ci.verificationJob)+1}}]){const directory=join(root,String(++n));await mkdir(directory,{mode:0o700});await assert.rejects(createTaskRestoredCandidate({...base,...changes,directory,verified:true}));}}finally{await rm(root,{recursive:true,force:true});}
});

test('default candidate capability module loads without namespace tooling or a compiler being packaged',async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-restored-default-'));try{await mkdir(join(root,'ci'),{mode:0o700});for(const name of ['storage-restored-candidate.mjs','ci/postgres-candidate-pipeline.mjs','ci/postgres-runner-routing.mjs'])await writeFile(join(root,name),await readFile(new URL('../'+name,import.meta.url)),{mode:0o600});const api=await import(new URL('file://'+join(root,'storage-restored-candidate.mjs')).href);assert.throws(()=>api.inspectTaskRestoredCandidate({}),/storage_restored_candidate_binding/);}finally{await rm(root,{recursive:true,force:true});}
});
