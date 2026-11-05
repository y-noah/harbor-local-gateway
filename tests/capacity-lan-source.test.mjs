import test from 'node:test';
import assert from 'node:assert/strict';
import {matchCapacityLanBoundary,selectCapacityIpv4Route,capacityLanPolicy} from '../tools/capacity/lan-source.mjs';
const policy={segmentId:'synthetic-lab',cidr:'192.0.2.0/24',serverInterface:'lan0',clientInterface:'eth0'},serverRoute={targetAddress:'192.0.2.20',sourceAddress:'192.0.2.10',interface:'lan0',destination:'192.0.2.0',prefixLength:24,gateway:'0.0.0.0',onLink:true},remoteRoute={interface:'eth0',targetAddress:'192.0.2.10',sourceAddress:'192.0.2.20',destination:'192.0.2.0',prefixLength:24,gateway:'0.0.0.0',onLink:true},connections=[{localAddress:'192.0.2.10',remoteAddress:'192.0.2.20'}];
test('LAN matching requires the approved boundary and both native on-link observations, not offhost or different addresses alone',()=>{
 assert.equal(matchCapacityLanBoundary({policy,serverRoute,remoteRoute,connections}).matched,true);
 assert.equal(matchCapacityLanBoundary({policy,serverRoute:{...serverRoute,gateway:null},remoteRoute:{...remoteRoute,gateway:null},connections}).matched,true);
 assert.equal(matchCapacityLanBoundary({policy,serverRoute,remoteRoute:{...remoteRoute,gateway:'192.0.2.1'},connections}).matched,false);
 for(const input of [
  {policy,serverRoute,remoteRoute:{...remoteRoute,sourceAddress:'198.51.100.20',destination:'0.0.0.0',prefixLength:0,gateway:'198.51.100.1',onLink:false},connections:[{localAddress:'192.0.2.10',remoteAddress:'198.51.100.20'}]},
  {policy,serverRoute,remoteRoute:null,connections},
  {policy,serverRoute:{...serverRoute,sourceAddress:'192.0.2.11'},remoteRoute,connections},
  {policy,serverRoute:{...serverRoute,targetAddress:'192.0.2.21'},remoteRoute,connections},
  {policy,serverRoute:{...serverRoute,gateway:'192.0.2.1',onLink:false},remoteRoute,connections},
  {policy,serverRoute,remoteRoute:{...remoteRoute,sourceAddress:'192.0.2.21'},connections},
  {policy:{...policy,cidr:'127.0.0.0/8'},serverRoute,remoteRoute,connections:[{localAddress:'127.0.0.1',remoteAddress:'127.0.0.2'}]},
  {policy:{...policy,clientInterface:'different'},serverRoute,remoteRoute,connections},
 ])assert.equal(matchCapacityLanBoundary(input).matched,false);
 assert.equal(capacityLanPolicy({...policy,cidr:'192.0.2.1/24'}),null);
});
test('native proc routing uses the longest actual route and rejects ambiguous equal prefixes',()=>{
 const header='Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\n',wan='wan0 00000000 010064C6 0003 0 0 100 00000000 0 0 0\n',lan='lan0 000200C0 00000000 0001 0 0 0 00FFFFFF 0 0 0\n';
 const {targetAddress,sourceAddress,...expectedRoute}=serverRoute;assert.deepEqual(selectCapacityIpv4Route(header+wan+lan,'192.0.2.20'),expectedRoute);
 assert.equal(selectCapacityIpv4Route(header+wan+lan,'198.51.100.20').onLink,false);
 assert.throws(()=>selectCapacityIpv4Route(header+lan+lan,'192.0.2.20'),/route_ambiguous/);
});

async function nativeSourceRejectionChild(){
 const {mock}=await import('node:test'),assert=(await import('node:assert/strict')).default,{mkdtemp,mkdir,writeFile,rm}=await import('node:fs/promises'),{join}=await import('node:path'),{tmpdir}=await import('node:os'),artifact=await import('./tools/capacity/runtime-artifact.mjs');
 const directory=await mkdtemp(join(tmpdir(),'harbor-lan-source-boundary-')),relative='tools/capacity/remote-route-proof.py',file=join(directory,relative),source=Object.freeze({});let phase='digest';
 try{
  await mkdir(join(directory,'tools/capacity'),{recursive:true});await writeFile(file,'raise RuntimeError("must never execute")\n',{mode:0o600});
  mock.module('./tools/capacity/runtime-artifact.mjs',{namedExports:{...artifact,capacityRuntimeArtifactBinding:()=>({trust:{harness:{directory},tools:{python:{file:'/not-an-executable'}}},harness:{files:{[relative]:'0'.repeat(64)}}}),readCapacityRuntimeRegular:(...args)=>phase==='digest'?Buffer.from('unapproved source bytes'):artifact.readCapacityRuntimeRegular(...args)}});
  mock.module('./tools/capacity/nginx-source.mjs',{namedExports:{capacityNginxSourceBinding:handle=>handle===source?{trusted:true,artifact:{}}:null,inspectCapacityNginxSource:async()=>({}),readCapacityNginxNetworkIdentity:async()=>({socketUid:process.getuid(),namespace:'mocked-boundary-only'})}});
  const {readCapacityNginxLanRoute}=await import('./tools/capacity/lan-source.mjs');await assert.rejects(readCapacityNginxLanRoute(source,'192.0.2.20','192.0.2.10'),/native_observer_digest/);
  phase='path';await assert.rejects(readCapacityNginxLanRoute(source,'192.0.2.20','192.0.2.10'),/capacity_runtime_artifact_parent|capacity_runtime_artifact_file/);
  process.stdout.write(JSON.stringify({verificationBoundaryMocked:true,actualUntrustedPathRejected:true,digestRejectedBeforeExecution:true,formalProven:false})+'\n');
 }finally{mock.restoreAll();await rm(directory,{recursive:true,force:true});}
}
test('the native observer rejects mismatched bytes and an actual untrusted path before execution across a mocked ownership boundary',{timeout:20000},async()=>{
 const {execFile}=await import('node:child_process'),{promisify}=await import('node:util'),program='('+nativeSourceRejectionChild.toString()+')().catch(error=>{console.error(error);process.exitCode=1;});',value=await promisify(execFile)(process.execPath,['--experimental-test-module-mocks','--input-type=module','-e',program],{cwd:process.cwd(),env:{PATH:process.env.PATH},timeout:15000,maxBuffer:65536});assert.deepEqual(JSON.parse(value.stdout.trim().split('\n').at(-1)),{verificationBoundaryMocked:true,actualUntrustedPathRejected:true,digestRejectedBeforeExecution:true,formalProven:false});
});
