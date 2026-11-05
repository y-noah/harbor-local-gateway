import {capacityRuntimeArtifactBinding,inspectCapacityRuntimeArtifact,readCapacityRuntimeRegular as regularBytes} from './runtime-artifact.mjs';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {writeFile,lstat,realpath,readdir,chmod} from 'node:fs/promises';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {isOwnedRuntime,startOwnedCapacityChild,terminateOwnedCapacityChild} from './owned-runtime.mjs';
import {isOwnedCapacityResourceAllocator} from './resource-allocator.mjs';

const sandboxProcesses=new WeakMap();
export const capacityRuntimeSandboxBinding=child=>sandboxProcesses.get(child)??null;
const execute=promisify(execFile),hash=value=>createHash('sha256').update(value).digest('hex');
const fail=code=>{throw Error(code);};
const source=fileURLToPath(new URL('./runtime-entry.c',import.meta.url)),extractor=fileURLToPath(new URL('./runtime-extract.py',import.meta.url));
async function input(signal,{pythonFile='/usr/bin/python3',extractorFile=extractor}={}){signal?.throwIfAborted();const python=await realpath(pythonFile),helper=await realpath(extractorFile);return {python,pythonSha256:hash(await regularBytes(python,128*1024**2,{signal})),nativeSha256:hash(await regularBytes(source,1048576,{signal})),helper,helperSha256:hash(await regularBytes(helper,4*1024**2,{signal}))};}
async function directory(path){const info=await lstat(path);if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==process.getuid()||info.mode&0o077||await realpath(path)!==path)fail('capacity_runtime_entry_directory');return info;}
export async function buildCapacityRuntimeEntry({directory:destination,signal,pythonFile,extractorFile}={}){
 await directory(destination);if((await readdir(destination)).length)fail('capacity_runtime_entry_directory');const manifest=await input(signal,{pythonFile,extractorFile}),binary=join(destination,'entry');
 await execute('/usr/bin/cc',['-std=c11','-O2','-Wall','-Wextra','-Werror','-DHARBOR_RUNTIME_PYTHON='+JSON.stringify(manifest.python),'-DHARBOR_RUNTIME_HELPER='+JSON.stringify(manifest.helper),source,'-o',binary],{signal,timeout:30000,maxBuffer:65536});await chmod(binary,0o700);
 if(JSON.stringify(await input(signal,{pythonFile,extractorFile}))!==JSON.stringify(manifest))fail('capacity_runtime_entry_changed');await writeFile(join(destination,'manifest.json'),JSON.stringify({schema:'capacity-runtime-entry-v1',...manifest,binarySha256:hash(await regularBytes(binary,16*1024**2,{signal}))}),{mode:0o600,flag:'wx'});return {binary,directory:destination};
}
async function load(destination,signal){signal?.throwIfAborted();
 await directory(destination);const binary=join(destination,'entry'),manifestFile=join(destination,'manifest.json');for(const [path,mode] of [[binary,0o700],[manifestFile,0o600]]){const info=await lstat(path);if(!info.isFile()||info.isSymbolicLink()||info.uid!==process.getuid()||(info.mode&0o777)!==mode||info.size>16*1024**2)fail('capacity_runtime_entry_unproven');}
 const saved=JSON.parse((await regularBytes(manifestFile,65536,{signal})).toString('utf8')),current=await input(signal);if(saved.schema!=='capacity-runtime-entry-v1'||Object.keys(current).some(key=>current[key]!==saved[key])||saved.binarySha256!==hash(await regularBytes(binary,16*1024**2,{signal})))fail('capacity_runtime_entry_unproven');return {binary,sha256:saved.binarySha256};
}

// A namespace child is not itself a formal capability. Only the fixed runtime
// producer can bind its actual OCI B/root trust/cgroup to a factory epoch.
export async function startCapacityRuntimeSandbox({entryDirectory,runtime,resourceAllocator,role,configuration,signal,trustedArtifact}={}){
 if(!isOwnedRuntime(runtime)||!['gateway','worker','helper'].includes(role)||resourceAllocator!==undefined&&!isOwnedCapacityResourceAllocator(resourceAllocator))fail('capacity_runtime_launcher_configuration');if(configuration?.entry!==undefined&&!['restored-report','remote-broker'].includes(configuration.entry))fail('capacity_runtime_launch_entry');if(configuration?.entry==='remote-broker'&&role!=='helper'||role==='helper'&&configuration?.entry!=='remote-broker')fail('capacity_runtime_launcher_configuration');signal?.throwIfAborted();let artifact;if(trustedArtifact!==undefined){await inspectCapacityRuntimeArtifact(trustedArtifact);const record=capacityRuntimeArtifactBinding(trustedArtifact);if(configuration?.entry==='remote-broker'&&['tools/capacity/remote-broker-runtime.mjs','tools/capacity/runtime-source-loader.mjs','tools/capacity/remote-endpoint.mjs','tools/capacity/remote-launcher.mjs'].some(name=>!record.harness.files[name]))fail('capacity_runtime_remote_harness_incomplete');artifact={binary:record.trust.tools.launcher.file,sha256:record.trust.tools.launcher.sha256};}else artifact=await load(entryDirectory,signal);const encoded=Buffer.from(JSON.stringify({...configuration,native:{file:artifact.binary,sha256:artifact.sha256}}));if(encoded.length>65536)fail('capacity_runtime_launch_frame');
 const options={role,stdio:['ignore','pipe','pipe','ipc','pipe'],env:{...Object.fromEntries(Object.keys(process.env).map(key=>[key,undefined])),...(configuration?.entry==='restored-report'?{HARBOR_RESTORED_RUNTIME_ROLE:role}:{}),PATH:'/usr/local/bin:/usr/bin:/bin',HOME:'/tmp',TMPDIR:'/tmp',LD_PRELOAD:'',LD_LIBRARY_PATH:'',NODE_OPTIONS:'',NODE_PATH:'',PYTHONPATH:'',PYTHONHOME:''}},child=resourceAllocator?await resourceAllocator.start(runtime,artifact.binary,[],options):startOwnedCapacityChild(runtime,artifact.binary,[],options);
 try{signal?.throwIfAborted();await new Promise((resolve,reject)=>{const control=child.stdio[4],exit=()=>reject(Error('capacity_runtime_launch_exited'));child.once('exit',exit);control.once('error',reject);control.end(encoded,()=>{child.off('exit',exit);resolve();});});sandboxProcesses.set(child,Object.freeze({runtime,role,configuration,trustedArtifact:trustedArtifact??null,child}));return child;}catch(error){await terminateOwnedCapacityChild(runtime,child).catch(()=>{});throw error;}
}
