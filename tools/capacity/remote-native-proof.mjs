import {readFile,readlink,stat} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {capacityRuntimeSandboxBinding} from './runtime-entry.mjs';
import {capacityRuntimeArtifactBinding,inspectCapacityRuntimeArtifact} from './runtime-artifact.mjs';
import {ownedProcessProof} from './owned-runtime.mjs';

const fail=code=>{throw Error('capacity_remote_native_'+code);},hash=value=>createHash('sha256').update(value).digest('hex');
const below=(path,root)=>root==='/'?path.startsWith('/'):path===root||path.startsWith(root+'/');
async function text(path,max=1048576){const value=await readFile(path);if(value.length>max)fail('kernel_budget');return value.toString('utf8');}
async function identity(pid){const value=await text('/proc/'+pid+'/stat',65536),parts=value.slice(value.lastIndexOf(')')+2).split(' ');if(!/^\d+$/.test(parts[19])||['Z','X','x','T','t'].includes(parts[0]))fail('process_lost');return {pid,parent:Number(parts[1]),started:parts[19]};}
async function tree(pid){const rows=[];async function visit(pid,parent){if(rows.length>=256)fail('process_budget');const row=await identity(pid);if(parent!==undefined&&row.parent!==parent)fail('parent_changed');rows.push(row);const children=(await text('/proc/'+pid+'/task/'+pid+'/children',65536)).trim().split(/\s+/).filter(Boolean);for(const value of children){if(!/^[1-9]\d*$/.test(value))fail('process_identity');await visit(Number(value),pid);}}await visit(pid);return rows;}
function mounts(value){return value.trim().split('\n').map(line=>{const parts=line.split(' ');return {path:parts[4]?.replace(/\\([0-7]{3})/g,(_,n)=>String.fromCharCode(parseInt(n,8))),readonly:parts[5]?.split(',').includes('ro')};});}
function readonly(path,rows){let selected;for(const row of rows)if(below(path,row.path)&&(!selected||row.path.length>selected.path.length))selected=row;return selected?.readonly===true;}

// This observer reads the external host kernel. A namespace PID or a source
// digest reported over IPC is never used to discover or authorize a process.
export async function inspectCapacityNativeRemoteExecution({artifact,runtime,child}={}){
 await inspectCapacityRuntimeArtifact(artifact);const launch=capacityRuntimeSandboxBinding(child),proof=ownedProcessProof(runtime,child);
 if(!launch||launch.runtime!==runtime||launch.trustedArtifact!==artifact||launch.role!=='helper'||launch.configuration.entry!=='remote-broker'||proof.role!=='helper')fail('launch_unowned');
 const expected=capacityRuntimeArtifactBinding(artifact),before=await tree(child.pid),outerNamespace=await readlink('/proc/'+child.pid+'/ns/pid'),nodes=[];
 for(const row of before){
  const prefix='/proc/'+row.pid,argv=(await text(prefix+'/cmdline',65536)).split('\0').filter(Boolean);
  if(argv[0]!=='/usr/local/bin/node')continue;
  if(argv[1]!=='--import=/app/tools/capacity/runtime-source-loader.mjs'||!['/app/tools/capacity/remote-broker-runtime.mjs','/app/tools/capacity/remote-launcher.mjs'].includes(argv[2])||argv[3]!== (argv[2].endsWith('remote-broker-runtime.mjs')?'--owned-capacity-remote-broker':'--owned-capacity-remote-executor')||argv.length!==4)fail('executable_arguments');
  const [status,mountText,pidNamespace,actual,node]=await Promise.all([text(prefix+'/status',65536),text(prefix+'/mountinfo'),readlink(prefix+'/ns/pid'),stat(prefix+'/exe',{bigint:true}),stat(prefix+'/root/usr/local/bin/node',{bigint:true})]);
  if(pidNamespace===outerNamespace||!/^NoNewPrivs:\s+1$/m.test(status)||!/^Seccomp:\s+2$/m.test(status)||!/^CapEff:\s+0+$/m.test(status)||!/^CapPrm:\s+0+$/m.test(status)||!actual.isFile()||!node.isFile()||['dev','ino','size'].some(key=>actual[key]!==node[key]))fail('execution_unsealed');
  const namespacePids=/^NSpid:\s+([\d\s]+)$/m.exec(status)?.[1].trim().split(/\s+/).map(Number),sealedMounts=mounts(mountText);if(!namespacePids||namespacePids.length<2||!readonly('/',sealedMounts)||!readonly('/usr/local/bin/node',sealedMounts)||!readonly('/app/tools/capacity/runtime-source-loader.mjs',sealedMounts))fail('namespace_unproven');
  const sourceRows=[];for(const [path,digest] of Object.entries(expected.harness.files)){const target='/app/'+path;if(!readonly(target,sealedMounts)||hash(Buffer.from(await text(prefix+'/root'+target,4*1024**2)))!==digest)fail('source_changed');sourceRows.push([path,digest]);}
  // Native file-backed executable mappings must also stay on the sealed root.
  // Anonymous/JIT memory is not represented as an immutable file source.
  for(const line of (await text(prefix+'/maps',4*1024**2)).split('\n')){const parts=/^\S+\s+(\S+)\s+\S+\s+\S+\s+\S+\s*(.*)$/.exec(line);if(!parts||!parts[1].includes('x')||!parts[2].startsWith('/')||parts[2].startsWith('/memfd:'))continue;if(parts[2].endsWith(' (deleted)')||!readonly(parts[2],sealedMounts))fail('native_source_writable');}
  nodes.push({pid:row.pid,started:row.started,namespacePid:namespacePids.at(-1),namespace:pidNamespace,entry:argv[2],sourceDigest:hash(JSON.stringify(sourceRows.sort())),rootReadOnly:true});
 }
 const broker=nodes.filter(row=>row.entry.endsWith('remote-broker-runtime.mjs')),executors=nodes.filter(row=>row.entry.endsWith('remote-launcher.mjs'));if(broker.length!==1||executors.length!==1||broker[0].namespace!==executors[0].namespace||broker[0].namespacePid!==2)fail('execution_sources_missing');
 const supervisor=before.find(row=>row.pid===broker[0].pid)?.parent,supervisorStatus=await text('/proc/'+supervisor+'/status',65536);if(!/^NSpid:\s+\d+(?:\s+\d+)*\s+1$/m.test(supervisorStatus)||await readlink('/proc/'+supervisor+'/ns/pid')!==broker[0].namespace)fail('namespace_supervisor_unproven');
 const after=await tree(child.pid);if(before.length!==after.length||before.some(row=>!after.some(next=>next.pid===row.pid&&next.started===row.started&&next.parent===row.parent)))fail('tree_changed');ownedProcessProof(runtime,child);await inspectCapacityRuntimeArtifact(artifact);
 return Object.freeze({scope:'actual-root-observed-native-remote-tree',businessDigest:expected.observed.manifestDigest,harnessDigest:expected.trust.harness.manifestSha256,processes:Object.freeze(nodes.map(row=>Object.freeze(row))),supervisor:Object.freeze({pid:supervisor,started:before.find(row=>row.pid===supervisor)?.started,namespacePid:1}),primarySourceOnly:true});
}
