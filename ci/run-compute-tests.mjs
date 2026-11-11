import {mkdtempSync,writeFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';

// Use LUMI's existing public Bun catalog without requiring a new OCI conversion.
// Tests themselves still run on the same official Node version as Docker CI.
const version='24.21.0';
const sha256='6e1db87ef58b8819e5d5402eff1536491b18edd8eb7bee5ef7897876e88dc5ff';
if(process.platform!=='linux'||process.arch!=='x64')throw new Error('compute_platform_unsupported');
const directory=mkdtempSync(join(tmpdir(),'harbor-compute-node-'));
function run(command,args){
  const result=spawnSync(command,args,{stdio:'inherit'});
  if(result.error)throw result.error;
  if(result.signal)throw new Error('compute_child_interrupted');
  return result.status??1;
}
try{
  const response=await fetch(`https://nodejs.org/dist/v${version}/node-v${version}-linux-x64.tar.gz`,{signal:AbortSignal.timeout(120000),redirect:'error'});
  if(!response.ok)throw new Error('compute_node_download_failed');
  const archive=Buffer.from(await response.arrayBuffer());
  if(createHash('sha256').update(archive).digest('hex')!==sha256)throw new Error('compute_node_digest_mismatch');
  const path=join(directory,'node.tar.gz');writeFileSync(path,archive,{mode:0o600});
  if(run('tar',['-xzf',path,'-C',directory])!==0)throw new Error('compute_node_extract_failed');
  process.exitCode=run(join(directory,`node-v${version}-linux-x64/bin/node`),[fileURLToPath(new URL('./run-test-partition.mjs',import.meta.url)),'compute']);
}finally{rmSync(directory,{recursive:true,force:true});}
