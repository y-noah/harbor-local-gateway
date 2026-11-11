import {readdirSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';
import {spawnSync} from 'node:child_process';

const root=fileURLToPath(new URL('../',import.meta.url));
// These suites need only Node and repository files: no root, database or private network.
const compute=new Set(['ui.test.mjs','nightly-plan.test.mjs']);
const kernel=new Set(['capacity-recovery-process-tree.test.mjs','storage-receiver-candidate-root.test.mjs']);
export function testPartition(mode){
  if(!['compute','docker','kernel'].includes(mode))throw new Error('unknown_test_partition');
  const all=readdirSync(new URL('../tests/',import.meta.url)).filter(name=>name.endsWith('.test.mjs')).sort();
  if([...compute,...kernel].some(name=>!all.includes(name)))throw new Error('partition_test_missing');
  return all.filter(name=>mode==='compute'?compute.has(name):mode==='kernel'?kernel.has(name):!compute.has(name)&&!kernel.has(name)).map(name=>'tests/'+name);
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  const files=testPartition(process.argv[2]);
  if(!files.length)throw new Error('empty_test_partition');
  const child=spawnSync(process.execPath,['--test',...files],{cwd:root,stdio:'inherit'});
  if(child.error)throw child.error;
  if(child.signal)process.kill(process.pid,child.signal);
  else process.exitCode=child.status??1;
}
