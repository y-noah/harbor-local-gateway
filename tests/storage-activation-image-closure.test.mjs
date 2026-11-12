import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,readdir,copyFile,cp,symlink,rm,appendFile,lstat,writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {imageManifest,verifyOperatorToolsProjection} from '../ci/postgres-image-manifest.mjs';
const execute=promisify(execFile),source=fileURLToPath(new URL('..',import.meta.url));

test('B-shaped package leaves native overlay empty and its verified host projection imports the complete operator chain',{timeout:30000},async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-activation-b-closure-')),old=process.env.HARBOR_IMAGE_REVISION;try{
  for(const directory of ['ci','ci/storage-operator-tools','public','clients','migrations'])await mkdir(join(root,directory),{recursive:true});
  for(const name of await readdir(source))if(name.endsWith('.mjs')||['package.json','package-lock.json','storage-receiver-candidate-signal.py'].includes(name))await copyFile(join(source,name),join(root,name));
  for(const name of await readdir(join(source,'ci')))if(/^postgres-.+\.mjs$/.test(name)||['postgres-deploy.py','deploy-wrapper.py'].includes(name))await copyFile(join(source,'ci',name),join(root,'ci',name));
  for(const name of await readdir(join(source,'migrations')))if(name.endsWith('.sql'))await copyFile(join(source,'migrations',name),join(root,'migrations',name));
  for(const name of ['public/index.html','public/app.js','public/style.css','clients/image-mcp.mjs','clients/harbor.example.toml'])await copyFile(join(source,name),join(root,name));
  for(const name of await readdir(join(source,'tools/capacity')))if(/\.(mjs|py|c)$/.test(name))await copyFile(join(source,'tools/capacity',name),join(root,'ci/storage-operator-tools',name));
  await symlink(join(source,'node_modules'),join(root,'node_modules'),'dir');await assert.rejects(lstat(join(root,'tools/capacity')),{code:'ENOENT'});
  process.env.HARBOR_IMAGE_REVISION='a'.repeat(40);const manifest=await imageManifest({create:true,rootDirectory:root});assert.match(manifest.files['storage-receiver-candidate-signal.py'],/^[a-f0-9]{64}$/);await appendFile(join(root,'storage-receiver-candidate-signal.py'),'\n# altered safety helper\n');await assert.rejects(imageManifest({rootDirectory:root}),/manifest mismatch/);await copyFile(join(source,'storage-receiver-candidate-signal.py'),join(root,'storage-receiver-candidate-signal.py'));await rm(join(root,'storage-receiver-candidate-signal.py'));await assert.rejects(imageManifest({rootDirectory:root}),{code:'ENOENT'});await copyFile(join(source,'storage-receiver-candidate-signal.py'),join(root,'storage-receiver-candidate-signal.py'));await mkdir(join(root,'tools'),{recursive:true});await cp(join(root,'ci/storage-operator-tools'),join(root,'tools/capacity'),{recursive:true});assert.equal((await verifyOperatorToolsProjection({rootDirectory:root})).operatorToolsProjectionVerified,true);
  for(const name of ['key-consumption-policy.sql','recording-retention-unlimited.sql']){
   const path='migrations/'+name;assert.match(manifest.files[path],/^[a-f0-9]{64}$/);
   await appendFile(join(root,path),'\n-- altered optional migration\n');await assert.rejects(imageManifest({rootDirectory:root}),/manifest mismatch/);
   await copyFile(join(source,path),join(root,path));assert.equal((await imageManifest({rootDirectory:root})).revision,'a'.repeat(40));
   await rm(join(root,path));await assert.rejects(imageManifest({rootDirectory:root}),/manifest mismatch/);await copyFile(join(source,path),join(root,path));
  }
  const entries=['storage-operator-context.mjs','storage-activation-issuer.mjs','storage-operator-backup.mjs','storage-activation-deploy.mjs'],program='await Promise.all('+JSON.stringify(entries.map(name=>pathToFileURL(join(root,name)).href))+'.map(value=>import(value)));console.log(JSON.stringify({imported:4}));';const result=await execute(process.execPath,['--input-type=module','-e',program],{timeout:15000,maxBuffer:65536});assert.equal(JSON.parse(result.stdout).imported,4);
  await appendFile(join(root,'tools/capacity/owned-runtime.mjs'),'\n// altered execution projection\n');await assert.rejects(verifyOperatorToolsProjection({rootDirectory:root}),/projection differs/);assert.equal((await imageManifest({rootDirectory:root})).revision,'a'.repeat(40));
  await cp(join(root,'ci/storage-operator-tools/owned-runtime.mjs'),join(root,'tools/capacity/owned-runtime.mjs'));await writeFile(join(root,'tools/capacity/injected.mjs'),'export const injected=true');await assert.rejects(verifyOperatorToolsProjection({rootDirectory:root}),/projection differs/);
 }finally{if(old===undefined)delete process.env.HARBOR_IMAGE_REVISION;else process.env.HARBOR_IMAGE_REVISION=old;await rm(root,{recursive:true,force:true});}
});
