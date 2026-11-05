import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {mkdtemp,mkdir,writeFile,readFile,rm,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {storageOperatorCandidateMaterialRoots,storageOperatorCandidateMaterialMounts} from '../storage-operator-materials.mjs';
import {captureFileSet,restoreFileSet} from '../migration-artifacts.mjs';

function configuration(root='/private/source'){
  return {sourceDataDir:join(root,'data'),candidate:{secretsDirectory:join(root,'secrets')},fileRoots:[{id:'data',source:join(root,'data')},{id:'runtime',source:join(root,'runtime'),databasePrefix:'/old/container/accounts',destinationPrefix:'/app/runtime'},{id:'secrets',source:join(root,'secrets')}]};
}
test('candidate mounts only restored explicit roots and permits a configured legacy account prefix',()=>{
  const config=configuration(),rows=storageOperatorCandidateMaterialRoots(config);assert.deepEqual(rows.map(row=>row.id),['data','runtime','secrets']);
  assert.deepEqual(storageOperatorCandidateMaterialMounts(config,'/private/restored'),['--mount','type=bind,src=/private/restored/data,dst=/app/data,readonly','--mount','type=bind,src=/private/restored/runtime,dst=/app/runtime,readonly','--mount','type=bind,src=/private/restored/secrets,dst=/run/secrets,readonly']);
  delete config.fileRoots[1].databasePrefix;assert.equal(storageOperatorCandidateMaterialRoots(config).length,3);
});
test('missing, unrelated, overlapping and Docker option-injecting material roots are rejected before side effects',()=>{
  const reject=mutate=>{const config=configuration();mutate(config);assert.throws(()=>storageOperatorCandidateMaterialRoots(config),{code:'storage_operator_candidate_material_coverage'});};
  reject(config=>config.fileRoots.pop());reject(config=>config.fileRoots[2].source='/elsewhere/secrets');reject(config=>config.fileRoots[0].source='/elsewhere/data');reject(config=>config.fileRoots.push({...config.fileRoots[2]}));
  reject(config=>config.fileRoots[2].source=config.candidate.secretsDirectory=join(config.sourceDataDir,'secrets'));reject(config=>config.fileRoots[1].destinationPrefix='/other/runtime');reject(config=>config.fileRoots[1].source='/private/runtime,readonly=false');
  assert.throws(()=>storageOperatorCandidateMaterialMounts(configuration(),'/private/restored,dst=/other'),{code:'storage_operator_candidate_material_coverage'});
});
test('encrypted file-set roundtrip preserves prepared TLS and database report materials after originals change',{timeout:15000},async()=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-material-coverage-')),config=configuration(join(root,'source')),key=randomBytes(32),archive=join(root,'archive'),restored=join(root,'restored');
  try{
    await mkdir(join(root,'source'),{mode:0o700});for(const path of [archive,restored,...config.fileRoots.map(row=>row.source)])await mkdir(path,{mode:0o700});
    await mkdir(join(config.candidate.secretsDirectory,'device'),{mode:0o700});const originals=new Map([['device/server.key',randomBytes(32)],['device/ca.key',randomBytes(32)],['device/server.pem',Buffer.from('synthetic server certificate')],['device/ca.pem',Buffer.from('synthetic device CA')],['postgres-report.key',randomBytes(32)],['postgres-ca.crt',Buffer.from('synthetic database CA')]]);
    for(const [name,bytes] of originals)await writeFile(join(config.candidate.secretsDirectory,name),bytes,{mode:0o600});await writeFile(join(config.sourceDataDir,'key-encryption.key'),randomBytes(32),{mode:0o600});
    const captured=await captureFileSet({roots:config.fileRoots,directory:archive,key});for(const name of originals.keys())await writeFile(join(config.candidate.secretsDirectory,name),'changed original',{mode:0o600});
    await restoreFileSet({directory:archive,destination:restored,key,expectedManifestDigest:captured.manifestDigest});
    for(const [name,bytes] of originals)assert.deepEqual(await readFile(join(restored,'secrets',name)),bytes);
    assert.ok(storageOperatorCandidateMaterialMounts(config,restored).every(value=>!value.includes(config.candidate.secretsDirectory)));
  }finally{key.fill(0);await rm(root,{recursive:true,force:true});}
});
test('declared source symlink cannot be captured as approved secrets',{timeout:15000},async()=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-material-link-'));try{
    const source=join(root,'source'),archive=join(root,'archive');await mkdir(source,{mode:0o700});await mkdir(archive,{mode:0o700});await symlink(source,join(root,'alias'));
    await assert.rejects(captureFileSet({roots:[{id:'secrets',source:join(root,'alias')}],directory:archive,key:randomBytes(32)}));
  }finally{await rm(root,{recursive:true,force:true});}
});
