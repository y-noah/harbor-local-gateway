import test from 'node:test';import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,rm,readdir,symlink} from 'node:fs/promises';
import {join} from 'node:path';import {tmpdir} from 'node:os';import {randomBytes} from 'node:crypto';
import {captureFileSet,restoreFileSet,readManifest,verifyFileSetSources} from '../migration-artifacts.mjs';
import {periodicApplicationDirectoryExclusions} from '../postgres-periodic-application-policy.mjs';

const selection=()=>periodicApplicationDirectoryExclusions({runtimeRebuildableCaches:['codex-launcher-aliases']});
async function directory(path){await mkdir(path,{recursive:true,mode:0o700});return path;}

test('declared launcher directories can disappear and rebuild without archiving links or losing persistent data',async()=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-directory-cache-')),key=randomBytes(32);
  try{
    const runtime=await directory(join(root,'runtime')),home=await directory(join(runtime,'accounts','synthetic')),temporary=await directory(join(home,'tmp','arg0')),data=await directory(join(root,'data')),secrets=await directory(join(root,'secrets'));
    const outside=join(root,'outside');await writeFile(outside,'must not follow',{mode:0o600});
    const auth=join(home,'auth.json'),state=join(home,'state_5.sqlite'),job=join(runtime,'jobs','synthetic','context.json');await directory(join(runtime,'jobs','synthetic'));
    for(const [path,value] of [[auth,'synthetic authentication'],[state,'required session state'],[job,'required workspace context'],[join(data,'key-encryption.key'),'required key'],[join(secrets,'device.key'),'required device material']])await writeFile(path,value,{mode:0o600});
    await symlink(outside,join(temporary,'apply_patch'));
    const roots=[{id:'runtime',source:runtime},{id:'data',source:data},{id:'secrets',source:secrets}],legacy=await directory(join(root,'legacy'));
    await assert.rejects(captureFileSet({roots,directory:legacy,key}),{code:'transfer_unsupported_file_type'});
    const archive=await directory(join(root,'archive')),excludeDirectories=selection();
    const captured=await captureFileSet({roots,directory:archive,key,excludeDirectories,afterFile:async entry=>{if(entry.root==='runtime'&&entry.relative==='accounts/synthetic/auth.json')await rm(temporary,{recursive:true});}});
    const manifest=await readManifest(join(archive,'files.enc'),key);assert.deepEqual(manifest.excludeDirectories,excludeDirectories);assert.equal(Object.hasOwn(manifest,'excludeFiles'),false);
    await directory(temporary);await symlink(outside,join(temporary,'codex-linux-sandbox'));assert.equal(await verifyFileSetSources(manifest,{excludeDirectories}),true);
    await assert.rejects(verifyFileSetSources(manifest),{code:'transfer_file_selection_mismatch'});
    await assert.rejects(verifyFileSetSources(manifest,{excludeDirectories:[{root:'runtime',pattern:'jobs/*/tmp/arg0'}]}),{code:'transfer_file_selection_mismatch'});
    const restored=await directory(join(root,'restored'));await restoreFileSet({directory:archive,destination:restored,key,expectedManifestDigest:captured.manifestDigest});
    assert.deepEqual(await readdir(join(restored,'runtime/accounts/synthetic/tmp')),[]);
    for(const [path,value] of [['runtime/accounts/synthetic/auth.json','synthetic authentication'],['runtime/accounts/synthetic/state_5.sqlite','required session state'],['runtime/jobs/synthetic/context.json','required workspace context'],['data/key-encryption.key','required key'],['secrets/device.key','required device material']])assert.equal(await readFile(join(restored,path),'utf8'),value);
    assert.equal(await readFile(outside,'utf8'),'must not follow');
    await writeFile(auth,'changed auth',{mode:0o600});await assert.rejects(verifyFileSetSources(manifest,{excludeDirectories}),{code:'transfer_files_changed'});await writeFile(auth,'synthetic authentication',{mode:0o600});
    await writeFile(state,'changed state',{mode:0o600});await assert.rejects(verifyFileSetSources(manifest,{excludeDirectories}),{code:'transfer_files_changed'});
  }finally{key.fill(0);await rm(root,{recursive:true,force:true});}
});

test('a cache root symlink is rejected and a same-name ordinary file remains part of the backup',async()=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-directory-type-')),key=randomBytes(32);
  try{
    const runtime=await directory(join(root,'runtime')),tmp=await directory(join(runtime,'accounts','synthetic','tmp')),outside=await directory(join(root,'outside')),cache=join(tmp,'arg0'),excludeDirectories=selection();
    await symlink(outside,cache);await assert.rejects(captureFileSet({roots:[{id:'runtime',source:runtime}],directory:await directory(join(root,'rejected')),key,excludeDirectories}),{code:'transfer_unsupported_file_type'});
    await rm(cache);await writeFile(cache,'ordinary persistent file',{mode:0o600});
    const archive=await directory(join(root,'archive')),captured=await captureFileSet({roots:[{id:'runtime',source:runtime}],directory:archive,key,excludeDirectories}),restored=await directory(join(root,'restored'));
    await restoreFileSet({directory:archive,destination:restored,key,expectedManifestDigest:captured.manifestDigest});assert.equal(await readFile(join(restored,'runtime/accounts/synthetic/tmp/arg0'),'utf8'),'ordinary persistent file');
    const manifest=await readManifest(join(archive,'files.enc'),key);await writeFile(cache,'changed ordinary file',{mode:0o600});await assert.rejects(verifyFileSetSources(manifest,{excludeDirectories}),{code:'transfer_files_changed'});
    await rm(cache);await directory(cache);await symlink(outside,join(runtime,'accounts','synthetic','unknown-link'));
    await assert.rejects(captureFileSet({roots:[{id:'runtime',source:runtime}],directory:await directory(join(root,'unknown-link')),key,excludeDirectories}),{code:'transfer_unsupported_file_type'});
  }finally{key.fill(0);await rm(root,{recursive:true,force:true});}
});

test('directory selection keeps traversal budgets and does not select same paths in other material roots',async()=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-directory-scope-')),key=randomBytes(32);
  try{
    const runtime=await directory(join(root,'runtime')),cache=await directory(join(runtime,'accounts','synthetic','tmp','arg0')),data=await directory(join(root,'data')),required=await directory(join(data,'accounts','synthetic','tmp','arg0'));
    await writeFile(join(required,'required.json'),'required data',{mode:0o600});await symlink(root,join(cache,'apply_patch'));
    const roots=[{id:'runtime',source:runtime},{id:'data',source:data}],excludeDirectories=selection();
    await assert.rejects(captureFileSet({roots,directory:await directory(join(root,'bounded')),key,excludeDirectories,maxFiles:1}),{code:'transfer_file_capacity'});
    await assert.rejects(captureFileSet({roots,directory:await directory(join(root,'wrong-root')),key,excludeDirectories:[{root:'unknown',pattern:'accounts/*/tmp/arg0'}]}),{code:'transfer_file_exclusions_invalid'});
    const archive=await directory(join(root,'archive')),captured=await captureFileSet({roots,directory:archive,key,excludeDirectories}),restored=await directory(join(root,'restored'));
    await restoreFileSet({directory:archive,destination:restored,key,expectedManifestDigest:captured.manifestDigest});assert.equal(await readFile(join(restored,'data/accounts/synthetic/tmp/arg0/required.json'),'utf8'),'required data');
  }finally{key.fill(0);await rm(root,{recursive:true,force:true});}
});
