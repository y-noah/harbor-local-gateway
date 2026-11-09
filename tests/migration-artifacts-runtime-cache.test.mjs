import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtemp,mkdir,writeFile,readFile,rm,readdir,symlink,rename} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {randomBytes} from 'node:crypto';
import {captureFileSet,restoreFileSet,readManifest,verifyFileSetSources,fileSetExclusions} from '../migration-artifacts.mjs';
import {periodicApplicationFileExclusions} from '../postgres-periodic-application-policy.mjs';

test('account model catalog replacement does not invalidate an opted-in backup and other JSON remains authenticated',async()=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-model-catalog-')),key=randomBytes(32);
  try{
    const runtime=join(root,'runtime'),data=join(root,'data'),secrets=join(root,'secrets'),account=join(runtime,'accounts','synthetic'),jobs=join(runtime,'jobs'),legacy=join(root,'legacy'),archive=join(root,'archive'),restored=join(root,'restored');
    for(const path of [account,jobs,data,secrets,legacy,archive,restored])await mkdir(path,{recursive:true,mode:0o700});
    const cache=join(account,'models_cache.json'),auth=join(account,'auth.json'),state=join(account,'state_5.sqlite'),job=join(jobs,'models_cache.json');
    for(const [path,value] of [[cache,'{"models":[],"fetched_at":1}'],[auth,'synthetic credentials'],[state,'required session state'],[job,'required job data'],[join(data,'models_cache.json'),'required data'],[join(secrets,'models_cache.json'),'required secrets']])await writeFile(path,value,{mode:0o600});
    let revision=1;const churn=async({root:id,relative})=>{if(id==='runtime'&&relative==='jobs/models_cache.json'){const pending=cache+'.pending';await writeFile(pending,JSON.stringify({models:[],fetched_at:++revision}),{mode:0o600});await rename(pending,cache);}};
    const roots=[{id:'runtime',source:runtime},{id:'data',source:data},{id:'secrets',source:secrets}];
    await assert.rejects(captureFileSet({roots,directory:legacy,key,afterFile:churn}),{code:'transfer_files_changed'});
    const excludeFiles=periodicApplicationFileExclusions({excludeModelCatalogCache:true});
    const bundle=await captureFileSet({roots,directory:archive,key,excludeFiles,afterFile:churn}),manifest=await readManifest(join(archive,'files.enc'),key);
    assert.deepEqual(manifest.excludeFiles,excludeFiles);await assert.rejects(verifyFileSetSources(manifest),{code:'transfer_file_selection_mismatch'});
    assert.equal(await verifyFileSetSources(manifest,{excludeFiles}),true);
    await restoreFileSet({directory:archive,destination:restored,key,expectedManifestDigest:bundle.manifestDigest});
    assert.deepEqual((await readdir(join(restored,'runtime/accounts/synthetic'))).sort(),['auth.json','state_5.sqlite']);
    for(const [path,value] of [['runtime/accounts/synthetic/auth.json','synthetic credentials'],['runtime/accounts/synthetic/state_5.sqlite','required session state'],['runtime/jobs/models_cache.json','required job data'],['data/models_cache.json','required data'],['secrets/models_cache.json','required secrets']])assert.equal(await readFile(join(restored,path),'utf8'),value);
    await writeFile(auth,'changed credentials',{mode:0o600});await assert.rejects(verifyFileSetSources(manifest,{excludeFiles}),{code:'transfer_files_changed'});await writeFile(auth,'synthetic credentials',{mode:0o600});
    await writeFile(job,'changed job data',{mode:0o600});await assert.rejects(verifyFileSetSources(manifest,{excludeFiles}),{code:'transfer_files_changed'});
  }finally{key.fill(0);await rm(root,{recursive:true,force:true});}
});

test('declared runtime caches can change during capture while auth and unknown SQLite files remain strictly verified',async()=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-runtime-cache-')),key=randomBytes(32);let cache;
  try{
    const source=join(root,'source'),account=join(source,'account'),legacy=join(root,'legacy'),archive=join(root,'archive'),restored=join(root,'restored');
    for(const path of [source,legacy,archive,restored])await mkdir(path,{mode:0o700});await mkdir(account,{mode:0o700});
    const auth=join(account,'auth.json'),unknown=join(account,'unknown.sqlite');await writeFile(auth,'synthetic login material',{mode:0o600});await writeFile(unknown,'must remain covered',{mode:0o600});
    cache=new DatabaseSync(join(account,'state_5.sqlite'));cache.exec('PRAGMA journal_mode=WAL; CREATE TABLE cache(value TEXT)');let revision=0;
    const churn=async({relative})=>{if(relative==='account/unknown.sqlite'){cache.prepare('INSERT INTO cache VALUES(?)').run('synthetic '+(++revision));await writeFile(join(account,'logs_'+revision+'.sqlite'),'rebuildable cache',{mode:0o600});}};
    await assert.rejects(captureFileSet({roots:[{id:'runtime',source}],directory:legacy,key,afterFile:churn}),{code:'transfer_files_changed'});
    const excludeFiles=fileSetExclusions(['state','logs'].flatMap(name=>['','-wal','-shm'].map(suffix=>({root:'runtime',pattern:'**/'+name+'_*.sqlite'+suffix}))));
    await writeFile(join(account,'logs_0.sqlite'),'temporary cache',{mode:0o600});await writeFile(join(account,'logs_0.sqlite-wal'),'temporary sidecar',{mode:0o600});
    const bundle=await captureFileSet({roots:[{id:'runtime',source}],directory:archive,key,excludeFiles,afterFile:async value=>{if(value.relative==='account/auth.json'){await rm(join(account,'logs_0.sqlite'));await rm(join(account,'logs_0.sqlite-wal'));}await churn(value);}});
    const manifest=await readManifest(join(archive,'files.enc'),key);assert.deepEqual(manifest.excludeFiles,excludeFiles);
    await assert.rejects(verifyFileSetSources(manifest),{code:'transfer_file_selection_mismatch'});assert.equal(await verifyFileSetSources(manifest,{excludeFiles}),true);
    await restoreFileSet({directory:archive,destination:restored,key,expectedManifestDigest:bundle.manifestDigest});
    assert.deepEqual((await readdir(join(restored,'runtime/account'))).sort(),['auth.json','unknown.sqlite']);assert.equal(await readFile(join(restored,'runtime/account/auth.json'),'utf8'),'synthetic login material');assert.equal(await readFile(join(restored,'runtime/account/unknown.sqlite'),'utf8'),'must remain covered');
    await writeFile(auth,'changed credential',{mode:0o600});await assert.rejects(verifyFileSetSources(manifest,{excludeFiles}),{code:'transfer_files_changed'});await writeFile(auth,'synthetic login material',{mode:0o600});
    await writeFile(unknown,'changed unknown database',{mode:0o600});await assert.rejects(verifyFileSetSources(manifest,{excludeFiles}),{code:'transfer_files_changed'});
    const bounded=join(root,'bounded');await mkdir(bounded,{mode:0o700});await assert.rejects(captureFileSet({roots:[{id:'runtime',source}],directory:bounded,key,excludeFiles,maxFiles:4}),{code:'transfer_file_capacity'});
    const disappearing=join(root,'disappearing-directory');await mkdir(disappearing,{mode:0o700});await mkdir(join(account,'logs_3.sqlite'),{mode:0o700});
    await assert.rejects(captureFileSet({roots:[{id:'runtime',source}],directory:disappearing,key,excludeFiles,afterFile:async value=>{if(value.relative==='account/auth.json')await rm(join(account,'logs_3.sqlite'),{recursive:true});}}),{code:'ENOENT'});
    const unsafe=join(root,'unsafe');await mkdir(unsafe,{mode:0o700});await symlink('unknown.sqlite',join(account,'logs_4.sqlite'));
    await assert.rejects(captureFileSet({roots:[{id:'runtime',source}],directory:unsafe,key,excludeFiles}),{code:'transfer_unsupported_file_type'});
  }finally{cache?.close();key.fill(0);await rm(root,{recursive:true,force:true});}
});
