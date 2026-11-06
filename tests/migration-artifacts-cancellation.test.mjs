import test from 'node:test';
import assert from 'node:assert/strict';
import {PassThrough} from 'node:stream';
import {mkdtemp,mkdir,writeFile,readFile,stat,rm,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomBytes} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {sealStream,captureFileSet,verifyFileSetSources,readManifest,digestFile} from '../migration-artifacts.mjs';

async function fixture(t){
  const root=await mkdtemp(join(tmpdir(),'harbor-artifact-bounds-')),key=randomBytes(32),source=join(root,'source'),archive=join(root,'archive');
  await mkdir(source,{mode:0o700});await mkdir(archive,{mode:0o700});
  t.after(async()=>{await rm(root,{recursive:true,force:true});key.fill(0);});return {root,key,source,archive};
}

test('active encryption cancellation closes a real input stream and removes only its own ciphertext',{timeout:10000},async t=>{
  const f=await fixture(t),input=new PassThrough(),controller=new AbortController(),reason=Error('owned capture cancelled'),path=join(f.archive,'partial.enc'),keep=join(f.archive,'keep');
  await writeFile(keep,'preserve',{mode:0o600});
  const operation=sealStream(input,path,f.key,{signal:controller.signal,maxBytes:1048576}),rejection=assert.rejects(operation,error=>error===reason);
  input.write(Buffer.alloc(262144,0x42));
  const deadline=Date.now()+5000;
  for(;;){let bytes=0;try{bytes=(await stat(path)).size;}catch(error){if(error.code!=='ENOENT')throw error;}if(bytes>20)break;assert.ok(Date.now()<deadline,'encryption did not enter its actual stream');await delay(1);}
  controller.abort(reason);await rejection;
  assert.equal(input.destroyed,true);await assert.rejects(stat(path),{code:'ENOENT'});assert.equal(await readFile(keep,'utf8'),'preserve');
});

test('capture propagates cancellation before a file read and never publishes a complete catalog',async t=>{
  const f=await fixture(t),controller=new AbortController(),reason=Error('capture boundary cancelled');await writeFile(join(f.source,'content'),'synthetic',{mode:0o600});
  await assert.rejects(captureFileSet({roots:[{id:'data',source:f.source}],directory:f.archive,key:f.key,signal:controller.signal,beforeFile:()=>controller.abort(reason)}),error=>error===reason);
  assert.deepEqual(await readdir(f.archive),[]);assert.equal(await readFile(join(f.source,'content'),'utf8'),'synthetic');
  await assert.rejects(verifyFileSetSources({roots:[],files:0},{signal:controller.signal}),error=>error===reason);
});

test('source verification enforces per-file and aggregate bytes while preserving excluded-file and legacy semantics',async t=>{
  const f=await fixture(t);await writeFile(join(f.source,'a'),'AAAA',{mode:0o600});await writeFile(join(f.source,'b'),'BBBB',{mode:0o600});await writeFile(join(f.source,'ignored'),'ignored',{mode:0o600});
  const exclude=[{root:'data',relative:'ignored'}];
  await captureFileSet({roots:[{id:'data',source:f.source}],directory:f.archive,key:f.key,maxFiles:2,maxFileBytes:4,maxTotalBytes:8,exclude});
  const manifest=await readManifest(join(f.archive,'files.enc'),f.key);
  assert.equal(await verifyFileSetSources(manifest,{exclude}),true);
  assert.equal(await verifyFileSetSources(manifest,{exclude,maxFiles:2,maxFileBytes:4,maxTotalBytes:8}),true);
  await assert.rejects(verifyFileSetSources(manifest,{exclude,maxFileBytes:3}),{code:'transfer_file_capacity'});
  await assert.rejects(verifyFileSetSources(manifest,{exclude,maxTotalBytes:7}),{code:'transfer_file_capacity'});
  await assert.rejects(digestFile(join(f.source,'a'),{maxBytes:3}),{code:'transfer_file_capacity'});
  await assert.rejects(verifyFileSetSources(manifest,{exclude,maxFiles:1}),{code:'transfer_file_manifest_invalid'});
  assert.equal((await digestFile(join(f.source,'a'))).bytes,4);
});

test('directory enumeration rejects an oversized source before writing any encrypted file',async t=>{
  const f=await fixture(t);for(const name of ['a','b','c'])await writeFile(join(f.source,name),'x',{mode:0o600});
  await assert.rejects(captureFileSet({roots:[{id:'data',source:f.source}],directory:f.archive,key:f.key,maxFiles:2}),{code:'transfer_file_capacity'});
  assert.deepEqual(await readdir(f.archive),[]);assert.deepEqual((await readdir(f.source)).sort(),['a','b','c']);
});
