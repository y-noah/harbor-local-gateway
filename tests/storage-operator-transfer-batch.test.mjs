import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {fileURLToPath} from 'node:url';
import {canonical} from '../migration-artifacts.mjs';

if(process.argv.includes('--context-fixture')){
 // Exercise the actual root context factory and its private WeakMap. Only
 // root-file/physical I/O is synthetic; this grants no real root authority.
 const source={container:'synthetic-source',project:'synthetic',imageId:'sha256:'+'a'.repeat(64),uid:1000,user:'1000',dataDirectory:'/opt/synthetic/data',supervisorKind:'docker-restart'},candidate={image:'registry.invalid/gateway@sha256:'+'b'.repeat(64),imageDigest:'sha256:'+'b'.repeat(64),network:'synthetic',secretsDirectory:'/opt/synthetic/secrets',publicOrigin:'http://synthetic.invalid:8092',deviceOrigin:'https://synthetic.invalid:8093'},fileRoots=[{id:'data',source:source.dataDirectory},{id:'runtime',source:'/opt/synthetic/runtime',destinationPrefix:'/app/runtime'},{id:'secrets',source:candidate.secretsDirectory}],registry={database:'synthetic',databaseOid:'1',schema:'harbor_operator_registry',allowMetadataWrites:true},base={version:1,source,candidate,fileRoots,registry,runDirectory:'/opt/synthetic/run',expectedCluster:'42',backupRequired:{local:true,offhost:true},backupKeyVersion:'synthetic'};
 const previousConfiguration={sourceDataDir:source.dataDirectory,runDirectory:base.runDirectory,fileRoots,expectedCluster:base.expectedCluster,candidate,backupRequired:base.backupRequired,sourceIdentity:{device:'1',inode:'2'},sourceController:source,registry,backupKeyVersion:base.backupKeyVersion},hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
 async function contextFor(config){
  const context=vm.createContext({Buffer,URL,structuredClone,performance,process:{platform:'linux',getuid:()=>0}}),nativeFs=await import('node:fs/promises'),cache=new Map();
  const fakeFs={...nativeFs,realpath:async path=>path,lstat:async()=>({isDirectory:()=>true,isSymbolicLink:()=>false,dev:1n,ino:2n}),open:async path=>{assert.ok(['/etc/harbor-storage-operator.json','/etc/harbor-storage-operator.env'].includes(path));const text=path.endsWith('.json')?JSON.stringify(config):'HARBOR_STORAGE_OPERATOR_ADMIN_DATABASE_URL=postgres://synthetic@localhost/synthetic\n';return {stat:async()=>({isFile:()=>true,uid:0,mode:0o100600,size:Buffer.byteLength(text)}),readFile:async()=>text,close:async()=>{}};}};
  const module=new vm.SourceTextModule(await readFile(new URL('../storage-operator-context.mjs',import.meta.url),'utf8'),{context});
  await module.link(async specifier=>{if(cache.has(specifier))return cache.get(specifier);const values=specifier==='node:fs/promises'?fakeFs:specifier.startsWith('node:')?await import(specifier):await import(new URL(specifier,new URL('../storage-operator-context.mjs',import.meta.url)));const external=new vm.SyntheticModule(Object.keys(values),function(){for(const [name,value] of Object.entries(values))this.setExport(name,value);},{context});cache.set(specifier,external);return external;});await module.evaluate();const value=await module.namespace.createDockerStorageOperatorContext('/etc/harbor-storage-operator.json');return module.namespace.storageOperatorContext(value);
 }
 const legacy=await contextFor(base);assert.equal(Object.hasOwn(legacy.configuration,'transferBatchRows'),false);assert.equal(legacy.configurationDigest,hash(previousConfiguration),'omission must preserve the complete former configuration digest');
 for(const value of [1,100,500]){const configured=await contextFor({...base,transferBatchRows:value});assert.equal(configured.configuration.transferBatchRows,value);assert.equal(configured.configurationDigest,hash({...previousConfiguration,transferBatchRows:value}));assert.notEqual(configured.configurationDigest,legacy.configurationDigest);assert.ok(Object.isFrozen(configured.configuration));}
 for(const value of [0,-1,501,1.5,'500',null,false,{},[]])await assert.rejects(contextFor({...base,transferBatchRows:value}),{code:'storage_operator_transfer_batch_rows'});
 process.stdout.write(JSON.stringify({omittedDigestPreserved:true,explicitBoundsBoundToDigest:true,invalidRejected:true})+'\n');
}else test('root transfer batch option preserves omitted configuration digest and binds only explicit valid integers',{timeout:15000},async()=>{
 const {stdout}=await promisify(execFile)(process.execPath,['--experimental-vm-modules',fileURLToPath(import.meta.url),'--context-fixture'],{timeout:10000,maxBuffer:65536});assert.deepEqual(JSON.parse(stdout),{omittedDigestPreserved:true,explicitBoundsBoundToDigest:true,invalidRejected:true});
});
