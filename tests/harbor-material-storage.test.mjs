import assert from 'node:assert/strict';
import {test,afterEach,mock} from 'node:test';
import fs from 'node:fs/promises';
import childProcess from 'node:child_process';
import {syncBuiltinESMExports} from 'node:module';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {requireInstalledMaterialStorage,requireMaterialMaintenanceInactive} from '../harbor-material-storage.mjs';
import {withPostgresBackupOperation} from '../postgres-backup-lock.mjs';
const absent=()=>Object.assign(Error('absent'),{code:'ENOENT'}),maintenance='/etc/harbor-material-maintenance.json',config='/etc/harbor-data-paths.json';
afterEach(()=>{mock.restoreAll();syncBuiltinESMExports();});
function files(fn){mock.method(fs,'lstat',fn);syncBuiltinESMExports();}
function execute(fn){mock.method(childProcess,'execFile',fn);syncBuiltinESMExports();}
test('从未启用的安装不启动helper',async()=>{
 files(async()=>{throw absent();});execute(()=>assert.fail('unexpected helper'));
 assert.deepEqual(await requireInstalledMaterialStorage('restore','/var/lib/harbor-restore'),{enabled:false});
});
test('维护标记在最终manifest出现前即阻止恢复和新生成入口',async()=>{
 files(async path=>{if(path===maintenance)return {};throw absent();});execute(()=>assert.fail('unexpected helper'));
 await assert.rejects(requireInstalledMaterialStorage('restore','/var/lib/harbor-restore'),{code:'harbor_material_maintenance_in_progress'});
 await assert.rejects(requireMaterialMaintenanceInactive(),{code:'harbor_material_maintenance_in_progress'});
});
test('已启用安装使用固定helper与隔离环境并绑定调用方路径',async()=>{
 files(async path=>{if(path===config)return {};throw absent();});const calls=[];
 execute((binary,args,options,callback)=>{calls.push({binary,args,options});callback(null,'{}','');});
 assert.deepEqual(await requireInstalledMaterialStorage('restore','/var/lib/harbor-restore'),{enabled:true});
 assert.equal(calls.length,1);assert.equal(calls[0].binary,'/usr/bin/python3');
 assert.deepEqual(calls[0].args,['-I','-B','/usr/local/lib/harbor/harbor-material-storage.py','guard-path','restore','/var/lib/harbor-restore']);
 assert.deepEqual(calls[0].options.env,{PATH:'/usr/bin:/bin',LANG:'C'});
});
test('掉盘或helper失败输出固定错误且不回退到旧盘',async()=>{
 files(async path=>{if(path===config)return {};throw absent();});execute((binary,args,options,callback)=>callback(Error('private child output')));
 await assert.rejects(requireInstalledMaterialStorage('restore','/var/lib/harbor-restore'),error=>error.code==='harbor_material_storage_unavailable'&&!error.message.includes('private'));
});
test('取消在启动helper前结束',async()=>{
 files(async path=>{if(path===config)return {};throw absent();});execute(()=>assert.fail('must not launch'));
 const controller=new AbortController();controller.abort(Error('synthetic cancellation'));
 await assert.rejects(requireInstalledMaterialStorage('restore','/var/lib/harbor-restore',{signal:controller.signal}),/synthetic cancellation/);
});
test('真实归档flock取得后再出现维护标记时仍拒绝新备份操作',async()=>{
 const directory=await fs.mkdtemp(join(tmpdir(),'harbor-material-lock-')),original=fs.lstat;let checks=0,ran=false;
 files(async path=>{if(path===maintenance){if(++checks===1)throw absent();return {};}return original(path);});
 try{
  await assert.rejects(withPostgresBackupOperation({scope:'existing-cluster',archiveDirectory:directory,restoreUid:process.getuid()},async()=>{ran=true;}),{code:'harbor_material_maintenance_in_progress'});
  assert.equal(ran,false);assert.equal(checks,2);
 }finally{await fs.rm(directory,{recursive:true,force:true});}
});
