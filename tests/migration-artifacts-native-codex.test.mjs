import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {mkdtemp,mkdir,lstat,readdir,readFile,writeFile,rm} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {randomBytes} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {captureFileSet,restoreFileSet,verifyFileSetSources,readManifest} from '../migration-artifacts.mjs';
import {periodicApplicationDirectoryExclusions} from '../postgres-periodic-application-policy.mjs';

const binary=process.env.HARBOR_TEST_NATIVE_CODEX_BINARY;
test('native Codex launcher aliases are rebuilt after a live encrypted capture and restore',{
  skip:!binary?'需显式提供已核验的原生Codex binary；不使用用户认证或发模型请求':false,timeout:45000,
},async()=>{
  // Codex deliberately skips helper aliases for CODEX_HOME under /tmp. Use the
  // existing ignored work directory, with an isolated home and no credentials.
  const work=resolve('work');await mkdir(work,{recursive:true,mode:0o700});const info=await lstat(work);
  assert.ok(info.isDirectory()&&!info.isSymbolicLink()&&(info.mode&0o022)===0);
  const root=await mkdtemp(join(work,'native-codex-backup-')),key=randomBytes(32),runtime=join(root,'runtime'),home=join(runtime,'accounts','synthetic');let active;
  async function aliases(currentHome){
    const directory=join(currentHome,'tmp','arg0');let children;
    try{children=await readdir(directory,{withFileTypes:true});}catch(error){if(error.code==='ENOENT')return [];throw error;}
    const links=[];
    for(const child of children){
      if(!child.isDirectory())continue;
      for(const row of await readdir(join(directory,child.name),{withFileTypes:true}))if(row.isSymbolicLink())links.push(row.name);
    }
    return links;
  }
  async function start(currentHome,sqliteName){
    const sqlite=join(root,sqliteName);await mkdir(sqlite,{mode:0o700});
    const child=spawn(resolve(binary),['app-server','--listen','stdio://','--disable','plugins','--disable','apps','-c','cli_auth_credentials_store="file"'],{
      cwd:root,detached:true,stdio:['pipe','pipe','pipe'],env:{PATH:'/usr/bin:/bin',HOME:root,CODEX_HOME:currentHome,CODEX_SQLITE_HOME:sqlite,CODEX_MANAGED_BY_NPM:'1',HTTP_PROXY:'http://127.0.0.1:9',HTTPS_PROXY:'http://127.0.0.1:9',ALL_PROXY:'http://127.0.0.1:9',NO_PROXY:'127.0.0.1,localhost'},
    });
    let initialized=false,output='';child.stdout.on('data',bytes=>{
      output+=bytes.toString();let newline;
      while((newline=output.indexOf('\n'))>=0){const line=output.slice(0,newline);output=output.slice(newline+1);try{const message=JSON.parse(line);if(message.id===1&&!message.error)initialized=true;}catch{}}
      if(output.length>1048576)output='';
    });child.stderr.resume();child.stdin.on('error',()=>{});
    let exited,failed;const done=new Promise(resolve=>{child.once('error',error=>{failed=error;});child.once('close',(code,signal)=>{exited={code,signal};resolve(exited);});});
    active={child,done};child.stdin.write(JSON.stringify({id:1,method:'initialize',params:{clientInfo:{name:'harbor-private-backup-test',version:'1.0.0'},capabilities:{experimentalApi:true}}})+'\n');
    const end=Date.now()+10000;let found=[];
    while(Date.now()<end){if(failed)throw failed;if(exited)throw Error('native_codex_closed_before_aliases');found=await aliases(currentHome);if(initialized&&found.length>=2)return found;await delay(25);}
    throw Error('native_codex_aliases_deadline');
  }
  async function stop(){
    const current=active;current.child.stdin.end();let timer;
    try{assert.deepEqual(await Promise.race([current.done,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('native_codex_close_deadline')),10000);})]),{code:0,signal:null});active=null;}finally{clearTimeout(timer);}
  }
  try{
    await mkdir(home,{recursive:true,mode:0o700});await writeFile(join(home,'auth.json'),'{"OPENAI_API_KEY":null}',{mode:0o600});
    await writeFile(join(home,'persistent-context.json'),'synthetic persistent context',{mode:0o600});
    const first=await start(home,'sqlite-first');assert.ok(first.includes('apply_patch'));
    const archive=join(root,'archive');await mkdir(archive,{mode:0o700});
    await assert.rejects(captureFileSet({roots:[{id:'runtime',source:runtime}],directory:archive,key}),{code:'transfer_unsupported_file_type'});
    const selected=join(root,'selected');await mkdir(selected,{mode:0o700});const excludeDirectories=periodicApplicationDirectoryExclusions({runtimeRebuildableCaches:['codex-launcher-aliases']});
    const capture=await captureFileSet({roots:[{id:'runtime',source:runtime}],directory:selected,key,excludeDirectories});
    const manifest=await readManifest(join(selected,'files.enc'),key);assert.deepEqual(manifest.excludeDirectories,excludeDirectories);
    await stop();assert.deepEqual(await aliases(home),[]);assert.equal(await verifyFileSetSources(manifest,{excludeDirectories}),true);
    const restored=join(root,'restored');await mkdir(restored,{mode:0o700});await restoreFileSet({directory:selected,destination:restored,key,expectedManifestDigest:capture.manifestDigest});
    const restoredHome=join(restored,'runtime','accounts','synthetic');assert.deepEqual(await aliases(restoredHome),[]);
    assert.equal(await readFile(join(restoredHome,'auth.json'),'utf8'),'{"OPENAI_API_KEY":null}');
    assert.equal(await readFile(join(restoredHome,'persistent-context.json'),'utf8'),'synthetic persistent context');
    const second=await start(restoredHome,'sqlite-second');assert.deepEqual([...second].sort(),[...first].sort());await stop();assert.deepEqual(await aliases(restoredHome),[]);
  }finally{
    if(active){if(active.child.pid&&active.child.exitCode===null&&active.child.signalCode===null)try{process.kill(-active.child.pid,'SIGKILL');}catch(error){if(error.code!=='ESRCH')throw error;}await active.done;}
    key.fill(0);await rm(root,{recursive:true,force:true});
  }
});
