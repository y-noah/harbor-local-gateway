import {test} from 'node:test';
import {tmpdir} from 'node:os';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,rmSync,writeFileSync} from 'node:fs';
import net from 'node:net';
import {resolve,join} from 'node:path';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {acquireInstance} from '../instance.mjs';
const scratch=join(tmpdir(),'harbor-instance-tests');mkdirSync(scratch,{recursive:true});
function temp(t){const dir=mkdtempSync(join(scratch,'lock-'));t.after(()=>{assert.ok(resolve(dir).startsWith(scratch));rmSync(dir,{recursive:true,force:true});});return dir;}
test('second workspace instance fails before opening or recovering the store',async t=>{
  const dir=temp(t),first=await acquireInstance(dir);
  try{await assert.rejects(acquireInstance(dir),{code:'EADDRINUSE'});}finally{await first.release();}
  const next=await acquireInstance(dir);await next.release();await next.release();
});
test('different isolated workspaces can run concurrently',async t=>{
  const dir=temp(t),a=await acquireInstance(join(dir,'one')),b=await acquireInstance(join(dir,'two'));
  await Promise.all([a.release(),b.release()]);
});
for(const signal of ['SIGTERM','SIGKILL'])test('process '+signal+' releases kernel workspace lock', {skip:!['win32','linux'].includes(process.platform),timeout:10000},async t=>{
  const dir=temp(t),moduleUrl=new URL('../instance.mjs',import.meta.url).href;
  const code=`import {acquireInstance} from ${JSON.stringify(moduleUrl)}; await acquireInstance(${JSON.stringify(dir)}); console.log('locked');`;
  const child=spawn(process.execPath,['--input-type=module','-e',code],{windowsHide:true,stdio:['ignore','pipe','pipe']});
  t.after(()=>child.kill());
  await new Promise((resolve,reject)=>{child.once('error',reject);child.stdout.once('data',resolve);child.once('exit',c=>reject(Error('Child exited early '+c)));});
  await assert.rejects(acquireInstance(dir),{code:'EADDRINUSE'});
  const closed=once(child,'exit');child.kill(signal);await closed;
  const recovered=await acquireInstance(dir);await recovered.release();
});

test('Linux gateway restarts after stop/crash and a failed candidate releases its lock', {skip:process.platform!=='linux',timeout:20000},async t=>{
 const dir=temp(t),data=join(dir,'data'),runtime=join(dir,'runtime');mkdirSync(data);
 writeFileSync(join(data,'settings.json'),JSON.stringify({adminToken:'fixture-admin',codexBinary:process.execPath,reserveTokens:100}));
 const reservation=net.createServer();await new Promise(r=>reservation.listen(0,'127.0.0.1',r));const port=reservation.address().port;
 t.after(()=>reservation.close());await new Promise(r=>reservation.close(r));
 const children=[];t.after(()=>children.forEach(child=>child.kill('SIGKILL')));
 const start=()=>{const child=spawn(process.execPath,[new URL('../server.mjs',import.meta.url).pathname],{env:{...process.env,HARBOR_DATA_DIR:data,HARBOR_RUNTIME_DIR:runtime,HARBOR_PORT:String(port),HARBOR_LISTEN_HOST:'127.0.0.1',HARBOR_PUBLIC_ORIGINS:''},stdio:['ignore','pipe','pipe']});children.push(child);return child;};
 const ready=child=>new Promise((resolve,reject)=>{let text='';child.stdout.on('data',part=>{text+=part;if(text.includes('Harbor ready'))resolve();});child.once('error',reject);child.once('exit',code=>reject(Error('gateway exited before ready '+code)));});
 for(const signal of ['SIGTERM','SIGKILL']){
  const child=start();await ready(child);assert.equal((await fetch('http://127.0.0.1:'+port+'/health')).status,200);
  const exited=once(child,'exit');child.kill(signal);await exited;
 }
 await new Promise(r=>reservation.listen(port,'127.0.0.1',r));const candidate=start();const [code]=await once(candidate,'exit');assert.notEqual(code,0);await new Promise(r=>reservation.close(r));
 const restored=start();await ready(restored);assert.equal((await fetch('http://127.0.0.1:'+port+'/health')).status,200);const exited=once(restored,'exit');restored.kill('SIGTERM');await exited;
});
