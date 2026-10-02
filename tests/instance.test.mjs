import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,rmSync} from 'node:fs';
import {resolve,join} from 'node:path';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {acquireInstance} from '../instance.mjs';
const scratch=resolve(import.meta.dirname,'../../../work/instance-tests');mkdirSync(scratch,{recursive:true});
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
test('Windows process crash releases named pipe workspace lock', {skip:process.platform!=='win32'},async t=>{
  const dir=temp(t),moduleUrl=new URL('../instance.mjs',import.meta.url).href;
  const code=`import {acquireInstance} from ${JSON.stringify(moduleUrl)}; await acquireInstance(${JSON.stringify(dir)}); console.log('locked');`;
  const child=spawn(process.execPath,['--input-type=module','-e',code],{windowsHide:true,stdio:['ignore','pipe','pipe']});
  t.after(()=>child.kill());
  await new Promise((resolve,reject)=>{child.once('error',reject);child.stdout.once('data',resolve);child.once('exit',c=>reject(Error('Child exited early '+c)));});
  await assert.rejects(acquireInstance(dir),{code:'EADDRINUSE'});
  const closed=once(child,'exit');child.kill();await closed;
  const recovered=await acquireInstance(dir);await recovered.release();
});
