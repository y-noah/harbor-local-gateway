import test from 'node:test';
import {tmpdir} from 'node:os';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {mkdtempSync,mkdirSync,rmSync,existsSync,statSync,writeFileSync,readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {join,resolve} from 'node:path';
import {runCodex} from '../codex.mjs';
import {runClaude} from '../claude.mjs';
import {readRateLimits} from '../rates.mjs';
const scratch=join(tmpdir(),'harbor-provider-tests');mkdirSync(scratch,{recursive:true});
const childFile=fileURLToPath(new URL('./provider-child.mjs',import.meta.url));
function fixture(t,provider,scenario){
  const dir=mkdtempSync(join(scratch,'case-'));let child;
  t.after(()=>{child?.kill();assert.ok(resolve(dir).startsWith(resolve(scratch)));rmSync(dir,{recursive:true,force:true});});
  const run=provider==='codex'?runCodex:runClaude;
  return {call:extra=>run({binary:process.execPath,home:dir,workspace:join(dir,'job'),messages:[{role:'user',content:'hello'}],timeoutMs:3000,spawnProcess:(_,args,options)=>(child=spawn(process.execPath,[childFile,provider,scenario],options)),...extra})};
}
for(const provider of ['codex','claude']){
  test(provider+' reads fragmented Unicode and final JSON without newline, with resumed usage delta',async t=>{
    const f=fixture(t,provider,'ok');const r=await f.call({sessionId:'fixed-thread',previousUsage:{input:100,output:10,cached:40}});
    assert.equal(r.text,'中文测试 🍋 Unicode');assert.equal(r.input,20);assert.equal(r.output,10);assert.equal(r.cached,10);
  });
  test(provider+' rejects abnormal exit even when complete result was printed',async t=>{
    await assert.rejects(fixture(t,provider,'nonzero').call(),e=>e.usageKnown===true&&e.input===120&&e.output===20);
  });
  test(provider+' timeout terminates real subprocess and marks usage unknown',async t=>{
    const begin=Date.now();await assert.rejects(fixture(t,provider,'hang').call({timeoutMs:80}),e=>e.usageKnown===false);assert.ok(Date.now()-begin<2000);
  });
  test(provider+' client abort terminates real subprocess and marks usage unknown',async t=>{
    const controller=new AbortController();const call=fixture(t,provider,'hang').call({signal:controller.signal});setTimeout(()=>controller.abort(),70);
    await assert.rejects(call,e=>e.usageKnown===false);
  });
  test(provider+' rejects missing usage instead of silently charging zero',async t=>{
    await assert.rejects(fixture(t,provider,'missing-usage').call(),e=>e.usageKnown===false);
  });
  test(provider+' refuses a changed upstream session during resume',async t=>{
    await assert.rejects(fixture(t,provider,'different-session').call({sessionId:'fixed-thread'}),e=>e.usageKnown===false);
  });
  test(provider+' rejects oversized output and does not echo raw diagnostics',async t=>{
    await assert.rejects(fixture(t,provider,'oversize').call(),e=>e.usageKnown===false&&e.message.length<200);
    await assert.rejects(fixture(t,provider,'invalid-json').call(),e=>e.usageKnown===false&&!e.message.includes('private'));
  });
}
test('Codex explicit failure after an agent message is still a failed request',async t=>{
  await assert.rejects(fixture(t,'codex','error').call(),e=>e.usageKnown===true);
});
test('Codex blocks unexpected tools and rejects impossible cached usage',async t=>{
  await assert.rejects(fixture(t,'codex','tool').call());
  await assert.rejects(fixture(t,'codex','bad-cache').call(),e=>e.usageKnown===false);
});
test('quota subprocess handles initialize/read exchange and contains malformed output or hangs',async()=>{
  const call=scenario=>readRateLimits('unused',scratch,{timeoutMs:scenario==='hang'?100:3000,spawnProcess:(_,args,options)=>spawn(process.execPath,[childFile,'rates',scenario],options)});
  assert.equal((await call('ok')).rateLimits.primary.usedPercent,25);
  for(const scenario of ['error','oversize','hang'])await assert.rejects(call(scenario),e=>!e.message.includes('private'));
});

test('quota subprocess cancellation skips pre-aborted spawn and stops a running official protocol exchange',async()=>{
  const before=new AbortController();before.abort();let spawned=false;
  await assert.rejects(readRateLimits('unused',scratch,{signal:before.signal,spawnProcess:()=>{spawned=true;}}),/已取消/);assert.equal(spawned,false);
  const controller=new AbortController();let child;
  const pending=readRateLimits('unused',scratch,{signal:controller.signal,spawnProcess:(_,args,options)=>child=spawn(process.execPath,[childFile,'rates','hang'],options)});
  const ended=new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('cancelled quota child did not exit')),3000);child.once('close',()=>{clearTimeout(timer);resolve();});});
  controller.abort();await assert.rejects(pending,/已取消/);await ended;
});

test('quota reads isolate SQLite from account state and settle only after graceful child close and cleanup',async t=>{
  const home=mkdtempSync(join(scratch,'quota-account-'));t.after(()=>rmSync(home,{recursive:true,force:true}));
  const state=join(home,'state_5.sqlite');writeFileSync(state,'existing CLI context');
  let sqliteHome,closed=false,child;
  const result=await readRateLimits('unused',home,{spawnProcess:(_,args,options)=>{
    assert.equal(options.env.CODEX_HOME,home);sqliteHome=options.env.CODEX_SQLITE_HOME;
    assert.ok(sqliteHome&&!resolve(sqliteHome).startsWith(resolve(home)+'/'));assert.equal(statSync(sqliteHome).mode&0o777,0o700);
    child=spawn(process.execPath,[childFile,'rates','cache'],options);child.once('close',()=>{closed=true;});return child;
  }});
  assert.equal(result.rateLimits.primary.usedPercent,25);assert.equal(closed,true);assert.equal(child.exitCode,0);assert.equal(child.signalCode,null);
  assert.equal(existsSync(sqliteHome),false);assert.equal(readFileSync(state,'utf8'),'existing CLI context');
});

test('quota spawn errors and cancellation clean their private cache after the real process exits',async()=>{
  let sqliteHome;
  await assert.rejects(readRateLimits('unused',scratch,{spawnProcess:(_,args,options)=>{sqliteHome=options.env.CODEX_SQLITE_HOME;throw Error('private spawn diagnostic');}}),/无法启动/);
  assert.equal(existsSync(sqliteHome),false);
  const controller=new AbortController();let closed=false;
  const pending=readRateLimits('unused',scratch,{signal:controller.signal,spawnProcess:(_,args,options)=>{
    sqliteHome=options.env.CODEX_SQLITE_HOME;const child=spawn(process.execPath,[childFile,'rates','hang'],options);child.once('close',()=>{closed=true;});return child;
  }});
  controller.abort();await assert.rejects(pending,/已取消/);assert.equal(closed,true);assert.equal(existsSync(sqliteHome),false);
});

test('quota cancellation after a result keeps the slot and cache until the real child closes',{timeout:5000},async t=>{
  const controller=new AbortController();let child,cache,ready,settled=false;
  const received=new Promise(resolve=>{ready=resolve;});
  t.after(()=>{if(child?.exitCode===null&&child?.signalCode===null)child.kill('SIGKILL');});
  const pending=readRateLimits('unused',scratch,{signal:controller.signal,spawnProcess:(_,args,options)=>{
    cache=options.env.CODEX_SQLITE_HOME;child=spawn(process.execPath,[childFile,'rates','delayed-close'],{...options,stdio:['pipe','pipe','pipe','ipc']});child.stdin.once('finish',ready);return child;
  }}).finally(()=>{settled=true;});
  const rejected=assert.rejects(pending,/已取消/);
  await received;assert.equal(existsSync(cache),true);controller.abort();await Promise.resolve();assert.equal(settled,false);assert.equal(existsSync(cache),true);
  child.send('release');await rejected;assert.equal(child.exitCode,0);assert.equal(existsSync(cache),false);
});

test('quota close deadline rejects a received result and preserves uncertain native cache for inspection',{timeout:5000},async t=>{
  t.mock.timers.enable({apis:['setTimeout']});let child,cache,ready;
  const received=new Promise(resolve=>{ready=resolve;});
  t.after(()=>{if(child?.exitCode===null&&child?.signalCode===null)child.kill('SIGKILL');if(child?.signalCode==='SIGKILL')rmSync(cache,{recursive:true,force:true});});
  const pending=readRateLimits('unused',scratch,{closeTimeoutMs:100,spawnProcess:(_,args,options)=>{
    cache=options.env.CODEX_SQLITE_HOME;child=spawn(process.execPath,[childFile,'rates','delayed-close'],{...options,stdio:['pipe','pipe','pipe','ipc']});child.stdin.once('finish',ready);return child;
  }});
  const rejected=assert.rejects(pending,/未正常退出/);await received;t.mock.timers.tick(100);await rejected;
  assert.equal(child.signalCode,'SIGKILL');assert.equal(existsSync(cache),true);
});

for(const provider of ['codex','claude'])test(provider+' optional text observer sees parsed Unicode and cannot turn a successful model call into a failure',async t=>{
 const seen=[],result=await fixture(t,provider,'ok').call({onText:text=>{seen.push(text);throw Error('synthetic observer unavailable');}});assert.deepEqual(seen,['中文测试 🍋 Unicode']);assert.equal(result.text,'中文测试 🍋 Unicode');assert.equal(result.usageKnown,true);
});
