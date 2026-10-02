import test from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {mkdtempSync,mkdirSync,rmSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {join,resolve} from 'node:path';
import {runCodex} from '../codex.mjs';
import {runClaude} from '../claude.mjs';
import {readRateLimits} from '../rates.mjs';
const scratch=fileURLToPath(new URL('../../../work/provider-tests/',import.meta.url));mkdirSync(scratch,{recursive:true});
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
