import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,writeFile,readFile,stat,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';

test('CLI preserves its real completed report and exits nonzero when final target cleanup rejects',{skip:!process.env.HARBOR_CAPACITY_PG_BIN,timeout:120000},async()=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-capacity-cli-close-')),adapter=join(root,'adapter.mjs'),output=join(root,'report.json'),factory=new URL('../tools/capacity/gateway-adapter.mjs',import.meta.url).href,execute=promisify(execFile);
  try{
    await writeFile(adapter,'import {createTarget as original} from '+JSON.stringify(factory)+';export async function createTarget(options){const target=await original({...options,employeeCount:1});target.plan={...target.plan,scenarios:[{id:"cleanup-report",durationMs:200,modelQps:5,adminQps:5,employees:1,administrators:1,upstreamMs:[30,40],payloadBytes:1024}]};const close=target.close;target.close=async()=>{await close();throw Error("private-cleanup-detail-must-not-escape");};return target;}\n',{mode:0o600});
    let failure;try{await execute(process.execPath,['tools/capacity/cli.mjs','run','--profile','selfcheck','--adapter',adapter,'--pg-bin',process.env.HARBOR_CAPACITY_PG_BIN,...(process.env.HARBOR_CAPACITY_PG_LIB?['--pg-lib',process.env.HARBOR_CAPACITY_PG_LIB]:[]),'--output',output],{cwd:new URL('..',import.meta.url),timeout:110000,maxBuffer:1048576});assert.fail('cleanup rejection must produce a nonzero exit');}catch(error){failure=error;}
    assert.equal(failure.code,1);assert.equal(failure.killed,false);const serialized=await readFile(output,'utf8'),report=JSON.parse(serialized);
    assert.equal(report.schema,'harbor-capacity-report-v1');assert.equal(report.evidence.scenarios['cleanup-report'].completed,true);assert.equal(report.evidence.dataReconciliation.passed,true);assert.equal(report.evidence.dataReconciliation.counts.accepted,1);assert.ok(report.metrics.arrivals.sent>=2);assert.equal(report.verdict.status,'failed');assert.ok(report.verdict.failures.includes('capacity_target_cleanup_failed'));assert.deepEqual(report.cleanup,{passed:false,code:'capacity_target_cleanup_failed'});assert.equal(serialized.includes('private-cleanup-detail-must-not-escape'),false);assert.equal(failure.stderr.includes('private-cleanup-detail-must-not-escape'),false);assert.equal((await stat(output)).mode&0o777,0o600);
  }finally{await rm(root,{recursive:true,force:true});}
});
