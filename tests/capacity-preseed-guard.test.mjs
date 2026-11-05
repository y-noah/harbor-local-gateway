import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,writeFile,stat,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createTarget,preflightCapacityTarget} from '../tools/capacity/gateway-adapter.mjs';
const execute=promisify(execFile);

test('formal run without execution authorization rejects before loading an adapter',async()=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-capacity-guard-')),adapter=join(root,'adapter.mjs'),marker=join(root,'loaded');try{
    await writeFile(adapter,'import {writeFileSync} from "node:fs";writeFileSync('+JSON.stringify(marker)+',"loaded");export async function createTarget(){throw Error("must_not_run");}\n');let result;try{await execute(process.execPath,['tools/capacity/cli.mjs','run','--adapter',adapter],{cwd:new URL('..',import.meta.url),timeout:10000,maxBuffer:65536});assert.fail('formal guard did not reject');}catch(error){result=error;}assert.match(result.stderr,/capacity_formal_execution_not_authorized/);await assert.rejects(stat(marker),{code:'ENOENT'});
  }finally{await rm(root,{recursive:true,force:true});}
});

test('current formal run, seed and verify reject before allocating a PG or reaching its invalid binary path',async()=>{
  for(const purpose of ['run','seed','verify']){let setup=false;await assert.rejects(createTarget({profile:'formal',purpose,binDirectory:'/definitely-unavailable/postgresql/18/bin',onSetup:async()=>{setup=true;}}),/capacity_formal_adapter_incomplete/);assert.equal(setup,false);assert.equal(preflightCapacityTarget({profile:'formal',purpose}).phase,'before_allocation');}
  assert.equal(preflightCapacityTarget({profile:'selfcheck'}).eligible,true);
});
