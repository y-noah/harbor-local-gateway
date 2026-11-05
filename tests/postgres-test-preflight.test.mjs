import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {fileURLToPath} from 'node:url';
import {postgresTestStoragePreflight} from '../ci/postgres-test-preflight.mjs';

test('CI storage floor rejects an insufficient filesystem without touching data or connecting to PostgreSQL',async()=>{
  const calls=[],inspect=async(path,options)=>{calls.push({path,options});return {bavail:path==='workspace'?10n:4n,bsize:1024n};};
  const result=await postgresTestStoragePreflight({env:{HARBOR_TEST_PG_MIN_FREE_BYTES:'5120'},workspace:'workspace',temporary:'temporary',inspect});
  assert.equal(result.passed,false);assert.deepEqual(result.filesystems,[{role:'workspace',available_bytes:'10240',passed:true},{role:'temporary',available_bytes:'4096',passed:false}]);
  assert.deepEqual(calls,[{path:'workspace',options:{bigint:true}},{path:'temporary',options:{bigint:true}}]);assert.equal(result.postgres_service_storage_proven,false);
});

test('CI storage boundary and large observations retain exact byte amounts',async()=>{
  const atBoundary=await postgresTestStoragePreflight({env:{HARBOR_TEST_PG_MIN_FREE_BYTES:'5120'},inspect:async()=>({bavail:5n,bsize:1024n})});assert.equal(atBoundary.passed,true);
  const large=await postgresTestStoragePreflight({env:{HARBOR_TEST_PG_MIN_FREE_BYTES:'9007199254740993'},inspect:async()=>({bavail:9007199254740992n,bsize:1n})});assert.equal(large.passed,false);assert.equal(large.filesystems[0].available_bytes,'9007199254740992');
});

test('invalid storage configuration is rejected before filesystem observation and does not echo environment values',async()=>{
  for(const value of ['0','-1','1.5','NaN',' 1024','fixture-private-value','1'.repeat(20),true]){
    let calls=0;await assert.rejects(postgresTestStoragePreflight({env:{HARBOR_TEST_PG_MIN_FREE_BYTES:value},inspect:async()=>{calls++;}}),error=>error.code==='ci_pg_storage_configuration'&&!error.message.includes(String(value)));assert.equal(calls,0);
  }
  await assert.rejects(postgresTestStoragePreflight({env:{},inspect:async()=>({bavail:-1n,bsize:1n})}),{code:'ci_pg_storage_observation'});
});

test('default budget is reported separately from actual local filesystem observations',async()=>{
  const result=await postgresTestStoragePreflight({env:{}});assert.equal(result.minimum_free_bytes,String(2n*1024n**3n));assert.equal(result.scope,'test-job-filesystems-only');assert.equal(result.filesystems.length,2);
  for(const filesystem of result.filesystems){assert.match(filesystem.available_bytes,/^\d+$/);assert.equal(filesystem.passed,BigInt(filesystem.available_bytes)>=BigInt(result.minimum_free_bytes));}
  assert.equal(result.postgres_service_storage_proven,false);
});

test('real preflight command exits unsuccessfully before tests when the observed disk cannot meet its budget',async()=>{
  await assert.rejects(promisify(execFile)(process.execPath,[fileURLToPath(new URL('../ci/postgres-test-preflight.mjs',import.meta.url))],{env:{...process.env,HARBOR_TEST_PG_MIN_FREE_BYTES:'9999999999999999999'},timeout:10000,maxBuffer:8192}),error=>{
    assert.equal(error.code,1);const result=JSON.parse(error.stdout);assert.equal(result.passed,false);assert.equal(result.postgres_service_storage_proven,false);assert.match(error.stderr,/已阻止运行大数据测试/);return true;
  });
});
