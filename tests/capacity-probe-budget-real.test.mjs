import test from 'node:test';
import assert from 'node:assert/strict';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {createRecordingProbeStore} from '../postgres-recording-storage.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('the production probe factory rejects a real over-budget query despite looser caller settings, then serves the next query',{
  skip:!binDirectory?'需本任务独占PG18工具，不连接共享数据库':false,timeout:30000
},async()=>{
  const lab=await createPrivatePostgres({binDirectory,libraryDirectory});let probe;
  try{
    probe=createRecordingProbeStore({connection:lab.connection,poolSize:8,connectionTimeoutMs:3000,statementTimeoutMs:10000});
    assert.equal(probe.pool.options.max,1);assert.equal(probe.pool.options.connectionTimeoutMillis,1000);assert.equal(probe.pool.options.statement_timeout,1000);assert.equal(probe.pool.options.query_timeout,1000);
    assert.equal((await probe.get('SELECT 1 n')).n,1);
    await assert.rejects(probe.get('SELECT pg_sleep(3)'),error=>error.code==='57014'||error.message==='Query read timeout');
    assert.equal((await probe.get('SELECT 2 n')).n,2);
  }finally{try{await probe?.close();}finally{await lab.close();}}
});
