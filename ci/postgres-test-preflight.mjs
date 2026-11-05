import {statfs} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';

// A private 100k-request planning diagnostic occupied about 1.06 GiB including
// its cluster. Two GiB is an operational floor with room for temporary/WAL
// growth, not a proof that all PostgreSQL workloads fit in that amount.
const defaultMinimum=2n*1024n**3n;
export async function postgresTestStoragePreflight({env=process.env,workspace=process.cwd(),temporary=tmpdir(),inspect=statfs}={}){
  const configured=env.HARBOR_TEST_PG_MIN_FREE_BYTES;
  if(configured!==undefined&&(typeof configured!=='string'||!/^[1-9][0-9]{0,18}$/.test(configured)))throw Object.assign(new Error('CI PostgreSQL storage budget is invalid'),{code:'ci_pg_storage_configuration'});
  const minimum=configured===undefined?defaultMinimum:BigInt(configured),filesystems=[];
  for(const [role,path] of [['workspace',workspace],['temporary',temporary]]){
    const info=await inspect(path,{bigint:true});
    if(typeof info.bavail!=='bigint'||typeof info.bsize!=='bigint'||info.bavail<0n||info.bsize<=0n)throw Object.assign(new Error('CI filesystem observation is unavailable'),{code:'ci_pg_storage_observation'});
    const available=info.bavail*info.bsize;
    filesystems.push({role,available_bytes:available.toString(),passed:available>=minimum});
  }
  return {schema:'harbor-ci-pg-storage-preflight-v1',scope:'test-job-filesystems-only',minimum_free_bytes:minimum.toString(),passed:filesystems.every(value=>value.passed),filesystems,postgres_service_storage_proven:false};
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  try{
    const result=await postgresTestStoragePreflight();console.log(JSON.stringify(result));
    if(!result.passed){console.error('CI可用磁盘低于测试预算，已阻止运行大数据测试；未清理或重试共享资源。');process.exitCode=1;}
  }catch(error){console.error(JSON.stringify({code:['ci_pg_storage_configuration','ci_pg_storage_observation'].includes(error?.code)?error.code:'ci_pg_storage_unavailable'}));process.exitCode=1;}
}
