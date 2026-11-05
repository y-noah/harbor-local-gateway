import {mkdtemp,mkdir,writeFile,readFile,lstat,rm} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {pathToFileURL} from 'node:url';
import {checkServerIdentity} from 'node:tls';
import pg from 'pg';
import {PostgresStore} from '../postgres-store.mjs';
import {imageManifest} from './postgres-image-manifest.mjs';
import {createReportedRecordingStorageProbe,verifyRecordingStorageReport,recordingDatabaseIdentity} from '../postgres-storage-report.mjs';
const fail=code=>{throw Error('harbor_infrastructure_smoke_'+code);};
export function infrastructureSmokeEnvironment(env){
 if(env.CI!=='true'||env.HARBOR_INFRA_SMOKE!=='1'||env.CI_JOB_NAME!=='smoke_postgres_infrastructure'||! /^[a-f0-9]{40}$/.test(env.CI_COMMIT_SHA??'')||!['CI_PIPELINE_ID','CI_JOB_ID'].every(key=>/^[1-9]\d*$/.test(env[key]??''))||!env.CI_PROJECT_DIR?.startsWith('/')||env.CI_PROJECT_DIR==='/'||env.CI_PROJECT_DIR.split('/').includes('..')||env.POSTGRES_USER!=='postgres'||env.POSTGRES_DB!=='harbor_infra_smoke'||env.POSTGRES_PASSWORD!=='harbor-infra-smoke-only')fail('environment');
 return Object.freeze({share:join(env.CI_PROJECT_DIR,'.ci-postgres-infra-smoke',env.CI_JOB_ID),host:'harbor-infra-pg',database:'harbor_infra_smoke',user:'postgres',password:env.POSTGRES_PASSWORD,port:5432});
}
async function until(operation,predicate,timeoutMs=60000){const deadline=performance.now()+timeoutMs;for(;;){const value=await operation();if(predicate(value))return value;if(performance.now()>=deadline)fail('deadline');await new Promise(resolve=>setTimeout(resolve,100));}}
export async function runInfrastructureSmoke({env=process.env}={}){
 const fixture=infrastructureSmokeEnvironment(env),image=await imageManifest();if(image.revision!==env.CI_COMMIT_SHA)fail('business_revision');let store,directory,ownedShare=false;
 try{
  directory=await mkdtemp(join(tmpdir(),'harbor-infra-smoke-'));const parent=join(env.CI_PROJECT_DIR,'.ci-postgres-infra-smoke');try{await mkdir(parent,{mode:0o700});}catch(error){if(error.code!=='EEXIST')throw error;}const parentInfo=await lstat(parent);if(!parentInfo.isDirectory()||parentInfo.isSymbolicLink()||parentInfo.uid!==0||parentInfo.mode&0o077)fail('shared_directory');await mkdir(fixture.share,{recursive:false,mode:0o700});ownedShare=true;await writeFile(join(fixture.share,'request'),'public-ca-only\n',{flag:'wx',mode:0o600});
  const caFile=join(fixture.share,'ca.pem');await until(async()=>{try{const info=await lstat(caFile);return info.isFile()&&!info.isSymbolicLink()&&info.uid===0&&!(info.mode&0o022)&&info.size<=16384;}catch{return false;}},Boolean);const ca=await readFile(caFile);
  const connection={host:fixture.host,port:fixture.port,database:fixture.database,user:fixture.user,password:fixture.password,ssl:{ca,rejectUnauthorized:true},connectionTimeoutMillis:2000,query_timeout:2000};store=new PostgresStore({connection,poolSize:2,connectionTimeoutMs:2000,statementTimeoutMs:2000});store.pool.options.query_timeout=2000;
  await until(async()=>{try{return (await store.get('SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()')).ssl===true;}catch{return false;}},Boolean);
  const identity=await store.get("SELECT current_setting('server_version_num')::integer version,current_database() database,current_setting('data_directory') directory,current_setting('server_encoding') encoding,(SELECT datcollate FROM pg_database WHERE datname=current_database()) collation,(SELECT datctype FROM pg_database WHERE datname=current_database()) ctype");if(identity.version<180000||identity.version>=190000||identity.database!==fixture.database||identity.encoding!=='UTF8'||identity.collation!=='C'||identity.ctype!=='C'||identity.directory!=='/opt/product-development/harbor-local-gateway/postgres-infra/db/18/docker')fail('database_identity');
  const marker=JSON.parse((await store.get("SELECT pg_read_file('/run/harbor-infra-control/smoke.json',0,1024,true) marker")).marker);if(marker.schema!=='harbor-infra-smoke-v1'||marker.commit!==env.CI_COMMIT_SHA||marker.pipeline!==env.CI_PIPELINE_ID||marker.job!==env.CI_JOB_ID)fail('service_scope');
  if((await store.get("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='harbor') present")).present)fail('existing_schema');
  if((await store.get("SELECT EXISTS(SELECT 1 FROM pg_database d CROSS JOIN LATERAL aclexplode(COALESCE(d.datacl,acldefault('d',d.datdba))) a WHERE a.grantee=0 AND a.privilege_type IN ('CONNECT','TEMPORARY')) present")).present)fail('public_database_privilege');
  for(const [kind,options,code] of [['plaintext',{ssl:false},'28000'],['wrong-hostname',{ssl:{ca,rejectUnauthorized:true,checkServerIdentity:(_host,certificate)=>checkServerIdentity('wrong.synthetic.invalid',certificate)}},'ERR_TLS_CERT_ALTNAME_INVALID']]){
   const client=new pg.Client({...connection,...options});let rejected=false;try{await client.connect();}catch(error){rejected=error.code===code;}finally{await client.end().catch(()=>{});}if(!rejected)fail(kind+'_not_rejected');
  }
  await store.migrate();const keyFile=join(directory,'report.key'),data=join(directory,'app');await mkdir(data,{mode:0o700});await mkdir(join(data,'recording-spool'),{mode:0o700});const key=Buffer.from((await store.get("SELECT encode(pg_read_binary_file('/run/harbor-report.key'),'hex') value")).value,'hex');if(key.length!==32)fail('report_key');await writeFile(keyFile,key,{flag:'wx',mode:0o600});const probe=createReportedRecordingStorageProbe(store,{reportKeyFile:keyFile,appPaths:[data,join(data,'recording-spool')]});
  await until(async()=>{try{await probe();return probe.status().verified;}catch{return false;}},Boolean,30000);const first=(await store.get('SELECT sampled_at FROM harbor.recording_storage_reports')).sampled_at;await until(async()=> (await store.get('SELECT sampled_at FROM harbor.recording_storage_reports')).sampled_at,value=>value>first,15000);await probe();
  const row=await store.get('SELECT payload,signature FROM harbor.recording_storage_reports'),db=await recordingDatabaseIdentity(store);let tampered=false;try{verifyRecordingStorageReport({...row,signature:'0'.repeat(64)},key,db);}catch{tampered=true;}if(!tampered)fail('tampered_report');
  return Object.freeze({scope:'isolated-official-infrastructure-service',businessRevision:env.CI_COMMIT_SHA,pg18:true,tlsVerified:true,plaintextRejected:true,wrongHostnameRejected:true,actualDatabaseHostReport:true,reportRefreshVerified:true,tamperedReportRejected:true,productionMigrationVerified:false,deploymentAllowed:false});
 }finally{await store?.close();if(directory)await rm(directory,{recursive:true,force:true});if(ownedShare)await rm(fixture.share,{recursive:true,force:true});}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href)runInfrastructureSmoke().then(value=>console.log(JSON.stringify(value))).catch(()=>{process.stderr.write('Harbor基础设施镜像隔离验证未通过\n');process.exitCode=1;});
