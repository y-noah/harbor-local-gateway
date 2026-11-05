import {readFile,writeFile,lstat,readdir} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {parseDigestManifest} from './postgres-candidate-pipeline.mjs';
const fail=()=>{throw Error('harbor_infrastructure_digest_invalid');};
export function infrastructureContext(env){
 for(const name of ['CI_PROJECT_ID','CI_PIPELINE_ID','HARBOR_GATEWAY_PRODUCER_JOB_ID','HARBOR_POSTGRES_INFRASTRUCTURE_PRODUCER_JOB_ID'])if(!/^[1-9]\d*$/.test(env[name]??''))fail();
 if(!/^[a-f0-9]{40}$/.test(env.CI_COMMIT_SHA??'')||!/^[a-z0-9.:-]+$/.test(env.HARBOR_REGISTRY_HOST??'')||!['HARBOR_PROJECT','CI_PROJECT_NAME'].every(name=>/^[a-z0-9._-]+$/.test(env[name]??'')))fail();
 return {project:env.CI_PROJECT_ID,pipeline:env.CI_PIPELINE_ID,commit:env.CI_COMMIT_SHA,repository:`${env.HARBOR_REGISTRY_HOST}/${env.HARBOR_PROJECT}/${env.CI_PROJECT_NAME}`,gatewayProducer:env.HARBOR_GATEWAY_PRODUCER_JOB_ID,infrastructureProducer:env.HARBOR_POSTGRES_INFRASTRUCTURE_PRODUCER_JOB_ID};
}
export function parseInfrastructureDigest(text,context){
 const lines=text.split('\n'),repository=context.repository+'-pg-infra';if(lines.length!==10||lines.pop()!=='')fail();
 const prefix=[['schema','1'],['project',context.project],['pipeline',context.pipeline],['commit',context.commit],['role','postgres-infrastructure'],['repository',repository],['producer',context.infrastructureProducer]];
 if(prefix.some(([key,value],index)=>lines[index]!==key+'='+value)||!/^digest=sha256:[a-f0-9]{64}$/.test(lines[7])||!/^parent=sha256:[a-f0-9]{64}$/.test(lines[8]))fail();return {repository,digest:lines[7].slice(7),parent:lines[8].slice(7)};
}
export function infrastructureChild(context,business,infrastructure){
 if(business.parent!=='none'||infrastructure.parent!==business.digest)fail();
 return {workflow:{rules:[{if:'$CI_PIPELINE_SOURCE == "parent_pipeline"'},{when:'never'}]},stages:['verify'],default:{tags:['aistrong'],interruptible:true},smoke_postgres_infrastructure:{stage:'verify',timeout:'10 minutes',image:{name:business.repository+'@'+business.digest,entrypoint:[''],docker:{user:'0:0'}},services:[{name:infrastructure.repository+'@'+infrastructure.digest,alias:'harbor-infra-pg',entrypoint:['/usr/local/bin/harbor-postgres-infra-smoke']}],variables:{DOCKER_AUTH_CONFIG:'{"auths":{"${HARBOR_REGISTRY_HOST}":{"username":"${GATEWAY_REGISTRY_USERNAME}","password":"${GATEWAY_REGISTRY_PASSWORD}"}}}',CI:'true',HARBOR_INFRA_SMOKE:'1',POSTGRES_USER:'postgres',POSTGRES_DB:'harbor_infra_smoke',POSTGRES_PASSWORD:'harbor-infra-smoke-only',POSTGRES_INITDB_ARGS:'--auth-local=peer --auth-host=scram-sha-256 --no-locale --encoding=UTF8',PGDATA:'/opt/product-development/harbor-local-gateway/postgres-infra/db/18/docker'},script:['node /app/ci/postgres-infra-smoke.mjs']}};
}
export async function generateInfrastructurePipeline({env=process.env,directory=process.cwd()}={}){
 const context=infrastructureContext(env),root=join(directory,'.ci-image-digests',context.pipeline),names=await readdir(root),rows=[];
 for(const [role,producer] of [['gateway',context.gatewayProducer],['postgres-infrastructure',context.infrastructureProducer]]){
  if(names.filter(name=>name.startsWith(role+'-')&&name.endsWith('.manifest')).length!==1)fail();const file=join(root,role+'-'+producer+'.manifest'),info=await lstat(file);if(!info.isFile()||info.isSymbolicLink()||info.size>4096)fail();const text=await readFile(file,'utf8');rows.push(role==='gateway'?parseDigestManifest(text,context,role):parseInfrastructureDigest(text,context));
 }
 const output=join(directory,'postgres-infrastructure-child.yml');await writeFile(output,JSON.stringify(infrastructureChild(context,...rows),null,2)+'\n',{flag:'wx',mode:0o600});return output;
}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href)generateInfrastructurePipeline().catch(()=>{process.stderr.write('harbor_infrastructure_digest_invalid\n');process.exitCode=1;});
