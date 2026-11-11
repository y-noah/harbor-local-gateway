import {dockerRunnerTag} from './postgres-runner-routing.mjs';
import {readFile,writeFile,lstat,readdir} from 'node:fs/promises';
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
const fail=()=>{throw Error('harbor_image_digest_invalid');};
const integer=value=>typeof value==='string'&&/^[1-9]\d*$/.test(value);
export function digestContext(env){
 for(const key of ['CI_PROJECT_ID','CI_PIPELINE_ID','HARBOR_GATEWAY_PRODUCER_JOB_ID','HARBOR_POSTGRES_VALIDATION_PRODUCER_JOB_ID'])if(!integer(env[key]))fail();
 if(!/^[a-f0-9]{40}$/.test(env.CI_COMMIT_SHA??'')||!/^[a-z0-9.:-]+$/.test(env.HARBOR_REGISTRY_HOST??'')||!['HARBOR_PROJECT','CI_PROJECT_NAME'].every(key=>/^[a-z0-9._-]+$/.test(env[key]??'')))fail();
 return {project:env.CI_PROJECT_ID,pipeline:env.CI_PIPELINE_ID,commit:env.CI_COMMIT_SHA,repository:`${env.HARBOR_REGISTRY_HOST}/${env.HARBOR_PROJECT}/${env.CI_PROJECT_NAME}`,gatewayProducer:env.HARBOR_GATEWAY_PRODUCER_JOB_ID,postgresProducer:env.HARBOR_POSTGRES_VALIDATION_PRODUCER_JOB_ID};
}
export function parseDigestManifest(text,context,role){
 const producer=role==='gateway'?context.gatewayProducer:context.postgresProducer,repository=context.repository+(role==='gateway'?'':'-pg-validation');
 if(!['gateway','postgres-validation'].includes(role))fail();
 const lines=text.split('\n');if(lines.length!==10||lines.pop()!=='')fail();
 const expected=[['schema','1'],['project',context.project],['pipeline',context.pipeline],['commit',context.commit],['role',role],['repository',repository],['producer',producer]];
 if(expected.some(([key,value],index)=>lines[index]!==`${key}=${value}`)||!/^digest=sha256:[a-f0-9]{64}$/.test(lines[7])||!/^parent=(none|sha256:[a-f0-9]{64})$/.test(lines[8]))fail();
 return {repository,digest:lines[7].slice(7),parent:lines[8].slice(7),producer};
}
export function candidateChildPipeline(context,gateway,postgres,runnerTag){
 if(gateway.parent!=='none'||postgres.parent!==gateway.digest)fail();
 return {workflow:{rules:[{if:'$CI_PIPELINE_SOURCE == "parent_pipeline"'},{when:'never'}]},stages:['verify'],default:{tags:[dockerRunnerTag(runnerTag)],interruptible:true},verify_postgres_candidate:{stage:'verify',image:{name:`${gateway.repository}@${gateway.digest}`,entrypoint:['']},services:[{name:`${postgres.repository}@${postgres.digest}`,alias:'harbor-pg'}],variables:{DOCKER_AUTH_CONFIG:'{"auths":{"${HARBOR_REGISTRY_HOST}":{"username":"${GATEWAY_REGISTRY_USERNAME}","password":"${GATEWAY_REGISTRY_PASSWORD}"}}}',CI:'true',HARBOR_PRIVATE_CANDIDATE_SERVICE:'1',HARBOR_CANDIDATE_TEST:'1',POSTGRES_DB:'harbor_ci',POSTGRES_USER:'harbor_ci',POSTGRES_PASSWORD:'harbor-ci-test-only',HARBOR_TEST_DATABASE_URL:'postgresql://harbor_ci:harbor-ci-test-only@harbor-pg:5432/harbor_ci'},script:['node /app/ci/postgres-candidate-check.mjs']}};
}
export async function generateCandidatePipeline({env=process.env,directory=process.cwd()}={}){
 const context=digestContext(env),root=join(directory,'.ci-image-digests',context.pipeline),names=await readdir(root),rows=[];
 for(const [role,producer] of [['gateway',context.gatewayProducer],['postgres-validation',context.postgresProducer]]){
  const name=`${role}-${producer}.manifest`;if(names.filter(value=>value.startsWith(role+'-')&&value.endsWith('.manifest')).length!==1)fail();
  const path=join(root,name),info=await lstat(path);if(!info.isFile()||info.isSymbolicLink()||info.size>4096)fail();rows.push(parseDigestManifest(await readFile(path,'utf8'),context,role));
 }
 const output=join(directory,'postgres-candidate-child.yml');await writeFile(output,JSON.stringify(candidateChildPipeline(context,...rows,env.HARBOR_DOCKER_RUNNER_TAG),null,2)+'\n',{flag:'wx',mode:0o600});return output;
}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href)generateCandidatePipeline().catch(()=>{process.stderr.write('harbor_image_digest_invalid\n');process.exitCode=1;});
