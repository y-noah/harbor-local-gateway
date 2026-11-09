import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,rm,access} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {transportContext,releaseProducer,runCommand,stageOutput,transportReleaseImage} from '../ci/transport-release-image.mjs';
const commit='a'.repeat(40),hash='sha256:'+'b'.repeat(64),repository='registry.test/harbor/harbor-local-gateway';
const env={CI:'true',HARBOR_IMAGE_STAGE:'1',CI_JOB_NAME:'prepare_gateway_image_testing',CI_PROJECT_ID:'12',CI_PIPELINE_ID:'34',CI_JOB_ID:'78',CI_COMMIT_SHA:commit,HARBOR_GATEWAY_PRODUCER_JOB_ID:'56',HARBOR_REGISTRY_HOST:'registry.test',HARBOR_PROJECT:'harbor',CI_PROJECT_NAME:'harbor-local-gateway',GATEWAY_DEPLOY_HOST:'gateway.invalid',GATEWAY_REGISTRY_USERNAME:'synthetic-robot',GATEWAY_REGISTRY_PASSWORD:'synthetic-registry-secret'};
function receipt(request,phase){const {action,...identity}=request,binding=Object.fromEntries(['image','role','manifestDigest','configDigest','parentDigest'].sort().map(key=>[key,request[key]]));return {contract:'harbor-image-stage-v1',stageId:createHash('sha256').update(JSON.stringify(binding)).digest('hex'),identity,phase,ciVerified:false,deploymentAllowed:false,businessContainersTouched:false,...(phase==='complete'?{loadCompleted:true,loadClientExitCode:0,nativeImageImported:true,allConfigAndLayersVerified:true}:{})};}
const request={action:'prepare-image',image:repository+':'+commit,role:'gateway',manifestDigest:hash,configDigest:'sha256:'+'c'.repeat(64),parentDigest:'none',archiveSha256:'d'.repeat(64),archiveBytes:10240,producer:{project:'12',pipeline:'34',job:'56'}};
async function fixture(t){const directory=await mkdtemp(join(tmpdir(),'harbor-image-stage-test-'));t.after(()=>rm(directory,{recursive:true,force:true}));const root=join(directory,'.ci-image-digests/34');await mkdir(root,{recursive:true});await writeFile(join(root,'gateway-56.manifest'),`schema=1\nproject=12\npipeline=34\ncommit=${commit}\nrole=gateway\nrepository=${repository}\nproducer=56\ndigest=${hash}\nparent=none\n`);return directory;}

test('transport context rejects foreign jobs, injected hosts, disabled modes and unknown roles before any process',()=>{
 const context=transportContext(env);assert.equal(context.repository,repository);assert.equal(context.role,'gateway');
 for(const change of [{CI:'false'},{HARBOR_IMAGE_STAGE:'true'},{CI_JOB_NAME:'other'},{HARBOR_IMAGE_STAGE_ROLE:'other'},{GATEWAY_DEPLOY_HOST:'-oProxyCommand=evil'},{GATEWAY_DEPLOY_HOST:'host\ncommand'},{CI_COMMIT_SHA:'latest'},{HARBOR_PROJECT:'../other'}])assert.throws(()=>transportContext({...env,...change}),/harbor_image_stage_/);
});
test('producer uses the exact current source and infrastructure remains bound to its business parent',async t=>{
 const directory=await fixture(t),context=transportContext(env);assert.equal((await releaseProducer(context,directory)).digest,hash);
 const file=join(directory,'.ci-image-digests/34/gateway-56.manifest');await writeFile(file,(await readFile(file,'utf8')).replace('pipeline=34','pipeline=99'));await assert.rejects(releaseProducer(context,directory));
});
test('stage receipt requires exact image/nonce, real load completion and no release authority',()=>{
 assert.equal(stageOutput(JSON.stringify({image_staging:receipt(request,'complete')}),request).phase,'complete');
 for(const change of [{loadCompleted:false},{loadClientExitCode:1},{allConfigAndLayersVerified:false},{ciVerified:true},{deploymentAllowed:true},{businessContainersTouched:true},{stageId:'0'.repeat(64)},{identity:{...request,image:'other'}}])assert.throws(()=>stageOutput(JSON.stringify({image_staging:{...receipt(request,'complete'),...change}}),request),/harbor_image_stage_/);
});

test('CI observes one detached import after disconnect and never uploads or loads it a second time',async t=>{
 const directory=await fixture(t),commands=[],observations=[];let saved,uploads=0,queries=0,temporary;
 const run=async(command,args,options={})=>{
  commands.push({command,args});
  if(command==='skopeo'){
   if(args[0]==='login'){assert.equal(options.input,env.GATEWAY_REGISTRY_PASSWORD+'\n');assert.equal(args.includes(env.GATEWAY_REGISTRY_PASSWORD),false);temporary=args[args.indexOf('--authfile')+1].replace(/\/registry.json$/,'');}
   else{assert.ok(args.includes('--preserve-digests'));assert.ok(args.includes('docker://'+repository+'@'+hash));}
   return '';
  }
  if(command==='python3'){
   if(args[1]==='delta'){await writeFile(args[3],Buffer.alloc(10240));return JSON.stringify({transportBytes:10240,transportSha256:request.archiveSha256});}
   await writeFile(args[3],Buffer.alloc(request.archiveBytes));saved={...request};return JSON.stringify(saved);
  }
  assert.equal(command,'ssh');assert.ok(args.includes('StrictHostKeyChecking=yes'));assert.equal(args.some(value=>value.includes(env.GATEWAY_REGISTRY_PASSWORD)),false);
  if(typeof options.input==='string'){
   const query=JSON.parse(options.input);assert.equal(query.action,'image-stage-status');queries++;
   return JSON.stringify({image_staging:receipt(saved,uploads===0?'absent':queries===2?'importing':'complete')});
  }
  const chunks=[];for await(const block of options.input)chunks.push(Buffer.from(block));const wire=Buffer.concat(chunks),index=wire.indexOf(10);const sent=JSON.parse(wire.subarray(0,index));assert.equal(wire.length-index-1,sent.transportBytes);if(sent.action==='image-stage-plan')return JSON.stringify({image_staging:{...receipt(saved,'absent'),missingBlobs:[hash],reuseBytes:0}});assert.equal(sent.action,'prepare-image');for(const key of Object.keys(saved))assert.deepEqual(sent[key],saved[key]);uploads++;
  throw Error('synthetic observer disconnected after accepted upload');
 };
 const result=await transportReleaseImage({env,directory,run,observe:value=>observations.push(value),wait:async()=>{}});
 assert.equal(result.phase,'complete');assert.equal(uploads,1);assert.equal(queries,3);assert.equal(commands.some(row=>row.command==='docker'),false);assert.equal(commands.some(row=>row.args.some(value=>String(value).includes('/deploy-entry'))),false);
 assert.equal(observations.at(-1).deploymentAllowed,false);await assert.rejects(access(temporary));
 const output=JSON.parse(await readFile(join(directory,'.ci-image-stage/34/gateway-78.json'),'utf8'));assert.equal(output.phase,'complete');assert.equal(JSON.stringify(output).includes(env.GATEWAY_REGISTRY_PASSWORD),false);
});

test('real child cancellation waits for close and preserves bounded output errors',async()=>{
 assert.equal(await runCommand(process.execPath,['-e','process.stdout.write("closed")']), 'closed');
 await assert.rejects(runCommand(process.execPath,['-e','process.stdout.write("x".repeat(20000));setInterval(()=>{},1000)'],{maxBytes:1000,timeoutMs:3000}),/harbor_image_stage_command_failed/);
 const abort=new AbortController();abort.abort();await assert.rejects(runCommand(process.execPath,['-e','process.exit(99)'],{signal:abort.signal}));
});

test('new CI stage remains opt-in, waits actual candidates, and does not replace the installed release consumer',async()=>{
 const ci=await readFile(new URL('../.gitlab-ci.yml',import.meta.url),'utf8'),gateway=ci.split('\nprepare_gateway_image_testing:')[1].split('\nprepare_postgres_tools_image_testing:')[0],infra=ci.split('\nprepare_postgres_tools_image_testing:')[1];
 assert.match(gateway,/HARBOR_IMAGE_STAGE == "1"/);assert.match(gateway,/when: manual/);assert.match(gateway,/job: verify_postgres_candidate/);assert.match(gateway,/resource_group: harbor-gateway-93/);assert.match(gateway,/interruptible: false/);assert.match(gateway,/node ci\/transport-release-image\.mjs/);assert.doesNotMatch(gateway,/storage-backup|install-tools|deploy-entry-installed/);
 assert.match(infra,/job: verify_postgres_infrastructure_smoke/);assert.match(infra,/HARBOR_POSTGRES_INFRA_SMOKE/);
 assert.match(ci.split('\ndeploy_gateway_testing:')[1].split('\n# Static UI')[0],/HARBOR_IMAGE_STAGE == "1"'\n      when: never/);
 assert.match(ci.split('\ndeploy_gateway_entry_installed_testing:')[1].split('\n# Only stages')[0],/bash ci\/deploy-entry-installed\.sh/);
});
