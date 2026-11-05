import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {join} from 'node:path';
import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {fixture} from './device-fixture.mjs';
import {compileCapacityWindowsHelper} from '../tools/capacity/device-adapter.mjs';
import {createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
import {observeCapacityWindowsHelper} from '../tools/capacity/windows-helper-observer.mjs';
import {createWindowsDeviceBridge} from '../clients/harbor-device/wsl-bridge.mjs';
import {classifyStageContent} from '../streaming-stage-observer.mjs';
const exec=promisify(execFile),powershell='/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe',quote=value=>"'"+value.replaceAll("'","''")+"'";
const ps=async code=>(await exec(powershell,['-NoProfile','-NonInteractive','-EncodedCommand',Buffer.from(code,'utf16le').toString('base64')],{timeout:15000,maxBuffer:65536})).stdout.trim();
const path=async value=>(await exec('wslpath',['-w',value])).stdout.trim();
async function invoke(binary,args,input=''){return new Promise((resolve,reject)=>{const child=execFile(binary,args,{timeout:30000,maxBuffer:65536},(error,stdout)=>error?reject(Error('synthetic_windows_helper_command_failed')):resolve(stdout));child.stdin.end(input);});}
async function until(check,timeoutMs=15000){const deadline=performance.now()+timeoutMs;for(;;){if(await check())return;assert.ok(performance.now()<deadline,'Windows delivery condition timed out');await delay(10);}}
const enabled=process.env.HARBOR_TEST_WINDOWS_DEVICE==='1'&&(process.env.HARBOR_TEST_PG_SOCKET||process.env.HARBOR_TEST_DATABASE_URL);
test('actual CNG helper TCP flush observes authenticated model content through the WSL bridge',{skip:enabled?false:'需显式Windows与隔离PG',timeout:90000},async t=>fixture(t,async({root,devices,first,origin,store})=>{
 const runtime=createOwnedRuntime(),binary=await compileCapacityWindowsHelper(root),windowsRoot=await ps('[IO.Path]::Combine([IO.Path]::GetTempPath(),'+quote('HarborCapacity-Delivery-'+randomUUID())+')'),state=windowsRoot+'\\state.json',trace=randomUUID().toUpperCase();let prepared=false,bridge,serving,observation;
 try{
  const invitation=await devices.invite({member_id:first.memberId}),port=await ps('$s=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0);try{$s.Start();$s.LocalEndpoint.Port}finally{$s.Stop()}');prepared=true;
  assert.equal(JSON.parse(await invoke(binary,['prepare','--state',state,'--gateway',origin,'--ca-file',await path(join(root,'ca.pem')),'--port',port,'--allow-software-key','--name','合成交付测点设备'],invitation.token+'\n')).state,'pending');
  const device=await store.get('SELECT id,version,hardware_state FROM harbor.employee_devices WHERE member_id=$1',first.memberId);await devices.approve(device.id,{expected_version:device.version,credentials_only:device.hardware_state==='unavailable'});assert.equal(JSON.parse(await invoke(binary,['complete','--state',state])).state,'approved');
  const diagnosed=(await invoke(binary,['diagnose','--state',state,'--capacity-observe-delivery'])).trim().split(/\r?\n/).map(JSON.parse);assert.ok(diagnosed.some(row=>row.state==='ready'));assert.ok(diagnosed.some(row=>row.capacity_delivery&&row.type==='status'&&row.fault===false));
  const rows=[];let running=false;serving=runtime.start(binary,['serve','--state',state,'--capacity-observe-delivery','--capacity-control-stdin'],{role:'helper',stdio:['pipe','pipe','pipe']});serving.stderr.resume();
  observation=observeCapacityWindowsHelper({runtime,child:serving,onEvent:row=>{assert.ok(rows.length<128);rows.push(row);}});
  serving.stdout.on('data',chunk=>{if(chunk.toString().includes('"state":"running"'))running=true;});
  await until(()=>running||serving.exitCode!==null);assert.equal(running,true);const token=await invoke(binary,['local-token','--state',state]);bridge=createWindowsDeviceBridge({helper:binary,stateFile:state,localToken:token});const address=await bridge.listen();
  const verified=await fetch('http://127.0.0.1:'+address.port+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+first.secret,'x-harbor-local-token':token,'content-type':'application/json'},body:JSON.stringify({model:'gpt-6.1-sol',stream:true,input:'合成设备首次受控调用验证'})});assert.equal(verified.status,200);await verified.text();await devices.policy(first.memberId,{mode:'enforced',expected_version:0});
  await observation.sample();const sent=process.hrtime.bigint();
  const response=await fetch('http://127.0.0.1:'+address.port+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+first.secret,'x-harbor-local-token':token,'x-capacity-trace':trace,'content-type':'application/json'},body:JSON.stringify({model:'gpt-6.1-sol',stream:true,input:'合成Windows交付测点'})});assert.equal(response.status,200);assert.match(await response.text(),/response.completed/);await until(()=>rows.some(row=>row.type==='end'&&row.trace===trace));const received=process.hrtime.bigint();
  const content=rows.filter(row=>row.type==='content'&&row.trace===trace);assert.equal(content.length,1);const identity=classifyStageContent({type:'response.output_text.delta',delta:'合成设备输出'});assert.deepEqual(Object.fromEntries(['contentKind','hash','bytes'].map(key=>[key,content[0][key]])),identity);const end=rows.find(row=>row.type==='end'&&row.trace===trace);assert.equal(end.complete,true);assert.ok(BigInt(end.atTicks)>=BigInt(content[0].atTicks));assert.ok(BigInt(content[0].frequency)>0n);assert.equal(content[0].formalQualified,false);assert.equal(observation.status().fault,null);assert.ok(rows.every(row=>!JSON.stringify(row).includes('合成设备输出')));
  assert.equal(observation.bounds(content[0]).known,false);await observation.sample();const bounds=observation.bounds(content[0]);assert.equal(bounds.known,true);assert.ok(BigInt(bounds.lowerNs)<=sent);assert.ok(BigInt(bounds.upperNs)>=received);assert.equal(bounds.formalQualified,false);assert.throws(()=>observation.bounds({...content[0]}),/event_not_owned/);
  assert.equal((await store.get('SELECT device_id FROM harbor.requests WHERE member_id=$1 ORDER BY started DESC,id DESC LIMIT 1',first.memberId)).device_id,device.id);
  await observation.stop();assert.equal(serving.exitCode,0);assert.equal(observation.status().terminal,true);assert.equal(observation.status().fault,null);assert.equal(observation.bounds(content[0]).known,false);
 }finally{
  await bridge?.close({force:true});await runtime.close();await observation?.close();let removed=true;if(prepared&&await ps('Test-Path -LiteralPath '+quote(state))==='True')try{await invoke(binary,['remove','--state',state]);}catch{removed=false;}
  if(!removed)throw Error('windows_delivery_identity_cleanup_incomplete');await ps('if(Test-Path -LiteralPath '+quote(windowsRoot)+'){Remove-Item -LiteralPath '+quote(windowsRoot)+' -Recurse -Force}');
 }
}));
