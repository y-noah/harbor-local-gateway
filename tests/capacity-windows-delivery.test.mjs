import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,writeFile,rm,chmod} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {classifyStageContent} from '../streaming-stage-observer.mjs';
const exec=promisify(execFile),compiler='/mnt/c/Windows/Microsoft.NET/Framework64/v4.0.30319/csc.exe';
const options={skip:process.env.HARBOR_TEST_WINDOWS_DEVICE!=='1'?'需实际Windows .NET工具链':false,timeout:60000};
const harness=String.raw`using System;using System.IO;using System.Text;using System.Diagnostics;using System.Threading;using System.Collections.Generic;using System.Web.Script.Serialization;
class DeliveryFixture {
 static int OutputFailures;
 sealed class Throwing:TextWriter {readonly TextWriter target;readonly bool once;public Throwing(TextWriter value,bool one){target=value;once=one;}public override Encoding Encoding{get{return Encoding.UTF8;}}public override void WriteLine(string value){if(!once||OutputFailures==0){Interlocked.Increment(ref OutputFailures);throw new IOException("synthetic output failure");}target.WriteLine(value);}}
 sealed class Blocked:TextWriter {public override Encoding Encoding{get{return Encoding.UTF8;}}public override void WriteLine(string value){new ManualResetEvent(false).WaitOne();}}
 static int Main(string[] args){var input=new JavaScriptSerializer{MaxJsonLength=8388608}.DeserializeObject(Console.ReadLine()) as Dictionary<string,object>;if(args[0]=="blocked")Console.SetOut(new Blocked());if(args[0]=="throw"||args[0]=="throw-once")Console.SetOut(new Throwing(Console.Out,args[0]=="throw-once"));CapacityDeliveryObservation.Configure(args[0]!="off");var probe=CapacityDeliveryObservation.Begin(new Dictionary<string,string>(StringComparer.OrdinalIgnoreCase){{"X-Capacity-Trace",input.ContainsKey("trace")?(string)input["trace"]:"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}},input.ContainsKey("contentType")?(string)input["contentType"]:"text/event-stream");if(probe!=null){foreach(string chunk in (object[])input["chunks"]){var bytes=Convert.FromBase64String(chunk);probe.Written(bytes,bytes.Length,Stopwatch.GetTimestamp());}probe.End(Stopwatch.GetTimestamp(),true);}HarborDevice.Main(args[0]=="off"?new string[]{"invalid"}:new string[]{"invalid","--capacity-observe-delivery"});Console.Error.WriteLine(new JavaScriptSerializer().Serialize(new{probe=CapacityDeliveryObservation.Status(),writerFailures=OutputFailures}));return 0;}
}`;
async function fixture(t){
 const root=await mkdtemp(join(tmpdir(),'harbor-windows-delivery-'));t.after(()=>rm(root,{recursive:true,force:true}));const source=join(root,'Fixture.cs'),binary=join(root,'Fixture.exe'),path=async value=>(await exec('wslpath',['-w',value])).stdout.trim();await writeFile(source,harness);
 await exec(compiler,['/nologo','/define:HARBOR_CAPACITY_OBSERVATION','/target:exe','/main:DeliveryFixture','/out:'+await path(binary),'/r:System.Web.Extensions.dll','/r:System.Net.Http.dll','/r:System.Management.dll',await path(source),await path(fileURLToPath(new URL('../clients/harbor-device/HarborDevice.cs',import.meta.url))),await path(fileURLToPath(new URL('../clients/harbor-device/CapacityDeliveryObservation.cs',import.meta.url)))],{timeout:30000,maxBuffer:65536});
 await chmod(binary,0o700);
 return {async run(bytes,{chunk=65536,mode='on',repeat=1,trace,contentType}={}){const chunks=[];for(let index=0;index<repeat;index++)for(let at=0;at<bytes.length;at+=chunk)chunks.push(bytes.subarray(at,at+chunk).toString('base64'));const input=JSON.stringify({chunks,trace,contentType})+'\n';assert.ok(input.length<8388608);return new Promise((resolve,reject)=>{const child=execFile(binary,[mode],{timeout:15000,maxBuffer:1048576},(error,stdout,stderr)=>error?reject(error):resolve({raw:stdout,stderr,rows:stdout.trim()?stdout.trim().split(/\r?\n/).map(JSON.parse):[]}));child.stdin.end(input);});}};
}
test('actual Windows QPC observer matches content identity across UTF8 and SSE boundaries without exposing content',options,async t=>{
 const f=await fixture(t),events=[{type:'response.output_text.delta',item_id:'i',output_index:0,delta:'中文😀\n代码'},{type:'response.function_call_arguments.delta',item_id:'tool',output_index:1,delta:'{"synthetic_private":"hidden"}'},{type:'response.reasoning_text.delta',delta:'unrecorded reasoning'},{type:'response.completed',response:{status:'completed',output:[{type:'message',content:[{text:'完成'}]},{type:'function_call',name:'synthetic_tool',arguments:'{}'}]}}],bytes=Buffer.from(': heartbeat\r\n\r\n'+events.map((event,index)=>(index===0?'event: '+event.type+'\r\n':'')+'data: '+JSON.stringify(index===0?{...event,type:undefined}:event)+'\r\n\r\n').join('')+'data: [DONE]\r\n\r\n');
 for(const chunk of [1,17,65536]){const result=await f.run(bytes,{chunk}),content=result.rows.filter(row=>row.type==='content');assert.deepEqual(content.map(({contentKind,hash,bytes})=>({contentKind,hash,bytes})),events.map(event=>classifyStageContent(event)).filter(Boolean));assert.deepEqual(content.map(row=>row.ordinal),[0,1,2]);assert.ok(content.every(row=>/^[0-9]+$/.test(row.atTicks)&&BigInt(row.frequency)>0n&&row.formalQualified===false));assert.equal(result.rows.find(row=>row.type==='end').complete,true);assert.equal(result.rows.at(-1).type,'status');assert.equal(result.rows.at(-1).fault,false);for(const text of ['中文','synthetic_private','hidden','unrecorded reasoning'])assert.ok(!result.raw.includes(text));}
 assert.equal((await f.run(bytes,{mode:'off'})).rows.length,0);for(const invalid of [{trace:'synthetic-secret-do-not-log'},{contentType:'text/event-stream-other'}]){const result=await f.run(bytes,invalid);assert.equal(result.rows.some(row=>row.type==='content'),false);assert.ok(!result.raw.includes('synthetic-secret-do-not-log'));}
});
test('malformed, truncated and oversized SSE produce missing-source evidence instead of content',options,async t=>{
 const f=await fixture(t);for(const bytes of [Buffer.from('data: not-json\n\n'),Buffer.from([0xff,0xfe]),Buffer.from('data: {"type":"response.output_text.delta","delta":"unfinished'),Buffer.from('data: '+'x'.repeat(1048577))]){const result=await f.run(bytes);assert.equal(result.rows.some(row=>row.type==='content'),false);assert.equal(result.rows.find(row=>row.type==='end').complete,false);assert.ok(result.rows.some(row=>row.type==='unavailable'));}
});
test('full 64KiB input following a split four-byte UTF8 character retains every decoded character',options,async t=>{
 const f=await fixture(t),prefix='data: {"type":"response.output_text.delta","delta":"',delta='a'.repeat(65536-Buffer.byteLength(prefix)-3)+'😀'+'b'.repeat(65535),bytes=Buffer.from(prefix+delta+'"}\n\n');
 assert.equal(bytes[65535],0x98);assert.equal(bytes[65536],0x80);const result=await f.run(bytes,{chunk:65536}),row=result.rows.find(row=>row.type==='content');assert.ok(row);assert.deepEqual(Object.fromEntries(['contentKind','hash','bytes'].map(key=>[key,row[key]])),classifyStageContent({type:'response.output_text.delta',delta}));assert.equal(result.rows.find(row=>row.type==='end').complete,true);
});
test('mixed LF and CRLF SSE separators match the Node frame boundary across arbitrary chunks',options,async t=>{
 const f=await fixture(t),event={type:'response.output_text.delta',delta:'mixed line boundary'};
 for(const ending of ['\n\n','\r\n\r\n','\n\r\n','\r\n\n'])for(const chunk of [1,31,65536]){const result=await f.run(Buffer.from('data: '+JSON.stringify(event)+ending),{chunk}),rows=result.rows.filter(row=>row.type==='content');assert.equal(rows.length,1);assert.equal(rows[0].hash,classifyStageContent(event).hash);assert.equal(result.rows.find(row=>row.type==='end').complete,true);}
});
test('a stream BOM is removed exactly once across chunks while a BOM inside content remains hashed',options,async t=>{
 const f=await fixture(t),event={type:'response.output_text.delta',delta:'kept\ufeffcontent'},bytes=Buffer.from('\ufeffdata: '+JSON.stringify(event)+'\n\n');
 for(const chunk of [1,2,3,17,65536]){const result=await f.run(bytes,{chunk}),rows=result.rows.filter(row=>row.type==='content');assert.equal(rows.length,1);assert.equal(rows[0].ordinal,0);assert.equal(rows[0].hash,classifyStageContent(event).hash);assert.equal(rows[0].bytes,Buffer.byteLength(event.delta));assert.equal(result.rows.find(row=>row.type==='end').complete,true);}
});
test('blocked diagnostic stdout cannot block the observed transfer thread and its pending output stays bounded',options,async t=>{
 const f=await fixture(t),bytes=Buffer.from('data: {"type":"response.output_text.delta","delta":"synthetic"}\n\n'),began=performance.now();const blocked=await f.run(bytes,{mode:'blocked',repeat:3000});assert.equal(blocked.raw,'');assert.ok(performance.now()-began<10000);const status=JSON.parse(blocked.stderr.trim().split(/\r?\n/).at(-1)).probe;assert.ok(status.dropped>0);assert.equal(status.fault,true);assert.ok(status.queued<=status.maxPending);assert.equal(status.formalQualified,false);const result=await f.run(bytes,{repeat:100});assert.ok(result.rows.filter(row=>row.type==='content').length<=100);assert.equal(result.rows.at(-1).type,'status');if(result.rows.at(-1).dropped>0)assert.equal(result.rows.at(-1).fault,true);
});
test('an actual output IOException accounts for failed and pending records and closes without blocking the producer',options,async t=>{
 const f=await fixture(t),bytes=Buffer.from('data: {"type":"response.output_text.delta","delta":"synthetic"}\n\n');
 for(const mode of ['throw','throw-once']){const began=performance.now(),result=await f.run(bytes,{mode,repeat:50}),status=JSON.parse(result.stderr.trim().split(/\r?\n/).at(-1));assert.ok(status.writerFailures>0);assert.equal(status.probe.fault,true);assert.ok(status.probe.dropped>=51);assert.equal(status.probe.queued,0);assert.ok(performance.now()-began<10000);if(mode==='throw-once'){assert.equal(result.rows.at(-1).type,'status');assert.equal(result.rows.at(-1).fault,true);assert.equal(result.rows.at(-1).dropped,51);}else assert.equal(result.rows.length,0);}
});
