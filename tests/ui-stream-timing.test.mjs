import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {createRequestStreamTiming} from '../request-stream-timing.mjs';
const source=readFileSync(new URL('../public/app.js',import.meta.url),'utf8'),start=source.indexOf('function requestStreamTimingDetail('),end=source.indexOf('const recordingStates=',start);
assert.ok(start>=0&&end>start);
const context=vm.createContext({fmt:value=>String(value),esc:value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),requestIdentity:()=>({member:'合成员工',account:'合成账号',model:'合成模型',key:'合成Key',device:'合成设备',deviceName:'合成电脑',system:'合成系统',generation:1,agent:'合成客户端'}),date:()=>'',referenceTotal:()=>'',referenceMoney:()=>'',referenceState:()=>'',businessOutcomeLabels:{},phaseLabels:{},state:{capabilities:{}},button:()=>''});
vm.runInContext(source.slice(start,end),context);
const render=timing=>{context.timing=timing;return vm.runInContext('requestStreamTimingDetail(timing)',context);};
function sample(){let now=0n;const timing=createRequestStreamTiming({clock:()=>now}),at=(ms,fn)=>{now=BigInt(ms)*1000000n;return fn();};at(1,()=>timing.bodyEnd());for(const [i,stage] of ['validated','sessionReady','admissionEnd','captureReady','credentialsStart','credentialsReady'].entries())at(i+2,()=>timing.preDispatch(stage));at(15,()=>timing.dispatch());at(20,()=>timing.headers());at(22,()=>timing.rawChunk(20));at(23,()=>timing.decodedEvent());const content=at(25,()=>timing.upstreamEvent({type:'response.output_text.delta',delta:'合成内容'})),write=at(28,()=>timing.writeStart({bytes:20,contentToken:content}));at(31,()=>timing.writeEnd(write));at(40,()=>timing.transportEnd({outcome:'ok'}));at(42,()=>timing.responseEnd());return at(43,()=>timing.finish({outcome:'ok'}));}
test('请求详情在原折叠诊断中展示真实流式分段，区分派发前处理、上游与socket回调',()=>{
 const timing=sample(),html=render(timing);assert.equal(timing.complete,true);for(const value of ['流式分段','时序完整','派发 → 上游首个可见内容','10.00 ms','3.00 ms','0.00 ms','写入回调不代表客户端收到','准入事务返回','派发前检查点'])assert.ok(html.includes(value),value);assert.doesNotMatch(html,/排队时间|客户端已收到|模型生成耗时/);
 context.row={id:'synthetic',started:1,ended:2,streamTiming:timing,timeline:[]};const detail=vm.runInContext('phaseDetail(row)',context);assert.match(detail,/<details class="request-diagnostics">[\s\S]*流式分段[\s\S]*<\/details>/);assert.doesNotMatch(detail,/<details[^>]*\bopen\b/);assert.equal((detail.match(/<h2>/g)||[]).length,1);
});
test('旧请求和未知版本不凭旧queued阶段推断流式数据',()=>{for(const timing of [null,undefined,{}, {...sample(),version:2},{...sample(),clock:'wall-clock'}])assert.equal(render(timing),'');});
test('缺失与无效耗时保持未记录，真实零值才显示零；不执行或显示任意原因字段',()=>{
 const timing=sample(),html=render({...timing,complete:false,durationsMs:{bodyRead:null,beforeDispatch:'0',upstreamHeaders:Infinity,upstreamFirstRawChunk:-1,firstRawToDecoded:0},counts:{rawChunks:null,rawBytes:-1,completedWrites:0,writes:0},missing:['missing_dispatch','<script>alert(1)</script>','missing_dispatch']});assert.match(html,/时序不完整/);assert.equal((html.match(/0\.00 ms/g)||[]).length,1);assert.match(html,/缺少派发/);assert.equal((html.match(/缺少派发/g)||[]).length,1);assert.doesNotMatch(html,/<script>|Infinity|NaN|alert\(1\)/);assert.match(html,/未记录/);assert.match(html,/写入完成 \/ 发起：0 \/ 0/);
});
test('派发前检查点只使用独立观测偏移，不将缺点补零或推断锁等待',()=>{
 const timing={...sample(),preDispatchOffsetsMs:{admissionEnd:8}},html=render(timing);assert.match(html,/准入事务返回<\/td><td>8\.00 ms/);assert.match(html,/会话准备结束<\/td><td>未记录/);assert.doesNotMatch(html,/锁等待|事务已提交/);
});
