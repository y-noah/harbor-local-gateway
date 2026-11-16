import test from 'node:test';
import assert from 'node:assert/strict';
import {recordingInputDiagnosticsConfiguration,createRecordingInputDiagnostics,recordingInputShape} from '../recording-input-diagnostics.mjs';
import {captureRequestContext,captureRequestContextAsync} from '../recording-request-context.mjs';

test('诊断默认关闭，服务端开关和资源配置严格校验，构造参数覆盖合法环境值',()=>{
 assert.equal(recordingInputDiagnosticsConfiguration().enabled,false);
 assert.equal(createRecordingInputDiagnostics().status(),undefined);
 assert.equal(createRecordingInputDiagnostics().beginRequest(),undefined);
 const env={HARBOR_RECORDING_INPUT_DIAGNOSTICS:'1',HARBOR_RECORDING_INPUT_DIAGNOSTICS_MAX_REQUESTS:'3'};
 assert.equal(recordingInputDiagnosticsConfiguration({env}).maxRequests,3);
 assert.equal(recordingInputDiagnosticsConfiguration({env,configuration:{enabled:false,maxRequests:2}}).enabled,false);
 assert.equal(recordingInputDiagnosticsConfiguration({env,configuration:{maxRequests:2}}).maxRequests,2);
 for(const configuration of [null,[],{enabled:'1'},{enabled:null},{maxRequests:0},{maxGroups:257},{maxItemsPerRequest:null},{ttlMs:3600001},{unknown:true}])assert.throws(()=>recordingInputDiagnosticsConfiguration({configuration}),{code:'recording_configuration'});
 for(const env of [{HARBOR_RECORDING_INPUT_DIAGNOSTICS:'true'},{HARBOR_RECORDING_INPUT_DIAGNOSTICS_TTL_MS:'1.1'},{HARBOR_RECORDING_INPUT_DIAGNOSTICS_MAX_GROUPS:'0'}])assert.throws(()=>recordingInputDiagnosticsConfiguration({env}),{code:'recording_configuration'});
});
test('未知输入在observer前仅构造固定DTO，原类型文字参数和任意字段不回显，原覆盖判据不变',async()=>{
 const secrets=['synthetic_sensitive_type','Bearer synthetic-secret','私密协议','x'.repeat(300)];
 for(const type of secrets){
  const payload={input:[{type,content:'synthetic user text',input:'synthetic arguments',api_key:'synthetic-key',arbitrary_private_field:'synthetic value'}]},events=[];
  const expected=captureRequestContext(payload),view=await captureRequestContextAsync(payload,{onUnsupportedInputShape:value=>{assert.equal(Object.isFrozen(value),true);events.push(value);}});
  assert.deepEqual(view,expected);assert.deepEqual(view.reasons,['unsupported_input']);assert.equal(events.length,1);assert.equal(events[0].has_content,true);assert.equal(events[0].has_input,true);
  const text=JSON.stringify(events);for(const value of [type,'synthetic user text','synthetic arguments','synthetic-key','arbitrary_private_field'])assert.ok(!text.includes(value));
 }
 const input={type:'custom_tool_call',call_id:'synthetic',name:'exec',input:'synthetic'};
 let observed=0;assert.equal(captureRequestContext({input:[input]},{onUnsupportedInputShape:()=>observed++}).state,'complete');assert.equal(observed,0);
});
test('同步异常及异步拒绝不改变unsupported_input、可采前缀或异步提取结果',async()=>{
 const payload={input:[{role:'user',content:'synthetic visible prefix'},{type:'synthetic_unknown'}]},expected=captureRequestContext(payload);
 for(const observer of [()=>{throw Error('synthetic');},()=>Promise.reject(Error('synthetic'))]){
  assert.deepEqual(captureRequestContext(payload,{onUnsupportedInputShape:observer}),expected);
  assert.deepEqual(await captureRequestContextAsync(payload,{onUnsupportedInputShape:observer}),expected);
 }
 await new Promise(resolve=>setImmediate(resolve));
});
test('agent_message已知明文上下文不再误报未知input，诊断DTO仍不读取内容',async()=>{
 const item={type:'agent_message',author:'synthetic author',recipient:'synthetic recipient',content:[{type:'input_text',text:'synthetic visible content'},{type:'encrypted_content',get encrypted_content(){throw Error('must not read opaque child');}}]};
 for(const capture of [captureRequestContext,captureRequestContextAsync]){
  const diagnostics=createRecordingInputDiagnostics({enabled:true}),view=await capture({input:[item]},{onUnsupportedInputShape:diagnostics.beginRequest()});
  assert.equal(view.state,'complete');assert.equal(view.opaqueReasoningExcluded,1);assert.match(JSON.stringify(view),/synthetic visible content/);assert.deepEqual(diagnostics.status().groups,[]);
 }
 const guarded={type:'agent_message',get content(){throw Error('must not read content');},get author(){throw Error('must not read identity');}};
 assert.equal(recordingInputShape(guarded,'input','array_item').type,'agent_message');
 assert.equal(recordingInputShape(guarded,'input','array_item').has_content,true);
});
test('请求数、每请求shape数、组合数分别封顶，组合只保留枚举计数',()=>{
 const d=createRecordingInputDiagnostics({enabled:true,maxRequests:2,maxGroups:1,maxItemsPerRequest:2,ttlMs:50,clock:()=>0});
 const a=recordingInputShape(null,'input','array_item'),b=recordingInputShape({},'input','array_item'),first=d.beginRequest();
 assert.equal(first(a),true);assert.equal(first(b),false);assert.equal(first(a),false);
 const second=d.beginRequest();assert.equal(second(a),true);assert.equal(d.beginRequest(),undefined);
 const state=d.status();assert.equal(state.state,'request_limit');assert.equal(state.requests,2);assert.equal(state.groups.length,1);assert.equal(state.groups[0].count,2);assert.equal(state.overflow,1);assert.equal(state.itemLimitRequests,1);assert.equal(state.truncated,true);
 assert.equal(second({...a,secret:'synthetic',has_type:undefined}),false);
});
test('TTL到期后包括已发callback都停止采样，关闭清空并撤销旧callback，不需要定时器',()=>{
 let now=0;const d=createRecordingInputDiagnostics({enabled:true,ttlMs:5,clock:()=>now}),observe=d.beginRequest(),a=recordingInputShape(1,'messages','top_level');observe(a);
 now=5;assert.equal(d.beginRequest(),undefined);assert.equal(observe(a),false);assert.equal(d.status().state,'expired');assert.equal(d.status().groups[0].count,1);
 d.close();assert.equal(d.status(),undefined);assert.equal(observe(a),false);assert.equal(d.beginRequest(),undefined);
});

test('每请求预算用完后停止构造额外诊断DTO，原提取仍按原节点预算继续',()=>{
 let reads=0;const payload={input:Array.from({length:20},()=>({get type(){reads++;return 'synthetic_unknown';}}))};
 const expected=captureRequestContext(payload),baselineReads=reads;reads=0;
 const diagnostics=createRecordingInputDiagnostics({enabled:true,maxItemsPerRequest:1});
 const actual=captureRequestContext(payload,{onUnsupportedInputShape:diagnostics.beginRequest()});
 assert.deepEqual(actual,expected);assert.equal(reads,baselineReads+1);assert.equal(diagnostics.status().groups[0].count,1);assert.equal(diagnostics.status().itemLimitRequests,1);
});

test('input为null而提取回退messages时诊断source指向实际选中输入，原覆盖不变',()=>{
 const payload={input:null,messages:{type:'message',role:'user',content:'synthetic private text'}},events=[],expected=captureRequestContext(payload);
 assert.deepEqual(captureRequestContext(payload,{onUnsupportedInputShape:value=>events.push(value)}),expected);
 assert.equal(events.length,1);assert.equal(events[0].source,'messages');assert.equal(events[0].location,'top_level');assert.equal(events[0].type,'message');assert.ok(!JSON.stringify(events).includes('synthetic private text'));
});
