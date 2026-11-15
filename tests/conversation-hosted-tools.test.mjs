import test from 'node:test';
import assert from 'node:assert/strict';
import {ConversationCapture} from '../conversation-capture.mjs';
import {assembleRecording} from '../recording-search-index.mjs';

const capture=()=>{const events=[],collector=new ConversationCapture({emit:event=>events.push(event)});return {events,collector,assembled:()=>assembleRecording(events.map((event,index)=>({...event,sequence:index+1})))};};

test('Code Interpreter保存原代码和日志，图片输出省略且不伪造call ID',()=>{
  const c=capture(),item={type:'code_interpreter_call',id:'code',container_id:'container',status:'completed',code:'print("visible")',outputs:[{type:'logs',logs:'visible log'},{type:'image',url:'https://synthetic.invalid/PRIVATE_IMAGE?token=PRIVATE_TOKEN'}]};
  const before=JSON.stringify(item),summary=c.collector.finish({output:[item]}),items=c.assembled();
  assert.equal(summary.incomplete,false);assert.equal(summary.coverage.captured.toolCalls,1);assert.equal(summary.coverage.captured.toolResults,1);assert.equal(items[0].text,item.code);assert.equal(items[0].metadata.container_id,'container');assert.equal(items[0].metadata.callId,null);assert.equal(JSON.parse(items[1].text)[0].logs,'visible log');assert.ok(!JSON.stringify(c.events).includes('PRIVATE_'));assert.equal(JSON.stringify(item),before);
});

test('Code Interpreter流式片段与完整代码按item合并且原文本脱敏',()=>{
  const c=capture(),code='Authorization: Bearer opaque-fixture-secret\nprint("visible")',item={type:'code_interpreter_call',id:'code',container_id:'container',status:'completed',code,outputs:null};
  c.collector.event({type:'response.output_item.added',output_index:1,item:{...item,status:'in_progress',code:'',outputs:null}});
  for(const delta of [code.slice(0,30),code.slice(30)])c.collector.event({type:'response.code_interpreter_call_code.delta',item_id:'code',output_index:1,delta});
  c.collector.event({type:'response.code_interpreter_call_code.done',item_id:'code',output_index:1,code});c.collector.event({type:'response.output_item.done',output_index:1,item});
  const result=c.collector.finish({output:[item]}),calls=c.assembled().filter(item=>item.kind==='tool_call');assert.equal(calls.length,1);assert.ok(calls[0].text.includes('print("visible")'));assert.ok(!JSON.stringify(c.events).includes('opaque-fixture-secret'));assert.equal(result.incomplete,false);assert.equal(result.coverage.seen.toolCalls,1);
});

test('MCP流式参数、返回值、错误和审批关联均保留且不要求不存在的call ID',()=>{
  const c=capture(),args=JSON.stringify({password:'opaque-password',path:'visible.txt'}),item={type:'mcp_call',id:'mcp',name:'read',server_label:'files',approval_request_id:'approval',arguments:args,status:'failed',output:'visible partial result',error:{type:'mcp_protocol_error',code:2,message:'visible failure'}};
  c.collector.event({type:'response.output_item.added',output_index:0,item:{...item,arguments:'',status:'in_progress',output:null,error:null}});
  c.collector.event({type:'response.mcp_call_arguments.delta',item_id:'mcp',output_index:0,delta:args.slice(0,18)});c.collector.event({type:'response.mcp_call_arguments.delta',item_id:'mcp',output_index:0,delta:args.slice(18)});c.collector.event({type:'response.mcp_call_arguments.done',item_id:'mcp',output_index:0,arguments:args});
  c.collector.event({type:'response.mcp_call.failed',item_id:'mcp',output_index:0});c.collector.event({type:'response.output_item.done',output_index:0,item});
  const result=c.collector.finish({output:[item]}),items=c.assembled();assert.equal(result.incomplete,false);assert.equal(result.coverage.seen.toolCalls,1);assert.equal(result.coverage.seen.toolResults,1);assert.equal(items.length,2);assert.equal(JSON.parse(items[0].text).path,'visible.txt');assert.equal(items[0].metadata.server_label,'files');assert.equal(items[0].metadata.approval_request_id,'approval');assert.equal(items[0].metadata.error.code,2);assert.equal(items[1].text,item.output);assert.ok(!JSON.stringify(c.events).includes('opaque-password'));
});

test('Hosted工具中断保留收到的片段并明确未完成，缺少item ID不能冒充完整',()=>{
  const c=capture();c.collector.event({type:'response.output_item.added',output_index:0,item:{type:'code_interpreter_call',id:'code',container_id:'container',code:'',status:'in_progress',outputs:null}});c.collector.event({type:'response.code_interpreter_call_code.delta',output_index:0,item_id:'code',delta:'print("partial")'});
  assert.equal(c.collector.finish().incomplete,true);assert.equal(c.assembled()[0].text,'print("partial")');
  const missing=capture();assert.equal(missing.collector.finish({output:[{type:'mcp_call',name:'read',server_label:'files',arguments:'{}',status:'completed',output:null}]}).incomplete,true);
});

test('Code Interpreter空代码保留null事实，日志片段跨元素脱敏',()=>{
  const c=capture(),secret='sk-proj-opaque-fixture-secret';const result=c.collector.finish({output:[{type:'code_interpreter_call',id:'code',container_id:'container',status:'completed',code:null,outputs:[{type:'logs',logs:secret.slice(0,10)},{type:'logs',logs:secret.slice(10)+'\nvisible tail'}]}]});
  assert.equal(result.incomplete,false);assert.equal(c.assembled()[0].metadata.code_state,'unavailable');assert.ok(!JSON.stringify(c.events).includes('opaque-fixture-secret'));assert.ok(JSON.stringify(c.events).includes('visible tail'));
});
test('代码从null变为已收到后更新可得状态，最终null不擦除已收到的代码',()=>{
  for(const finalCode of ['print(1)',null]){
    const c=capture(),initial={type:'code_interpreter_call',id:'code',container_id:'container',status:'in_progress',code:null,outputs:null};
    c.collector.event({type:'response.output_item.added',output_index:0,item:initial});c.collector.event({type:'response.code_interpreter_call_code.delta',item_id:'code',output_index:0,delta:'print(1)'});c.collector.event({type:'response.code_interpreter_call_code.done',item_id:'code',output_index:0,code:'print(1)'});
    const result=c.collector.finish({output:[{...initial,status:'completed',code:finalCode}]});const call=c.assembled().find(item=>item.kind==='tool_call');assert.equal(call.text,'print(1)');assert.equal(call.metadata.code_state,'received');assert.equal(Object.hasOwn(call.metadata,'code'),false);assert.equal(result.incomplete,false);
  }
  const c=capture(),initial={type:'code_interpreter_call',id:'code',container_id:'container',status:'in_progress',code:null,outputs:null};c.collector.event({type:'response.output_item.added',output_index:0,item:initial});c.collector.event({type:'response.code_interpreter_call_code.delta',item_id:'code',output_index:0,delta:'print("partial")'});assert.equal(c.collector.finish({output:[{...initial,status:'completed'}]}).incomplete,true);assert.equal(c.assembled()[0].text,'print("partial")');
});
test('logs形状的普通业务对象及MCP未知错误内容保留原字段，不升级为代码输出协议',()=>{
  const diagnostic={type:'logs',logs:'visible',level:'warning',exit_code:2};
  const ordinary=capture();assert.equal(ordinary.collector.finish({output:[{type:'function_call_output',id:'result',call_id:'call',output:[diagnostic]}]}).incomplete,false);assert.deepEqual(JSON.parse(ordinary.assembled()[0].text),[diagnostic]);
  for(const content of [{diagnostics:[diagnostic]},[diagnostic],['visible',2],[{type:'text',text:'visible diagnostic',severity:'fatal',exit_code:2}]]){
    const c=capture(),result=c.collector.finish({output:[{type:'mcp_call',id:'mcp',name:'exec',server_label:'server',arguments:'{}',status:'failed',output:null,error:{type:'mcp_tool_execution_error',content}}]});
    assert.equal(result.incomplete,false);assert.deepEqual(c.assembled()[0].metadata.error.content,content);
  }
});
test('Hosted added结果生命周期不被代码或参数done提前关闭，终态缺省可选输出不擦已收正文',()=>{
  for(const item of [{type:'code_interpreter_call',id:'code',container_id:'container',code:'print(1)',outputs:[],status:'in_progress'},{type:'code_interpreter_call',id:'code',container_id:'container',code:'print(1)',outputs:null,status:'in_progress'},{type:'mcp_call',id:'mcp',name:'exec',server_label:'server',arguments:'{}',output:'visible result',status:'in_progress'},{type:'mcp_call',id:'mcp',name:'exec',server_label:'server',arguments:'{}',status:'in_progress'}]){
    const isCode=item.type==='code_interpreter_call',field=isCode?'code':'arguments',eventType=isCode?'response.code_interpreter_call_code.done':'response.mcp_call_arguments.done',c=capture();
    c.collector.event({type:'response.output_item.added',output_index:0,item});c.collector.event({type:eventType,item_id:item.id,output_index:0,[field]:item[field]});assert.equal(c.collector.finish().incomplete,true,item.type);assert.ok(c.assembled().some(row=>row.text===item[field]));
    const done=capture();done.collector.event({type:'response.output_item.added',output_index:0,item});done.collector.event({type:eventType,item_id:item.id,output_index:0,[field]:item[field]});const terminal={...item,status:'completed'};if(!isCode)delete terminal.output;done.collector.event({type:'response.output_item.done',output_index:0,item:terminal});assert.equal(done.collector.finish({output:[terminal]}).incomplete,false);assert.equal(done.assembled().filter(row=>row.kind==='tool_call').length,1);if(item.output)assert.equal(done.assembled().find(row=>row.kind==='tool_result').text,item.output);
  }
});
test('Hosted已有可见输出遇到终态null或缺省保留body，仅更新实际快照可得事实',()=>{
  for(const initial of [{type:'code_interpreter_call',id:'code',container_id:'container',code:'print(1)',outputs:[{type:'logs',logs:'visible received log'}],status:'in_progress'},{type:'mcp_call',id:'mcp',name:'exec',server_label:'server',arguments:'{}',output:'visible received output',status:'in_progress'}])for(const finalValue of [null,undefined]){
    const c=capture(),output=initial.type==='code_interpreter_call'?'outputs':'output';c.collector.event({type:'response.output_item.added',output_index:0,item:initial});const final={...initial,status:'completed'};if(finalValue===undefined)delete final[output];else final[output]=finalValue;c.collector.event({type:'response.output_item.done',output_index:0,item:final});const result=c.collector.finish({output:[final]}),body=c.assembled().find(row=>row.kind==='tool_result');
    assert.equal(result.incomplete,false);assert.ok(body.text.includes('visible received'));assert.equal(body.metadata.item_pending,false);assert.equal(body.metadata.output_snapshot_available,finalValue!==null);assert.equal(c.assembled().filter(row=>row.kind==='tool_result').length,1);
  }
});
test('MCP失败事件先于参数done到达时，参数闭合不回退已收到的失败状态',()=>{
  const c=capture();c.collector.event({type:'response.output_item.added',output_index:0,item:{type:'mcp_call',id:'mcp',name:'exec',server_label:'server',arguments:'',status:'in_progress'}});c.collector.event({type:'response.mcp_call.failed',item_id:'mcp',output_index:0});c.collector.event({type:'response.mcp_call_arguments.done',item_id:'mcp',output_index:0,arguments:'{}'});assert.equal(c.collector.finish().incomplete,true);assert.equal(c.assembled()[0].metadata.status,'failed');
});
