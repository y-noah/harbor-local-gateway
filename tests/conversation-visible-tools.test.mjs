import test from 'node:test';
import assert from 'node:assert/strict';
import {ConversationCapture,currentInput} from '../conversation-capture.mjs';
import {assembleRecording} from '../recording-search-index.mjs';

const capture=options=>{const events=[],collector=new ConversationCapture({...options,emit:options?.emit??(event=>events.push(event))});return {events,collector,assembled:()=>assembleRecording(events.map((event,index)=>({...event,sequence:index+1})))};};

test('MCP目录保留schema定义及目录来源，敏感默认值过滤且重复快照不重复正文',()=>{
  const c=capture(),item={type:'mcp_list_tools',id:'catalog',server_label:'server',tools:[{name:'exec',input_schema:{type:'object',properties:{api_key:{type:'string',default:'opaque-fixture-secret'},path:{type:'string'}}},description:'visible tool',annotations:{readOnlyHint:true}}],error:null},before=JSON.stringify(item);
  c.collector.event({type:'response.output_item.added',output_index:0,item});c.collector.event({type:'response.output_item.done',output_index:0,item});const result=c.collector.finish({output:[item]}),items=c.assembled();
  assert.equal(result.incomplete,false);assert.equal(items.length,1);assert.equal(items[0].metadata.record_type,'mcp_list_tools');assert.equal(items[0].metadata.server_label,'server');assert.equal(items[0].metadata.callId,null);assert.equal(JSON.parse(items[0].text).tools[0].input_schema.properties.api_key.type,'string');assert.ok(!JSON.stringify(c.events).includes('opaque-fixture-secret'));assert.equal(JSON.stringify(item),before);assert.equal(result.coverage.captured.toolResults,1);
});

test('MCP审批提议与客户端决策保留真实关联，不伪造成工具已执行',()=>{
  const request={type:'mcp_approval_request',id:'approval',name:'write',server_label:'server',arguments:'{"password":"opaque-fixture-secret","path":"visible.txt"}'},upstream=capture();
  assert.equal(upstream.collector.finish({output:[request]}).incomplete,false);const [proposed]=upstream.assembled();assert.equal(proposed.metadata.record_type,'mcp_approval_request');assert.equal(proposed.metadata.item,'approval');assert.equal(proposed.metadata.callId,null);assert.equal(JSON.parse(JSON.parse(proposed.text).arguments).path,'visible.txt');assert.ok(!JSON.stringify(upstream.events).includes('opaque-fixture-secret'));
  const response={type:'mcp_approval_response',approval_request_id:'approval',approve:false,id:null,reason:'visible refusal'},client=capture(),payload={input:[{role:'user',content:'private old prompt'},request,response]},turn=currentInput(payload);
  assert.equal(turn.events.length,1);client.collector.input(payload,turn);assert.equal(client.collector.finish().incomplete,false);const [decision]=client.assembled();assert.equal(decision.provenance,'client');assert.equal(decision.metadata.approval_request_id,'approval');assert.equal(decision.metadata.record_type,'mcp_approval_response');assert.equal(JSON.parse(decision.text).approve,false);assert.ok(!JSON.stringify(client.events).includes('private old prompt'));
});

test('Web与文件搜索保留实际查询结果文本，引用URL和文件标识省略',()=>{
  const c=capture(),items=[{type:'web_search_call',id:'web',status:'completed',action:{type:'search',query:'visible search',sources:[{type:'url',url:'https://synthetic.invalid/PRIVATE_URL'}]}},{type:'file_search_call',id:'file',status:'completed',queries:['visible query'],results:[{file_id:'PRIVATE_FILE',filename:'visible.txt',score:0.9,text:'visible retrieved text',attributes:{category:'visible'}}]}];
  assert.equal(c.collector.finish({output:items}).incomplete,false);const body=c.assembled();assert.equal(body.length,2);assert.equal(JSON.parse(body[0].text).action.query,'visible search');assert.equal(JSON.parse(body[1].text).results[0].text,'visible retrieved text');assert.ok(!JSON.stringify(c.events).includes('PRIVATE_'));assert.ok(body.every(item=>item.metadata.callId===null));
});

test('Computer动作和安全检查保留，客户端截图只保留附件元数据',()=>{
  const c=capture(),call={type:'computer_call',id:'computer',call_id:'computer-call',status:'completed',actions:[{type:'type',text:'visible input'},{type:'click',button:'left',x:1,y:2}],pending_safety_checks:[{id:'check',code:null,message:'visible safety check'}]};
  assert.equal(c.collector.finish({output:[call]}).incomplete,false);const [action]=c.assembled();assert.equal(JSON.parse(action.text).actions[0].text,'visible input');assert.equal(action.metadata.callId,'computer-call');
  const client=capture(),payload={input:[{type:'computer_call_output',call_id:'computer-call',output:{type:'computer_screenshot',image_url:'data:image/png;base64,PRIVATE_IMAGE'},acknowledged_safety_checks:[{id:'check',code:null,message:'visible acknowledgement'}],status:null}]};client.collector.input(payload);assert.equal(client.collector.finish().incomplete,false);const [result]=client.assembled();assert.equal(result.provenance,'client');assert.equal(result.metadata.callId,'computer-call');assert.ok(!JSON.stringify(client.events).includes('PRIVATE_IMAGE'));assert.ok(result.text.includes('computer_screenshot'));assert.ok(result.text.includes('visible acknowledgement'));
});

test('Search状态事件与added/done/final合并为一个实际调用',()=>{
  const c=capture(),item={type:'web_search_call',id:'web',status:'completed',action:{type:'search',queries:['visible query']}};
  c.collector.event({type:'response.output_item.added',output_index:0,item:{...item,status:'in_progress'}});c.collector.event({type:'response.web_search_call.searching',output_index:0,item_id:'web'});c.collector.event({type:'response.output_item.done',output_index:0,item});const result=c.collector.finish({output:[item]});assert.equal(result.incomplete,false);assert.equal(c.assembled().length,1);assert.equal(c.assembled()[0].metadata.status,'completed');assert.equal(result.coverage.seen.toolCalls,1);
});

test('可见工具协议仍保留未支持字段、预算和持久化失败的完整性事实',()=>{
  const c=capture(),result=c.collector.finish({output:[{type:'web_search_call',id:'web',status:'completed',action:{type:'future_action',private:'PRIVATE_UNSUPPORTED'}}]});assert.equal(result.incomplete,true);assert.ok(!JSON.stringify(c.events).includes('PRIVATE_UNSUPPORTED'));
  const small=capture({maxBytes:256});assert.equal(small.collector.finish({output:[{type:'file_search_call',id:'file',status:'completed',queries:['visible '.repeat(1000)],results:null}]}).truncated,true);assert.ok(small.collector.bytes<=256);
  const failed=capture({emit:()=>false}),failure=failed.collector.finish({output:[{type:'mcp_list_tools',id:'catalog',server_label:'server',tools:[]}]});assert.equal(failure.failed,true);assert.equal(failure.coverage.captured.toolResults,0);
});
