import test from 'node:test';
import assert from 'node:assert/strict';
import {ConversationCapture} from '../conversation-capture.mjs';
import {assembleRecording} from '../recording-search-index.mjs';
import {captureRequestContext} from '../recording-request-context.mjs';

const capture=options=>{const events=[],collector=new ConversationCapture({...options,emit:event=>events.push(event)});return {events,collector,assembled:()=>assembleRecording(events.map((event,index)=>({...event,sequence:index+1})))};};

test('function/custom/shell/patch八类caller保留真实作用域，async和environment仅在协议允许处保存',()=>{
  const caller={type:'program',caller_id:'parent'},items=[{type:'function_call',id:'function',call_id:'c1',name:'lookup',arguments:'{}',async:true},{type:'custom_tool_call',id:'custom',call_id:'c2',name:'exec',input:'visible',async:false},{type:'shell_call',id:null,call_id:'c3',action:{commands:['printf visible']},environment:{type:'local',skills:[{name:'visible',description:'visible skill',path:'/tmp/visible'}]}},{type:'apply_patch_call',id:null,call_id:'c4',operation:{type:'delete_file',path:'visible.txt'}},{type:'function_call_output',id:null,call_id:null,name:null,output:'visible'},{type:'custom_tool_call_output',id:'custom-output',call_id:'c2',output:'visible'},{type:'shell_call_output',id:null,call_id:'c3',output:[{stdout:'visible',stderr:'',outcome:{type:'exit',exit_code:0}}]},{type:'apply_patch_call_output',id:null,call_id:'c4',status:'completed'}].map(item=>({...item,caller}));
  const c=capture(),result=c.collector.finish({output:items});assert.equal(result.incomplete,false);const body=c.assembled();assert.ok(body.every(item=>item.metadata.caller.type==='program'&&item.metadata.caller.caller_id==='parent'));
  assert.equal(body.find(item=>item.metadata.toolType==='function_call'&&item.kind==='tool_call').metadata.async,true);assert.equal(body.find(item=>item.metadata.toolType==='custom_tool_call'&&item.kind==='tool_call').metadata.async,false);assert.equal(body.find(item=>item.metadata.representation==='command').metadata.environment.skills[0].path,'/tmp/visible');
  const nullable=body.find(item=>item.kind==='tool_result'&&item.metadata.toolType==='function_call');assert.equal(nullable.metadata.item,null);assert.equal(nullable.metadata.callId,null);assert.equal(nullable.metadata.name,null);assert.equal(nullable.metadata.association_state,'unavailable');
  const client=capture();client.collector.input({input:items.filter(item=>item.type.endsWith('_output'))});assert.equal(client.collector.finish().incomplete,false);assert.ok(client.assembled().every(item=>item.provenance==='client'&&item.metadata.caller.caller_id==='parent'));
});

test('合法null关联原样保存但缺失required关联仍披露，非法形状不冒充合法null',()=>{
  for(const provenance of ['client','upstream']){
    const c=capture(),item={type:'function_call_output',id:null,call_id:null,name:null,caller:null,status:null,output:'visible'};
    if(provenance==='client')c.collector.input({input:[item]});const result=c.collector.finish(provenance==='upstream'?{output:[item]}:undefined),[body]=c.assembled();assert.equal(result.incomplete,false);assert.equal(body.metadata.association_state,'unavailable');assert.equal(body.metadata.caller,null);assert.equal(body.metadata.callId,null);
  }
  const missing=capture();assert.equal(missing.collector.finish({output:[{type:'function_call_output',output:'visible'}]}).incomplete,true);
  for(const caller of [{type:'future'},[],42,{type:'program',caller_id:null}]){const c=capture();assert.equal(c.collector.finish({output:[{type:'custom_tool_call',id:'custom',call_id:'call',name:'exec',input:'visible',caller}]}).incomplete,true);}
  const wrong=capture();assert.equal(wrong.collector.finish({output:[{type:'mcp_call',id:'mcp',name:'lookup',server_label:'server',arguments:'{}',caller:{type:'direct'}}]}).incomplete,true);assert.ok(wrong.assembled().every(item=>!Object.hasOwn(item.metadata,'caller')));
});

test('共享节点预算扣除每个可见item实际过滤消耗，单项合法但总量超限停止采集',()=>{
  const item={type:'file_search_call',id:'first',status:'completed',queries:['visible query'],results:[{filename:'visible.txt',text:'visible result'}]},cost=captureRequestContext({input:[item]},{maxNodes:2048}).nodesConsumed;
  assert.ok(cost>0);const single=capture({maxNodes:cost+1});assert.equal(single.collector.finish({output:[item]}).truncated,false);
  const combined=capture({maxNodes:cost*2+1}),result=combined.collector.finish({output:[item,{...item,id:'second'}]});assert.equal(result.truncated,true);assert.ok(result.coverage.reasons.includes('size_limit'));assert.ok(combined.collector.remainingNodes<=0);assert.ok(combined.assembled().some(item=>item.text.includes('visible query')));
});

test('可见工具added快照在中断时保留但不伪装完整，终态同字段关闭且不重复',()=>{
  for(const item of [{type:'web_search_call',id:'web',status:'in_progress'},{type:'file_search_call',id:'file',status:'in_progress'},{type:'computer_call',id:'computer',call_id:'call',status:'in_progress',pending_safety_checks:[]}]){
    const c=capture();c.collector.event({type:'response.output_item.added',output_index:0,item});const result=c.collector.finish();assert.equal(result.incomplete,true);assert.equal(c.assembled().length,1);
    const complete=capture();complete.collector.event({type:'response.output_item.added',output_index:0,item});complete.collector.event({type:'response.output_item.done',output_index:0,item:{...item,status:'completed'}});assert.equal(complete.collector.finish({output:[{...item,status:'completed'}]}).incomplete,false);assert.equal(complete.assembled().length,1);
  }
});
test('新增工具结果added保持开放，done/final闭合且不重复已收正文',()=>{
  for(const item of [{type:'mcp_list_tools',id:'catalog',server_label:'server',tools:[],error:null},{type:'shell_call_output',id:'shell-output',call_id:'call',status:'in_progress',output:[]}]){
    const c=capture();c.collector.event({type:'response.output_item.added',output_index:0,item});assert.equal(c.collector.finish().incomplete,true);assert.equal(c.assembled().length,1);
    const done=capture(),terminal={...item,...(item.type==='shell_call_output'?{status:'completed'}:{})};done.collector.event({type:'response.output_item.added',output_index:0,item});done.collector.event({type:'response.output_item.done',output_index:0,item:terminal});assert.equal(done.collector.finish({output:[terminal]}).incomplete,false);assert.equal(done.assembled().length,1);
  }
  const c=capture(),item={type:'shell_call_output',id:'output',call_id:'call',status:'in_progress',output:[]};c.collector.event({type:'response.output_item.added',output_index:0,item});const result=c.collector.finish({output:[{...item,status:'completed',output:[{stdout:'visible',stderr:'',outcome:{type:'exit',exit_code:0}}]}]});assert.equal(result.incomplete,false);assert.equal(result.coverage.seen.toolResults,1);assert.ok(c.assembled().some(item=>item.text.includes('visible')));
});
test('工具结果null与真实ID双向更新关联状态，缺失字段保留已知关联',()=>{
  for(const [initial,final,expected] of [[null,'actual-call','provided'],['old-call',null,'unavailable'],['actual-call',undefined,'provided']]){
    const c=capture(),item={type:'function_call_output',id:'result',call_id:initial,name:'old-name',output:'visible'},terminal={type:'function_call_output',id:'result',...(final===undefined?{}:{call_id:final}),...(final===null?{name:null}:{}) ,output:'visible'};
    c.collector.event({type:'response.output_item.added',output_index:0,item});c.collector.event({type:'response.output_item.done',output_index:0,item:terminal});const result=c.collector.finish({output:[terminal]}),[body]=c.assembled();assert.equal(result.incomplete,false);assert.equal(c.assembled().length,1);assert.equal(body.text,'visible');assert.equal(body.metadata.association_state,expected);assert.equal(body.metadata.callId,final===undefined?initial:final);if(final===null)assert.equal(body.metadata.name,null);
  }
  const invalid=capture(),item={type:'function_call_output',id:'result',call_id:'known',output:'visible'};invalid.collector.event({type:'response.output_item.added',output_index:0,item});assert.equal(invalid.collector.finish({output:[{...item,call_id:5}]}).incomplete,true);
});
