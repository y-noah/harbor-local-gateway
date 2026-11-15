import test from 'node:test';
import assert from 'node:assert/strict';
import {ConversationCapture} from '../conversation-capture.mjs';
import {assembleRecording} from '../recording-search-index.mjs';

const capture=options=>{const events=[],collector=new ConversationCapture({...options,emit:event=>events.push(event)});return {events,collector,assembled:()=>assembleRecording(events.map((event,index)=>({...event,sequence:index+1})))};};

test('Shell多命令delta与最终action采用相同命令字段，不把原文拼进JSON',()=>{
  const c=capture(),commands=['printf first','printf second'],item={type:'shell_call',id:'shell',call_id:'call',status:'completed',action:{commands,timeout_ms:null,max_output_length:4096}};
  c.collector.event({type:'response.output_item.added',output_index:0,item:{...item,status:'in_progress',action:{...item.action,commands:[]}}});
  for(const [command_index,command] of commands.entries()){
    c.collector.event({type:'response.shell_call_command.added',output_index:0,command_index,command:''});
    for(const delta of [command.slice(0,6),command.slice(6)])c.collector.event({type:'response.shell_call_command.delta',output_index:0,command_index,delta});
    c.collector.event({type:'response.shell_call_command.done',output_index:0,command_index,command});
  }
  c.collector.event({type:'response.output_item.done',output_index:0,item});const summary=c.collector.finish({output:[item]}),body=c.assembled();
  assert.equal(summary.incomplete,false);assert.equal(summary.coverage.seen.toolCalls,1);assert.deepEqual(body.filter(item=>item.metadata.representation==='command').map(item=>[item.metadata.command_index,item.text]),[[0,commands[0]],[1,commands[1]]]);assert.deepEqual(JSON.parse(body.find(item=>item.metadata.representation==='call_metadata').text),{timeout_ms:null,max_output_length:4096});
});

test('Shell stdout/stderr按显式command_index分流，done不关闭其他命令，扁平final只保留输出位置',()=>{
  const c=capture(),start={type:'shell_call_output',id:'output',call_id:'call',status:'in_progress',output:[]};c.collector.event({type:'response.output_item.added',output_index:0,item:start});
  const delta=(command_index,value)=>c.collector.event({type:'response.shell_call_output_content.delta',item_id:'output',output_index:0,command_index,delta:value});
  delta(0,{stdout:'first '});delta(1,{stdout:'second '});delta(0,{stderr:'visible warning'});
  const first=[{stdout:'first ',stderr:'visible warning',outcome:{type:'exit',exit_code:2}},{stdout:'tail',stderr:'',outcome:{type:'timeout'}}];
  c.collector.event({type:'response.shell_call_output_content.done',item_id:'output',output_index:0,command_index:0,output:first});delta(1,{stdout:'tail'});
  const second=[{stdout:'second tail',stderr:'',outcome:{type:'exit',exit_code:0}}];c.collector.event({type:'response.shell_call_output_content.done',item_id:'output',output_index:0,command_index:1,output:second});
  const final={...start,status:'completed',output:[...first,...second]};c.collector.event({type:'response.output_item.done',output_index:0,item:final});const summary=c.collector.finish({output:[final]}),body=c.assembled(),streams=body.filter(item=>item.metadata.representation==='stream_channel');
  assert.equal(summary.incomplete,false);assert.equal(summary.coverage.seen.toolResults,1);assert.equal(streams.find(item=>item.metadata.command_index===0&&item.metadata.channel==='stdout').text,'first tail');assert.equal(streams.find(item=>item.metadata.command_index===1&&item.metadata.channel==='stdout').text,'second tail');assert.equal(streams.find(item=>item.metadata.channel==='stderr').text,'visible warning');
  const snapshots=body.filter(item=>item.metadata.representation==='item_output_snapshot');assert.deepEqual(snapshots.map(item=>item.metadata.output_position),[0,1,2]);assert.ok(snapshots.every(item=>item.metadata.association_state==='unavailable'&&!Object.hasOwn(item.metadata,'command_index')));assert.deepEqual(snapshots.map(item=>JSON.parse(item.text)),final.output);assert.equal(body.filter(item=>item.metadata.representation==='command_output_snapshot').length,3);
});

test('Shell流中断保留原始已收片段、明确不完整，并跨每个delta分割脱敏',()=>{
  const raw='Authorization: Bearer opaque-fixture-secret\nprintf visible';
  for(let split=0;split<=raw.length;split++){
    const c=capture();c.collector.event({type:'response.output_item.added',output_index:0,item:{type:'shell_call',id:'shell',call_id:'call',action:{commands:[]},status:'in_progress'}});
    for(const delta of [raw.slice(0,split),raw.slice(split)])c.collector.event({type:'response.shell_call_command.delta',output_index:0,command_index:0,delta});
    const summary=c.collector.finish();assert.equal(summary.incomplete,true);assert.ok(!JSON.stringify(c.events).includes('opaque-fixture-secret'));assert.ok(c.assembled().some(item=>item.text.includes('printf visible')));
  }
  const c=capture();c.collector.event({type:'response.output_item.added',output_index:0,item:{type:'shell_call_output',id:'output',call_id:'call',output:[]}});c.collector.event({type:'response.shell_call_output_content.delta',item_id:'output',output_index:0,command_index:2,delta:{stdout:'visible partial'}});assert.equal(c.collector.finish().incomplete,true);assert.ok(c.assembled().some(item=>item.metadata.command_index===2&&item.text==='visible partial'));
});

test('Shell输出快照在首次字段过滤前连续脱敏同一channel，保留client来源与退出结果',()=>{
  for(const [a,b] of [['Authorization: Bearer ','opaque-fixture-secret\nvisible'],['{"api_key":"','opaque-fixture-secret","path":"visible"}']]){
    const c=capture(),payload={input:[{type:'shell_call_output',call_id:'call',output:[{stdout:a,stderr:'',outcome:{type:'exit',exit_code:0}},{stdout:b,stderr:'',outcome:{type:'timeout'}}]}]};c.collector.input(payload);const result=c.collector.finish(),body=c.assembled();assert.equal(result.incomplete,false);assert.ok(!JSON.stringify(c.events).includes('opaque-fixture-secret'));assert.ok(body.every(item=>item.provenance==='client'));assert.deepEqual(body.map(item=>item.metadata.output_position),[0,1]);assert.ok(body.map(item=>JSON.parse(item.text).stdout).join('').includes('visible'));
  }
});
test('Shell空done或空channel不擦除已收stdout/stderr，终态空数组仍保留实际片段',()=>{
  for(const output of [[],[{stdout:'',stderr:'',outcome:{type:'exit',exit_code:0}}]]){
    const c=capture(),item={type:'shell_call_output',id:'output',call_id:'call',output:[],status:'in_progress'};c.collector.event({type:'response.output_item.added',output_index:0,item});
    c.collector.event({type:'response.shell_call_output_content.delta',item_id:'output',output_index:0,command_index:3,delta:{stdout:'visible stdout',stderr:'visible stderr'}});
    c.collector.event({type:'response.shell_call_output_content.done',item_id:'output',output_index:0,command_index:3,output});c.collector.event({type:'response.output_item.done',output_index:0,item:{...item,status:'completed'}});
    const result=c.collector.finish({output:[{...item,status:'completed'}]}),streams=c.assembled().filter(item=>item.metadata.representation==='stream_channel');assert.equal(result.incomplete,false);assert.equal(streams.find(item=>item.metadata.channel==='stdout').text,'visible stdout');assert.equal(streams.find(item=>item.metadata.channel==='stderr').text,'visible stderr');assert.ok(streams.every(item=>item.metadata.command_index===3));assert.equal(result.coverage.seen.toolResults,1);
  }
});
