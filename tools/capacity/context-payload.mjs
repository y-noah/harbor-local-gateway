import {createHash} from 'node:crypto';

// Exact serialized component-content bytes, matching RecordingContextStore's
// byte accounting. The payload contains only generated instructions/history.
export function capacityHistoricalContext(index,bytes){
 if(!Number.isSafeInteger(index)||index<0||!Number.isSafeInteger(bytes)||bytes<1024||bytes>1048576)throw Error('capacity_context_payload_configuration');
 const components=[
  {kind:'instruction',content:{text:'仅处理容量实验中的合成仓库。保留代码和工具结果，不包含真实客户资料。'}},
  {kind:'tool_schema',content:{name:'synthetic_exec',description:'读取合成代码',input_schema:{type:'object',properties:{command:{type:'string'}},required:['command']}}},
  {kind:'history',content:{role:'user',text:'历史合成提问 context_seed_'+index+'：检查函数边界。'}},
  {kind:'history',content:{role:'assistant',text:'历史合成分析 '+index+'\n'}},
  {kind:'context',content:{workspace:'/workspace/capacity-synthetic',turn:index,source:'generated-history'}}
 ];
 const size=()=>components.reduce((n,row)=>n+Buffer.byteLength(JSON.stringify(row.content)),0),remaining=bytes-size();if(remaining<0)throw Error('capacity_context_payload_capacity');
 // Hex characters need no JSON escaping and avoid a compressible blank corpus.
 components[3].content.text+=createHash('shake256',{outputLength:Math.ceil(remaining/2)}).update('context-history:'+index).digest('hex').slice(0,remaining);
 if(size()!==bytes)throw Error('capacity_context_payload_size_mismatch');
 return {components,bytes,contentHash:createHash('sha256').update(JSON.stringify(components.map(row=>[row.kind,row.content]))).digest('hex')};
}
