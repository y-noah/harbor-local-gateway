// Exercise the received-context path while keeping the expanded JSON body at
// the declared size. Only the final user message belongs to the current turn.
export function capacityModelPayload({model,trace,bytes=1024}={}){
 if(typeof model!=='string'||typeof trace!=='string'||!Number.isSafeInteger(bytes)||bytes<1)throw Error('capacity_model_payload_configuration');
 let input='容量合成请求 '+trace+' ';
 const payload={stream:true,model,instructions:'容量合成指令：返回受控结果。',
  tools:[{type:'function',name:'capacity_probe',description:'合成工具定义',parameters:{type:'object',properties:{label:{type:'string'}},required:['label'],additionalProperties:false},strict:true}],
  input:[{role:'user',content:[{type:'input_text',text:'此前的合成问题'}]},{role:'assistant',content:[{type:'output_text',text:'此前的合成回复'}]},{role:'user',content:[{type:'input_text',text:input}]}],
  metadata:{capacity_trace:trace}};
 const padding=bytes-Buffer.byteLength(JSON.stringify(payload));if(padding<0)throw Error('capacity_model_payload_too_small');
 input+='.'.repeat(padding);payload.input.at(-1).content[0].text=input;
 return {payload,input};
}
