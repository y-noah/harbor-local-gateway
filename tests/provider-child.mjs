// Executed only by providers.test.mjs; never contacts an external service.
const [provider,scenario]=process.argv.slice(2);
process.stdin.resume();
if(scenario==='hang'){setInterval(()=>{},1000);}
else if(scenario==='oversize'){process.stdout.write('x'.repeat(2100000));}
else if(scenario==='invalid-json'){process.stdout.write('private upstream diagnostic should never be echoed');}
else if(provider==='rates'){
  let input='';process.stdin.on('data',b=>{input+=b.toString();let at;while((at=input.indexOf('\n'))!==-1){const message=JSON.parse(input.slice(0,at));input=input.slice(at+1);if(message.id===1)process.stdout.write(JSON.stringify({id:1,result:{}})+'\n');if(message.id===2)process.stdout.write(JSON.stringify(scenario==='error'?{id:2,error:{message:'private'}}:{id:2,result:{rateLimits:{primary:{usedPercent:25,windowDurationMins:300,resetsAt:9999999999}}}})+'\n');}});
}
else {
  const text='中文测试 🍋 Unicode',thread=scenario==='different-session'?'wrong-thread':'fixed-thread';
  let output;
  if(provider==='codex'){
    const rows=[{type:'thread.started',thread_id:thread},{type:'item.completed',item:{type:'agent_message',text}}];
    if(scenario==='tool')rows.push({type:'item.started',item:{type:'command_execution'}});
    if(scenario==='error')rows.push({type:'turn.failed'});
    rows.push({type:'turn.completed',usage:{input_tokens:120,output_tokens:20,cached_input_tokens:scenario==='bad-cache'?999:50}});
    if(scenario==='missing-usage')rows.pop();output=rows.map(x=>JSON.stringify(x)).join('\n');
  }else{
    output=JSON.stringify({type:'result',subtype:'success',result:text,session_id:thread,modelUsage:{sonnet:scenario==='missing-usage'?{}:{inputTokens:70,outputTokens:20,cacheReadInputTokens:50,cacheCreationInputTokens:0}}});
  }
  const bytes=Buffer.from(output),at=bytes.indexOf(Buffer.from('中文'))+1;
  process.stdout.write(bytes.subarray(0,at));
  setTimeout(()=>{process.stdout.write(bytes.subarray(at));process.stderr.write('private diagnostic');process.exitCode=scenario==='nonzero'?7:0;},10);
}
