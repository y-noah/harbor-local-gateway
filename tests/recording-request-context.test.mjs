import test from 'node:test';
import assert from 'node:assert/strict';
import {captureRequestContext,captureRequestContextAsync} from '../recording-request-context.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {randomBytes} from 'node:crypto';

test('工具错误的普通业务字段不按协议片段裁掉，跨片凭据和附件仍过滤',async()=>{
 const payload={input:[{type:'mcp_call',id:'synthetic-error',name:'exec',server_label:'synthetic',arguments:'{}',output:null,error:{type:'mcp_tool_execution_error',content:[{type:'text',text:'Authorization: Bearer ',severity:'fatal',exit_code:2},{type:'text',text:'private-error-secret',detail:'visible'},{type:'image',url:'https://private.example/image'}]}}]},view=captureRequestContext(payload);
 assert.equal(view.state,'complete');assert.deepEqual(view.reasons,[]);const content=view.components[0].content.value.error.content;assert.equal(content[0].severity,'fatal');assert.equal(content[0].exit_code,2);assert.equal(content[1].detail,'visible');assert.match(content[2].content,/附件数据已省略/);assert.ok(!JSON.stringify(view).includes('private-error-secret'));assert.ok(!JSON.stringify(view).includes('private.example'));assert.deepEqual(await captureRequestContextAsync(payload),view);
});

test('内部节点消耗可供工具适配器共享预算且截断时不超过批准额度',async()=>{
 const payload={input:[{type:'web_search_call',id:'synthetic-search',status:'completed',action:{type:'search',queries:['合成问题一','合成问题二']}}]};
 for(const maxNodes of [1,8,16,64,2048]){
  const view=captureRequestContext(payload,{maxNodes});assert.ok(Number.isSafeInteger(view.nodesConsumed));assert.ok(view.nodesConsumed>0&&view.nodesConsumed<=maxNodes);assert.deepEqual(await captureRequestContextAsync(payload,{maxNodes}),view);
  if(view.reasons.includes('size_limit'))assert.equal(view.nodesConsumed,maxNodes);
 }
 const invalid=captureRequestContext(null);assert.equal(invalid.nodesConsumed,0);assert.deepEqual(await captureRequestContextAsync(null),invalid);
});

test('代码解释器与MCP历史保留可见执行信息且不要求虚构call_id',async()=>{
 const input=[{type:'code_interpreter_call',id:'synthetic-code',code:'print("synthetic")',container_id:'synthetic-container',outputs:[{type:'logs',logs:'合成日志'},{type:'image',url:'https://private.example/image.png'}],status:'completed'},
 {type:'mcp_call',id:'synthetic-mcp',name:'lookup',server_label:'synthetic',arguments:'{"query":"合成输入","api_key":"private-mcp-secret"}',approval_request_id:null,output:'合成输出',error:null,status:'completed'}];
 const original=structuredClone(input),view=captureRequestContext({input});assert.equal(view.state,'complete');assert.deepEqual(input,original);assert.deepEqual(await captureRequestContextAsync({input}),view);
 const [code,mcp]=view.components.map(item=>item.content.value);assert.equal(code.code,input[0].code);assert.equal(code.outputs[0].logs,'合成日志');assert.equal(code.outputs[1].content,'[附件数据已省略]');assert.equal(mcp.output,'合成输出');assert.equal(mcp.approval_request_id,null);assert.equal(mcp.error,null);assert.equal(Object.hasOwn(mcp,'call_id'),false);
 for(const secret of ['private-mcp-secret','private.example'])assert.ok(!JSON.stringify(view).includes(secret));
 const nullable=captureRequestContext({input:[{type:'code_interpreter_call',id:'code',code:null,outputs:null},{type:'mcp_call',id:'mcp',arguments:'{}',output:null}]});assert.equal(nullable.state,'complete');
 for(const logs of [['Authorization: Bearer ','private-split-secret'],['{"api_key":"','private-json-secret"}']]){
  const payload={input:[{type:'code_interpreter_call',outputs:logs.map(logs=>({type:'logs',logs}))}]},result=captureRequestContext(payload);for(const secret of ['private-split-secret','private-json-secret'])assert.ok(!JSON.stringify(result).includes(secret));assert.deepEqual(await captureRequestContextAsync(payload),result);
 }
 const optional=captureRequestContext({input:[{type:'shell_call_output',output:[],status:null,max_output_length:null}]});assert.equal(optional.state,'complete');assert.equal(optional.components[0].content.value.status,null);assert.equal(optional.components[0].content.value.max_output_length,null);
 for(const item of [{type:'code_interpreter_call',code:42},{type:'code_interpreter_call',outputs:{}},{type:'mcp_call',arguments:{}},{type:'mcp_call',output:[]}]){const result=captureRequestContext({input:[item]});assert.equal(result.state,'incomplete');assert.ok(result.reasons.includes('unsupported_content'));}
 for(const output of [null,42,'invalid',[],{type:'unknown',value:'private-hidden'},{type:'logs',logs:{}},{type:'logs',logs:'ok',image_url:'https://private.example/hidden',thinking:'private-opaque'}]){
  const payload={input:[{type:'code_interpreter_call',outputs:[output]}]},result=captureRequestContext(payload);assert.equal(result.state,'incomplete');for(const secret of ['private-hidden','private.example','private-opaque'])assert.ok(!JSON.stringify(result).includes(secret));assert.deepEqual(await captureRequestContextAsync(payload),result);
 }
});

test('shell与apply_patch历史保留可见命令补丁和结构化执行结果',async()=>{
 const input=[{type:'shell_call',id:'shell',call_id:'shell-id',action:{commands:['echo synthetic'],timeout_ms:1000,max_output_length:1024},status:'completed'},
 {type:'shell_call_output',call_id:'shell-id',max_output_length:1024,output:[{stdout:'合成输出',stderr:'',outcome:{type:'exit',exit_code:0}},{stdout:'',stderr:'合成超时',outcome:{type:'timeout'}}]},
 {type:'apply_patch_call',call_id:'patch-id',operation:{type:'update_file',path:'synthetic.txt',diff:'@@\n-old\n+new'},status:'completed'},
 {type:'apply_patch_call_output',call_id:'patch-id',status:'completed'}];
 const view=captureRequestContext({input});assert.equal(view.state,'complete');assert.deepEqual(view.components.map(item=>item.content.value),input);assert.deepEqual(await captureRequestContextAsync({input}),view);
 const filtered=captureRequestContext({input:[{type:'shell_call',action:{commands:['Authorization: Bearer private-shell-secret']}},{type:'apply_patch_call',operation:{type:'create_file',path:'synthetic.txt',diff:'Authorization: Bearer private-patch-secret'}}]});
 assert.equal(filtered.state,'complete');assert.ok(!JSON.stringify(filtered).includes('private-shell-secret'));assert.ok(!JSON.stringify(filtered).includes('private-patch-secret'));
 const malformed=captureRequestContext({input:[{type:'shell_call_output',output:[],max_output_length:-1}]});assert.equal(malformed.state,'incomplete');assert.ok(malformed.reasons.includes('unsupported_content'));
 for(const [type,key] of [['shell_call','action'],['apply_patch_call','operation']])for(const value of [null,42,'invalid',[]]){
  const payload={input:[{type,[key]:value}]},result=captureRequestContext(payload);assert.equal(result.state,'incomplete');assert.ok(result.reasons.includes('unsupported_content'));assert.equal(Object.hasOwn(result.components[0].content.value,key),false);assert.deepEqual(await captureRequestContextAsync(payload),result);
 }
});

test('工具上下文保留显式状态错误及耗时，未知耗时不推算且错误沿用脱敏与预算',async()=>{
 const input=['function_call','custom_tool_call','function_call_output','custom_tool_call_output'].map(type=>({type,call_id:'synthetic-call',status:'failed',error:{code:'synthetic-error',message:'Authorization: Bearer private-tool-secret'},duration_ms:12.5,elapsed_ms:0,...(type.endsWith('_output')?{output:'合成结果'}:type==='function_call'?{arguments:'{}'}:{input:'合成命令'})}));
 const original=structuredClone(input),view=captureRequestContext({input});
 assert.equal(view.state,'complete');assert.deepEqual(input,original);assert.ok(!JSON.stringify(view).includes('private-tool-secret'));
 for(const component of view.components){const value=component.content.value;assert.equal(value.status,'failed');assert.equal(value.duration_ms,12.5);assert.equal(value.elapsed_ms,0);assert.equal(value.error.code,'synthetic-error');}
 assert.deepEqual(await captureRequestContextAsync({input}),view);
 const absent=captureRequestContext({input:[{type:'function_call',call_id:'synthetic',arguments:'{}'}]}).components[0].content.value;
 assert.equal(Object.hasOwn(absent,'duration_ms'),false);assert.equal(Object.hasOwn(absent,'error'),false);
 for(const duration of [-1,'12',null,NaN,Infinity,{},[]]){const result=captureRequestContext({input:[{type:'custom_tool_call',input:'合成命令',duration_ms:duration}]});assert.equal(result.state,'incomplete');assert.ok(result.reasons.includes('unsupported_content'));assert.equal(Object.hasOwn(result.components[0].content.value,'duration_ms'),false);}
 const bounded=captureRequestContext({input:[{type:'function_call_output',output:'ok',error:{message:'x'.repeat(10000)}}]},{maxBytes:1024});assert.equal(bounded.state,'incomplete');assert.ok(bounded.reasons.includes('size_limit'));
});

test('协作与同步采集结构、脱敏及截断事实完全一致，并实际让出事件循环',async()=>{
 const payload={instructions:'合成说明',tools:[{type:'custom',name:'exec'}],input:[{role:'user',content:[{type:'input_text',text:'Authorization: Bearer '},{type:'input_text',text:'private-secret'}]},...Array.from({length:2000},()=>({role:'assistant',content:'合成历史'}))]};
 for(const options of [{},{maxNodes:80},{maxComponents:12},{maxBytes:1024}]){
  let yielded=false;const tick=new Promise(resolve=>setImmediate(()=>{yielded=true;resolve();}));
  const actual=await captureRequestContextAsync(payload,{...options,sliceMs:Number.MIN_VALUE});assert.equal(yielded,true);await tick;
  assert.deepEqual(actual,captureRequestContext(payload,options));assert.ok(!JSON.stringify(actual).includes('private-secret'));
 }
});

test('协作采集在开始前和分段期间响应取消，非法时间片拒绝',async()=>{
 for(const sliceMs of [0,-1,NaN,Infinity,'5',null])await assert.rejects(captureRequestContextAsync({}, {sliceMs}),TypeError);
 const before=new AbortController();before.abort();await assert.rejects(captureRequestContextAsync({}, {signal:before.signal}),{name:'AbortError'});
 const during=new AbortController(),interrupt=new Promise(resolve=>setImmediate(()=>{during.abort();resolve();}));
 await assert.rejects(captureRequestContextAsync({input:Array.from({length:2000},()=>({role:'user',content:'合成历史'}))},{sliceMs:Number.MIN_VALUE,signal:during.signal}),{name:'AbortError'});await interrupt;
});

test('内联工具目录保留来源与命名空间，嵌套schema仍只保留定义而不泄漏敏感默认值',()=>{
 const input=[{type:'additional_tools',id:'catalog',role:'developer',tools:[{type:'namespace',name:'functions',tools:[{type:'function',name:'lookup',parameters:{type:'object',properties:{api_key:{type:'string',default:'private-default'},query:{type:'string'}}}},{type:'custom',name:'exec',format:{type:'grammar',syntax:'lark',definition:'start: "ok"'}}]}]},
 {type:'message',role:'assistant',phase:'commentary',content:[{type:'output_text',text:'准备查询',annotations:[],logprobs:[]}]},
 {type:'custom_tool_call',id:'tool',call_id:'call',name:'exec',namespace:'functions',input:'合成命令'},
 {type:'custom_tool_call_output',call_id:'call',namespace:'functions',output:'合成结果'}];
 const original=structuredClone(input),view=captureRequestContext({input,tool_choice:{type:'function',name:'lookup',namespace:'functions'}});
 assert.equal(view.state,'complete');assert.equal(view.components[0].kind,'tool_schema');assert.equal(view.components[0].content.source,'input');assert.equal(view.components[0].content.index,0);
 const catalog=view.components[0].content.value;assert.equal(catalog.type,'additional_tools');assert.equal(catalog.tools[0].tools[0].parameters.properties.api_key.type,'string');assert.ok(!JSON.stringify(view).includes('private-default'));assert.deepEqual(input,original);
 assert.equal(view.components[1].content.value.phase,'commentary');assert.equal(view.components[2].content.value.namespace,'functions');assert.equal(view.components[3].content.value.namespace,'functions');assert.equal(view.components.at(-1).content.value.namespace,'functions');
 for(const tools of [null,{},[null],[1]])assert.equal(captureRequestContext({input:[{type:'additional_tools',role:'developer',tools}]}).state,'incomplete');
 for(const input of [[{role:'assistant',phase:{data:'private-phase'},content:'x'}],[{type:'custom_tool_call',call_id:'c',namespace:{data:'private-namespace'},input:'x'}]]){const result=captureRequestContext({input});assert.equal(result.state,'incomplete');assert.ok(!JSON.stringify(result).includes('private-'));}
 const nullable=[{role:'assistant',phase:null,content:[{type:'output_text',text:'x',annotations:null,logprobs:null}]},{type:'custom_tool_call',call_id:'c',namespace:null,input:'x'}];const result=captureRequestContext({input:nullable});assert.equal(result.state,'complete');assert.deepEqual(result.components.map(item=>item.content.value),nullable);
});

test('正文引用与概率元信息有类型边界，token连续脱敏且原始字节不能绕过过滤',()=>{
 const part={type:'output_text',text:'公开正文',annotations:[{type:'url_citation',url:'https://private.example/?token=private-url',title:'公开来源',start_index:0,end_index:2},{type:'file_citation',file_id:'private-reference',filename:'report.txt',index:0}],
  logprobs:[{token:'sk-',logprob:-0.5,bytes:[115,107,45],top_logprobs:[{token:' ',logprob:-1,bytes:[32]}]},{token:'synthetic-secret',logprob:-0.3,bytes:[1,2,3],top_logprobs:[]}]};
 const view=captureRequestContext({input:[{role:'assistant',phase:'final',content:[part]}]});assert.equal(view.state,'complete');const saved=view.components[0].content.value.content[0];
 assert.equal(saved.annotations[0].title,'公开来源');assert.equal(saved.annotations[1].filename,'report.txt');assert.equal(saved.annotations[0].url,'[引用已省略]');assert.equal(saved.logprobs[0].logprob,-0.5);assert.deepEqual(saved.logprobs[0].bytes,{omitted:true,elements:3});
 for(const secret of ['synthetic-secret','private.example','private-url','private-reference','115,107,45'])assert.ok(!JSON.stringify(view).includes(secret));
 const ordinary=captureRequestContext({input:[{role:'assistant',content:[{type:'output_text',text:'hello',logprobs:[{token:'hello',logprob:-1,top_logprobs:[{token:'world',logprob:-2}]}]}]}]});assert.equal(ordinary.components[0].content.value.content[0].logprobs[0].token,'hello');
 const malformed=[{annotations:{type:'url_citation'}},{annotations:[{type:['url_citation'],title:'private-hidden'}]},{annotations:[{type:'url_citation',title:{data:'private-hidden'}}]},{annotations:[{type:'file_citation',file_id:'private-reference',file_data:'private-hidden'}]},{logprobs:[{token:{data:'private-hidden'},bytes:{data:'private-hidden'}}]},{logprobs:[{token:'ok',logprob:{data:'private-hidden'},extra:'private-hidden'}]}];
 for(const extra of malformed){const result=captureRequestContext({input:[{role:'assistant',content:[{type:'output_text',text:'body',...extra}]}]});assert.equal(result.state,'incomplete');assert.ok(!JSON.stringify(result).includes('private-hidden'));}
 const bounded=captureRequestContext({input:[{role:'assistant',content:[{type:'output_text',text:'body',logprobs:Array.from({length:1000},()=>({token:'word',logprob:-1,top_logprobs:[]}))}]}]},{maxNodes:60});assert.ok(bounded.reasons.includes('size_limit'));
});

test('上下文保存可见指令、schema、完整输入顺序和协议续接，不冒充本轮或执行证明',()=>{
 const payload={instructions:'系统说明',tools:[{type:'function',name:'read',parameters:{type:'object'}}],input:[{role:'developer',content:'开发者说明'},{role:'user',content:'重复问题'},{role:'assistant',content:'历史回答'},{role:'user',content:'重复问题'},{type:'custom_tool_call',call_id:'c1',input:'cat a\n'},{type:'custom_tool_call_output',call_id:'c1',output:'  原始结果\n'}],previous_response_id:'previous'};
 const result=captureRequestContext(payload);assert.equal(result.state,'complete');assert.equal(result.components.length,9);
 assert.deepEqual(result.components.slice(2,8).map(row=>row.content.value),payload.input);assert.equal(result.components[2].kind,'instruction');assert.equal(result.components[3].kind,'context');
 assert.equal(result.components[5].content.value.content,'重复问题');assert.deepEqual(result.components.at(-1).content,{source:'previous_response_id',value:'previous'});
 assert.equal(Buffer.byteLength(JSON.stringify(result.components)),result.bytes);
});
test('进入缓冲前脱敏，排除附件地址和不透明内容，原请求不被修改',()=>{
 const payload={instructions:'Authorization: Bearer synthetic-secret-value',input:[{role:'user',content:[{type:'input_image',image_url:'https://private.example/image?token=private',bytes:50},{type:'input_text',text:'说明'}]},{type:'reasoning',id:'r',encrypted_content:'opaque-private-data',summary:[{type:'summary_text',text:'公开摘要'}]},{type:'compaction',encrypted_content:'opaque-compaction'}]};
 const before=JSON.stringify(payload),result=captureRequestContext(payload),text=JSON.stringify(result);
 for(const value of ['synthetic-secret-value','private.example','opaque-private-data','opaque-compaction'])assert.ok(!text.includes(value));
 assert.match(text,/公开摘要/);assert.equal(result.opaqueReasoningExcluded,2);assert.equal(JSON.stringify(payload),before);assert.equal(result.state,'complete');
});
test('已知原始推理字段只计排除而非未支持，公开摘要保留且不读取原始值',()=>{
 for(const content of [null,[],[{type:'reasoning_text',text:'SYNTHETIC_PRIVATE_REASONING'}]]){
  const result=captureRequestContext({input:[{type:'reasoning',content,encrypted_content:'synthetic-opaque',summary:[{type:'summary_text',text:'公开摘要'}]}]});assert.equal(result.state,'complete');assert.equal(result.opaqueReasoningExcluded,1);assert.deepEqual(result.reasons,[]);assert.match(JSON.stringify(result),/公开摘要/);assert.ok(!JSON.stringify(result).includes('SYNTHETIC_PRIVATE_REASONING'));assert.ok(!JSON.stringify(result).includes('synthetic-opaque'));
 }
 const item={type:'reasoning',summary:[]};Object.defineProperty(item,'content',{enumerable:true,get(){assert.fail('raw reasoning must not be read');}});assert.equal(captureRequestContext({input:[item]}).state,'complete');
 const unknown=captureRequestContext({input:[{type:'reasoning',content:[],summary:[],future_field:'SYNTHETIC_UNKNOWN'}]});assert.equal(unknown.state,'incomplete');assert.deepEqual(unknown.reasons,['unsupported_field']);assert.ok(!JSON.stringify(unknown).includes('SYNTHETIC_UNKNOWN'));
});
test('未知输入和非法结构明确不完整，不静默通过；大小、深度、组件和遍历预算有界',()=>{
 assert.deepEqual(captureRequestContext(null).reasons,['invalid_payload']);
 assert.deepEqual(captureRequestContext({input:[{type:'new_protocol'}],tools:{}}).reasons,['unsupported_input','unsupported_tools']);
 for(const options of [{maxBytes:256},{maxComponents:1},{maxNodes:2}]){
  const result=captureRequestContext({instructions:'说明',input:Array.from({length:30},()=>({role:'user',content:'中文🙂'.repeat(100)}))},options);
  assert.equal(result.state,'incomplete');assert.ok(result.reasons.includes('size_limit'));assert.ok(result.bytes<=(options.maxBytes??2*1024*1024));assert.equal(result.bytes,Buffer.byteLength(JSON.stringify(result.components)));
 }
 let nested='末端';for(let i=0;i<40;i++)nested={value:nested};assert.equal(captureRequestContext({instructions:nested}).state,'incomplete');
 for(const options of [{maxBytes:0},{maxComponents:0},{maxNodes:0}])assert.throws(()=>captureRequestContext({},options),TypeError);
});
test('无输入和纯文本输入、重复工具配置均保留真实出现次数',()=>{
 assert.deepEqual(captureRequestContext({}).components,[]);const tool={type:'custom',name:'exec'};
 const result=captureRequestContext({input:'问题\n代码',tools:[tool,tool]});assert.equal(result.state,'complete');assert.equal(result.components.length,3);assert.deepEqual(result.components[0].content.value,result.components[1].content.value);assert.equal(result.components[2].content.value,'问题\n代码');
});
test('跨文本片段连续脱敏，未知附件与隐藏字段不进入缓冲',()=>{
 const result=captureRequestContext({input:[{role:'user',content:[{type:'input_text',text:'Authorization: Bearer '},{type:'input_text',text:'synthetic-secret-value'}]},{role:'assistant',content:'公开',reasoning_content:'hidden-private-text'},{role:'user',content:[{type:'future_attachment',url:'https://private.example/ticket',data:'private-binary'}]}]});
 const text=JSON.stringify(result);for(const secret of ['synthetic-secret-value','hidden-private-text','private.example','private-binary'])assert.ok(!text.includes(secret));assert.ok(text.includes('公开'));assert.equal(result.state,'incomplete');assert.ok(result.reasons.includes('unsupported_content'));
});
test('Chat消息与附件元数据保留，但文件引用和下载地址不保存',()=>{
 const result=captureRequestContext({messages:[{role:'system',content:'系统说明'},{role:'user',content:[{type:'input_file',filename:'example.txt',mime_type:'text/plain',bytes:10,file_id:'private-reference',file_data:'private-binary'}]}]});
 assert.equal(result.state,'complete');assert.equal(result.components[0].kind,'instruction');assert.equal(result.components[0].content.source,'messages');const text=JSON.stringify(result);assert.match(text,/example.txt/);assert.match(text,/text\/plain/);assert.ok(!text.includes('private-reference'));assert.ok(!text.includes('private-binary'));
});
test('未知项同样消耗预算，混排与未知结构不可绕过',()=>{
 let reads=0;const input=new Proxy(Array.from({length:10000},()=>({type:'future'})),{get(target,key){if(/^\d+$/.test(String(key)))reads++;return Reflect.get(target,key);}});
 const result=captureRequestContext({input},{maxNodes:3});assert.ok(reads<=3);assert.ok(result.reasons.includes('size_limit'));
 assert.equal(captureRequestContext({input:[{type:'future',role:'assistant',content:'unknown'}]}).state,'incomplete');
 const schema={type:'function',name:'f',parameters:{type:'object',properties:{value:{type:'string'}}}};assert.deepEqual(captureRequestContext({tools:[schema]}).components[0].content.value,schema);
});
test('正文结构按位置过滤，合法文本额外字段和无类型块不得夹带附件',()=>{
 const result=captureRequestContext({input:[{role:'user',content:[{type:'input_text',text:'公开',data:'private-binary',url:'https://private.example/ticket'},{reasoning:'hidden-private-text'}]}]});
 const text=JSON.stringify(result);for(const secret of ['private-binary','private.example','hidden-private-text'])assert.ok(!text.includes(secret));assert.ok(text.includes('公开'));assert.equal(result.state,'incomplete');
 const wide={role:'user',content:'公开'};for(let i=0;i<1000;i++)wide['unknown'+i]='private';assert.ok(captureRequestContext({input:[wide]},{maxNodes:30}).reasons.includes('size_limit'));
});
test('Chat嵌套文件保留名称，schema敏感参数定义保留而运行值脱敏',()=>{
 const result=captureRequestContext({tools:[{type:'function',name:'f',parameters:{type:'object',properties:{api_key:{type:'string',description:'请输入凭据',default:'private-default'},password:{type:'string',examples:['private-example']}}}}],messages:[{role:'user',content:[{type:'file',file:{filename:'nested.txt',file_id:'private-reference',file_data:'private-binary'}}]}]});
 assert.equal(result.state,'complete');const properties=result.components[0].content.value.parameters.properties;assert.equal(properties.api_key.type,'string');assert.equal(properties.password.type,'string');const text=JSON.stringify(result);assert.match(text,/nested.txt/);assert.match(text,/referencePresent":true/);for(const secret of ['private-default','private-example','private-reference','private-binary'])assert.ok(!text.includes(secret));
});
test('schema权限仅用于真实定义，敏感上下文贯穿组合定义及数组，碰撞明确不完整',()=>{
 const result=captureRequestContext({tools:[{type:'function',name:'f',parameters:{type:'object',properties:{password:{anyOf:[{type:'string',default:'private-nested',examples:['private-example']}]}}}}],input:[{type:'function_call_output',call_id:'c',output:{properties:{api_key:'private-runtime'}}}]});
 assert.equal(result.state,'complete');const text=JSON.stringify(result);for(const secret of ['private-nested','private-example','private-runtime'])assert.ok(!text.includes(secret));assert.equal(result.components[0].content.value.parameters.properties.password.anyOf[0].type,'string');
 const collision=captureRequestContext({tools:[{type:'function',name:'f',parameters:{properties:{'sk-one':{type:'string'},'sk-two':{type:'number'}}}}]});assert.equal(collision.state,'incomplete');assert.ok(collision.reasons.includes('schema_name_collision'));
});
test('文本、拒绝和公开摘要的叶子只接受字符串，不通过对象载入隐藏数据',()=>{
 for(const type of ['input_text','output_text','summary_text','refusal']){
  const part={type,[type==='refusal'?'refusal':'text']:{reasoning:'private-hidden',url:'https://private.example',data:'private-binary'}},result=captureRequestContext({input:[{role:'user',content:[part]}]});
  assert.equal(result.state,'incomplete');for(const secret of ['private-hidden','private.example','private-binary'])assert.ok(!JSON.stringify(result).includes(secret));
 }
});
test('单段大文本超过预算仍保存可用前缀而非丢掉全部正文',()=>{
 const result=captureRequestContext({input:'中文🙂'.repeat(1000)},{maxBytes:512});assert.equal(result.state,'incomplete');assert.ok(result.components.length>0);assert.ok(JSON.stringify(result.components).includes('中文🙂'));assert.ok(result.bytes<=512);
});
test('所有正文容器按协议验证，非法指令、拒绝和裸文本数组不能夹带数据',()=>{
 for(const payload of [{instructions:{reasoning:'private-hidden',data:'private-binary'}},{messages:[{role:'assistant',refusal:{data:'private-binary'}}]},{messages:[{role:'user',content:['Authorization: Bearer ','private-secret']}]}]){
  const result=captureRequestContext(payload);assert.equal(result.state,'incomplete');for(const secret of ['private-hidden','private-binary','private-secret'])assert.ok(!JSON.stringify(result).includes(secret));
 }
});
test('协议正文前缀保留，输入自带截断字段不导致异常，安全识别URL附件引用',()=>{
 for(const type of ['input_text','output_text','summary_text']){
  const result=captureRequestContext({input:[{role:'user',content:[{type,text:'中文🙂'.repeat(1000)}]}]},{maxBytes:512});assert.equal(result.state,'incomplete');assert.match(JSON.stringify(result),/中文🙂/);
 }
 const output={recording_truncated:'原有字段'};for(let i=0;i<2000;i++)output['field'+i]='value';assert.equal(captureRequestContext({input:[{type:'function_call_output',output}]},{maxNodes:100}).state,'incomplete');
 const result=captureRequestContext({input:[{role:'user',content:[{type:'input_file',file_url:'https://private.example/file'},{type:'image_url',image_url:{url:'https://private.example/image'}}]}]});assert.equal(result.state,'complete');assert.equal(result.components[0].content.value.content.filter(part=>part.referencePresent).length,2);assert.ok(!JSON.stringify(result).includes('private.example'));
});
test('工具结果复用原采集过滤器，JSON可见结构不泄漏跨片凭据和附件',()=>{
 const output=JSON.stringify({content:[{type:'input_text',text:'Authorization: Bearer '},{type:'input_text',text:'private-tool-secret'},{type:'input_file',file_data:'private-binary',file_url:'https://private.example'}]});
 const result=captureRequestContext({input:[{type:'function_call_output',call_id:'c',output}]});assert.equal(result.state,'complete');for(const secret of ['private-tool-secret','private-binary','private.example'])assert.ok(!JSON.stringify(result).includes(secret));
});
test('嵌套工具协议和续接对象使用各自位置白名单，未知摘要容器明确不完整',()=>{
 const result=captureRequestContext({messages:[{role:'assistant',tool_calls:[{type:'function',id:'c',function:{name:'f',arguments:'{}',reasoning:'private-hidden',data:'private-binary',file_url:'https://private.example'}}]}],conversation:{id:'conversation',data:'private-binary'}});
 assert.equal(result.state,'incomplete');const text=JSON.stringify(result);assert.ok(text.includes('arguments'));for(const secret of ['private-hidden','private-binary','private.example'])assert.ok(!text.includes(secret));
 for(const summary of ['公开摘要',{type:'summary_text',text:'公开摘要'}])assert.equal(captureRequestContext({input:[{type:'reasoning',summary}]}).state,'incomplete');
});
test('拒绝文本连续脱敏，消息元数据与工具参数不能注入对象',()=>{
 const refused=captureRequestContext({input:[{role:'assistant',content:[{type:'refusal',refusal:'Authorization: Bearer '},{type:'refusal',refusal:'private-refusal-secret'}]}]});assert.equal(refused.state,'complete');assert.ok(!JSON.stringify(refused).includes('private-refusal-secret'));
 for(const input of [[{role:'user',content:'公开',name:{data:'private-binary'}}],[{type:'function_call',arguments:{data:'private-binary'}}],[{role:'assistant',tool_calls:{type:'function',function:{name:'f'}}}]]){const result=captureRequestContext({input});assert.equal(result.state,'incomplete');assert.ok(!JSON.stringify(result).includes('private-binary'));}
 assert.equal(captureRequestContext({conversation:'conversation-id'}).state,'complete');
});
test('工具过滤与请求共享预算，所有工具路径保留安全附件元数据',()=>{
 let reads=0;const make=()=>new Proxy(Object.fromEntries(Array.from({length:30},(_,i)=>['v'+i,i])),{get(target,key){if(/^v\d+$/.test(String(key)))reads++;return Reflect.get(target,key);}});
 const limited=captureRequestContext({input:Array.from({length:5},()=>({type:'function_call_output',output:make()}))},{maxNodes:100});assert.equal(limited.state,'incomplete');assert.ok(limited.reasons.includes('size_limit'));assert.ok(reads<100);
 const part={type:'file',file:{filename:'tool.txt',mime_type:'text/plain',size:10,file_id:'private-reference',file_data:'private-binary'}};
 for(const input of [[{role:'tool',content:[part]}],[{type:'function_call_output',output:[part]}],[{type:'custom_tool_call_output',output:JSON.stringify({content:[part]})}]]){
  const result=captureRequestContext({input});assert.equal(result.state,'complete');const text=JSON.stringify(result);assert.match(text,/tool.txt/);assert.match(text,/text\/plain/);assert.match(text,/referencePresent/);assert.ok(!text.includes('private-reference'));assert.ok(!text.includes('private-binary'));
 }
});
test('按具体消息种类区分字段，协议元数据和工具定义容器不能夹带任意对象',()=>{
 for(const payload of [{input:[{role:'user',output:{data:'private-binary'}}]},{input:[{type:'function_call',output:{data:'private-binary'}}]},{previous_response_id:{data:'private-binary'}},{parallel_tool_calls:{data:'private-binary'}},{tool_choice:[{type:'function',name:'f'}]},{tools:['private-binary']}]){
  const result=captureRequestContext(payload);assert.equal(result.state,'incomplete');assert.ok(!JSON.stringify(result).includes('private-binary'));
 }
});
test('同一Chat消息的content和refusal共同过滤，保持原字段位置和正常正文',()=>{
 for(const content of ['Authorization: Bearer ',[{type:'text',text:'Authorization: Bearer '}]]){
  const result=captureRequestContext({messages:[{role:'assistant',content,refusal:'private-chat-secret'}]});assert.equal(result.state,'complete');assert.ok(!JSON.stringify(result).includes('private-chat-secret'));
 }
 const result=captureRequestContext({messages:[{role:'assistant',content:'正常内容',refusal:'正常拒绝'}]});assert.equal(result.components[0].content.value.content,'正常内容');assert.equal(result.components[0].content.value.refusal,'正常拒绝');
});
test('每种文本片段仅允许其正文键，备用正文键不得绕过连续脱敏',()=>{
 for(const type of ['input_text','output_text','summary_text','refusal']){
  const field=type==='refusal'?'refusal':'text',extra=field==='text'?'refusal':'text',result=captureRequestContext({input:[{role:'assistant',content:[{type,[field]:'Authorization: Bearer ',[extra]:'private-extra-secret'}]}]});
  assert.equal(result.state,'incomplete');assert.ok(result.reasons.includes('unsupported_field'));assert.ok(!JSON.stringify(result).includes('private-extra-secret'));
 }
});
test('原始stdout保持业务文本契约，不因JSON外形自动升级成协议片段',()=>{
 const raw='{ "content": [{"type":"future_visible_part","data":"visible future business value"}], "value":90071992547409931234567 }';
 const result=captureRequestContext({input:[{type:'custom_tool_call_output',output:raw}]});assert.equal(result.state,'complete');assert.equal(result.components[0].content.value.output,raw);
 const structured=captureRequestContext({input:[{type:'custom_tool_call_output',output:JSON.parse(raw)}]});assert.equal(structured.state,'incomplete');assert.ok(structured.reasons.includes('unsupported_content'));
});
test('schema字面量没有定义豁免，枚举和默认值保留所有普通业务字段',()=>{
 const ordinary={type:'text',text:'visible',weight:3},properties={example:{type:'object',default:{properties:{api_key:'private-default'}},examples:[{properties:{password:'private-example'}}],const:{properties:{token:'private-const'}},enum:[ordinary,{properties:{api_key:'private-enum'}}]}};
 const result=captureRequestContext({tools:[{type:'function',name:'f',parameters:{type:'object',properties}}]});assert.equal(result.state,'complete');const restored=result.components[0].content.value.parameters.properties.example;assert.deepEqual(restored.enum[0],ordinary);for(const secret of ['private-default','private-example','private-const','private-enum'])assert.ok(!JSON.stringify(result).includes(secret));
 const crypto=new RecordingStore(null,{encryptionKey:randomBytes(32)}),roundtrip=crypto.open(crypto.seal(result,'synthetic-literal'),'synthetic-literal');assert.deepEqual(roundtrip,result);
 const dependent={type:'object',dependentRequired:{api_key:['token']},dependencies:{password:['name']}};assert.deepEqual(captureRequestContext({tools:[{type:'function',name:'f',parameters:dependent}]}).components[0].content.value.parameters,dependent);
});

 test('概率token跨正文块连续脱敏，互斥候选不能用空白截断过滤状态',()=>{
 const parts=[{type:'output_text',text:'Authorization: Bearer ',logprobs:[{token:'Authorization: Bearer ',logprob:-1}]},{type:'output_text',text:'synthetic-suffix',logprobs:[{token:'synthetic-suffix',logprob:-1}]}];
 const result=captureRequestContext({input:[{role:'assistant',content:parts}]});assert.equal(result.state,'complete');assert.ok(!JSON.stringify(result).includes('synthetic-suffix'));
 const candidates=captureRequestContext({input:[{role:'assistant',content:[{type:'output_text',text:'safe',logprobs:[{token:'ok',top_logprobs:[{token:'sk-'},{token:' '}]},{token:'ok',top_logprobs:[{token:'synthetic-suffix'}]}]}]}]});assert.equal(candidates.state,'complete');assert.ok(!JSON.stringify(candidates).includes('synthetic-suffix'));assert.equal(candidates.components[0].content.value.content[0].logprobs[0].top_logprobs[0].token,'[已脱敏]');
 });

 test('概率序列预算缺口不泄漏跨块后缀，候选省略不消耗实际token预算',()=>{
 const payload={input:[{role:'assistant',content:[{type:'output_text',text:'safe',logprobs:[{token:'sk',top_logprobs:[{token:'x'.repeat(3000)}]},{token:'-'}]},{type:'output_text',text:'safe',logprobs:[{token:'synthetic-private-suffix'}]}]}]};
 const complete=captureRequestContext(payload,{maxBytes:2000});assert.equal(complete.state,'complete');assert.ok(!JSON.stringify(complete).includes('synthetic-private-suffix'));
 payload.input[0].content[0].logprobs[0].token='sk'+ 'x'.repeat(3000);
 const cut=captureRequestContext(payload,{maxBytes:2000});assert.equal(cut.state,'incomplete');assert.ok(cut.reasons.includes('size_limit'));assert.ok(!JSON.stringify(cut).includes('synthetic-private-suffix'));
 const independent=captureRequestContext({input:[...payload.input,{role:'assistant',content:[{type:'output_text',text:'ordinary',logprobs:[{token:'ordinary',logprob:-1}]}]}]},{maxBytes:2000});assert.equal(independent.components.at(-1).content.value.content[0].logprobs[0].token,'ordinary');
 });

 test('消息正文概率缺省或不对齐时整组token脱敏，合法空值不使上下文失败',()=>{
 for(const metadata of [{},{logprobs:null},{logprobs:[]}]){
  const result=captureRequestContext({input:[{role:'assistant',content:[{type:'output_text',text:'Authorization: Bearer ',...metadata},{type:'output_text',text:'synthetic-gap-suffix',logprobs:[{token:'synthetic-gap-suffix'}]}]},{role:'assistant',content:[{type:'output_text',text:'ordinary',logprobs:[{token:'ordinary'}]}]}]});assert.equal(result.state,'complete');assert.ok(!JSON.stringify(result).includes('synthetic-gap-suffix'));assert.equal(result.components.at(-1).content.value.content[0].logprobs[0].token,'ordinary');
 }
 for(const first of [{type:'text',text:'Authorization: Bearer '},{type:'refusal',refusal:'Authorization: Bearer '}]){const result=captureRequestContext({input:[{role:'assistant',content:[first,{type:'output_text',text:'synthetic-mixed-suffix',logprobs:[{token:'synthetic-mixed-suffix'}]}]}]});assert.ok(!JSON.stringify(result).includes('synthetic-mixed-suffix'));}
 const invalid=captureRequestContext({input:[{role:'assistant',content:[{type:'output_text',text:'Authorization: Bearer synthetic-invalid-suffix',logprobs:[{}, {token:'synthetic-invalid-suffix'}]}]}]});assert.equal(invalid.state,'incomplete');assert.ok(invalid.reasons.includes('unsupported_content'));assert.ok(!JSON.stringify(invalid).includes('synthetic-invalid-suffix'));
 const mismatch=captureRequestContext({input:[{role:'assistant',content:[{type:'output_text',text:'Authorization: Bearer synthetic-mismatch-suffix',logprobs:[{token:'synthetic-mismatch-suffix'}]}]}]});assert.ok(!JSON.stringify(mismatch).includes('synthetic-mismatch-suffix'));
 });

test('MCP目录与审批历史保留官方字段和关联，工具schema复用定义脱敏且不推造call_id',async()=>{
 const input=[{type:'mcp_list_tools',id:'list-synthetic',server_label:'synthetic',tools:[{name:'lookup',description:null,annotations:{readOnlyHint:true,api_key:'private-annotation'},input_schema:{type:'object',properties:{api_key:{type:'string',default:'private-default',anyOf:[{type:'string',enum:['private-enum']}]},query:{type:'string',default:'ordinary'}}}}],error:null},
 {type:'mcp_approval_request',id:'approval-synthetic',name:'lookup',server_label:'synthetic',arguments:'{"query":"普通查询","api_key":"private-argument"}'},
 {type:'mcp_approval_response',approval_request_id:'approval-synthetic',approve:false,id:null,reason:null},
 {type:'mcp_approval_response',approval_request_id:'approval-synthetic',approve:true,id:'decision-synthetic',reason:'Authorization: Bearer private-reason'}];
 const original=structuredClone(input),view=captureRequestContext({input});assert.equal(view.state,'complete');assert.deepEqual(input,original);assert.deepEqual(await captureRequestContextAsync({input},{sliceMs:Number.MIN_VALUE}),view);
 const [list,request,denied,approved]=view.components.map(row=>row.content.value);assert.deepEqual(view.components.map(row=>row.content.index),[0,1,2,3]);assert.ok(view.components.every(row=>row.kind==='context'));assert.equal(list.server_label,'synthetic');assert.equal(list.error,null);assert.equal(list.tools[0].description,null);assert.equal(list.tools[0].input_schema.properties.api_key.type,'string');assert.equal(list.tools[0].input_schema.properties.api_key.anyOf[0].type,'string');assert.equal(list.tools[0].input_schema.properties.query.default,'ordinary');assert.equal(list.tools[0].annotations.readOnlyHint,true);assert.equal(request.id,denied.approval_request_id);assert.equal(denied.approve,false);assert.equal(approved.approve,true);assert.equal(denied.id,null);assert.equal(denied.reason,null);assert.equal(approved.id,'decision-synthetic');for(const row of [list,request,denied,approved])assert.equal(Object.hasOwn(row,'call_id'),false);
 for(const secret of ['private-annotation','private-default','private-enum','private-argument','private-reason'])assert.ok(!JSON.stringify(view).includes(secret));
 const absent=captureRequestContext({input:[{type:'mcp_approval_response',approval_request_id:'approval-synthetic',approve:true},{type:'mcp_list_tools',id:'list',server_label:'synthetic',tools:[{name:'boolean-schema',input_schema:false}]}]});assert.equal(absent.state,'complete');assert.equal(absent.components[1].content.value.tools[0].input_schema,false);assert.equal(Object.hasOwn(absent.components[0].content.value,'id'),false);
});

test('MCP目录和审批拒绝畸形字段，隐藏字段不落库且预算与异步边界一致',async()=>{
 const invalid=[{type:'mcp_list_tools',tools:[{type:'reasoning',summary:[{type:'summary_text',text:'private-hidden'}]}]},{type:'mcp_list_tools',tools:[{type:'image',url:'https://private-hidden.example/image'}]},{type:'mcp_list_tools',tools:{}},{type:'mcp_list_tools',tools:[null]},{type:'mcp_list_tools',tools:[{name:3,input_schema:{}}]},{type:'mcp_list_tools',tools:[{name:'lookup',description:{value:'private-hidden'}}]},{type:'mcp_list_tools',error:{value:'private-hidden'}},{type:'mcp_approval_request',arguments:{value:'private-hidden'}},{type:'mcp_approval_response',approve:'false'},{type:'mcp_approval_response',approve:1},{type:'mcp_approval_response',approval_request_id:null},{type:'mcp_approval_response',id:7},{type:'mcp_approval_response',reason:{value:'private-hidden'}},{type:'mcp_list_tools',tools:[{name:'lookup',input_schema:{},runtime_value:'private-hidden'}]},{type:'mcp_approval_response',approve:true,call_id:'private-hidden'}];
 for(const item of invalid){const result=captureRequestContext({input:[item]});assert.equal(result.state,'incomplete',JSON.stringify(item));assert.ok(result.reasons.some(reason=>['unsupported_content','unsupported_field'].includes(reason)));assert.ok(!JSON.stringify(result).includes('private-hidden'));assert.deepEqual(await captureRequestContextAsync({input:[item]}),result);}
 const opaque=captureRequestContext({input:[{type:'mcp_approval_request',id:'request',arguments:'{}',encrypted_content:'private-opaque',reasoning_content:'private-hidden'}]});assert.equal(opaque.opaqueReasoningExcluded,2);assert.ok(!JSON.stringify(opaque).includes('private-'));
 const payload={input:[{type:'mcp_list_tools',id:'list',server_label:'synthetic',tools:Array.from({length:100},(_,i)=>({name:'tool-'+i,input_schema:{type:'object',properties:{secret:{type:'string',default:'private-default'}}}}))}]};for(const options of [{maxNodes:60},{maxBytes:512}]){const view=captureRequestContext(payload,options);assert.equal(view.state,'incomplete');assert.ok(view.reasons.includes('size_limit'));assert.ok(!JSON.stringify(view).includes('private-default'));assert.deepEqual(await captureRequestContextAsync(payload,options),view);}
});

test('搜索和计算机历史按官方动作字段保留可见事实，引用截图仅元数据',async()=>{
 const input=[{type:'web_search_call',id:'web',status:'completed',action:{type:'search',query:'公开查询',queries:['查询一','查询二'],sources:[{type:'url',url:'https://private-web.example/?token=private-url'}]}},
 {type:'web_search_call',id:'open',status:'completed',action:{type:'open_page',url:null}},{type:'web_search_call',id:'find',status:'completed',action:{type:'find_in_page',pattern:'公开模式',url:'https://private-web.example'}},
 {type:'file_search_call',id:'file',status:'completed',queries:['公开文件'],results:[{attributes:{role:'业务值',count:2,approved:false,api_key:'private-attribute'},file_id:'private-file-id',filename:'sample.txt',score:0.9,text:'检索正文'}]},
 {type:'computer_call',id:'computer',call_id:'real-call',status:'completed',pending_safety_checks:[{id:'check',code:null,message:'公开安全检查'}],action:{type:'click',button:'left',x:1,y:2,keys:null},actions:[{type:'double_click',x:2,y:3,keys:[]},{type:'drag',path:[{x:1,y:2},{x:3,y:4}]},{type:'keypress',keys:['CTRL','A']},{type:'move',x:4,y:5},{type:'screenshot'},{type:'scroll',x:1,y:2,scroll_x:0,scroll_y:12},{type:'type',text:'公开输入'},{type:'wait'}]},
 {type:'computer_call_output',call_id:'real-call',id:null,status:null,output:{type:'computer_screenshot',image_url:'https://private-image.example/shot',file_id:'private-shot-id'},acknowledged_safety_checks:[{id:'check',code:null,message:null}]}];
 const original=structuredClone(input),view=captureRequestContext({input});assert.equal(view.state,'complete');assert.deepEqual(await captureRequestContextAsync({input}),view);assert.deepEqual(input,original);const rows=view.components.map(row=>row.content.value);assert.equal(rows[0].action.query,'公开查询');assert.equal(rows[0].action.sources[0].url,'[引用已省略]');assert.equal(rows[1].action.url,null);assert.equal(rows[2].action.pattern,'公开模式');assert.equal(rows[3].results[0].text,'检索正文');assert.equal(rows[3].results[0].attributes.role,'业务值');assert.equal(rows[3].results[0].file_id,'[引用已省略]');assert.equal(rows[4].call_id,rows[5].call_id);assert.equal(rows[4].actions[1].path[1].y,4);assert.equal(rows[4].actions[6].text,'公开输入');assert.equal(rows[5].output.attachmentType,'computer_screenshot');assert.equal(rows[5].output.referencePresent,true);assert.equal(rows[5].output.content,'[附件数据已省略]');assert.equal(rows[5].id,null);assert.equal(rows[5].status,null);assert.equal(rows[5].acknowledged_safety_checks[0].message,null);for(const secret of ['private-web','private-url','private-attribute','private-file-id','private-image','private-shot'])assert.ok(!JSON.stringify(view).includes(secret));
 const empty=captureRequestContext({input:[{type:'file_search_call',queries:[],results:null},{type:'computer_call_output',output:{type:'computer_screenshot'},acknowledged_safety_checks:null}]});assert.equal(empty.state,'complete');assert.equal(empty.components[1].content.value.acknowledged_safety_checks,null);
});

test('搜索结果、查询与批量计算机文本连续脱敏，不让拆分凭据进入上下文',async()=>{
 for(const parts of [['Authorization: Bearer ','private-split-secret'],['{"api_key":"','private-json-secret"}']]){
  const cases=[{type:'file_search_call',queries:parts},{type:'file_search_call',results:parts.map(text=>({text}))},{type:'web_search_call',action:{type:'search',query:parts[0],queries:[parts[1]]}},{type:'web_search_call',action:{type:'search',queries:parts}},{type:'computer_call',actions:parts.map(text=>({type:'type',text}))},{type:'computer_call',pending_safety_checks:parts.map((message,i)=>({id:String(i),message}))}];
  for(const item of cases){const payload={input:[item]},view=captureRequestContext(payload);assert.equal(view.state,'complete');for(const secret of ['private-split-secret','private-json-secret'])assert.ok(!JSON.stringify(view).includes(secret),JSON.stringify(item));assert.deepEqual(await captureRequestContextAsync(payload),view);}
 }
});

test('搜索和计算机协议拒绝未知动作与隐藏载荷，合法字面业务字段不冒充协议',async()=>{
 const invalid=[{type:'web_search_call',action:{type:'unknown',text:'private-hidden'}},{type:'web_search_call',action:{type:'search',queries:{text:'private-hidden'}}},{type:'web_search_call',action:{type:'search',sources:[{type:'url',url:{text:'private-hidden'}}]}},{type:'web_search_call',status:null},{type:'file_search_call',queries:[{text:'private-hidden'}]},{type:'file_search_call',results:{}},{type:'file_search_call',results:[{type:'reasoning',summary:[{type:'summary_text',text:'private-hidden'}]}]},{type:'file_search_call',results:[{attributes:{key:{text:'private-hidden'}}}]},{type:'file_search_call',results:[{text:{value:'private-hidden'}}]},{type:'file_search_call',results:[{score:2}]},{type:'computer_call',action:[]},{type:'computer_call',action:{type:'click',button:'invalid',x:'1',y:2}},{type:'computer_call',action:{type:'keypress',keys:null}},{type:'computer_call',actions:[{type:'type',text:{value:'private-hidden'}}]},{type:'computer_call',actions:[{type:'drag',path:[{x:1,y:2,extra:'private-hidden'}]}]},{type:'computer_call',pending_safety_checks:[{id:'check',message:{value:'private-hidden'}}]},{type:'computer_call_output',output:{type:'image',image_url:'https://private-hidden.example'}},{type:'computer_call_output',output:{type:'computer_screenshot',file_id:{value:'private-hidden'},thinking:'private-hidden'}},{type:'computer_call_output',acknowledged_safety_checks:{text:'private-hidden'}}];
 for(const item of invalid){const view=captureRequestContext({input:[item]});assert.equal(view.state,'incomplete',JSON.stringify(item));assert.ok(!JSON.stringify(view).includes('private-hidden'));assert.deepEqual(await captureRequestContextAsync({input:[item]}),view);}
 const payload={input:[{type:'computer_call',actions:Array.from({length:100},()=>({type:'type',text:'公开输入'}))}]};for(const options of [{maxNodes:60},{maxBytes:512}]){const view=captureRequestContext(payload,options);assert.equal(view.state,'incomplete');assert.ok(view.reasons.includes('size_limit'));assert.deepEqual(await captureRequestContextAsync(payload,options),view);}
});

test('官方caller仅适用于八类工具，保留真实关联与null；async仅函数和自定义调用',async()=>{
 const calls=[{type:'function_call',id:'function',call_id:'f-call',name:'lookup',arguments:'{}',async:false},{type:'function_call_output',id:null,call_id:null,name:null,namespace:null,output:'结果'},
 {type:'custom_tool_call',id:'custom',call_id:'c-call',name:'exec',input:'公开命令',async:true},{type:'custom_tool_call_output',id:'custom-output',call_id:'c-call',output:'结果'},
 {type:'shell_call',id:null,call_id:'shell-call',action:{commands:['echo visible'],timeout_ms:null,max_output_length:null}},{type:'shell_call_output',id:null,call_id:'shell-call',output:[],max_output_length:null},
 {type:'apply_patch_call',id:null,call_id:'patch-call',status:'completed',operation:{type:'delete_file',path:'visible.txt'}},{type:'apply_patch_call_output',id:null,call_id:'patch-call',status:'completed',output:null}];
 for(const caller of [null,{type:'direct'},{type:'program',caller_id:'actual-program-call'}]){
  const input=calls.map(item=>({...item,caller})),original=structuredClone(input),view=captureRequestContext({input});assert.equal(view.state,'complete');assert.deepEqual(input,original);assert.deepEqual(await captureRequestContextAsync({input}),view);for(const item of view.components)assert.deepEqual(item.content.value.caller,caller);assert.equal(view.components[0].content.value.async,false);assert.equal(view.components[2].content.value.async,true);assert.equal(view.components[1].content.value.call_id,null);assert.equal(view.components[1].content.value.name,null);for(const index of [1,4,5,6,7])assert.equal(view.components[index].content.value.id,null);
 }
});

test('shell环境保留local技能可见配置或container引用，凭据附件与未知协议仍省略',async()=>{
 const input=[{type:'shell_call',call_id:'local',environment:{type:'local',skills:[{name:'公开技能',description:'用途说明',path:'/workspace/skills/visible'}]},action:{commands:['echo visible']}},{type:'shell_call',call_id:'container',environment:{type:'container_reference',container_id:'actual-container'},action:{commands:[]}},{type:'shell_call',call_id:'none',environment:null,action:{commands:[]}}];
 const view=captureRequestContext({input});assert.equal(view.state,'complete');assert.deepEqual(view.components.map(item=>item.content.value.environment),input.map(item=>item.environment));assert.deepEqual(await captureRequestContextAsync({input}),view);
 const cases=[{type:'shell_call',environment:{type:'container_reference',container_id:{data:'private-hidden'}}},{type:'shell_call',environment:{type:'container_auto',file_ids:['private-hidden']}},{type:'shell_call',environment:{type:'local',skills:null}},{type:'shell_call',environment:{type:'local',skills:[null]}},{type:'shell_call',environment:{type:'local',skills:[{name:{data:'private-hidden'}}]}},{type:'shell_call',environment:{type:'local',skills:[{name:'visible',description:'visible',path:'visible',source:{data:'private-hidden'}}]}},{type:'shell_call',environment:{type:'local',credentials:{api_key:'private-hidden'}}},{type:'apply_patch_call',environment:{type:'local',credentials:'private-hidden'}},{type:'shell_call_output',environment:{type:'local'}},{type:'mcp_call',caller:{type:'program',caller_id:'private-hidden'}},{type:'computer_call',caller:{type:'direct'}},{type:'shell_call',async:false},{type:'function_call',async:'false'},{type:'custom_tool_call',async:null},{type:'function_call',caller:{type:'program',caller_id:{data:'private-hidden'}}},{type:'function_call',caller:{type:'direct',caller_id:'private-hidden'}},{type:'function_call',caller:{type:'unknown',data:'private-hidden'}},{type:'function_call',caller:[]},{type:'function_call',id:null},{type:'custom_tool_call_output',id:null}];
 for(const item of cases){const result=captureRequestContext({input:[item]});assert.equal(result.state,'incomplete',JSON.stringify(item));assert.ok(!JSON.stringify(result).includes('private-hidden'));assert.deepEqual(await captureRequestContextAsync({input:[item]}),result);}
 const forged=captureRequestContext({input:[{type:'shell_call',environment:{type:'local',skills:[{type:'function_call_output',name:null,description:'visible',path:'visible'}]}}]});assert.equal(forged.state,'incomplete');assert.equal(Object.hasOwn(forged.components[0].content.value.environment.skills[0],'name'),false);
 const split={input:[{type:'shell_call',environment:{type:'local',skills:[{name:'one',description:'Authorization: Bearer ',path:'one'},{name:'two',description:'private-skill-secret',path:'two'}]}}]};const safe=captureRequestContext(split);assert.equal(safe.state,'complete');assert.ok(!JSON.stringify(safe).includes('private-skill-secret'));
 const large={input:[{type:'shell_call',environment:{type:'local',skills:Array.from({length:100},()=>({name:'visible',description:'visible',path:'visible'}))}}]};for(const options of [{maxNodes:60},{maxBytes:512}]){const result=captureRequestContext(large,options);assert.equal(result.state,'incomplete');assert.ok(result.reasons.includes('size_limit'));assert.deepEqual(await captureRequestContextAsync(large,options),result);}
});

// Protocol fixture follows openai/codex rust-v0.167.2 models.rs AgentMessage /
// AgentMessageInputContent, not CLI item.completed.agent_message{text} events.
test('agent_message retains only its explicit wire fields without inventing a message role',()=>{
 const item={type:'agent_message',id:null,author:'synthetic-agent-a',recipient:'synthetic-agent-b',content:[{type:'input_text',text:'synthetic visible text'},{type:'encrypted_content',get encrypted_content(){throw Error('opaque must not be accessed');}}]};
 const view=captureRequestContext({input:[item]});assert.equal(view.state,'complete');assert.equal(view.opaqueReasoningExcluded,1);
 const saved=view.components[0].content.value;assert.deepEqual(saved,{type:'agent_message',id:null,author:item.author,recipient:item.recipient,content:[{type:'input_text',text:'synthetic visible text'},{type:'encrypted_content'}]});assert.equal(Object.hasOwn(saved,'role'),false);
});
test('agent typed unknown fields, malformed children and identity never become silently complete',()=>{
 for(const change of [{auxiliary:'PRIVATE_UNKNOWN'},{author:{text:'PRIVATE_IDENTITY'}},{content:'PRIVATE_CONTENT'},{content:[{type:'future_part',text:'PRIVATE_UNKNOWN'}]},{content:[{type:'input_text'}]},{content:[{type:'input_text',text:'safe',extra:'PRIVATE_EXTRA'}]}]){
  const view=captureRequestContext({input:[{type:'agent_message',author:'a',recipient:'b',content:[],...change}]});assert.equal(view.state,'incomplete');assert.ok(!JSON.stringify(view).includes('PRIVATE_'));
 }
 const missing=captureRequestContext({input:[{type:'agent_message',content:[]}]});assert.equal(missing.state,'incomplete');
});
test('agent message text shares redaction and budgets while arbitrary typed input stays unsupported',()=>{
 const view=captureRequestContext({input:[{type:'agent_message',author:'Bearer synthetic-credential',recipient:'b',content:[{type:'input_text',text:'Bearer '},{type:'input_text',text:'synthetic-secret'}]}]});assert.ok(!JSON.stringify(view).includes('synthetic-secret'));assert.ok(!JSON.stringify(view).includes('synthetic-credential'));
 const unknown=captureRequestContext({input:[{type:'synthetic_unknown',content:[{type:'input_text',text:'PRIVATE_UNKNOWN'}]}]});assert.equal(unknown.state,'incomplete');assert.deepEqual(unknown.reasons,['unsupported_input']);assert.deepEqual(unknown.components,[]);
 const limited=captureRequestContext({input:[{type:'agent_message',author:'a',recipient:'b',content:Array.from({length:100},()=>({type:'input_text',text:'synthetic'}))}]},{maxNodes:32});assert.equal(limited.state,'incomplete');assert.ok(limited.reasons.includes('size_limit'));
});

test('agent保留字段位于错误层级必须incomplete且不读取未知字段值',()=>{
 for(const key of ['thinking','reasoning_content','encrypted_content'])for(const where of ['item','text']){
  const item={type:'agent_message',author:'a',recipient:'b',content:[{type:'input_text',text:'safe'}]};Object.defineProperty(where==='item'?item:item.content[0],key,{enumerable:true,get(){throw Error('must not read misplaced opaque field');}});
  const view=captureRequestContext({input:[item]});assert.equal(view.state,'incomplete');assert.deepEqual(view.reasons,['unsupported_field']);assert.equal(view.opaqueReasoningExcluded,0);assert.equal(view.components[0].content.value.content[0].text,'safe');
 }
});
