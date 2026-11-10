import test from 'node:test';import assert from 'node:assert/strict';import http from 'node:http';
import {mkdtemp,mkdir,writeFile,rm} from 'node:fs/promises';import {join} from 'node:path';
import {runAppServer} from './codex-client-fixture.mjs';import {captureRequestContext} from '../recording-request-context.mjs';

const binary=process.env.HARBOR_CODEX_BINARY;
test('真实Codex多轮线协议的内联工具目录、命名空间与消息阶段均可采集',{skip:!binary,timeout:60000},async()=>{
 const root=await mkdtemp('/tmp/harbor-context-native-'),home=join(root,'client'),workspace=join(root,'workspace'),calls=[];
 await mkdir(home,{mode:0o700});await mkdir(workspace,{mode:0o700});
 const server=http.createServer(async(req,res)=>{
  try{
   if(!new URL(req.url,'http://127.0.0.1').pathname.endsWith('/responses')){res.writeHead(200,{'content-type':'application/json'});res.end(JSON.stringify({data:[],models:[]}));return;}
   let text='';for await(const chunk of req)text+=chunk;const payload=JSON.parse(text);calls.push(payload);
   const tools=[...(payload.tools||[]),...(payload.input||[]).filter(item=>item.type==='additional_tools').flatMap(item=>item.tools||[])];
   const namespace=tools.find(tool=>tool.type==='namespace'&&tool.tools?.some(tool=>tool.name==='exec'));
   assert.ok(namespace||tools.some(tool=>tool.name==='exec'),'实际客户端应暴露合成exec工具');
   const items=calls.length===1?[{type:'reasoning',id:'synthetic-reasoning',summary:[{type:'summary_text',text:'Synthetic public summary'}],content:[{type:'reasoning_text',text:'SYNTHETIC_PRIVATE_REASONING'}],encrypted_content:'synthetic-opaque-reasoning'},{type:'message',id:'synthetic-comment',role:'assistant',phase:'commentary',content:[{type:'output_text',text:'Synthetic tool follows',annotations:[],logprobs:[]}]},{type:'custom_tool_call',id:'synthetic-tool',call_id:'synthetic-call',name:'exec',...(namespace?{namespace:namespace.name}:{}),input:"text('synthetic wire result')"}]:[{type:'message',id:'synthetic-final',role:'assistant',phase:'final',content:[{type:'output_text',text:'Synthetic wire complete',annotations:[],logprobs:[]}]}];
   const response={id:'synthetic-response-'+calls.length,status:'completed',model:payload.model,output:items,usage:{input_tokens:10,output_tokens:5,total_tokens:15}};
   res.writeHead(200,{'content-type':'text/event-stream'});res.end([{type:'response.created',response:{id:response.id}},...items.map((item,index)=>({type:'response.output_item.done',output_index:index,item})),{type:'response.completed',response}].map(event=>'data: '+JSON.stringify(event)+'\n\n').join(''));
  }catch{res.writeHead(500);res.end('synthetic fixture failure');}
 });
 try{
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port+'/v1';
  await writeFile(join(home,'config.toml'),'model = "gpt-6.1-sol"\nmodel_provider = "fixture"\nweb_search = "disabled"\n[model_providers.fixture]\nname = "Synthetic fixture"\nbase_url = "'+origin+'"\nwire_api = "responses"\nenv_key = "HARBOR_SYNTHETIC_KEY"\nrequires_openai_auth = false\nsupports_websockets = false\n[features]\nmemories = false\n[memories]\ngenerate_memories = false\nuse_memories = false\n',{mode:0o600});
  const result=await runAppServer({binary,cwd:workspace,env:{PATH:process.env.PATH,LANG:'C.UTF-8',CODEX_HOME:home,RUST_LOG:'off',HARBOR_SYNTHETIC_KEY:'synthetic-only'}});assert.equal(result.code,0);assert.equal(calls.length,2);
  for(const payload of calls){const view=captureRequestContext(payload);assert.equal(view.state,'complete',view.reasons.join(','));assert.equal(view.components.filter(item=>item.content.source==='input').length,payload.input.length);
   assert.ok(!JSON.stringify(view).includes('SYNTHETIC_PRIVATE_REASONING'));assert.ok(!JSON.stringify(view).includes('synthetic-opaque-reasoning'));if(payload.input.some(item=>item.type==='reasoning')){assert.match(JSON.stringify(view),/Synthetic public summary/);assert.equal(view.opaqueReasoningExcluded,1);}
   for(const component of view.components){const source=payload.input[component.content.index],saved=component.content.value;if(component.content.source!=='input')continue;if(source.phase!==undefined)assert.equal(saved.phase,source.phase);if(source.namespace!==undefined)assert.equal(saved.namespace,source.namespace);if(source.type==='additional_tools'){assert.equal(component.kind,'tool_schema');assert.equal(saved.tools.length,source.tools.length);}}
  }
 }finally{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));await rm(root,{recursive:true,force:true});}
});
