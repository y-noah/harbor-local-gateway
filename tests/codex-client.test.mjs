import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,mkdirSync,readFileSync,writeFileSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {tmpdir} from 'node:os';
import {runAppServer} from './codex-client-fixture.mjs';
import {createGateway} from '../server.mjs';
import {createResponsesTransport} from '../responses.mjs';

// Optional real-client qualification. It uses fixture upstreams, never a real subscription.
const binary=process.env.HARBOR_CODEX_BINARY;
const png='iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aX1cAAAAASUVORK5CYII=';
test('official Codex authenticates with employee Key, discovers and executes MCP image generation/edit tools through gateway', {skip:!binary,timeout:60000},async t=>{
 const root=mkdtempSync(join(tmpdir(),'harbor-native-client-')),home=join(root,'client');mkdirSync(home);
 let turns=0;const calls=[];
 const transport=createResponsesTransport({credentials:async()=>({token:'fixture-upstream-only',accountId:'fixture-account'}),fetchImpl:async(url,options)=>{
  const payload=JSON.parse(options.body);calls.push({url,payload,headers:options.headers});
  if(url.endsWith('/images/generations')||url.endsWith('/images/edits'))return Response.json({created:1,background:'opaque',data:[{b64_json:png}],usage:{input_tokens:20,output_tokens:30}});
  assert.ok(url.endsWith('/responses'));turns++;
  const inputs=[
   'const r=await tools.mcp__harbor_image__image_generate({prompt:"fixture blue circle",output_path:"generated.png"}); for(const c of r.content)if(c.type==="image")image(c);else if(c.type==="text")text(c.text);',
   'const r=await tools.mcp__harbor_image__image_generate({prompt:"fixture red circle",output_path:"edited.png",reference_paths:["generated.png"]}); for(const c of r.content)if(c.type==="image")image(c);else if(c.type==="text")text(c.text);'
  ];
  const item=turns<=inputs.length?{type:'custom_tool_call',id:'ct_'+turns,call_id:'call_'+turns,name:'exec',input:inputs[turns-1]}:{type:'message',id:'msg_final',role:'assistant',content:[{type:'output_text',text:'Fixture complete'}]};
  const response={id:'resp_'+turns,status:'completed',output:[item],usage:{input_tokens:10,output_tokens:5,total_tokens:15}};
  return new Response([{type:'response.created',response:{id:response.id}},{type:'response.output_item.done',output_index:0,item},{type:'response.completed',response}].map(e=>'data: '+JSON.stringify(e)+'\n\n').join(''),{headers:{'Content-Type':'text/event-stream'}});
 }});
 const app=createGateway({dataDir:join(root,'data'),runtimeDir:join(root,'runtime'),config:{adminToken:'fixture-admin',codexBinary:binary},checker:async()=>true,rateReader:async()=>null,responsesTransport:transport});
 app.store.run("INSERT INTO accounts(id,name,home,state) VALUES('fixture-account','Fixture','fixture-home','ready')");
 await new Promise(r=>app.server.listen(0,'127.0.0.1',r));
 t.after(async()=>{await app.close();rmSync(root,{recursive:true,force:true});});
 const member=app.store.createMember('Fixture native client',null,null);
 const config=readFileSync(new URL('../clients/harbor.example.toml',import.meta.url),'utf8').replace(/base_url = "[^"]+"/,'base_url = "http://127.0.0.1:'+app.server.address().port+'/v1"');
 writeFileSync(join(home,'config.toml'),config+'\n[mcp_servers.harbor_image]\ncommand = '+JSON.stringify(process.execPath)+'\nargs = '+JSON.stringify([fileURLToPath(new URL('../clients/image-mcp.mjs',import.meta.url)),root])+'\nenv_vars = ["HARBOR_API_KEY", "HARBOR_BASE_URL"]\ndefault_tools_approval_mode = "approve"\n');
 const env={...process.env,CODEX_HOME:home,RUST_LOG:'off',HARBOR_API_KEY:member.secret,HARBOR_BASE_URL:'http://127.0.0.1:'+app.server.address().port+'/v1'};delete env.OPENAI_API_KEY;
 const result=await runAppServer({binary,cwd:root,env});assert.equal(result.code,0,result.stderr);
 for(const path of ['/images/generations','/images/edits'])assert.equal(calls.filter(c=>c.url.endsWith(path)).length,1,'missing MCP call '+path+'; '+JSON.stringify(calls.filter(c=>c.url.endsWith('/responses')).map(c=>({tools:c.payload.tools,input:c.payload.input.filter(x=>x.type.includes('output'))}))));
 assert.equal(turns,3);assert.ok(calls.every(c=>c.headers.Authorization==='Bearer fixture-upstream-only'));
 for(const name of ['generated.png','edited.png'])assert.deepEqual(readFileSync(join(root,name)),Buffer.from(png,'base64'));
 const edit=calls.find(c=>c.url.endsWith('/images/edits'));assert.match(edit.payload.images[0].image_url,/^data:image\/png;base64,/);
 assert.equal(app.store.get('SELECT used FROM keys WHERE id=?',member.keyId).used,145);
 assert.match(result.stdout,/Fixture complete/);
});
