#!/usr/bin/env node
import {realpath,open,stat} from 'node:fs/promises';
import {resolve,relative,isAbsolute,dirname} from 'node:path';
import {pathToFileURL} from 'node:url';
import {randomUUID} from 'node:crypto';
import {createInterface} from 'node:readline';

const maxBytes=128*1024*1024;
const schema={type:'object',additionalProperties:false,required:['prompt','output_path'],properties:{
  prompt:{type:'string',description:'图片生成或修改要求'},
  output_path:{type:'string',description:'工作区内尚不存在的输出文件路径，PNG 格式；父目录须已存在'},
  reference_paths:{type:'array',items:{type:'string'},description:'改图时提供工作区内 PNG、JPEG 或 WebP 图片路径；不传则生成新图'},
  transparent_background:{type:'boolean'},size:{type:'string',description:'图片尺寸，缺省由上游决定'},
  quality:{type:'string',enum:['auto','low','medium','high']}
}};
function imageMime(data){
  if(data.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10])))return 'image/png';
  if(data[0]===255&&data[1]===216&&data[2]===255)return 'image/jpeg';
  if(data.toString('ascii',0,4)==='RIFF'&&data.toString('ascii',8,12)==='WEBP')return 'image/webp';
  throw Error('文件不是支持的 PNG、JPEG 或 WebP 图片');
}
export async function createImageTool({root,baseUrl,key,fetchImpl=fetch}){
  if(!key)throw Error('未加载 HARBOR_API_KEY');
  const workspace=await realpath(root),base=new URL(baseUrl);
  if(!['https:','http:'].includes(base.protocol)||base.username||base.password||base.search||base.hash)throw Error('HARBOR_BASE_URL 无效');
  const within=path=>{const rel=relative(workspace,path);if(rel==='..'||rel.startsWith('..'+(process.platform==='win32'?'\\':'/'))||isAbsolute(rel))throw Error('图片路径必须位于配置的工作区内');return path;};
  const session=randomUUID();
  return async function generate(args,signal){
    if(!args||typeof args!=='object'||Array.isArray(args)||Object.keys(args).some(k=>!Object.hasOwn(schema.properties,k)))throw Error('图片参数无效');
    if(typeof args.prompt!=='string'||!args.prompt.trim()||typeof args.output_path!=='string'||!args.output_path)throw Error('请提供 prompt 和 output_path');
    if(args.transparent_background!==undefined&&typeof args.transparent_background!=='boolean')throw Error('transparent_background 必须为布尔值');
    if(args.quality!==undefined&&!schema.properties.quality.enum.includes(args.quality))throw Error('quality 无效');
    if(args.size!==undefined&&(typeof args.size!=='string'||!args.size))throw Error('size 无效');
    if(args.reference_paths!==undefined&&(!Array.isArray(args.reference_paths)||args.reference_paths.some(p=>typeof p!=='string'||!p)))throw Error('reference_paths 必须为图片路径数组');
    const target=within(resolve(workspace,args.output_path));
    const parent=await realpath(dirname(target));within(parent);
    // Exclusive creation reserves the filename before spending subscription usage.
    let output;try{output=await open(target,'wx',0o600);}catch(error){if(error.code==='EEXIST')throw Error('输出文件已存在，请选择新路径');throw Error('无法创建输出文件，请检查工作区和父目录');}
    let succeeded=false;
    try{
      const images=[];
      for(const ref of args.reference_paths||[]){
        const file=within(await realpath(within(resolve(workspace,ref))));
        const info=await stat(file);if(!info.isFile()||info.size>maxBytes)throw Error('参考图片无效或超过传输上限');
        const handle=await open(file,'r');let bytes;try{bytes=await handle.readFile();}finally{await handle.close();}
        images.push({image_url:`data:${imageMime(bytes)};base64,${bytes.toString('base64')}`});
      }
      const payload={model:'gpt-image-2',prompt:args.prompt,n:1,...(images.length?{images}:{}),...(args.transparent_background===undefined?{}:{background:args.transparent_background?'transparent':'opaque'}),...(args.size?{size:args.size}:{}),...(args.quality?{quality:args.quality}:{})};
      const body=JSON.stringify(payload);if(Buffer.byteLength(body)>maxBytes)throw Error('图片请求超过网关传输上限');
      signal?.throwIfAborted();
      let response;
      try{response=await fetchImpl(base.href.replace(/\/$/,'')+(images.length?'/images/edits':'/images/generations'),{method:'POST',headers:{Authorization:'Bearer '+key,'Content-Type':'application/json','thread-id':session},body,signal,redirect:'error'});}catch{throw Error(signal?.aborted?'图片请求已取消，可能已消耗订阅额度，请勿自动重试':'图片连接中断，可能已消耗订阅额度，请勿自动重试');}
      if(!response.ok){await response.body?.cancel();throw Error(`网关图片请求失败（HTTP ${response.status}），请检查账号状态或网关记录；未自动重试`);}
      let value;try{value=await response.json();}catch{throw Error('网关图片响应格式无效；未自动重试');}
      if(!Array.isArray(value.data)||value.data.length!==1||typeof value.data[0]?.b64_json!=='string'||!value.data[0].b64_json||!/^[A-Za-z0-9+/]+={0,2}$/.test(value.data[0].b64_json))throw Error('网关未返回有效图片；未自动重试');
      const bytes=Buffer.from(value.data[0].b64_json,'base64');const mimeType=imageMime(bytes);
      signal?.throwIfAborted();await output.writeFile(bytes);await output.sync();succeeded=true;
      return {content:[{type:'text',text:JSON.stringify({path:target,mimeType,bytes:bytes.length})},{type:'image',mimeType,data:bytes.toString('base64')}]};
    }finally{
      await output.close();
      if(!succeeded){const {unlink}=await import('node:fs/promises');await unlink(target).catch(()=>{});}
    }
  };
}
export async function serve({input=process.stdin,output=process.stdout,generate}){
  const pending=new Map(),lines=createInterface({input,crlfDelay:Infinity});
  const send=value=>output.write(JSON.stringify({jsonrpc:'2.0',...value})+'\n');
  lines.on('line',line=>{
    let message;try{message=JSON.parse(line);}catch{send({id:null,error:{code:-32700,message:'Invalid JSON'}});return;}
    if(!message||typeof message!=='object'||Array.isArray(message)||message.jsonrpc!=='2.0'||typeof message.method!=='string'){send({id:null,error:{code:-32600,message:'Invalid request'}});return;}
    if(message.method==='notifications/cancelled'){pending.get(message.params?.requestId)?.abort();return;}
    if(message.id===undefined)return;
    const id=message.id;
    if(message.method==='initialize'){send({id,result:{protocolVersion:'2024-11-05',capabilities:{tools:{}},serverInfo:{name:'harbor-image',version:'1.0.0'}}});return;}
    if(message.method==='ping'){send({id,result:{}});return;}
    if(message.method==='tools/list'){send({id,result:{tools:[{name:'image_generate',description:'通过 Harbor 现有 Codex 订阅生成或修改图片并保存工作区。需要改图时传 reference_paths；失败不自动重试。',inputSchema:schema}]}});return;}
    if(message.method!=='tools/call'){send({id,error:{code:-32601,message:'Method not found'}});return;}
    if(message.params?.name!=='image_generate'){send({id,error:{code:-32602,message:'Unknown tool'}});return;}
    if(pending.has(id)){send({id,error:{code:-32600,message:'Duplicate request ID'}});return;}
    const controller=new AbortController();pending.set(id,controller);
    Promise.resolve().then(()=>generate(message.params.arguments,controller.signal)).then(result=>send({id,result}),error=>send({id,result:{isError:true,content:[{type:'text',text:error.message}]}})).finally(()=>pending.delete(id));
  });
  lines.on('close',()=>{for(const controller of pending.values())controller.abort();});
}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href){
  try{
    const root=process.argv[2];if(!root)throw Error('请在 MCP args 中指定允许读写图片的工作区绝对路径');
    if(!isAbsolute(root))throw Error('工作区必须为绝对路径');
    const generate=await createImageTool({root,baseUrl:process.env.HARBOR_BASE_URL||'',key:process.env.HARBOR_API_KEY});
    await serve({generate});
  }catch{process.stderr.write('Harbor 图片 MCP 启动失败：请检查 Node 版本、工作区路径和 HARBOR_BASE_URL / HARBOR_API_KEY 配置。\n');process.exitCode=1;}
}
