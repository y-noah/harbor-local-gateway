import {Readable} from 'node:stream';
import {createZstdDecompress} from 'node:zlib';
import {AppError} from './store.mjs';

// OpenAI Responses documented transport bounds; unrelated to model token limits.
export const responseBodyBytes=128*1024*1024;
export const maxExpansionRatio=100;
const cancelled=signal=>signal?.reason instanceof AppError?signal.reason:new AppError(499,'请求解析已取消','request_cancelled');
export async function readJsonBody(req,options={}){
  const {signal}=options;if(signal?.aborted)throw cancelled(signal);
  const abort=()=>{if(!req.readableEnded)req.destroy(cancelled(signal));};signal?.addEventListener('abort',abort,{once:true});
  try{return await readBody(req,options);}finally{signal?.removeEventListener('abort',abort);}
}
async function readBody(req,{maxBytes=128*1024,compressed=false,accounting,parse,signal}={}) {
  if(!String(req.headers['content-type']||'').startsWith('application/json'))throw new AppError(415,'请使用 application/json');
  const encoding=String(req.headers['content-encoding']||'identity').toLowerCase().trim();
  if(encoding!=='identity'&&!(compressed&&encoding==='zstd'))throw new AppError(415,'不支持此请求压缩编码','unsupported_content_encoding');
  const tooLarge=()=>new AppError(413,`请求体超过 ${maxBytes/1024/1024} MiB 传输上限`,'request_too_large');
  const length=req.headers['content-length'];
  if(length!==undefined&&(!/^\d+$/.test(length)||!Number.isSafeInteger(Number(length))))throw new AppError(400,'Content-Length 无效');
  if(Number(length)>maxBytes){req.resume();throw tooLarge();}
  if(signal?.aborted)throw cancelled(signal);
  accounting?.start(Number(length)||0);
  let wireBytes=0,chunks=[];
  // Keep the incoming socket available to return a structured 413 on chunked bodies.
  for await(const chunk of req.iterator({destroyOnReturn:false})){
    wireBytes+=chunk.length;if(wireBytes>maxBytes){req.resume();throw tooLarge();}accounting?.resize('wire',Math.max(Number(length)||0,wireBytes));chunks.push(chunk);
  }
  // A parsing worker must own its ArrayBuffer, never a shared Node Buffer pool.
  const combine=(blocks,size)=>{if(!parse)return Buffer.concat(blocks,size);const value=Buffer.allocUnsafeSlow(size);let offset=0;for(const block of blocks){value.set(block,offset);offset+=block.length;}return value;};
  if(signal?.aborted)throw cancelled(signal);
  let decoded;
  if(encoding==='zstd'){
    const input=Readable.from(chunks),decoder=createZstdDecompress({signal});let size=0,output=[];
    input.pipe(decoder);
    try{
      for await(const chunk of decoder){size+=chunk.length;if(size>maxBytes)throw tooLarge();accounting?.resize('expanded',size);output.push(chunk);}
      if(size>wireBytes*maxExpansionRatio)throw new AppError(413,'请求压缩展开比例超过官方100倍限制','request_too_large');
      accounting?.resize('decoded',size);decoded=combine(output,size);output=[];accounting?.resize('expanded',0);
    }catch(e){if(signal?.aborted)throw cancelled(signal);if(e instanceof AppError)throw e;throw new AppError(400,'zstd 请求体无效或不完整','invalid_compressed_body');}
    finally{input.destroy();decoder.destroy();}
  }else{accounting?.resize('decoded',wireBytes);decoded=combine(chunks,wireBytes);}
  chunks=[];accounting?.resize('wire',0);accounting?.resize('text',decoded.length*2);
  const decodedBytes=decoded.length;let value;
  try{value=parse?await parse(decoded,{signal,accounting}):JSON.parse(decoded.toString('utf8'));}
  catch(error){if(error instanceof AppError)throw error;throw new AppError(400,'请求必须是 JSON 对象','invalid_json');}
  if(!value||typeof value!=='object'||Array.isArray(value))throw new AppError(400,'请求必须是 JSON 对象','invalid_json');
  accounting?.complete(decodedBytes);return value;
}
