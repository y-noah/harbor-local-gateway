import {Readable} from 'node:stream';
import {createZstdDecompress} from 'node:zlib';
import {AppError} from './store.mjs';

// OpenAI Responses documented transport bounds; unrelated to model token limits.
export const responseBodyBytes=128*1024*1024;
export const maxExpansionRatio=100;
export async function readJsonBody(req,{maxBytes=128*1024,compressed=false}={}) {
  if(!String(req.headers['content-type']||'').startsWith('application/json'))throw new AppError(415,'请使用 application/json');
  const encoding=String(req.headers['content-encoding']||'identity').toLowerCase().trim();
  if(encoding!=='identity'&&!(compressed&&encoding==='zstd'))throw new AppError(415,'不支持此请求压缩编码','unsupported_content_encoding');
  const tooLarge=()=>new AppError(413,`请求体超过 ${maxBytes/1024/1024} MiB 传输上限`,'request_too_large');
  const length=req.headers['content-length'];
  if(length!==undefined&&(!/^\d+$/.test(length)||!Number.isSafeInteger(Number(length))))throw new AppError(400,'Content-Length 无效');
  if(Number(length)>maxBytes){req.resume();throw tooLarge();}
  let wireBytes=0,chunks=[];
  // Keep the incoming socket available to return a structured 413 on chunked bodies.
  for await(const chunk of req.iterator({destroyOnReturn:false})){
    wireBytes+=chunk.length;if(wireBytes>maxBytes){req.resume();throw tooLarge();}chunks.push(chunk);
  }
  let decoded;
  if(encoding==='zstd'){
    const input=Readable.from(chunks),decoder=createZstdDecompress();let size=0,output=[];
    input.pipe(decoder);
    try{
      for await(const chunk of decoder){size+=chunk.length;if(size>maxBytes)throw tooLarge();output.push(chunk);}
      if(size>wireBytes*maxExpansionRatio)throw new AppError(413,'请求压缩展开比例超过官方100倍限制','request_too_large');
      decoded=Buffer.concat(output,size);output=[];
    }catch(e){if(e instanceof AppError)throw e;throw new AppError(400,'zstd 请求体无效或不完整','invalid_compressed_body');}
    finally{input.destroy();decoder.destroy();}
  }else decoded=Buffer.concat(chunks,wireBytes);
  chunks=[];
  try{const value=JSON.parse(decoded.toString('utf8'));if(!value||typeof value!=='object'||Array.isArray(value))throw Error();return value;}
  catch{throw new AppError(400,'请求必须是 JSON 对象','invalid_json');}
}
