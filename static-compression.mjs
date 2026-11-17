import {gzip} from 'node:zlib';
import {acceptedContentEncodings,varyAcceptEncoding} from './admin-json-compression.mjs';

// The fixed public assets are compressed once at startup. No response, Key or
// user data enters this map; requests only select an already prepared buffer.
export async function createStaticCompression(files,{gzipImpl=gzip}={}){
 const assets=new Map(await Promise.all([...files].map(async([name,source])=>{
  let compressed;try{compressed=await new Promise((resolve,reject)=>gzipImpl(source,(error,value)=>error?reject(error):resolve(value)));}catch{}
  return [name,{source,compressed:Buffer.isBuffer(compressed)?compressed:null}];
 })));
 return {send(req,res,name){
  const asset=assets.get(name);if(!asset)return false;
  const preference=acceptedContentEncodings(req.headers['accept-encoding']);let payload=asset.source,status=200,encoding;
  if(!preference.gzip&&!preference.identity){status=406;payload=Buffer.alloc(0);}
  else if(preference.gzip>0&&preference.gzip>=preference.identity&&asset.compressed){payload=asset.compressed;encoding='gzip';}
  else if(!preference.identity){status=503;payload=Buffer.alloc(0);}
  varyAcceptEncoding(res);res.setHeader('Content-Length',payload.length);
  if(encoding)res.setHeader('Content-Encoding',encoding);else res.removeHeader('Content-Encoding');
  res.writeHead(status);res.end(payload);return true;
 }};
}
