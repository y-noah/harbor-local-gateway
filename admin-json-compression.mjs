import {gzip} from 'node:zlib';

function accepted(header){
 if(header===undefined||header==='')return {gzip:0,identity:1};
 const entries=new Map();
 for(const part of String(header).split(',')){
  const [raw,...parameters]=part.trim().split(';'),name=raw.trim().toLowerCase();if(!/^[a-z0-9!#$%&'*+.^_`|~-]+$/.test(name))continue;
  let quality=1;for(const parameter of parameters){const match=/^\s*q\s*=\s*(0(?:\.\d{0,3})?|1(?:\.0{0,3})?)\s*$/i.exec(parameter);if(!match){quality=0;break;}quality=Number(match[1]);}
  entries.set(name,entries.has(name)?Math.min(entries.get(name),quality):quality);
 }
 return {gzip:entries.get('gzip')??entries.get('*')??0,identity:entries.get('identity')??(entries.get('*')===0?0:1)};
}
function vary(res){
 const previous=res.getHeader('Vary'),values=(Array.isArray(previous)?previous.join(','):String(previous??'')).split(',').map(s=>s.trim()).filter(Boolean);
 if(!values.some(s=>s==='*'||s.toLowerCase()==='accept-encoding'))values.push('Accept-Encoding');res.setHeader('Vary',values.join(', '));
}

// Callers select safe routes; payload bytes are never retained after this send.
export function createAdminJsonCompression({maxInputBytes=2097152,gzipImpl=gzip}={}){
 if(!Number.isSafeInteger(maxInputBytes)||maxInputBytes<1||maxInputBytes>2097152||typeof gzipImpl!=='function')throw TypeError('invalid_admin_compression_configuration');
 let active=false,closed=false;const idle=[];
 const cancelled=(req,res,signal)=>signal?.aborted||req.aborted||res.destroyed||res.writableEnded;
 async function send(req,res,encoded,{status=200,authenticated=false,signal,beforeHeaders}={}){
  if(req.method!=='GET'||status!==200||authenticated!==true||res.headersSent||res.getHeader('Content-Encoding')!==undefined||typeof encoded!=='string'&&!Buffer.isBuffer(encoded)||beforeHeaders!==undefined&&typeof beforeHeaders!=='function')throw TypeError('invalid_admin_compression_response');
  if(cancelled(req,res,signal))return {sent:false,status,encoding:null};
  const preference=accepted(req.headers['accept-encoding']),source=Buffer.isBuffer(encoded)?encoded:Buffer.from(encoded);let payload=source,encoding='identity',responseStatus=status;
  if(!preference.gzip&&!preference.identity){responseStatus=406;payload=Buffer.from('{"error":{"code":"encoding_not_acceptable"}}');}
  else if(preference.gzip>0&&preference.gzip>=preference.identity){
   if(closed||active||source.length>maxInputBytes){if(!preference.identity){responseStatus=503;payload=Buffer.from('{"error":{"code":"compression_unavailable"}}');}}
   else{
    active=true;let compressed;
    try{compressed=await new Promise((resolve,reject)=>{try{gzipImpl(source,(error,value)=>error?reject(error):resolve(value));}catch(error){reject(error);}});}catch{}finally{active=false;for(const done of idle.splice(0))done();}
    if(cancelled(req,res,signal))return {sent:false,status,encoding:null};
    if(Buffer.isBuffer(compressed)&&(compressed.length<source.length||!preference.identity)){payload=compressed;encoding='gzip';}
    else if(!preference.identity){responseStatus=503;payload=Buffer.from('{"error":{"code":"compression_unavailable"}}');}
   }
  }
  if(cancelled(req,res,signal))return {sent:false,status:responseStatus,encoding:null};
  vary(res);res.setHeader('Content-Type','application/json; charset=utf-8');res.setHeader('Cache-Control','no-store');res.setHeader('Content-Length',payload.length);
  if(encoding==='gzip')res.setHeader('Content-Encoding','gzip');else res.removeHeader('Content-Encoding');
  if(responseStatus===200)beforeHeaders?.();
  if(cancelled(req,res,signal))return {sent:false,status:responseStatus,encoding:null};
  res.writeHead(responseStatus);res.end(payload);return {sent:true,status:responseStatus,encoding};
 }
 async function close(){closed=true;if(active)await new Promise(resolve=>idle.push(resolve));}
 return {send,close,status:()=>({active,closed})};
}
