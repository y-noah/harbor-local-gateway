import http from 'node:http';
import {fstatSync,lstatSync} from 'node:fs';
import {lstat,readFile,realpath} from 'node:fs/promises';
import {dirname,resolve} from 'node:path';
import {isIP} from 'node:net';

export const gatewayClientAddressHeader='x-harbor-entry-client-address';
const verifiedRequests=new WeakMap();
const fail=code=>{const error=new Error('网关传输校验未通过');error.code=code;throw error;};
export function gatewayClientAddress(req){
 const value=verifiedRequests.get(req)??req.socket?.remoteAddress;
 if(typeof value!=='string'||!isIP(value))return null;
 if(isIP(value)===6){
  // WHATWG canonicalization expands a dotted tail into the same two words as
  // hexadecimal mapped addresses; only the exact ::ffff:0:0/96 prefix maps.
  let canonical;try{canonical=new URL('http://['+value+']/').hostname.slice(1,-1);}catch{return value;}
  const mapped=/^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/.exec(canonical);
  if(mapped){const high=parseInt(mapped[1],16),low=parseInt(mapped[2],16);return [high>>>8,high&255,low>>>8,low&255].join('.');}
 }
 return value;
}

async function rootListener({fd,path}={}){
 if(process.platform!=='linux'||!Number.isSafeInteger(fd)||fd<0||typeof path!=='string'||path!==resolve(path)||path.includes('\0')||Buffer.byteLength(path)>=108)fail('gateway_transport_listener_invalid');
 const socket=fstatSync(fd),file=await lstat(path);
 if(!socket.isSocket()||socket.uid!==0||!file.isSocket()||file.isSymbolicLink()||file.uid!==0||(file.mode&0o777)!==0o600||await realpath(path)!==path)fail('gateway_transport_listener_untrusted');
 for(let parent=dirname(path);;parent=dirname(parent)){
  const info=await lstat(parent);
  if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==0||info.mode&0o022)fail('gateway_transport_directory_untrusted');
  if(parent==='/')break;
 }
 const rows=(await readFile('/proc/self/net/unix','utf8')).trim().split('\n').slice(1).map(row=>row.trim().split(/\s+/));
 const row=rows.find(row=>row[6]===String(socket.ino));
 if(!row||row[4]!=='0001'||!(Number.parseInt(row[3],16)&0x10000)||row.slice(7).join(' ')!==path)fail('gateway_transport_fd_path_mismatch');
 return {fd,path,socket,file,closed:false};
}
function unchanged(listener){
 if(listener.closed)fail('gateway_transport_listener_closed');
 const socket=fstatSync(listener.fd),file=lstatSync(listener.path);
 if(!socket.isSocket()||socket.dev!==listener.socket.dev||socket.ino!==listener.socket.ino||socket.uid!==0||!file.isSocket()||file.dev!==listener.file.dev||file.ino!==listener.file.ino||file.uid!==0||(file.mode&0o777)!==0o600)fail('gateway_transport_listener_changed');
}

// The factory owns the server/listener association. A JSON description or a
// token from another listener cannot make a direct TCP request trusted.
export async function createRootUnixGatewayReceiver(options,handler){
 if(typeof handler!=='function'||!options||Object.keys(options).some(key=>!['fd','path'].includes(key)))fail('gateway_transport_listener_invalid');
 const listener=await rootListener(options);
 const server=http.createServer((req,res)=>{
  try{
   unchanged(listener);
   const occurrences=req.rawHeaders.filter((_,index)=>index%2===0&&req.rawHeaders[index].toLowerCase()===gatewayClientAddressHeader).length,address=req.headers[gatewayClientAddressHeader];
   if(occurrences!==1||typeof address!=='string'||!isIP(address))fail('gateway_transport_metadata_invalid');
   verifiedRequests.set(req,address);
  }catch{
   if(!res.headersSent)res.writeHead(400,{'Content-Type':'application/json','Connection':'close'});
   res.end('{"error":{"code":"invalid_transport"}}');return;
  }
  try{Promise.resolve(handler(req,res)).catch(()=>res.destroy());}catch{res.destroy();}
 });
 server.once('close',()=>{listener.closed=true;});
 await new Promise((resolve,reject)=>{server.once('error',reject);server.listen({fd:listener.fd},()=>{server.off('error',reject);resolve();});});
 return {server,close:()=>new Promise((resolve,reject)=>server.close(error=>error?reject(error):resolve()))};
}
