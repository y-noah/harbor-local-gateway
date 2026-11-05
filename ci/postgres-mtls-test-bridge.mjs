// CI-only Node mTLS bridge. This is not the Windows/CNG device helper.
import http from 'node:http';
import https from 'node:https';
import {timingSafeEqual} from 'node:crypto';
const hop=new Set(['host','connection','keep-alive','proxy-authenticate','proxy-authorization','te','trailer','transfer-encoding','upgrade','x-harbor-local-token','x-harbor-hardware','x-harbor-installation']);
const paths=new Set(['/api/me','/v1/models','/v1/codex/models','/v1/codex/image-mcp.mjs','/v1/responses','/v1/chat/completions','/v1/platform/chat/completions','/v1/platform/models','/v1/images/generations','/v1/images/edits']);
const equal=(left,right)=>typeof left==='string'&&Buffer.byteLength(left)===Buffer.byteLength(right)&&timingSafeEqual(Buffer.from(left),Buffer.from(right));
export async function createCandidateMtlsTestBridge({origin,ca,cert,key,localToken,hardware,installation,maxConcurrent=32,maxBodyBytes=134217728}={}){
  const remote=new URL(origin);if(remote.protocol!=='https:'||remote.hostname!=='127.0.0.1'||!remote.port||remote.pathname!=='/'||remote.username||remote.password||remote.search||remote.hash||!ca||!cert||!key||typeof localToken!=='string'||!/^[A-Za-z0-9_-]{32,200}$/.test(localToken)||![hardware,installation].every(value=>/^[a-f0-9]{64}$/.test(value))||!Number.isSafeInteger(maxConcurrent)||maxConcurrent<1||maxConcurrent>32||!Number.isSafeInteger(maxBodyBytes)||maxBodyBytes<1024||maxBodyBytes>134217728)throw Error('candidate_mtls_bridge_configuration');
  const agent=new https.Agent({ca,cert,key,keepAlive:true,maxSockets:maxConcurrent,rejectUnauthorized:true}),requests=new Set(),sockets=new Set();let closing=false,closePromise,forwarded=0;
  const reject=(res,status,code)=>{if(res.headersSent){res.destroy();return;}res.writeHead(status,{'content-type':'application/json'});res.end(JSON.stringify({error:{code}}));};
  const server=http.createServer((req,res)=>{
    let path;try{path=new URL(req.url,'http://127.0.0.1');}catch{}
    if(closing){reject(res,503,'candidate_mtls_bridge_closed');return;}
    if(req.headers.host!=='127.0.0.1:'+server.address().port||req.headers.origin!==undefined||['sec-fetch-site','sec-fetch-dest','sec-fetch-user'].some(name=>req.headers[name]!==undefined)||!equal(req.headers['x-harbor-local-token'],localToken)||!path||!paths.has(path.pathname)||!req.url.startsWith('/')||req.url.startsWith('//')||req.url.includes('\\')||req.url.split('?')[0]!==path.pathname||req.url.length>2048||!['GET','POST'].includes(req.method)){reject(res,403,'candidate_mtls_local_denied');return;}
    if(requests.size>=maxConcurrent){reject(res,503,'candidate_mtls_bridge_busy');return;}if(Number(req.headers['content-length']??0)>maxBodyBytes){reject(res,413,'candidate_mtls_bridge_body_limit');return;}
    const headers=Object.fromEntries(Object.entries(req.headers).filter(([name])=>!hop.has(name)&&!name.startsWith('x-harbor-device')));headers['x-harbor-hardware']=hardware;headers['x-harbor-installation']=installation;
    const upstream=https.request(new URL(req.url,remote),{method:req.method,headers,agent},response=>{res.writeHead(response.statusCode,Object.fromEntries(Object.entries(response.headers).filter(([name])=>!hop.has(name))));response.pipe(res);response.once('error',()=>res.destroy());});requests.add(upstream);forwarded++;let bytes=0,finished=false;
    req.on('data',chunk=>{bytes+=chunk.length;if(bytes>maxBodyBytes){upstream.destroy();reject(res,413,'candidate_mtls_bridge_body_limit');}});req.once('aborted',()=>upstream.destroy());res.once('finish',()=>{finished=true;});res.once('close',()=>{if(!finished)upstream.destroy();});upstream.once('close',()=>requests.delete(upstream));upstream.once('error',()=>reject(res,502,'candidate_mtls_upstream_failed'));req.pipe(upstream);
  });
  server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});server.requestTimeout=0;server.headersTimeout=30000;
  try{await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve);});}catch(error){agent.destroy();throw error;}
  return {origin:'http://127.0.0.1:'+server.address().port,status:()=>({scope:'node-private-ci-mtls-not-windows-cng',forwarded,active:requests.size}),close:()=>closePromise??=(async()=>{closing=true;for(const request of requests)request.destroy();for(const socket of sockets)socket.destroy();agent.destroy();await new Promise(resolve=>server.close(resolve));})()};
}
