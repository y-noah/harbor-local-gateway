import http from 'node:http';
import {spawn} from 'node:child_process';
import {Duplex} from 'node:stream';
import {timingSafeEqual} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';

const hop=new Set(['host','connection','keep-alive','proxy-authenticate','proxy-authorization','te','trailer','transfer-encoding','upgrade','x-harbor-local-token']);
const allowed=path=>{try{if(typeof path!=='string'||!path.startsWith('/')||path.startsWith('//')||path.includes('\\')||/[\x00-\x20]/.test(path))return false;const normalized=new URL(path,'http://127.0.0.1');if(normalized.pathname!==path.split('?')[0])return false;return normalized.pathname==='/api/me'||normalized.pathname==='/v1/models'||normalized.pathname.startsWith('/v1/codex/')||normalized.pathname==='/v1/responses'||normalized.pathname.startsWith('/v1/images/')||normalized.pathname==='/v1/chat/completions'||normalized.pathname==='/v1/platform/models'||normalized.pathname==='/v1/platform/chat/completions';}catch{return false;}};
const equal=(left,right)=>{if(typeof left!=='string')return false;const a=Buffer.from(left),b=Buffer.from(right);return a.length===b.length&&timingSafeEqual(a,b);};
function relaySocket(child){
  const socket=new Duplex({read(){child.stdout.resume();},write(chunk,encoding,callback){if(child.stdin.write(chunk))callback();else child.stdin.once('drain',callback);},final(callback){child.stdin.end(callback);},destroy(error,callback){child.kill();child.stdin.destroy();child.stdout.destroy();callback(error);}});
  child.stdout.on('data',chunk=>{if(!socket.push(chunk))child.stdout.pause();});child.stdout.once('end',()=>socket.push(null));child.stdout.once('error',error=>socket.destroy(error));child.stdin.once('error',error=>socket.destroy(error));child.once('error',error=>socket.destroy(error));child.once('exit',code=>{if(code!==0&&!socket.destroyed)socket.destroy(new Error('device_windows_relay_failed'));});
  for(const name of ['setTimeout','setNoDelay','setKeepAlive','ref','unref'])socket[name]=()=>socket;
  socket.connecting=false;return socket;
}
export function createWindowsDeviceBridge({helper,stateFile,localToken,port=0,maxConcurrent=32,maxBodyBytes=134217728,spawnProcess=spawn}={}){
  if(typeof helper!=='string'||!helper||typeof stateFile!=='string'||!stateFile||typeof localToken!=='string'||localToken.length<32||localToken.length>200||!Number.isInteger(port)||port<0||port>65535||!Number.isInteger(maxConcurrent)||maxConcurrent<1||maxConcurrent>128||!Number.isSafeInteger(maxBodyBytes)||maxBodyBytes<1024||maxBodyBytes>134217728)throw new Error('device_bridge_configuration_invalid');
  const active=new Set(),sockets=new Set();let stopping=false,listening=false,closePromise;
  const json=(res,status,code)=>{if(!res.headersSent){res.writeHead(status,{'content-type':'application/json'});res.end(JSON.stringify({error:{code,message:code}}));}else res.destroy();};
  const server=http.createServer((req,res)=>{
    const actual=server.address()?.port,host=req.headers.host;
    if(stopping){json(res,503,'device_bridge_stopping');return;}
    if(host!=='127.0.0.1:'+actual||req.headers.origin!==undefined||['sec-fetch-site','sec-fetch-dest','sec-fetch-user'].some(name=>req.headers[name]!==undefined)||!equal(req.headers['x-harbor-local-token'],localToken)||!allowed(req.url)){json(res,403,'device_local_unauthorized');return;}
    if(active.size>=maxConcurrent){json(res,503,'device_bridge_busy');return;}
    if(Number(req.headers['content-length']||0)>maxBodyBytes){json(res,413,'device_request_too_large');return;}
    const child=spawnProcess(helper,['relay','--state',stateFile],{stdio:['pipe','pipe','pipe'],windowsHide:true,env:{PATH:process.env.PATH,SystemRoot:process.env.SystemRoot,WSL_INTEROP:process.env.WSL_INTEROP,WSL_DISTRO_NAME:process.env.WSL_DISTRO_NAME}});active.add(child);child.stderr.resume();
    const headers=Object.fromEntries(Object.entries(req.headers).filter(([name])=>!hop.has(name)&&!name.startsWith('x-harbor-device')));headers.host='127.0.0.1:'+actual;headers.connection='close';
    const socket=relaySocket(child),agent=new http.Agent({keepAlive:false});agent.createConnection=()=>socket;const upstream=http.request({method:req.method,path:req.url,host:'127.0.0.1',port:actual,headers,agent},response=>{
      const outgoing=Object.fromEntries(Object.entries(response.headers).filter(([name])=>!hop.has(name)));res.writeHead(response.statusCode,outgoing);response.pipe(res);response.once('error',()=>res.destroy());
    });
    let bytes=0,complete=false;req.on('data',chunk=>{bytes+=chunk.length;if(bytes>maxBodyBytes){json(res,413,'device_request_too_large');upstream.destroy();res.once('finish',()=>req.destroy());}});
    req.once('aborted',()=>upstream.destroy());res.once('finish',()=>{complete=true;});res.once('close',()=>{if(!complete)upstream.destroy();});upstream.once('error',()=>json(res,503,'device_transport_unavailable'));
    child.once('close',()=>active.delete(child));req.pipe(upstream);
  });
  server.headersTimeout=30000;server.requestTimeout=0;server.maxConnections=maxConcurrent*2;
  server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});
  return {server,listen:async()=>{if(stopping)throw new Error('device_bridge_stopping');if(!listening){await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(port,'127.0.0.1',()=>{server.off('error',reject);listening=true;resolve();});});}return server.address();},
    status:()=>({listening,stopping,active:active.size}),close:({force=false}={})=>{stopping=true;if(force){for(const child of active)child.kill();for(const socket of sockets)socket.destroy();}return closePromise??=(async()=>{if(listening){server.closeIdleConnections();await new Promise(resolve=>server.close(resolve));}})();}};
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  const args=process.argv.slice(2),option=name=>{const at=args.indexOf(name);return at<0?undefined:args[at+1];};
  try{const bridge=createWindowsDeviceBridge({helper:option('--helper'),stateFile:option('--state'),localToken:process.env.HARBOR_DEVICE_LOCAL_TOKEN,port:Number(option('--port')??43129)});const address=await bridge.listen();console.log(JSON.stringify({state:'running',local_port:address.port,identity:'shared_windows_device'}));let stopping=false;for(const signal of ['SIGINT','SIGTERM'])process.on(signal,()=>{if(stopping)return;stopping=true;void bridge.close().catch(()=>{process.exitCode=1;});});}
  catch{console.error('device_bridge_start_failed_no_credentials_logged');process.exitCode=1;}
}
