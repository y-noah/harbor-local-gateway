import https from 'node:https';
import {createHash,X509Certificate} from 'node:crypto';
import {AppError} from './store.mjs';

const verified=new WeakMap();
const digest=value=>createHash('sha256').update(value).digest('hex');
const hardware=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value)?value:null;
// Only this same-process TLS listener can populate proof. A forwarded header or
// a plain HTTP request can never manufacture an entry in this WeakMap.
export function deviceTransportProof(request){return verified.get(request)??null;}
export function createDeviceTlsServer({key,cert,ca,handler,host='127.0.0.1',port=0,maxConnections=256,requestTimeoutMs=300000,headersTimeoutMs=30000}={}){
  if(!key||!cert||!ca||typeof handler!=='function'||typeof host!=='string'||!Number.isInteger(port)||port<0||port>65535||!Number.isInteger(maxConnections)||maxConnections<1||maxConnections>10000)throw new AppError(503,'设备TLS入口配置无效','device_tls_configuration');
  const sockets=new Set();let listening=false,closing=false,closed;
  const server=https.createServer({key,cert,ca,minVersion:'TLSv1.2',requestCert:true,rejectUnauthorized:false,requestTimeout:requestTimeoutMs,headersTimeout:headersTimeoutMs},(request,response)=>{
    const raw=request.socket.getPeerCertificate()?.raw;
    // A supplied but unverifiable certificate must not silently become a
    // compatible unsigned request. Missing proof remains distinguishable.
    const proof={encrypted:true,authorized:!!raw&&request.socket.authorized===true,invalidCertificate:!!raw&&request.socket.authorized!==true,
      certificateFingerprint:raw&&request.socket.authorized===true?digest(new X509Certificate(raw).raw):null,
      hardwareFingerprint:hardware(request.headers['x-harbor-hardware']),installationFingerprint:hardware(request.headers['x-harbor-installation'])};
    for(const name of Object.keys(request.headers))if(name.startsWith('x-harbor-device')||['x-harbor-hardware','x-harbor-installation','x-forwarded-client-cert','ssl-client-cert','ssl-client-verify'].includes(name))delete request.headers[name];
    verified.set(request,Object.freeze(proof));
    return Promise.resolve(handler(request,response)).catch(()=>{if(!response.headersSent){response.writeHead(503,{'Content-Type':'application/json'});response.end(JSON.stringify({error:{code:'device_gateway_unavailable',message:'设备入口暂不可用'}}));}else response.destroy();});
  });
  server.maxConnections=maxConnections;
  server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});
  return {server,listen:async()=>{if(closing)throw new AppError(503,'设备入口已关闭','device_tls_closed');if(!listening){await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(port,host,()=>{server.off('error',reject);listening=true;resolve();});});}return server.address();},
    close:({force=false}={})=>{closing=true;if(force)for(const socket of sockets)socket.destroy();return closed??=(async()=>{if(!listening)return;server.closeIdleConnections();await new Promise(resolve=>server.close(resolve));})();},status:()=>({listening,closing,connections:sockets.size})};
}
