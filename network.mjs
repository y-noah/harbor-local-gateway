import {isIP} from 'node:net';

export function networkConfig(env=process.env) {
  const listenHost=env.HARBOR_LISTEN_HOST||'127.0.0.1';
  if(!isIP(listenHost))throw Error('HARBOR_LISTEN_HOST 必须是 IP 地址');
  const origins=(env.HARBOR_PUBLIC_ORIGINS||'').split(',').filter(Boolean).map(value=>{
    const url=new URL(value);
    if(!['http:','https:'].includes(url.protocol)||url.username||url.password||url.pathname!=='/'||url.search||url.hash||url.origin!==value)throw Error('HARBOR_PUBLIC_ORIGINS 必须是完整且无路径的 HTTP Origin');
    return url.origin;
  });
  if(!['127.0.0.1','::1'].includes(listenHost)&&!origins.length)throw Error('远程监听必须配置 HARBOR_PUBLIC_ORIGINS');
  const port=env.HARBOR_PORT===undefined?undefined:Number(env.HARBOR_PORT);
  if(port!==undefined&&(!Number.isInteger(port)||port<1||port>65535))throw Error('HARBOR_PORT 必须是 1–65535 的整数');
  return {listenHost,origins,port};
}

export function acceptsRequest(headers,network) {
  const host=headers.host||'';
  const local=/^(127\.0\.0\.1|localhost)(:\d+)?$/.test(host);
  const origins=network.origins.filter(origin=>new URL(origin).host===host);
  if(!local&&!origins.length)return false;
  if(!headers.origin)return true;
  return (local&&headers.origin==='http://'+host)||origins.includes(headers.origin);
}
