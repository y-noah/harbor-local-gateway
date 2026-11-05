import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,writeFile,readFile,rm,stat} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createPublicKey,randomBytes,X509Certificate} from 'node:crypto';
import {AppError} from './store.mjs';
import {deviceDigest} from './device-proof.mjs';

const run=promisify(execFile),invalid=()=>new AppError(400,'设备签名请求无效','device_csr_invalid');
export function createDeviceCertificateAuthority({certificateFile,keyFile,openssl='openssl',days=30,maxConcurrent=2,maxQueued=8,timeoutMs=15000}={}){
  if(typeof certificateFile!=='string'||typeof keyFile!=='string'||typeof openssl!=='string'||!Number.isInteger(days)||days<1||days>365||!Number.isInteger(maxConcurrent)||maxConcurrent<1||maxConcurrent>8||!Number.isInteger(maxQueued)||maxQueued<0||maxQueued>64||!Number.isInteger(timeoutMs)||timeoutMs<1000||timeoutMs>60000)throw new AppError(503,'设备证书签发配置无效','device_ca_configuration');
  let active=0,closing=false;const queue=[];
  const drain=()=>{while(!closing&&active<maxConcurrent&&queue.length){const next=queue.shift();clearTimeout(next.timer);active++;next.resolve(()=>{active--;drain();});}};
  async function enter(){if(closing)throw new AppError(503,'设备签发器已关闭','device_ca_closed');if(active<maxConcurrent){active++;return ()=>{active--;drain();};}if(queue.length>=maxQueued)throw new AppError(503,'设备签发队列已满','device_ca_busy');return new Promise((resolve,reject)=>{const item={resolve,reject};item.timer=setTimeout(()=>{const index=queue.indexOf(item);if(index>=0)queue.splice(index,1);reject(new AppError(503,'设备签发等待超时','device_ca_timeout'));},timeoutMs);queue.push(item);});}
  async function inDirectory(operation){const directory=await mkdtemp(join(tmpdir(),'harbor-device-ca-'));try{return await operation(directory);}finally{await rm(directory,{recursive:true,force:true});}}
  async function inspect(csr){
    if(typeof csr!=='string'||csr.length>16384||!csr.startsWith('-----BEGIN CERTIFICATE REQUEST-----')||!csr.trimEnd().endsWith('-----END CERTIFICATE REQUEST-----'))throw invalid();
    const release=await enter();try{return await inDirectory(async directory=>{
      const path=join(directory,'request.pem');await writeFile(path,csr,{mode:0o600});
      let pem;try{pem=(await run(openssl,['req','-in',path,'-verify','-pubkey','-noout'],{timeout:timeoutMs,maxBuffer:32768})).stdout;}catch{throw invalid();}
      try{const key=createPublicKey(pem),bits=key.asymmetricKeyDetails?.modulusLength;if(key.asymmetricKeyType!=='rsa'||!Number.isInteger(bits)||bits<2048||bits>8192)throw invalid();return {publicKey:key.export({type:'spki',format:'pem'}).toString(),publicKeySha256:deviceDigest(key.export({type:'spki',format:'der'}))};}catch{throw invalid();}
    });}finally{release();}
  }
  async function issue({csr,deviceId,generation}){
    if(!/^[a-f0-9-]{36}$/.test(deviceId)||!Number.isSafeInteger(generation)||generation<1)throw new AppError(503,'设备证书目标无效','device_ca_configuration');
    await inspect(csr);const release=await enter();try{
      const info=await stat(keyFile);if(!info.isFile()||process.getuid&&info.uid!==process.getuid()||process.platform!=='win32'&&(info.mode&0o077)!==0)throw new AppError(503,'设备CA私钥属主或权限不符合要求','device_ca_configuration');
      return await inDirectory(async directory=>{
        const request=join(directory,'request.pem'),extensions=join(directory,'extensions.cnf'),output=join(directory,'certificate.pem');await writeFile(request,csr,{mode:0o600});
        await writeFile(extensions,'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=clientAuth\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer\n',{mode:0o600});
        const serial=randomBytes(20);serial[0]&=0x7f;if(serial.every(value=>value===0))serial[19]=1;
        try{await run(openssl,['x509','-req','-in',request,'-CA',certificateFile,'-CAkey',keyFile,'-set_serial','0x'+serial.toString('hex'),'-days',String(days),'-sha256','-subj','/CN=harbor-device-'+deviceId+'-'+generation,'-extfile',extensions,'-out',output],{timeout:timeoutMs,maxBuffer:32768});}catch{throw new AppError(503,'设备证书签发失败','device_ca_failed');}
        const certificate=await readFile(output,'utf8'),parsed=new X509Certificate(certificate);return {certificate,fingerprint:deviceDigest(parsed.raw),expires:Date.parse(parsed.validTo),publicKeySha256:deviceDigest(parsed.publicKey.export({type:'spki',format:'der'}))};
      });
    }finally{release();}
  }
  return {inspect,issue,status:()=>({active,queued:queue.length,closing}),close:()=>{closing=true;for(const item of queue.splice(0)){clearTimeout(item.timer);item.reject(new AppError(503,'设备签发器已关闭','device_ca_closed'));}}};
}
