import {createPrivateKey,createPublicKey,createHash,X509Certificate} from 'node:crypto';
import {relative,isAbsolute} from 'node:path';
import {readCapacityRuntimeRegular} from './runtime-artifact.mjs';

const fail=()=>{throw Error('capacity_remote_root_signer_material_boundary');};
const privatePem=/-----BEGIN ([A-Z0-9 ]*PRIVATE KEY)-----[\s\S]*?-----END \1-----/g;
// Content inspection alone grants no source or root capability. Production
// callers obtain every byte through the fixed trusted regular-file reader.
export function assertCapacityRemoteCertificateContent(value,{signingKey,privateMaterial=false}={}){
 if(!Buffer.isBuffer(value)||!signingKey||signingKey.asymmetricKeyType!=='ed25519')fail();
 const publicKey=createPublicKey(signingKey),text=value.toString('ascii');if(!Buffer.from(text,'ascii').equals(value))fail();
 const pattern=privateMaterial?privatePem:/-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----/g,blocks=[...text.matchAll(pattern)];
 if(!blocks.length||privateMaterial&&blocks.length!==1||text.replace(pattern,'').trim())fail();
 try{for(const row of blocks){const pem=/^-----BEGIN ([A-Z0-9 ]+)-----([\s\S]*?)-----END \1-----$/.exec(row[0]);if(!pem)fail();const encoded=pem[2].replace(/\s/g,''),der=Buffer.from(encoded,'base64');if(!encoded.length||der.toString('base64')!==encoded)fail();let actual;if(privateMaterial){const type={'PRIVATE KEY':'pkcs8','RSA PRIVATE KEY':'pkcs1','EC PRIVATE KEY':'sec1'}[pem[1]];if(!type)fail();const key=createPrivateKey(row[0]);if(createPublicKey(key).equals(publicKey))fail();actual=key.export({format:'der',type});}else actual=new X509Certificate(row[0]).raw;if(!actual.equals(der))fail();}}catch{fail();}
}
export async function readCapacityRemoteCertificateMaterials(plan,signingKey,{maxBytes,maxFiles}={}){
 if(!Number.isSafeInteger(maxBytes)||maxBytes<1||!Number.isSafeInteger(maxFiles)||maxFiles<1)fail();
 const files=new Map();for(const tls of [plan.tls,...plan.agents.map(row=>row.tls)]){
  if(!tls||typeof tls!=='object'||Array.isArray(tls)||Object.keys(tls).some(key=>!['caFile','certFile','keyFile','peerCertificateHash','caSha256','certSha256','keySha256','connection'].includes(key)))fail();
  for(const field of ['caFile','certFile','keyFile'])if(tls[field]!==undefined){const path=tls[field];if(typeof path!=='string'||!isAbsolute(path))fail();const name=relative(plan.certificateDirectory,path);if(!name||isAbsolute(name)||name.split('/').some(part=>part==='..'||part==='.'||!part))fail();const previous=files.get(path);if(previous&&previous.privateMaterial!==(field==='keyFile'))fail();files.set(path,{path,name,privateMaterial:field==='keyFile'});}
 }
 const directories=new Set();for(const row of files.values()){const parts=row.name.split('/');for(let count=1;count<parts.length;count++)directories.add(parts.slice(0,count).join('/'));}if(files.size+directories.size>maxFiles)fail();const result=[];let bytes=0;for(const row of files.values()){const content=await readCapacityRuntimeRegular(row.path,Math.min(1048576,maxBytes-bytes),{trusted:true});bytes+=content.length;if(bytes>maxBytes)fail();assertCapacityRemoteCertificateContent(content,{signingKey,privateMaterial:row.privateMaterial});result.push({...row,content,sha256:createHash('sha256').update(content).digest('hex')});}
 return result;
}
