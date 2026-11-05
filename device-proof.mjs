import {createHash,createPublicKey,verify} from 'node:crypto';
import {AppError} from './store.mjs';

export const deviceDigest=value=>createHash('sha256').update(value).digest('hex');
export function deviceEnrollmentMessage(value){
  return JSON.stringify({protocol:'harbor-device-enrollment-v1',challenge:value.challenge,nonce:value.nonce,scope:value.scope,
    csr_sha256:deviceDigest(value.csr),name:value.name,system:value.system,fingerprint:value.fingerprint??null,
    installation_fingerprint:value.installation_fingerprint??null,fingerprint_source:value.fingerprint_source});
}
export function verifyDeviceEnrollment(publicKey,message,signature){
  if(typeof signature!=='string'||signature.length>2048||!/^[A-Za-z0-9+/]+={0,2}$/.test(signature))return false;
  try{return verify('sha256',Buffer.from(message),createPublicKey(publicKey),Buffer.from(signature,'base64'));}catch{return false;}
}
export function deviceHardwareDigest(scope,value){
  if(typeof scope!=='string'||!/^[a-f0-9]{64}$/.test(scope)||typeof value!=='string')throw new AppError(400,'设备指纹输入无效','device_fingerprint_invalid');
  const normalized=value.trim().toLowerCase().replaceAll('-','');
  if(!/^[a-f0-9]{32}$/.test(normalized)||/^0+$/.test(normalized)||/^f+$/.test(normalized))throw new AppError(400,'设备硬件标识未能核验','device_fingerprint_invalid');
  return deviceDigest('harbor-device-fingerprint-v1\0'+scope+'\0'+normalized);
}
