import {join} from 'node:path';
import {readFile} from 'node:fs/promises';
import {X509Certificate,createPrivateKey,createPublicKey} from 'node:crypto';
import {createSecureContext} from 'node:tls';
import {regularFile,transferError} from './migration-artifacts.mjs';
import {deviceMaterials,verifyExistingDevices} from './ci/postgres-device-check.mjs';
const fail=code=>{throw transferError(code);};
async function material(path,secret=false){const info=await regularFile(path);if(info.size>65536||(info.mode&(secret?0o077:0o022)))fail('storage_operator_application_device_material');return readFile(path);}
export async function verifyRestoredDeviceMaterials(store,destination,{candidate,hasSecrets=false,signal}={}){
 const count=Number((await store.get('SELECT COUNT(*)::text n FROM harbor.employee_devices')).n);if(!hasSecrets)return {devices:count,existingDeviceTrustVerified:count===0,serverTlsIdentityVerified:false,reportKeyRestoredVerified:false,missing:['restored_device_and_report_materials']};
 const secrets=join(destination,'secrets'),directory=join(secrets,'device');let ca,key,certificate,issuer;
 try{ca=await material(join(directory,'ca.pem'));key=await material(join(directory,'server.key'),true);certificate=await material(join(directory,'server.pem'));issuer=await material(join(directory,'ca.key'),true);const root=new X509Certificate(ca),leaf=new X509Certificate(certificate),at=Date.now();if(!root.ca||at<Date.parse(root.validFrom)||at>=Date.parse(root.validTo)||at<Date.parse(leaf.validFrom)||at>=Date.parse(leaf.validTo)||!createPublicKey(createPrivateKey(issuer)).export({type:'spki',format:'der'}).equals(root.publicKey.export({type:'spki',format:'der'})))fail('storage_operator_application_device_material');createSecureContext({key,cert:certificate,ca});await verifyExistingDevices(store,root,ca,{signal});
  let serverTlsIdentityVerified=false;if(candidate){const origin=new URL(candidate.deviceOrigin);await deviceMaterials({HARBOR_DEVICE_TLS_ENABLED:'1',HARBOR_DEVICE_TLS_PORT:origin.port||'443',HARBOR_DEVICE_PUBLIC_ORIGIN:origin.origin,HARBOR_PUBLIC_ORIGINS:candidate.publicOrigin+','+origin.origin,HARBOR_DEVICE_TLS_KEY_FILE:join(directory,'server.key'),HARBOR_DEVICE_TLS_CERT_FILE:join(directory,'server.pem'),HARBOR_DEVICE_CA_CERT_FILE:join(directory,'ca.pem'),HARBOR_DEVICE_CA_KEY_FILE:join(directory,'ca.key')},{signal});serverTlsIdentityVerified=true;}
  const report=await material(join(secrets,'postgres-report.key'),true);try{if(report.length!==32)fail('storage_operator_application_report_key');}finally{report.fill(0);}const postgresCa=new X509Certificate(await material(join(secrets,'postgres-ca.crt')));if(!postgresCa.ca||at<Date.parse(postgresCa.validFrom)||at>=Date.parse(postgresCa.validTo))fail('storage_operator_application_postgres_ca');return {devices:count,existingDeviceTrustVerified:true,serverTlsIdentityVerified,reportKeyRestoredVerified:true,missing:serverTlsIdentityVerified?[]:['actual_restored_server_tls_origin']};
 }finally{key?.fill(0);issuer?.fill(0);}
}
