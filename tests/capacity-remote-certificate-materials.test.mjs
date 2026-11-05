import test from 'node:test';
import assert from 'node:assert/strict';
import {generateKeyPairSync} from 'node:crypto';
import {mkdtemp,writeFile,readFile,link,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
import {assertCapacityRemoteCertificateContent} from '../tools/capacity/remote-certificate-materials.mjs';

test('namespace TLS material rejects signing key copies, hardlinks, DER re-encoding and appended private blocks',async()=>{
 const signer=generateKeyPairSync('ed25519'),tls=generateKeyPairSync('ed25519'),secret=signer.privateKey.export({format:'pem',type:'pkcs8'}),ordinary=tls.privateKey.export({format:'pem',type:'pkcs8'}),directory=await mkdtemp(join(tmpdir(),'harbor-signer-material-'));
 try{const certificates=await syntheticCertificates(directory);const source=join(directory,'signer.pem'),alias=join(directory,'tls.pem');await writeFile(source,secret,{mode:0o600});await link(source,alias);
  for(const value of [Buffer.from(secret),await readFile(alias),signer.privateKey.export({format:'der',type:'pkcs8'}),Buffer.from(ordinary+'\n'+secret),Buffer.concat([Buffer.from(ordinary),signer.privateKey.export({format:'der',type:'pkcs8'})])])assert.throws(()=>assertCapacityRemoteCertificateContent(value,{signingKey:signer.privateKey,privateMaterial:true}),/signer_material_boundary/);
  const wrap=(label,bytes)=>Buffer.from('-----BEGIN '+label+'-----\n'+bytes.toString('base64').match(/.{1,64}/g).join('\n')+'\n-----END '+label+'-----\n');
  const embedded=Buffer.concat([tls.privateKey.export({format:'der',type:'pkcs8'}),signer.privateKey.export({format:'der',type:'pkcs8'})]);assert.throws(()=>assertCapacityRemoteCertificateContent(wrap('PRIVATE KEY',embedded),{signingKey:signer.privateKey,privateMaterial:true}),/signer_material_boundary/);
  const certDer=new (await import('node:crypto')).X509Certificate(certificates.ca).raw;assert.throws(()=>assertCapacityRemoteCertificateContent(wrap('CERTIFICATE',Buffer.concat([certDer,signer.privateKey.export({format:'der',type:'pkcs8'})])),{signingKey:signer.privateKey}),/signer_material_boundary/);
  assert.doesNotThrow(()=>assertCapacityRemoteCertificateContent(Buffer.from(ordinary),{signingKey:signer.privateKey,privateMaterial:true}));
  assert.doesNotThrow(()=>assertCapacityRemoteCertificateContent(certificates.ca,{signingKey:signer.privateKey}));
  assert.doesNotThrow(()=>assertCapacityRemoteCertificateContent(Buffer.concat([certificates.server.cert,certificates.ca]),{signingKey:signer.privateKey}));
  assert.throws(()=>assertCapacityRemoteCertificateContent(Buffer.concat([certificates.ca,signer.privateKey.export({format:'der',type:'pkcs8'})]),{signingKey:signer.privateKey}),/signer_material_boundary/);
  assert.throws(()=>assertCapacityRemoteCertificateContent(Buffer.from(ordinary),{signingKey:signer.privateKey}),/signer_material_boundary/);
 }finally{await rm(directory,{recursive:true,force:true});}
});
