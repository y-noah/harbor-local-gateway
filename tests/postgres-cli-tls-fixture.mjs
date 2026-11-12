import assert from 'node:assert/strict';
import net from 'node:net';
import tls from 'node:tls';
import {networkInterfaces,tmpdir} from 'node:os';
import {mkdtemp,readFile,writeFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {certificateFixture} from './device-fixture.mjs';
const execute=promisify(execFile);

// Own the TLS endpoint instead of changing the shared CI PostgreSQL service.
// Every PostgreSQL byte after SSL negotiation still reaches the real test DB.
export async function postgresCliTlsFixture(t,connectionString){
 const address=Object.values(networkInterfaces()).flat().find(row=>row.family==='IPv4'&&!row.internal)?.address;
 assert.ok(address,'a nonloopback interface is required for the real CLI TLS boundary');
 const upstream=new URL(connectionString),directory=await mkdtemp(join(tmpdir(),'harbor-cli-tls-')),sockets=new Set();let certificates,server,secureServer,handshakes=0;
 const close=async()=>{for(const socket of sockets)socket.destroy();if(server?.listening)await new Promise(resolve=>server.close(resolve));await certificates?.issuer.close();await rm(directory,{recursive:true,force:true});};
 try{
  certificates=await certificateFixture(directory);
  await writeFile(join(directory,'server.cnf'),'subjectAltName=IP:'+address+'\nextendedKeyUsage=serverAuth\n');
  await execute('openssl',['x509','-req','-in',join(directory,'server.csr'),'-CA',join(directory,'ca.pem'),'-CAkey',join(directory,'ca-key.pem'),'-set_serial','2','-days','1','-extfile',join(directory,'server.cnf'),'-out',join(directory,'server.pem')],{timeout:10000,maxBuffer:32768});
  const track=socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));};
  secureServer=tls.createServer({key:certificates.key,cert:await readFile(join(directory,'server.pem'))},client=>{
   handshakes++;assert.equal(client.encrypted,true);const source=net.connect({host:upstream.hostname,port:Number(upstream.port||5432)});track(client);track(source);
   const destroy=()=>{client.destroy();source.destroy();};client.on('error',destroy);source.on('error',destroy);client.once('close',()=>source.destroy());source.once('close',()=>client.destroy());client.pipe(source);source.pipe(client);
  });secureServer.on('tlsClientError',()=>{});
  server=net.createServer(client=>{
   track(client);client.on('error',()=>{});let prefix=Buffer.alloc(0);
   const start=chunk=>{
    prefix=Buffer.concat([prefix,chunk]);if(prefix.length<8)return;
    client.removeListener('data',start);
    if(prefix.length!==8||prefix.readUInt32BE(0)!==8||prefix.readUInt32BE(4)!==80877103){client.destroy();return;}
    client.write('S');secureServer.emit('connection',client);
   };client.on('data',start);
  });await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,address,resolve);});
  t.after(close);const url=new URL(connectionString);url.hostname=address;url.port=String(server.address().port);
  for(const key of ['ssl','sslmode','sslrootcert','sslcert','sslkey'])url.searchParams.delete(key);
  url.searchParams.set('sslmode','verify-full');url.searchParams.set('sslrootcert',join(directory,'ca.pem'));
  return {connectionString:url.href,handshakes:()=>handshakes};
 }catch(error){await close();throw error;}
}
