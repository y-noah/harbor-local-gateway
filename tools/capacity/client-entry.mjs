import net from 'node:net';
import {isOwnedLaboratory,ownedProcessProof} from './owned-runtime.mjs';
import {ownsListener} from './resource-assessment.mjs';

const entries=new WeakMap();
export const capacityClientEntryBinding=value=>entries.get(value)??null;

// The synthetic passthrough deliberately does not qualify a production proxy
// or LAN. Its target is an observed listener of this exact owned laboratory.
export async function createCapacityClientEntry({laboratory,maxConnections=2048}={}){
 if(!isOwnedLaboratory(laboratory)||!Number.isInteger(maxConnections)||maxConnections<1||maxConnections>10000)throw Error('capacity_client_entry_configuration');
 let backend,closed=false,closePromise,connections=0,rejected=0;const sockets=new Set();
 const server=net.createServer(client=>{
  if(closed||!backend||sockets.size>=maxConnections*2){rejected++;client.destroy();return;}
  connections++;const upstream=net.connect({host:'127.0.0.1',port:backend.port});
  for(const socket of [client,upstream]){sockets.add(socket);socket.once('close',()=>sockets.delete(socket));socket.on('error',()=>{client.destroy();upstream.destroy();});}
  client.pipe(upstream);upstream.pipe(client);
 });
 await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve);});
 const origin='https://127.0.0.1:'+server.address().port;
 const attach=async gateway=>{
  if(closed||!gateway?.child||!gateway.ready?.tlsOrigin)throw Error('capacity_client_entry_target_invalid');const proof=ownedProcessProof(laboratory.runtime,gateway.child);if(proof.role!=='gateway')throw Error('capacity_client_entry_target_invalid');
  await ownsListener(gateway.child,gateway.ready.tlsOrigin);const target=new URL(gateway.ready.tlsOrigin);
  if(backend&&(backend.origin!==target.origin||backend.child!==gateway.child&&backend.child.exitCode===null&&backend.child.signalCode===null))throw Error('capacity_client_entry_rebind_invalid');
  backend={origin:target.origin,port:Number(target.port),child:gateway.child};return entry;
 };
 const close=()=>closePromise??=(async()=>{closed=true;for(const socket of sockets)socket.destroy();await new Promise(resolve=>server.close(resolve));})();
 const entry=Object.freeze({origin,attach,close,status:()=>({closed,attached:!!backend,active:sockets.size,connections,rejected,scope:'owned-loopback-tls-passthrough',actualDeploymentProxyProven:false,lanProven:false})});
 entries.set(entry,Object.freeze({laboratory,origin,status:entry.status,isOpen:()=>!closed&&!!backend,scope:'owned-loopback-tls-passthrough',verified:false}));return entry;
}
