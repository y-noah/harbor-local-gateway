import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {fileURLToPath} from 'node:url';
import {isIP} from 'node:net';
import {createHash} from 'node:crypto';
import {capacityRuntimeArtifactBinding,readCapacityRuntimeRegular} from './runtime-artifact.mjs';
import {join} from 'node:path';
import {readFile,readdir,readlink} from 'node:fs/promises';
import {networkInterfaces} from 'node:os';
import {ownedProcessProof} from './owned-runtime.mjs';
import {capacityNginxSourceBinding,inspectCapacityNginxSource,readCapacityNginxNetworkIdentity} from './nginx-source.mjs';
const fail=code=>{throw Error('capacity_lan_'+code);},hash=value=>createHash('sha256').update(value).digest('hex');
const ipv4=value=>isIP(value)===4?value.split('.').reduce((number,part)=>number*256+Number(part),0):null;
const address=value=>[24,16,8,0].map(bits=>String(value>>>bits&255)).join('.');
const mask=prefix=>prefix===0?0:(0xffffffff<<(32-prefix))>>>0;
function cidr(value){if(typeof value!=='string')return null;const parts=value.split('/'),network=ipv4(parts[0]),prefix=Number(parts[1]);if(parts.length!==2||network===null||!/^\d{1,2}$/.test(parts[1])||prefix<0||prefix>32||(network&mask(prefix))>>>0!==network)return null;return {network,prefix,mask:mask(prefix)};}
const inside=(value,boundary)=>{const number=ipv4(value);return number!==null&&((number&boundary.mask)>>>0)===boundary.network;};
const networkContains=(route,target)=>{const network=ipv4(route?.destination),number=ipv4(target),prefix=route?.prefixLength;return network!==null&&number!==null&&Number.isInteger(prefix)&&prefix>=0&&prefix<=32&&((network&mask(prefix))>>>0)===network&&((number&mask(prefix))>>>0)===network;};
export function capacityLanPolicy(value){const boundary=cidr(value?.cidr);if(!boundary||typeof value.segmentId!=='string'||!value.segmentId||value.segmentId.length>128||!['serverInterface','clientInterface'].every(key=>/^[a-zA-Z0-9_.:-]{1,64}$/.test(value[key]??'')))return null;return {boundary,segmentId:value.segmentId,serverInterface:value.serverInterface,clientInterface:value.clientInterface};}
// Pure boundary matching does not issue a formal capability. Callers must
// authenticate both native observations and the root policy separately.
export function matchCapacityLanBoundary({policy,serverRoute,remoteRoute,connections}={}){
 const selected=capacityLanPolicy(policy),missing=[];
 if(!selected)return {matched:false,missing:['formal_lan_boundary_not_configured']};
 if(!Array.isArray(connections)||!connections.length)return {matched:false,missing:['formal_lan_socket_path_unproven']};
 const remote=remoteRoute;if(!remote||!['targetAddress','sourceAddress'].every(key=>ipv4(remote[key])!==null)||remote.interface!==selected.clientInterface||remote.onLink!==true||![null,'0.0.0.0'].includes(remote.gateway)||!networkContains(remote,remote.targetAddress))missing.push('formal_lan_remote_route_unproven');
 if(!serverRoute||serverRoute.interface!==selected.serverInterface||serverRoute.onLink!==true||![null,'0.0.0.0'].includes(serverRoute.gateway))missing.push('formal_lan_server_route_unproven');
 for(const row of connections){if(ipv4(row.remoteAddress)===null||ipv4(row.localAddress)===null||row.remoteAddress.startsWith('127.')||row.localAddress.startsWith('127.')||!inside(row.remoteAddress,selected.boundary)||!inside(row.localAddress,selected.boundary))missing.push('formal_lan_endpoints_outside_boundary');if(remote&&(row.remoteAddress!==remote.sourceAddress||row.localAddress!==remote.targetAddress))missing.push('formal_lan_remote_socket_mismatch');if(serverRoute&&(serverRoute.targetAddress!==row.remoteAddress||serverRoute.sourceAddress!==row.localAddress||!networkContains(serverRoute,row.remoteAddress)))missing.push('formal_lan_server_route_unproven');}
 return {matched:missing.length===0,missing:[...new Set(missing)],evidence:{scope:'root-policy-and-two-native-onlink-routes',segmentHash:hash(selected.segmentId),boundaryHash:hash(policy.cidr),serverInterfaceHash:hash(selected.serverInterface),clientInterfaceHash:hash(selected.clientInterface),connections:connections.length}};
}
function fromHex(value){if(!/^[a-f0-9]{8}$/i.test(value))fail('route_format');return parseInt(value.match(/../g).reverse().join(''),16)>>>0;}
export function selectCapacityIpv4Route(text,target){if(ipv4(target)===null||typeof text!=='string'||text.length>1048576)fail('route_format');const rows=[];for(const line of text.trim().split('\n').slice(1)){const values=line.trim().split(/\s+/);if(values.length<8)fail('route_format');const destination=fromHex(values[1]),gateway=fromHex(values[2]),flags=parseInt(values[3],16),bits=fromHex(values[7]);if(!Number.isSafeInteger(flags))fail('route_format');let prefix=0;while(prefix<32&&((bits&mask(prefix+1))>>>0)===mask(prefix+1))prefix++;if(mask(prefix)!==bits)fail('route_format');const row={interface:values[0],destination:address(destination),prefixLength:prefix,gateway:address(gateway),onLink:Boolean(flags&1)&&gateway===0};if(flags&1&&networkContains(row,target))rows.push(row);}rows.sort((a,b)=>b.prefixLength-a.prefixLength);if(!rows.length||rows.length>1&&rows[0].prefixLength===rows[1].prefixLength)fail('route_ambiguous');return rows[0];}
// Direct entry has no forwarding hop. These observations issue no capability;
// the topology owner still binds the current factory epoch and TLS challenge.
export function capacityDirectEntryConfiguration(value){
 if(!value||value.type!=='direct'||Object.keys(value).some(key=>!['type','publicOrigin','listenAddress'].includes(key)))fail('direct_configuration');
 let url;try{url=new URL(value.publicOrigin);}catch{fail('direct_configuration');}
 if(url.protocol!=='https:'||url.origin!==value.publicOrigin||url.username||url.password||ipv4(value.listenAddress)===null||value.listenAddress==='0.0.0.0'||value.listenAddress.startsWith('127.')||url.hostname.includes(':')||isIP(url.hostname)===4&&url.hostname!==value.listenAddress)fail('direct_configuration');
 if(!Object.values(networkInterfaces()).flat().some(row=>row.family==='IPv4'&&!row.internal&&row.address===value.listenAddress))fail('direct_address_not_local');
 return Object.freeze({type:'direct',publicOrigin:value.publicOrigin,listenAddress:value.listenAddress,port:Number(url.port||443)});
}
export async function inspectCapacityDirectListener({runtime,gateway,configuration,expected}={}){
 const entry=capacityDirectEntryConfiguration(configuration),child=gateway?.child,proof=ownedProcessProof(runtime,child);
 if(proof.role!=='gateway'||child.exitCode!==null||child.signalCode!==null||Number(new URL(gateway.ready.tlsOrigin).port||443)!==entry.port)fail('direct_gateway_unowned');
 const prefix='/proc/'+child.pid,identity=async()=>{const raw=await readFile(prefix+'/stat','utf8'),fields=raw.slice(raw.lastIndexOf(')')+2).trim().split(/\s+/),namespace=await readlink(prefix+'/ns/net');if(['T','t','Z','X','x'].includes(fields[0])||namespace!==await readlink('/proc/self/ns/net'))fail('direct_process_changed');return {pid:child.pid,startTicks:fields[19],namespace};};
 const before=await identity(),inodes=new Set(),fds=await readdir(prefix+'/fd');if(fds.length>4096)fail('direct_kernel_budget');
 for(const fd of fds)try{const link=await readlink(prefix+'/fd/'+fd),match=/^socket:\[(\d+)\]$/.exec(link);if(match)inodes.add(match[1]);}catch(error){if(error.code!=='ENOENT')throw error;}
 const listeners=[];for(const family of ['tcp','tcp6']){
  const text=await readFile(prefix+'/net/'+family,'utf8');if(text.length>4*1024**2)fail('direct_kernel_budget');
  for(const line of text.trim().split('\n').slice(1)){const cells=line.trim().split(/\s+/),[address,port]=cells[1].split(':');if(cells[3]!=='0A'||parseInt(port,16)!==entry.port)continue;
   // IPv6 overlap is deliberately unknown; direct supports a concrete IPv4 bind.
   if(family==='tcp6')fail('direct_listener_ambiguous');
   const target=entry.listenAddress.split('.').reverse().map(part=>Number(part).toString(16).padStart(2,'0')).join('').toUpperCase();
   if(address==='00000000'||address===target)listeners.push({inode:cells[9],uid:Number(cells[7]),address});
  }
 }
 const target=entry.listenAddress.split('.').reverse().map(part=>Number(part).toString(16).padStart(2,'0')).join('').toUpperCase();
 if(listeners.length!==1||listeners[0].address!==target||!inodes.has(listeners[0].inode))fail('direct_listener_unowned_or_shared');
 const listener={...before,inode:listeners[0].inode,socketUid:listeners[0].uid,listenAddress:entry.listenAddress,port:entry.port};
 if(JSON.stringify(before)!==JSON.stringify(await identity())||expected&&Object.keys(listener).some(key=>listener[key]!==expected[key]))fail('direct_listener_changed');
 ownedProcessProof(runtime,child);return Object.freeze(listener);
}
export function capacityDirectClientConnections(sockets,configuration,network){
 const entry=capacityDirectEntryConfiguration(configuration);
 if(!network||network.targetAddress!==entry.listenAddress||ipv4(network.sourceAddress)===null||!Array.isArray(sockets)||!sockets.length)fail('direct_socket_unproven');
 for(const socket of sockets)if(socket?.source!=='native-http-socket'||socket.peerAddressHash!==hash(entry.listenAddress)||socket.localAddressHash!==hash(network.sourceAddress)||socket.peerPort!==entry.port||!Number.isInteger(socket.localPort)||socket.localPort<1||socket.localPort>65535)fail('direct_socket_unproven');
 return sockets.map(()=>Object.freeze({remoteAddress:network.sourceAddress,localAddress:entry.listenAddress}));
}

// A main-table /proc route can disagree with policy routing. Use the same fixed
// native RTNETLINK observer as the remote signer, with the actual owned listener
// socket UID, and only while its processes share this observer's network ns.
export async function readCapacityNginxLanRoute(source,target,sourceAddress,{signal}={}){
 const record=capacityNginxSourceBinding(source);if(!record)fail('source_not_owned');
 return readNativeLanRoute({record,inspect:()=>readCapacityNginxNetworkIdentity(source),recheck:()=>inspectCapacityNginxSource(source)},target,sourceAddress,{signal});
}
export async function readCapacityDirectLanRoute(options,target,sourceAddress,{signal}={}){
 const inspect=()=>inspectCapacityDirectListener(options);
 return readNativeLanRoute({record:{trusted:options.artifact!==undefined,artifact:options.artifact},inspect,recheck:inspect},target,sourceAddress,{signal});
}
async function readNativeLanRoute({record,inspect,recheck},target,sourceAddress,{signal}={}){
 signal?.throwIfAborted();
 if(ipv4(target)===null||ipv4(sourceAddress)===null)fail('source_not_owned');const before=await inspect(),artifact=record.trusted?capacityRuntimeArtifactBinding(record.artifact):null;
 const relative='tools/capacity/remote-route-proof.py',script=artifact?join(artifact.trust.harness.directory,relative):fileURLToPath(new URL('./remote-route-proof.py',import.meta.url));if(record.trusted&&!artifact?.harness.files[relative])fail('native_observer_unapproved');
 const scriptBytes=await readCapacityRuntimeRegular(script,65536,{trusted:record.trusted,signal});if(record.trusted&&hash(scriptBytes)!==artifact.harness.files[relative])fail('native_observer_digest');
 // Execute these exact verified bytes; reopening the path after checking it
 // would allow a different engine to supply the formal network observation.
 const program="import base64,json,sys;namespace={'__name__':'capacity_native_route','__file__':sys.argv[1]};exec(compile(base64.b64decode(sys.argv[2],validate=True),sys.argv[1],'exec'),namespace);print(json.dumps(namespace['inspect'](sys.argv[3],uid=int(sys.argv[4]),source_address=sys.argv[5]),separators=(',',':')))";
 let route;try{signal?.throwIfAborted();const operation=promisify(execFile)(artifact?.trust.tools.python.file??'python3',['-I','-B','-c',program,script,scriptBytes.toString('base64'),target,String(before.socketUid),sourceAddress],{env:{PATH:'/usr/bin:/bin',PYTHONPATH:'',PYTHONHOME:''},timeout:5000,maxBuffer:8192,signal,killSignal:'SIGKILL'}),closed=new Promise(resolve=>operation.child.once('close',resolve));let value;try{value=await operation;}finally{await closed;}route=JSON.parse(value.stdout);}catch{fail('native_route_unproven');}
 const after=await inspect();if(before.socketUid!==after.socketUid||before.namespace!==after.namespace||route?.targetAddress!==target||route.sourceAddress!==sourceAddress||route.scope!=='actual-rtnetlink-selected-ipv4-route')fail('native_route_changed');await recheck();return route;
}
