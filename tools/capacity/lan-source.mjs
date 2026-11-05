import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {fileURLToPath} from 'node:url';
import {isIP} from 'node:net';
import {createHash} from 'node:crypto';
import {capacityRuntimeArtifactBinding,readCapacityRuntimeRegular} from './runtime-artifact.mjs';
import {join} from 'node:path';
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
// A main-table /proc route can disagree with policy routing. Use the same fixed
// native RTNETLINK observer as the remote signer, with the actual owned listener
// socket UID, and only while its processes share this observer's network ns.
export async function readCapacityNginxLanRoute(source,target,sourceAddress,{signal}={}){
 signal?.throwIfAborted();
 const record=capacityNginxSourceBinding(source);if(!record||ipv4(target)===null||ipv4(sourceAddress)===null)fail('source_not_owned');const before=await readCapacityNginxNetworkIdentity(source),artifact=record.trusted?capacityRuntimeArtifactBinding(record.artifact):null;
 const relative='tools/capacity/remote-route-proof.py',script=artifact?join(artifact.trust.harness.directory,relative):fileURLToPath(new URL('./remote-route-proof.py',import.meta.url));if(record.trusted&&!artifact?.harness.files[relative])fail('native_observer_unapproved');
 const scriptBytes=await readCapacityRuntimeRegular(script,65536,{trusted:record.trusted,signal});if(record.trusted&&hash(scriptBytes)!==artifact.harness.files[relative])fail('native_observer_digest');
 // Execute these exact verified bytes; reopening the path after checking it
 // would allow a different engine to supply the formal network observation.
 const program="import base64,json,sys;namespace={'__name__':'capacity_native_route','__file__':sys.argv[1]};exec(compile(base64.b64decode(sys.argv[2],validate=True),sys.argv[1],'exec'),namespace);print(json.dumps(namespace['inspect'](sys.argv[3],uid=int(sys.argv[4]),source_address=sys.argv[5]),separators=(',',':')))";
 let route;try{signal?.throwIfAborted();const operation=promisify(execFile)(artifact?.trust.tools.python.file??'python3',['-I','-B','-c',program,script,scriptBytes.toString('base64'),target,String(before.socketUid),sourceAddress],{env:{PATH:'/usr/bin:/bin',PYTHONPATH:'',PYTHONHOME:''},timeout:5000,maxBuffer:8192,signal,killSignal:'SIGKILL'}),closed=new Promise(resolve=>operation.child.once('close',resolve));let value;try{value=await operation;}finally{await closed;}route=JSON.parse(value.stdout);}catch{fail('native_route_unproven');}
 const after=await readCapacityNginxNetworkIdentity(source);if(before.socketUid!==after.socketUid||before.namespace!==after.namespace||route?.targetAddress!==target||route.sourceAddress!==sourceAddress||route.scope!=='actual-rtnetlink-selected-ipv4-route')fail('native_route_changed');await inspectCapacityNginxSource(source);return route;
}
