import http from 'node:http';
import {constants} from 'node:fs';
import {open,lstat,chmod,unlink} from 'node:fs/promises';
import {dirname,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createGatewayEntry} from './gateway-entry.mjs';

export const gatewayEntryConfigPath='/etc/harbor-entry/config.json';
export const gatewayEntryControlPath='/run/harbor-entry/control.sock';
const failure=code=>Object.assign(new Error(code),{code});
const keys=(value,allowed)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>allowed.includes(key));
const socketPath=value=>typeof value==='string'&&value===resolve(value)&&!value.includes('\0')&&Buffer.byteLength(value)<108;
export function validateGatewayEntryProcessConfig(value){
 if(!keys(value,['listenPort','slots','initialSlot','limits','shutdownTimeoutMs','drainTimeoutMs'])||!Number.isInteger(value.listenPort)||value.listenPort<1024||value.listenPort>65535||!keys(value.slots,['legacy','candidate'])||!socketPath(value.slots.legacy)||!socketPath(value.slots.candidate)||value.slots.legacy===value.slots.candidate||!['legacy','candidate'].includes(value.initialSlot)||!Number.isSafeInteger(value.shutdownTimeoutMs)||value.shutdownTimeoutMs<1||value.shutdownTimeoutMs>2147483647)throw failure('entry_config_invalid');
 if(value.drainTimeoutMs!==undefined&&(!Number.isSafeInteger(value.drainTimeoutMs)||value.drainTimeoutMs<1||value.drainTimeoutMs>120000))throw failure('entry_config_invalid');
 // Delegate all admission-policy validation to the existing factory.
 const probe=createGatewayEntry({route:{socketPath:value.slots[value.initialSlot]},limits:value.limits});
 probe.server.removeAllListeners();
 return structuredClone(value);
}
async function trustedParents(path){
 for(let parent=dirname(path);;parent=dirname(parent)){
  const info=await lstat(parent);if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==0||(info.mode&0o022))throw failure('entry_directory_untrusted');if(parent==='/')break;
 }
}
export async function readGatewayEntryProcessConfig(){
 if(process.getuid?.()!==0)throw failure('entry_root_required');
 await trustedParents(gatewayEntryConfigPath);
 const file=await open(gatewayEntryConfigPath,constants.O_RDONLY|constants.O_NOFOLLOW);
 try{const info=await file.stat();if(!info.isFile()||info.uid!==0||(info.mode&0o777)!==0o600||info.size>8192)throw failure('entry_config_untrusted');const raw=await file.readFile('utf8');if(Buffer.byteLength(raw)>8192)throw failure('entry_config_untrusted');return validateGatewayEntryProcessConfig(JSON.parse(raw));}finally{await file.close();}
}
const listen=(server,options)=>new Promise((resolve,reject)=>{const error=error=>reject(error);server.once('error',error);server.listen(options,()=>{server.off('error',error);resolve();});});
const closeServer=server=>new Promise(resolve=>{server.close(()=>resolve());server.closeAllConnections();});
const reply=(res,status,value)=>{res.writeHead(status,{'Content-Type':'application/json','Cache-Control':'no-store','Connection':'close'});res.end(JSON.stringify(value));};
async function commandBody(req){
 if(req.headers['content-type']!=='application/json')throw failure('entry_command_invalid');
 const length=req.headers['content-length'];if(length!==undefined&&(!/^\d+$/.test(length)||Number(length)>2048))throw failure('entry_command_invalid');
 const chunks=[];let bytes=0;for await(const chunk of req){bytes+=chunk.length;if(bytes>2048)throw failure('entry_command_invalid');chunks.push(chunk);}
 try{return JSON.parse(Buffer.concat(chunks).toString());}catch{throw failure('entry_command_invalid');}
}

// The production CLI fixes both paths and validates root ownership. This factory
// also supports isolated local tests; it is not exposed on the data listener.
export async function createGatewayEntryProcess({config,controlPath=gatewayEntryControlPath}={}){
 config=validateGatewayEntryProcessConfig(config);
 if(!socketPath(controlPath)||Object.values(config.slots).includes(controlPath))throw failure('entry_control_path_invalid');
 const entry=createGatewayEntry({route:{socketPath:config.slots[config.initialSlot]},limits:config.limits}),clients=new Set();
 let slot=config.initialSlot,phase='active',epoch=0,busy=false,closingRequested=false,stopping,ownedControl;
 const operation=new AbortController();
 entry.server.on('connection',socket=>{clients.add(socket);socket.once('close',()=>clients.delete(socket));});
 const state=()=>({epoch,slot,phase,...entry.status()});
 async function shutdown(){
  return stopping??=(async()=>{
   operation.abort(failure('entry_stopping'));if(!closingRequested){closingRequested=true;phase='closing';epoch++;}
   // The same deadline also covers incomplete HTTP headers, which have not
   // reached admission and therefore do not appear in the inflight counter.
   const deadline=setTimeout(()=>{for(const socket of clients)socket.destroy();},config.shutdownTimeoutMs);
   try{
    try{entry.hold();await entry.waitForDrain({timeoutMs:config.shutdownTimeoutMs});}catch{for(const socket of clients)socket.destroy();}
    await entry.close();
   }finally{clearTimeout(deadline);}
   phase='closed';await closeServer(control);
   if(ownedControl){try{const now=await lstat(controlPath);if(now.dev===ownedControl.dev&&now.ino===ownedControl.ino)await unlink(controlPath);}catch(error){if(error.code!=='ENOENT')throw error;}}
  })();
 }
 const control=http.createServer(async(req,res)=>{
  if(req.method==='GET'&&req.url==='/state'){reply(res,200,state());return;}
  if(req.method!=='POST'||req.url!=='/control'){reply(res,404,{error:{code:'entry_control_not_found'}});return;}
  let command;try{command=await commandBody(req);}catch{if(!res.destroyed)reply(res,400,{error:{code:'entry_command_invalid'}});return;}
  const valid=keys(command,['command','expectedEpoch','slot'])&&['hold','drain','activate','resume','close'].includes(command.command)&&Number.isSafeInteger(command.expectedEpoch)&&command.expectedEpoch>=0&&(command.command==='activate'?['legacy','candidate'].includes(command.slot):command.slot===undefined);
  if(!valid){reply(res,400,{error:{code:'entry_command_invalid'}});return;}
  if(closingRequested||stopping||busy||command.expectedEpoch!==epoch){reply(res,409,{error:{code:'entry_epoch_or_operation_conflict'},...state()});return;}
  busy=true;if(command.command==='drain')req.socket.setTimeout(Math.min((config.drainTimeoutMs??config.shutdownTimeoutMs)+1000,2147483647));
  try{
   if(command.command==='close'){closingRequested=true;phase='closing';epoch++;res.once('finish',()=>setImmediate(()=>void shutdown().catch(()=>{process.exitCode=1;})));reply(res,202,{accepted:true,epoch});return;}
   if(command.command==='hold'){if(phase!=='active')throw failure('entry_phase_conflict');entry.hold();phase='holding';}
   if(command.command==='drain'){if(phase!=='holding')throw failure('entry_phase_conflict');await entry.waitForDrain({timeoutMs:config.drainTimeoutMs??config.shutdownTimeoutMs,signal:operation.signal});if(stopping)throw failure('entry_stopping');phase='drained';}
   if(command.command==='activate'){if(phase!=='drained')throw failure('entry_phase_conflict');entry.activate({socketPath:config.slots[command.slot]});slot=command.slot;phase='active';}
   if(command.command==='resume'){if(!['holding','drained'].includes(phase))throw failure('entry_phase_conflict');entry.resume();phase='active';}
   epoch++;reply(res,200,state());
  }catch(error){if(!res.destroyed)reply(res,409,{error:{code:error.code||'entry_operation_failed'},...state()});}finally{busy=false;}
 });
 control.requestTimeout=5000;control.headersTimeout=5000;control.timeout=5000;control.maxHeadersCount=16;
 control.on('timeout',socket=>socket.destroy());control.on('clientError',(_error,socket)=>socket.destroy());
 try{
  // Never unlink a pre-existing socket: it might belong to another live entry.
  await listen(control,{path:controlPath});ownedControl=await lstat(controlPath);await chmod(controlPath,0o600);
  await listen(entry.server,{host:'::',port:config.listenPort,ipv6Only:false});
 }catch(error){for(const socket of clients)socket.destroy();await entry.close();await closeServer(control);if(ownedControl){try{await unlink(controlPath);}catch{}}throw error;}
 return {state,shutdown,server:entry.server};
}

async function main(){
 const config=await readGatewayEntryProcessConfig();await trustedParents(gatewayEntryControlPath);
 for(const path of Object.values(config.slots)){await trustedParents(path);try{const info=await lstat(path);if(!info.isSocket()||info.uid!==0||(info.mode&0o777)!==0o600)throw failure('entry_slot_untrusted');}catch(error){if(error.code!=='ENOENT')throw error;}}
 process.umask(0o077);let runtime,stopRequested=false;
 const stop=()=>{stopRequested=true;if(runtime)void runtime.shutdown().catch(()=>{process.exitCode=1;});};
 for(const signal of ['SIGTERM','SIGINT'])process.on(signal,stop);
 runtime=await createGatewayEntryProcess({config});if(stopRequested)await runtime.shutdown();
}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href)main().catch(error=>{console.error(JSON.stringify({error:{code:/^entry_|^gateway_entry_/.test(error.code||'')?error.code:'entry_start_failed'}}));process.exitCode=1;});
