import net from 'node:net';
import {createHash} from 'node:crypto';
import {resolve,join} from 'node:path';
import {tmpdir} from 'node:os';

// A kernel-owned endpoint releases automatically even after a crashed process.
// Acquire this before opening SQLite: Store startup performs interruption recovery.
export function instanceEndpoint(dataDir){
  const path=resolve(dataDir),canonical=process.platform==='win32'?path.toLowerCase():path;
  const digest=createHash('sha256').update(canonical).digest('hex').slice(0,32);
  return process.platform==='win32'?`\\\\.\\pipe\\harbor-${digest}`:join(tmpdir(),`harbor-${digest}.sock`);
}
export async function acquireInstance(dataDir){
  const server=net.createServer(socket=>socket.destroy());
  const endpoint=instanceEndpoint(dataDir);
  await new Promise((resolve,reject)=>{
    const failed=error=>{const wrapped=new Error(error.code==='EADDRINUSE'?'Harbor workspace is already running or initializing.':`Cannot acquire Harbor workspace lock: ${error.message}`);wrapped.code=error.code;reject(wrapped);};
    server.once('error',failed);
    server.listen(endpoint,()=>{server.removeListener('error',failed);resolve();});
  });
  let release;
  return {release:()=>release||(release=new Promise((resolve,reject)=>server.close(error=>error?reject(error):resolve())))};
}
