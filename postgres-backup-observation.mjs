import pg from 'pg';
import Connection from 'pg/lib/connection.js';
import {performance} from 'node:perf_hooks';
import {PostgresStore} from './postgres-store.mjs';
import {transferError} from './migration-artifacts.mjs';

const unknown=()=>transferError('backup_observation_cleanup_unknown');
export function assertBackupObservationOperation({signal,deadline}={}){if(signal!==undefined&&!(signal instanceof AbortSignal)||deadline!==undefined&&(!Number.isFinite(deadline)||deadline<0||!signal))throw transferError('backup_observation_options');signal?.throwIfAborted();if(deadline!==undefined&&performance.now()>=deadline)throw transferError('backup_observation_deadline');}
// pg 8.23.1's Connection is used deliberately: Client.cancel connects in
// plaintext. Here CancelRequest is queued only after verified TLS completes.
async function cancelObservedQuery(client,ended,timeoutMs,state){
 if(!state.pending){client.connection.stream.destroy();await ended;return;}
 if(client._ended){await ended;throw unknown();}
 if(!client.connection.stream.encrypted||client.connection.stream.authorized!==true||!Number.isInteger(client.processID)||!Number.isInteger(client.secretKey)){client.connection.stream.destroy();await ended;throw unknown();}
 const control=new Connection({ssl:{...client.ssl,...(client.ssl?.key!==undefined?{key:client.ssl.key}:{}),rejectUnauthorized:true},sslNegotiation:client.connection.sslNegotiation});
 let ready=false,failed=false;let settle;
 const settled=new Promise(resolve=>{settle=resolve;}),onReady=()=>{ready=true;settle();},onEnd=()=>settle(),closed=new Promise(resolve=>control.once('end',resolve));
 client.connection.once('readyForQuery',onReady);client.once('end',onEnd);
 const abort=()=>{failed=true;control.stream.destroy();client.connection.stream.destroy();};
 const timer=setTimeout(abort,timeoutMs);control.on('error',abort);control.on('errorMessage',abort);
 control.once('connect',()=>{if(control.sslNegotiation!=='direct')control.requestSsl();});
 control.once('sslconnect',()=>control.stream.once('secureConnect',()=>{if(control.stream.authorized!==true||!control.stream.encrypted){abort();return;}control.cancel(client.processID,client.secretKey);}));
 try{
  control.connect(client.port,client.host);await closed;await settled;
  if(!ready||failed)throw unknown();
  if(!client._ended&&!client._ending)void client.end();await ended;
 }finally{clearTimeout(timer);client.connection.off('readyForQuery',onReady);client.off('end',onEnd);control.stream.destroy();client.connection.stream.destroy();await Promise.all([closed,ended]);}
 if(failed)throw unknown();
}

// Only fixed-profile backup preparation supplies these operation controls.
// Ordinary stores and observation callers retain their original defaults.
export async function backupObservationStore(options,{signal,deadline}={}){
 const check=()=>assertBackupObservationOperation({signal,deadline});check();
 const budget=deadline===undefined?Infinity:Math.floor(deadline-performance.now());if(budget<100)throw transferError('backup_observation_deadline');
 const closing=[],clients=[],states=new WeakMap();let store,checkedOut;
 const stop=row=>{if(!row.stopping){row.stopping=cancelObservedQuery(row.client,row.ended,Math.min(3000,budget),row);closing.push(row.stopping);void row.stopping.catch(()=>{});}return row.stopping;};
 const Client=signal?class extends pg.Client{
  constructor(configuration){super(configuration);this.on('error',()=>{});const ended=new Promise(resolve=>this.once('end',resolve)),row={client:this,ended,pending:false,stopping:null},ready=()=>{row.pending=false;};clients.push(row);states.set(this,row);this.connection.on('readyForQuery',ready);const cancel=()=>{void stop(row);};signal.addEventListener('abort',cancel,{once:true});this.once('end',()=>{signal.removeEventListener('abort',cancel);this.connection.off('readyForQuery',ready);});if(signal.aborted)queueMicrotask(cancel);}
 }:undefined;
 store=new PostgresStore({...options,poolSize:1,...(signal||deadline!==undefined?{connectionTimeoutMs:Math.min(3000,budget),statementTimeoutMs:Math.min(10000,budget),Client}:{})});
 const close=async()=>{for(const row of clients)if(row.pending)void stop(row);let failed=false;try{await Promise.all(closing);}catch{failed=true;}checkedOut?.release();await store.close();await Promise.all(clients.map(value=>value.ended));if(failed)throw unknown();};
 // Keep the observed client checked out until cancellation has witnessed the
 // original query's ReadyForQuery. Pool.query releases erroring clients early.
 try{if(signal||deadline!==undefined)checkedOut=await store.pool.connect();check();}catch(error){await close();check();throw error;}
 const query=checkedOut?checkedOut.query.bind(checkedOut):store.query.bind(store);store.query=async(...args)=>{check();const row=states.get(checkedOut);if(row){if(row.pending)throw unknown();row.pending=true;}const result=await query(...args);check();return result;};
 return {store,check,close};
}
