import test from 'node:test';
import assert from 'node:assert/strict';
import {performance} from 'node:perf_hooks';
import Connection from 'pg/lib/connection.js';
import {backupObservationStore} from '../postgres-backup-observation.mjs';
import {postgresBackupContext} from '../postgres-backup-context.mjs';

async function bounded(promise,label){let timer;try{return await Promise.race([promise,new Promise((resolve,reject)=>{timer=setTimeout(()=>reject(Error('synthetic_observation_'+label)),5000);})]);}finally{clearTimeout(timer);}}

test('real TLS ErrorResponse does not settle an observation before its delayed or missing ReadyForQuery',{
 skip:process.env.HARBOR_PERIODIC_ROOT_TEST!=='1'?'仅自有root namespace和真实verify-full PG，不连接共享数据库':false,timeout:60000
},async t=>{
 const {createPeriodicRootFixture}=await import('./periodic-root-fixture.mjs'),fixture=await createPeriodicRootFixture({directory:process.env.HARBOR_PERIODIC_ROOT_DIRECTORY,nodeFile:process.execPath,sourceDirectory:new URL('..',import.meta.url).pathname,pgBinDirectory:process.env.HARBOR_CAPACITY_PG_BIN,pgLibraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB});t.after(()=>fixture.close());
 const record=postgresBackupContext(fixture.context);
 for(const mode of ['delayed','missing']){
  const cancellation=new AbortController(),observed=await backupObservationStore(record.adminOptions,{signal:cancellation.signal,deadline:performance.now()+1000}),client=observed.store.pool._clients[0],originalEmit=client.connection.emit,originalCancel=Connection.prototype.cancel;let errorSeen=false,heldReady,cancelSent,readyArrived,closed=false,closing;const cancelled=new Promise(resolve=>{cancelSent=resolve;}),arrived=new Promise(resolve=>{readyArrived=resolve;});
  // Delay only the actual PG message at the driver event boundary. The query,
  // TLS sessions and cancellation request all execute against the private PG.
  client.connection.emit=function(name,...args){if(name==='errorMessage')errorSeen=true;if(name==='readyForQuery'&&errorSeen){heldReady=args;readyArrived();return true;}return originalEmit.call(this,name,...args);};
  Connection.prototype.cancel=function(pid,secret){assert.ok(this.stream.encrypted&&this.stream.authorized===true,'cancel uses verified TLS');assert.ok(pid===client.processID&&secret===client.secretKey,'cancel is bound to the original session');const result=originalCancel.call(this,pid,secret);cancelSent();return result;};
  try{
   const query=observed.store.get('SELECT synthetic_observation_missing_column').catch(error=>error.code);assert.equal(await bounded(query,'query-error'),'42703');await bounded(arrived,'real-ready-message');assert.equal(client._getActiveQuery(),null,'pg clears its active query on ErrorResponse');assert.equal(client.readyForQuery,false,'the real ReadyForQuery has not reached the driver');
   cancellation.abort();closing=observed.close().then(()=>{closed=true;});const rejected=mode==='missing'?assert.rejects(closing,{code:'backup_observation_cleanup_unknown'}):null;await bounded(cancelled,'verified-cancel');await new Promise(resolve=>setImmediate(resolve));assert.equal(closed,false,'ErrorResponse alone must not let close finish');
   if(mode==='delayed'){originalEmit.call(client.connection,'readyForQuery',...heldReady);await bounded(closing,'settled-close');assert.equal(closed,true);}else{await bounded(rejected,'missing-ready-unknown');assert.equal(closed,false);}
   assert.equal(client._ended,true);assert.equal(observed.store.pool.totalCount,0);const deadline=Date.now()+5000;for(;;){if(!(await fixture.store.get('SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1) present',client.processID)).present)break;if(Date.now()>=deadline)throw Error('synthetic_observation_backend_not_closed');await new Promise(resolve=>setImmediate(resolve));}
  }finally{cancellation.abort();await Promise.allSettled([closing??observed.close()]);client.connection.emit=originalEmit;Connection.prototype.cancel=originalCancel;}
 }
});
