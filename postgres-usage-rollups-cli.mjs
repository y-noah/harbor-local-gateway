import {isIP} from 'node:net';
import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';
import {PostgresStore} from './postgres-store.mjs';
import {AppError} from './store.mjs';
import {migrateUsageRollups,verifyUsageRollupAvailability,usageRollupsReady,backfillUsageRollups,reconcileUsageRollups,disableUsageRollups,planUsageRollupCascade,resolveUsageRollupCascade,verifyUsageRollupCascadeAvailability,repairUsageRollupCascade,rollbackUsageRollupCascade,recoverUsageRollupCascadeReceipt} from './postgres-usage-rollups.mjs';

const invalid=()=>new AppError(400,'小时用量汇总参数无效；使用 install、backfill、verify 或 disable','database_configuration');
const cascadeActions=['cascade-plan','cascade-status','cascade-verify','cascade-repair','cascade-rollback','cascade-receipt'];
export function usageRollupArguments(args){
 if(!Array.isArray(args)||args.some(arg=>typeof arg!=='string'))throw invalid();
 const [action='verify',...flags]=args;if(!['install','backfill','verify','disable',...cascadeActions].includes(action))throw invalid();
 const value={action},seen=new Set();
 for(const flag of flags){
  if(flag==='--publish-ready'&&action==='verify'){if(seen.has('publishReady'))throw invalid();seen.add('publishReady');value.publishReady=true;continue;}
  if(action==='install'&&flag.startsWith('--runtime-role=')){const role=flag.slice('--runtime-role='.length);if(seen.has('runtimeRole')||!/^[a-z_][a-z0-9_]{0,62}$/.test(role))throw invalid();seen.add('runtimeRole');value.runtimeRole=role;continue;}
  const match=/^--(batch-size|batches|statement-timeout-ms|lock-timeout-ms|budget-ms)=([1-9]\d*)$/.exec(flag);if(!match)throw invalid();
  const [,name,raw]=match,number=Number(raw),spec={
   'batch-size':['batchSize',1,1000],batches:['batches',1,100],
   'statement-timeout-ms':['statementTimeoutMs',100,60000],
   'lock-timeout-ms':['lockTimeoutMs',1,5000],
   'budget-ms':['budgetMs',100,300000]
  }[name];
  if(action!=='backfill'&&!(action==='verify'&&name==='statement-timeout-ms')||seen.has(name)||!Number.isSafeInteger(number)||number<spec[1]||number>spec[2])throw invalid();seen.add(name);value[spec[0]]=number;
 }
 return value;
}
// No daemon, scheduling or implicit activation: each invocation is finite.
export async function runUsageRollupsCommand(args=[],database={}, {clock=Date.now,cascade}={}){
 const command=usageRollupArguments(args),connectionString=database.connectionString??(database.connection?undefined:process.env.HARBOR_DATABASE_URL);
 const store=new PostgresStore({...database,connectionString,statementTimeoutMs:command.statementTimeoutMs??30000});
 try{
  await store.verifySchema();
  if(cascadeActions.includes(command.action)){
   if(!cascade||typeof cascade!=='object'||Array.isArray(cascade))throw invalid();
   if(command.action==='cascade-plan')return {action:command.action,...await planUsageRollupCascade(store,{runtimeWitness:cascade.runtimeWitness})};
   if(command.action==='cascade-status')return {action:command.action,...await resolveUsageRollupCascade(store,{binding:cascade.binding})};
   if(command.action==='cascade-verify'){const status=await verifyUsageRollupCascadeAvailability(store,{binding:cascade.binding,requireSafe:true});return {action:command.action,safe:status.safe};}
   if(command.action==='cascade-repair')return {action:command.action,...await repairUsageRollupCascade(store,{binding:cascade.binding,repairId:cascade.repairId})};
   if(command.action==='cascade-receipt')return {action:command.action,receipt:await recoverUsageRollupCascadeReceipt(store,{binding:cascade.binding,repairId:cascade.repairId})};
   return {action:command.action,...await rollbackUsageRollupCascade(store,{receipt:cascade.receipt,activeBinding:cascade.binding})};
  }
  if(command.action==='install'){const changed=await migrateUsageRollups(store,{apply:true,runtimeRole:command.runtimeRole});return {action:'install',installed:true,changed,ready:await usageRollupsReady(store)};}
  const installed=await verifyUsageRollupAvailability(store);
  if(!installed)return {action:command.action,installed:false,ready:false};
  if(command.action==='disable'){await disableUsageRollups(store);return {action:'disable',installed:true,ready:false};}
  if(command.action==='verify')return {action:'verify',installed:true,...await reconcileUsageRollups(store,{publishReady:command.publishReady??false,statementTimeoutMs:command.statementTimeoutMs??30000})};
  const started=clock(),budget=command.budgetMs??30000,maxBatches=command.batches??1;let scanned=0,batches=0,exhausted=false;
  while(batches<maxBatches&&clock()-started<budget){
   const remaining=Math.max(100,Math.floor(budget-(clock()-started)));
   const batch=await backfillUsageRollups(store,{batchSize:command.batchSize??100,statementTimeoutMs:Math.min(command.statementTimeoutMs??5000,remaining),lockTimeoutMs:command.lockTimeoutMs??250});
   scanned+=batch.scanned;batches++;if(batch.exhausted||batch.scanned===0){exhausted=!!batch.exhausted;break;}
  }
  return {action:'backfill',installed:true,batches,scanned,exhausted,ready:await usageRollupsReady(store)};
 }finally{await store.close();}
}
export async function readUsageCascadeInput(stream){
 let bytes=0,chunks=[];for await(const chunk of stream){bytes+=Buffer.byteLength(chunk);if(bytes>131072)throw invalid();chunks.push(Buffer.from(chunk));}
 let value;try{value=JSON.parse(Buffer.concat(chunks).toString('utf8'));}catch{throw invalid();}
 if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(key=>!['connection','cascade'].includes(key)))throw invalid();
 const c=value.connection,tls=c?.ssl;
 if(!c||typeof c!=='object'||Array.isArray(c)||Object.keys(c).some(key=>!['host','port','database','user','password','ssl','application_name'].includes(key))||isIP(c.host)!==4||!Number.isInteger(c.port)||c.port<1||c.port>65535||typeof c.database!=='string'||!/^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/.test(c.database)||typeof c.user!=='string'||!c.user||Buffer.byteLength(c.user)>256||/[\x00-\x1f]/.test(c.user)||c.password!==null&&typeof c.password!=='string'||c.application_name!=='harbor-usage-cascade-maintenance')throw invalid();
 if(!tls||typeof tls!=='object'||Array.isArray(tls)||Object.keys(tls).some(key=>!['ca','cert','key','rejectUnauthorized','servername'].includes(key))||tls.rejectUnauthorized!==true||typeof tls.ca!=='string'||!tls.ca.includes('BEGIN CERTIFICATE')||typeof tls.servername!=='string'||!/^[a-zA-Z0-9_.-]{1,253}$/.test(tls.servername)||!!tls.cert!==!!tls.key||['cert','key'].some(key=>tls[key]!==undefined&&typeof tls[key]!=='string'))throw invalid();
 return value;
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{
  const args=process.argv.slice(2),command=usageRollupArguments(args);
  if(cascadeActions.includes(command.action)){
   const input=await readUsageCascadeInput(process.stdin);console.log(JSON.stringify(await runUsageRollupsCommand(args,{connection:input.connection},{cascade:input.cascade})));
  }else console.log(JSON.stringify(await runUsageRollupsCommand(args)));
 }catch(error){
  if(process.argv[2]?.startsWith('cascade-'))console.error(JSON.stringify({ok:false,code:typeof error?.code==='string'&&/^[A-Za-z0-9_]{1,80}$/.test(error.code)?error.code:'usage_cascade_operation_failed'}));
  else console.error(error instanceof AppError?error.message:'小时用量汇总操作未完成，请核对数据库权限、结构与并发状态');process.exitCode=1;
 }
}
