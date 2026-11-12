import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';
import {PostgresStore} from './postgres-store.mjs';
import {AppError} from './store.mjs';
import {migrateUsageRollups,verifyUsageRollupAvailability,usageRollupsReady,backfillUsageRollups,reconcileUsageRollups,disableUsageRollups} from './postgres-usage-rollups.mjs';

const invalid=()=>new AppError(400,'小时用量汇总参数无效；使用 install、backfill、verify 或 disable','database_configuration');
export function usageRollupArguments(args){
 if(!Array.isArray(args)||args.some(arg=>typeof arg!=='string'))throw invalid();
 const [action='verify',...flags]=args;if(!['install','backfill','verify','disable'].includes(action))throw invalid();
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
export async function runUsageRollupsCommand(args=[],database={}, {clock=Date.now}={}){
 const command=usageRollupArguments(args),connectionString=database.connectionString??(database.connection?undefined:process.env.HARBOR_DATABASE_URL);
 const store=new PostgresStore({...database,connectionString,statementTimeoutMs:command.statementTimeoutMs??30000});
 try{
  await store.verifySchema();
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
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{console.log(JSON.stringify(await runUsageRollupsCommand(process.argv.slice(2))));}
 catch(error){console.error(error instanceof AppError?error.message:'小时用量汇总操作未完成，请核对数据库权限、结构与并发状态');process.exitCode=1;}
}
