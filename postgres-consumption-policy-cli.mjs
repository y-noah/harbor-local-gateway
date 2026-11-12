import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';
import {PostgresStore} from './postgres-store.mjs';
import {AppError} from './store.mjs';
import {migrateConsumptionPolicy,verifyConsumptionPolicyAvailability} from './postgres-consumption-policy.mjs';

const invalid=()=>new AppError(400,'Key消耗来源参数无效；使用 install [--runtime-role=角色] 或 verify [--require-installed]','database_configuration');
export function consumptionPolicyArguments(args){
 if(!Array.isArray(args)||args.some(value=>typeof value!=='string'))throw invalid();
 const [action='verify',...flags]=args;if(!['install','verify'].includes(action))throw invalid();
 const result={action};
 for(const flag of flags){
  if(action==='verify'&&flag==='--require-installed'&&!result.requireInstalled){result.requireInstalled=true;continue;}
  if(action==='install'&&flag.startsWith('--runtime-role=')&&result.runtimeRole===undefined){
   const role=flag.slice('--runtime-role='.length);if(!/^[a-z_][a-z0-9_]{0,62}$/.test(role))throw invalid();result.runtimeRole=role;continue;
  }
  throw invalid();
 }
 return result;
}
// Installation is an explicit owner operation. Verification is a read-only,
// finite transaction and never applies core or optional migrations.
export async function runConsumptionPolicyCommand(args=[],database={}){
 const command=consumptionPolicyArguments(args),connectionString=database.connectionString??(database.connection?undefined:process.env.HARBOR_DATABASE_URL);
 const store=new PostgresStore({...database,connectionString});
 try{
  if(command.action==='verify')return await store.transaction(async()=>{
   await store.verifySchema();const installed=await verifyConsumptionPolicyAvailability(store,{required:command.requireInstalled??false});return {action:'verify',installed};
  },{readOnly:true});
  await store.verifySchema();
  const owner=await store.get('SELECT current_user name'),tables=await store.all("SELECT pg_get_userbyid(relowner) owner FROM pg_class WHERE oid=ANY(ARRAY['harbor.keys'::regclass,'harbor.requests'::regclass,'harbor.gateway_settings'::regclass])");
  if(tables.length!==3||tables.some(row=>row.owner!==owner.name))throw new AppError(403,'Key消耗来源安装须由原表owner执行','consumption_migration_owner_invalid');
  const changed=await migrateConsumptionPolicy(store,{apply:true,runtimeRole:command.runtimeRole});await verifyConsumptionPolicyAvailability(store,{required:true});return {action:'install',installed:true,changed};
 }finally{await store.close();}
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 try{console.log(JSON.stringify(await runConsumptionPolicyCommand(process.argv.slice(2))));}
 catch(error){
  const safe=new Set(['database_configuration','database_migration_conflict','database_migration_required','consumption_runtime_invalid','consumption_runtime_required','consumption_migration_owner_invalid']);
  console.error(JSON.stringify({error:{code:safe.has(error?.code)?error.code:error?.code==='42501'?'database_permission_denied':'consumption_policy_command_failed',message:'Key消耗来源操作未完成，请核对参数、数据库结构和权限'}}));process.exitCode=1;
 }
}
