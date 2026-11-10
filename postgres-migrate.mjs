import {PostgresStore} from './postgres-store.mjs';
import {AppError} from './store.mjs';
import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';

export function postgresMigrationArguments(args){
  if(Array.isArray(args)&&args.length===0)return {};
  if(Array.isArray(args)&&args.length===1){
    if(args[0]==='--recording-context=storage')return {recordingContext:true};
    if(args[0]==='--recording-context=index')return {recordingContextIndex:true};
    if(args[0]==='--recording-retention=unlimited')return {recordingRetentionUnlimited:true};
  }
  throw new AppError(400,'迁移参数无效，仅支持 --recording-context=storage、--recording-context=index 或 --recording-retention=unlimited','database_configuration');
}
export async function migratePostgres(options={},migrationOptions={}){
  const connectionString=options.connectionString??(options.connection?undefined:process.env.HARBOR_DATABASE_URL);
  const store=new PostgresStore({...options,connectionString,statementTimeoutMs:120000});
  try{return await store.migrate(migrationOptions);}finally{await store.close();}
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  try{console.log(JSON.stringify(await migratePostgres({},postgresMigrationArguments(process.argv.slice(2)))));}
  catch(e){console.error(e.status?e.message:'PostgreSQL迁移未完成，请核对数据库权限和连接配置');process.exitCode=1;}
}
