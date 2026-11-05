import {PostgresStore} from './postgres-store.mjs';
import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';

export async function migratePostgres(options={}){
  const connectionString=options.connectionString??(options.connection?undefined:process.env.HARBOR_DATABASE_URL);
  const store=new PostgresStore({...options,connectionString,statementTimeoutMs:120000});
  try{return await store.migrate();}finally{await store.close();}
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  try{console.log(JSON.stringify(await migratePostgres()));}
  catch(e){console.error(e.status?e.message:'PostgreSQL迁移未完成，请核对数据库权限和连接配置');process.exitCode=1;}
}
