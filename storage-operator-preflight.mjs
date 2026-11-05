import {transferError} from './migration-artifacts.mjs';
export async function cleanupStorageOperatorPreflight({name,imageId,token,inspect,remove}={}){
  if(!/^harbor-storage-preflight-[a-f0-9]{24}$/.test(name??'')||!/^sha256:[a-f0-9]{64}$/.test(imageId??'')||!/^[a-f0-9]{32}$/.test(token??'')||typeof inspect!=='function'||typeof remove!=='function')throw transferError('storage_operator_preflight_cleanup_identity');
  const row=await inspect(name);if(!row)return {removed:true,alreadyAbsent:true};
  if(row.Name!=='/'+name||row.Image!==imageId||row.Config?.Labels?.['com.harbor.storage-operator-preflight']!=='true'||row.Config?.Labels?.['com.harbor.storage-operator-preflight-token']!==token||JSON.stringify(row.Config?.Entrypoint)!==JSON.stringify(['node'])||JSON.stringify(row.Config?.Cmd)!==JSON.stringify(['/app/ci/postgres-preflight.mjs']))throw transferError('storage_operator_preflight_cleanup_identity');
  await remove(name,{force:true});if(await inspect(name))throw transferError('storage_operator_preflight_cleanup_unknown');return {removed:true,alreadyAbsent:false};
}
