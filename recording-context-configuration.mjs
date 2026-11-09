import {AppError} from './store.mjs';
import {postgresContextSchemaVersions} from './postgres-store.mjs';

// Called only with the already verified database version and server-owned config.
// Availability does not enable capture and never grants migration authority.
export const contextCaptureGuardContract='v1';
export function recordingContextConfiguration({env={},enabled,allowed,schemaVersion}={}){
 const flag=env.HARBOR_RECORDING_CONTEXT_ENABLED;
 if(flag!==undefined&&!['0','1'].includes(flag)||enabled!==undefined&&typeof enabled!=='boolean'||allowed!==undefined&&typeof allowed!=='boolean')throw new AppError(503,'上下文采集开关须为0或1，构造选项及持久许可须为布尔值','recording_configuration');
 const captureEnabled=enabled??flag==='1',available=[postgresContextSchemaVersions.storage,postgresContextSchemaVersions.index].includes(schemaVersion);
 if(captureEnabled&&!available)throw new AppError(503,'上下文结构尚未迁移','database_migration_required');
 return {enabled:captureEnabled&&allowed!==false,available};
}
