import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {AppError} from './store.mjs';
// Existing BIGINT expiry columns stay compatible with backups and old readers.
// The maximum supported JavaScript date is the wire sentinel for no expiry.
export const unlimitedRecordingExpiry=8640000000000000;
export const isRecordingRetentionDays=value=>value===null||Number.isInteger(value)&&value>=1&&value<=365;
export const recordingRetentionDays=plan=>plan.days===0?null:plan.days;
const migrationName='schema.recording-retention-unlimited';
export async function migrateRecordingRetention(store,{apply=false}={}){
 const saved=await store.get('SELECT value FROM harbor.gateway_settings WHERE name=$1',migrationName);
 if(!saved&&!apply)return false;
 const sql=await readFile(new URL('./migrations/recording-retention-unlimited.sql',import.meta.url),'utf8'),checksum=createHash('sha256').update(sql).digest('hex');
 if(saved){const value=JSON.parse(saved.value);if(value.version!==1||value.checksum!==checksum)throw new AppError(503,'正文无限期保留迁移校验不一致','database_migration_conflict');return false;}
 await store.query(sql);await store.run('INSERT INTO harbor.gateway_settings(name,value) VALUES($1,$2)',migrationName,JSON.stringify({version:1,checksum}));return true;
}
export async function requireUnlimitedRecordingRetention(store){
 if(!await store.get('SELECT value FROM harbor.gateway_settings WHERE name=$1',migrationName))throw new AppError(503,'请先安装正文无限期保留兼容迁移','database_migration_required');
 await migrateRecordingRetention(store);
}
