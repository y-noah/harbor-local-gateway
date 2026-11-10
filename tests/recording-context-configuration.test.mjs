import test from 'node:test';import assert from 'node:assert/strict';
import {recordingContextConfiguration} from '../recording-context-configuration.mjs';
import {postgresMigrationArguments} from '../postgres-migrate.mjs';
test('上下文可用性不自动启用，服务端开关只接受0/1且构造覆盖保持原契约',()=>{
 for(const schemaVersion of [22,23,24]){
  assert.deepEqual(recordingContextConfiguration({schemaVersion}),{enabled:false,available:schemaVersion!==22});
  assert.equal(recordingContextConfiguration({schemaVersion,env:{HARBOR_RECORDING_CONTEXT_ENABLED:'0'}}).enabled,false);
  assert.equal(recordingContextConfiguration({schemaVersion,env:{HARBOR_RECORDING_CONTEXT_ENABLED:'1'},enabled:false}).enabled,false);
 }
 for(const schemaVersion of [23,24])assert.equal(recordingContextConfiguration({schemaVersion,env:{HARBOR_RECORDING_CONTEXT_ENABLED:'1'}}).enabled,true);
 assert.throws(()=>recordingContextConfiguration({schemaVersion:22,env:{HARBOR_RECORDING_CONTEXT_ENABLED:'1'}}),{code:'database_migration_required'});
 for(const flag of ['',null,true,1,'true','01','1\n'])assert.throws(()=>recordingContextConfiguration({schemaVersion:24,env:{HARBOR_RECORDING_CONTEXT_ENABLED:flag},enabled:false}),{code:'recording_configuration'});
 for(const enabled of [null,'true',1,{}])assert.throws(()=>recordingContextConfiguration({schemaVersion:24,enabled}),{code:'recording_configuration'});
 for(const allowed of [undefined,true,false])for(const enabled of [undefined,true,false]){
  assert.equal(recordingContextConfiguration({schemaVersion:24,env:{HARBOR_RECORDING_CONTEXT_ENABLED:'1'},enabled,allowed}).enabled,enabled!==false&&allowed!==false);
 }
 for(const allowed of [null,'true',1,{}])assert.throws(()=>recordingContextConfiguration({schemaVersion:24,allowed}),{code:'recording_configuration'});
});
test('显式迁移CLI严格限定两个选项，默认不扩大迁移范围',()=>{
 assert.deepEqual(postgresMigrationArguments([]),{});assert.deepEqual(postgresMigrationArguments(['--recording-context=storage']),{recordingContext:true});assert.deepEqual(postgresMigrationArguments(['--recording-context=index']),{recordingContextIndex:true});assert.deepEqual(postgresMigrationArguments(['--recording-retention=unlimited']),{recordingRetentionUnlimited:true});
 for(const args of [null,{},['--recording-context'],['--recording-context=0'],['--recording-context=24'],['--recording-context=index','--recording-context=storage'],['--recording-context=index','--recording-context=index'],['postgresql://synthetic'],['--help']])assert.throws(()=>postgresMigrationArguments(args),{code:'database_configuration'});
});
