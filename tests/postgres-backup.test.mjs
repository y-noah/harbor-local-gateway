import test from 'node:test';
import assert from 'node:assert/strict';
import {createTaskPostgresBackupContext,createPostgresBackupContext,postgresBackupContext} from '../postgres-backup-context.mjs';
import {createPostgresBaseBackup,inspectPostgresBackup,checkpointPostgresBackup} from '../postgres-backup.mjs';
import {storageBackupCommand} from '../storage-backup.mjs';
test('backup and WAL proofs cannot be supplied as JSON or copied contexts',async()=>{
 assert.throws(()=>postgresBackupContext({scope:'existing-cluster'}),{code:'backup_context_required'});assert.throws(()=>inspectPostgresBackup({backupId:'synthetic',restoreVerified:true}),{code:'backup_proof_required'});
 await assert.rejects(checkpointPostgresBackup({scope:'existing-cluster',passed:true}),{code:'backup_proof_required'});await assert.rejects(createPostgresBaseBackup({}),{code:'backup_context_required'});await assert.rejects(createTaskPostgresBackupContext({laboratory:{recovery:true}}),{code:'backup_task_ownership_required'});
});
test('management entry rejects arbitrary URLs, commands, profile paths and WAL traversal before connection',async()=>{
 for(const args of [['backup','postgresql://synthetic'],['run','shell'],['archive-wal','../../file'],['archive-wal','000000010000000000000001','extra'],['baseline-handoff'],['baseline-handoff','postgresql://synthetic'],['baseline-handoff','../../file']])await assert.rejects(storageBackupCommand(args));
 await assert.rejects(createPostgresBackupContext('/tmp/caller-profile.json'),{code:'backup_root_profile_required'});
});

test('private source server settings reject arbitrary GUCs and malformed numeric limits before launch',async()=>{
 const {validateRehearsalServerSettings}=await import('../postgres-recovery.mjs');assert.deepEqual(validateRehearsalServerSettings(undefined),{});assert.deepEqual(validateRehearsalServerSettings({max_connections:150}),{max_connections:150});for(const value of [{archive_command:'arbitrary'},{max_connections:true},{max_connections:1.1},{max_connections:0},{max_connections:1001},[],null])assert.throws(()=>validateRehearsalServerSettings(value),{code:'recovery_server_settings_invalid'});
});
test('root can read authenticated 0600 PostgreSQL-owned archive manifests without weakening owner checks',{skip:process.getuid()!==0?'本机无免密sudo；在root CI执行独立PG属主验证':false},async()=>{
 const {mkdtemp,chown,chmod,rm}=await import('node:fs/promises'),{join}=await import('node:path'),{tmpdir}=await import('node:os'),{randomBytes}=await import('node:crypto'),{writeManifest,readManifest}=await import('../migration-artifacts.mjs'),{readWalManifest}=await import('../postgres-recovery.mjs'),root=await mkdtemp(join(tmpdir(),'harbor-root-wal-')),key=randomBytes(32),path=join(root,'cluster.enc'),ownerUid=65534;try{await writeManifest(path,{version:1,systemIdentifier:'123'},key);await chown(path,ownerUid,ownerUid);await assert.rejects(readManifest(path,key),{code:'transfer_manifest_not_private'});assert.equal((await readWalManifest(path,key,{ownerUid})).systemIdentifier,'123');await assert.rejects(readWalManifest(path,key,{ownerUid:0}),{code:'recovery_manifest_not_private'});await chmod(path,0o644);await assert.rejects(readWalManifest(path,key,{ownerUid}),{code:'recovery_manifest_not_private'});}finally{await rm(root,{recursive:true,force:true});key.fill(0);}
});
