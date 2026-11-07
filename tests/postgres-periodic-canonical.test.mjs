import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {mkdir,writeFile,readFile,rename,unlink} from 'node:fs/promises';
import {join} from 'node:path';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createTaskPostgresBackupContext,closePostgresBackupContext,postgresBackupContext} from '../postgres-backup-context.mjs';
import {createPeriodicSourceBase} from '../postgres-backup.mjs';
import {publishPeriodicBaseVerification} from '../postgres-periodic-base-verification.mjs';
import {capturePeriodicApplicationBackup} from '../postgres-periodic-application-backup.mjs';
import {createPeriodicTransferIndex,releasePeriodicTransferIndex,usePeriodicTransferIndex} from '../postgres-periodic-transfer-index.mjs';
import {authenticatePeriodicApplicationRetentionGroup} from '../postgres-periodic-application-retention.mjs';
import {withPostgresBackupOperation,withBackupArchiveLock} from '../postgres-backup-lock.mjs';
import {readManifest,digestFile} from '../migration-artifacts.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('canonical control-only closure is authenticated by retention and its live references remain leased',{
 skip:!binDirectory?'需自有合成PG18，不连接共享服务':false,timeout:120000
},async t=>{
 const archiveKey=randomBytes(32),applicationKey=randomBytes(32),lab=await createPrivatePostgres({binDirectory,libraryDirectory,recovery:{backupKey:archiveKey}}),store=new PostgresStore({connection:lab.connection});let context,index;
 t.after(async()=>{try{if(index)releasePeriodicTransferIndex(index);if(context)await closePostgresBackupContext(context);}finally{try{await store.close();}finally{await lab.close();archiveKey.fill(0);applicationKey.fill(0);}}});
 await store.migrate();await store.initializePricing();const database=await store.get('SELECT current_database() name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) oid'),materialRoots=[];
 for(const id of ['data','runtime','secrets']){const directory=join(lab.directory,'material-'+id);await mkdir(directory,{mode:0o700});await writeFile(join(directory,'synthetic.txt'),'synthetic canonical material',{mode:0o600});materialRoots.push({id,directory});}
 context=await createTaskPostgresBackupContext({laboratory:lab,directory:join(lab.directory,'source-backups'),archiveKey,applicationKey,binDirectory,libraryDirectory});
 const proof=await createPeriodicSourceBase(context);await publishPeriodicBaseVerification(proof);
 const point=await capturePeriodicApplicationBackup(context,proof,{version:1,enabled:true,database,materialRoots,publicOrigin:'http://localhost:18094',deviceOrigin:'https://localhost:18095',maxBytes:4096,maxFiles:20});assert.equal(point.backupComplete,true,point.failureCode);
 const record=postgresBackupContext(context),base=join(record.artifactDirectory,proof.backupId),directory=join(base,'points',point.pointId,'handoff'),mirror=join(lab.directory,'index');await mkdir(mirror,{mode:0o700});index=await createPeriodicTransferIndex(context,proof,point.pointId,{directory:mirror});
 const metadata=await readManifest(join(base,'backup.enc'),archiveKey),authenticate=()=>withPostgresBackupOperation(record,()=>authenticatePeriodicApplicationRetentionGroup(record,base,metadata));
 const inventory=await authenticate();assert.ok(inventory.allowed.has('points/'+point.pointId+'/handoff/handoff.enc'));assert.ok(inventory.segments.length>0);assert.deepEqual(await digestFile(join(directory,'handoff.enc')),await digestFile(join(mirror,'handoff.enc')));
 await usePeriodicTransferIndex(index,async()=>{await assert.rejects(withBackupArchiveLock({directory:record.archiveDirectory,uid:record.restoreUid,exclusive:true},()=>assert.fail('exclusive cleanup must not run during transfer')),{code:'backup_coordination_busy'});});
 const cluster=join(record.archiveDirectory,'cluster.enc'),savedCluster=cluster+'.canonical-test';const clusterBytes=await readFile(cluster);await rename(cluster,savedCluster);try{await assert.rejects(authenticate(),{code:'ENOENT'});assert.deepEqual(await readFile(savedCluster),clusterBytes);}finally{await rename(savedCluster,cluster);}
 const unknown=join(directory,'unknown.enc');await writeFile(unknown,'preserve unknown source bytes',{mode:0o600});try{await assert.rejects(authenticate(),{code:'periodic_transfer_index_conflict'});assert.equal(await readFile(unknown,'utf8'),'preserve unknown source bytes');}finally{await unlink(unknown);}
 const manifest=await readManifest(join(directory,'handoff.enc'),applicationKey),page=join(directory,manifest.parts[0].name),bytes=await readFile(page),damaged=Buffer.from(bytes);damaged[damaged.length-1]^=1;await writeFile(page,damaged);try{await assert.rejects(authenticate(),{code:'backup_handoff_catalog_changed'});assert.deepEqual(await readFile(page),damaged);}finally{await writeFile(page,bytes);}
 assert.ok((await authenticate()).allowed.has('points/'+point.pointId+'/handoff/handoff.enc'));
});
