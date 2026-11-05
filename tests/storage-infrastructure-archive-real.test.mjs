import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm,readFile,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {randomBytes,createHash} from 'node:crypto';
import {createPitrRehearsal} from '../postgres-recovery.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {canonical,writeManifest} from '../migration-artifacts.mjs';
import {inspectInfrastructureArchiveFiles} from '../ci/storage-recovery/archive-observe.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('archive file component requires bound metadata and an actually archived authenticated WAL; no installed-root or Docker authority is granted',{skip:!binDirectory?'需明确新私有PG18':false,timeout:60000},async()=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-archive-component-')),key=randomBytes(32);let rehearsal,store;try{
  rehearsal=await createPitrRehearsal({directory,key,pgBinDir:binDirectory,libraryDirectory});store=new PostgresStore(rehearsal.adminOptions);await rehearsal.mark();await rehearsal.flushWal();let row;const deadline=performance.now()+5000;for(;;){row=await store.get("SELECT system_identifier::text cluster,current_setting('data_directory') directory,(SELECT archived_count FROM pg_stat_archiver)::bigint count,(SELECT last_archived_wal FROM pg_stat_archiver) wal FROM pg_control_system()");if(row.count>0&&/^[0-9A-F]{24}$/.test(row.wal??''))break;assert.ok(performance.now()<deadline,'actual archiver stats did not advance');await new Promise(resolve=>setTimeout(resolve,50));}const observed={cluster:row.cluster,directory:row.directory,archivedCount:row.count,lastArchivedWal:row.wal},configuration={version:1,expectedCluster:row.cluster,uid:process.getuid(),pgDataDirectory:row.directory,archiveDirectory:rehearsal.archive,keyFile:join(directory,'archive.key'),keyVersion:'component-only'},configurationDigest=createHash('sha256').update(canonical(configuration)).digest('hex'),archive={configuration,configurationDigest,key};
  // This private fixture exercises integrity only. It is not an installed
  // profile, container identity or a production backup initialization proof.
  assert.equal((await inspectInfrastructureArchiveFiles({archive,observed})).metadataReady,false);
  const metadata={version:1,scope:'existing-cluster',systemIdentifier:row.cluster,primaryData:row.directory,archiveProfileDigest:configurationDigest,keyVersion:'component-only'};await writeManifest(join(rehearsal.archive,'cluster.enc'),metadata,key,{replace:true});const complete=await inspectInfrastructureArchiveFiles({archive,observed});assert.equal(complete.profileBound,true);assert.equal(complete.metadataReady,true);assert.equal(complete.walArchiveVerified,true,JSON.stringify(complete));
  assert.equal((await inspectInfrastructureArchiveFiles({archive,observed:{...observed,archivedCount:0}})).walArchiveVerified,false);await writeManifest(join(rehearsal.archive,'cluster.enc'),{...metadata,archiveProfileDigest:'0'.repeat(64)},key,{replace:true});assert.equal((await inspectInfrastructureArchiveFiles({archive,observed})).metadataReady,false);await writeManifest(join(rehearsal.archive,'cluster.enc'),metadata,key,{replace:true});
  const file=join(rehearsal.archive,row.wal+'.enc'),bytes=await readFile(file);bytes[30]^=1;await writeFile(file,bytes);const tampered=await inspectInfrastructureArchiveFiles({archive,observed});assert.equal(tampered.metadataReady,true);assert.equal(tampered.walArchiveVerified,false);assert.deepEqual(tampered.missing,['actual_wal_integrity']);
 }finally{await store?.close();await rehearsal?.close();key.fill(0);await rm(directory,{recursive:true,force:true});}
});
