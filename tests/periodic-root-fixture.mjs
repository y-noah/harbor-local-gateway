import assert from 'node:assert/strict';
import {spawn,execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {createServer} from 'node:net';
import {randomBytes,randomUUID,createHash} from 'node:crypto';
import {mkdir,mkdtemp,writeFile,readFile,lstat,realpath,chown,chmod,appendFile,rm,readlink} from 'node:fs/promises';
import {join,dirname,resolve} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {setTimeout as delay} from 'node:timers/promises';

const exec=promisify(execFile),installedSource=resolve(dirname(fileURLToPath(import.meta.url)),'..');
const quote=value=>"'"+String(value).replaceAll("'","'\\''")+"'";
async function trustedDirectory(directory){
  for(let at=directory;;at=dirname(at)){const i=await lstat(at);assert.ok(i.isDirectory()&&!i.isSymbolicLink()&&i.uid===0&&!(i.mode&0o022),'fixture requires root-owned, immutable ancestors');if(at==='/')break;}
}
async function privateDirectory(directory,uid,gid){await mkdir(directory,{mode:0o700});await chown(directory,uid,gid);return directory;}
async function secret(path,value,uid=0,gid=0){await writeFile(path,value,{flag:'wx',mode:0o600});await chown(path,uid,gid);}
async function freePort(){const server=createServer();await new Promise((yes,no)=>{server.once('error',no);server.listen(0,'127.0.0.1',yes);});const port=server.address().port;await new Promise(yes=>server.close(yes));return port;}

// The caller first copies this source and its complete dependencies under a
// root-owned /var/lib ancestor, then execs that installation's Node in a new
// mount namespace. No user namespace or host /etc writes grant test authority.
export async function createPeriodicRootFixture({directory,uid=65534,gid=65534,nodeFile=process.execPath,sourceDirectory=installedSource,pgBinDirectory=process.env.HARBOR_CAPACITY_PG_BIN,pgLibraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,preserve=false}={}){
  assert.equal(process.platform,'linux');assert.equal(process.getuid(),0,'requires actual root');
  assert.equal(process.env.HARBOR_PERIODIC_ROOT_TEST,'1','root fixture must be explicitly enabled');
  assert.notEqual(await readlink('/proc/self/ns/mnt'),await readlink('/proc/1/ns/mnt'),'requires a private mount namespace');
  assert.equal(uid,65534);assert.equal(gid,65534);assert.equal(await realpath(nodeFile),await realpath(process.execPath));
  sourceDirectory=await realpath(sourceDirectory);assert.equal(sourceDirectory,installedSource,'execute the copied trusted installation');
  for(const path of [sourceDirectory,dirname(await realpath(nodeFile)),pgBinDirectory,pgLibraryDirectory]){assert.ok(path&&resolve(path)===path);await trustedDirectory(path);}
  assert.ok(!sourceDirectory.startsWith('/tmp/'),'untrusted temporary ancestor is not an installation');
  if(directory===undefined)directory=await mkdtemp('/var/lib/harbor-periodic-root-');else{assert.ok(directory.startsWith('/var/lib/')&&resolve(directory)===directory);await mkdir(directory,{mode:0o750});}
  await chown(directory,0,gid);await chmod(directory,0o750);await trustedDirectory(directory);
  const root=directory,pgdata=join(root,'pgdata'),socket=join(root,'socket'),archive=join(root,'wal'),restore=join(root,'restore'),artifacts=join(root,'artifacts'),tls=join(root,'tls'),password=randomBytes(32).toString('hex'),archiveKey=randomBytes(32),applicationKey=randomBytes(32),master=randomBytes(32),recording=randomBytes(32),nonce=randomUUID(),port=await freePort();
  assert.ok(Buffer.byteLength(join(restore,'123456789012','socket','.s.PGSQL.55479'))<108);
  const env={PATH:'/usr/local/bin:/usr/bin:/bin',LD_LIBRARY_PATH:pgLibraryDirectory,OPENSSL_CONF:'/dev/null',HARBOR_BACKUP_RESTORE_TOKEN:nonce};
  const tool=(name,args,options={})=>exec(join(pgBinDirectory,name),args,{uid,gid,env,timeout:30000,maxBuffer:65536,...options});
  let store,context,ca,child,owner,etcMounted=false,closePromise;
  const imported=name=>import(pathToFileURL(join(sourceDirectory,name)).href);
  const processes=await imported('postgres-backup-processes.mjs');
  const close=()=>closePromise??=(async()=>{
    const errors=[];
    try{if(context)await (await imported('postgres-backup-context.mjs')).closePostgresBackupContext(context);}catch(error){errors.push(error);}
    try{await store?.close();}catch(error){errors.push(error);}
    try{await ca?.issuer.close();}catch(error){errors.push(error);}
    try{
      if(child&&child.exitCode===null&&child.signalCode===null){
        const rows=await processes.restoreOwnedProcesses(owner);assert.ok(rows.some(row=>row.pid===child.pid));
        assert.equal(Number((await readFile(join(pgdata,'postmaster.pid'),'utf8')).split('\n')[0]),child.pid);
        await tool('pg_ctl',['-D',pgdata,'-m','fast','-w','-t','15','stop'],{timeout:20000});
        const end=Date.now()+5000;while(child.exitCode===null&&child.signalCode===null){assert.ok(Date.now()<end,'source PG did not exit');await delay(10);}
      }
      if(owner)assert.equal((await processes.restoreOwnedProcesses(owner)).length,0);
      await processes.assertPostgresRestoreDirectoryUnused(root);
    }catch(error){errors.push(error);}
    archiveKey.fill(0);applicationKey.fill(0);master.fill(0);recording.fill(0);
    if(errors.length){const failure=new AggregateError(errors,'periodic_root_fixture_cleanup_unverified');failure.retainedDirectory=root;throw failure;}
    if(etcMounted){await exec('/bin/umount',['/etc'],{timeout:10000});etcMounted=false;}
    if(!preserve)await rm(root,{recursive:true,force:false});
    return {postgresExitedVerified:true,namespaceEtcRestored:true,syntheticDirectoryRemoved:!preserve};
  })();
  try{
    const machine=await readFile('/etc/machine-id');assert.match(machine.toString().trim(),/^[a-f0-9]{32}$/);
    await exec('/bin/mount',['--make-rprivate','/'],{timeout:10000});
    await exec('/bin/mount',['-t','tmpfs','-o','mode=0755,nosuid,nodev','harbor-periodic-test-etc','/etc'],{timeout:10000});etcMounted=true;
    await writeFile('/etc/machine-id',machine,{mode:0o644});
    await writeFile('/etc/passwd','root:x:0:0:root:/root:/bin/sh\nnobody:x:65534:65534:synthetic:/nonexistent:/usr/sbin/nologin\n',{mode:0o644});
    await writeFile('/etc/group','root:x:0:\nnogroup:x:65534:\n',{mode:0o644});
    await writeFile('/etc/nsswitch.conf','passwd: files\ngroup: files\nhosts: files dns\n',{mode:0o644});
    await writeFile('/etc/hosts','127.0.0.1 localhost\n::1 localhost\n',{mode:0o644});
    for(const path of [pgdata,socket,archive,restore,tls])await privateDirectory(path,uid,gid);
    await privateDirectory(artifacts,0,0);
    const passwordFile=join(root,'database-password'),archiveKeyFile=join(root,'archive.key'),applicationKeyFile=join(root,'application.key');await secret(passwordFile,password+'\n',uid,gid);await secret(archiveKeyFile,archiveKey,uid,gid);assert.notDeepEqual(applicationKey,archiveKey);await secret(applicationKeyFile,applicationKey);
    const applicationEncryption={version:1,keyFile:applicationKeyFile,keyVersion:'root-application-v3',keyDigest:createHash('sha256').update(Buffer.concat([Buffer.from('harbor-periodic-application-key-v1\0'),applicationKey])).digest('hex')};
    await tool('initdb',['-D',pgdata,'-U','harbor_root_admin','--auth-local=scram-sha-256','--auth-host=scram-sha-256','--pwfile',passwordFile,'--no-locale','--encoding=UTF8']);
    const certs=await imported('tests/device-fixture.mjs');const previous=process.env.OPENSSL_CONF;process.env.OPENSSL_CONF='/dev/null';try{ca=await certs.certificateFixture(tls);}finally{if(previous===undefined)delete process.env.OPENSSL_CONF;else process.env.OPENSSL_CONF=previous;}
    for(const name of ['ca.pem','server.pem','server-key.pem']){await chown(join(tls,name),uid,gid);await chmod(join(tls,name),0o600);}
    const archiveCommand=[nodeFile,join(sourceDirectory,'storage-backup.mjs'),'archive-wal','%f'].map(quote).join(' ');
    const setting=value=>"'"+value.replaceAll("'","''")+"'";
    await appendFile(join(pgdata,'postgresql.conf'),`\nlisten_addresses='127.0.0.1'\nport=${port}\nunix_socket_directories=${setting(socket)}\nssl=on\nssl_cert_file=${setting(join(tls,'server.pem'))}\nssl_key_file=${setting(join(tls,'server-key.pem'))}\narchive_mode=on\narchive_timeout=60\narchive_command=${setting(archiveCommand)}\nmax_connections=30\nlog_statement='none'\n`);
    child=spawn(join(pgBinDirectory,'postgres'),['-D',pgdata],{uid,gid,env,detached:true,stdio:['ignore','ignore','pipe']});let startupError;child.once('error',error=>{startupError=error;});child.stderr.resume();
    owner={pid:child.pid,uid,nonce,members:new Map(),data:pgdata};
    const {PostgresStore}=await imported('postgres-store.mjs'),connectionString=new URL('postgresql://127.0.0.1:'+port+'/postgres');connectionString.username='harbor_root_admin';connectionString.password=password;connectionString.searchParams.set('sslmode','verify-full');connectionString.searchParams.set('sslrootcert',join(tls,'ca.pem'));
    store=new PostgresStore({connectionString:connectionString.toString(),poolSize:2});
    const deadline=Date.now()+20000;for(;;){if(startupError)throw startupError;assert.equal(child.exitCode,null,'synthetic source PG exited');try{await store.get('SELECT 1 n');break;}catch(error){if(Date.now()>=deadline)throw error;await delay(25);}}
    await processes.restoreOwnedProcesses(owner);
    await store.migrate();await store.initializePricing();
    const materialRoots=[];for(const id of ['data','runtime','secrets'])materialRoots.push({id,directory:await privateDirectory(join(root,id),uid,gid)});
    const data=materialRoots[0].directory,runtime=materialRoots[1].directory,secrets=materialRoots[2].directory;
    await privateDirectory(join(data,'recording-spool'),uid,gid);await privateDirectory(join(secrets,'device'),uid,gid);
    await secret(join(data,'settings.json'),JSON.stringify({port:8092,codexBinary:nodeFile,adminPasswordHash:{salt:'a'.repeat(32),digest:'b'.repeat(64)}}),uid,gid);
    for(const [name,key] of [['key-encryption.key',master],['recording-encryption.key',recording]])await secret(join(data,name),key,uid,gid);
    for(const [name,value] of [['ca.pem',ca.ca],['server.pem',ca.cert],['server.key',ca.key],['ca.key',await readFile(join(tls,'ca-key.pem'))]])await secret(join(secrets,'device',name),value,uid,gid);
    await secret(join(secrets,'postgres-report.key'),randomBytes(32),uid,gid);await secret(join(secrets,'postgres-ca.crt'),ca.ca,uid,gid);
    const {createPostgresMemberAdmin}=await imported('postgres-member-admin.mjs'),{encryptStoredKey,hash}=await imported('store.mjs');
    const member=(await createPostgresMemberAdmin(store,{encryptionKey:master}).handle('/api/admin/members','POST',{name:'root周期合成员工',expires:null,quota:null,allowed_models:null})).value;
    const account='synthetic-'+randomUUID(),upstream='synthetic-upstream-'+randomUUID(),databasePrefix='/synthetic-container/runtime';materialRoots[1].databasePrefix=databasePrefix;
    await store.run("INSERT INTO harbor.accounts(id,name,home,state,auth_mode,api_secret_cipher,api_secret_digest) VALUES($1,'合成离线上游',$2,'ready','openai_api',$3,$4)",account,databasePrefix,encryptStoredKey(master,upstream,'upstream:'+account),hash(upstream));
    const actor=await store.authenticate(member.secret),requestId=await store.observe(actor,'synthetic','synthetic','gpt-6.1-sol'),{RecordingStore}=await imported('recording-store.mjs'),{createRecordingIndexCrypto}=await imported('recording-search-index.mjs'),records=new RecordingStore(store,{encryptionKey:recording}),crypto=createRecordingIndexCrypto(recording);
    try{await records.start(requestId,{turnState:'known'});await records.append(requestId,[{sourceKey:'synthetic-user',kind:'user',provenance:'client',content:{text:'synthetic root periodic body'}}]);await records.finish(requestId);
      const revision=(await store.get('SELECT revision::text revision FROM harbor.recording_work WHERE request_id=$1',requestId)).revision,at=Date.now(),part={kind:'user',field:'synthetic-field',text:'synthetic derived body',searchText:'synthetic derived body'},bytes=Buffer.byteLength(JSON.stringify(part));
      await store.run("INSERT INTO harbor.recording_documents VALUES($1,$2,1,$3,'complete','known',$4,1,$5,$6)",requestId,revision,at,at+86400000,bytes,crypto.keyVersion);await store.run("INSERT INTO harbor.recording_document_parts VALUES($1,1,'user','synthetic-field',$2,$3,'{}')",requestId,crypto.seal(part,'document:'+requestId+':'+revision+':1'),bytes);
      const exportId=randomUUID(),text='synthetic encrypted export';await store.run("INSERT INTO harbor.background_jobs(id,kind,status,available_at,created,updated) VALUES($1,'recording.export','completed',$2,$2,$2)",exportId,at);await store.run("INSERT INTO harbor.recording_exports(id,created,expires,actor,options,visibility,status,rows,bytes,chunks) VALUES($1,$2,$3,'synthetic','{}',pg_current_snapshot(),'complete',1,$4,1)",exportId,at,at+86400000,Buffer.byteLength(text));await store.run('INSERT INTO harbor.recording_export_chunks VALUES($1,1,$2,$3)',exportId,Buffer.byteLength(text),crypto.seal({text},'export:'+exportId+':1'));
    }finally{records.key.fill(0);}
    await store.run("INSERT INTO harbor.gateway_settings(name,value) VALUES('recording.key.binding',$1)",JSON.stringify({version:1,digest:createHash('sha256').update(Buffer.concat([Buffer.from('harbor-recording-key-binding-v1\0'),recording])).digest('hex')}));
    const {backupInventory}=await imported('postgres-backup-inventory.mjs'),inventory=await backupInventory(store),expectedCluster=(await store.get('SELECT system_identifier::text id FROM pg_control_system()')).id,database=await store.get('SELECT current_database() name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) oid');
    const archiveProfile={version:1,expectedCluster,uid,pgDataDirectory:pgdata,archiveDirectory:archive,keyFile:archiveKeyFile,keyVersion:'synthetic-root-v1'};
    await writeFile('/etc/harbor-storage-archive.json',JSON.stringify(archiveProfile),{flag:'wx',mode:0o644});
    const profile={version:1,dedicatedCluster:true,expectedCluster,...inventory,artifactDirectory:artifacts,pgBinDirectory,libraryDirectory:pgLibraryDirectory,archiveProfile:'/etc/harbor-storage-archive.json',processTimeoutMs:120000,maxBackupBytes:1073741824,maxWalSegments:128,checkpointMode:'fast',restoreDirectory:restore,applicationEncryption};
    await secret('/etc/harbor-storage-backup.json',JSON.stringify(profile));await secret('/etc/harbor-storage-operator.env','HARBOR_STORAGE_OPERATOR_ADMIN_DATABASE_URL='+connectionString.toString()+'\n');
    await secret('/etc/harbor-storage-application.json',JSON.stringify({version:1,enabled:true,database,materialRoots,publicOrigin:'http://localhost:18094',deviceOrigin:'https://localhost:18095',maxBytes:16777216,maxFiles:100}));
    await (await imported('storage-backup.mjs')).storageBackupCommand(['prepare-archive']);
    const backup=await imported('postgres-backup-context.mjs');context=await backup.createPostgresBackupContext();assert.equal(context.scope,'existing-cluster');
    const policy=await (await imported('postgres-periodic-application-policy.mjs')).loadPeriodicApplicationPolicy();
    const tlsVerified=(await store.get('SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()')).ssl;assert.equal(tlsVerified,true);
    return {context,policy,store,materialRoots,directory:root,pgdata,nodeFile,sourceDirectory,pgBinDirectory,pgLibraryDirectory,uid,gid,member,requestId,account,tlsVerified,applicationKeyFile,applicationEncryption,postgresPid:child.pid,close};
  }catch(error){try{await close();}catch(cleanup){throw new AggregateError([error,cleanup],'periodic_root_fixture_initialize_cleanup_failed',{cause:error});}throw error;}
}
