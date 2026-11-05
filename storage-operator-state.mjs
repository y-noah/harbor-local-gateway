import {createHmac,randomBytes} from 'node:crypto';
import {join} from 'node:path';
import {PostgresStore} from './postgres-store.mjs';
import {canonical,artifactKey,transferError} from './migration-artifacts.mjs';
import {storageOperatorContext} from './storage-operator-context.mjs';
import {initializeRehearsalTarget} from './sqlite-postgres-transfer.mjs';

const fail=code=>{throw transferError(code);},schema='harbor_operator_registry',sessions=new WeakMap();
const ident=value=>{if(typeof value!=='string'||!/^[a-z][a-z0-9_]{0,62}$/.test(value))fail('storage_operator_identifier');return '"'+value+'"';};
const mac=(key,payload)=>createHmac('sha256',key).update('harbor-storage-allocation-v1\0'+canonical(payload)).digest('hex');
const envelope=(key,payload)=>({payload,mac:mac(key,payload)});
function unpack(key,value){if(!value?.payload||value.mac!==mac(key,value.payload))fail('storage_operator_registry_authentication');return value.payload;}
function session(value){const result=sessions.get(value);if(!result||result.closed)fail('storage_operator_registry_required');return result;}
async function catalog(store){
  const namespace=await store.get('SELECT oid::text oid,nspowner::text owner FROM pg_namespace WHERE nspname=$1',schema);
  const relations=await store.all("SELECT c.oid::text oid,c.relname name,c.relkind kind,c.relowner::text owner FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 ORDER BY c.relname",schema);
  const columns=await store.all("SELECT c.relname relation,a.attname name,a.atttypid::text type,a.attnotnull required,pg_get_expr(d.adbin,d.adrelid) expression FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum WHERE n.nspname=$1 AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum",schema);
  const constraints=await store.all("SELECT c.relname relation,con.conname name,pg_get_constraintdef(con.oid) definition FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 ORDER BY c.relname,con.conname",schema);
  return {namespace,relations,columns,constraints};
}
async function verifyRegistry(store,key,identity,expected){
  const actual=await catalog(store);if(!actual.namespace||actual.namespace.owner!==identity.owner)fail('storage_operator_registry_foreign');
  // This version contains only one ordinary metadata table and its primary
  // index. No executable behavior is permitted, even from another namespace.
  if(await store.get('SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=$1',schema)||await store.get('SELECT 1 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1',schema)||await store.get('SELECT 1 FROM pg_rewrite r JOIN pg_class c ON c.oid=r.ev_class JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1',schema)||await store.get('SELECT 1 FROM pg_policy p JOIN pg_class c ON c.oid=p.polrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1',schema))fail('storage_operator_registry_foreign');
  if(await store.get("SELECT 1 FROM pg_namespace n CROSS JOIN LATERAL aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a WHERE n.nspname=$1 AND a.grantee<>n.nspowner",schema)||await store.get("SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a WHERE n.nspname=$1 AND (c.relowner<>$2::oid OR a.grantee<>c.relowner)",schema,identity.owner))fail('storage_operator_registry_permissions');
  // The signed schema catalog includes relation OIDs, attributes and constraints;
  // a same-name replacement cannot become a new creation epoch.
  const comment=await store.get("SELECT obj_description(oid,'pg_namespace') value FROM pg_namespace WHERE nspname=$1",schema);let signed;try{signed=JSON.parse(comment.value);}catch{fail('storage_operator_registry_foreign');}
  if(!signed?.payload)fail('storage_operator_registry_foreign');const record=unpack(key,signed);if(canonical(record)!==canonical({version:1,identity,catalog:actual})||expected&&expected!==actual.namespace.oid)fail('storage_operator_registry_foreign');
  return actual.namespace.oid;
}
// Resume inspection must not initialize missing metadata or acquire a writable
// allocation session merely by opening a signed, existing registry.
export async function openStorageOperatorRegistry(context,{key,cluster,expectedSchemaOid,readOnly=false}={}){
  if(typeof readOnly!=='boolean')fail('storage_operator_registry_mode');
  artifactKey(key);const trusted=storageOperatorContext(context),config=trusted.configuration.registry;if(!config||config.allowMetadataWrites!==true||config.schema!==schema)fail('storage_operator_registry_configuration');
  const store=new PostgresStore({...trusted.adminOptions,poolSize:1});try{
    const identity=await store.get("SELECT (SELECT system_identifier::text FROM pg_control_system()) cluster,current_database() database,(SELECT oid::text FROM pg_database WHERE datname=current_database()) database_oid,(SELECT oid::text FROM pg_roles WHERE rolname=current_user) owner");
    if(identity.cluster!==cluster||identity.database!==config.database||identity.database_oid!==config.databaseOid)fail('storage_operator_registry_database_identity');
    if(!readOnly)await store.transaction(async()=>{
      await store.query("SELECT pg_advisory_xact_lock(19028731,668190)");
      if(!await store.get('SELECT 1 FROM pg_namespace WHERE nspname=$1',schema)){
        if(expectedSchemaOid)fail('storage_operator_registry_foreign');
        await store.query('CREATE SCHEMA '+schema);await store.query('REVOKE ALL ON SCHEMA '+schema+' FROM PUBLIC');
        await store.query('CREATE TABLE '+schema+'.runs(run_id text PRIMARY KEY,record jsonb NOT NULL)');await store.query('REVOKE ALL ON TABLE '+schema+'.runs FROM PUBLIC');
        const signed=envelope(key,{version:1,identity,catalog:await catalog(store)});await store.query('COMMENT ON SCHEMA '+schema+" IS '"+JSON.stringify(signed).replaceAll("'","''")+"'");
      }
    });
    const schemaOid=await store.transaction(()=>verifyRegistry(store,key,identity,expectedSchemaOid),{level:'REPEATABLE READ',readOnly:true}),value=Object.freeze({});sessions.set(value,{store,key:Buffer.from(key),identity,schemaOid,readOnly,closed:false});return value;
  }catch(error){await store.close();throw error;}
}
export async function closeStorageOperatorRegistry(value){const record=session(value);record.closed=true;try{await record.store.close();}finally{record.key.fill(0);}}
export function inspectStorageOperatorRegistry(value){const record=session(value);return Object.freeze({...record.identity,schemaOid:record.schemaOid});}
export async function readStorageOperatorAllocation(value,state){
  const record=session(value);await verifyRegistry(record.store,record.key,record.identity,record.schemaOid);
  const row=await record.store.get('SELECT record FROM '+schema+'.runs WHERE run_id=$1',state.runId);if(!row)return null;
  const payload=unpack(record.key,row.record);if(payload.version!==1||payload.runId!==state.runId||payload.cluster!==state.cluster||payload.configurationDigest!==state.configurationDigest||canonical(payload.intent)!==canonical(state.allocation?.intent))fail('storage_operator_allocation_binding');return payload;
}
async function persist(record,payload){if(record.readOnly)fail('storage_operator_registry_readonly');await record.store.transaction(async()=>{await record.store.query('LOCK TABLE '+schema+'.runs IN SHARE ROW EXCLUSIVE MODE');await verifyRegistry(record.store,record.key,record.identity,record.schemaOid);await record.store.run('INSERT INTO '+schema+'.runs VALUES($1,$2::jsonb) ON CONFLICT(run_id) DO UPDATE SET record=EXCLUDED.record',payload.runId,JSON.stringify(envelope(record.key,payload)));});}
async function exactRole(store,name,oid){const row=await store.get('SELECT oid::text oid FROM pg_roles WHERE rolname=$1',name);if(!row||row.oid!==oid)fail('storage_operator_role_generation');return row;}
export async function verifyStorageOperatorAllocation(value,state,{manifest,databaseOid}={}){
  const record=session(value),saved=await readStorageOperatorAllocation(value,state);if(!saved?.migrationOid||!saved.databaseOid)fail('storage_operator_allocation_generation_missing');
  await exactRole(record.store,saved.intent.migration,saved.migrationOid);const db=await record.store.get('SELECT oid::text oid FROM pg_database WHERE datname=$1',saved.intent.database);if(db?.oid!==saved.databaseOid)fail('storage_operator_database_identity');
  if(state.allocation?.migrationOid&&state.allocation.migrationOid!==saved.migrationOid||state.allocation?.databaseOid&&state.allocation.databaseOid!==saved.databaseOid)fail('storage_operator_allocation_binding');
  if(manifest){const raw=record.store.pool.options,connection={host:raw.host,port:raw.port,database:saved.intent.database,user:saved.intent.migration,password:state.allocation.migrationPassword,...(raw.ssl?{ssl:raw.ssl}:{})};if(manifest.database!==saved.intent.database||manifest.user!==saved.intent.migration||databaseOid!==saved.databaseOid||canonical(manifest.connection)!==canonical(connection))fail('storage_operator_target_allocation_mismatch');}
  return saved;
}
export async function prepareStorageOperatorTarget(context,{registry,state,save,directory,key,afterPhase}={}){
  const record=session(registry),trusted=storageOperatorContext(context);if(record.readOnly)fail('storage_operator_registry_readonly');
  if(!state.allocation){const nonce=randomBytes(12).toString('hex');state.allocation={intent:{nonce,database:'harbor_rehearsal_'+nonce,migration:'harbor_migration_'+nonce},migrationPassword:randomBytes(32).toString('base64url'),stage:'intent'};await save();}
  const intent=state.allocation.intent;if(!intent||!/^harbor_rehearsal_[a-f0-9]{24}$/.test(intent.database??'')||intent.migration!=='harbor_migration_'+intent.database.slice(17)||!/^[A-Za-z0-9_-]{43}$/.test(state.allocation.migrationPassword??''))fail('storage_operator_allocation_binding');
  let saved=await readStorageOperatorAllocation(registry,state);
  if(!saved){
    await record.store.transaction(async()=>{
      if(await record.store.get('SELECT 1 FROM pg_roles WHERE rolname=$1',intent.migration)||await record.store.get('SELECT 1 FROM pg_database WHERE datname=$1',intent.database))fail('storage_operator_allocation_name_conflict');
      await record.store.query('CREATE ROLE '+ident(intent.migration)+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD '"+state.allocation.migrationPassword+"'");
      const migrationOid=(await record.store.get('SELECT oid::text oid FROM pg_roles WHERE rolname=$1',intent.migration)).oid;
      saved={version:1,runId:state.runId,cluster:state.cluster,configurationDigest:state.configurationDigest,intent,migrationOid,stage:'role_created'};await persist(record,saved);
    });await afterPhase?.('allocation_role_committed');
  }
  await exactRole(record.store,intent.migration,saved.migrationOid);
  if(!saved.databaseOid){
    if(saved.stage!=='role_created'){
      // No OID witness means no claim of ownership. Revoke only the role whose
      // creation epoch is proven; preserve the unknown database and all files.
      await record.store.query('ALTER ROLE '+ident(intent.migration)+' NOLOGIN');fail('storage_operator_database_creation_unknown');
    }
    saved.stage='database_creating';await persist(record,saved);state.allocation={...state.allocation,intent,migrationOid:saved.migrationOid,stage:saved.stage};await save();
    try{
      await record.store.query('CREATE DATABASE '+ident(intent.database)+' OWNER '+ident(intent.migration));await afterPhase?.('allocation_database_created_unwitnessed');
      const db=await record.store.get('SELECT oid::text oid,datdba::text owner FROM pg_database WHERE datname=$1',intent.database);if(!db||db.owner!==saved.migrationOid)fail('storage_operator_database_creation_unknown');
      saved={...saved,databaseOid:db.oid,stage:'database_witnessed'};await persist(record,saved);await afterPhase?.('allocation_database_witnessed');
    }catch(error){await exactRole(record.store,intent.migration,saved.migrationOid);await record.store.query('ALTER ROLE '+ident(intent.migration)+' NOLOGIN');throw error;}
  }
  state.allocation={...state.allocation,intent,migrationOid:saved.migrationOid,databaseOid:saved.databaseOid,stage:saved.stage};state.registrySchemaOid=record.schemaOid;await save();await verifyStorageOperatorAllocation(registry,state);
  await record.store.query('REVOKE ALL ON DATABASE '+ident(intent.database)+' FROM PUBLIC');await record.store.query('ALTER ROLE '+ident(intent.migration)+' LOGIN');const raw=record.store.pool.options,connection={host:raw.host,port:raw.port,database:intent.database,user:intent.migration,password:state.allocation.migrationPassword,...(raw.ssl?{ssl:raw.ssl}:{})};
  await initializeRehearsalTarget({version:1,rehearsal:true,created:Date.now(),database:intent.database,user:intent.migration,connection,systemIdentifier:state.cluster,allowPlaintextLocalNetwork:trusted.adminOptions.allowPlaintextLocalNetwork===true},{directory,key});
  await afterPhase?.('target_manifest_prepared');
}

// Must run inside the target promotion transaction, before any role is adopted.
export async function witnessStorageOperatorPromotion(store,state){
  const old=await store.get("SELECT to_regclass('harbor_operator.role_generations') relation");
  let saved;if(old.relation)saved=await store.get('SELECT * FROM harbor_operator.role_generations WHERE singleton=true FOR UPDATE');
  if(saved&&(saved.run_id!==state.runId||saved.binding!==state.bindingDigest||saved.database_oid!==state.databaseOid||saved.migration_oid!==state.allocation?.migrationOid))fail('storage_operator_role_generation');
  const oids={};for(const [kind,name] of [['owner',state.owner],['runtime',state.runtime]]){
    const actual=await store.get('SELECT oid::text oid FROM pg_roles WHERE rolname=$1',name);
    if(saved){if(actual?.oid!==saved[kind+'_oid'])fail('storage_operator_role_generation');}
    else{if(actual)fail('storage_operator_unwitnessed_role');await store.query('CREATE ROLE '+ident(name)+' NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS');}
    oids[kind+'Oid']=(await store.get('SELECT oid::text oid FROM pg_roles WHERE rolname=$1',name)).oid;
  }
  if(!saved){await store.query('CREATE TABLE harbor_operator.role_generations(singleton boolean PRIMARY KEY CHECK(singleton),run_id text NOT NULL,binding text NOT NULL,database_oid text NOT NULL,migration_oid text NOT NULL,owner_oid text NOT NULL,runtime_oid text NOT NULL)');await store.run('INSERT INTO harbor_operator.role_generations VALUES(true,$1,$2,$3,$4,$5,$6)',state.runId,state.bindingDigest,state.databaseOid,state.allocation.migrationOid,oids.ownerOid,oids.runtimeOid);}
  return oids;
}
export async function readStorageOperatorRoleGenerations(store,state){
  const exists=await store.get("SELECT to_regclass('harbor_operator.role_generations') relation");if(!exists.relation)fail('storage_operator_allocation_generation_missing');const row=await store.get('SELECT * FROM harbor_operator.role_generations WHERE singleton=true');
  if(!row||row.run_id!==state.runId||row.binding!==state.bindingDigest||row.database_oid!==state.databaseOid||row.migration_oid!==state.allocation?.migrationOid)fail('storage_operator_role_generation');
  for(const [name,oid] of [[state.allocation.intent.migration,row.migration_oid],[state.owner,row.owner_oid],[state.runtime,row.runtime_oid]])await exactRole(store,name,oid);
  if(state.roleGenerations&&(state.roleGenerations.ownerOid!==row.owner_oid||state.roleGenerations.runtimeOid!==row.runtime_oid))fail('storage_operator_role_generation');return {ownerOid:row.owner_oid,runtimeOid:row.runtime_oid};
}
