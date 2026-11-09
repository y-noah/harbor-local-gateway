// 受控维护操作：调用方先用固定 root 配置、加密迁移状态和实际容器核对 binding。
export async function upgradeOwnedContext(store,binding){
 const need=(ok,code)=>{if(!ok)throw Error(code);},ident=value=>'"'+value.replaceAll('"','""')+'"';
 need(binding&&['databaseOid','ownerOid','runtimeOid'].every(k=>typeof binding[k]==='string'&&/^\d+$/.test(binding[k])),'context_role_binding_invalid');
 const tables=['recording_context_objects','recording_context_parts','recording_context_snapshots','recording_context_references','recording_context_work','recording_context_documents','recording_context_document_parts','recording_context_export_objects'];
 return store.transaction(async()=>{
  await store.query("SET LOCAL lock_timeout='5s'");await store.query("SET LOCAL statement_timeout='120s'");
  const db=await store.get('SELECT oid::text oid,datdba::text owner_oid FROM pg_database WHERE datname=current_database()');need(db.oid===binding.databaseOid&&db.owner_oid===binding.ownerOid,'context_database_binding_changed');
  // Bind grants to the witnessed role generations, preventing drop/recreate
  // between catalog validation and name-based SET ROLE / GRANT statements.
  const roles=await store.all('SELECT oid::text oid,rolname name,rolsuper,rolcreatedb,rolcreaterole,rolinherit,rolreplication,rolbypassrls FROM pg_authid WHERE oid::text=ANY($1::text[]) ORDER BY oid FOR UPDATE',[binding.ownerOid,binding.runtimeOid]);
  const owner=roles.find(x=>x.oid===binding.ownerOid),runtime=roles.find(x=>x.oid===binding.runtimeOid);
  need(roles.length===2&&owner&&runtime&&roles.every(r=>['rolsuper','rolcreatedb','rolcreaterole','rolinherit','rolreplication','rolbypassrls'].every(k=>r[k]===false)),'context_role_generation_changed');
  need(!(await store.get('SELECT EXISTS(SELECT 1 FROM pg_auth_members WHERE roleid::text=ANY($1::text[]) OR member::text=ANY($1::text[])) found',[binding.ownerOid,binding.runtimeOid])).found,'context_role_membership_changed');
  need((await store.get("SELECT nspowner::text owner_oid FROM pg_namespace WHERE nspname='harbor'")).owner_oid===binding.ownerOid,'context_schema_owner_changed');
  await store.query('SET LOCAL ROLE '+ident(owner.name));
  const result=await store.migrate({recordingContextIndex:true});
  for(const name of tables){
   need((await store.get('SELECT relowner::text owner_oid FROM pg_class WHERE oid=$1::regclass','harbor.'+name)).owner_oid===binding.ownerOid,'context_table_owner_changed');
   await store.query('GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE harbor.'+ident(name)+' TO '+ident(runtime.name));
   need((await store.get("SELECT has_table_privilege($1,$2,'SELECT,INSERT,UPDATE,DELETE') permitted",runtime.name,'harbor.'+name)).permitted,'context_runtime_grant_incomplete');
  }
  need(!(await store.get("SELECT has_schema_privilege($1,'harbor','CREATE') permitted",runtime.name)).permitted,'context_runtime_ddl_not_allowed');
  await store.query('SET LOCAL ROLE '+ident(runtime.name));await store.verifySchema();
  for(const name of tables)await store.query('SELECT 1 FROM harbor.'+ident(name)+' LIMIT 0');
  await store.query('RESET ROLE');return {...result,contextTables:tables.length,runtimeContextDml:true};
 });
}
