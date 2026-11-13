import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {migrateDepartmentIsolation,createPostgresDepartments} from '../postgres-departments.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresAudit} from '../postgres-audit.mjs';
import {createPostgresHistoryQueries} from '../postgres-history-query.mjs';
import {historyQuery} from '../observability.mjs';
import {referenceMetric} from '../pricing.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL;
const integration={skip:!url?'需隔离真实PostgreSQL':false,timeout:60000};
const options=database=>{const u=new URL(url);if(database)u.pathname='/'+database;return {connectionString:u.href,allowPlaintextLocalNetwork:true,poolSize:5};};

test('审计归属独立持久化且历史所有读取按范围查询，同日夜间批次不互相覆盖',integration,async()=>{
 const database='harbor_history_'+randomUUID().replaceAll('-',''),admin=new PostgresStore(options());let store;
 try{
  await admin.query('CREATE DATABASE '+database);store=new PostgresStore(options(database));await store.migrate();
  const now=Date.now(),day=new Date(now).toLocaleDateString('sv-SE',{timeZone:'Asia/Shanghai'});
  await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)','old-business',now,'member.create',JSON.stringify({department_id:'spoofed-global',name:'历史已删除员工'}));
  await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)','old-pricing',now,'pricing.configure','{}');
  await migrateDepartmentIsolation(store,{apply:true});
  assert.equal((await store.get("SELECT department_id FROM harbor.audit_scopes WHERE audit_id='old-business'")).department_id,'department-default');
  assert.equal((await store.get("SELECT department_id FROM harbor.audit_scopes WHERE audit_id='old-pricing'")).department_id,null);
  const issuer=createDepartmentScopeIssuer(),root=issuer.superAdmin({credentialId:'synthetic-root',credentialVersion:0}),departments=createPostgresDepartments(store,{issuer});
  const a=await departments.createDepartment(root,{name:'合成历史部门A'}),b=await departments.createDepartment(root,{name:'合成历史部门B'}),ka=await departments.issueManagementKey(root,a.id,{label:'合成A'}),kb=await departments.issueManagementKey(root,b.id,{label:'合成B'}),sa=await departments.authenticateManagementKey(ka.secret),sb=await departments.authenticateManagementKey(kb.secret);
  for(const [department,account,member] of [[a,'account-a','member-a'],[b,'account-b','member-b']]){
   await store.run('INSERT INTO harbor.accounts(id,department_id,name,home) VALUES($1,$2,$3,$1)',account,department.id,'合成'+account);
   await store.run('INSERT INTO harbor.members(id,department_id,name,created) VALUES($1,$2,$3,$4)',member,department.id,'合成'+member,now);
  }
  const writer=createPostgresAudit(store),reader=createPostgresHistoryQueries(store),query=params=>historyQuery(new URLSearchParams(params));
  const event=await writer.write(sa,{action:'member.update',detail:'member-b',subject:{kind:'member',id:'member-a'}});
  await writer.write(sa,{action:'synthetic.a.only',detail:{department_id:b.id}});
  await writer.write(sb,{action:'synthetic.b.only',detail:{department_id:a.id}});
  await writer.write(root,{action:'pricing.configure',detail:{version:1}});
  assert.equal((await store.get('SELECT department_id FROM harbor.audit_scopes WHERE audit_id=$1',event)).department_id,a.id);
  await assert.rejects(writer.write(JSON.parse(JSON.stringify(sa)),{action:'member.update',detail:{}}),{status:403});
  await assert.rejects(writer.write(sa,{action:'member.update',detail:{},subject:{kind:'member',id:'member-b'}}),{status:403});
  await assert.rejects(writer.write(undefined,{action:'member.update',detail:{department_id:a.id}}),{status:403});
  const viewA=await reader.list(query({}),{scope:sa}),viewB=await reader.list(query({}),{scope:sb}),global=await reader.list(query({}),{scope:root});
  assert.ok(viewA.rows.every(row=>row.department_id===a.id));assert.ok(viewB.rows.every(row=>row.department_id===b.id));
  assert.deepEqual(viewA.accounts.map(row=>row.id),['account-a']);assert.deepEqual(viewB.accounts.map(row=>row.id),['account-b']);
  assert.ok(viewA.actions.includes('synthetic.a.only'));assert.ok(!viewA.actions.includes('synthetic.b.only'));assert.ok(global.actions.includes('synthetic.b.only'));
  assert.doesNotMatch(JSON.stringify(viewA),/合成member-b|合成account-b/);assert.equal(viewA.rows.find(row=>row.id===event).detail.member,null);
  assert.equal(viewA.rows.find(row=>row.id===event).actor.id,sa.credential_id);
  assert.equal((await reader.list(query({action:'synthetic.b.only'}),{scope:sa})).pagination.total,0);
  await assert.rejects(store.run('UPDATE harbor.audit_scopes SET department_id=$1,scope_identity=$2 WHERE audit_id=$3',b.id,'department:'+b.id,event),{code:'23514'});
  await assert.rejects(store.transaction(async()=>{
   await store.run('DELETE FROM harbor.audit_scopes WHERE audit_id=$1',event);
   await store.run("INSERT INTO harbor.audit_scopes(audit_id,department_id,scope_kind,scope_identity,actor_kind,actor_id,actor_version) VALUES($1,$2,'department',$3,'department_admin',$4,0)",event,b.id,'department:'+b.id,sb.credential_id);
  }),{code:'23514'});
  assert.equal((await store.get('SELECT department_id FROM harbor.audit_scopes WHERE audit_id=$1',event)).department_id,a.id);
  const cascade=await writer.write(sa,{action:'synthetic.cascade',detail:{}});await store.run('DELETE FROM harbor.audit WHERE id=$1',cascade);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.audit_scopes WHERE audit_id=$1',cascade)).n,0);
  const lifecycle=await departments.createDepartment(root,{name:'合成生命周期'}),rootLifecycle=issuer.forDepartment(root,lifecycle.id);
  await departments.updateDepartment(rootLifecycle,lifecycle.id,{enabled:false,expected_version:lifecycle.version});
  const lifecycleKey=await departments.issueManagementKey(rootLifecycle,lifecycle.id,{label:'停用时管理'});
  await departments.revokeManagementKey(rootLifecycle,lifecycle.id,lifecycleKey.key.id,{expected_version:lifecycleKey.key.version});
  await assert.rejects(writer.write(rootLifecycle,{action:'account.check',detail:'account-a',subject:{kind:'account',id:'account-a'}}),{status:403});
  await assert.rejects(writer.write(rootLifecycle,{action:'department.update',detail:{},subject:{kind:'department',id:b.id}}),{status:403});

  const summary=account=>({metric:referenceMetric,moves:[{session_id:'legacy-session',from:account,to:account}],skipped:[],summaries:[],applied:[]});
  for(const [department,account] of [[a,'account-a'],[b,'account-b']])await store.run('INSERT INTO harbor.balance_runs(department_id,day,started,ended,status,summary) VALUES($1,$2,$3,$3,\'completed\',$4)',department.id,day,now,JSON.stringify(summary(account)));
  const runsA=await reader.list(query({kind:'nightly'}),{scope:sa}),runsB=await reader.list(query({kind:'nightly'}),{scope:sb});
  assert.equal(runsA.pagination.total,1);assert.equal(runsA.rows[0].department_id,a.id);assert.equal(runsB.rows[0].department_id,b.id);
  assert.equal((await reader.nightly(day,{scope:sa})).counts.moves,1);assert.equal((await reader.page(day,{scope:sa})).items[0].from,'account-a');
  assert.equal((await reader.page(day,{scope:sb})).items[0].from,'account-b');assert.equal((await reader.revisions(day,{scope:sa})).items.length,0);
  await assert.rejects(reader.nightly(day,{scope:root}),{code:'history_department_required'});
  await assert.rejects(store.run('UPDATE harbor.balance_runs SET department_id=$1 WHERE department_id=$2 AND day=$3',b.id,a.id,day),{code:'23514'});

  for(const [department,suffix] of [[a,'a'],[b,'b']]){
   const plan='plan-'+suffix,account='account-'+suffix,session='session-'+suffix;
   await store.run('INSERT INTO harbor.sessions(id,department_id,member_id,account_id,created,updated) VALUES($1,$2,$3,$4,$5,$5)',session,department.id,'member-'+suffix,account,now);
   await store.run("INSERT INTO harbor.background_jobs(id,kind,available_at,created,updated,department_id,scope_kind,scope_identity,actor_kind,actor_id,actor_version) VALUES($1,'routing.nightly',$2,$2,$2,$3,'department',$4,'system','system:routing.nightly',0)",plan,now,department.id,'department:'+department.id);
   await store.run('INSERT INTO harbor.routing_plans(id,created,expires,header,bytes,department_id) VALUES($1,$2,$3,$4::jsonb,1,$5)',plan,now,now+3600000,JSON.stringify({metric:referenceMetric,counts:{moves:1,skipped:0,groups:1,fixed:0}}),department.id);
   for(const [section,payload] of [['groups',{accounts:[account]}],['moves',{session_id:session,from:account,to:account}]])await store.run('INSERT INTO harbor.routing_plan_items(plan_id,section,ordinal,payload,bytes) VALUES($1,$2,1,$3::jsonb,$4)',plan,section,JSON.stringify(payload),Buffer.byteLength(JSON.stringify(payload)));
   await store.run("INSERT INTO harbor.nightly_plan_applications(plan_id,department_id,slot,started,ended,status) VALUES($1,$2,$3,$4,$4,'completed')",plan,department.id,day,now);
   await store.run('INSERT INTO harbor.nightly_plan_group_results(plan_id,group_ordinal,at,status,reason,moved,department_id) VALUES($1,1,$2,$3,$4,$5,$6)',plan,now,suffix==='a'?'applied':'skipped',suffix==='a'?null:'protected',suffix==='a'?1:0,department.id);
   await store.run('UPDATE harbor.balance_runs SET plan_id=$1 WHERE department_id=$2 AND day=$3',plan,department.id,day);
  }
  const modernA=await reader.nightly(day,{scope:sa}),modernB=await reader.nightly(day,{scope:sb});
  assert.equal(modernA.revisionAware,true);assert.equal(modernA.results.applied,1);assert.equal(modernA.revisionCount,1);
  assert.equal(modernB.results.applied,0);assert.equal(modernB.results.skipped,1);assert.equal(modernB.revisionCount,1);
  assert.equal((await reader.revisions(day,{scope:sa})).items.length,1);
  assert.equal((await reader.page(day,{scope:sa})).items[0].session_id,'session-a');assert.equal((await reader.page(day,{scope:sb})).items[0].result.reason,'protected');
  await assert.rejects(store.run("INSERT INTO harbor.nightly_plan_group_results(plan_id,group_ordinal,at,status,moved,department_id) VALUES('plan-a',2,$1,'skipped',0,$2)",now,b.id),{code:'23503'});
  for(const limit of [1,25,101]){await assert.rejects(reader.page(day,{scope:sa,limit}),{status:400});await assert.rejects(reader.revisions(day,{scope:sa,limit}),{status:400});}

  await store.run("INSERT INTO harbor.accounts(id,department_id,name,home) VALUES('handoff-target-a',$1,'合成转移目标','synthetic')",a.id);
  const handoff=randomUUID(),legacyHandoff=randomUUID(),automaticHandoff=randomUUID();
  const insertHandoff=(id,origin,status,actorKind,actorId)=>store.run("INSERT INTO harbor.session_handoffs(id,department_id,session_id,member_id,source_account_id,target_account_id,expected_route_version,source_epoch,target_epoch,source_nightly_enabled,origin,status,requested,expires,finished,actor_kind,actor_id,actor_version) VALUES($1,$2,'session-a','member-a','account-a','handoff-target-a',0,0,0,1,$3,$4,$5,$6,$7,$8,$9,0)",id,a.id,origin,status,now,now+3600000,status==='waiting'?null:now,actorKind,actorId);
  await insertHandoff(handoff,'manual','waiting','department_admin',sa.credential_id);
  assert.equal((await departments.authorizeHandoffActor(handoff)).credential_id,sa.credential_id);
  assert.equal((await store.transaction(()=>departments.authorizeHandoffActor(handoff,{lock:true}))).department_id,a.id);
  await assert.rejects(departments.authorizeHandoffActor(handoff,{lock:true}),{code:'database_transaction_required'});
  const finalize=issuer.system({purpose:'session.handoff.finalize',departmentId:a.id}),finalEvent={action:'session.handoff.cancelled',detail:{handoff},subject:{kind:'session',id:'session-a'}};
  await assert.rejects(writer.write(finalize,finalEvent),{status:403});
  await store.run("UPDATE harbor.session_handoffs SET status='cancelled',finished=$1 WHERE id=$2",now,handoff);
  await insertHandoff(legacyHandoff,'manual','waiting','legacy_admin','migration-v1');
  await insertHandoff(automaticHandoff,'automatic','cancelled','system','system:routing.nightly');
  await assert.rejects(departments.authorizeHandoffActor(legacyHandoff),{status:403});
  await assert.rejects(writer.write(finalize,{...finalEvent,detail:{handoff:automaticHandoff}}),{status:403});
  await assert.rejects(writer.write(finalize,{...finalEvent,detail:{handoff:randomUUID()}}),{status:403});
  await assert.rejects(writer.write(finalize,{...finalEvent,subject:{kind:'session',id:'session-b'}}),{status:403});
  for(const action of ['session.route','session.handoff.completed','session.handoff.failed'])await assert.rejects(writer.write(finalize,{...finalEvent,action}),{status:403});
  await assert.rejects(store.run('UPDATE harbor.session_handoffs SET actor_id=$1 WHERE id=$2',sb.credential_id,handoff),{code:'23514'});
  await departments.updateDepartment(root,a.id,{enabled:false,expected_version:a.version});
  assert.ok(await writer.write(finalize,finalEvent));
  await assert.rejects(writer.write(issuer.system({purpose:'session.handoff.finalize',departmentId:b.id}),finalEvent),{status:403});

  await departments.revokeManagementKey(root,a.id,ka.key.id,{expected_version:(await store.get('SELECT version FROM harbor.department_admin_keys WHERE id=$1',ka.key.id)).version});
  await assert.rejects(reader.list(query({}),{scope:sa}),{status:403});
  await assert.rejects(store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)','unowned-new',now,'synthetic.missing.owner',JSON.stringify({department_id:b.id})),{code:'23514'});
  await assert.rejects(store.transaction(async()=>{await store.query('ALTER TABLE harbor.audit_scopes DISABLE TRIGGER audit_scope_immutable');await store.run('DELETE FROM harbor.audit_scopes WHERE audit_id=$1',event);await assert.rejects(reader.list(query({}),{scope:sb}),{code:'audit_scope_required'});throw Error('synthetic rollback');}),/synthetic rollback/);
 }finally{await store?.close();await admin.query('DROP DATABASE '+database);await admin.close();}
});
