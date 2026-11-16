import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {randomUUID,randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresDepartments,migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createAdminAuth,passwordHash} from '../auth.mjs';
import {createPostgresDepartmentHttp} from '../postgres-department-http.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresMemberLimits} from '../postgres-member-limits.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独占真实PG':false,timeout:60000},at=Date.parse('2026-11-17T12:00:00+08:00');
const connection=name=>{const address=new URL(url);if(name)address.pathname='/'+name;return {connectionString:address.href,allowPlaintextLocalNetwork:true};};
async function fixture(t){
 const root=new PostgresStore(connection()),name='harbor_member_share_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(connection(name));let admin,server;
 t.after(async()=>{if(server){server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}await admin?.close();await store.close();await root.query('DROP DATABASE '+name);await root.close();});
 await store.migrate();await store.initializePricing();await migrateDepartmentIsolation(store,{apply:true});
 const issuer=createDepartmentScopeIssuer();let auth;const departments=createPostgresDepartments(store,{issuer,authorizeSuperAdmin:credential=>auth.authorizeSuperAdmin(credential)});auth=createAdminAuth({adminPasswordHash:passwordHash('synthetic-root')},{scopeIssuer:issuer,departmentKeys:{authenticate:departments.authenticateManagementKey,validate:departments.validateManagementSession}});
 const login=async secret=>{const res={setHeader(name,value){if(name==='Set-Cookie')this.cookie=value.split(';')[0];}};await auth.loginAsync(secret,res);return res.cookie;},rootCookie=await login('synthetic-root'),superAdmin=await auth.context({headers:{cookie:rootCookie}}),cookies={},groups={};
 for(const label of ['a','b']){const department=await departments.createDepartment(superAdmin,{name:'合成性能部门'+label}),management=await departments.issueManagementKey(superAdmin,department.id,{label:'synthetic management'});cookies[label]=await login(management.secret);groups[label]=[];
  for(let i=0;i<(label==='a'?12:2);i++){const id=randomUUID(),key=randomUUID(),rules=i%3===0?[{model:'gpt-6.1-sol',hours:24,limit_usd:'20.00'},{model:'gpt-6-astra',hours:168,limit_usd:'40.00'}]:[{model:i%2?'gpt-6.1-sol':'gpt-6-astra',hours:i%2?168:24,limit_usd:'10.00'}];
   await store.run('INSERT INTO harbor.members(id,name,created,usage_limits,department_id) VALUES($1,$2,$3,$4,$5)',id,'合成限额员工'+label+i,at+i,JSON.stringify({enabled:i!==11,rules}),department.id);await store.run('INSERT INTO harbor.keys(id,member_id,digest,prefix,created,department_id) VALUES($1,$2,$3,$3,$4,$5)',key,id,'synthetic-'+key,at,department.id);groups[label].push(id);
   const version=JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'")).value).active;
   for(let j=0;j<4;j++){const request=randomUUID(),model=j%2?'gpt-6-astra':'gpt-6.1-sol';await store.run('INSERT INTO harbor.requests(id,member_id,key_id,department_id,started,ended,status,ip,model,price_version,input,output,cached,usage_unknown) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,1000,10,200,$11)',request,id,key,department.id,at-1000,j===3?null:at-100,j===3?'running':'ok','synthetic',model,version,j===2?1:0);await store.projectPrice({id:request},at);}
  }
 }
 const readBody=async req=>{let text='';for await(const chunk of req)text+=chunk;return text?JSON.parse(text):{};},boundary=createPostgresDepartmentHttp({auth,departments,issuer,installed:true,readBody});admin=createPostgresMemberAdmin(store,{clock:()=>at,encryptionKey:randomBytes(32),departmentActors:departments});
 let observations=null;const query=store.query.bind(store);store.query=(sql,args)=>{observations?.push(sql);return query(sql,args);};
 server=http.createServer((req,res)=>{(async()=>{const scope=await boundary.context(req),url=new URL(req.url,'http://localhost'),result=await admin.handle(url.pathname,req.method,await readBody(req),url.searchParams,{scope});res.writeHead(result?.status??404,{'content-type':'application/json'});res.end(JSON.stringify(result?.value??{}));})().catch(error=>{res.writeHead(error.status??500,{'content-type':'application/json'});res.end(JSON.stringify({code:error.code,message:error.message}));});});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 const call=async(label,params='usage_profile=list&page_size=20')=>{const response=await fetch('http://127.0.0.1:'+server.address().port+'/api/admin/members?'+params,{headers:{cookie:cookies[label]}});return {status:response.status,value:await response.json()};};
 return {store,groups,call,capture:()=>{observations=[];return observations;},stopCapture:()=>{const rows=observations;observations=null;return rows;}};
}
const bookRead=sql=>sql.includes("WHERE s.name='pricing.state'");
const configRead=sql=>sql.startsWith('SELECT usage_limits,usage_limits_version FROM harbor.members');

test('多员工不同规则HTTP列表与原status逐项等价，只读一次当前价格且不重读各员工配置',integration,async t=>{
 const f=await fixture(t),limits=createPostgresMemberLimits(f.store,{clock:()=>at});await f.call('a');
 f.capture();const started=performance.now(),actual=await f.call('a'),elapsed=performance.now()-started,sql=f.stopCapture();assert.equal(actual.status,200,JSON.stringify(actual.value));assert.equal(actual.value.rows.length,12);assert.ok(actual.value.rows.every(row=>f.groups.a.includes(row.id)));assert.ok(!actual.value.rows.some(row=>f.groups.b.includes(row.id)));
 assert.equal(sql.filter(bookRead).length,1);assert.equal(sql.filter(configRead).length,0);
 f.capture();const expected=new Map(),legacyStarted=performance.now();await f.store.transaction(async()=>{for(const row of actual.value.rows)if(row.usage_limits.enabled)expected.set(row.id,(await limits.status(row.id,at)).usage);},{level:'REPEATABLE READ',readOnly:true});const legacyMs=performance.now()-legacyStarted,legacySql=f.stopCapture();
 for(const row of actual.value.rows)assert.deepEqual(row.limit_usage,expected.get(row.id)??[]);
 assert.equal(legacySql.filter(bookRead).length,11);assert.equal(legacySql.filter(configRead).length,11);assert.ok(actual.value.rows.some(row=>row.limit_usage.some(rule=>rule.unresolved>0)));assert.ok(actual.value.rows.some(row=>row.limit_usage.some(rule=>rule.in_flight>0)));
 console.log(JSON.stringify({module:'member-list-limit-sharing',httpMs:elapsed,legacyLimitReadsMs:legacyMs,httpQueries:sql.length,legacyLimitQueries:legacySql.length,bookReads:{list:1,legacy:11},configReads:{list:0,legacy:11}}));
 f.capture();assert.equal((await f.call('a','view=list&page_size=20')).status,200);assert.equal(f.stopCapture().filter(bookRead).length,0);
});

test('共享价格仅限本次事务，新请求价格更新可见；prepared接口拒绝事务外调用',integration,async t=>{
 const f=await fixture(t),limits=createPostgresMemberLimits(f.store,{clock:()=>at});await assert.rejects(limits.statusForRows([],at),{code:'member_limits_transaction_required'});
 const before=await f.call('a'),state=JSON.parse((await f.store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'")).value),book=await f.store.pricingBook(state.active);delete book.rates['gpt-6-astra'];
 await f.store.run('INSERT INTO harbor.pricing_versions VALUES($1,$2,$3)','synthetic-new-active',at,JSON.stringify(book));await f.store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='pricing.state'",JSON.stringify({active:'synthetic-new-active'}));
 const after=await f.call('a');assert.equal(after.status,200,JSON.stringify(after.value));const beforeRules=before.value.rows.flatMap(row=>row.limit_usage),afterRules=after.value.rows.flatMap(row=>row.limit_usage);assert.ok(beforeRules.some(rule=>rule.model==='gpt-6-astra'&&!rule.invalid_model));assert.ok(afterRules.filter(rule=>rule.model==='gpt-6-astra').every(rule=>rule.invalid_model&&rule.blocked));
});
