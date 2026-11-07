import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {randomBytes} from 'node:crypto';
import {Store} from '../store.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {quoteReference,emptyReferenceQuote} from '../pricing.mjs';
import {readManifest} from '../migration-artifacts.mjs';
import {createRehearsalTarget,captureSqliteSource,transferSqlite,openRehearsalTarget,destroyRehearsalTarget,verifyTransferredSqliteSnapshot} from '../sqlite-postgres-transfer.mjs';
const socket=process.env.HARBOR_TEST_PG_SOCKET,url=process.env.HARBOR_TEST_DATABASE_URL,options=url?{connectionString:url,allowPlaintextLocalNetwork:true}:{connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_ci',database:'postgres'}};
const integration={skip:socket||url?false:'需显式隔离PG环境；不接触线上数据库'};
const fields=['input','output','cached','adjustment','total'];
// Preserve the former scalar projection as an independent result oracle.
function scalar(row,books){const revision=row.price_version??null,book=revision?books.get(revision):null,quote=book||row.manual_cost_atoms!==null&&row.manual_cost_atoms!==undefined?quoteReference(row,book??{rates:{},aliases:[]},revision):emptyReferenceQuote('unpriced',row.billing_model||row.model,revision);return {request_id:row.id,state:quote.state,model:quote.model,model_source:quote.modelSource??null,revision:quote.revision,...Object.fromEntries(fields.map(f=>[f+'_atoms',quote[f+'Atoms']===null?null:quote[f+'Atoms'].toString()]))};}
async function fixture(t){
 const root=await mkdtemp(join(tmpdir(),'harbor-price-batch-')),source=join(root,'source'),directory=join(root,'target'),key=randomBytes(32);await mkdir(source,{mode:0o700});await mkdir(directory,{mode:0o700});let created=false,legacy;
 t.after(async()=>{legacy?.close();if(created)await destroyRehearsalTarget(options,{directory,key});await rm(root,{recursive:true,force:true});});
 legacy=new Store(join(source,'gateway.sqlite'),{encryptionKey:randomBytes(32)});const member=legacy.createMember('合成批量迁移员工',null,null),person=legacy.authenticate(member.secret),at=Date.parse('2026-11-01T00:00:00Z');
 const books=new Map([['synthetic-v1',{rates:{'synthetic-model':{input:'2',cached:'0.2',output:'10'}},aliases:[{name:'synthetic-alias',target:'synthetic-model',from:at-1,to:at+1}]}],['synthetic-v2',{rates:{'synthetic-model':{input:'4',cached:null,output:'20'}},aliases:[]}]]);
 for(const [id,book] of books)legacy.run('INSERT INTO pricing_versions(id,created,book) VALUES(?,?,?)',id,at,JSON.stringify(book));
 const variants=[{}, {price_version:'synthetic-v2'}, {model:'synthetic-alias'}, {model:'synthetic-unknown'}, {price_version:null}, {price_version:'synthetic-missing'}, {manual_cost_atoms:'123456789123456789123'}, {manual_cost_atoms:'0',price_version:'synthetic-missing'}, {usage_unknown:1}, {input:0,output:0,cached:0}, {billing_model:'synthetic-model',model:'synthetic-unknown'}, {price_version:'synthetic-v2',cached:1}];
 const rows=[];
 for(let i=0;i<variants.length;i++){const id=legacy.observe(person,'synthetic','synthetic','gpt-6.1-sol');legacy.finish(id,{input:100,output:10,cached:2});const values={id:'synthetic-request-'+String(i).padStart(3,'0'),started:at,ended:at+10,model:'synthetic-model',billing_model:null,price_version:'synthetic-v1',manual_cost_atoms:null,input:100,output:10,cached:2,usage_unknown:0,...variants[i]};legacy.run('UPDATE requests SET '+Object.keys(values).map(k=>k+'=?').join(',')+' WHERE id=?',...Object.values(values),id);rows.push(legacy.get('SELECT * FROM requests WHERE id=?',values.id));}
 legacy.close();legacy=null;await createRehearsalTarget(options,{directory,key});created=true;await captureSqliteSource({sourceDataDir:source,directory,key});return {directory,key,rows,expected:rows.map(row=>scalar(row,books))};
}
const priceSelect='SELECT request_id,state,model,model_source,revision,input_atoms::text';
async function savedPrices(store){return store.all('SELECT request_id,state,model,model_source,revision,input_atoms::text input_atoms,output_atoms::text output_atoms,cached_atoms::text cached_atoms,adjustment_atoms::text adjustment_atoms,total_atoms::text total_atoms FROM harbor.request_prices ORDER BY request_id');}
test('historical pricing batches match scalar mixed books/models/manual atoms and verify with real READ ONLY',integration,async t=>{
 const f=await fixture(t),original=PostgresStore.prototype.query,counts={books:0,writes:0,reads:0};PostgresStore.prototype.query=function(sql,args=[]){if(sql.startsWith('SELECT id,book FROM harbor.pricing_versions'))counts.books++;if(sql.startsWith('INSERT INTO harbor.request_prices'))counts.writes++;if(sql.startsWith(priceSelect))counts.reads++;return original.call(this,sql,args);};
 try{await transferSqlite({...f,batchRows:3});}finally{PostgresStore.prototype.query=original;}
 assert.equal(counts.writes,8);assert.equal(counts.reads,8);assert.ok(counts.books<=8);const opened=await openRehearsalTarget(f);try{
  assert.deepEqual(await savedPrices(opened.store),f.expected);let readOnlyChecks=0,writes=0;const query=opened.store.query.bind(opened.store);opened.store.query=async(sql,args=[])=>{if(sql.startsWith('INSERT INTO harbor.request_prices'))writes++;if(sql.startsWith(priceSelect)){assert.equal((await query('SHOW transaction_read_only')).rows[0].transaction_read_only,'on');readOnlyChecks++;}return query(sql,args);};
  const verified=await verifyTransferredSqliteSnapshot({...f,store:opened.store,batchRows:3});assert.ok(verified.rows>=f.rows.length);assert.equal(readOnlyChecks,4);assert.equal(writes,0);await opened.store.run('DELETE FROM harbor.request_prices WHERE request_id=$1',f.rows[0].id);await assert.rejects(verifyTransferredSqliteSnapshot({...f,store:opened.store,batchRows:3}),{code:'transfer_price_conflict'});
 }finally{await opened.store.close();}
});
test('historical price conflict rolls back the whole replayed request batch without overwriting the old price',integration,async t=>{
 const f=await fixture(t);await assert.rejects(transferSqlite({...f,batchRows:3,afterBatch:({table})=>{if(table==='requests')throw Error('synthetic_after_commit');}}),/synthetic_after_commit/);
 let opened=await openRehearsalTarget(f);try{await opened.store.run('UPDATE harbor.request_prices SET total_atoms=7 WHERE request_id=$1',f.rows[0].id);await opened.store.run('DELETE FROM harbor.requests WHERE id=$1',f.rows[1].id);}finally{await opened.store.close();}
 await assert.rejects(transferSqlite({...f,batchRows:3}),{code:'transfer_price_conflict'});opened=await openRehearsalTarget(f);try{assert.equal((await opened.store.get('SELECT COUNT(*) n FROM harbor.requests WHERE id=$1',f.rows[1].id)).n,0);assert.equal((await opened.store.get('SELECT total_atoms::text value FROM harbor.request_prices WHERE request_id=$1',f.rows[0].id)).value,'7');assert.equal((await readManifest(join(f.directory,'progress.enc'),f.key)).completed,false);}finally{await opened.store.close();}
});
test('cancellation after batch price INSERT rolls back before checkpoint and permits the original safe retry',integration,async t=>{
 const f=await fixture(t),abort=new AbortController(),original=PostgresStore.prototype.query;let interrupted=false;PostgresStore.prototype.query=async function(sql,args=[]){const result=await original.call(this,sql,args);if(!interrupted&&sql.startsWith('INSERT INTO harbor.request_prices')){interrupted=true;abort.abort();}return result;};
 try{await assert.rejects(transferSqlite({...f,batchRows:3,signal:abort.signal}),{code:'transfer_interrupted'});}finally{PostgresStore.prototype.query=original;}
 const opened=await openRehearsalTarget(f);try{assert.equal((await opened.store.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);assert.equal((await readManifest(join(f.directory,'progress.enc'),f.key)).completed,false);}finally{await opened.store.close();}
 await transferSqlite({...f,batchRows:3});const resumed=await openRehearsalTarget(f);try{assert.deepEqual(await savedPrices(resumed.store),f.expected);}finally{await resumed.store.close();}
});
