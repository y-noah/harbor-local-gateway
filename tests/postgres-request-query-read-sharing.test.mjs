import test from 'node:test';
import assert from 'node:assert/strict';
import {createPostgresRequestQueries} from '../postgres-request-query.mjs';
import {usageWindow} from '../store.mjs';

const at=Date.parse('2026-11-12T12:00:00+08:00'),snapshot='00000000-0000-4000-8000-000000000001',window=usageWindow(at);
function fixture({total=21}={}){
 const statements=[],previews=[];
 const store={transaction:operation=>operation(),run:async()=>assert.fail('有效查询快照不得写入'),get:async(sql,...args)=>{statements.push({sql,args});if(sql.includes('pg_current_snapshot()'))return {visibility:'1:1:'};if(sql.includes('FROM harbor.request_query_snapshots'))return {visibility:'1:1:',window_start:window.start,window_end:window.end,filters:'["","",""]'};if(sql.startsWith('SELECT COUNT(*) n'))return {n:total};assert.fail('非预期读取');},all:async(sql,...args)=>{statements.push({sql,args});if(sql.startsWith('WITH data AS'))return [{g_model:1,g_member:1,requests:total,success:total,active:0,average_ms:100,known:0,unmetered:0,unpriced:total,pending:0,expired:0,input:'0',output:'0',cached:'0',adjustment:'0',total:'0',revisions:[]}];if(sql.startsWith('SELECT r.*')){const offset=sql.includes(' OFFSET ')?args.at(-1):0,limit=sql.includes(' OFFSET ')?args.at(-2):args.at(-1);return Array.from({length:total},(_,i)=>({id:'synthetic-request-'+i,started:at-i,ended:at-i+1,status:'ok',model:'synthetic-model',price_version:null,projected_revision:null,book_exists:false})).slice(offset,offset+limit);}return [];} };
 const reader=createPostgresRequestQueries(store,{clock:()=>at,readInputPreviews:async ids=>{previews.push(ids);return new Map(ids.map(id=>[id,{input_preview:'合成问题',input_preview_state:'available'}]));}});
 return {reader,statements,previews};
}
const input=(view='all',extra={})=>({view,page:1,pageSize:20,snapshot,...extra});

test('all在同一读取中复用统计总数，保留分页游标与输入摘要',async()=>{
 const f=fixture();try{const result=await f.reader.read(input());assert.equal(f.statements.filter(x=>x.sql.startsWith('SELECT COUNT(*) n')).length,0);assert.equal(result.pagination.total,21);assert.equal(result.pagination.totalPages,2);assert.equal(result.rows.length,20);assert.deepEqual(f.previews,[result.rows.map(row=>row.id)]);assert.ok(result.rows.every(row=>row.input_preview==='合成问题'&&row.input_preview_state==='available'));assert.ok(result.rows.every(row=>row.standard.state==='unpriced'));
 const cursor=JSON.parse(Buffer.from(result.pagination.nextCursor,'base64url').toString());assert.equal(cursor.snapshot,snapshot);assert.equal(cursor.id,result.rows.at(-1).id);assert.equal(cursor.started,result.rows.at(-1).started);
 assert.ok(f.statements.findIndex(x=>x.sql.startsWith('WITH data AS'))<f.statements.findIndex(x=>x.sql.startsWith('SELECT r.*')));
 }finally{await f.reader.close();}
});

test('all越界页码和空集使用精确统计总数，不再补发COUNT',async()=>{
 for(const total of [0,21]){const f=fixture({total});try{const result=await f.reader.read(input('all',{page:99}));assert.equal(result.pagination.page,total?2:1);assert.equal(result.pagination.total,total);assert.equal(result.rows.length,total?1:0);assert.equal(f.statements.filter(x=>x.sql.startsWith('SELECT COUNT(*) n')).length,0);}finally{await f.reader.close();}}
});

test('list-only仍独立计数，summary-only不读取分页及输入摘要',async()=>{
 for(const view of ['list','summary']){const f=fixture();try{const result=await f.reader.read(input(view));assert.equal(f.statements.filter(x=>x.sql.startsWith('SELECT COUNT(*) n')).length,view==='list'?1:0);assert.equal(f.previews.length,view==='list'?1:0);assert.equal('pagination' in result,view==='list');assert.equal('summary' in result,view==='summary');}finally{await f.reader.close();}}
});
