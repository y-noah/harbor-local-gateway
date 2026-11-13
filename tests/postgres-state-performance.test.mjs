import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {PostgresStore} from '../postgres-store.mjs';
import {usageWindow} from '../store.mjs';
import {createPostgresStateQueries} from '../postgres-state-query.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET;
function options(database='postgres'){
  if(url){const address=new URL(url);address.pathname='/'+database;return {connectionString:address.toString(),allowPlaintextLocalNetwork:true};}
  return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_scale',database}};
}

test('people=none preserves complete overview DTO across usage boundaries, session pages and an empty window',{
  skip:!url&&!socket?'需隔离PG；CI test_postgres执行真实链路':false
},async()=>{
  const admin=new PostgresStore(options()),database='harbor_test_'+randomUUID().replaceAll('-','');let store;
  try{
    await admin.query('CREATE DATABASE '+database+" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'");
    store=new PostgresStore(options(database));await store.migrate();await store.initializePricing();
    const at=Date.parse('2026-11-08T10:00:00+08:00'),window=usageWindow(at),member=randomUUID(),accounts=[randomUUID(),randomUUID()],sessions=Array.from({length:27},()=>randomUUID()),revision=(await store.get('SELECT id FROM harbor.pricing_versions LIMIT 1')).id;
    await store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',member,'合成概览员工',at);
    for(const [index,id] of accounts.entries())await store.run("INSERT INTO harbor.accounts(id,name,home) VALUES($1,$2,'synthetic')",id,'合成概览账号'+index);
    await store.run("UPDATE harbor.accounts SET kind='qwen',auth_mode='platform',platform_models=$2 WHERE id=$1",accounts[1],JSON.stringify(['synthetic-model']));
    for(const [index,id] of sessions.entries())await store.run('INSERT INTO harbor.sessions(id,member_id,account_id,created,updated) VALUES($1,$2,$3,$4,$4)',id,member,accounts[0],at-index);
    const records=[
      {ended:window.start,session:sessions[26]},
      {ended:window.start-1,session:sessions[26]},
      {ended:at-12*3600000,session:sessions[0]},
      {ended:at,session:sessions[1]},
      {ended:window.end,session:sessions[1]},
      {started:at-100,ended:null,status:'running',session:sessions[26]},
      {ended:at-200,unattributed:true,stale:true}
    ];
    for(const record of records){
      const id=randomUUID(),started=record.started??record.ended-100;
      await store.run('INSERT INTO harbor.requests(id,member_id,account_id,session_id,started,ended,status,ip,model,price_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)',id,record.unattributed?null:member,record.unattributed?null:accounts[0],record.session??null,started,record.ended,record.status??'ok','synthetic','synthetic',revision);
      await store.run("INSERT INTO harbor.request_prices(request_id,state,revision,total_atoms,input_atoms,output_atoms,cached_atoms,adjustment_atoms,measured_at) VALUES($1,'priced',$2,9007199254740993,9007199254740993,0,0,0,$3)",id,record.stale?'synthetic-stale':revision,at);
    }
    const indexStatements=(await readFile(new URL('../ci/storage-recovery/request-overview-index.sql',import.meta.url),'utf8')).split('-- statement-boundary');
    assert.equal(indexStatements.length,3);
    const statisticsStatements=(await readFile(new URL('../ci/storage-recovery/request-statistics-indexes.sql',import.meta.url),'utf8')).split('-- statement-boundary');assert.equal(statisticsStatements.length,4);
    const initialReader=createPostgresStateQueries(store,{clock:()=>at});let withoutIndex;
    try{withoutIndex=await initialReader.read({view:'overview'});}finally{await initialReader.close();}
    for(let attempt=0;attempt<2;attempt++)for(const statement of [...indexStatements,...statisticsStatements])await store.query(statement);
    for(const [observedAt,page] of [[at,1],[at,2],[at,999],[at+15*86400000,1]]){
      const reader=createPostgresStateQueries(store,{clock:()=>observedAt,balancingReader:async time=>({observedAt:time})});
      try{
        const full=await reader.read({people:'all',sessionPage:page}),compact=await reader.read({people:'none',sessionPage:page});
        assert.deepEqual(compact,{...full,members:[],keys:[],peopleLoaded:false});
        const sessionView=await reader.read({view:'sessions',sessionPage:page});
        assert.equal(sessionView.view,'sessions');assert.deepEqual(sessionView.sessions,full.sessions);assert.deepEqual(sessionView.sessionsPagination,full.sessionsPagination);assert.deepEqual(sessionView.usageWindow,full.usageWindow);
        assert.deepEqual(sessionView.requests,[]);assert.deepEqual(sessionView.audit,[]);assert.deepEqual(sessionView.members,[]);assert.deepEqual(sessionView.keys,[]);assert.deepEqual(sessionView.daily,[]);
        for(const field of ['totals','balancing','recentUsageWindow','memberCount','keyCount'])assert.equal(Object.hasOwn(sessionView,field),false,field+' is not needed by the sessions page');
        assert.deepEqual(sessionView.accounts.map(row=>row.id),full.accounts.map(row=>row.id));
        const accountsView=await reader.read({view:'accounts'});
        assert.equal(accountsView.view,'accounts');
        assert.deepEqual(accountsView.accounts,full.accounts);
        assert.deepEqual(accountsView.balancing,full.balancing);
        assert.deepEqual(accountsView.usageWindow,full.usageWindow);
        for(const name of ['members','keys','sessions','sessionsPagination','requests','totals','daily','audit'])assert.equal(Object.hasOwn(accountsView,name),false,name+' is outside the accounts view');

        assert.deepEqual(compact.recentUsageWindow,{start:observedAt-12*3600000,end:observedAt,hours:12});
        assert.equal(compact.memberCount,1);assert.equal(compact.keyCount,0);
        const light=await reader.read({view:'overview'});
        for(const entry of [sessionView,light])assert.equal(entry.accounts.find(row=>row.id===accounts[1]).platform_models,JSON.stringify(['synthetic-model']));
        assert.deepEqual(light.totals,{requests:compact.totals.requests,success:compact.totals.success});
        assert.deepEqual(light.daily,compact.daily.map(({day,requests})=>({day,requests})));
        const costs=await reader.read({view:'overview-cost'});assert.deepEqual(costs.daily,compact.daily);assert.deepEqual(costs.usageWindow,light.usageWindow);
        for(const field of ['accounts','members','keys','requests','sessions','audit'])assert.equal(Object.hasOwn(costs,field),false);
        if(observedAt===at)assert.deepEqual(light,withoutIndex);

        if(observedAt===at){
          if(page===1)for(const filters of [{sessionQ:'合成概览员工'},{sessionQ:'no-synthetic-match'},{sessionAccount:accounts[0]},{sessionAccount:accounts[1]},{sessionStatus:'enabled'},{sessionStatus:'disabled'},{sessionType:'unclassified'}]){
            const original=await reader.read({...filters,people:'none',sessionPageSize:20}),selected=await reader.read({...filters,view:'sessions',sessionPageSize:20});assert.deepEqual(selected.sessions,original.sessions);assert.deepEqual(selected.sessionsPagination,original.sessionsPagination);
          }
          assert.equal(compact.totals.requests,5);assert.equal(compact.totals.standard.unpriced,1);assert.equal(compact.totals.standard.total_exact,'36028.797018963972');
          assert.equal(compact.sessionsPagination.total,27);assert.equal(compact.sessionsPagination.page,page===1?1:2);
          assert.equal(compact.sessions.length,page===1?25:2);
          if(page!==1)assert.equal(compact.sessions.find(row=>row.id===sessions[26]).request_count,2);
          assert.equal(compact.accounts.find(row=>row.id===accounts[1]).standard.requests,0);
        }else{assert.equal(compact.totals.requests,0);assert.deepEqual(compact.daily,[]);assert.equal(compact.sessionsPagination.total,0);assert.deepEqual(compact.requests,[]);}
      }finally{await reader.close();}
    }
    await store.query('DROP INDEX harbor.request_type_statistics_cover');
    await store.query('CREATE INDEX request_type_statistics_cover ON harbor.requests(usage_at) INCLUDE(status)');
    await assert.rejects(store.query(statisticsStatements[0]),/harbor_statistics_index_conflict/);
    assert.equal((await store.get("SELECT pg_get_indexdef('harbor.request_type_statistics_cover'::regclass) definition")).definition,'CREATE INDEX request_type_statistics_cover ON harbor.requests USING btree (usage_at) INCLUDE (status)');
    await store.query('DROP INDEX harbor.request_overview_cover');
    await store.query('CREATE INDEX request_overview_cover ON harbor.requests(usage_at) INCLUDE(status,ip)');
    const before=(await store.get("SELECT pg_get_indexdef('harbor.request_overview_cover'::regclass) definition")).definition;
    await assert.rejects(store.query(indexStatements[0]),/harbor_overview_index_conflict/);
    assert.equal((await store.get("SELECT pg_get_indexdef('harbor.request_overview_cover'::regclass) definition")).definition,before);
  }finally{await store?.close();await admin.query('DROP DATABASE IF EXISTS '+database);await admin.close();}
});

test('compact partial rollup matches every full DTO field for mixed frozen prices and incomplete usage',{
  skip:!url&&!socket?'需隔离PG；CI test_postgres执行真实链路':false
},async()=>{
  const admin=new PostgresStore(options()),database='harbor_test_'+randomUUID().replaceAll('-','');let store;
  try{
    await admin.query('CREATE DATABASE '+database+" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'");
    store=new PostgresStore(options(database));await store.migrate();await store.initializePricing();
    const at=Date.parse('2026-11-08T10:00:00+08:00'),member=randomUUID(),accounts=[randomUUID(),randomUUID()],sessions=[randomUUID(),randomUUID()],first=(await store.get('SELECT id FROM harbor.pricing_versions LIMIT 1')).id,second='synthetic-second-revision';
    await store.run('INSERT INTO harbor.pricing_versions VALUES($1,$2,$3)',second,at,JSON.stringify({rates:{},aliases:[]}));
    await store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',member,'合成聚合员工',at);
    for(const [index,id] of accounts.entries()){
      await store.run("INSERT INTO harbor.accounts(id,name,home) VALUES($1,$2,'synthetic')",id,'合成聚合账号'+index);
      await store.run('INSERT INTO harbor.sessions(id,member_id,account_id,created,updated) VALUES($1,$2,$3,$4,$4)',sessions[index],member,id,at);
    }
    const large='9007199254740993',records=[
      {amount:large},
      {amount:'7',revision:second},
      {amount:'13',revision:second,priceState:'manual'},
      {amount:null,priceState:'unmetered',status:'interrupted',unknown:true},
      {amount:null,priceState:'pending',status:'running',running:true},
      {amount:'100',projectedRevision:second},
      {amount:'100',revision:'synthetic-missing-book'},
      {amount:null,priceState:'expired'},
      {amount:null,priceState:'legacy_manual',status:'reconciled'},
      {noProjection:true},
      {amount:'101',unattributed:true},
      {amount:'19',account:1},
      {amount:'23',dayBefore:true},
      {amount:'0',status:'failed'}
    ];
    for(const record of records){
      const request=randomUUID(),ended=at-(record.dayBefore?86400000:100),started=ended-100,revision=record.revision??first,account=record.account??0;
      await store.run('INSERT INTO harbor.requests(id,member_id,account_id,session_id,started,ended,status,ip,model,price_version,usage_unknown) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)',
        request,record.unattributed?null:member,record.unattributed?null:accounts[account],record.unattributed?null:sessions[account],started,record.running?null:ended,record.status??'ok','synthetic-ip-'+account,'synthetic-model',revision,record.unknown?1:0);
      if(record.noProjection)continue;
      const amount=record.amount??null;
      await store.run('INSERT INTO harbor.request_prices(request_id,state,revision,total_atoms,input_atoms,output_atoms,cached_atoms,adjustment_atoms,measured_at) VALUES($1,$2,$3,$4,$5,$6,$6,$6,$7)',
        request,record.priceState??'priced',record.projectedRevision??revision,amount,amount,amount===null?null:'0',at);
    }
    const reader=createPostgresStateQueries(store,{clock:()=>at,balancingReader:async observedAt=>({observedAt})});
    try{
      for(const selection of [{},{sessionAccount:accounts[1]},{sessionStatus:'disabled'},{sessionQ:'不匹配'}]){
        const full=await reader.read({people:'all',...selection}),compact=await reader.read({people:'none',...selection});
        assert.deepEqual(compact,{...full,members:[],keys:[],peopleLoaded:false});
        const accountsView=await reader.read({view:'accounts'});
        assert.equal(accountsView.view,'accounts');
        assert.deepEqual(accountsView.accounts,full.accounts);
        assert.deepEqual(accountsView.balancing,full.balancing);
        assert.deepEqual(accountsView.usageWindow,full.usageWindow);
        for(const name of ['members','keys','sessions','sessionsPagination','requests','totals','daily','audit'])assert.equal(Object.hasOwn(accountsView,name),false,name+' is outside the accounts view');

        assert.equal(compact.totals.standard.requests,records.length);
        assert.equal(compact.totals.standard.known,7);
        assert.equal(compact.totals.standard.unpriced,3);
        assert.equal(compact.totals.standard.unmetered,2);
        assert.equal(compact.totals.standard.pending,1);
        assert.equal(compact.totals.standard.expired,1);
        assert.deepEqual(compact.totals.standard.revisions,[first,second].sort());
        assert.equal(compact.totals.standard.total_exact,'9007.199254741156');
        assert.equal(compact.accounts.find(row=>row.id===accounts[1]).standard.total_exact,'0.000000000019');
        assert.equal(compact.daily.length,2);
      }
    }finally{await reader.close();}
  }finally{await store?.close();await admin.query('DROP DATABASE IF EXISTS '+database);await admin.close();}
});
