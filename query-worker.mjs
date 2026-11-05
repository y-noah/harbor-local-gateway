import {parentPort,workerData} from 'node:worker_threads';
import {DatabaseSync} from 'node:sqlite';
import {quoteReference,publicUsage,decorateReferenceRequest,emptyReferenceQuote} from './pricing.mjs';
import {requestStatistics,requestList} from './request-query.mjs';

const db=new DatabaseSync(workerData.filename,{readOnly:true});
db.exec('PRAGMA busy_timeout=2000');
const cached=new Map();
parentPort.on('message',({id,query,window,at,kind})=>{
  try{
    db.exec('BEGIN');
    const version=db.prepare('SELECT version FROM request_query_version WHERE singleton=1').get().version;
    const key=JSON.stringify([kind,version,window.start,window.end,query.member,query.model,query.status,query.snapshot,...(kind==='list'?[query.page,query.pageSize,query.cursor]:[])]);
    let result=cached.get(key);
    if(!result){
      const active=JSON.parse(db.prepare("SELECT value FROM gateway_settings WHERE name='pricing.state'").get().value).active;
      const books=new Map();
      const book=revision=>{if(!books.has(revision)){const row=db.prepare('SELECT book FROM pricing_versions WHERE id=?').get(revision);books.set(revision,row?JSON.parse(row.book):null);}return books.get(revision);};
      const quote=row=>{
        const revision=row.price_version||active,value=book(revision);
        if(!value)return emptyReferenceQuote('unpriced',row.billing_model||row.model,revision);
        return quoteReference(row,value,revision);
      };
      result=kind==='list'?requestList({db,get:(sql,...args)=>db.prepare(sql).get(...args),all:(sql,...args)=>db.prepare(sql).all(...args),pricing:{decorate:row=>decorateReferenceRequest(row,publicUsage(quote(row)),window.start)}},query,window,at):requestStatistics(db,query,window,quote,at);
      cached.set(key,result);while(cached.size>8)cached.delete(cached.keys().next().value);
    }
    db.exec('COMMIT');parentPort.postMessage({id,result});
  }catch(e){
    try{db.exec('ROLLBACK');}catch{}
    parentPort.postMessage({id,error:{status:e.status===400?400:503,code:e.status===400?'invalid_request_snapshot':'statistics_unavailable',message:e.status===400?'请求分页快照已失效，请刷新列表':'统计暂不可用，请稍后重试'}});
  }
});
