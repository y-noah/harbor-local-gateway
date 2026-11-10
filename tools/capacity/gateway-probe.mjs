import {AsyncLocalStorage} from 'node:async_hooks';
import {createHash,randomUUID} from 'node:crypto';
import {isPromise} from 'node:util/types';
import {startRuntimeMetrics} from './runtime-metrics.mjs';
const promiseThen=Promise.prototype.then;

const routeWords=new Set(('api admin type-statistics device-policy recording codex sessions challenge chat completions coverage device device-invitations enroll explanation export image-mcp.mjs ips member-action-batches nightly plans platform previews providers quotas rebuild refresh register schedule selection weights download cancel progress revoke renew verify credentials certificates login logout session state status requests members keys devices history member-types templates recordings search exports retention jobs config usage-limits model-permissions allowed-models quota check handoffs accounts packages account-packages account-policy preview execute retry undo models me health ready v1 responses batch member-config-batches member-access-batches key-access-batches device-revocations pricing policy limits defaults account-checks quota-refresh extra-usage manual reset-card-used records batches app.js style.css index.html favicon.ico').split(' '));
const digest=value=>createHash('sha256').update(value).digest('hex');
// Unknown paths and invalid query values remain distinguishable without
// exporting identifiers, user text, credentials or arbitrary query strings.
export function capacityWireSignature(method,path){
 const verb=['GET','POST','PATCH','PUT','DELETE','HEAD','OPTIONS'].includes(method)?method:'OTHER';
 try{const url=new URL(path,'http://capacity.invalid'),typeRoute=/^\/api\/admin\/member-types\/([a-zA-Z0-9-]{1,80})(\/templates)?$/.exec(url.pathname),parameterizedType=typeRoute&&(method==='PATCH'&&!typeRoute[2]||method==='GET'&&typeRoute[2]),parts=url.pathname.split('/').filter(Boolean).map((part,index)=>parameterizedType&&index===3?':id':/^(?:[a-z0-9_.-]+(?::|%3a))?[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(part)?':id':part),known=parts.every(part=>part===':id'||part===':invalid'||/^:other-[a-f0-9]{16}$/.test(part)||routeWords.has(part)||/^usage-[a-f]$/.test(part)),route=known?'/'+parts.join('/'):(url.pathname.startsWith('/api/')?'/api/:other-':'/:other-')+digest(url.pathname).slice(0,16);return verb+' '+route+(route==='/api/admin/requests'?'?view='+(['list','summary','all'].includes(url.searchParams.get('view')||'all')?(url.searchParams.get('view')||'all'):'invalid'):'');}catch{return verb+' /:invalid';}
}
const fingerprints=headers=>Object.fromEntries(['cookie','authorization'].map(key=>[key,typeof headers[key]==='string'?digest(headers[key]):null]));

// Benchmark-only wrappers. No production source or authentication is replaced.
// Measured overhead remains part of the observed gateway cost.
export function createGatewayProbe({sink=()=>{},maxPendingAdmissions=10000}={}){
  if(!Number.isSafeInteger(maxPendingAdmissions)||maxPendingAdmissions<1)throw new Error('capacity_admission_probe_budget_invalid');
  const context=new AsyncLocalStorage(),transactions=new AsyncLocalStorage(),restores=[];let sampler,dropped=0,pendingAdmissions=0,unconfirmedAdmissions=0,pendingHandlers=0,unobservableHandlers=0,attached=false,closed=false,closePromise;
  const queuedContexts=new WeakMap(),recoveryOwners=new Map();
  const emit=record=>{try{const returned=sink(record);if(returned&&(isPromise(returned)||typeof returned.then==='function')){dropped++;Promise.resolve(returned).catch(()=>{});}}catch{dropped++;}};
  const rejectionCodes=new Set(['gateway_busy','inference_capacity_busy','gateway_ownership_lost','member_model_limit_exceeded','session_quota_exhausted','no_account_capacity','member_busy','routing_preparation_busy','metering_recovery_required']);
  const publishAdmission=(row,event)=>{row.requestId=event.requestId;emit({...event,at:Date.now()});};
  const publishUnconfirmed=events=>{for(const event of events){unconfirmedAdmissions++;emit({...event,kind:'gateway-admission-unconfirmed',at:Date.now()});}};
  function wrap(object,key,create){const original=object[key];object[key]=create(original);restores.push(()=>{object[key]=original;});}
  function attach(app){
    if(attached||closed)throw new Error('capacity_gateway_probe_already_attached');const servers=[app.server,app.deviceTls?.server].filter(Boolean);if(!servers.length||servers.some(server=>server.listeners('request').length!==1)||typeof app.store?.observe!=='function'||typeof app.store?.finish!=='function'||typeof app.sessionHandoffs?.arrive!=='function'||typeof app.status!=='function')throw new Error('capacity_gateway_probe_invalid');attached=true;
    for(const server of [app.server,app.deviceTls?.server].filter(Boolean)){
      const handlers=server.listeners('request');if(handlers.length!==1)throw new Error('capacity_gateway_handler_ambiguous');
      const original=handlers[0],handler=function(req,res){
        const input=req.headers['x-capacity-trace'],trace=typeof input==='string'&&/^[a-zA-Z0-9-]{1,100}$/.test(input)?input:null,wireId=randomUUID(),signature=capacityWireSignature(req.method,req.url),row={trace,start:Date.now(),requestId:null,model:null};let terminal=false;
        emit({kind:'gateway-request-start',trace,wireId,signature,credentials:fingerprints(req.headers),at:row.start});
        const originalEnd=res.end;res.end=function(chunk,...args){if(res.statusCode>=400&&(typeof chunk==='string'||Buffer.isBuffer(chunk))&&Buffer.byteLength(chunk)<=4096){try{const code=JSON.parse(String(chunk))?.error?.code;emit({kind:'gateway-rejection',trace,wireId,status:res.statusCode,code:rejectionCodes.has(code)?code:'unknown',at:Date.now()});}catch{}}return originalEnd.call(this,chunk,...args);};
        return context.run(row,()=>{
          req.once('end',()=>{row.bodyEnd=Date.now();emit({kind:'gateway-body-end',trace,wireId,at:row.bodyEnd});});
          res.once('finish',()=>{terminal=true;emit({kind:'gateway-response-end',trace,wireId,signature,requestId:row.requestId,status:res.statusCode,at:Date.now()});});
          res.once('close',()=>{if(!terminal){terminal=true;emit({kind:'gateway-response-abort',trace,wireId,signature,requestId:row.requestId,status:res.statusCode,at:Date.now()});}});
          let result;
          try{result=original.call(this,req,res);}catch(error){unobservableHandlers++;emit({kind:'gateway-handler-unobservable',trace,wireId,signature,reason:'handler_threw',at:Date.now()});throw error;}
          if(!isPromise(result)){unobservableHandlers++;emit({kind:'gateway-handler-unobservable',trace,wireId,signature,reason:'not_awaitable',at:Date.now()});return result;}
          pendingHandlers++;
          const settled=outcome=>{
            pendingHandlers--;
            if(row.captureFinished)emit({kind:'gateway-captured-output',trace,requestId:row.requestId,verified:!row.captureUnproven,fields:[...(row.capturedOutput??[])].map(([field,value])=>({field,hash:value.hash.copy().digest('hex'),bytes:value.bytes})),at:Date.now()});
            emit({kind:'gateway-handler-settled',trace,wireId,signature,requestId:row.requestId,outcome,at:Date.now()});
          };
          try{promiseThen.call(result,()=>settled('resolved'),()=>settled('rejected'));}catch{pendingHandlers--;unobservableHandlers++;emit({kind:'gateway-handler-unobservable',trace,wireId,signature,reason:'observation_unavailable',at:Date.now()});}
          return result;
        });
      };
      server.removeListener('request',original);server.on('request',handler);restores.push(()=>{server.removeListener('request',handler);server.on('request',original);});
    }
    // An ID allocated inside a transaction is not an admission fact until
    // the outer transaction commits. Concurrent async branches stay separate.
    if(typeof app.store.transaction==='function')wrap(app.store,'transaction',original=>function(...args){
      const row=context.getStore();if(!row)return original.apply(this,args);const parent=transactions.getStore(),frame={row,parent:parent?.row===row&&!parent.finished?parent:null,events:[],finished:false};
      return transactions.run(frame,async()=>{
        try{const result=await original.apply(this,args);frame.finished=true;const events=frame.events;frame.events=[];
          if(frame.parent&&!frame.parent.finished)for(const event of events)frame.parent.events.push(event);
          else{pendingAdmissions-=events.length;if(frame.parent)publishUnconfirmed(events);else for(const event of events)publishAdmission(row,event);}return result;
        }catch(error){frame.finished=true;pendingAdmissions-=frame.events.length;publishUnconfirmed(frame.events);frame.events=[];throw error;}
      });
    });
    wrap(app.store,'observe',original=>async function(...args){const options=args[4],canStamp=options===undefined||options!==null&&typeof options==='object',startedAt=canStamp?(options?.at===undefined?Date.now():options.at):undefined;if(canStamp)args[4]={...options,at:startedAt};const result=await original.apply(this,args),row=context.getStore();if(row){row.model=typeof args[3]==='string'?args[3]:null;const event={kind:'gateway-admitted',trace:row.trace,requestId:result,memberId:args[0].id,keyId:args[0].key_id,deviceId:args[4]?.device?.deviceId??null,startedAt},frame=transactions.getStore();if(frame?.row===row&&!frame.finished){if(pendingAdmissions>=maxPendingAdmissions)dropped++;else{frame.events.push(event);pendingAdmissions++;}}else publishAdmission(row,event);}return result;});
    wrap(app.store,'finish',original=>async function(...args){const result=await original.apply(this,args),row=context.getStore();if(row)emit({kind:'gateway-settled',trace:row.trace,requestId:args[0],contextMatches:row.requestId===args[0],at:Date.now(),settled:result.settled,usageKnown:args[1]?.usageKnown!==false});return result;});
    // A shared recovery drain can run in another request's async context.
    // Bind only accepted queue commands, not arbitrary store.finish calls.
    const recovery=app.meteringRecovery?.buffer;
    if(recovery&&['enqueue','apply','released','endPhase'].every(key=>typeof recovery[key]==='function')&&Array.isArray(recovery.queue)){
      let replayRequest=null;
      const retire=requestId=>{const owner=recoveryOwners.get(requestId);if(owner&&--owner.commands===0)recoveryOwners.delete(requestId);};
      wrap(recovery,'enqueue',original=>function(command){
        const row=context.getStore(),accepted=original.call(this,command);
        if(accepted&&command?.type==='settle'&&row){
          const value=this.queue.at(-1)?.value;
          if(!value||value.type!=='settle'||value.requestId!==command.requestId){dropped++;return accepted;}
          queuedContexts.set(value,row);const owner=recoveryOwners.get(value.requestId);
          if(owner){owner.commands++;if(owner.row!==row)dropped++;}
          else if(recoveryOwners.size<maxPendingAdmissions)recoveryOwners.set(value.requestId,{row,commands:1});else dropped++;
        }
        return accepted;
      });
      wrap(recovery,'apply',original=>async function(value){
        const owner=value?.type==='settle'?recoveryOwners.get(value.requestId):null,row=queuedContexts.get(value)??owner?.row;
        if(this.writerPhase==='replay')replayRequest=owner?value.requestId:null;
        return row?context.run(row,()=>original.call(this,value)):original.call(this,value);
      });
      wrap(recovery,'released',original=>function(entries){const result=original.call(this,entries);if(this.writerPhase==='atomic')for(const entry of entries)if(entry.value.type==='settle')retire(entry.value.requestId);return result;});
      wrap(recovery,'endPhase',original=>function(phase,started,succeeded){const result=original.call(this,phase,started,succeeded);if(phase==='replay'){if(succeeded&&replayRequest)retire(replayRequest);replayRequest=null;}return result;});
    }
    // Hash only the already-filtered recording commands. A cancelled client
    // can leave a genuine captured prefix even after the upstream finished.
    // This private request-local witness is never a normal-success oracle.
    if(typeof app.buffer?.append==='function'&&typeof app.buffer?.finish==='function'){
      wrap(app.buffer,'append',original=>function(requestId,events){
        const accepted=original.call(this,requestId,events),row=context.getStore();if(!row||row.requestId!==requestId)return accepted;
        const output=row.capturedOutput??=(new Map());if(accepted!==true||!Array.isArray(events)){row.captureUnproven=true;return accepted;}
        for(const event of events){if(event?.kind!=='assistant')continue;const part=event.content;
          if(typeof part?.text!=='string'||typeof part.field!=='string'||!['append','replace'].includes(part.mode)){row.captureUnproven=true;continue;}
          const field=digest(part.field);if(!output.has(field)&&output.size>=16){row.captureUnproven=true;continue;}
          if(!output.has(field)||part.mode==='replace')output.set(field,{hash:createHash('sha256'),bytes:0});const state=output.get(field);state.hash.update(part.text);state.bytes+=Buffer.byteLength(part.text);
        }return accepted;
      });
      wrap(app.buffer,'finish',original=>function(requestId,...args){
        const accepted=original.call(this,requestId,...args),row=context.getStore();if(row&&row.requestId===requestId){row.captureFinished=true;if(accepted!==true)row.captureUnproven=true;}return accepted;
      });
    }
    wrap(app.sessionHandoffs,'arrive',original=>function(...args){const ticket=original.apply(this,args),bind=ticket.bind;ticket.bind=async function(...values){const start=Date.now();try{return await bind.apply(this,values);}finally{emit({kind:'handoff-boundary-call',trace:context.getStore()?.trace??null,durationMs:Date.now()-start,at:Date.now(),deductibleWait:false});}};return ticket;});
    sampler=startRuntimeMetrics({sink:emit,status:app.status});return app;
  }
  return {context:()=>context.getStore(),attach,status:()=>({dropped,pendingAdmissions,unconfirmedAdmissions,pendingHandlers,unobservableHandlers,pendingRecoveryBindings:recoveryOwners.size}),close:()=>closePromise??=(async()=>{closed=true;try{await sampler?.stop();}finally{recoveryOwners.clear();for(const restore of restores.reverse())restore();}})()};
}
