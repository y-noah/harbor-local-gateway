import {randomUUID} from 'node:crypto';

// Benchmark-only observation. Synchronous waiting-count changes identify the
// actual enqueue branch; a quick bind/enter is never deducted as queue time.
// The ticket endpoint is named explicitly: it is not a TCP finish timestamp.
export function createCapacityStageProbe({context,sink=()=>{},clock=Date.now,maxPending=10000}={}){
  if(typeof context!=='function'||typeof sink!=='function'||typeof clock!=='function'||!Number.isSafeInteger(maxPending)||maxPending<1)throw new Error('capacity_stage_probe_configuration');
  let attached=false,closed=false,pending=0,dropped=0,missingTrace=0;const restores=[],tickets=new Set();
  const trace=()=>{const value=context()?.trace;if(typeof value==='string'&&/^[a-zA-Z0-9-]{1,100}$/.test(value))return value;missingTrace++;return null;};
  const emit=event=>{if(closed)return;try{sink(event);}catch{dropped++;}};
  function call(gate,original,receiver,args,category,owner){
    if(closed||!owner)return original.apply(receiver,args);
    if(pending>=maxPending){dropped++;return original.apply(receiver,args);}
    const spanId=randomUUID(),start=clock(),before=gate.status().waiting;pending++;
    let result;
    try{result=original.apply(receiver,args);}catch(error){pending--;emit({kind:'gateway-wait-span',trace:owner,spanId,category,start,end:clock(),queued:false,outcome:'rejected'});throw error;}
    const queued=gate.status().waiting===before+1;
    return Promise.resolve(result).then(value=>{pending--;emit({kind:'gateway-wait-span',trace:owner,spanId,category,start,end:clock(),queued,outcome:'granted'});return value;},error=>{pending--;emit({kind:'gateway-wait-span',trace:owner,spanId,category,start,end:clock(),queued,outcome:'rejected'});throw error;});
  }
  function attach(app){
    if(attached||closed)throw new Error('capacity_stage_probe_already_attached');
    if(typeof app?.preparation?.enter!=='function'||typeof app.preparation.status!=='function'||typeof app?.sessionHandoffs?.arrive!=='function'||typeof app.sessionHandoffs.status!=='function')throw new Error('capacity_stage_probe_invalid');
    attached=true;
    const enter=app.preparation.enter,arrive=app.sessionHandoffs.arrive;
    app.preparation.enter=function(...args){return call(app.preparation,enter,this,args,'preparation',trace());};
    app.sessionHandoffs.arrive=function(...args){
      const ticket=arrive.apply(this,args),owner=trace();if(!owner||closed)return ticket;
      if(tickets.size>=maxPending){dropped++;return ticket;}
      const bind=ticket.bind,close=ticket.close,entry={ticket,bind,close};tickets.add(entry);let released=false;
      ticket.bind=function(...values){return call(app.sessionHandoffs,bind,this,values,'handoff',owner);};
      ticket.close=function(...values){const result=close.apply(this,values);if(!released){released=true;tickets.delete(entry);ticket.bind=bind;ticket.close=close;emit({kind:'gateway-ticket-closed',trace:owner,at:clock()});}return result;};
      return ticket;
    };
    restores.push(()=>{app.preparation.enter=enter;app.sessionHandoffs.arrive=arrive;});return app;
  }
  return {attach,status:()=>({pending,tickets:tickets.size,dropped,missingTrace,closed}),close:()=>{if(closed)return;closed=true;for(const restore of restores.reverse())restore();for(const {ticket,bind,close} of tickets){ticket.bind=bind;ticket.close=close;}tickets.clear();}};
}
