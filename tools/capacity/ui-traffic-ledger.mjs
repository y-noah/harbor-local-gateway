import {createHash,randomUUID} from 'node:crypto';
import {performance} from 'node:perf_hooks';
import {capacityWireSignature} from './gateway-probe.mjs';
import {capacityObservedApiClass} from './observation-adapter.mjs';

const fail=code=>{throw new Error(code);};
const hash=value=>createHash('sha256').update(String(value)).digest('hex');
const sources=new Set(['browser','baseline','model','auxiliary']);
export function uiTrafficCategory(method,path){
 const signature=capacityWireSignature(method,path),kind=capacityObservedApiClass({method,route:signature.slice(signature.indexOf(' ')+1)});return kind==='static'?'asset':kind==='unclassified'?'auxiliary':kind;
}

// This ledger never sends HTTP. Headers enter only through fingerprinting;
// reports retain category counts and bounded, hashed issue references.
export function createUiTrafficLedger({origin,adminFingerprints,employeeFingerprints=[],scopeId=randomUUID(),maxRequests=500000,maxIssues=1000,now=()=>performance.now()}={}){
 const entry=new URL(origin);
 if(!['http:','https:'].includes(entry.protocol)||entry.username||entry.password||entry.pathname!=='/'||entry.search||entry.hash||!Array.isArray(adminFingerprints)||!adminFingerprints.length||!Array.isArray(employeeFingerprints)||!/^[-a-f0-9]{36}$/.test(scopeId)||!Number.isSafeInteger(maxRequests)||maxRequests<1||maxRequests>2000000||!Number.isSafeInteger(maxIssues)||maxIssues<1||maxIssues>10000||typeof now!=='function')fail('capacity_ui_ledger_configuration');
 const admin=[...adminFingerprints],employees=[...employeeFingerprints];
 if(admin.some(value=>!/^[a-f0-9]{64}$/.test(value))||new Set(admin).size!==admin.length||employees.some(value=>!/^[a-f0-9]{64}$/.test(value)))fail('capacity_ui_ledger_actors');
 const rows=new Map(),wireIds=new Map(),requests=new Map(),issues=[],unknown=new Map();let phase='preparation',sequence=0,dropped=0,issueCount=0,measurementStart=null,measurementEnd=null,closed=false;
 function issue(code,trace){issueCount++;if(issues.length<maxIssues)issues.push({code,reference:hash(trace).slice(0,16)});}
 function assertOpen(){if(closed)fail('capacity_ui_ledger_closed');}
 function expected(source,index){const values=source==='model'?employees:admin;if(!Number.isInteger(index)||index<0||index>=values.length)fail('capacity_ui_ledger_actor');return {field:source==='model'?'authorization':'cookie',value:values[index]};}
 function plan({source='browser',actorIndex=0,method='GET',route,credentials}={}){
  assertOpen();if(!sources.has(source)||!['GET','POST','PATCH','PUT','DELETE','HEAD','OPTIONS'].includes(method))fail('capacity_ui_ledger_request');
  if(typeof route!=='string'||route.length>500||!route.startsWith('/')||route.startsWith('//')||/[\r\n]/.test(route))fail('capacity_ui_ledger_route');const parsed=new URL(route,entry);if(parsed.origin!==entry.origin||parsed.username||parsed.password)fail('capacity_ui_ledger_foreign_origin');
  const binding=expected(source,actorIndex);if(credentials?.[binding.field]!==binding.value)fail('capacity_ui_ledger_credential_mismatch');
  const trace=scopeId+'-'+(++sequence);if(rows.size>=maxRequests){dropped++;fail('capacity_ui_ledger_budget');}
  rows.set(trace,{source,actorIndex,phase,signature:capacityWireSignature(method,parsed.href),category:uiTrafficCategory(method,parsed.href),credentials:{[binding.field]:binding.value},issuedAt:now(),clientResponse:0,clientTerminal:0,serverStart:0,serverTerminal:0,clientOutcome:null,serverOutcome:null,clientStatus:null,serverStatus:null,wireId:null,terminalWireId:null,wireIdentityValid:true,notSent:false});
  return trace;
 }
 function client(trace,event){
  assertOpen();const row=rows.get(trace);if(!row){issue('unplanned_client_event',trace);return;}
  if(event.kind==='response'){row.clientResponse++;if(row.clientResponse>1)issue('duplicate_client_response',trace);row.clientStatus=event.status;}
  else if(['finished','failed','cancelled'].includes(event.kind)){row.clientTerminal++;if(row.clientTerminal>1)issue('duplicate_client_terminal',trace);row.clientOutcome=event.kind;row.clientEnd=now();if(event.notSent===true){if(row.serverStart||row.serverTerminal)issue('not_sent_conflicts_with_wire',trace);row.notSent=true;}}
  else fail('capacity_ui_ledger_client_event');
 }
 function server(event){
  assertOpen();if(!['gateway-request-start','gateway-response-end','gateway-response-abort'].includes(event?.kind))return;
  const row=rows.get(event.trace);
  if(!row){const signature=typeof event.signature==='string'&&/^(GET|POST|PATCH|PUT|DELETE|HEAD|OPTIONS) \/[a-zA-Z0-9_:/?=.-]{0,200}$/.test(event.signature)?event.signature:'unclassified',key=event.kind+' '+signature;if(unknown.has(key)||unknown.size<maxIssues)unknown.set(key,(unknown.get(key)??0)+1);else dropped++;return;}
  if(row.notSent)issue('wire_after_not_sent',event.trace);
  if(event.kind==='gateway-request-start'){
   row.serverStart++;if(typeof event.wireId!=='string'||!/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(event.wireId)){row.wireIdentityValid=false;issue('server_wire_identity_missing',event.trace);}else{if(wireIds.has(event.wireId)&&wireIds.get(event.wireId)!==event.trace){row.wireIdentityValid=false;issue('server_wire_identity_reused',event.trace);}else wireIds.set(event.wireId,event.trace);row.wireId??=event.wireId;}if(row.serverStart>1)issue('duplicate_server_arrival',event.trace);
   if(event.signature!==row.signature)issue('server_signature_mismatch',event.trace);
   const field=row.source==='model'?'authorization':'cookie';if(event.credentials?.[field]!==row.credentials[field])issue('server_credential_mismatch',event.trace);
  }else{
   row.serverTerminal++;row.terminalWireId=event.wireId??null;if(!row.wireId||event.wireId!==row.wireId){row.wireIdentityValid=false;issue('server_wire_terminal_mismatch',event.trace);}if(row.serverTerminal>1)issue('duplicate_server_terminal',event.trace);
   row.serverOutcome=event.kind==='gateway-response-end'?'finished':'aborted';row.serverStatus=event.status??null;
  }
 }
 function collect(){
  const counts=new Map(),missing=[],failures=[];let matched=0,pending=0,notSent=0;
  for(const row of rows.values()){
   const key=[row.phase,row.source,row.category].join('/'),c=counts.get(key)??{phase:row.phase,source:row.source,category:row.category,planned:0,serverArrivals:0,serverFinished:0,serverAborted:0,clientFinished:0,clientFailed:0,clientCancelled:0,notSent:0,normalAttempts:0,httpSucceeded:0,httpFailed:0,httpNonSuccess:0,cancelledHttpNonSuccess:0,statusClasses:{},pending:0};
   if(row.clientStatus!==null){const key=Number.isInteger(row.clientStatus)?Math.floor(row.clientStatus/100)+'xx':'invalid';c.statusClasses[key]=(c.statusClasses[key]??0)+1;if(row.clientStatus<200||row.clientStatus>=300)c.httpNonSuccess++;if((row.clientStatus<200||row.clientStatus>=300)&&row.clientOutcome==='cancelled')c.cancelledHttpNonSuccess++;}if(row.clientTerminal&&row.clientOutcome!=='cancelled'){c.normalAttempts++;if(row.clientOutcome==='finished'&&row.clientStatus>=200&&row.clientStatus<300)c.httpSucceeded++;else c.httpFailed++;}
   c.planned++;c.serverArrivals+=row.serverStart;c.serverFinished+=row.serverOutcome==='finished'?1:0;c.serverAborted+=row.serverOutcome==='aborted'?1:0;c.clientFinished+=row.clientOutcome==='finished'?1:0;c.clientFailed+=row.clientOutcome==='failed'?1:0;c.clientCancelled+=row.clientOutcome==='cancelled'?1:0;
   if(row.notSent&&row.serverStart===0&&row.serverTerminal===0&&row.clientTerminal===1){notSent++;c.notSent++;}
   else if(row.serverStart===1&&row.serverTerminal===1&&row.clientTerminal===1&&row.wireIdentityValid&&row.wireId&&row.terminalWireId===row.wireId){matched++;if(row.clientOutcome==='finished'&&(row.serverOutcome!=='finished'||row.clientStatus!==row.serverStatus||!Number.isInteger(row.clientStatus)))failures.push('client_server_completion_mismatch');}
   else{pending++;c.pending++;}
   counts.set(key,c);
  }
  if(dropped)missing.push('traffic_ledger_budget_exhausted');if(unknown.size)missing.push('unattributed_server_traffic');if(pending)missing.push('unreconciled_client_server_requests');if(measurementStart===null||measurementEnd===null)missing.push('measurement_window_not_closed');
  if(issueCount)failures.push('traffic_ledger_integrity_error');
  return {passed:failures.length?false:missing.length?null:true,scopeId,scope:'independent-ui-additional-load',normalMixReplaced:false,measurementMs:measurementStart!==null&&measurementEnd!==null?measurementEnd-measurementStart:null,planned:rows.size,matched,notSent,accounted:matched+notSent,pending,dropped,issueCount,issues:[...issues],failures:[...new Set(failures)],missing,counts:[...counts.values()],unattributed:[...unknown].map(([operation,count])=>({operation,count})),containsRawCredentials:false};
 }
 const observer={
  beforeRequest:async({requestId,actorIndex,source='browser',method,route,credentials})=>{
   if(typeof requestId!=='string'||!/^[a-zA-Z0-9-]{1,100}$/.test(requestId)||requests.has(requestId))fail('capacity_ui_ledger_request_identity');
   const trace=plan({source,actorIndex,method,route,credentials});requests.set(requestId,trace);return {'x-capacity-trace':trace};
  },
  response:({requestId,status})=>client(requests.get(requestId),{kind:'response',status}),
  requestFinished:({requestId})=>client(requests.get(requestId),{kind:'finished'}),
  requestFailed:({requestId,cancelled=false,notSent=false})=>client(requests.get(requestId),{kind:cancelled?'cancelled':'failed',notSent})
 };
 return {scopeId,plan,client,server,observer,collect,
  beginMeasurement:()=>{assertOpen();if(measurementStart!==null)fail('capacity_ui_ledger_window');measurementStart=now();phase='measurement';},
  endMeasurement:()=>{assertOpen();if(measurementStart===null||measurementEnd!==null)fail('capacity_ui_ledger_window');measurementEnd=now();phase='drain';},
  phase:()=>phase,close:()=>{closed=true;},status:()=>({phase,requests:rows.size,pending:collect().pending,closed})};
}
