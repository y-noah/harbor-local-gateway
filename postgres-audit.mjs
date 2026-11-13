import {id,AppError} from './store.mjs';
import {requireDepartmentScope} from './department-access.mjs';
import {createAdminDepartments} from './postgres-admin-departments.mjs';
import {createRoutingDepartments} from './postgres-routing-departments.mjs';
import {verifyDepartmentIsolationAvailability,assertDepartmentIsolationReadiness} from './postgres-departments.mjs';
const subjects={account:'accounts',member:'members',key:'keys',session:'sessions',request:'requests',type:'member_types',department:'departments',export:'recording_export_jobs'};
const denied=()=>{throw new AppError(403,'审计缺少可信身份或对象归属不符','department_forbidden');};
const instances=new WeakMap();
export async function assertPostgresAuditScopes(store){
 if(await store.get('SELECT 1 FROM harbor.audit a LEFT JOIN harbor.audit_scopes o ON o.audit_id=a.id WHERE o.audit_id IS NULL LIMIT 1'))throw new AppError(503,'存在缺少可信归属的审计记录，请完成审计写入适配','audit_scope_required');
}
const systemActions={
 'routing.nightly':/^(session\.(route|handoff\.(requested|completed|cancelled|failed|superseded))|balancing\.weights\.auto)$/,
 'routing.expiry':/^(session\.nightly\.expired|balancing\.weights\.(expired|auto))$/,
 'device.enrollment':/^device\.(register|renewal_request)$/,
 'account.quota.refresh':/^(account\.package\.recognize|balancing\.weights\.auto)$/,
 'account.login.recover':/^account\.login\.failed$/,
 'session.handoff.finalize':/^session\.handoff\.(cancelled|failed|superseded)$/,
 'account.extra.recover':/^account\.extra\./,
};
export function createPostgresAudit(store,{clock=Date.now}={}){
 if(instances.get(store)?.has(clock))return instances.get(store).get(clock);
 const admin=createAdminDepartments(store),routing=createRoutingDepartments(store,{clock});let initialization;
 async function installed(){const pending=initialization??=verifyDepartmentIsolationAvailability(store);let value;try{value=await pending;}catch(error){if(initialization===pending)initialization=undefined;throw error;}await assertDepartmentIsolationReadiness(store,value);return value;}
 async function write(scope,{action,detail,subject,at=clock()}={}){
  if(typeof action!=='string'||!action.length||action.length>160||!Number.isSafeInteger(at)||at<0||subject&&(!Object.hasOwn(subjects,subject.kind)||typeof subject.id!=='string'||!subject.id.length))throw new AppError(503,'审计调用参数无效','audit_configuration');
  const isolated=await installed(),auditId=id(),content=typeof detail==='string'?detail:JSON.stringify(detail??{});
  return store.transaction(async()=>{
   if(!isolated){if(scope!==undefined&&scope!==null&&requireDepartmentScope(scope,{allowGlobal:true}).view!=='global')denied();await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',auditId,at,action,content);return auditId;}
   const actor=requireDepartmentScope(scope,{allowGlobal:true,allowSystem:true,roles:['super_admin','department_admin','employee','system']});
   if(actor.role==='employee'){if(action==='account.extra.reservation_transfer'&&subject?.kind!=='request')denied();if(!['device.hardware_review','session.route','balancing.weights.auto','account.route_block','account.route_recover','account.extra.reservation_transfer'].includes(action))denied();await routing.resolve(actor,{employeeOnly:true,lock:true});}
   else if(actor.role==='system'){
    if(!systemActions[actor.purpose]?.test(action)||actor.view!=='department')denied();
    const automaticTerminal=actor.purpose==='routing.nightly'&&/^session\.handoff\.(cancelled|failed|superseded)$/.test(action);
    if(automaticTerminal){
     let event=detail;try{if(typeof event==='string')event=JSON.parse(event);}catch{denied();}
     if(subject?.kind!=='session'||!event||typeof event.handoff!=='string'||!await store.get(`SELECT h.id FROM harbor.session_handoffs h JOIN harbor.automatic_handoff_groups g ON g.id=h.automatic_group_id AND g.department_id=h.department_id JOIN harbor.routing_plans p ON p.id=g.plan_id AND p.department_id=g.department_id JOIN harbor.background_jobs j ON j.id=p.id AND j.department_id=p.department_id WHERE h.id=$1 AND h.department_id=$2 AND h.session_id=$3 AND h.origin='automatic' AND h.actor_kind='system' AND h.actor_id='system:routing.nightly' AND h.actor_version=0 AND h.status=$4 AND h.finished IS NOT NULL AND g.status=$4 AND g.finished IS NOT NULL AND j.kind='routing.nightly' AND j.status='completed' AND j.actor_kind='system' AND j.actor_id='system:routing.nightly' AND j.actor_version=0 AND j.scope_kind='department' AND j.scope_identity='department:'||h.department_id`,event.handoff,actor.department_id,subject.id,action.split('.').at(-1)))denied();
    }else if(['account.login.recover','session.handoff.finalize'].includes(actor.purpose)){
     if(!await store.get('SELECT id FROM harbor.departments WHERE id=$1 FOR SHARE',actor.department_id))denied();
     if(actor.purpose==='account.login.recover'&&subject?.kind!=='account')denied();
     if(actor.purpose==='session.handoff.finalize'){
      let event=detail;try{if(typeof event==='string')event=JSON.parse(event);}catch{denied();}
      if(subject?.kind!=='session'||!event||typeof event.handoff!=='string'||!await store.get("SELECT id FROM harbor.session_handoffs WHERE id=$1 AND department_id=$2 AND session_id=$3 AND origin='manual' AND status=$4 AND finished IS NOT NULL",event.handoff,actor.department_id,subject.id,action.split('.').at(-1)))denied();
     }
    }else await routing.resolve(actor,{lock:true});
   }
   else{
    const lifecycle=actor.role==='super_admin'&&subject?.kind==='department'&&['department.create','department.update','department.management_key.issue','department.management_key.revoke'].includes(action);
    await admin.current(actor,{lock:true,write:action!=='recording.export.download'&&!lifecycle});
   }
   let departmentId=actor.department_id;
   if(subject){let owner;if(subject.kind==='department')owner=await store.get('SELECT id department_id FROM harbor.departments WHERE id=$1',subject.id);else if(subject.kind==='export')owner=await store.get('SELECT department_id FROM harbor.background_jobs WHERE id=$1 AND kind=$2',subject.id,'recording.export');else owner=await store.get('SELECT department_id'+(subject.kind==='member'?',id member_id':['key','session','request'].includes(subject.kind)?',member_id':'')+' FROM harbor.'+subjects[subject.kind]+' WHERE id=$1',subject.id);
    if(!owner||actor.view==='department'&&owner.department_id!==actor.department_id||actor.role==='employee'&&['member','key','session','request'].includes(subject.kind)&&owner.member_id!==actor.member_id)denied();departmentId=owner.department_id;
   }
   if(action==='pricing.configure'){if(actor.role!=='super_admin'||actor.view!=='global'||subject)denied();departmentId=null;}
   if(departmentId===null&&!(action==='pricing.configure'||actor.role==='super_admin'&&subject?.kind==='export'))denied();
   await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',auditId,at,action,content);
   await store.run('INSERT INTO harbor.audit_scopes(audit_id,department_id,scope_kind,scope_identity,actor_kind,actor_id,actor_version,subject_kind,subject_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)',auditId,departmentId,departmentId===null?'global':'department',departmentId===null?'global':'department:'+departmentId,actor.role,actor.credential_id,actor.credential_version,subject?.kind??null,subject?.id??null);
   return auditId;
  });
 }
 async function writeMany(scope,events,{at=clock()}={}){
  if(!Array.isArray(events)||!Number.isSafeInteger(at)||at<0)throw new AppError(503,'审计批量参数无效','audit_configuration');
  if(await installed())return store.transaction(async()=>{const result=[];for(const event of events)result.push(await write(scope,{...event,at}));return result;});
  if(scope!==undefined&&scope!==null&&requireDepartmentScope(scope,{allowGlobal:true}).view!=='global')denied();
  const rows=events.map(event=>{if(typeof event.action!=='string'||!event.action.length)throw new AppError(503,'审计批量参数无效','audit_configuration');return {...event,audit:event.audit??id(),at,detail:typeof event.detail==='string'?event.detail:JSON.stringify(event.detail??{})};});
  await store.transaction(()=>store.run('INSERT INTO harbor.audit SELECT p.audit,p.at,p.action,p.detail FROM jsonb_to_recordset($1::jsonb) AS p(audit text,at bigint,action text,detail text)',JSON.stringify(rows)));
  return rows.map(row=>row.audit);
 }
 const service={write,writeMany};if(!instances.has(store))instances.set(store,new Map());instances.get(store).set(clock,service);return service;
}
