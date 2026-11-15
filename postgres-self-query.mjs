import {requireDepartmentScope} from './department-access.mjs';
import {createRoutingDepartments} from './postgres-routing-departments.mjs';
import {AppError,usageWindow,storedModelAllowlist} from './store.mjs';
import {fields,validPrice,joins,aggregate,aggregateSql} from './postgres-reference-query.mjs';
import {createPostgresMemberLimits} from './postgres-member-limits.mjs';
import {createPostgresConsumptionPolicy,verifyConsumptionPolicyAvailability} from './postgres-consumption-policy.mjs';
import {verifyUsageRollupAvailability,usageRollupsReady,usageRollupMoneySql} from './postgres-usage-rollups.mjs';

export function createPostgresSelfQuery(store,{queryQueue,clock=Date.now}={}){
  const access=createRoutingDepartments(store,{clock}),limits=createPostgresMemberLimits(store,{clock}),consumption=createPostgresConsumptionPolicy(store,{clock});let consumptionInitialization,rollupInitialization;
  return (secret,{scope,signal}={})=>queryQueue.run(async()=>{
   const initialized=await (consumptionInitialization??=verifyConsumptionPolicyAvailability(store).then(available=>({available}),error=>{consumptionInitialization=undefined;throw error;}));
   return store.transaction(async()=>{
    const at=clock(),member=await store.authenticate(secret,{at}),currentScope=store.departmentScope?.(member),department=await access.resolve(currentScope,{member,employeeOnly:true}),window=usageWindow(at);
    if(scope!==undefined){const initial=requireDepartmentScope(scope,{roles:['employee']});if(!department||['department_id','member_id','credential_id','credential_version','member_version','department_version'].some(field=>initial[field]!==department[field]))throw new AppError(403,'员工或部门授权已变化','department_forbidden');}
    const policy=initialized.available?await consumption.read(member.key_id,{memberId:member.id}):null;
    if(department)await (rollupInitialization??=verifyUsageRollupAvailability(store).catch(error=>{rollupInitialization=undefined;throw error;}));
    let values;
    if(department&&await usageRollupsReady(store)){
      // Authentication fixes both owners in this snapshot. The same bounded
      // hourly projection preserves edge pricing. Department composite foreign
      // keys prove each Key request belongs to its authenticated member;
      // legacy schemas lack that guarantee and keep the paired raw predicate.
      const empty={requests:0,known:0,unmetered:0,unpriced:0,pending:0,expired:0,...Object.fromEntries(fields.map(field=>[field,'0']))};
      const usage=async(kind,owner)=>aggregate((await store.all(usageRollupMoneySql(kind),[owner],window.start,window.end,0,0))[0]??empty).standard;
      values={member:await usage('member',member.id),key:await usage('key',member.key_id)};
    }else{
      const rows=await store.all(`WITH data AS MATERIALIZED (
        SELECT r.key_id,r.started,r.ended,r.status,
          CASE WHEN ${validPrice} THEN p.state ELSE 'unpriced' END price_state,
          CASE WHEN ${validPrice} THEN p.revision ELSE NULL END revision,
          ${fields.map(f=>`CASE WHEN ${validPrice} THEN p.${f}_atoms ELSE NULL END ${f}`).join(',')}
        FROM harbor.requests r${joins} WHERE r.member_id=$1 AND r.usage_at>=$2 AND r.usage_at<$3${department?' AND r.department_id=$5':''}
      ) SELECT 'member' scope,${aggregateSql} FROM data UNION ALL SELECT 'key' scope,${aggregateSql} FROM data WHERE key_id=$4`,member.id,window.start,window.end,member.key_id,...(department?[department.department_id]:[]));
      values=Object.fromEntries(rows.map(row=>[row.scope,aggregate(row).standard]));
    }
    return {id:member.id,name:member.name,...(department?{department_id:department.department_id}:{}),standard:values.member,keyStandard:values.key,expires:member.expires,allowed_models:storedModelAllowlist(member.allowed_models),usage_limits:await limits.status(member.id,at),usageWindow:window,...(policy??{})};
   },{readOnly:true,level:'REPEATABLE READ'});
  },{signal});
}
