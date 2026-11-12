import {usageWindow,storedModelAllowlist} from './store.mjs';
import {fields,validPrice,joins,aggregate,aggregateSql} from './postgres-reference-query.mjs';
import {createPostgresMemberLimits} from './postgres-member-limits.mjs';
import {createPostgresConsumptionPolicy,verifyConsumptionPolicyAvailability} from './postgres-consumption-policy.mjs';

export function createPostgresSelfQuery(store,{queryQueue,clock=Date.now}={}){
  const limits=createPostgresMemberLimits(store,{clock}),consumption=createPostgresConsumptionPolicy(store,{clock});let consumptionInitialization;
  return secret=>queryQueue.run(async()=>{
   const initialized=await (consumptionInitialization??=verifyConsumptionPolicyAvailability(store).then(available=>({available}),error=>{consumptionInitialization=undefined;throw error;}));
   return store.transaction(async()=>{
    const at=clock(),member=await store.authenticate(secret,{at}),window=usageWindow(at);
    const policy=initialized.available?await consumption.read(member.key_id,{memberId:member.id}):null;
    const rows=await store.all(`WITH data AS MATERIALIZED (
      SELECT r.key_id,r.started,r.ended,r.status,
        CASE WHEN ${validPrice} THEN p.state ELSE 'unpriced' END price_state,
        CASE WHEN ${validPrice} THEN p.revision ELSE NULL END revision,
        ${fields.map(f=>`CASE WHEN ${validPrice} THEN p.${f}_atoms ELSE NULL END ${f}`).join(',')}
      FROM harbor.requests r${joins} WHERE r.member_id=$1 AND r.usage_at>=$2 AND r.usage_at<$3
    ) SELECT 'member' scope,${aggregateSql} FROM data UNION ALL SELECT 'key' scope,${aggregateSql} FROM data WHERE key_id=$4`,member.id,window.start,window.end,member.key_id);
    const values=Object.fromEntries(rows.map(row=>[row.scope,aggregate(row).standard]));
    return {id:member.id,name:member.name,standard:values.member,keyStandard:values.key,expires:member.expires,allowed_models:storedModelAllowlist(member.allowed_models),usage_limits:await limits.status(member.id,at),usageWindow:window,...(policy??{})};
   },{readOnly:true,level:'REPEATABLE READ'});
  });
}
