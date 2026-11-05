import {usageWindow,storedModelAllowlist} from './store.mjs';
import {fields,validPrice,joins,aggregate,aggregateSql} from './postgres-reference-query.mjs';
import {createPostgresMemberLimits} from './postgres-member-limits.mjs';

export function createPostgresSelfQuery(store,{queryQueue,clock=Date.now}={}){
  const limits=createPostgresMemberLimits(store,{clock});
  return secret=>queryQueue.run(()=>store.transaction(async()=>{
    const at=clock(),member=await store.authenticate(secret,{at}),window=usageWindow(at);
    const rows=await store.all(`WITH data AS MATERIALIZED (
      SELECT r.key_id,r.started,r.ended,r.status,
        CASE WHEN ${validPrice} THEN p.state ELSE 'unpriced' END price_state,
        CASE WHEN ${validPrice} THEN p.revision ELSE NULL END revision,
        ${fields.map(f=>`CASE WHEN ${validPrice} THEN p.${f}_atoms ELSE NULL END ${f}`).join(',')}
      FROM harbor.requests r${joins} WHERE r.member_id=$1 AND r.usage_at>=$2 AND r.usage_at<$3
    ) SELECT 'member' scope,${aggregateSql} FROM data UNION ALL SELECT 'key' scope,${aggregateSql} FROM data WHERE key_id=$4`,member.id,window.start,window.end,member.key_id);
    const values=Object.fromEntries(rows.map(row=>[row.scope,aggregate(row).standard]));
    return {id:member.id,name:member.name,standard:values.member,keyStandard:values.key,expires:member.expires,allowed_models:storedModelAllowlist(member.allowed_models),usage_limits:await limits.status(member.id,at),usageWindow:window};
  },{readOnly:true,level:'REPEATABLE READ'}));
}
