import {randomUUID} from 'node:crypto';
import {AppError} from './store.mjs';
import {exactUsd,referenceModel} from './pricing.mjs';
import {memberLimitAtoms,normalizeMemberLimitRules} from './member-limits.mjs';

const hour=3600000;
const fail=(status,message,code='invalid_member_limits')=>{throw new AppError(status,message,code);};

export function postgresMemberLimitConfiguration(row){
    try{
      const value=JSON.parse(row.usage_limits);
      if(!value||typeof value.enabled!=='boolean'||Object.keys(value).some(k=>!['enabled','rules'].includes(k)))throw Error();
      const rules=normalizeMemberLimitRules(value.rules);if(value.enabled&&!rules.length)throw Error();
      return {enabled:value.enabled,rules,version:row.usage_limits_version};
    }catch{fail(409,'员工限额配置无效，请管理员重新设置','member_limits_invalid');}
}

// Amounts are exact NUMERIC/BigInt. Only one aggregate per configured rule
// crosses the database boundary; completed request histories stay in SQL.
export function createPostgresMemberLimits(store,{clock=Date.now}={}){
  async function configuration(memberId,{lock=false}={}){
    const row=await store.get('SELECT usage_limits,usage_limits_version FROM harbor.members WHERE id=$1 AND removed_at IS NULL'+(lock?' FOR UPDATE':''),memberId);
    if(!row)fail(404,'员工不存在或已移除');
    return postgresMemberLimitConfiguration(row);
  }
  async function currentBook(){
    const row=await store.get("SELECT p.book FROM harbor.gateway_settings s JOIN harbor.pricing_versions p ON p.id=(s.value::jsonb->>'active') WHERE s.name='pricing.state'");
    if(!row)fail(503,'计价版本尚未准备好','pricing_unavailable');return JSON.parse(row.book);
  }
  async function validate(value){
    if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(key=>!['enabled','rules'].includes(key))||typeof value.enabled!=='boolean')fail(400,'员工限额须明确选择开启或不限额');
    const next={enabled:value.enabled,rules:normalizeMemberLimitRules(value.rules)};if(next.enabled&&!next.rules.length)fail(400,'开启限额后至少配置一条模型规则');
    if(next.rules.length){const book=await currentBook(),at=clock();for(const rule of next.rules){const target=referenceModel(rule.model,at,book);if(target!==rule.model)fail(400,target?'请使用规范模型ID '+target+'，不能把别名保存为限额模型':'请先为模型配置明确费率，再设置限额');}}
    return next;
  }
  const effective=(rule,at,book)=>{const model=referenceModel(rule.model,at,book);return {...rule,effective_model:model,invalid_model:model===null};};
  async function save(memberId,payload,{source={kind:'custom'}}={}){
    if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).some(k=>!['enabled','rules','expected_version'].includes(k))||!Number.isSafeInteger(payload.expected_version)||payload.expected_version<0||payload.enabled===undefined&&payload.rules===undefined)fail(400,'员工限额配置字段无效');
    return store.transaction(async()=>{
      const previous=await configuration(memberId,{lock:true});
      if(payload.expected_version!==previous.version)fail(409,'限额设置已变化，请重新打开','member_limits_conflict');
      const next={enabled:payload.enabled===undefined?previous.enabled:payload.enabled,rules:payload.rules===undefined?previous.rules:normalizeMemberLimitRules(payload.rules)};
      if(typeof next.enabled!=='boolean'||next.enabled&&!next.rules.length)fail(400,'开启限额后至少配置一条模型规则');
      if(next.enabled||payload.rules!==undefined){const book=await currentBook(),at=clock();for(const rule of next.rules){const target=effective(rule,at,book).effective_model;if(target!==rule.model)fail(400,target?'请使用规范模型ID '+target+'，不能把别名保存为限额模型':'请先为模型配置明确费率，再设置限额');}}
      await store.run("UPDATE harbor.members SET usage_limits=$1,usage_limits_version=usage_limits_version+1,configuration_source=jsonb_set(configuration_source,'{limits}',$3::jsonb),defaults_version=defaults_version+1 WHERE id=$2",JSON.stringify(next),memberId,JSON.stringify(source));
      await store.run('INSERT INTO harbor.audit(id,at,action,detail) VALUES($1,$2,$3,$4)',randomUUID(),clock(),'member.limits',JSON.stringify({member:memberId,before:{enabled:previous.enabled,rules:previous.rules},after:next,version:previous.version+1}));
      return {...next,version:previous.version+1};
    });
  }
  async function calculate(memberId,at,config,book){
    const targets=config.rules.map(rule=>effective(rule,at,book));
    if(!targets.length)return {...config,currency:'USD',observed_at:at,usage:[]};
    const maximum=Math.max(...targets.map(rule=>rule.hours));
    const rows=await store.all(`WITH rules AS (
      SELECT * FROM jsonb_to_recordset($4::jsonb) AS x(position INTEGER,model TEXT,hours INTEGER)
    ), source AS MATERIALIZED (
      SELECT r.ended,r.started,r.status,r.usage_unknown,r.input,r.output,r.cached,r.manual_cost_atoms,r.price_version,
        COALESCE(r.billing_model,r.model) chosen,p.total_atoms,p.revision,p.request_id projected
      FROM harbor.requests r LEFT JOIN harbor.request_prices p ON p.request_id=r.id
      WHERE r.member_id=$1 AND ((r.ended>$2 AND r.ended<=$3) OR (r.ended IS NULL AND r.status IN ('queued','running')))
    ), books AS MATERIALIZED (
      SELECT id,book::jsonb book FROM harbor.pricing_versions WHERE id IN (SELECT DISTINCT price_version FROM source)
    ), measured AS MATERIALIZED (
      SELECT s.ended,s.status,CASE WHEN b.book->'rates' ? s.chosen THEN s.chosen
        WHEN a.target IS NOT NULL THEN a.target
        WHEN s.ended IS NULL THEN s.chosen
        WHEN $5::jsonb->'rates' ? s.chosen OR EXISTS(SELECT 1 FROM rules WHERE model=s.chosen) THEN s.chosen
        ELSE NULL END model,
      CASE WHEN s.ended IS NULL THEN NULL
        WHEN s.status<>'reconciled' AND s.usage_unknown=0 AND s.input=0 AND s.output=0 AND s.cached=0 AND s.manual_cost_atoms IS NULL THEN 0::numeric
        WHEN s.manual_cost_atoms ~ '^[0-9]+$' THEN s.manual_cost_atoms::numeric
        WHEN b.book IS NOT NULL AND s.projected IS NOT NULL AND s.revision=s.price_version THEN s.total_atoms
        ELSE NULL END atoms
      FROM source s LEFT JOIN books b ON b.id=s.price_version LEFT JOIN LATERAL (
        SELECT alias->>'target' target FROM jsonb_array_elements(COALESCE(b.book->'aliases','[]'::jsonb)) alias
        WHERE alias->>'name'=s.chosen AND s.started>=(alias->>'from')::bigint
          AND (alias->>'to' IS NULL OR s.started<(alias->>'to')::bigint)
          AND b.book->'rates' ? (alias->>'target') LIMIT 1
      ) a ON TRUE
    ) SELECT x.position,
      COALESCE(SUM(m.atoms) FILTER(WHERE m.ended>$3-x.hours::bigint*3600000 AND m.model=x.model),0)::text used,
      COUNT(*) FILTER(WHERE m.ended>$3-x.hours::bigint*3600000 AND
        ((m.model=x.model AND m.atoms IS NULL) OR (m.model IS NULL AND m.atoms IS DISTINCT FROM 0::numeric))) unresolved,
      COUNT(*) FILTER(WHERE m.ended IS NULL AND m.model=x.model AND m.status IN ('queued','running')) in_flight
      FROM rules x LEFT JOIN measured m ON (m.model=x.model OR m.model IS NULL)
      GROUP BY x.position ORDER BY x.position`,memberId,at-maximum*hour,at,
      JSON.stringify(targets.map((r,position)=>({position,model:r.effective_model||r.model,hours:r.hours}))),JSON.stringify(book));
    const usage=targets.map((rule,i)=>{
      const row=rows[i],used=BigInt(row.used),ceiling=memberLimitAtoms(rule.limit_usd),exceeded=used>=ceiling;
      return {...rule,used_usd:exactUsd(used),remaining_usd:exactUsd(used>=ceiling?0n:ceiling-used),unresolved:row.unresolved,in_flight:row.in_flight,exceeded,blocked:config.enabled&&(exceeded||row.unresolved>0||rule.invalid_model),window:{start:at-rule.hours*hour,end:at}};
    });
    return {...config,currency:'USD',observed_at:at,usage};
  }
  async function status(memberId,at=clock()){
    return store.transaction(async()=>calculate(memberId,at,await configuration(memberId),await currentBook()),{level:'REPEATABLE READ',readOnly:true});
  }
  async function check(memberId,requestedModel,{requestId,tools,at=clock()}={}){
    // May run inside the short admission transaction; no locks survive dispatch.
    return store.transaction(async()=>{
      const config=await configuration(memberId);if(!config.enabled)return;
      const current=await currentBook();
      if(config.rules.some(rule=>effective(rule,at,current).invalid_model))fail(409,'存在无法确认模型身份的限额规则，请管理员修正或关闭限额','member_limits_invalid');
      const request=requestId?await store.get('SELECT started,price_version FROM harbor.requests WHERE id=$1 AND member_id=$2',requestId,memberId):null;
      const book=request?.price_version?await store.pricingBook(request.price_version):current;
      const root=referenceModel(requestedModel,request?.started??at,book);
      if(!root)fail(409,'限额开启时须使用有明确费率或映射的模型，请管理员核对模型','member_limit_model_unknown');
      const requested=new Set([root]),stack=Array.isArray(tools)?[tools]:[];
      while(stack.length){for(const tool of stack.pop()){
        if(!tool||typeof tool!=='object')continue;
        if(tool.type==='image_generation')fail(403,'限额开启时不支持隐式图片模型工具，请使用明确模型的图片接口','member_limit_model_unknown');
        if(tool.model!==undefined){const model=referenceModel(tool.model,request?.started??at,book);if(!model)fail(409,'工具模型无法核对限额','member_limit_model_unknown');requested.add(model);}
        if(Array.isArray(tool.tools))stack.push(tool.tools);
      }}
      if(!config.rules.some(rule=>requested.has(effective(rule,at,current).effective_model)))return;
      for(const row of (await calculate(memberId,at,config,current)).usage){
        if(!requested.has(row.effective_model))continue;
        if(row.exceeded)fail(429,row.model+'已达到近'+row.hours+'小时参考USD上限'+row.limit_usd+'，请等待用量滚出窗口或联系管理员','member_model_limit_exceeded');
        if(row.unresolved)fail(409,row.model+'窗口内存在无法计价的记录，请管理员核对后重试','member_model_usage_unknown');
      }
    },{level:'REPEATABLE READ',readOnly:true});
  }
  return {configuration,validate,save,status,check};
}
