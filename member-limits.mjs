import {exactUsd,referenceModel} from './pricing.mjs';
const hour=3600000,scale=1000000000000n;
const failure=(status,message,code='invalid_member_limits')=>{throw Object.assign(new Error(message),{status,code});};
const modelId=value=>typeof value==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,199}$/.test(value)&&!['codex','claude'].includes(value);
function amount(value){
  if(typeof value!=='string'||!/^\d{1,12}(?:\.\d{1,2})?$/.test(value))failure(400,'上限须为非负USD金额，最多两位小数');
  const [whole,fraction='']=value.split('.');return BigInt(whole)*scale+BigInt(fraction.padEnd(12,'0'));
}
function rules(value){
  if(!Array.isArray(value)||value.length>50)failure(400,'最多配置50条模型限额');
  const seen=new Set();return value.map(row=>{
    if(!row||typeof row!=='object'||Array.isArray(row)||Object.keys(row).some(k=>!['model','hours','limit_usd'].includes(k))||!modelId(row.model)||![24,168].includes(row.hours))failure(400,'模型限额须填写精确模型ID和24/168小时窗口');
    const key=JSON.stringify([row.model,row.hours]);if(seen.has(key))failure(400,'同一模型和窗口不能重复配置');seen.add(key);
    const atoms=amount(row.limit_usd),cents=atoms/(scale/100n);return {model:row.model,hours:row.hours,limit_usd:(cents/100n)+'.'+String(cents%100n).padStart(2,'0')};
  });
}
export function createMemberLimits(store,{clock=Date.now}={}){
  function configuration(memberId){
    const row=store.get('SELECT usage_limits,usage_limits_version FROM members WHERE id=? AND removed_at IS NULL',memberId);if(!row)failure(404,'员工不存在或已移除');
    try{
      const value=JSON.parse(row.usage_limits);
      if(!value||typeof value.enabled!=='boolean'||Object.keys(value).some(k=>!['enabled','rules'].includes(k)))throw Error();
      const normalized=rules(value.rules);if(value.enabled&&!normalized.length)throw Error();
      return {enabled:value.enabled,rules:normalized,version:row.usage_limits_version};
    }catch{failure(409,'员工限额配置无效，请管理员重新设置','member_limits_invalid');}
  }
  function effective(rule,at=clock()){
    const model=referenceModel(rule.model,at,store.pricing.book(store.pricing.current()));
    return {...rule,effective_model:model,invalid_model:model===null};
  }
  function save(memberId,payload){
    if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).some(k=>!['enabled','rules','expected_version'].includes(k))||!Number.isSafeInteger(payload.expected_version)||payload.expected_version<0||payload.enabled===undefined&&payload.rules===undefined)failure(400,'员工限额配置字段无效');
    store.db.exec('BEGIN IMMEDIATE');try{
      const previous=configuration(memberId);if(payload.expected_version!==previous.version)failure(409,'限额设置已变化，请重新打开','member_limits_conflict');
      const next={enabled:payload.enabled===undefined?previous.enabled:payload.enabled,rules:payload.rules===undefined?previous.rules:rules(payload.rules)};
      if(typeof next.enabled!=='boolean'||next.enabled&&!next.rules.length)failure(400,'开启限额后至少配置一条模型规则');
      if(next.enabled||payload.rules!==undefined)for(const rule of next.rules){const target=effective(rule).effective_model;if(target!==rule.model)failure(400,target?'请使用规范模型ID '+target+'，不能把别名保存为限额模型':'请先为模型配置明确费率，再设置限额');}
      store.run('UPDATE members SET usage_limits=?,usage_limits_version=usage_limits_version+1 WHERE id=?',JSON.stringify(next),memberId);
      store.audit('member.limits',JSON.stringify({member:memberId,before:{enabled:previous.enabled,rules:previous.rules},after:next,version:previous.version+1}));
      store.db.exec('COMMIT');return configuration(memberId);
    }catch(error){store.db.exec('ROLLBACK');throw error;}
  }
  function status(memberId,at=clock()){
    const config=configuration(memberId),maximum=Math.max(0,...config.rules.map(r=>r.hours)),models=new Set(config.rules.map(r=>effective(r,at).effective_model||r.model));
    const rows=maximum?store.all('SELECT * FROM requests WHERE member_id=? AND ended>? AND ended<=?',memberId,at-maximum*hour,at):[];
    const current=store.pricing.book(store.pricing.current());
    const measured=rows.map(row=>{
      const book=row.price_version?store.pricing.book(row.price_version):null,chosen=row.billing_model||row.model;
      const model=referenceModel(chosen,row.started,book)||((models.has(chosen)||Object.hasOwn(current.rates,chosen))?chosen:null);
      const zero=row.status!=='reconciled'&&!row.usage_unknown&&row.input===0&&row.output===0&&row.cached===0&&row.manual_cost_atoms==null;
      const quoted=book?store.pricing.quoteAt(row):null;
      return {ended:row.ended,model,atoms:zero?0n:typeof row.manual_cost_atoms==='string'&&/^\d+$/.test(row.manual_cost_atoms)?BigInt(row.manual_cost_atoms):quoted?.totalAtoms??null};
    });
    const pending=maximum?store.all("SELECT model,billing_model,started,price_version FROM requests WHERE member_id=? AND ended IS NULL AND status IN ('queued','running')",memberId):[];
    const usage=config.rules.map(saved=>{
      const rule=effective(saved,at),model=rule.effective_model||rule.model;
      let used=0n,unresolved=0;
      for(const row of measured){if(row.ended<=at-rule.hours*hour||row.model!==model&&row.model!==null)continue;if(row.model===null){if(row.atoms!==0n)unresolved++;continue;}if(row.atoms===null)unresolved++;else used+=row.atoms;}
      const ceiling=amount(rule.limit_usd),exceeded=used>=ceiling;
      const inFlight=pending.filter(row=>(referenceModel(row.billing_model||row.model,row.started,store.pricing.book(row.price_version))||row.billing_model||row.model)===model).length;
      return {...rule,used_usd:exactUsd(used),remaining_usd:exactUsd(used>=ceiling?0n:ceiling-used),unresolved,in_flight:inFlight,exceeded,blocked:config.enabled&&(exceeded||unresolved>0||rule.invalid_model),window:{start:at-rule.hours*hour,end:at}};
    });
    return {...config,currency:'USD',observed_at:at,usage};
  }
  function check(memberId,requestedModel,{requestId,tools}={}){
    const config=configuration(memberId);if(!config.enabled)return;
    const at=clock();if(config.rules.some(rule=>effective(rule,at).invalid_model))failure(409,'存在无法确认模型身份的限额规则，请管理员修正或关闭限额','member_limits_invalid');
    const request=requestId?store.get('SELECT started,price_version FROM requests WHERE id=? AND member_id=?',requestId,memberId):null;
    const book=store.pricing.book(request?.price_version||store.pricing.current());
    const root=referenceModel(requestedModel,request?.started??at,book);
    if(!root)failure(409,'限额开启时须使用有明确费率或映射的模型，请管理员核对模型','member_limit_model_unknown');
    const requested=new Set([root]),stack=Array.isArray(tools)?[tools]:[];
    while(stack.length){for(const tool of stack.pop()){
      if(!tool||typeof tool!=='object')continue;
      if(tool.type==='image_generation')failure(403,'限额开启时不支持隐式图片模型工具，请使用明确模型的图片接口','member_limit_model_unknown');
      if(tool.model!==undefined){const model=referenceModel(tool.model,request?.started??at,book);if(!model)failure(409,'工具模型无法核对限额','member_limit_model_unknown');requested.add(model);}
      if(Array.isArray(tool.tools))stack.push(tool.tools);
    }}
    if(!config.rules.some(rule=>requested.has(effective(rule,at).effective_model)))return;
    for(const row of status(memberId,at).usage){
      if(!requested.has(row.effective_model))continue;
      if(row.exceeded)failure(429,row.model+'已达到近'+row.hours+'小时参考USD上限'+row.limit_usd+'，请等待用量滚出窗口或联系管理员','member_model_limit_exceeded');
      if(row.unresolved)failure(409,row.model+'窗口内存在无法计价的记录，请管理员核对后重试','member_model_usage_unknown');
    }
  }
  return {configuration,save,status,check};
}
