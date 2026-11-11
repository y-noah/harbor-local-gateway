import {AppError,id} from './store.mjs';

// Subscription price is descriptive metadata, never a token-capacity estimate.
export function subscriptionMonthlyUsd(value){
 if(value===null)return null;
 if(typeof value!=='number'||!Number.isFinite(value)||value<=0||!Number.isSafeInteger(Math.round(value*100))||Number(value.toFixed(2))!==value)throw new AppError(400,'订阅月费须为大于0、最多两位小数的美元金额','subscription_price_invalid');
 return value;
}
export function createPostgresSubscriptionPrices(store,{clock=Date.now}={}){
 const setting=accountId=>'subscription_monthly_usd:'+accountId;
 async function account(accountId){
  const row=await store.get('SELECT kind,auth_mode FROM harbor.accounts WHERE id=$1 AND removed_at IS NULL',accountId);
  if(!row)throw new AppError(404,'账号不存在或已移除');
  if(row.kind!=='codex'||row.auth_mode!=='subscription')throw new AppError(400,'仅Codex官方订阅可设置订阅月费','subscription_price_not_applicable');
 }
 async function read(accountId){
  await account(accountId);const row=await store.get('SELECT value FROM harbor.gateway_settings WHERE name=$1',setting(accountId));
  return row?JSON.parse(row.value):{monthly_usd:null,version:0};
 }
 async function save(accountId,payload){
  if(!payload||typeof payload!=='object'||Array.isArray(payload)||Object.keys(payload).some(key=>!['monthly_usd','expected_version'].includes(key))||!Number.isSafeInteger(payload.expected_version)||payload.expected_version<0)throw new AppError(400,'订阅月费配置无效');
  const amount=subscriptionMonthlyUsd(payload.monthly_usd);
  return store.transaction(async()=>{
   await store.get('SELECT id FROM harbor.accounts WHERE id=$1 FOR UPDATE',accountId);
   const previous=await read(accountId);if(previous.version!==payload.expected_version)throw new AppError(409,'订阅月费已变化，请重新读取','subscription_price_conflict');
   const next={monthly_usd:amount,version:previous.version+1};
   await store.run('INSERT INTO harbor.gateway_settings(name,value) VALUES($1,$2) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value',setting(accountId),JSON.stringify(next));
   await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',id(),clock(),'account.subscription.price',JSON.stringify({actor:'admin',account:accountId,previous,next}));
   return next;
  });
 }
 return {read,save};
}
