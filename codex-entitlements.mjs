import {createHash} from 'node:crypto';
import {sanitizeRecord} from './record-redaction.mjs';

const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const bool=value=>typeof value==='boolean'?value:null;
const integer=value=>Number.isSafeInteger(value)&&value>=0;
const timestamp=value=>integer(value)&&Number.isSafeInteger(value*1000)?value*1000:null;
const present=(value,key)=>Object.prototype.hasOwnProperty.call(value,key);
const text=(value,maxBytes)=>typeof value==='string'?sanitizeRecord(value,{maxBytes}):null;
const statuses=new Set(['available','redeeming','redeemed','unknown']);
const workspaceRefusals=new Set(['workspace_owner_credits_depleted','workspace_member_credits_depleted','workspace_owner_usage_limit_reached','workspace_member_usage_limit_reached']);

// A sparse/unknown read cannot clear a previous explicit denial. The latches
// are server-owned snapshot facts, separate from the latest nullable fields.
export function retainCodexQuotaDenials(previous,incoming){
  const prior=object(previous)?previous:{},next=object(incoming)?incoming:{};
  const old=object(prior.officialDenial)?prior.officialDenial:{};
  const ordinary=next.ordinaryUsageAllowed===false||next.limitReached===true||next.ordinaryUsageAllowed!==true&&(old.ordinary===true||prior.ordinaryUsageAllowed===false||prior.limitReached===true);
  const spendControl=next.spendControlReached===true||next.spendControlReached!==false&&(old.spend_control===true||prior.spendControlReached===true);
  const explicitWorkspace=workspaceRefusals.has(next.limitReachedType);
  const workspace=explicitWorkspace||!(next.ordinaryUsageAllowed===true&&next.spendControlReached!==true)&&(old.workspace===true||workspaceRefusals.has(prior.limitReachedType));
  return {...next,officialDenial:{ordinary,spend_control:spendControl,workspace}};
}

export function codexQuotaDenials(snapshot){return retainCodexQuotaDenials(snapshot,{}).officialDenial;}

// The pinned app-server protocol exposes neither credit units/scales nor model
// scope. Preserve its observations for display, never mint dispatch permission
// or convert the balance into USD/atoms. No redemption endpoint is called here.
export function normalizeCodexEntitlements(value,rate,at){
  if(!object(value)||!Number.isSafeInteger(at)||at<0)return null;
  const rawCredits=object(rate?.credits)?rate.credits:null;
  const summary=object(value.rateLimitResetCredits)?value.rateLimitResetCredits:null;
  if(!rawCredits&&!summary&&!present(value,'ordinaryUsageAllowed'))return null;
  let credits=null,resetCards=null;
  if(rawCredits){
    const balance=typeof rawCredits.balance==='string'&&/^-?(?:0|[1-9][0-9]{0,79})(?:\.[0-9]{1,80})?$/.test(rawCredits.balance)?rawCredits.balance:null;
    credits={has_credits:bool(rawCredits.hasCredits),unlimited:bool(rawCredits.unlimited),balance,unit:null,accounting_scale:null,model_scope:null,protocol_scope:null,dispatch_permission_verified:false};
  }
  if(summary){
    const remaining=integer(summary.availableCount)?summary.availableCount:null;
    const rows=Array.isArray(summary.credits)?summary.credits:null,cards=[],refs=new Set();
    let incomplete=false;
    for(const row of rows?.slice(0,128)||[]){
      if(!object(row)||typeof row.id!=='string'||!row.id||Buffer.byteLength(row.id)>2048){incomplete=true;continue;}
      const ref=createHash('sha256').update('harbor-reset-credit\0').update(row.id).digest('hex');
      if(refs.has(ref)){incomplete=true;continue;}refs.add(ref);
      const grantedAt=timestamp(row.grantedAt),expiryKnown=present(row,'expiresAt')&&(row.expiresAt===null||timestamp(row.expiresAt)!==null);
      if(grantedAt===null||!statuses.has(row.status)||!['codexRateLimits','unknown'].includes(row.resetType))incomplete=true;
      cards.push({ref,status:statuses.has(row.status)?row.status:'unknown',type:row.resetType==='codexRateLimits'?'codexRateLimits':'unknown',granted_at:grantedAt,expires_at:timestamp(row.expiresAt),expiry_known:expiryKnown,title:text(row.title,256),description:text(row.description,1024),windows:null});
    }
    // The backend may truncate details. Count remains its independent reported
    // total; an unavailable list must not be replaced with an empty list/zero.
    const detailsState=rows===null?'unknown':incomplete||rows.length>128||remaining===null||cards.length!==remaining?'partial':'complete';
    resetCards={remaining,details_state:detailsState,cards:rows===null?null:cards};
  }
  return {source:'official',checked:at,ordinary_usage_allowed:bool(value.ordinaryUsageAllowed),credits,reset_cards:resetCards,dispatch_permission_verified:false};
}
