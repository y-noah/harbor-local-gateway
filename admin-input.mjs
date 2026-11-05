import {AppError} from './store.mjs';

export function integer(value,label,{nullable=false,min=0,max=1e12}={}) {
  if(nullable && (value===null || value===''))return null;
  if(!Number.isSafeInteger(value)||value<min||value>max)throw new AppError(400,label+' 必须是有效整数');return value;
}
export function name(value){if(typeof value!=='string'||!value.trim()||value.trim().length>60)throw new AppError(400,'名称需为 1–60 字');return value.trim();}
export function expiry(value){return integer(value,'到期时间',{nullable:true,max:9e15});}
export function bool(value){if(typeof value!=='boolean')throw new AppError(400,'状态无效');return value?1:0;}

export function apiKeyValue(value){if(typeof value!=='string'||!value.trim()||value.length>4096||/\s/.test(value.trim()))throw new AppError(400,'请填写有效的官方API Key');return value.trim();}
export function apiWeightValue(value,other){
  if(!Number.isFinite(value)||value<0||value>100||Math.abs(value*100-Math.round(value*100))>1e-8)throw new AppError(400,'API分配占比须为0至100，最多两位小数');
  if(other+value>100+1e-8)throw new AppError(400,'所有API账号的人工占比总和不能超过100%');return value;
}
