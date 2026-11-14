import {AppError} from './store.mjs';

const minute=60000,day=86400000,offset=8*60*minute;
export const defaultTransferTimes=Object.freeze(['02:00','10:00','13:00','16:00','20:00']);
const invalid=()=>{throw new AppError(400,'定时转移须配置北京时间HH:mm时点、有效版本及生效时间','transfer_schedule_invalid');};
export function transferSchedule(value={}){
  if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>!['times','timezone','version','effective'].includes(k)))invalid();
  const times=Object.hasOwn(value,'times')?value.times:defaultTransferTimes,timezone=Object.hasOwn(value,'timezone')?value.timezone:'Asia/Shanghai',version=Object.hasOwn(value,'version')?value.version:0,effective=Object.hasOwn(value,'effective')?value.effective:0;
  if(timezone!=='Asia/Shanghai'||!Array.isArray(times)||!times.length||times.length>1440||times.some(time=>typeof time!=='string'||!/^([01]\d|2[0-3]):[0-5]\d$/.test(time))||!Number.isSafeInteger(version)||version<0||!Number.isSafeInteger(effective)||effective<0)invalid();
  return {times:[...new Set(times)].sort(),timezone,version,effective};
}
export function transferWindow(at,value={}){
  if(!Number.isSafeInteger(at)||at<0)invalid();
  const schedule=transferSchedule(value),midnight=Math.floor((at+offset)/day)*day-offset;
  const points=schedule.times.map(time=>({time,ms:(Number(time.slice(0,2))*60+Number(time.slice(3)))*minute}));
  const candidates=[-1,0,1].flatMap(n=>points.map(point=>({...point,at:midnight+n*day+point.ms}))).sort((a,b)=>a.at-b.at);
  const previous=candidates.filter(point=>point.at<=at).at(-1),next=candidates.find(point=>point.at>at),date=new Date(previous.at+offset).toISOString().slice(0,10);
  // Keep old 02:00 batch identities intact. Other slots have explicit suffixes;
  // ordering always uses timestamps, never these compatibility identifiers.
  const key=previous.time==='02:00'?date:date+'-'+previous.time.replace(':','');
  return {key,start:previous.at-16*60*minute,end:previous.at,until:next.at,time:previous.time,timezone:schedule.timezone,due:previous.at>schedule.effective,next:next.at};
}

export const nextTransferReset=(at,settings={},schedule={})=>settings.nightly===true?transferWindow(at,schedule).next:null;
