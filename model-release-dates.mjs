import {createHash} from 'node:crypto';

export const modelReleaseSource='https://developers.openai.com/api/docs/changelog.md';
export const modelReleaseSetting='model.release_dates';
const sourcePage='https://developers.openai.com/api/docs/changelog';
// Public release announcements verified against the official changelog. This
// registry is independent of fee books and is extended by controlled refreshes.
const initialDates={
  'gpt-6-astra':'2026-09-03',
  'gpt-6.1-sol':'2026-09-29',
  'gpt-6.1-terra':'2026-10-26',
};
const modelId=value=>typeof value==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,199}$/.test(value);
const months=['January','February','March','April','May','June','July','August','September','October','November','December'];
const maxBytes=3000000,maxModels=10000;
function validDate(value,at){
  if(typeof value!=='string'||!/^20\d{2}-\d{2}-\d{2}$/.test(value))return false;
  const parsed=Date.parse(value+'T00:00:00.000Z');
  return Number.isFinite(parsed)&&new Date(parsed).toISOString().slice(0,10)===value&&value<=new Date(at).toISOString().slice(0,10);
}
function entry(date){return {date,source:sourcePage};}
export function modelReleaseRegistry(saved,at=Date.now()){
  const dates=Object.fromEntries(Object.entries(initialDates).filter(([,date])=>validDate(date,at)).map(([model,date])=>[model,entry(date)]));
  if(saved?.dates&&typeof saved.dates==='object'&&!Array.isArray(saved.dates)){
    const rows=Object.entries(saved.dates);if(rows.length>maxModels)throw Error('model release registry too large');
    for(const [model,value] of rows)if(modelId(model)&&validDate(value?.date,at)&&value.source===sourcePage)dates[model]=entry(value.date);
  }
  return dates;
}
export function modelReleaseMetadata(model,saved,at=Date.now()){
  const value=modelReleaseRegistry(saved,at)[model];
  return {release_date:value?.date??null,release_date_source:value?.source??null};
}
export function parseModelReleaseDates(text,{at=Date.now()}={}){
  if(typeof text!=='string'||Buffer.byteLength(text)>maxBytes||!/^# Changelog\s*$/m.test(text))throw Error('official changelog format invalid');
  const dates={};let year=null,month=null,date=null,block=[];
  const consume=()=>{
    if(!date||!validDate(date,at))return;
    const body=block.join('\n'),declared=new Set([...body.matchAll(/(?:^| · )Model: ([a-zA-Z0-9][a-zA-Z0-9._-]{0,199})(?= · |\s*$)/gm)].map(match=>match[1]));
    for(const paragraph of body.split(/\n\s*\n/)){
      // Only models in the announcement's grammatical subject are releases.
      // A later link to an existing model or a new service tier is not one.
      const subject=paragraph.trim().match(/^(?:Released|Introducing|Introduced|Launched)\s+(.+?)(?=,\s+(?:a|an|the|our|bringing|which|with)\b|\s+(?:for|is|are|to|with)\s|$)/i)?.[1];
      if(!subject)continue;
      for(const match of subject.matchAll(/\[[^\]\n]+\]\(https:\/\/developers\.openai\.com\/api\/docs\/models\/([a-zA-Z0-9][a-zA-Z0-9._-]{0,199})\)/g)){
        const model=match[1];if(!declared.has(model))continue;
        if(!Object.hasOwn(dates,model)||date<dates[model].date)dates[model]=entry(date);
        if(Object.keys(dates).length>maxModels)throw Error('official changelog model limit');
      }
    }
  };
  for(const line of text.split(/\r?\n/)){
    const section=line.match(/^## ([A-Za-z]+), (20\d{2})\s*$/),day=line.match(/^### ([A-Za-z]{3}) (\d{1,2})\s*$/);
    if(section){consume();block=[];year=Number(section[2]);month=months.indexOf(section[1])+1;date=null;}
    else if(line.startsWith('## ')){consume();block=[];year=null;month=null;date=null;}
    else if(day){consume();block=[];date=year&&month&&months[month-1].slice(0,3)===day[1]?`${year}-${String(month).padStart(2,'0')}-${day[2].padStart(2,'0')}`:null;}
    else if(line.startsWith('### ')){consume();block=[];date=null;}
    else block.push(line);
  }
  consume();if(!Object.keys(dates).length)throw Error('official changelog has no verified releases');return dates;
}
export async function fetchModelReleaseDates({fetchImpl=fetch,signal=new AbortController().signal,at=Date.now()}={}){
  const bounded=AbortSignal.any([signal,AbortSignal.timeout(15000)]);bounded.throwIfAborted();
  const response=await fetchImpl(modelReleaseSource,{headers:{Accept:'text/markdown'},redirect:'error',signal:bounded});
  if(!response.ok||response.url&&response.url!==modelReleaseSource||!response.body||/^text\/html\b/i.test(response.headers.get('content-type')||'')||Number(response.headers.get('content-length'))>maxBytes){await response.body?.cancel().catch(()=>{});throw Error('official changelog unavailable');}
  const reader=response.body.getReader(),chunks=[];let size=0;
  const cancel=()=>{void reader.cancel(bounded.reason).catch(()=>{});};bounded.addEventListener('abort',cancel,{once:true});
  try{
    for(;;){bounded.throwIfAborted();const {done,value}=await reader.read();bounded.throwIfAborted();if(done)break;size+=value.length;if(size>maxBytes)throw Error('official changelog too large');chunks.push(value);}
    const text=Buffer.concat(chunks).toString('utf8');return {dates:parseModelReleaseDates(text,{at}),sourceHash:createHash('sha256').update(text).digest('hex')};
  }catch(error){await reader.cancel().catch(()=>{});throw error;}finally{bounded.removeEventListener('abort',cancel);reader.releaseLock();}
}
export function mergeModelReleaseDates(previous,{dates,sourceHash},at){
  const trusted=modelReleaseRegistry(previous,at),incoming=modelReleaseRegistry({dates},at),merged={...trusted};
  for(const [model,value] of Object.entries(incoming))if(!Object.hasOwn(merged,model)||value.date<merged[model].date)merged[model]=value;
  return {dates:merged,checkedAt:at,lastSuccess:at,sourceHash,error:null};
}
