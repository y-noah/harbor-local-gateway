import {usageWindow} from '../../store.mjs';
export function capacityLiveWindow(at=Date.now()){const window=usageWindow(at);return {...window,metadata30dStart:window.end-30*86400000};}
export function summarizeCapacityCoverage(samples,{profile='selfcheck'}={}){
  const valid=samples.filter(row=>row.verified!==false&&Number.isSafeInteger(row.metadata7d)&&Number.isSafeInteger(row.metadata30d)),unknown=samples.filter(row=>!valid.includes(row)),revisions=[...new Set(valid.map(row=>row.pricing_revision).filter(Boolean))],minimum=field=>valid.length?Math.min(...valid.map(row=>row[field])):null;
  const belowScale=valid.some(row=>row.metadata7d<1260000||row.metadata30d<5400000),range=rows=>rows.length?{from:rows[0].at,to:rows.at(-1).at}:null;
  return {passed:profile==='formal'&&(belowScale||revisions.length>1)?false:null,samples:samples.length,initialWindow:samples[0]?.window??null,finalWindow:samples.at(-1)?.window??null,minimumMetadata7d:unknown.length?null:minimum('metadata7d'),minimumMetadata30d:unknown.length?null:minimum('metadata30d'),observedMinimumMetadata7d:minimum('metadata7d'),observedMinimumMetadata30d:minimum('metadata30d'),validSamples:valid.length,validSampleRange:range(valid),unknownSamples:unknown.length,unknownSampleRange:range(unknown),pricingRevisions:revisions,scope:'observed_live_page_windows',missing:['未证明每个正式样本窗口的持续数据覆盖及索引版本；小资格不证明正式容量']};
}
