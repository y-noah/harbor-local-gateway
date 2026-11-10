import {datasetManifest} from './dataset.mjs';
import {capacityLiveWindow} from './coverage-adapter.mjs';
import {capacityRunLifetime} from './certificate-lifetime.mjs';
import {targets} from './scenarios.mjs';
import {capacitySeedWindowCounts} from './dataset-time.mjs';
export {capacitySeedWindowCounts} from './dataset-time.mjs';

export function capacityTargetDatasetManifest({profile='formal',purpose='run',runLifetimeMs,anchor=Date.now()}={}){
 const initial=datasetManifest({profile,anchor});
 // Existing diagnostic/seed/verify paths retain their declared corpus. The
 // formal owner separately requires a lifetime before any real run starts.
 if(profile!=='formal'||purpose!=='run'||runLifetimeMs===undefined)return initial;
 const lifetime=capacityRunLifetime(runLifetimeMs,{required:true}),until=anchor+lifetime;
 if(!Number.isSafeInteger(until)||capacityLiveWindow(until).start>=anchor)throw Error('capacity_dataset_coverage_horizon_unavailable');
 const make=(recent,total)=>datasetManifest({profile,anchor,metadata7d:recent,metadata30d:total,bodyRequests:total});
 const enoughRecent=recent=>capacitySeedWindowCounts(make(recent,Math.max(recent,initial.metadata30d)),until).metadata7d>=targets.metadata7d;
 const minimum=(start,enough)=>{
  let high=start;
  while(!enough(high)){if(high>Number.MAX_SAFE_INTEGER/2)throw Error('capacity_dataset_coverage_size_unavailable');high*=2;}
  let low=start;
  while(low<high){const middle=low+Math.floor((high-low)/2);if(enough(middle))high=middle;else low=middle+1;}
  // Preserve complete cycles of the existing 100-row body-size distribution.
  const result=Math.ceil(low/100)*100;if(!Number.isSafeInteger(result))throw Error('capacity_dataset_coverage_size_unavailable');return result;
 };
 const recent=minimum(initial.metadata7d,enoughRecent);
 const total=minimum(Math.max(recent,initial.metadata30d),value=>capacitySeedWindowCounts(make(recent,value),until).metadata30d>=targets.metadata30d);
 const manifest=make(recent,total),end=capacitySeedWindowCounts(manifest,until);
 if(end.metadata7d<targets.metadata7d||end.metadata30d<targets.metadata30d)throw Error('capacity_dataset_coverage_plan_invalid');
 return manifest;
}
