import {usageWindow} from '../../store.mjs';
import {capacityLiveWindow} from './coverage-adapter.mjs';

export function fixtureTimestamp(manifest,index){const visible=usageWindow(manifest.anchor),oldStart=visible.start-23*86400000;if(index<manifest.metadata7d)return visible.start+Math.floor(index*(manifest.anchor-visible.start)/manifest.metadata7d);return oldStart+Math.floor((index-manifest.metadata7d)*(visible.start-oldStart)/(manifest.metadata30d-manifest.metadata7d));}
const below=(manifest,first,end,time)=>{let low=first,high=end;while(low<high){const middle=low+Math.floor((high-low)/2);if(fixtureTimestamp(manifest,middle)<time)low=middle+1;else high=middle;}return low;};
function checkedWindow(at){
 if(!Number.isSafeInteger(at)||at<0)throw Error('capacity_dataset_window_invalid');
 const window=capacityLiveWindow(at);if(![window.start,window.end,window.metadata30dStart].every(Number.isSafeInteger))throw Error('capacity_dataset_window_invalid');return window;
}
// Both timestamp partitions are monotone. These counts use the actual seed
// generator rather than assuming the initial natural-day window stays fixed.
export function capacitySeedWindowCounts(manifest,at){
 const window=checkedWindow(at),count=start=>[[0,manifest.metadata7d],[manifest.metadata7d,manifest.metadata30d]].reduce((total,[first,end])=>total+below(manifest,first,end,window.end)-below(manifest,first,end,start),0);
 return {metadata7d:count(window.start),metadata30d:count(window.metadata30dStart)};
}
// Receipts cover the first bodyRequests indices. Expiry is strict: an item
// whose expires_at equals the observation time is already outside retention.
export function capacitySeedActiveBodyRanges(manifest,at){
 checkedWindow(at);const firstAlive=at-30*86400000+1;
 return [[0,Math.min(manifest.metadata7d,manifest.bodyRequests)],[manifest.metadata7d,manifest.bodyRequests]].filter(([first,end])=>end>first).map(([first,end])=>[below(manifest,first,end,firstAlive),end]).filter(([first,end])=>end>first);
}
