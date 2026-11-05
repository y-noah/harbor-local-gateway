import {createHash} from 'node:crypto';
import {remoteFailure,remoteId,remoteInteger} from './remote-protocol.mjs';
const fail=()=>{throw remoteFailure('capacity_remote_browser_activation_invalid');};
const keys=(value,allowed)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>allowed.includes(key));
const hash=value=>createHash('sha256').update(value).digest('hex');
// Root initialization fixes all network, executable and measurement policy.
// The later authenticated activation carries only one epoch and five cookies.
export function capacityRemoteBrowserActivationTemplate(value){
 if(value==null)return null;
 if(!keys(value,['id','origin','serverSpkiHash','deepPages','sampleIntervalMs','timeoutMs','maxSeries','searchTerm','maxActionMs'])||!remoteId(value.id))fail();
 let address;try{address=new URL(value.origin);}catch{fail();}if(address.protocol!=='https:'||address.origin!==value.origin||address.username||address.password||!/^[A-Za-z0-9+/]{43}=$/.test(value.serverSpkiHash??''))fail();
 const result={deepPages:100,sampleIntervalMs:1000,timeoutMs:15000,maxSeries:2048,searchTerm:'capacityScenario',maxActionMs:300000,...value};
 remoteInteger(result.deepPages,2,1000);remoteInteger(result.sampleIntervalMs,1,60000);remoteInteger(result.timeoutMs,100,60000);remoteInteger(result.maxSeries,1,10000);remoteInteger(result.maxActionMs,100,600000);
 if(typeof result.searchTerm!=='string'||Array.from(result.searchTerm).length<2||Array.from(result.searchTerm).length>128)fail();return Object.freeze(result);
}
export function capacityRemoteBrowserActivation(value){
 if(!keys(value,['bindingTag','epoch','cookies'])||Object.keys(value).length!==3||!remoteId(value.bindingTag)||!Number.isSafeInteger(value.epoch)||value.epoch<0||!Array.isArray(value.cookies)||value.cookies.length!==5)fail();
 const cookies=value.cookies.map(row=>{if(!keys(row,['name','value'])||Object.keys(row).length!==2||row.name!=='harbor_session'||typeof row.value!=='string'||!row.value.length||row.value.length>8192||/[\r\n;]/.test(row.value))fail();return Object.freeze({name:row.name,value:row.value});});
 if(new Set(cookies.map(row=>hash(row.value))).size!==5)fail();return Object.freeze({bindingTag:value.bindingTag,epoch:value.epoch,cookies:Object.freeze(cookies)});
}
export function capacityRemoteBrowserActivatedSummary(template,activation){return Object.freeze({id:template.id,origin:template.origin,bindingTag:activation.bindingTag,epoch:activation.epoch,cookieFingerprints:Object.freeze(activation.cookies.map(row=>hash(row.value))),...Object.fromEntries(['deepPages','sampleIntervalMs','timeoutMs','maxSeries','searchTerm','maxActionMs'].map(name=>[name,template[name]]))});}
