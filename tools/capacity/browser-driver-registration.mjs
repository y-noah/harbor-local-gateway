import {randomUUID,createHash} from 'node:crypto';
import {capacityClientEntryBinding} from './client-entry.mjs';
import {capacityRequestDriverRemoteBrowserBinding} from './request-driver.mjs';

const registries=new WeakMap(),tags=new WeakMap();
const fail=code=>{throw Error('capacity_browser_driver_'+code);},hash=value=>createHash('sha256').update(value).digest('hex');
const same=(left,right)=>JSON.stringify(left)===JSON.stringify(right);

// Configuration metadata only. A copied descriptor never registers a driver;
// consumers must obtain the registry view from their actual owned snapshot.
export function capacityBrowserSnapshotDescriptor(snapshot){
 if(!snapshot||typeof snapshot!=='object'||!Array.isArray(snapshot.adminActors)||snapshot.adminActors.length<5||!Number.isSafeInteger(snapshot.epoch)||snapshot.epoch<0)fail('snapshot_invalid');
 const entry=snapshot.entry?capacityClientEntryBinding(snapshot.entry):null;if(snapshot.entry&&(!entry||entry.laboratory!==snapshot.laboratory||!entry.isOpen()))fail('snapshot_invalid');
 let publicEntry=null;if(snapshot.readPublicEntry!=null||snapshot.publicEntryProof!=null){if(typeof snapshot.readPublicEntry!=='function'||!snapshot.publicEntryProof)fail('snapshot_invalid');publicEntry=snapshot.readPublicEntry();if(publicEntry?.tls!==true||publicEntry?.proxyPreserved!==true)fail('snapshot_invalid');}
 const origin=publicEntry?.origin??entry?.origin??snapshot.tlsOrigin;let url;try{url=new URL(origin);}catch{fail('snapshot_invalid');}if(url.protocol!=='https:'||url.origin!==origin||url.username||url.password)fail('snapshot_invalid');
 const cookies=snapshot.adminActors.slice(0,5).map(actor=>{const header=actor?.headers?.cookie;if(typeof header!=='string'||header.length>8192||/[\r\n;]/.test(header)||!header.startsWith('harbor_session=')||header.length===15)fail('snapshot_invalid');return {header,value:header.slice(15)};});
 const cookieFingerprints=cookies.map(row=>hash(row.value));if(new Set(cookieFingerprints).size!==5)fail('snapshot_invalid');let bindingTag=tags.get(snapshot);if(!bindingTag){bindingTag=randomUUID();tags.set(snapshot,bindingTag);}
 return Object.freeze({origin,bindingTag,epoch:snapshot.epoch,cookieFingerprints:Object.freeze(cookieFingerprints),cookieHeaderFingerprints:Object.freeze(cookies.map(row=>hash(row.header))),scope:'fixed-browser-profile-selection',formalQualified:false});
}

function driverIdentity(driver){
 const binding=capacityRequestDriverRemoteBrowserBinding(driver?.launchBrowser);
 if(!binding||binding.driver!==driver||!binding.identity||binding.identity.isOpen()!==true||typeof binding.identity.runId!=='string'||!/^[a-f0-9]{64}$/.test(binding.identity.endpointCertificateHash??'')||!Array.isArray(binding.identity.browserProfiles))return null;
 return binding.identity;
}
function matchedProfile(identity,descriptor){return identity.browserProfiles.find(row=>row.origin===descriptor.origin&&row.bindingTag===descriptor.bindingTag&&row.epoch===descriptor.epoch&&same(row.cookieFingerprints,descriptor.cookieFingerprints));}

// Only the factory keeps register(). The immutable snapshot receives view,
// which exposes no mutation operation. A caller-created registry cannot replace
// the view in the factory's private, immutable owned snapshot.
export function createCapacityBrowserDriverRegistry({currentSnapshot}={}){
 if(typeof currentSnapshot!=='function')fail('configuration');const registrations=new WeakMap(),view=Object.freeze(Object.create(null));
 const register=driver=>{
  const snapshot=currentSnapshot(),identity=driverIdentity(driver);if(!snapshot||!identity)fail('registration_invalid');const descriptor=capacityBrowserSnapshotDescriptor(snapshot),profile=matchedProfile(identity,descriptor);if(!profile||currentSnapshot()!==snapshot||driverIdentity(driver)!==identity)fail('registration_mismatch');
  registrations.set(driver,Object.freeze({snapshot,identity,profile,bindingTag:descriptor.bindingTag}));
  return Object.freeze({scope:'owned-snapshot-extra-browser-driver',runId:identity.runId,endpointCertificateHash:identity.endpointCertificateHash,profileId:profile.id,bindingTag:descriptor.bindingTag,epoch:descriptor.epoch,formalQualified:false});
 };
 registries.set(view,Object.freeze({currentSnapshot,registrations}));return Object.freeze({view,register});
}
export function isRegisteredCapacityBrowserDriver(view,snapshot,driver){
 try{
  const registry=registries.get(view),row=registry?.registrations.get(driver);if(!row||row.snapshot!==snapshot||registry.currentSnapshot()!==snapshot||driverIdentity(driver)!==row.identity)return false;
  const descriptor=capacityBrowserSnapshotDescriptor(snapshot);return descriptor.bindingTag===row.bindingTag&&matchedProfile(row.identity,descriptor)===row.profile&&registry.currentSnapshot()===snapshot;
 }catch{return false;}
}
