import test from 'node:test';
import assert from 'node:assert/strict';
import {capacityBrowserSnapshotDescriptor,createCapacityBrowserDriverRegistry,isRegisteredCapacityBrowserDriver} from '../tools/capacity/browser-driver-registration.mjs';
import {createCapacityRemoteBrowserService} from '../tools/capacity/remote-browser-service.mjs';
import {capacityRemoteFrameLimits} from '../tools/capacity/remote-protocol.mjs';

const metadata=()=>({epoch:0,entry:null,tlsOrigin:'https://127.0.0.1:1234',adminActors:Array.from({length:5},(_,index)=>({headers:{cookie:'harbor_session=synthetic-private-'+index}}))});
test('profile descriptors remain metadata and registry views expose no registration authority',()=>{
 const snapshot=metadata(),descriptor=capacityBrowserSnapshotDescriptor(snapshot),registry=createCapacityBrowserDriverRegistry({currentSnapshot:()=>snapshot});assert.equal(descriptor.formalQualified,false);assert.equal(capacityBrowserSnapshotDescriptor(snapshot).bindingTag,descriptor.bindingTag);assert.notEqual(capacityBrowserSnapshotDescriptor({...snapshot}).bindingTag,descriptor.bindingTag);assert.equal(JSON.stringify(descriptor).includes('synthetic-private'),false);
 assert.equal(Object.keys(registry.view).length,0);assert.equal(registry.view.register,undefined);assert.equal(isRegisteredCapacityBrowserDriver({...registry.view},snapshot,{}),false);assert.equal(isRegisteredCapacityBrowserDriver(registry.view,{...snapshot},{}),false);assert.throws(()=>registry.register({launchBrowser:async()=>{},status:()=>({authenticated:true,runId:'fake',endpointCertificateHash:'a'.repeat(64)})}),/registration_invalid/);
});
test('profile metadata rejects changed transports, incomplete actors and malformed cookie material',()=>{
 for(const mutation of [{tlsOrigin:'http://127.0.0.1:1234'},{epoch:-1},{adminActors:Array(5).fill({headers:{cookie:'harbor_session=duplicate'}})},{adminActors:[{headers:{cookie:'harbor_session=value; another=secret'}}]}])assert.throws(()=>capacityBrowserSnapshotDescriptor({...metadata(),...mutation}),/snapshot_invalid/);
});
test('fixed browser service rejects URL, script and unapproved profile RPC without launching a browser',async()=>{
 const source=metadata(),descriptor=capacityBrowserSnapshotDescriptor(source),plan={id:'approved',origin:descriptor.origin,bindingTag:descriptor.bindingTag,epoch:descriptor.epoch,cookies:source.adminActors.map(actor=>({name:'harbor_session',value:actor.headers.cookie.slice(15)})),serverSpkiHash:'A'.repeat(43)+'='};let launches=0;
 const service=createCapacityRemoteBrowserService({profiles:[plan],launchBrowser:async()=>{launches++;throw Error('must not launch');},emitChallenge:()=>false,emitState:()=>false,clockEpoch:'synthetic-epoch',frames:capacityRemoteFrameLimits()});
 for(const payload of [{profileId:'unknown'},{profileId:'approved',origin:'https://example.invalid'},{profileId:'approved',command:'shell'},{profileId:'approved',script:'arbitrary'}])await assert.rejects(service.command('open',payload,'synthetic-action'),/capacity_remote_browser_/);
 await assert.rejects(service.command('evaluate',{browserSession:'unknown',script:'arbitrary'},'synthetic-action'),/capacity_remote_browser_/);assert.equal(launches,0);assert.equal(service.status().sessions,0);assert.equal(service.status().pendingCallbacks,0);await service.close();
});

test('default snapshots accept absent public entry while inconsistent proof and reader pairs are rejected',()=>{
 const snapshot={...metadata(),publicEntryProof:null,readPublicEntry:null};assert.equal(capacityBrowserSnapshotDescriptor(snapshot).origin,snapshot.tlsOrigin);
 for(const change of [{publicEntryProof:{}},{readPublicEntry:()=>({origin:snapshot.tlsOrigin,tls:true,proxyPreserved:true})},{readPublicEntry:false}])assert.throws(()=>capacityBrowserSnapshotDescriptor({...snapshot,...change}),/snapshot_invalid/);
});
