import test from 'node:test';
import assert from 'node:assert/strict';
import {PassThrough} from 'node:stream';
import {PeriodicSshWire} from '../postgres-periodic-ssh-wire.mjs';
import {servePeriodicSshSource,receivePeriodicSshTransfer} from '../postgres-periodic-ssh-transfer.mjs';
import {periodicObjectStoreLifetime} from '../postgres-periodic-object-store.mjs';

function peers(){const forward=new PassThrough(),reverse=new PassThrough();return [new PeriodicSshWire(reverse,forward,{timeoutMs:1000}),new PeriodicSshWire(forward,reverse,{timeoutMs:1000})];}
test('source requires the live index, never a JSON copy or path-shaped descriptor',async()=>{
 const [sender,peer]=peers();try{await assert.rejects(servePeriodicSshSource({directory:'/etc',entries:[],scope:'periodic-application'},sender),{code:'periodic_transfer_index_required'});assert.equal(peer.input.readableLength,0);}finally{sender.close();peer.close();}
});
test('receiver requires the real owner capability before its first network read',async()=>{
 const fake={scope:'root-periodic-transport',directory:'/etc'};assert.throws(()=>periodicObjectStoreLifetime(fake),{code:'periodic_transfer_store_required'});
 const [peer,receiver]=peers();try{await assert.rejects(receivePeriodicSshTransfer(fake,receiver),{code:'periodic_transfer_store_required'});assert.equal(receiver.reading,false);}finally{peer.close();receiver.close();}
});
