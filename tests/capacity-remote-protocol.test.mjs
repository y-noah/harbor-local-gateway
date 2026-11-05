import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import {setTimeout as delay} from 'node:timers/promises';
import {createCapacityRemoteFrames} from '../tools/capacity/remote-protocol.mjs';

async function until(read,timeoutMs=3000){const deadline=Date.now()+timeoutMs;while(!read()){if(Date.now()>deadline)throw Error('condition timeout');await delay(5);}}
async function pair(){let peer,accept;const accepted=new Promise(resolve=>{accept=resolve;}),server=net.createServer(socket=>{peer=socket;accept();});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const socket=net.connect(server.address().port,'127.0.0.1');socket.on('error',()=>{});await accepted;return {socket,peer,close:async()=>{socket.destroy();peer.destroy();await new Promise(resolve=>server.close(resolve));}};}

test('real socket write queue reserves bytes for cancellation after data budget is full',{timeout:10000},async()=>{
 const p=await pair(),received=[];try{
  const sender=createCapacityRemoteFrames(p.socket,{maxFrameBytes:2048,maxBufferedBytes:8192,controlReserveBytes:2048,onFrame:()=>true,onClose:()=>{}}),reader=createCapacityRemoteFrames(p.peer,{maxFrameBytes:2048,maxBufferedBytes:8192,controlReserveBytes:2048,onFrame:value=>{received.push(value);return true;},onClose:()=>{}});
  p.socket.cork();let sent=0;for(;;){try{sender.send({kind:'data',value:'x'.repeat(1000)});sent++;}catch(error){assert.equal(error.code,'capacity_remote_transport_budget');assert.equal(error.delivery,'not-sent');break;}}
  assert.ok(sent>0);sender.send({kind:'cancel'},{control:true});assert.ok(sender.status().peakBufferedBytes<=8192);p.socket.uncork();await until(()=>received.length===sent+1);assert.equal(received.at(-1).kind,'cancel');reader.close();
 }finally{await p.close();}
});

test('oversized announced frame is rejected before payload allocation',{timeout:10000},async()=>{
 const p=await pair();let frames=0;try{const reader=createCapacityRemoteFrames(p.peer,{maxFrameBytes:2048,maxBufferedBytes:8192,controlReserveBytes:2048,onFrame:()=>{frames++;return true;},onClose:()=>{}}),header=Buffer.alloc(4);header.writeUInt32BE(0xffffffff);p.socket.write(header);await until(()=>reader.status().closed);assert.equal(frames,0);assert.equal(reader.status().failed,true);}finally{await p.close();}
});

test('malformed JSON permanently closes channel instead of continuing with later valid frames',{timeout:10000},async()=>{
 const p=await pair();let frames=0;try{const reader=createCapacityRemoteFrames(p.peer,{onFrame:()=>{frames++;return true;},onClose:()=>{}}),payload=Buffer.from('invalid'),header=Buffer.alloc(4);header.writeUInt32BE(payload.length);p.socket.write(Buffer.concat([header,payload]));await until(()=>reader.status().closed);assert.equal(frames,0);}finally{await p.close();}
});

test('multiple complete frames in one TCP chunk may exceed retained input budget',{timeout:10000},async()=>{
 const p=await pair(),received=[];let largest=0;try{
  p.peer.prependListener('data',chunk=>{largest=Math.max(largest,chunk.length);});const reader=createCapacityRemoteFrames(p.peer,{maxFrameBytes:1024,maxBufferedBytes:2048,controlReserveBytes:1024,onFrame:value=>{received.push(value);return true;},onClose:()=>{}});
  const frames=Array.from({length:40},(_,index)=>{const body=Buffer.from(JSON.stringify({index,value:'x'.repeat(300)})),header=Buffer.alloc(4);header.writeUInt32BE(body.length);return Buffer.concat([header,body]);});p.socket.write(Buffer.concat(frames));await until(()=>received.length===40);assert.ok(largest>2048,'actual TCP callback exercised coalesced input beyond budget');assert.deepEqual(received.map(value=>value.index),Array.from({length:40},(_,index)=>index));assert.equal(reader.status().failed,false);
 }finally{await p.close();}
});
