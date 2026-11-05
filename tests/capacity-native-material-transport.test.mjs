import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,rm,rename} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createControlledResponsesTransport,createControlledUpstream} from '../tools/capacity/controlled-upstream.mjs';
import {capacityCredentialProof} from '../tools/capacity/recovery-material-adapter.mjs';
const token=exp=>'synthetic.'+Buffer.from(JSON.stringify({exp})).toString('base64url')+'.synthetic';
test('native subscription credentials are read from real files and only their digest reaches the controlled upstream',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'harbor-capacity-native-material-')),events=[];let upstream;try{
    upstream=await createControlledUpstream({durationMs:30,firstContentMs:5,chunkIntervalMs:10,sink:row=>events.push(row)});const transport=createControlledResponsesTransport({origin:upstream.origin,loadAccountMaterials:true}),file=join(directory,'auth.json'),access=token(Math.floor(Date.now()/1000)+3600),accountId='synthetic-material-account';await writeFile(file,JSON.stringify({tokens:{access_token:access,account_id:accountId}}),{mode:0o600});
    const call=trace=>transport.run({binary:'must-not-launch',home:directory,payload:{model:'gpt-6.1-sol',input:'合成材料恢复资格',metadata:{capacity_trace:trace},stream:true}});const first=await call('material-first');assert.equal(first.input,1000);assert.equal(first.output,200);const proof=capacityCredentialProof({authorization:'Bearer '+access,'chatgpt-account-id':accountId});assert.equal(events.find(row=>row.kind==='upstream-dispatch'&&row.trace==='material-first').credentialProof,proof);assert.equal(JSON.stringify(events).includes(access),false);assert.equal(JSON.stringify(events).includes(accountId),false);
    await rename(file,file+'.saved');await assert.rejects(call('material-missing'));assert.equal(events.some(row=>row.trace==='material-missing'),false);await rename(file+'.saved',file);const second=await call('material-restored');assert.equal(second.input,1000);assert.equal(events.find(row=>row.kind==='upstream-dispatch'&&row.trace==='material-restored').credentialProof,proof);
    await writeFile(file,JSON.stringify({tokens:{access_token:token(Math.floor(Date.now()/1000)-3600),account_id:accountId}}));await assert.rejects(call('material-expired'),error=>error.code==='upstream_connection'&&error.usageKnown===true);assert.equal(events.some(row=>row.trace==='material-expired'),false);
    assert.throws(()=>createControlledResponsesTransport({origin:upstream.origin,loadAccountMaterials:'yes'}),/credentials_configuration/);
  }finally{await upstream?.close();await rm(directory,{recursive:true,force:true});}
});
