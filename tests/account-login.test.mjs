import test from 'node:test';
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {PassThrough,Writable} from 'node:stream';
import {startCodexLogin} from '../account-login.mjs';

const challenge={type:'chatgptDeviceCode',loginId:'test-login',verificationUrl:'https://auth.openai.com/codex/device',userCode:'TEST-ONLY'};
function fixture(result=challenge,extra={}){
  const child=new EventEmitter();child.stdout=new PassThrough();child.stderr=new PassThrough();let killed=false,args,env;const requests=[];
  const emit=value=>child.stdout.write(JSON.stringify(value)+'\n');
  child.kill=()=>{killed=true;};
  child.stdin=new Writable({write(bytes,_,done){const req=JSON.parse(bytes);requests.push(req);queueMicrotask(()=>{
    if(req.method==='initialize')emit({id:1,result:{}});
    if(req.method==='account/login/start'&&result)emit({id:2,...result.error?{error:result.error}:{result}});
  });done();}});
  const flow=startCodexLogin('fixture','isolated-account',{startTimeoutMs:1000,timeoutMs:3000,spawnProcess:(_,a,o)=>{args=a;env=o.env;return child;},...extra});
  return {flow,emit,child,requests,get killed(){return killed;},get args(){return args;},get env(){return env;}};
}
test('device authorization uses the official structured protocol and isolated credentials',async()=>{
  const f=fixture();assert.deepEqual(await f.flow.challenge,{type:'device',verificationUrl:challenge.verificationUrl,userCode:'TEST-ONLY'});
  assert.equal(f.env.CODEX_HOME,'isolated-account');assert.ok(f.args.includes('cli_auth_credentials_store="file"'));
  assert.deepEqual(f.requests.find(x=>x.method==='account/login/start').params,{type:'chatgptDeviceCode'});
  f.emit({method:'account/login/completed',params:{loginId:'another-login',success:true}});assert.equal(f.killed,false);
  f.emit({method:'account/login/completed',params:{loginId:'test-login',success:true}});assert.equal((await f.flow.completed).success,true);assert.equal(f.killed,true);
});
test('upstream errors and untrusted links cannot leak diagnostics or become a login link',async()=>{
  for(const result of [{error:{message:'private-token-in-upstream-error'}},{...challenge,verificationUrl:'https://evil.test/codex/device'},{...challenge,verificationUrl:'https://auth.openai.com/codex/device?token=private'},{...challenge,userCode:'<script>private</script>'}]){
    const f=fixture(result);await assert.rejects(f.flow.challenge,e=>!e.message.includes('private'));assert.equal((await f.flow.completed).success,false);assert.equal(f.killed,true);
  }
});
test('failure to start, challenge timeout, expiry and cancellation all end the subprocess',async()=>{
  const failed=fixture(null);queueMicrotask(()=>failed.child.emit('error',Error('private spawn error')));await assert.rejects(failed.flow.challenge,/无法启动/);assert.equal((await failed.flow.completed).success,false);
  const timeout=fixture(null,{startTimeoutMs:20});await assert.rejects(timeout.flow.challenge,/超时/);assert.equal(timeout.killed,true);
  const expiry=fixture(challenge,{timeoutMs:20});await expiry.flow.challenge;assert.match((await expiry.flow.completed).message,/超过/);assert.equal(expiry.killed,true);
  const cancelled=fixture();await cancelled.flow.challenge;cancelled.flow.cancel();assert.equal((await cancelled.flow.completed).message,'登录已取消');assert.equal(cancelled.killed,true);
});
test('official failures expose only a standard HTTP status and discard raw details',async()=>{
  const f=fixture({error:{message:'Device code auth request failed with status 403 Forbidden; private-token-detail'}});
  await assert.rejects(f.flow.challenge,e=>e.message.includes('HTTP 403')&&!e.message.includes('private'));assert.equal((await f.flow.completed).success,false);
});
