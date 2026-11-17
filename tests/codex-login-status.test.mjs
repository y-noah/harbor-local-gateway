import test from 'node:test';
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {PassThrough} from 'node:stream';
import {checkLogin} from '../codex.mjs';

function fixture(options={}){
 const child=new EventEmitter();child.stdout=new PassThrough();child.stderr=new PassThrough();let kills=0;
 child.kill=()=>{kills++;queueMicrotask(()=>child.emit('close',null));};
 const promise=checkLogin('synthetic-cli','synthetic-home',{spawnProcess:()=>child,...options});
 return {child,promise,get kills(){return kills;},close(code,text=''){child.stderr.write(text);child.emit('close',code);}};
}
test('仅明确订阅登录、未登录和API登录决定订阅状态',async()=>{
 for(const [code,output,expected] of [[0,'Logged in using ChatGPT\n',true],[0,'ChatGPT authentication active\n',true],[1,'Not logged in\n',false],[0,'Logged in using an API key - synthetic\n',false]]){
  const f=fixture();f.close(code,output);assert.equal(await f.promise,expected);
 }
});
test('启动故障及异常退出不是未登录，诊断不能泄露CLI内容',async()=>{
 const f=fixture();f.child.emit('error',Error('secret-file-path'));await assert.rejects(f.promise,e=>e.code==='subscription_check_unavailable'&&!e.message.includes('secret'));
 for(const [code,output] of [[1,'secret backend failure'],[null,'Logged in using ChatGPT'],[0,'unexpected secret output'],[1,'failed to connect ChatGPT']]){
  const f=fixture();f.close(code,output);await assert.rejects(f.promise,e=>e.code==='subscription_check_failed'&&!e.message.includes('secret'));
 }
 await assert.rejects(checkLogin('synthetic','synthetic',{spawnProcess(){throw Error('secret spawn');}}),{code:'subscription_check_unavailable'});
});
test('状态检查超时和取消单独报错并结束进程',async()=>{
 const timeout=fixture({timeoutMs:5});await assert.rejects(timeout.promise,{code:'subscription_check_timeout'});assert.equal(timeout.kills,1);
 const controller=new AbortController(),cancelled=fixture({signal:controller.signal});controller.abort();await assert.rejects(cancelled.promise,{code:'subscription_check_cancelled'});assert.equal(cancelled.kills,1);
 let spawned=false;await assert.rejects(checkLogin('synthetic','synthetic',{signal:controller.signal,spawnProcess(){spawned=true;}}),{code:'subscription_check_cancelled'});assert.equal(spawned,false);
});
