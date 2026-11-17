import test from 'node:test';
import assert from 'node:assert/strict';
import {createRouteAdmissionWait,routeAdmissionBusy} from '../postgres-route-wait.mjs';
import {AppError} from '../store.mjs';

test('自动换绑仅等DB指定旧请求，两个新请求不互等且不按时间重试',async t=>{
 t.mock.timers.enable({apis:['setTimeout']});const wait=createRouteAdmissionWait(),release=wait.own('old'),unrelated=wait.own('other-session');let old=true,calls=0;
 const admit=()=>{calls++;if(old)throw routeAdmissionBusy('old');return 'new-account';};
 const pending=[wait.run(admit),wait.run(admit)];await Promise.resolve();t.mock.timers.tick(120000);await Promise.resolve();assert.equal(calls,2);old=false;release();assert.deepEqual(await Promise.all(pending),['new-account','new-account']);assert.equal(calls,4);assert.equal(wait.status().owners,1);unrelated();
});
test('取消或关闭等待不会释放原请求owner或重放，遗留记录只检查一次',async()=>{
 const wait=createRouteAdmissionWait(),release=wait.own('old'),controller=new AbortController();let calls=0;
 const pending=wait.run(()=>{calls++;throw routeAdmissionBusy('old');},{signal:controller.signal});controller.abort(new AppError(503,'停止','service_shutdown'));await assert.rejects(pending,{code:'service_shutdown'});assert.equal(calls,1);assert.equal(wait.status().owners,1);release();
 await assert.rejects(wait.run(()=>{calls++;throw routeAdmissionBusy('old');}),{code:'session_route_owner_missing'});assert.equal(calls,2);
});
test('不重试普通busy、限额拒绝或外部事务中的路由冲突',async()=>{
 const wait=createRouteAdmissionWait();for(const error of [new AppError(409,'busy','session_busy'),new AppError(429,'limit','member_model_limit')]){let calls=0;await assert.rejects(wait.run(()=>{calls++;throw error;}),e=>e===error);assert.equal(calls,1);}
 const release=wait.own('old'),error=routeAdmissionBusy('old');await assert.rejects(wait.run(()=>{throw error;},{inTransaction:()=>true}),e=>e===error);release();
});

// A request can finish after the routing SQL snapshot but before rollback returns.
test('结束与回滚交错时使用已捕获owner，不误报遗留请求',async()=>{
 const wait=createRouteAdmissionWait(),release=wait.own('old'),busy=wait.capture();let calls=0;
 const value=await wait.run(async()=>{if(++calls===1){release();throw busy('old');}return 'ready';});assert.equal(value,'ready');assert.equal(calls,2);assert.equal(wait.status().owners,0);
});
