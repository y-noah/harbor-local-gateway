import test from 'node:test';
import assert from 'node:assert/strict';
import {transferSchedule,transferWindow,nextTransferReset,defaultTransferTimes} from '../transfer-schedule.mjs';

const at=value=>Date.parse('2026-11-03T'+value+'+08:00');
test('transfer schedule defaults to five actual Beijing slots and keeps the 02:00 historical identity',()=>{
  assert.deepEqual(transferSchedule().times,defaultTransferTimes);
  for(const time of defaultTransferTimes){
    const window=transferWindow(at(time+':00'));
    assert.equal(window.time,time);assert.equal(window.end,at(time+':00'));assert.equal(window.due,true);
    assert.equal(window.timezone,'Asia/Shanghai');assert.equal(window.start,window.end-16*3600000);
    assert.equal(window.key,time==='02:00'?'2026-11-03':'2026-11-03-'+time.replace(':',''));
  }
});
test('transfer recovery chooses only the most recent slot and wraps midnight without changing the 02:00 reset',()=>{
  assert.equal(transferWindow(at('12:59:59')).time,'10:00');
  const recovered=transferWindow(at('19:59:59'));assert.equal(recovered.time,'16:00');assert.equal(recovered.next,at('20:00:00'));
  const early=transferWindow(at('01:59:59'));assert.equal(early.key,'2026-11-02-2000');assert.equal(early.next,at('02:00:00'));
  assert.equal(transferWindow(at('13:01:00')).until,at('16:00:00'));
});
test('saving a transfer configuration never immediately replays an already due slot',()=>{
  const saved=transferSchedule({times:['20:00','10:00','10:00','02:00'],effective:at('10:00:01'),version:4});
  assert.deepEqual(saved.times,['02:00','10:00','20:00']);assert.equal(transferWindow(at('10:01:00'),saved).due,false);
  assert.equal(transferWindow(at('20:00:00'),saved).due,true);
  assert.equal(transferWindow(at('10:00:00'),{...saved,effective:at('10:00:00')}).due,false);
});
test('custom slots have stable identities even on either side of 02:00 and calendar rollover',()=>{
  const times=['23:59','00:00','02:00'];
  assert.equal(transferWindow(at('00:00:00'),{times}).key,'2026-11-03-0000');
  assert.equal(transferWindow(at('00:00:00'),{times}).until,at('02:00:00'));
  assert.equal(transferWindow(at('02:00:00'),{times}).until,at('23:59:00'));
  assert.equal(transferWindow(Date.parse('2027-01-01T00:00:00+08:00'),{times}).key,'2027-01-01-0000');
});
test('invalid or ambiguous transfer settings fail explicitly rather than changing the schedule',()=>{
  for(const value of [null,[],{times:[]},{times:null},{times:['2:00']},{times:['24:00']},{times:['02:60']},{times:['02:00',0]},{timezone:'UTC'},{version:-1},{effective:-1},{cron:'*'}])assert.throws(()=>transferSchedule(value),{code:'transfer_schedule_invalid'});
  for(const value of [NaN,-1,1.5])assert.throws(()=>transferWindow(value),{code:'transfer_schedule_invalid'});
});

test('temporary reset follows configured future slots only while enabled',()=>{
 const at=Date.parse('2026-11-03T01:59:00+08:00'),schedule={times:['10:00','16:00']};
 assert.equal(nextTransferReset(at,{nightly:false},schedule),null);
 assert.equal(nextTransferReset(at,{nightly:true},schedule),Date.parse('2026-11-03T10:00:00+08:00'));
 const boundary=Date.parse('2026-11-03T10:00:00+08:00');assert.equal(nextTransferReset(boundary,{nightly:true},schedule),Date.parse('2026-11-03T16:00:00+08:00'));
 assert.equal(nextTransferReset(at,{nightly:true},{times:['02:00']}),Date.parse('2026-11-03T02:00:00+08:00'));
});
