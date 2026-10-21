import test from 'node:test';
import assert from 'node:assert/strict';
import {planNightly,excessLoad} from '../nightly-plan.mjs';

function fixture(shares,rows,{fixed={},searchBudget}={}){
  const pool=shares.map((_,i)=>({id:String(i)})),movable=rows.map(([account,weight],i)=>({id:'s'+i,account_id:String(account),weight,route_version:0})).sort((a,b)=>b.weight-a.weight||a.id.localeCompare(b.id));
  const before=Object.fromEntries(pool.map(a=>[a.id,fixed[a.id]||0]));for(const s of movable)before[s.account_id]+=s.weight;
  const total=Object.values(before).reduce((a,b)=>a+b,0),target=new Map(shares.map((share,i)=>[String(i),total*share]));
  const input={pool,movable,before,target,total,...(searchBudget===undefined?{}:{searchBudget})},result=planNightly(input),loads=new Map(Object.entries(before));
  assert.equal(new Set(result.moves.map(m=>m.session_id)).size,result.moves.length);
  for(const m of result.moves){assert.ok(target.get(m.to)>0);assert.notEqual(m.from,m.to);loads.set(m.from,loads.get(m.from)-m.weight);loads.set(m.to,loads.get(m.to)+m.weight);}
  assert.ok([...loads.values()].every(x=>x>=-1e-8));
  assert.ok(Math.abs(result.planning.afterDeviation-excessLoad(loads,target)/total*100)<1e-8);
  return {input,result,loads};
}
test('heavy session priority can require more migrations than a light exact fit',()=>{
  const {result,loads}=fixture([.5,.5],[[0,90],[0,40],[0,20],[1,50]]);
  assert.deepEqual(result.planning.prioritySessions,['s0']);assert.equal(result.moves.length,2);
  assert.deepEqual(new Set(result.moves.map(m=>m.session_id)),new Set(['s0','s3']));assert.deepEqual([...loads.values()],[110,90]);
});
test('five-point tolerance accepts a heavy transfer without unnecessary compensation',()=>{
  const {result,loads}=fixture([.5,.5],[[0,60],[0,40],[0,30],[0,20],[1,50]]);
  assert.equal(result.moves.length,1);assert.equal(result.moves[0].session_id,'s0');assert.deepEqual([...loads.values()],[90,110]);
});
test('an oversized heavy session is skipped only because no positive-weight destination can hold it',()=>{
  const {result,loads}=fixture([.6,.4],[[0,120],[0,40],[1,40]]);
  assert.deepEqual(result.planning.heavySkipped,[{session_id:'s0',reason:'cannot_fit'}]);assert.deepEqual(result.planning.prioritySessions,['s1']);assert.deepEqual([...loads.values()],[120,80]);
});
test('multiple required heavy sessions precede the compensation minimum',()=>{
  const {result}=fixture([.5,.5],[[0,35],[0,30],[0,25],[1,10]]);
  assert.deepEqual(result.planning.prioritySessions,['s0']);
  // Selecting 35 covers the 35-unit excess outside tolerance, without forcing every heavy session out.
  assert.equal(result.moves.length,1);assert.equal(result.planning.afterDeviation,5);
  const next=fixture([.5,.5],[[0,20],[0,20],[0,20],[0,20],[0,10],[1,10]]);
  assert.deepEqual(next.result.planning.prioritySessions,['s0','s1']);assert.equal(next.result.moves.length,2);
});
test('three-account allocation jointly places a heavy session and a compensating transfer',()=>{
  const {result,loads}=fixture([.5,.3,.2],[[0,40],[0,20],[0,10],[1,10],[1,10],[2,10]]);
  assert.deepEqual(result.planning.heavySkipped,[{session_id:'s0',reason:'cannot_fit'}]);assert.deepEqual(result.planning.prioritySessions,['s1']);assert.equal(result.moves.length,2);assert.deepEqual([...loads.values()],[50,30,20]);
});
test('aggregate tolerance rejects accumulated deviations across ten accounts',()=>{
  const rows=Array.from({length:10},(_,i)=>[i,i<5?15:5]);
  const {result}=fixture(Array(10).fill(.1),rows);
  assert.equal(result.planning.beforeDeviation,25);assert.notEqual(result.planning.reason,'within_tolerance');
});
test('aggregate tolerance includes the boundary and does not migrate inside it',()=>{
  for(const delta of [0,4.9999,5])assert.equal(fixture([.5,.5],[[0,50+delta],[1,50-delta]]).result.moves.length,0);
  const {result}=fixture([.5,.5],[[0,50],[0,5.0001],[1,44.9999]]);assert.equal(result.planning.withinTolerance,true);assert.ok(result.moves.length>0);
});
test('equal-count choices put excess on the higher budget instead of the lower budget',()=>{
  // Required 30 leaves loads 50/60. Returning 10 or 20 takes two moves either way.
  const {result,loads}=fixture([65/110,45/110],[[0,30],[0,25],[0,25],[1,10],[1,20]]);
  assert.equal(result.planning.withinTolerance,true);assert.equal(result.moves.length,2);assert.deepEqual([...loads.values()],[70,40]);
  assert.ok(result.planning.prioritySessions.includes('s0'));
});
test('fixed load and zero targets remain hard constraints for receiving migrations',()=>{
  const {result}=fixture([.6,.4,0],[[0,20],[0,20],[1,10],[2,10]],{fixed:{0:40}});
  assert.ok(result.moves.every(m=>m.to!=='2'));assert.ok(result.moves.every(m=>!m.session_id.startsWith('fixed')));
  const blocked=fixture([.5,.5],[[0,20]],{fixed:{0:80}});assert.equal(blocked.result.planning.reason,'infeasible');assert.equal(blocked.loads.get('0'),80);assert.equal(blocked.result.planning.withinTolerance,false);
});
test('search exhaustion is distinct from infeasibility and cannot silently select only light sessions',()=>{
  const {result}=fixture([.5,.5],[[0,30],[0,20],[0,20],[1,30]],{searchBudget:0});
  assert.equal(result.planning.reason,'search_limited');assert.equal(result.planning.priorityComplete,false);assert.deepEqual(result.moves,[]);
});

// Independent exhaustive assignment oracle: no planner pruning, greedy phase, or move-count search.
test('small multi-account plans match exhaustive heavy-direction-count decisions',()=>{
  let seed=9173;const random=n=>{seed=(Math.imul(seed,1664525)+1013904223)>>>0;return seed%n;};
  for(let trial=0;trial<90;trial++){
    const count=2+random(3),size=3+random(5),budgets=Array.from({length:count},()=>1+random(5)),sum=budgets.reduce((a,b)=>a+b,0),shares=budgets.map(x=>x/sum),rows=Array.from({length:size},()=>[random(count),1+random(35)]);
    const {input,result,loads}=fixture(shares,rows),{pool,movable,before,target,total}=input,tolerance=total*.05,initialExcess=Object.entries(before).reduce((n,[a,w])=>n+Math.max(0,w-target.get(a)),0);
    const levels=[...new Set(budgets)].sort((a,b)=>a-b).slice(0,-1);
    const direction=amounts=>levels.map(level=>amounts.reduce((n,v,i)=>n+(budgets[i]===level?Math.max(0,v-total*shares[i]):0),0));
    const compare=(a,b)=>{for(let i=0;i<a.length;i++)if(Math.abs(a[i]-b[i])>1e-8)return a[i]-b[i];return 0;};
    const initialDirection=direction(pool.map(a=>before[a.id])),correction=initialExcess<=tolerance+1e-8;
    if(correction&&initialDirection.every(x=>x<1e-8)){assert.equal(result.moves.length,0);continue;}
    const feasible=[],destinations=[];
    function enumerate(index){
      if(index===size){
        const amounts=pool.map(()=>0);for(let i=0;i<size;i++)amounts[destinations[i]]+=rows[i][1];
        const excess=amounts.reduce((n,v,i)=>n+Math.max(0,v-total*shares[i]),0);
        if(excess<=tolerance+1e-8&&(!correction||compare(direction(amounts),initialDirection)<0))feasible.push({moved:new Set(rows.flatMap(([a],i)=>a===destinations[i]?[]:['s'+i])),amounts});
        return;
      }
      for(let a=0;a<count;a++){destinations[index]=a;enumerate(index+1);}
    }
    enumerate(0);
    if(!feasible.length){assert.equal(result.planning.reason,correction?'direction_unavailable':'infeasible',`trial ${trial}`);continue;}
    let candidates=feasible,covered=0;const required=[],released=Array(count).fill(0);
    for(const s of movable){
      if(covered>=(correction?initialDirection.reduce((a,b)=>a+b,0):initialExcess-tolerance)-1e-8)break;
      const a=Number(s.account_id),surplus=correction&&budgets[a]===Math.max(...budgets)?0:Math.max(0,before[a]-target.get(String(a))),old=Math.min(surplus,released[a]);
      if(old>=surplus-1e-8)continue;
      const next=candidates.filter(p=>p.moved.has(s.id));if(!next.length)continue;
      candidates=next;required.push(s.id);released[a]+=s.weight;covered+=Math.min(surplus,released[a])-old;
    }
    assert.deepEqual(result.planning.prioritySessions,required,`priority trial ${trial}`);
    assert.ok(result.planning.withinTolerance,`tolerance trial ${trial}`);
    assert.ok(result.planning.optimizationComplete,`complete trial ${trial}`);
    const score=p=>[...direction(p.amounts),p.moved.size,p.amounts.reduce((n,v,i)=>n+Math.max(0,v-total*shares[i]),0)];
    const best=candidates.map(score).sort(compare)[0];
    assert.equal(compare(score({amounts:pool.map(a=>loads.get(a.id)),moved:new Set(result.moves.map(m=>m.session_id))}),best),0,`direction/count trial ${trial}`);
  }
});

test('direction wins over fewer transfers',()=>{
  const {result,loads}=fixture([.6,.4],[[1,40],[1,40],[1,15],[1,4],[1,1]]);
  assert.equal(result.moves.length,4);assert.deepEqual([...loads.values()],[60,40]);assert.ok(result.planning.prioritySessions.includes('s0'));
});
test('inside tolerance corrects the low-budget side with heavy priority, including reverse compensation',()=>{
  const {result,loads}=fixture([.6,.4],[[0,35],[0,21],[1,30],[1,10],[1,4]]);
  assert.equal(result.planning.beforeDeviation,4);assert.deepEqual(result.planning.prioritySessions,['s2']);
  assert.equal(result.moves.length,2);assert.deepEqual([...loads.values()],[65,35]);assert.equal(result.planning.afterDeviation,5);
  // A third move can get closer to target (61/39); equivalent direction keeps two.
  assert.equal(fixture([.6,.4],[[0,64],[1,36]]).result.moves.length,0);
});
test('unavailable correction is explicit and protected load can still allow a partial improvement',()=>{
  const blocked=fixture([.6,.4],[[0,56],[1,44]]);
  assert.equal(blocked.result.moves.length,0);assert.equal(blocked.result.planning.reason,'direction_unavailable');
  const partial=fixture([.6,.4],[[1,1]],{fixed:{0:56,1:43}});
  assert.equal(partial.result.moves.length,1);assert.deepEqual([...partial.loads.values()],[57,43]);
  assert.equal(partial.result.planning.directionComplete,true);
});
test('budget-limited optimization retains a direction-correct witness and reports the unproven minimum',()=>{
  const {result,loads}=fixture([.6,.4],[[1,20],[1,20],[1,20],[1,20],[1,20]],{searchBudget:0});
  assert.equal(result.planning.priorityComplete,true);assert.equal(result.planning.directionComplete,true);
  assert.equal(result.planning.optimizationComplete,false);assert.equal(result.planning.searchLimited,true);
  assert.equal(result.moves.length,3);assert.deepEqual([...loads.values()],[60,40]);
});
