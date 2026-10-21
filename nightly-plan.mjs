export const nightlyTolerance=0.05;
const searchLimit=100000;

// Total variation: excess shares sum to the workload that is on the wrong side.
export const excessLoad=(loads,target)=>[...target].reduce((n,[a,t])=>n+Math.max(0,loads.get(a)-t),0);

export function planNightly({pool,movable,before,target,total,searchBudget=searchLimit}){
  const slack=total*nightlyTolerance,epsilon=total*Number.EPSILON*16;
  const initial=new Map(Object.entries(before)),beforeExcess=excessLoad(initial,target);
  const fixed=new Map(initial),byId=new Map(movable.map(s=>[s.id,s]));
  for(const s of movable)fixed.set(s.account_id,fixed.get(s.account_id)-s.weight);
  const levels=[...new Set(target.values())].sort((a,b)=>a-b);
  let budget=searchBudget;
  const spend=()=>budget>0?(budget--,true):false;
  const acceptable=loads=>excessLoad(loads,target)<=slack+epsilon;
  const loadsOf=moves=>{const loads=new Map(initial);for(const m of moves.values()){loads.set(m.from,loads.get(m.from)-m.weight);loads.set(m.to,loads.get(m.to)+m.weight);}return loads;};
  const move=(s,to)=>({session_id:s.id,from:s.account_id,to,version:s.route_version,weight:s.weight});
  const canReceive=(s,to)=>target.get(to)>0&&fixed.get(to)+s.weight<=target.get(to)+slack+epsilon;
  const bias=loads=>levels.slice(0,-1).map(level=>pool.reduce((n,a)=>n+(target.get(a.id)===level?Math.max(0,loads.get(a.id)-level):0),0));
  const compare=(left,right)=>{for(let i=0;i<left.length;i++)if(Math.abs(left[i]-right[i])>epsilon)return left[i]-right[i];return 0;};
  const compareBias=(loads,other)=>compare(bias(loads),bias(other));
  const correction=acceptable(initial);
  const valid=loads=>acceptable(loads)&&(!correction||compareBias(loads,initial)<0);
  const preferable=(loads,other)=>{
    // Equal-budget accounts are peers; only compare total error after direction.
    const direction=compareBias(loads,other);
    return direction<0||direction===0&&excessLoad(loads,target)<excessLoad(other,target)-epsilon;
  };
  // A linear pass supplies a direction-aware witness even for very large groups.
  function improveDirection(required,seed){
    const moves=new Map(seed),loads=loadsOf(moves);
    if(!acceptable(loads))return moves;
    for(const s of movable){
      const from=moves.get(s.id)?.to||s.account_id;let best=null;
      for(const a of pool){
        if(a.id===from||required.has(s.id)&&a.id===s.account_id||a.id!==s.account_id&&!canReceive(s,a.id))continue;
        loads.set(from,loads.get(from)-s.weight);loads.set(a.id,loads.get(a.id)+s.weight);
        const candidate=new Map(loads);
        loads.set(from,loads.get(from)+s.weight);loads.set(a.id,loads.get(a.id)-s.weight);
        if(!acceptable(candidate)||compareBias(candidate,loads)>=0)continue;
        if(!best||preferable(candidate,best.loads))best={to:a.id,loads:candidate};
      }
      if(best){
        loads.set(from,loads.get(from)-s.weight);loads.set(best.to,loads.get(best.to)+s.weight);
        if(best.to===s.account_id)moves.delete(s.id);else moves.set(s.id,move(s,best.to));
      }
    }
    return moves;
  }

  function greedy(required,seed=new Map(),bounded=true){
    const moves=new Map(seed),loads=loadsOf(moves);
    const relocate=(s,to)=>{
      const from=moves.get(s.id)?.to||s.account_id;
      loads.set(from,loads.get(from)-s.weight);loads.set(to,loads.get(to)+s.weight);
      if(to===s.account_id)moves.delete(s.id);else moves.set(s.id,move(s,to));
    };
    for(const sid of required){
      if(moves.has(sid))continue;
      const s=byId.get(sid);let best=null;
      for(const a of pool){
        if(a.id===s.account_id||!canReceive(s,a.id))continue;
        if(bounded&&!spend())return null;
        relocate(s,a.id);const candidate=new Map(loads);relocate(s,s.account_id);
        if(!best||excessLoad(candidate,target)<excessLoad(best.loads,target)-epsilon||Math.abs(excessLoad(candidate,target)-excessLoad(best.loads,target))<=epsilon&&preferable(candidate,best.loads))best={to:a.id,loads:candidate};
      }
      if(!best)return null;relocate(s,best.to);
    }
    for(const s of movable){
      if(acceptable(loads))break;
      const from=moves.get(s.id)?.to||s.account_id,oldExcess=excessLoad(loads,target);let best=null;
      for(const a of pool){
        if(a.id===from||required.has(s.id)&&a.id===s.account_id||a.id!==s.account_id&&(bounded?!canReceive(s,a.id):target.get(a.id)<=0))continue;
        if(bounded&&!spend())return null;
        relocate(s,a.id);const candidate=new Map(loads);relocate(s,from);
        if(excessLoad(candidate,target)>=oldExcess-epsilon)continue;
        if(!best||excessLoad(candidate,target)<excessLoad(best.loads,target)-epsilon||Math.abs(excessLoad(candidate,target)-excessLoad(best.loads,target))<=epsilon&&preferable(candidate,best.loads))best={to:a.id,loads:candidate};
      }
      if(best)relocate(s,best.to);
    }
    return improveDirection(required,moves);
  }
  function search(required,seed,{optimize=false}={}){
    if(!optimize&&seed&&valid(loadsOf(seed))&&[...required].every(s=>seed.has(s)))return {moves:seed,complete:true};
    const mandatory=movable.filter(s=>required.has(s.id)),optional=movable.filter(s=>!required.has(s.id)),items=[...mandatory,...optional],r=mandatory.length;
    const requiredPrefix=[0],optionalPrefix=[0];for(const s of mandatory)requiredPrefix.push(requiredPrefix.at(-1)+s.weight);for(const s of optional)optionalPrefix.push(optionalPrefix.at(-1)+s.weight);
    let best=seed&&valid(loadsOf(seed))&&[...required].every(s=>seed.has(s))?seed:null;
    let bestLoads=best?loadsOf(best):null;
    const lower=bias(fixed);
    const better=(candidate,loads)=>!best||compareBias(loads,bestLoads)<0||compareBias(loads,bestLoads)===0&&(candidate.size<best.size||candidate.size===best.size&&excessLoad(loads,target)<excessLoad(bestLoads,target)-epsilon);
    for(let count=Math.max(1,r);count<=(best&&compare(bias(bestLoads),lower)===0?best.size:items.length);count++){
      if(!spend())return {moves:best,complete:false};
      const loads=new Map(initial),assigned=new Map(fixed),selected=new Map(),stack=[{start:0,left:count}];
      const leave=()=>{const f=stack.pop();if(f.restore){const {s,to}=f.restore;loads.set(s.account_id,loads.get(s.account_id)+s.weight);loads.set(to,loads.get(to)-s.weight);assigned.set(to,assigned.get(to)-s.weight);selected.delete(s.id);}};
      try{
        while(stack.length){
          const frame=stack.at(-1);
          if(frame.index===undefined){
            if(!spend())return {moves:best,complete:false};
            if(!frame.left){
              if(valid(loads)){
                if(!optimize)return {moves:new Map(selected),complete:true};
                if(better(selected,loads)){best=new Map(selected);bestLoads=new Map(loads);}
              }
              leave();continue;
            }
            const remainingRequired=Math.max(0,r-frame.start),optionalStart=Math.max(0,frame.start-r),optionalCount=frame.left-remainingRequired;
            if(optionalCount<0||optionalStart+optionalCount>optional.length){leave();continue;}
            const capacity=requiredPrefix[r]-requiredPrefix[Math.min(r,frame.start)]+optionalPrefix[optionalStart+optionalCount]-optionalPrefix[optionalStart];
            if(excessLoad(loads,target)>slack+capacity+epsilon||excessLoad(assigned,target)>slack+epsilon){leave();continue;}
            if(optimize&&best){
              const irrevocable=bias(assigned),current=bias(loads);
              const optimistic=current.map((value,i)=>Math.max(irrevocable[i],value-capacity));
              const direction=compare(optimistic,bias(bestLoads));
              if(direction>0||direction===0&&count>best.size){leave();continue;}
            }
            frame.index=frame.start;frame.destination=0;
          }
          const last=frame.start<r?frame.start:items.length-frame.left;
          if(frame.index>last){leave();continue;}
          const index=frame.index,s=items[index],a=pool[frame.destination++];
          if(frame.destination===pool.length){frame.index++;frame.destination=0;}
          if(a.id===s.account_id||!canReceive(s,a.id))continue;
          loads.set(s.account_id,loads.get(s.account_id)-s.weight);loads.set(a.id,loads.get(a.id)+s.weight);assigned.set(a.id,assigned.get(a.id)+s.weight);selected.set(s.id,move(s,a.id));
          stack.push({start:index+1,left:frame.left-1,restore:{s,to:a.id}});
        }
      }finally{while(stack.length)leave();}
    }
    return {moves:best,complete:true};
  }
  const planning={strategy:'heavy-bias-first-v2',tolerance:nightlyTolerance*100,beforeDeviation:beforeExcess/total*100,afterDeviation:beforeExcess/total*100,prioritySessions:[],heavySkipped:[],priorityComplete:true,optimizationComplete:true,directionComplete:true,searchLimited:false,reason:'within_tolerance'};
  const finish=(moves,reason)=>{
    planning.reason=reason;planning.afterDeviation=excessLoad(loadsOf(moves),target)/total*100;planning.withinTolerance=acceptable(loadsOf(moves));planning.beforeLowBudgetExcess=bias(initial).map(x=>x/total*100);planning.afterLowBudgetExcess=bias(loadsOf(moves)).map(x=>x/total*100);
    return {moves:[...moves.values()],planning};
  };
  if(correction&&compareBias(initial,fixed)===0)return finish(new Map(),'within_tolerance');
  const empty=new Set(),baseline=greedy(empty,new Map(),false);
  const impossible=excessLoad(fixed,target)>slack+epsilon||movable.some(s=>pool.every(a=>fixed.get(a.id)+s.weight>target.get(a.id)+slack+epsilon));
  let witness=impossible?{moves:null,complete:true}:search(empty,baseline);
  if(!witness.moves){
    planning.priorityComplete=correction&&witness.complete;planning.optimizationComplete=witness.complete&&correction;planning.directionComplete=correction&&witness.complete;planning.searchLimited=!witness.complete;
    return finish(!correction&&witness.complete&&excessLoad(loadsOf(baseline),target)<beforeExcess-epsilon?baseline:new Map(),witness.complete?(correction?'direction_unavailable':'infeasible'):'search_limited');
  }
  const required=new Set(),released=new Map(pool.map(a=>[a.id,0])),needed=correction?bias(initial).reduce((n,x)=>n+x,0):beforeExcess-slack;
  let covered=0;
  for(const s of movable){
    if(covered>=needed-epsilon)break;
    const surplus=correction&&target.get(s.account_id)===levels.at(-1)?0:Math.max(0,initial.get(s.account_id)-target.get(s.account_id)),oldCredit=Math.min(surplus,released.get(s.account_id));
    if(oldCredit>=surplus-epsilon)continue;
    if(!pool.some(a=>a.id!==s.account_id&&canReceive(s,a.id))){planning.heavySkipped.push({session_id:s.id,reason:'cannot_fit'});continue;}
    let result;
    if(witness.moves.has(s.id))result=witness;
    else{
      const next=new Set(required);next.add(s.id);
      const candidate=greedy(next,witness.moves);
      result=search(next,candidate);
    }
    if(!result.moves){
      planning.heavySkipped.push({session_id:s.id,reason:result.complete?'infeasible':'search_limited'});
      if(!result.complete){planning.priorityComplete=false;planning.optimizationComplete=false;planning.directionComplete=false;planning.searchLimited=true;return finish(new Map(),'search_limited');}
      continue;
    }
    required.add(s.id);planning.prioritySessions.push(s.id);witness=result;
    released.set(s.account_id,released.get(s.account_id)+s.weight);covered+=Math.min(surplus,released.get(s.account_id))-oldCredit;
  }
  if(!correction&&covered<needed-epsilon){planning.priorityComplete=false;planning.optimizationComplete=false;planning.directionComplete=false;return finish(new Map(),'priority_unresolved');}
  const optimized=search(required,improveDirection(required,witness.moves),{optimize:true});planning.optimizationComplete=optimized.complete;planning.directionComplete=optimized.complete||compareBias(loadsOf(optimized.moves),fixed)===0;planning.searchLimited=!optimized.complete;
  return finish(optimized.moves,'balanced');
}
