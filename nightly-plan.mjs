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
  const preferable=(loads,other)=>{
    // Equal-budget accounts are peers; do not manufacture a preference from IDs.
    for(const level of levels.slice(0,-1)){
      const excess=x=>pool.reduce((n,a)=>n+(target.get(a.id)===level?Math.max(0,x.get(a.id)-level):0),0);
      const difference=excess(loads)-excess(other);if(Math.abs(difference)>epsilon)return difference<0;
    }
    return excessLoad(loads,target)<excessLoad(other,target)-epsilon;
  };
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
    return moves;
  }
  function search(required,seed,{optimize=false}={}){
    if(!optimize&&seed&&acceptable(loadsOf(seed))&&[...required].every(s=>seed.has(s)))return {moves:seed,complete:true};
    const mandatory=movable.filter(s=>required.has(s.id)),optional=movable.filter(s=>!required.has(s.id)),items=[...mandatory,...optional],r=mandatory.length;
    const requiredPrefix=[0],optionalPrefix=[0];for(const s of mandatory)requiredPrefix.push(requiredPrefix.at(-1)+s.weight);for(const s of optional)optionalPrefix.push(optionalPrefix.at(-1)+s.weight);
    let best=seed&&acceptable(loadsOf(seed))&&[...required].every(s=>seed.has(s))?seed:null;
    const limit=best?best.size:items.length;
    for(let count=Math.max(1,r);count<=limit;count++){
      if(!spend())return {moves:best,complete:false};
      let found=best?.size===count?best:null,foundLoads=found?loadsOf(found):null;
      const loads=new Map(initial),assigned=new Map(fixed),selected=new Map(),stack=[{start:0,left:count}];
      const leave=()=>{const f=stack.pop();if(f.restore){const {s,to}=f.restore;loads.set(s.account_id,loads.get(s.account_id)+s.weight);loads.set(to,loads.get(to)-s.weight);assigned.set(to,assigned.get(to)-s.weight);selected.delete(s.id);}};
      try{
        while(stack.length){
          const frame=stack.at(-1);
          if(frame.index===undefined){
            if(!spend())return {moves:found||best,complete:false};
            if(!frame.left){
              if(acceptable(loads)){
                if(!found||preferable(loads,foundLoads)){found=new Map(selected);foundLoads=new Map(loads);}
                if(!optimize)return {moves:found,complete:true};
              }
              leave();continue;
            }
            const remainingRequired=Math.max(0,r-frame.start),optionalStart=Math.max(0,frame.start-r),optionalCount=frame.left-remainingRequired;
            if(optionalCount<0||optionalStart+optionalCount>optional.length){leave();continue;}
            const capacity=requiredPrefix[r]-requiredPrefix[Math.min(r,frame.start)]+optionalPrefix[optionalStart+optionalCount]-optionalPrefix[optionalStart];
            if(excessLoad(loads,target)>slack+capacity+epsilon||excessLoad(assigned,target)>slack+epsilon){leave();continue;}
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
      if(found)return {moves:found,complete:true};
    }
    return {moves:best,complete:true};
  }
  const planning={strategy:'heavy-first-v1',tolerance:nightlyTolerance*100,beforeDeviation:beforeExcess/total*100,afterDeviation:beforeExcess/total*100,prioritySessions:[],heavySkipped:[],priorityComplete:true,optimizationComplete:true,searchLimited:false,reason:'within_tolerance'};
  const finish=(moves,reason)=>{
    planning.reason=reason;planning.afterDeviation=excessLoad(loadsOf(moves),target)/total*100;planning.withinTolerance=acceptable(loadsOf(moves));
    return {moves:[...moves.values()],planning};
  };
  if(acceptable(initial))return finish(new Map(),'within_tolerance');
  const empty=new Set(),baseline=greedy(empty,new Map(),false);
  const impossible=excessLoad(fixed,target)>slack+epsilon||movable.some(s=>pool.every(a=>fixed.get(a.id)+s.weight>target.get(a.id)+slack+epsilon));
  let witness=impossible?{moves:null,complete:true}:search(empty,baseline);
  if(!witness.moves){
    planning.priorityComplete=false;planning.optimizationComplete=false;planning.searchLimited=!witness.complete;
    return finish(witness.complete&&excessLoad(loadsOf(baseline),target)<beforeExcess-epsilon?baseline:new Map(),witness.complete?'infeasible':'search_limited');
  }
  const required=new Set(),released=new Map(pool.map(a=>[a.id,0])),needed=beforeExcess-slack;
  let covered=0;
  for(const s of movable){
    if(covered>=needed-epsilon)break;
    const surplus=Math.max(0,initial.get(s.account_id)-target.get(s.account_id)),oldCredit=Math.min(surplus,released.get(s.account_id));
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
      if(!result.complete){planning.priorityComplete=false;planning.optimizationComplete=false;planning.searchLimited=true;return finish(new Map(),'search_limited');}
      continue;
    }
    required.add(s.id);planning.prioritySessions.push(s.id);witness=result;
    released.set(s.account_id,released.get(s.account_id)+s.weight);covered+=Math.min(surplus,released.get(s.account_id))-oldCredit;
  }
  if(covered<needed-epsilon){planning.priorityComplete=false;planning.optimizationComplete=false;return finish(new Map(),'priority_unresolved');}
  const optimized=search(required,witness.moves,{optimize:true});planning.optimizationComplete=optimized.complete;planning.searchLimited=!optimized.complete;
  return finish(optimized.moves,'balanced');
}
