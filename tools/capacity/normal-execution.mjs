import {streamingStageReport} from './streaming-stage-report.mjs';
import {randomUUID} from 'node:crypto';
import {ownedCapacityTarget} from './gateway-adapter.mjs';
import {isOwnedObservationScope} from './observation-adapter.mjs';

const verdict=values=>values.some(value=>value===false)?false:values.length&&values.every(value=>value===true)?true:null;

// Every scene gets a new scope after real authentication. Qualification runs
// only after those scopes have closed; its auxiliary requests cannot become
// unattributed arrivals inside the prescribed normal management mix.
export async function createNormalExecution({target,profile='selfcheck',enabled=true}={}){
  if(!enabled){if(profile==='formal')throw new Error('capacity_normal_observation_required');return {target,close:async()=>{}};}
  const initial=ownedCapacityTarget(target,{profile}),required=['prepareActorEpoch','configureScenario','createObservationScope','collectBaseEvidence','collectEvidenceForScope','runQualificationProbes'];
  if(required.some(key=>typeof initial[key]!=='function'))throw new Error('capacity_normal_scope_contract_incomplete');
  const scenes=[];let active=null,closed=false;
  const closeActive=async({signal,observeReadiness=false}={})=>{if(!active)return;const current=active;active=null;try{if(observeReadiness)await current.scope.waitForDataReadiness({signal});}finally{await current.scope.close();}};
  const assertActive=()=>{if(closed||!active)throw new Error('capacity_normal_scope_not_active');return active.scope;};
  const observed={...target,
    get employeeActors(){return target.employeeActors;},get adminActors(){return target.adminActors;},
    sendModel:event=>assertActive().sendModel(event),sendAdmin:event=>assertActive().sendAdmin(event),onResult:event=>assertActive().onResult(event),
    onScenarioStart:async({scene,signal})=>{
      if(closed||active)throw new Error('capacity_normal_scope_lifecycle');
      await ownedCapacityTarget(target).prepareActorEpoch({phase:'normal-scenario',signal});
      const owned=ownedCapacityTarget(target),scope=await owned.createObservationScope({id:'normal-'+randomUUID(),kind:'normal',scenarioId:scene.id});
      if(!isOwnedObservationScope(scope))throw new Error('capacity_normal_scope_not_owned');
      active={id:scene.id,scope,epoch:owned.epoch};scenes.push(active);
      try{await owned.configureScenario({scene,signal});}catch(error){await closeActive();throw error;}
    },
    onScenarioEnd:options=>closeActive({...options,observeReadiness:true}),
    runQualificationProbes:async options=>{await closeActive();return ownedCapacityTarget(target).runQualificationProbes(options);},
    collectEvidence:async()=>{
      const owned=ownedCapacityTarget(target),base=await owned.collectBaseEvidence(),rows=[];
      for(const scene of scenes)rows.push({id:scene.id,epoch:scene.epoch,evidence:await owned.collectEvidenceForScope(scene.scope)});
      const scopes=rows.map(row=>row.evidence.scopeEvidence),data=scopes.map(row=>row.dataReconciliation),readiness=scopes.map(row=>row.dataReadiness?.passed??null),transport=scopes.map(row=>row.transport),counts={accepted:0,dispatched:0,persisted:0};
      let knownCounts=data.length>0;for(const row of data)for(const key of Object.keys(counts)){const count=row.counts?.[key];if(!Number.isSafeInteger(count)||count<0||!Number.isSafeInteger(counts[key]+count))knownCounts=false;else counts[key]+=count;}
      const streamingStages=streamingStageReport(rows.map(row=>({id:row.id,evidence:row.evidence.streamingStages})));
      const integrity=verdict([base.metricIntegrity?.passed,...transport.map(row=>row.passed)]),arrivals=verdict(transport.flatMap(row=>[row.passed,row.mix?.alignedToPlannedCounts]));
      return {...base,streamingStages,...Object.fromEntries(['requestStages','streamRelay','cancelRelease','bodyVisibility'].map(key=>[key,streamingStages[key]])),dataReconciliation:{passed:verdict([...data.map(row=>row.passed),knownCounts?true:null]),counts:knownCounts?counts:null,scenarios:rows.map((row,index)=>({id:row.id,...data[index]}))},dataReadiness:{passed:verdict(readiness),scope:'per-scene-normal',provesFormalTimeliness:false,scenarios:rows.map((row,index)=>({id:row.id,...scopes[index].dataReadiness}))},metricIntegrity:{...base.metricIntegrity,passed:integrity},serverArrivals:{passed:arrivals,scope:'per-scene-normal',scenarios:rows.map((row,index)=>({id:row.id,epoch:row.epoch,...transport[index]}))},normalObservation:{passed:verdict([integrity,arrivals,knownCounts?true:null,...data.map(row=>row.passed),...readiness,profile==='formal'?streamingStages.passed:streamingStages.passed===false?false:true]),scenarios:rows.map(row=>({id:row.id,epoch:row.epoch,scopeEpoch:row.evidence.scopeEpoch})),qualificationOutsideNormalScopes:true}};
    }
  };
  return {target:observed,close:async()=>{if(closed)return;closed=true;const results=await Promise.allSettled(scenes.map(row=>row.scope.close()));active=null;if(results.some(row=>row.status==='rejected'))throw new Error('capacity_normal_scope_cleanup_failed');}};
}
