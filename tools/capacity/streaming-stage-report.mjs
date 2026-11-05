const fields=['requestStages','streamRelay','cancelRelease','bodyVisibility'];
const verdict=values=>values.some(value=>value===false)?false:values.length&&values.every(value=>value===true)?true:null;
const knownFailure=evidence=>evidence?.passed===false||evidence?.runtime?.hardFailures>0||evidence?.issueCount>0;
export function streamingStageReport(rows=[]){
 const applicable=rows.filter(row=>(row.evidence?.counts?.started??0)>0),failed=rows.some(row=>knownFailure(row.evidence));
 return {passed:failed?false:verdict(applicable.map(row=>row.evidence.passed)),scope:'private-owned-streaming-stages',applicable:applicable.length>0,scenarios:rows.map(({id,evidence})=>({id,evidence})),...Object.fromEntries(fields.map(field=>[field,{passed:failed?false:verdict(applicable.map(row=>row.evidence[field]?.passed??null)),scenarios:applicable.map(({id,evidence})=>({id,...evidence[field]}))}]))};
}
// Missing formal transport/topology is retained as evidence in selfcheck, while
// a known source/data mismatch must still fail the existing qualification.
export function streamingStageQualification(evidence,{profile='selfcheck'}={}){
 if(!evidence)return {failed:false,missing:profile==='formal'};const applicable=(evidence.counts?.started??0)>0;
 return {failed:knownFailure(evidence),missing:applicable&&profile==='formal'&&evidence.passed!==true};
}
