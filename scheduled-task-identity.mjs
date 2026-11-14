// Recovery identities preserve immutable jobs written before per-slot expiry.
// The suffix is a persisted format version, not a second execution schedule.
export const scheduledDependencyContract='per-slot-v2';
export const scheduledRecoverySuffix=':schedule-v2';
// One v1 daily prerequisite -> v2 slot prerequisite transition per original job.
// The original ID already fixes slot/revision, and v2 dependency is that slot's
// immutable boundary. Config edits take effect after that boundary, never alter
// its dependency, so this format does not need a per-save recovery generation.
export const scheduledRecoverySql=column=>column+"||'"+scheduledRecoverySuffix+"'";
export const scheduledIdentitySql=(expected,actual)=>actual+' IN ('+expected+','+scheduledRecoverySql(expected)+')';
export const effectiveScheduledJobSql=column=>'COALESCE((SELECT id FROM harbor.background_jobs WHERE id='+scheduledRecoverySql(column)+'),'+column+')';
export const scheduledRecoveryId=id=>id+scheduledRecoverySuffix;
export const originalScheduledId=id=>id.endsWith(scheduledRecoverySuffix)?id.slice(0,-scheduledRecoverySuffix.length):id;
export const sameScheduledIdentity=(expected,actual)=>actual===expected||actual===scheduledRecoveryId(expected);
export function priorDailyExpiryBoundary(at){
 const day=86400000,offset=8*3600000,point=Math.floor((at+offset)/day)*day-offset+2*3600000;
 return point<=at?point:point-day;
}
