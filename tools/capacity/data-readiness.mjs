import {performance} from 'node:perf_hooks';
import {targets} from './scenarios.mjs';
// One MVCC snapshot observes source recording and its matching derived revision.
// These columns contain state/timing only; no source or derived body is selected.
export const capacityDataReadinessSql=`SELECT r.id request_id,r.status,r.ended,r.recording_state,c.persisted_sequence,c.updated recording_updated,c.expires_at,c.turn_state,w.revision,w.indexed_revision,w.indexed_at,w.last_error,d.source_revision,d.source_sequence,d.state document_state,d.turn_state document_turn_state,d.expires_at document_expires_at FROM harbor.requests r LEFT JOIN harbor.recordings c ON c.request_id=r.id LEFT JOIN harbor.recording_work w ON w.request_id=r.id LEFT JOIN harbor.recording_documents d ON d.request_id=r.id WHERE r.id=ANY($1::text[])`;
const finite=value=>typeof value==='number'&&Number.isFinite(value);
export function classifyCapacityDataReadiness({row,finishedAt,observedAt=performance.now(),timeOrigin=performance.timeOrigin}={}){
  if(!finite(finishedAt)||finishedAt<0||finishedAt>observedAt)return {state:'unknownFinishedAt',deadline:null};
  const deadline=finishedAt+targets.bodyVisibility.p99,result=state=>({state,deadline});
  if(row?.recording_state==='expired'||finite(row?.expires_at)&&row.expires_at<=timeOrigin+observedAt)return result('expired');
  if(['partial','truncated'].includes(row?.recording_state))return result('partial');
  if(row?.last_error||row&&row.status!=='ok'&&!['queued','running'].includes(row.status)||['failed','disabled','historical'].includes(row?.recording_state))return result('failed');
  const sourceReady=row?.status==='ok'&&row.recording_state==='complete'&&Number.isSafeInteger(row.persisted_sequence)&&row.persisted_sequence>0&&Number.isSafeInteger(row.revision)&&row.revision>0&&row.indexed_revision>=row.revision&&row.source_revision===row.revision&&row.source_sequence===row.persisted_sequence&&row.document_state===row.recording_state&&row.document_turn_state===row.turn_state&&row.document_expires_at===row.expires_at;
  if(sourceReady&&[row.indexed_at,row.recording_updated,row.ended].every(finite)){
    const sourceReadyAt=Math.max(row.indexed_at,row.recording_updated,row.ended)-timeOrigin;
    return {...result('ready'),readyObservedAt:observedAt,sourceReadyAt,sourceMarkerAfterDeadline:sourceReadyAt>deadline,timelinessUnknown:observedAt>deadline};
  }
  return result(observedAt>=deadline?'timedOut':'pending');
}

export function throwCapacityReadinessAborted(signal){if(signal?.aborted)throw new DOMException('Capacity readiness cancelled','AbortError');}
