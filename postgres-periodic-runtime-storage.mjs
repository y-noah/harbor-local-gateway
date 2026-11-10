import {transferError} from './migration-artifacts.mjs';

// Only fixed categories cross the private child IPC boundary. Never include
// driver messages, filesystem paths, report payloads or connection identities.
const reasons=new Set([
  'not_checked','not_configured','healthy','low','unknown','stopped','clock_reversed',
  'stale','report_stale','monitor_timeout','probe_failed','binding_unverified','capture_unavailable',
  'database_transport_not_local','database_namespace_mismatch','database_version_mismatch',
  'database_control_invalid','database_cluster_mismatch','database_relations_unverified',
  'filesystem_size_unrepresentable',
  'recording_storage_report_invalid','recording_storage_report_key_invalid',
  'recording_storage_report_signature','recording_storage_report_identity',
  'recording_storage_report_stale','recording_storage_report_local_paths',
  'recording_storage_report_unavailable'
]);
const category=(value,fallback)=>reasons.has(value)?value:fallback;

export function periodicRuntimeStorageStatus(monitor){
  const status=monitor.status(),storage=status.binding?.verified===true,captureAllowed=status.captureAllowed===true;
  return {storage,captureAllowed,storageReason:!storage
    ?category(status.binding?.reason,'binding_unverified')
    :!captureAllowed?category(status.diagnostics?.reason,'capture_unavailable'):null};
}

export function assertPeriodicRuntimeStorage(role,status){
  if(!['gateway','worker'].includes(role))throw transferError('periodic_application_runtime_storage');
  if(status?.storage===true&&status.captureAllowed===true)return;
  const reason=category(status?.storageReason,status?.storage===true?'capture_unavailable':'binding_unverified');
  throw transferError('periodic_application_runtime_storage_'+role+'_'+reason);
}
