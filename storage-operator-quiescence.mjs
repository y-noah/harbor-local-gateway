import {setTimeout as delay} from 'node:timers/promises';
import {performance} from 'node:perf_hooks';
import {transferError} from './migration-artifacts.mjs';

// No application name, role name or other caller-supplied identity is exempted.
// The managed publisher closes its connection after each report; wait for the
// same zero-other-client condition used by the original promotion check.
export async function assertStorageOperatorTargetIdle(store,{allowWait=false,deadline,signal,pollMs=50}={}){
  if(typeof allowWait!=='boolean'||!Number.isSafeInteger(pollMs)||pollMs<1||pollMs>1000||allowWait&&(!Number.isFinite(deadline)||deadline-performance.now()>120000))throw transferError('storage_operator_quiescence_configuration');
  for(;;){
    signal?.throwIfAborted();
    if(allowWait){
      if(deadline<=performance.now())throw transferError('storage_operator_target_has_other_connections');
      // pg_stat_activity caches a transaction's statistics snapshot. Without
      // clearing it, a connection that really closed would appear to persist.
      await store.query('SELECT pg_stat_clear_snapshot()');
    }
    const foreign=await store.get("SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND backend_type='client backend' AND pid<>pg_backend_pid() LIMIT 1");
    signal?.throwIfAborted();if(!foreign)return;
    if(!allowWait||deadline<=performance.now())throw transferError('storage_operator_target_has_other_connections');
    await delay(Math.min(pollMs,Math.max(1,deadline-performance.now())),undefined,{signal});
  }
}
