import {readdir,lstat,readFile,readlink} from 'node:fs/promises';
import {transferError} from './migration-artifacts.mjs';
const fail=code=>{throw transferError(code);};
async function processRows(){
 const rows=new Map();for(const name of await readdir('/proc')){if(!/^[1-9]\d*$/.test(name))continue;try{const directory='/proc/'+name;const uid=(await lstat(directory)).uid,text=await readFile(directory+'/stat','utf8'),fields=text.slice(text.lastIndexOf(')')+2).split(' ');rows.set(Number(name),{pid:Number(name),uid,parent:Number(fields[1]),state:fields[0],start:fields[19]});}catch(error){if(!['ENOENT','ESRCH'].includes(error.code))throw error;}}
 return rows;
}
async function rejectUnknownDirectoryUse(protectedRoot,rows,known=new Set()){
 if(protectedRoot)for(const row of rows.values())if(row.state!=='Z'&&!known.has(row.pid)){try{const cwd=(await readlink('/proc/'+row.pid+'/cwd')).replace(/ \(deleted\)$/,'');if(cwd===protectedRoot||cwd.startsWith(protectedRoot+'/'))fail('backup_restore_process_identity_unknown');}catch(error){if(!['ENOENT','ESRCH','EACCES','EPERM'].includes(error.code))throw error;}}
}
export async function assertPostgresRestoreDirectoryUnused(root){await rejectUnknownDirectoryUse(root,await processRows());}
// Observation only: no supplied record can mint a backup/restore capability.
export async function restoreOwnedProcesses(owner,{checkUnknown=true}={}){
 const rows=await processRows();
 const root=rows.get(owner.pid);if(root&&root.uid===owner.uid&&root.state!=='Z'&&!owner.start){const env=(await readFile('/proc/'+owner.pid+'/environ')).toString().split('\0');if(!env.includes('HARBOR_BACKUP_RESTORE_TOKEN='+owner.nonce))fail('backup_restore_process_identity_unknown');owner.start=root.start;owner.members.set(root.pid,root.start);}
 const known=new Set();for(const [pid,start] of owner.members){const row=rows.get(pid);if(row?.start===start&&row.uid===owner.uid)known.add(pid);else owner.members.delete(pid);}let changed=true;while(changed){changed=false;for(const row of rows.values())if(row.uid===owner.uid&&!known.has(row.pid)&&known.has(row.parent)){owner.members.set(row.pid,row.start);known.add(row.pid);changed=true;}}
 // PostgreSQL backends create their own sessions and overwrite argv/environ.
 // Kernel parentage and start time retain ownership across that transition.
 // A process merely using the private PGDATA is unknown, never a kill target.
 if(checkUnknown)await rejectUnknownDirectoryUse(owner.protectedRoot??owner.data,rows,known);
 return [...known].map(pid=>rows.get(pid)).filter(row=>row.state!=='Z');
}
