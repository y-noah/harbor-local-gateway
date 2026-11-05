import {open,lstat,realpath,readdir} from 'node:fs/promises';
import {constants} from 'node:fs';
import {dirname,resolve} from 'node:path';
import {transferError} from './migration-artifacts.mjs';
const fail=code=>{throw transferError(code);};
export async function verifyReceiverProtectedRecoveryDirectory(path,gid){
 if(process.getuid()!==0||typeof path!=='string'||resolve(path)!==path||await realpath(path)!==path)fail('backup_receiver_recovery_root_untrusted');for(let at=path;;at=dirname(at)){const info=await lstat(at);if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==0||info.mode&0o022)fail('backup_receiver_recovery_root_untrusted');if(at===path&&(info.gid!==gid||(info.mode&0o777)!==0o750))fail('backup_receiver_recovery_root_untrusted');if(at==='/')break;}
}
// The root-protected ancestor prevents the service UID from replacing the root
// directory. Every descendant is then opened relative to a held directory FD;
// neither a substituted symlink nor a later rename can redirect fchown.
export async function handoffReceiverRestoredTree(path,{uid,gid,keepRoot=false,signal}={}){
 if(process.getuid()!==0||!Number.isSafeInteger(uid)||uid<1||!Number.isSafeInteger(gid)||gid<1)fail('backup_receiver_handoff_root_required');
 async function visit(name,top=false){signal?.throwIfAborted();const file=await open(name,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);try{const info=await file.stat();if(top&&(!info.isDirectory()||info.uid!==0)||!info.isFile()&&!info.isDirectory()||!info.isDirectory()&&info.nlink!==1||info.uid!==0&&info.uid!==uid)fail('backup_receiver_handoff_file_invalid');if(info.isDirectory())for(const child of await readdir('/proc/self/fd/'+file.fd))await visit('/proc/self/fd/'+file.fd+'/'+child);signal?.throwIfAborted();await file.chown(top&&keepRoot?0:uid,gid);if(top&&keepRoot)await file.chmod(0o750);}finally{await file.close();}}
 await visit(path,true);
}
export async function writeReceiverRestoredAutoConfiguration(path,uid){const file=await open(path,constants.O_WRONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);try{const info=await file.stat();if(!info.isFile()||info.uid!==uid||info.nlink!==1)fail('backup_receiver_handoff_file_invalid');await file.truncate(0);await file.writeFile('\n');}finally{await file.close();}}
