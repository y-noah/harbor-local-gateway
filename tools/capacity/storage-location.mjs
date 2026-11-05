import {access,lstat,realpath} from 'node:fs/promises';
import {constants} from 'node:fs';
import {isAbsolute,dirname} from 'node:path';
import {tmpdir} from 'node:os';

// The default retains the old temporary-lab behavior. A selected volume must
// be an existing owner-controlled directory; we never create or chmod it.
export async function capacityStorageDirectory(directory){
 if(directory===undefined)return tmpdir();
 if(typeof directory!=='string'||!isAbsolute(directory))throw Error('capacity_storage_root_invalid');
 try{
  const actual=await realpath(directory),info=await lstat(directory);
  if(actual!==directory||!info.isDirectory()||info.isSymbolicLink()||info.uid!==process.getuid()||(info.mode&0o022)!==0)throw Error();
  // An otherwise private leaf can still be renamed by a writable parent.
  // Root-owned sticky /tmp is safe for this owner's entries; an untrusted
  // directory owner can replace entries even when its sticky bit is set.
  for(let parent=dirname(directory);;parent=dirname(parent)){
   const ancestor=await lstat(parent);if(!ancestor.isDirectory()||ancestor.isSymbolicLink()||![0,process.getuid()].includes(ancestor.uid)||(ancestor.mode&0o022)!==0&&(ancestor.mode&0o1000)===0)throw Error();
   if(parent==='/')break;
  }
  await access(directory,constants.W_OK|constants.X_OK);return directory;
 }catch{throw Error('capacity_storage_root_invalid');}
}
