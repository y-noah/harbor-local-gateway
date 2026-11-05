import {mkdir,writeFile,readFile,lstat,realpath} from 'node:fs/promises';
import {join,resolve,relative} from 'node:path';
import {createHash,randomBytes} from 'node:crypto';
import {isOwnedLaboratory,isOwnedMaterialRoot} from './owned-runtime.mjs';
import {PostgresStore} from '../../postgres-store.mjs';
const owners=new WeakMap();

export function capacityCredentialProof(headers){const value=new Headers(headers);return createHash('sha256').update((value.get('authorization')??'')+'\0'+(value.get('chatgpt-account-id')??'')).digest('hex');}
const digest=bytes=>createHash('sha256').update(bytes).digest('hex');
async function privateDirectory(path){try{const info=await lstat(path);return info.isDirectory()&&!info.isSymbolicLink()&&await realpath(path)===resolve(path)&&(!process.getuid||info.uid===process.getuid()&&(info.mode&0o077)===0);}catch{return false;}}
async function materialDirectories(laboratory,materialRoot,runtimeDir){
  const labPath=resolve(laboratory.directory),root=resolve(materialRoot.path),runtime=resolve(runtimeDir);if(!root.startsWith(labPath+'/')||runtime!==root&&!runtime.startsWith(root+'/'))throw new Error('capacity_account_material_not_owned');const directories=[labPath,root];let current=root;for(const part of relative(root,runtime).split('/').filter(Boolean)){current=join(current,part);directories.push(current);}for(const path of directories)if(!await privateDirectory(path))throw new Error('capacity_account_material_not_owned');return directories;
}

// These are synthetic credentials accepted only by the owned HTTP upstream.
// They prove file restoration and credential loading, never a real login.
export async function prepareCapacityAccountMaterials({store,runtimeDir,laboratory,materialRoot}={}){
  if(!isOwnedLaboratory(laboratory)||!isOwnedMaterialRoot(laboratory,materialRoot))throw new Error('capacity_account_material_not_owned');if(store.pool?.options.host!==laboratory.connection.host||Number(store.pool?.options.port)!==laboratory.connection.port)throw new Error('capacity_account_material_connection_mismatch');const directories=await materialDirectories(laboratory,materialRoot,runtimeDir);const observer=new PostgresStore({connection:laboratory.connection});let cluster;try{cluster=(await observer.get('SELECT system_identifier::text id FROM pg_control_system()')).id;}finally{await observer.close();}if((await store.get('SELECT system_identifier::text id FROM pg_control_system()')).id!==cluster)throw new Error('capacity_account_material_cluster_mismatch');
  const accounts=await store.all("SELECT id FROM harbor.accounts WHERE auth_mode='subscription' AND kind='codex' AND removed_at IS NULL ORDER BY id"),directory=join(resolve(runtimeDir),'accounts');await mkdir(directory,{recursive:true,mode:0o700});if(!await privateDirectory(directory))throw new Error('capacity_account_material_not_owned');directories.push(directory);const expected=[];
  for(const account of accounts){
    if(!/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(account.id))throw new Error('capacity_account_material_identity_invalid');
    const home=join(directory,account.id);await mkdir(home,{mode:0o700});const payload=Buffer.from(JSON.stringify({exp:Math.floor(Date.now()/1000)+7*86400,sub:'capacity-synthetic-only'})).toString('base64url'),token='capacity-synthetic.'+payload+'.'+randomBytes(24).toString('base64url'),bytes=Buffer.from(JSON.stringify({tokens:{access_token:token,account_id:account.id,refresh_token:'synthetic-unused-no-real-refresh'}}));
    await writeFile(join(home,'auth.json'),bytes,{mode:0o600,flag:'wx'});await store.run('UPDATE harbor.accounts SET home=$1 WHERE id=$2',home,account.id);expected.push(Object.freeze({accountId:account.id,home,fileDigest:digest(bytes),credentialProof:capacityCredentialProof({authorization:'Bearer '+token,'chatgpt-account-id':account.id})}));
  }
  Object.freeze(expected);owners.set(expected,{laboratory,materialRoot,cluster,directories:Object.freeze(directories)});return expected;
}
export async function verifyCapacityAccountMaterials({store,expected}={}){
  const ownership=owners.get(expected);if(!ownership||!isOwnedLaboratory(ownership.laboratory)||!isOwnedMaterialRoot(ownership.laboratory,ownership.materialRoot)||!expected.length)return false;for(const path of ownership.directories)if(!await privateDirectory(path))return false;if((await store.get('SELECT system_identifier::text id FROM pg_control_system()')).id!==ownership.cluster)return false;
  for(const entry of expected){const row=await store.get('SELECT home FROM harbor.accounts WHERE id=$1 AND removed_at IS NULL',entry.accountId);if(row?.home!==entry.home)return false;const path=join(entry.home,'auth.json');let info,bytes;try{if(!await privateDirectory(entry.home))return false;info=await lstat(path);if(!info.isFile()||info.isSymbolicLink()||info.size>1048576||process.getuid&&(info.uid!==process.getuid()||(info.mode&0o077)!==0))return false;bytes=await readFile(path);}catch{return false;}if(digest(bytes)!==entry.fileDigest)return false;}
  return true;
}
