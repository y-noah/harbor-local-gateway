import {pathToFileURL} from 'node:url';
import {resolve} from 'node:path';

// Preserve read-only boot readiness as a separate result. Authentication,
// signature, configuration and unknown failures never become readiness grants.
export function gatewayEntryStatusFailure(error){
 return new Set(['ECONNREFUSED','ECONNRESET','EPIPE','ENETUNREACH','EHOSTUNREACH','ETIMEDOUT','ENOTFOUND','57P03']).has(error?.code)?'entry_status_unavailable':'entry_status_denied';
}
async function main(){
 if(process.platform!=='linux'||process.getuid()!==0||process.argv.length!==2)throw Error('fixed root only');
 let input='';for await(const chunk of process.stdin){input+=chunk;if(Buffer.byteLength(input)>4096)throw Error('input budget');}
 const request=JSON.parse(input);
 if(!request||Object.keys(request).join(',')!=='image'||typeof request.image!=='string'||!/^[-a-zA-Z0-9_.:/]+:[a-f0-9]{40}$/.test(request.image))throw Error('fixed status input');
 // This is the same CI-verified B implementation, with no action/path supplied
 // by the caller and no business image or permission semantics changed.
 const {runStorageActivationDeployment}=await import('/opt/harbor-storage-tools/app/storage-activation-deploy.mjs');
 process.stdout.write(JSON.stringify(await runStorageActivationDeployment({action:'status',image:request.image}))+'\n');
}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href)main().catch(error=>{
 const code=gatewayEntryStatusFailure(error);process.stderr.write(JSON.stringify({code})+'\n');process.exitCode=code==='entry_status_unavailable'?75:1;
});
