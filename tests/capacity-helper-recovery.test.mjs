import test from 'node:test';
import assert from 'node:assert/strict';
import https from 'node:https';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID} from 'node:crypto';
import {compileCapacityWindowsHelper} from '../tools/capacity/device-adapter.mjs';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
import {createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
const execute=promisify(execFile),powershell='/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe',quote=value=>"'"+value.replaceAll("'","''")+"'";
const ps=async source=>(await execute(powershell,['-NoProfile','-NonInteractive','-EncodedCommand',Buffer.from(source,'utf16le').toString('base64')],{timeout:15000,maxBuffer:65536})).stdout.trim();

for(const outcome of ['timeout','rejected'])test('Windows prepare '+outcome+' preserves or rolls back only its own CNG identity',{skip:process.env.HARBOR_TEST_WINDOWS_DEVICE!=='1'?'需显式 Windows 实机开关':false,timeout:60000},async()=>{
  const directory=await mkdtemp(join(tmpdir(),'harbor-capacity-helper-recovery-')),runtime=createOwnedRuntime(),sockets=new Set();let server,windowsRoot,state,helper,identity,guardKey;
  try{
    const certificates=await syntheticCertificates(directory);helper=await compileCapacityWindowsHelper(directory);windowsRoot=await ps('[IO.Path]::Combine([IO.Path]::GetTempPath(),'+quote('HarborCapacity-Recovery-'+randomUUID())+')');state=windowsRoot+'\\state.json';
    let heldResponse,enteredResolve;const entered=new Promise(resolve=>{enteredResolve=resolve;});server=https.createServer(certificates.server,(req,res)=>{if(req.url==='/api/device/enroll/challenge'){req.resume();heldResponse=res;enteredResolve();}else{res.writeHead(404);res.end();}});server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
    const ca=(await execute('wslpath',['-w',join(directory,'ca.pem')])).stdout.trim(),child=runtime.start(helper,['prepare','--state',state,'--gateway','https://127.0.0.1:'+server.address().port,'--ca-file',ca,'--port','43219','--allow-software-key'],{role:'helper',stdio:['pipe','pipe','pipe']});child.stdin.end('synthetic-invitation-no-real-credential\n');
    const abort=AbortSignal.timeout(15000);await Promise.race([entered,new Promise((resolve,reject)=>abort.addEventListener('abort',()=>reject(new Error('helper never reached held enrollment')),{once:true}))]);
    identity=JSON.parse(await ps('$s=Get-Content -LiteralPath '+quote(state)+' -Raw|ConvertFrom-Json;$p=[System.Security.Cryptography.CngProvider]::new($s.KeyProvider);[pscustomobject]@{name=$s.KeyName;provider=$s.KeyProvider;exists=[System.Security.Cryptography.CngKey]::Exists($s.KeyName,$p);device=$s.DeviceId;certificate=$s.CertificateThumbprint}|ConvertTo-Json -Compress'));
    assert.equal(identity.exists,true);assert.ok(!identity.device);assert.ok(!identity.certificate);
    guardKey='harbor-capacity-unrelated-'+randomUUID();await ps('$p=[System.Security.Cryptography.CngKeyCreationParameters]::new();$p.Provider=[System.Security.Cryptography.CngProvider]::MicrosoftSoftwareKeyStorageProvider;$k=[System.Security.Cryptography.CngKey]::Create([System.Security.Cryptography.CngAlgorithm]::Rsa,'+quote(guardKey)+',$p);$k.Dispose()');
    if(outcome==='timeout'){await new Promise(resolve=>setTimeout(resolve,100));await runtime.stop(child);await execute(helper,['remove','--state',state],{timeout:30000,maxBuffer:65536});}else{heldResponse.writeHead(400,{'content-type':'application/json'});heldResponse.end(JSON.stringify({error:{code:'synthetic_rejection'}}));const deadline=Date.now()+15000;while(child.exitCode===null&&child.signalCode===null){assert.ok(Date.now()<deadline);await new Promise(resolve=>setTimeout(resolve,20));}}
    const remaining=JSON.parse(await ps('[pscustomobject]@{own=[System.Security.Cryptography.CngKey]::Exists('+quote(identity.name)+',[System.Security.Cryptography.CngProvider]::new('+quote(identity.provider)+'));other=[System.Security.Cryptography.CngKey]::Exists('+quote(guardKey)+',[System.Security.Cryptography.CngProvider]::MicrosoftSoftwareKeyStorageProvider);state=(Test-Path -LiteralPath '+quote(state)+')}|ConvertTo-Json -Compress'));assert.deepEqual(remaining,{own:false,other:true,state:false});
  }finally{
    await runtime.close();for(const socket of sockets)socket.destroy();if(server)await new Promise(resolve=>server.close(resolve));
    let identityCleaned=true;if(state&&helper&&await ps('Test-Path -LiteralPath '+quote(state))==='True')try{await execute(helper,['remove','--state',state],{timeout:30000,maxBuffer:65536});}catch{identityCleaned=false;}
    if(guardKey)await ps('$p=[System.Security.Cryptography.CngProvider]::MicrosoftSoftwareKeyStorageProvider;if([System.Security.Cryptography.CngKey]::Exists('+quote(guardKey)+',$p)){$k=[System.Security.Cryptography.CngKey]::Open('+quote(guardKey)+',$p);try{$k.Delete()}finally{$k.Dispose()}}');
    if(!identityCleaned){const error=new Error('capacity_helper_cleanup_incomplete');error.recoveryDirectory=directory;error.windowsRecoveryDirectory=windowsRoot;throw error;}
    if(windowsRoot)await ps('if(Test-Path -LiteralPath '+quote(windowsRoot)+'){Remove-Item -LiteralPath '+quote(windowsRoot)+' -Recurse -Force}');await rm(directory,{recursive:true,force:true});
  }
});
