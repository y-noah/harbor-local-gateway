import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {compileCapacityWindowsHelper} from '../tools/capacity/device-adapter.mjs';
const execute=promisify(execFile),compiler='/mnt/c/Windows/Microsoft.NET/Framework64/v4.0.30319/csc.exe';
test('production helper compilation excludes the capacity-only private enrollment protocol',{skip:process.env.HARBOR_TEST_WINDOWS_DEVICE!=='1',timeout:60000},async()=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-helper-observation-build-'));try{const windowsPath=async path=>(await execute('wslpath',['-w',path])).stdout.trim(),source=fileURLToPath(new URL('../clients/harbor-device/HarborDevice.cs',import.meta.url)),production=join(directory,'production.exe');await execute(compiler,['/nologo','/target:exe','/out:'+await windowsPath(production),'/r:System.Net.Http.dll','/r:System.Management.dll','/r:System.Web.Extensions.dll',await windowsPath(source)],{timeout:30000,maxBuffer:65536});const observed=await compileCapacityWindowsHelper(directory),marker=Buffer.from('capacity_trace_handshake_failed','utf16le');assert.equal((await readFile(production)).includes(marker),false);assert.equal((await readFile(observed)).includes(marker),true);const deliveryMarker=Buffer.from('helper-socket-write-complete-v1','utf16le');assert.equal((await readFile(production)).includes(deliveryMarker),false);assert.equal((await readFile(observed)).includes(deliveryMarker),true);
 }finally{await rm(directory,{recursive:true,force:true});}
});
