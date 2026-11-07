import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdir,writeFile,readFile,appendFile,copyFile,chmod,lstat,readlink,readdir} from 'node:fs/promises';
import {join,dirname} from 'node:path';
import {spawn,execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {once} from 'node:events';
import {createConnection,createServer} from 'node:net';
import {postgresBackupContext} from '../postgres-backup-context.mjs';
import {createPeriodicSourceBase} from '../postgres-backup.mjs';
import {publishPeriodicBaseVerification} from '../postgres-periodic-base-verification.mjs';
import {capturePeriodicApplicationBackup} from '../postgres-periodic-application-backup.mjs';
import {backupMachineIdentity} from '../postgres-backup-handoff.mjs';
import {createPeriodicTransferIndex,releasePeriodicTransferIndex} from '../postgres-periodic-transfer-index.mjs';
import {PeriodicSshWire} from '../postgres-periodic-ssh-wire.mjs';
import {servePeriodicSshSource} from '../postgres-periodic-ssh-transfer.mjs';
import {buildPeriodicSshArguments} from '../postgres-periodic-ssh-policy.mjs';

const exec=promisify(execFile),environment={PATH:'/usr/bin:/bin',LANG:'C'},wrapper='/usr/local/lib/harbor/periodic-receive.py',tools='/opt/harbor-storage-tools';
async function poll(check,label,timeoutMs=5000){const deadline=Date.now()+timeoutMs;for(;;){if(await check())return;if(Date.now()>=deadline)throw Error('synthetic_ssh_'+label);await new Promise(resolve=>setImmediate(resolve));}}
async function freePort(){const server=createServer();await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const port=server.address().port;await new Promise(resolve=>server.close(resolve));return port;}
test('real private sshd enforces forced receive, sudo argv, exclusive owner and clean Node environment',{
 skip:process.env.HARBOR_PERIODIC_ROOT_TEST!=='1'||!process.env.HARBOR_TEST_SSHD_BIN?'需自有root mount/network namespace及已核验真实sshd，不用替身冒充':false,timeout:240000
},async t=>{
 assert.equal(process.getuid(),0);assert.notEqual(await readlink('/proc/self/ns/net'),await readlink('/proc/1/ns/net'));await exec('/usr/sbin/ip',['link','set','lo','up']);
 const {createPeriodicRootFixture}=await import('./periodic-root-fixture.mjs'),fixture=await createPeriodicRootFixture({directory:process.env.HARBOR_PERIODIC_ROOT_DIRECTORY,nodeFile:process.execPath,sourceDirectory:new URL('..',import.meta.url).pathname,pgBinDirectory:process.env.HARBOR_CAPACITY_PG_BIN,pgLibraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB});
 const mounted=[];let daemon,daemonExit,first,firstExit,sourceIndex;let daemonLog='';
 t.after(async()=>{
  if(first&&first.exitCode===null){first.stdin.end();await firstExit;}
  if(daemon&&daemon.exitCode===null){try{await poll(async()=>!(await readFile('/proc/'+daemon.pid+'/task/'+daemon.pid+'/children','utf8')).trim(),'session_cleanup');}finally{daemon.kill('SIGTERM');await daemonExit;}}
  for(const path of mounted.reverse())await exec('/bin/umount',[path],{timeout:5000});
  if(sourceIndex)releasePeriodicTransferIndex(sourceIndex);await fixture.close();
 });
 async function tmpfs(path){await exec('/bin/mount',['-t','tmpfs','-o','mode=0755,nosuid,nodev','harbor-private-ssh',path],{timeout:5000});mounted.push(path);}
 await tmpfs('/opt');await tmpfs('/usr/local/lib');await tmpfs('/run');await mkdir('/run/sshd',{mode:0o755});
 await mkdir(join(tools,'node/bin'),{recursive:true,mode:0o755});await mkdir(join(tools,'app'),{mode:0o755});await writeFile(join(tools,'node/bin/node'),'',{mode:0o755});
 for(const [from,to] of [[fixture.sourceDirectory,join(tools,'app')],[process.execPath,join(tools,'node/bin/node')]]){await exec('/bin/mount',['--bind',from,to],{timeout:5000});mounted.push(to);}
 await mkdir(dirname(wrapper),{recursive:true,mode:0o755});await copyFile(new URL('../ci/storage-recovery/periodic-receive.py',import.meta.url),wrapper);await chmod(wrapper,0o644);
 const pam='auth required pam_permit.so\naccount required pam_permit.so\nsession required pam_permit.so\n';await mkdir('/etc/pam.d',{mode:0o755});await writeFile('/etc/pam.d/sudo',pam,{mode:0o644});await writeFile('/etc/pam.d/sshd',pam,{mode:0o644});
 await appendFile('/etc/passwd','sshd:x:65532:65532:synthetic sshd:/run/sshd:/usr/sbin/nologin\nharborrecv:x:65533:65534:synthetic receive:/nonexistent:/bin/sh\n');await appendFile('/etc/group','sshd:x:65532:\n');
 await writeFile('/etc/sudoers','Defaults !lecture\nDefaults env_reset\nDefaults !requiretty\nharborrecv ALL=(root) NOPASSWD: /usr/bin/python3 -I -B '+wrapper+' --root-receive\n',{mode:0o440});await exec('/usr/sbin/visudo',['-c','-f','/etc/sudoers'],{env:environment,timeout:5000});
 const root=fixture.directory,hostKey=join(root,'host-key'),clientKey=join(root,'client-key'),authorized=join(root,'authorized-keys'),known=join(root,'known-hosts'),configFile=join(root,'sshd.conf'),port=await freePort();
 for(const file of [hostKey,clientKey])await exec('/usr/bin/ssh-keygen',['-q','-t','ed25519','-N','','-f',file],{env:environment,timeout:5000});
 await writeFile(authorized,'restrict,command="/usr/bin/python3 -I -B '+wrapper+'" '+(await readFile(clientKey+'.pub','utf8')).trim()+'\n',{mode:0o644});await writeFile(known,'[127.0.0.1]:'+port+' '+(await readFile(hostKey+'.pub','utf8')).trim()+'\n',{mode:0o600});
 await writeFile(configFile,['ListenAddress 127.0.0.1','Port '+port,'HostKey '+hostKey,'PidFile /run/harbor-test-sshd.pid','AuthorizedKeysFile '+authorized,'AllowUsers harborrecv','AuthenticationMethods publickey','PubkeyAuthentication yes','PasswordAuthentication no','KbdInteractiveAuthentication no','PermitRootLogin no','PermitEmptyPasswords no','UsePAM yes','StrictModes yes','UseDNS no','PermitUserEnvironment no','PermitUserRC no','PermitTTY no','DisableForwarding yes','X11Forwarding no','AllowAgentForwarding no','AcceptEnv NODE_OPTIONS PYTHONPATH HARBOR_TEST_NODE','LogLevel ERROR'].join('\n')+'\n',{mode:0o600});
 const sshd=process.env.HARBOR_TEST_SSHD_BIN,sshdEnvironment={...environment,LD_LIBRARY_PATH:process.env.HARBOR_TEST_SSHD_LIB};assert.equal((await lstat(sshd)).uid,0);await exec(sshd,['-t','-f',configFile],{env:sshdEnvironment,timeout:5000});
 daemon=spawn(sshd,['-D','-e','-f',configFile],{env:sshdEnvironment,stdio:['ignore','ignore','pipe']});daemonExit=once(daemon,'exit');daemon.stderr.on('data',bytes=>{if(daemonLog.length<16384)daemonLog+=bytes.toString();});
 await poll(async()=>{assert.equal(daemon.exitCode,null,'private sshd exited: '+daemonLog);return new Promise(resolve=>{const socket=createConnection({host:'127.0.0.1',port});socket.once('connect',()=>{socket.destroy();resolve(true);});socket.once('error',()=>{socket.destroy();resolve(false);});});},'listen');
 const record=postgresBackupContext(fixture.context),objectDirectory=join(root,'object-store'),indexDirectory=join(root,'indices');for(const path of [objectDirectory,indexDirectory])await mkdir(path,{mode:0o700});
 await writeFile('/etc/harbor-storage-periodic-transfer.json',JSON.stringify({version:1,enabled:true,directory:objectDirectory,sourceMachine:await backupMachineIdentity(),expectedCluster:record.expected.system_id,keyVersion:record.keyVersion,archiveKeyFile:record.archiveKeyFile,archiveUid:record.restoreUid,applicationEncryption:record.applicationEncryption}),{mode:0o600});
 const policy={version:1,enabled:true,host:'127.0.0.1',port,user:'harborrecv',identityFile:clientKey,knownHostsFile:known,indexDirectory,timeoutMs:120000,connectTimeoutSeconds:2},args=buildPeriodicSshArguments(policy),cli=new URL('../storage-periodic-transfer.mjs',import.meta.url).pathname;
 await writeFile('/etc/harbor-storage-periodic-ssh.json',JSON.stringify(policy),{mode:0o600});
 const ssh=async parameters=>exec('/usr/bin/ssh',parameters,{env:environment,timeout:10000,maxBuffer:16384}),command=value=>[...args.slice(0,-1),value];
 for(const value of ['id','harbor-periodic-receive extra'])await assert.rejects(ssh(command(value)),error=>error.code===1&&/periodic_receive_command/.test(error.stderr));
 const tty=command('id').filter(value=>value!=='-T');tty.splice(tty.indexOf('--'),0,'-tt');await assert.rejects(ssh(tty),error=>/PTY allocation request failed/.test(error.stderr));
 const forwarding=args.slice(0,-1).map(value=>value==='ClearAllForwardings=yes'?'ClearAllForwardings=no':value);forwarding.splice(forwarding.indexOf('--'),0,'-N','-o','ExitOnForwardFailure=yes','-R','127.0.0.1:0:127.0.0.1:1');await assert.rejects(ssh(forwarding),error=>error.code===255&&/remote port forwarding failed/.test(error.stderr));
 first=spawn('/usr/bin/ssh',args,{env:environment,stdio:['pipe','pipe','pipe']});firstExit=once(first,'exit');first.stdout.resume();first.stderr.resume();
 const locked=async()=>{try{await lstat('/run/harbor-periodic-receive.lock');}catch(error){if(error.code==='ENOENT')return false;throw error;}try{await exec('/usr/bin/flock',['--exclusive','--nonblock','/run/harbor-periodic-receive.lock','/bin/true'],{env:environment,timeout:5000});return false;}catch(error){if(error.code===1)return true;throw error;}};
 await poll(async()=>{assert.equal(first.exitCode,null,'first legal SSH ended before receive lock');return locked();},'first_frame_owner');await assert.rejects(ssh(args),error=>error.code===1&&/periodic_receive_busy/.test(error.stderr));first.stdin.end();await firstExit;await poll(async()=>!await locked(),'owner_release');
 const proof=await createPeriodicSourceBase(fixture.context);await publishPeriodicBaseVerification(proof);const point=await capturePeriodicApplicationBackup(fixture.context,proof,fixture.policy);assert.equal(point.backupComplete,true,point.failureCode);
 const runSend=async()=>JSON.parse((await exec(process.execPath,[cli,'send'],{env:environment,timeout:135000,maxBuffer:8192})).stdout);
 const sent=await runSend();assert.equal(sent.transportComplete,true);for(const key of ['restoreVerified','offhostVerified','deploymentAllowed'])assert.equal(sent[key],false);assert.deepEqual(await readdir(join(objectDirectory,'incoming')),[]);const objectsBefore=await readdir(join(objectDirectory,'objects'));assert.ok(objectsBefore.length>0);assert.equal((await runSend()).transportComplete,true);assert.deepEqual(await readdir(join(objectDirectory,'objects')),objectsBefore);
 // Accepted client environment is deliberately hostile to Node. The forced
 // Python/sudo wrapper must discard it before the fixed Node entry executes.
 sourceIndex=await createPeriodicTransferIndex(fixture.context,proof,point.pointId,{directory:join(indexDirectory,proof.backupId+'--'+point.pointId)});const hostile=[...args];hostile.splice(hostile.indexOf('--'),0,'-o','SetEnv=NODE_OPTIONS=--require=/synthetic-missing-node-entry','-o','SetEnv=PYTHONPATH=/synthetic-foreign','-o','SetEnv=HARBOR_TEST_NODE=/bin/false');
 const peer=spawn('/usr/bin/ssh',hostile,{env:environment,stdio:['pipe','pipe','pipe']}),peerExit=once(peer,'exit'),wire=new PeriodicSshWire(peer.stdout,peer.stdin,{timeoutMs:30000});peer.stderr.resume();try{assert.equal((await servePeriodicSshSource(sourceIndex,wire,{finish:async({complete})=>{if(!complete)peer.kill('SIGKILL');assert.deepEqual(await peerExit,[0,null]);}})).transportComplete,true);}finally{peer.kill('SIGKILL');await peerExit;wire.close();}
 const dependency=join(fixture.sourceDirectory,'pricing-data.mjs'),mode=(await lstat(dependency)).mode&0o777;try{await chmod(dependency,mode|0o020);await assert.rejects(runSend(),error=>/periodic_ssh_process_failed/.test(error.stderr));}finally{await chmod(dependency,mode);}assert.equal((await runSend()).transportComplete,true);
 t.diagnostic(JSON.stringify({actualSsh:true,privateNetworkNamespace:true,forcedCommand:true,actualSudo:true,exclusiveReceiveBeforeFirstFrame:true,hostileEnvironmentDiscarded:true,dependencyWriteRefused:true,offhostVerified:false}));
});
