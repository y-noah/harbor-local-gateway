import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readlink,chmod,chown,symlink,unlink,rm} from 'node:fs/promises';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {join} from 'node:path';
import {periodicSshPolicy,buildPeriodicSshArguments,loadPeriodicSshPolicy,periodicSshExecutable,periodicSshRemoteCommand} from '../postgres-periodic-ssh-policy.mjs';

const enabled={version:1,enabled:true,host:'backup.example.invalid',user:'harbor_backup',identityFile:'/etc/harbor-backup/ssh.key',knownHostsFile:'/etc/harbor-backup/known_hosts',indexDirectory:'/var/lib/harbor-backup/index'};
test('disabled SSH policy loads no identities and cannot produce an invocation',async()=>{
 assert.deepEqual(periodicSshPolicy({version:1,enabled:false,identityFile:'/missing'}),{version:1,enabled:false});
 assert.throws(()=>buildPeriodicSshArguments({version:1,enabled:false}),{code:'periodic_ssh_disabled'});
 await assert.rejects(loadPeriodicSshPolicy('/tmp/caller-policy.json'),{code:'periodic_ssh_fixed_profile_required'});
 if(process.getuid?.()!==0)await assert.rejects(loadPeriodicSshPolicy(),{code:'periodic_ssh_root_required'});
});
test('SSH uses an explicit pinned identity and host file, fixed executable and fixed command',()=>{
 const policy=periodicSshPolicy(enabled),args=buildPeriodicSshArguments(policy);
 assert.equal(periodicSshExecutable,'/usr/bin/ssh');assert.equal(periodicSshRemoteCommand,'harbor-periodic-receive');assert.ok(Object.isFrozen(policy)&&Object.isFrozen(args));
 assert.deepEqual(args.slice(-3),['--',enabled.host,periodicSshRemoteCommand]);
 for(const setting of ['BatchMode=yes','StrictHostKeyChecking=yes','GlobalKnownHostsFile=/dev/null','UserKnownHostsFile='+enabled.knownHostsFile,'IdentitiesOnly=yes','IdentityAgent=none','PasswordAuthentication=no','KbdInteractiveAuthentication=no','ClearAllForwardings=yes','PermitLocalCommand=no','ProxyCommand=none','ProxyJump=none'])assert.ok(args.includes(setting),setting);
 assert.deepEqual(args.slice(0,3),['-F','/dev/null','-T']);assert.equal(args[args.indexOf('-i')+1],enabled.identityFile);
 for(const host of ['127.0.0.1','2001:db8::1','backup.example.invalid.'])assert.equal(periodicSshPolicy({...enabled,host}).host,host);
});
test('invalid configuration cannot select a command, prompt, proxy, relative path or invalid budget',()=>{
 for(const extra of [{command:'sh'},{sshExecutable:'/bin/sh'},{host:'-oProxyCommand=sh'},{host:'host\nother'},{user:'root@other'},{port:0},{port:65536},{timeoutMs:999},{timeoutMs:3600001},{connectTimeoutSeconds:0},{timeoutMs:1000,connectTimeoutSeconds:2},{identityFile:'relative'},{indexDirectory:'/'},{knownHostsFile:enabled.identityFile},{identityFile:enabled.indexDirectory+'/key'},{identityFile:'/etc/../secret'},{knownHostsFile:'/etc/key\0tail'}])assert.throws(()=>periodicSshPolicy({...enabled,...extra}),{code:'periodic_ssh_policy_invalid'});
 assert.throws(()=>periodicSshPolicy({version:1,enabled:false,command:'sh'}),{code:'periodic_ssh_policy_invalid'});
 for(const name of ['identityFile','knownHostsFile'])for(const file of ['/etc/key with space','/etc/key%h','/etc/key${USER}','/etc/"key"','/etc/key\\tail'])assert.throws(()=>periodicSshPolicy({...enabled,[name]:file}),{code:'periodic_ssh_policy_invalid'});
});

test('private root namespace validates real fixed configuration and protected identity files',{
 skip:process.env.HARBOR_PERIODIC_SSH_POLICY_ROOT_TEST!=='1'?'需显式启用独立root挂载namespace；不改宿主/etc':false,timeout:30000
},async()=>{
 assert.equal(process.getuid(),0);assert.notEqual(await readlink('/proc/self/ns/mnt'),await readlink('/proc/1/ns/mnt'));
 const run=promisify(execFile);await run('/usr/bin/mount',['--make-rprivate','/']);const root=await mkdtemp('/var/lib/harbor-ssh-policy-'),etc=join(root,'etc'),trust=join(root,'trust'),indexes=join(root,'indexes');let mounted=false;
 try{
  await mkdir(etc,{mode:0o755});await mkdir(trust,{mode:0o700});await mkdir(indexes,{mode:0o700});await writeFile(join(etc,'passwd'),'root:x:0:0:synthetic:/root:/bin/sh\n',{mode:0o644});await writeFile(join(etc,'group'),'root:x:0:\n',{mode:0o644});await run('/usr/bin/mount',['--bind',etc,'/etc']);mounted=true;
  assert.deepEqual(await loadPeriodicSshPolicy(),{version:1,enabled:false});
  const file='/etc/harbor-storage-periodic-ssh.json',identity=join(trust,'identity'),hosts=join(trust,'known_hosts'),configuration={...enabled,identityFile:identity,knownHostsFile:hosts,indexDirectory:indexes};
  const save=value=>writeFile(file,JSON.stringify(value),{mode:0o600});
  await save({version:1,enabled:false,identityFile:'/missing-key'});assert.deepEqual(await loadPeriodicSshPolicy(),{version:1,enabled:false});
  await writeFile(identity,'synthetic identity; never used to authenticate',{mode:0o600});await writeFile(hosts,'synthetic host pin; no SSH connection',{mode:0o644});await save(configuration);
  assert.equal((await loadPeriodicSshPolicy()).host,enabled.host);
  const expanded=await run(periodicSshExecutable,['-G',...buildPeriodicSshArguments(await loadPeriodicSshPolicy())],{timeout:5000,maxBuffer:1048576}),settings=new Map(expanded.stdout.trim().split('\n').map(line=>{const at=line.indexOf(' ');return [line.slice(0,at),line.slice(at+1)];}));
  assert.equal(settings.get('batchmode'),'yes');assert.ok(['yes','true'].includes(settings.get('stricthostkeychecking')));assert.equal(settings.get('checkhostip'),'no');assert.equal(settings.get('updatehostkeys'),'false');assert.equal(settings.get('userknownhostsfile'),hosts);assert.equal(settings.get('globalknownhostsfile'),'/dev/null');assert.equal(settings.get('identityfile'),identity);assert.equal(settings.get('identityagent'),'none');assert.equal(settings.get('passwordauthentication'),'no');
  await chmod(identity,0o644);await assert.rejects(loadPeriodicSshPolicy(),{code:'periodic_ssh_identity_untrusted'});await chmod(identity,0o600);
  await chmod(hosts,0o666);await assert.rejects(loadPeriodicSshPolicy(),{code:'periodic_ssh_known_hosts_untrusted'});await chmod(hosts,0o644);
  await chmod(indexes,0o755);await assert.rejects(loadPeriodicSshPolicy(),{code:'periodic_ssh_index_untrusted'});await chmod(indexes,0o700);
  await chmod(trust,0o777);await assert.rejects(loadPeriodicSshPolicy(),{code:'periodic_ssh_policy_path_untrusted'});await chmod(trust,0o700);
  await chown(file,65534,65534);await assert.rejects(loadPeriodicSshPolicy(),{code:'periodic_ssh_policy_untrusted'});await chown(file,0,0);
  await save({...configuration,identityFile:join(trust,'link')});await symlink(identity,join(trust,'link'));await assert.rejects(loadPeriodicSshPolicy(),{code:'periodic_ssh_identity_untrusted'});await unlink(join(trust,'link'));
  await save({...configuration,identityFile:join(trust,'missing')});await assert.rejects(loadPeriodicSshPolicy(),{code:'periodic_ssh_identity_missing'});
  await save(configuration);assert.equal((await loadPeriodicSshPolicy()).enabled,true);
 }finally{if(mounted)await run('/usr/bin/umount',['/etc']);await rm(root,{recursive:true,force:false});}
});
