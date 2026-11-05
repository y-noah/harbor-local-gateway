import {readFile,writeFile,mkdtemp,chmod,rm} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {rootCertificates} from 'node:tls';
import {spawn} from 'node:child_process';

export function officialDebianHttpsSources(source,{mirrorOrigin}={}){
  let mirror;
  if(mirrorOrigin!==undefined&&mirrorOrigin!==''){
    try{
      if(typeof mirrorOrigin!=='string')throw Error();
      mirror=new URL(mirrorOrigin);
      if(mirror.protocol!=='https:'||mirror.username||mirror.password||mirror.pathname!=='/'||mirror.search||mirror.hash||![mirror.origin,mirror.origin+'/'].includes(mirrorOrigin))throw Error();
    }catch{throw new Error('ci_debian_mirror_invalid');}
  }
  let count=0;
  const result=source.replace(/^URIs:[^\r\n]*$/gm,line=>{
    const values=line.slice(5).trim().split(/\s+/);
    if(!values[0])throw new Error('ci_debian_sources_invalid');
    const upgraded=values.map(value=>{
      const url=new URL(value);
      const official=url.hostname==='deb.debian.org'&&!url.port,selectedMirror=mirror&&url.origin===mirror.origin;
      if(!['http:','https:'].includes(url.protocol)||!official&&!selectedMirror||url.username||url.password||url.search||url.hash||!['/debian','/debian-security'].includes(url.pathname.replace(/\/$/,'')))throw new Error('ci_debian_sources_invalid');
      url.protocol='https:';
      return mirror?new URL(url.pathname,mirror.origin).href:url.href;
    });
    count++;return 'URIs: '+upgraded.join(' ');
  });
  if(!count)throw new Error('ci_debian_sources_missing');return result;
}

// Await the real child close, including an AbortSignal-induced error.
export function runApt(args,{signal,command='apt-get'}={}){
  return new Promise((resolve,reject)=>{
    const child=spawn(command,args,{stdio:'inherit',signal});let failure;
    child.once('error',error=>{failure=error;});
    child.once('close',code=>{if(failure)reject(failure);else if(code!==0)reject(new Error('ci_apt_command_failed'));else resolve();});
  });
}

export async function installDebianTestDependencies(packages,{sourcesFile='/etc/apt/sources.list.d/debian.sources',temporaryRoot='/tmp',run=runApt,signal,mirrorOrigin}={}){
  if(!Array.isArray(packages)||!packages.length||packages.some(value=>typeof value!=='string'||!/^[a-z0-9][a-z0-9+.-]*$/.test(value)))throw new Error('ci_apt_packages_invalid');
  signal?.throwIfAborted();const original=await readFile(sourcesFile,'utf8'),upgraded=officialDebianHttpsSources(original,{mirrorOrigin});
  const directory=await mkdtemp(join(temporaryRoot,'harbor-ci-apt-')),caFile=join(directory,'roots.pem');
  try{
    // This contains only public Node/Mozilla roots. APT's _apt user must read it.
    await chmod(directory,0o755);await writeFile(caFile,rootCertificates.join('\n')+'\n',{mode:0o644,flag:'wx'});await chmod(caFile,0o644);
    signal?.throwIfAborted();if(upgraded!==original)await writeFile(sourcesFile,upgraded);
    const trust=['-o','Acquire::https::CAInfo='+caFile];
    await run([...trust,'update'],{signal});signal?.throwIfAborted();
    await run([...trust,'install','-y','--no-install-recommends',...new Set(['ca-certificates',...packages])],{signal});
  }finally{await rm(directory,{recursive:true,force:true});}
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  const controller=new AbortController(),handlers=new Map();let stopped;
  for(const name of ['SIGINT','SIGTERM']){const handler=()=>{stopped=name;controller.abort();};handlers.set(name,handler);process.once(name,handler);}
  try{await installDebianTestDependencies(process.argv.slice(2),{signal:controller.signal,mirrorOrigin:process.env.HARBOR_TEST_DEBIAN_MIRROR_ORIGIN});}
  catch{console.error('CI Debian HTTPS依赖安装失败，未关闭TLS或仓库签名校验。');process.exitCode=stopped==='SIGINT'?130:stopped==='SIGTERM'?143:1;}
  finally{for(const [name,handler] of handlers)process.off(name,handler);}
}
