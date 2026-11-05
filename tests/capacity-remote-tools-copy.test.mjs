import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {fileURLToPath} from 'node:url';
const execute=promisify(execFile),module=fileURLToPath(new URL('../tools/capacity/runtime-extract.py',import.meta.url));
const script=String.raw`import importlib.util,pathlib,tempfile,os,json,hashlib,stat,shutil,sys
spec=importlib.util.spec_from_file_location('actual_capacity_runtime',sys.argv[1]);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
mode=sys.argv[2]
if mode=='fd-bound':
 import resource
 resource.setrlimit(resource.RLIMIT_NOFILE,(64,64))
with tempfile.TemporaryDirectory(prefix='harbor-remote-tools-component-') as temporary:
 base=pathlib.Path(temporary);root=base/'root';(root/'app').mkdir(parents=True);source=base/'source';source.write_bytes(b'component-only-browser-source');source.chmod(0o600)
 item={'name':'modules/browser.mjs','source':str(source),'sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'bytes':source.stat().st_size,'executable':False}
 if mode=='executable':item['executable']=True
 if mode=='escape':item['name']='../escape'
 if mode=='writable':source.chmod(0o666)
 if mode=='symlink':link=base/'link';link.symlink_to(source);item['source']=str(link)
 if mode=='wrong-hash':item['sha256']='0'*64
 plan={'schema':'capacity-remote-tools-v1','files':[item]}
 if mode=='fd-bound':plan['files']=[dict(item,name='modules/source-'+str(n)+'.mjs') for n in range(150)]
 if mode=='duplicate':plan['files'].append(dict(item))
 manifest=base/'manifest.json';manifest.write_text(json.dumps(plan));configuration={'remoteTools':{'manifestFile':str(manifest),'manifestSha256':hashlib.sha256(manifest.read_bytes()).hexdigest()}}
 if mode=='manifest':configuration['remoteTools']['manifestSha256']='0'*64
 if mode=='existing':(root/'app/remote-tools').mkdir();(root/'app/remote-tools/sentinel').write_text('B-existing')
 budget=m.Budget(max_bytes=16384 if mode=='fd-bound' else 1024,max_files=1000 if mode=='fd-bound' else 10,timeout_seconds=5)
 if mode=='growth':
  original=budget.file
  def grow(size):
   original(size)
   if size:source.write_bytes(source.read_bytes()+b'x'*2048)
  budget.file=grow
 root_fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY);held=[]
 def target(parts):
  parent=os.dup(root_fd);held.append(parent)
  for part in parts:
   try:os.mkdir(part,0o755,dir_fd=parent)
   except FileExistsError:pass
   child=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent);held.append(child);parent=child
  return parent
 try:
  if mode=='fd-bound':
   before=len(os.listdir('/proc/self/fd'));m.copy_remote_tools(configuration,root_fd,target,budget);assert len(list((root/'app/remote-tools/modules').iterdir()))==150;assert len(os.listdir('/proc/self/fd'))<=before+1
  elif mode in ['valid','executable']:
   m.copy_remote_tools(configuration,root_fd,target,budget);copied=root/'app/remote-tools/modules/browser.mjs';assert copied.read_bytes()==source.read_bytes();assert copied.stat().st_ino!=source.stat().st_ino;assert stat.S_IMODE(copied.stat().st_mode)==(0o555 if mode=='executable' else 0o444);source.write_text('changed later');assert copied.read_bytes()==b'component-only-browser-source';assert budget.bytes==item['bytes'];assert budget.files==3
  else:
   failed=False
   try:m.copy_remote_tools(configuration,root_fd,target,budget)
   except (m.ExtractionError,OSError):failed=True
   assert failed
   if mode=='existing':assert (root/'app/remote-tools/sentinel').read_text()=='B-existing'
   if mode=='growth':assert (root/'app/remote-tools/modules/browser.mjs').stat().st_size<=item['bytes'];assert budget.bytes<=1024
  print(mode+': component assertions passed')
 finally:
  for fd in reversed(held):os.close(fd)
  os.close(root_fd)
`;
for(const mode of ['valid','executable','escape','writable','symlink','wrong-hash','duplicate','manifest','existing','growth','fd-bound'])test('remote tools independent native copy component: '+mode,{timeout:10000},async()=>{const result=await execute('/usr/bin/python3',['-I','-B','-c',script,module,mode],{timeout:8000,maxBuffer:4096});assert.match(result.stdout,/component assertions passed/);});
