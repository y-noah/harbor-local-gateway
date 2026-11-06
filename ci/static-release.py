#!/usr/bin/python3
"""仅发布经 root 批准的三个静态文件，不调用容器生命周期或业务数据接口。"""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
import urllib.error
import urllib.request

CONFIG='/etc/harbor-static-release.json'
HISTORY='/opt/product-development/harbor-static-releases'
FILES=('style.css','app.js','index.html')
MAX_FILE=4*1024*1024
MAX_BUNDLE=12*1024*1024
SHA=re.compile(r'^[a-f0-9]{64}$')
REVISION=re.compile(r'^[a-f0-9]{40}$')

def digest(value):return hashlib.sha256(value).hexdigest()
def require(condition,message):
    if not condition:raise ValueError(message)
def exact(value,keys):require(isinstance(value,dict) and set(value)==set(keys),'Unexpected static release fields')
def secure_stat(info,owner=0,directory=False):
    require((stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)) and info.st_uid==owner and not info.st_mode&0o022 and (directory or info.st_nlink==1),'Unsafe static release file ownership or type')
def secure_directory(path):
    fd=os.open('/',os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
    try:
        for part in Path(path).parts[1:]:
            next_fd=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=fd)
            os.close(fd);fd=next_fd;secure_stat(os.fstat(fd),directory=True)
        return fd
    except BaseException:os.close(fd);raise

def read_regular(directory,name,limit=MAX_FILE,owner=0):
    fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=directory)
    try:
        info=os.fstat(fd);secure_stat(info,owner=owner);require(info.st_size<=limit,'Static file exceeds limit')
        with os.fdopen(os.dup(fd),'rb') as stream:value=stream.read(limit+1)
        require(len(value)<=limit and os.fstat(fd).st_size==info.st_size,'Static file changed during read')
        return value
    finally:os.close(fd)

def approval():
    parent=secure_directory(str(Path(CONFIG).parent))
    try:
        raw=read_regular(parent,Path(CONFIG).name,65536)
        require(stat.S_IMODE(os.stat(Path(CONFIG).name,dir_fd=parent,follow_symlinks=False).st_mode)==0o600,'Static approval must be 0600')
    finally:os.close(parent)
    value=json.loads(raw);exact(value,('version','backend','revision','bundle_sha256','before','after','source_hashes','compatibility_sha256'))
    require(value['version']==1 and REVISION.fullmatch(value['revision']) is not None and all(isinstance(value[k],str) and SHA.fullmatch(value[k]) for k in ('bundle_sha256','compatibility_sha256')),'Invalid static approval')
    for key in ('before','after'):
        exact(value[key],FILES);require(all(isinstance(x,str) and SHA.fullmatch(x) for x in value[key].values()),'Invalid asset digest')
    require(isinstance(value['source_hashes'],dict) and 1<len(value['source_hashes'])<=16,'Invalid retained source count')
    for revision,files in value['source_hashes'].items():
        require(isinstance(revision,str) and REVISION.fullmatch(revision),'Invalid retained source revision');exact(files,FILES)
        require(all(isinstance(x,str) and SHA.fullmatch(x) for x in files.values()),'Invalid retained source digest')
    exact(value['backend'],('container_id','image_id','configured_image','source_revision','pid','start_ticks','started_at','root_device','root_inode','public_device','public_inode','server_sha256'))
    b=value['backend'];require(SHA.fullmatch(b['container_id']) and re.fullmatch('sha256:[a-f0-9]{64}',b['image_id']) and REVISION.fullmatch(b['source_revision']) and SHA.fullmatch(b['server_sha256']),'Invalid backend approval')
    require(all(type(b[k]) is int and b[k]>0 for k in ('pid','root_device','root_inode','public_device','public_inode')) and isinstance(b['start_ticks'],str) and b['start_ticks'].isdigit(),'Invalid process approval')
    return value

def bundle(stream,request,approved,retain=False):
    exact(request,('action','revision'));require(request['action']=='deploy-static' and request['revision']==approved['revision'],'Static revision is not approved')
    raw=stream.read(MAX_BUNDLE+1);require(len(raw)<=MAX_BUNDLE and digest(raw)==approved['bundle_sha256'],'Static bundle is not approved')
    value=json.loads(raw);exact(value,('version','revision','backend_revision','sources','source_hashes','before','after','assets','compatibility'))
    require(value['version']==1 and value['revision']==approved['revision'] and value['backend_revision']==approved['backend']['source_revision'] and value['source_hashes']==approved['source_hashes'] and value['before']==approved['before'] and value['after']==approved['after'],'Invalid static bundle identity')
    exact(value['assets'],('app.js','index.html'))
    proof=value['compatibility'];evidence=json.dumps(proof,sort_keys=True,separators=(',',':')).encode()
    require(digest(evidence)==approved['compatibility_sha256'],'Compatibility evidence is not approved')
    exact(proof,('contract','backend_revision','revision','before','after','source_hashes','cases','passed'))
    cases={'old-html','new-html','old-delayed-script','new-delayed-script','rollback-old-html','rollback-live-new-page','cached-old-css','cached-new-page'}
    require(proof['contract']=='harbor-static-bootstrap-v1' and proof['passed'] is True and isinstance(proof['cases'],list) and cases.issubset(set(proof['cases'])) and proof['before']==approved['before'] and proof['after']==approved['after'] and proof['source_hashes']==approved['source_hashes'] and proof['backend_revision']==value['backend_revision'] and proof['revision']==approved['revision'],'Static compatibility verification is incomplete')
    require(approved['before']['style.css']==approved['after']['style.css'],'Original compatibility stylesheet cannot change')
    files={}
    for name,text in value['assets'].items():
        require(isinstance(text,str) and len(text)<=MAX_FILE*4//3+8,'Invalid static file encoding')
        content=base64.b64decode(text,validate=True);require(len(content)<=MAX_FILE and digest(content)==approved['after'][name],'Static file digest mismatch');files[name]=content
    return (files,raw) if retain else files

class Backend:
    """只接受固定容器代次；proc root 是唯一特许跟随的内核符号链接。"""
    def __init__(self,api,approved):self.api=api;self.approved=approved;self.root=None;self.public=None
    def verify(self):
        b=self.approved['backend'];row=self.api['inspect_container'](self.api['NAME']);require(row is not None,'Backend identity is unknown')
        c=row['Config'];s=row['State'];h=row['HostConfig'];labels=c.get('Labels') or {}
        require(row['Id']==b['container_id'] and row['Image']==b['image_id'] and c.get('Image')==b['configured_image'] and labels.get('org.opencontainers.image.revision')==b['source_revision'] and labels.get('com.aistrong.project')==self.api['NAME'],'Backend immutable identity changed')
        require(s.get('Running') is True and not any(s.get(k) for k in ('Paused','Restarting','Dead','OOMKilled')) and s.get('Health',{}).get('Status')=='healthy' and s.get('Pid')==b['pid'] and s.get('StartedAt')==b['started_at'],'Backend process generation changed')
        require(c.get('Cmd')==['node','server.mjs'] and c.get('User')=='node' and h.get('Privileged') is False and 'ALL' in (h.get('CapDrop') or []),'Backend does not meet static update boundary')
        port=str(self.api['static_port']);bindings=h.get('PortBindings') or {}
        require(set(bindings)=={port+'/tcp'} and bindings[port+'/tcp'] and all(item.get('HostPort')==port for item in bindings[port+'/tcp']),'Backend HTTP port differs from root configuration')
        require([item for item in c.get('Env',[]) if item.startswith('HARBOR_PORT=')]==['HARBOR_PORT='+port],'Backend HTTP environment changed')
        mounts={(m.get('Source'),m.get('Destination'),m.get('Type')) for m in row.get('Mounts',[])}
        require(mounts=={(self.api['STACK']+'/data','/app/data','bind'),(self.api['STACK']+'/runtime','/app/runtime','bind')},'Backend mounts changed')
        proc=Path('/proc')/str(b['pid']);raw=(proc/'stat').read_text();require(raw[raw.rfind(')')+2:].split()[19]==b['start_ticks'],'PID start ticks changed')
        info=(proc/'root').stat();require((info.st_dev,info.st_ino)==(b['root_device'],b['root_inode']),'Process root changed')
        if self.root is not None:require((os.fstat(self.root).st_dev,os.fstat(self.root).st_ino)==(info.st_dev,info.st_ino),'Pinned process root changed')
        if self.public is not None:
            info=os.stat('public',dir_fd=self.app,follow_symlinks=False);require((info.st_dev,info.st_ino)==(b['public_device'],b['public_inode']) and stat.S_ISDIR(info.st_mode),'Public directory changed')
        return row
    def open(self):
        self.verify();b=self.approved['backend'];self.root=os.open('/proc/'+str(b['pid'])+'/root',os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
        try:
            require((os.fstat(self.root).st_dev,os.fstat(self.root).st_ino)==(b['root_device'],b['root_inode']),'Opened process root changed')
            self.app=os.open('app',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=self.root);secure_stat(os.fstat(self.app),directory=True)
            require(digest(read_regular(self.app,'server.mjs'))==b['server_sha256'],'Backend source bytes changed')
            self.public=os.open('public',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=self.app);secure_stat(os.fstat(self.public),directory=True)
            info=os.fstat(self.public);require((info.st_dev,info.st_ino)==(b['public_device'],b['public_inode']),'Opened public directory changed');self.verify()
            return self.public
        except BaseException:self.close();raise
    def close(self):
        for name in ('public','app','root'):
            fd=getattr(self,name,None)
            if fd is not None:os.close(fd);setattr(self,name,None)

def atomic_file(directory,name,content,mode=0o644,owner=0):
    require(name in FILES or name in ('receipt.json','backup.json','bundle.json'),'Unexpected destination name')
    temporary='.harbor-static-'+secrets.token_hex(16)
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,mode,dir_fd=directory)
    try:
        os.fchmod(fd,mode);os.fchown(fd,owner,-1)
        with os.fdopen(os.dup(fd),'wb') as stream:stream.write(content);stream.flush();os.fsync(stream.fileno())
        secure_stat(os.fstat(fd),owner=owner)
        os.replace(temporary,name,src_dir_fd=directory,dst_dir_fd=directory);os.fsync(directory)
    finally:
        os.close(fd)
        try:os.unlink(temporary,dir_fd=directory)
        except FileNotFoundError:pass

def assets(directory,owner=0):return {name:read_regular(directory,name,owner=owner) for name in FILES}
def hashes(files):return {name:digest(files[name]) for name in FILES}
def check_http(port,expected):
    # No credentials, redirects, proxies, or business writes are used for this check.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self,*args):return None
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
    def get(path,headers=None):
        request=urllib.request.Request('http://127.0.0.1:'+str(port)+path,headers=headers or {})
        try:
            with opener.open(request,timeout=5) as r:return r.status,dict(r.headers),r.read(MAX_FILE+1)
        except urllib.error.HTTPError as e:return e.code,dict(e.headers),b''
    require(get('/health')[0]==200 and get('/api/admin/state')[0]==401 and get('/health',{'Host':'untrusted.invalid'})[0]==403,'Health or authentication check failed')
    for name in FILES:
        code,headers,body=get('/' if name=='index.html' else '/'+name,{'If-None-Match':'"harbor-static-probe"'})
        require(code==200 and digest(body)==expected[name] and headers.get('Cache-Control')=='no-store' and 'ETag' not in headers,'Served static bytes or cache contract changed')

def publish(public,files,approved,verify,http,history,owner=0):
    """单入口 HTML 提交；失败逻辑回退保留兼容启动器，避免迟到新页面加载旧 JS。"""
    before=assets(public,owner);require(hashes(before)==approved['before'],'Installed static baseline changed')
    backup={name:base64.b64encode(before[name]).decode() for name in FILES}
    atomic_file(history,'backup.json',json.dumps(backup,separators=(',',':')).encode(),0o600,owner)
    receipt={'version':1,'static_revision':approved['revision'],'backend_revision':approved['backend']['source_revision'],'backend_image':approved['backend']['image_id'],'bundle_sha256':approved['bundle_sha256'],'phase':'prepared','before':approved['before'],'after':approved['after'],'rollback_contract':'restore-index-retain-compatible-bootstrap-v1'}
    def save():atomic_file(history,'receipt.json',json.dumps(receipt,sort_keys=True).encode(),0o600,owner)
    save();verify();http(approved['before'])
    try:
        verify();require(hashes(assets(public,owner))==approved['before'],'Static baseline changed before commit')
        # Every previously published HTML is retained as a branch in the bootstrap.
        atomic_file(public,'app.js',files['app.js'],owner=owner)
        partial={**approved['before'],'app.js':approved['after']['app.js']};verify();http(partial)
        atomic_file(public,'index.html',files['index.html'],owner=owner)
        verify();require(hashes(assets(public,owner))==approved['after'],'Installed static bytes differ');http(approved['after']);verify()
        receipt['phase']='active';receipt['installed']=approved['after'];save();return receipt
    except BaseException:
        verify()
        # replace() may have committed even when its following fsync raised. Observe pinned files,
        # never infer installation solely from a function's return or an in-memory flag.
        installed=hashes(assets(public,owner));require(installed['style.css']==approved['before']['style.css'],'Compatibility stylesheet changed')
        require(installed['app.js'] in (approved['before']['app.js'],approved['after']['app.js']),'Cannot overwrite independently changed bootstrap')
        bootstrap=installed['app.js']==approved['after']['app.js']
        if installed['index.html']==approved['after']['index.html'] and installed['index.html']!=approved['before']['index.html']:
            atomic_file(public,'index.html',before['index.html'],owner=owner)
        else:require(installed['index.html']==approved['before']['index.html'],'Cannot overwrite independently changed HTML')
        expected={**approved['before'],**({'app.js':approved['after']['app.js']} if bootstrap else {})}
        verify();require(hashes(assets(public,owner))==expected,'Logical rollback differs');http(expected)
        receipt['phase']='rolled_back';receipt['installed']=expected;receipt['compatible_bootstrap_retained']=bootstrap;save();raise

def deploy(request,stream,api):
    require(os.getuid()==0,'Static deployment requires root');approved=approval();files,raw=bundle(stream,request,approved,retain=True)
    backend=Backend(api,approved);history=None;parent=None
    try:
        public=backend.open();parent=secure_directory(HISTORY)
        try:os.mkdir(approved['revision'],0o700,dir_fd=parent)
        except FileExistsError:pass
        history=os.open(approved['revision'],os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=parent);secure_stat(os.fstat(history),directory=True)
        if os.listdir(history):
            receipt=json.loads(read_regular(history,'receipt.json',65536))
            require(receipt.get('phase')=='active' and receipt.get('bundle_sha256')==approved['bundle_sha256'] and receipt.get('after')==approved['after'],'Interrupted or rolled back static release requires retained-history inspection')
            backend.verify();require(hashes(assets(public))==approved['after'],'Active static receipt differs from installed bytes');check_http(api['static_port'],approved['after']);backend.verify()
            print(json.dumps({'static_release':receipt,'already_active':True,'backend_restarted':False,'container_image_updated':False},sort_keys=True));return
        atomic_file(history,'bundle.json',raw,0o600)
        receipt=publish(public,files,approved,backend.verify,lambda expected:check_http(api['static_port'],expected),history)
        print(json.dumps({'static_release':receipt,'backend_restarted':False,'container_image_updated':False},sort_keys=True))
    finally:
        backend.close()
        for fd in (history,parent):
            if fd is not None:os.close(fd)
