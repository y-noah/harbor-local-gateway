#!/usr/bin/python3
"""Root-owned forced SSH command, limited to one Harbor service."""
import fcntl
import json
import os
import gzip
import shutil
import tarfile
import tempfile
from pathlib import Path
import re
import subprocess
import sys
import syslog
import time
import importlib.util
import stat
import urllib.request
import urllib.error

CONFIG='/etc/harbor-gateway-deploy.json'
NAME='harbor-local-gateway'
STACK='/opt/product-development/harbor-local-gateway'
STATIC_HELPER='/usr/local/lib/harbor/static-release.py'
ENTRY_HELPER='/usr/local/lib/harbor/gateway-entry-controller.py'
LOCAL_ASSURANCE_HELPER='/usr/local/lib/harbor/local-assurance-deploy.py'
IMAGE_STAGE_HELPER='/usr/local/lib/harbor/release-image.py'

def validate(payload,config):
    if not isinstance(payload,dict) or set(payload)!={'action','image'} or payload['action'] not in ('deploy','deploy-entry-postgres'):
        raise ValueError('Only a deployment request is allowed')
    image=payload['image']
    if not isinstance(image,str) or not re.fullmatch(re.escape(config['image_prefix'])+r':[0-9a-f]{40}',image):
        raise ValueError('Only an immutable image from this service is allowed')
    return image

def deployment_policy(config,action):
    # Existing 8092 must remain available: no root flag can re-enable the old stop-first path.
    port=config.get('port')
    if type(port)!=int or not 1<=port<=65535:raise ValueError('Invalid root deployment port')
    policy=config.get('availability_policy','preserve-current' if port==8092 else 'legacy-replace')
    if policy not in ('preserve-current','legacy-replace'):raise ValueError('Invalid root availability policy')
    if port==8092 and policy!='preserve-current':raise ValueError('Port 8092 requires preserve-current policy')
    if action not in ('deploy','deploy-static','deploy-entry-postgres'):raise ValueError('Unknown deployment action')
    if action=='deploy-entry-postgres' and (port!=8092 or policy!='preserve-current' or config.get('storage_engine')!='postgres'):raise ValueError('Entry deployment requires the preserved 8092 PostgreSQL composition')
    if action=='deploy' and policy=='preserve-current':raise ValueError('Stop-first deployment is prohibited; only approved static release is available')
    return policy

def deployment_action(payload,config):
    enabled=config.get('entry_deployment_enabled',False)
    if type(enabled)!=bool:raise ValueError('Root entry deployment flag must be boolean')
    action=payload.get('action')
    if action=='deploy' and enabled:action='deploy-entry-postgres'
    deployment_policy(config,action)
    return action


def run(*args,check=True,timeout=None):
    return subprocess.run(args,check=check,capture_output=True,text=True,timeout=timeout)

def docker(*args,check=True,timeout=None):
    return run('/usr/bin/docker','--config',STACK+'/secrets/registry',*args,check=check,timeout=timeout)

def inspect(name):
    result=docker('inspect',name,check=False,timeout=10)
    return json.loads(result.stdout)[0] if result.returncode==0 else None

def inspect_container(name):
    """PG-only lookup: a failed inspection is not evidence of absence."""
    result=docker('container','inspect',name,check=False,timeout=10)
    if result.returncode==0:
        value=json.loads(result.stdout)
        if not isinstance(value,list) or len(value)!=1 or not isinstance(value[0],dict) or not isinstance(value[0].get('Id'),str) or not value[0]['Id'] or not isinstance(value[0].get('Config'),dict) or type(value[0].get('State',{}).get('Running'))!=bool:raise RuntimeError('Container inspection response is invalid')
        return value[0]
    inventory=docker('container','ls','--all','--filter','name=^/'+re.escape(name)+'$','--format','{{json .Names}}',check=False,timeout=10)
    if inventory.returncode==0 and not inventory.stdout.strip():return None
    raise RuntimeError('Container existence is not verified')

def inspect_image(name):
    """PG-only image proof: neither absence nor daemon failure is a capability."""
    result=docker('image','inspect',name,check=False,timeout=60)
    if result.returncode!=0:raise RuntimeError('Image inspection failed')
    value=json.loads(result.stdout)
    if not isinstance(value,list) or len(value)!=1 or not isinstance(value[0],dict) or not isinstance(value[0].get('Id'),str) or not value[0]['Id'] or not isinstance(value[0].get('Config'),dict):raise RuntimeError('Image inspection response is invalid')
    return value[0]

def http_code(port,path,headers=None):
    try:
        with urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:'+str(port)+path,headers=headers or {}),timeout=5) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code
    except (OSError,urllib.error.URLError):
        return 0

MAX_ARCHIVE_BYTES=2*1024**3

def validate_archive(path,image):
    # Docker load runs with administrator rights; reject every foreign tag and unsafe tar entry first.
    with tarfile.open(path,mode='r:') as archive:
        members={}
        for member in archive:
            name=member.name
            parts=Path(name).parts
            if not parts or Path(name).is_absolute() or '..' in parts or not (member.isfile() or member.isdir()) or name in members or len(members)>=512:
                raise ValueError('Unsafe image archive')
            members[name]=member
        def metadata(name):
            member=members.get(name)
            if not member or not member.isfile() or member.size>1024*1024:
                raise ValueError('Invalid image metadata')
            return json.load(archive.extractfile(member))
        manifest=metadata('manifest.json')
        if not isinstance(manifest,list) or len(manifest)!=1 or not isinstance(manifest[0],dict) or manifest[0].get('RepoTags')!=[image]:
            raise ValueError('Only this exact service image may be imported')
        record=manifest[0]
        config_path=record.get('Config')
        layers=record.get('Layers')
        if not isinstance(config_path,str) or not isinstance(layers,list) or not layers or not all(isinstance(layer,str) for layer in layers):
            raise ValueError('Invalid image manifest')
        references=[config_path,*layers]
        if len(set(references))!=len(references) or any(name not in members or not members[name].isfile() for name in references):
            raise ValueError('Missing or duplicate image content')
        metadata(config_path)
        allowed={'manifest.json',*references}
        if 'repositories' in members:
            repository,tag=image.rsplit(':',1)
            repositories=metadata('repositories')
            if not isinstance(repositories,dict) or set(repositories)!={repository} or not isinstance(repositories[repository],dict) or set(repositories[repository])!={tag}:
                raise ValueError('Foreign repository metadata')
            allowed.add('repositories')
        # Skopeo's docker-archive format contains only the manifest, config, layers and optional repositories.
        if any(member.isfile() and name not in allowed for name,member in members.items()):
            raise ValueError('Unexpected archive content')

def receive_archive(stream,directory):
    Path(directory).mkdir(mode=0o700,parents=True,exist_ok=True)
    if shutil.disk_usage(directory).free<MAX_ARCHIVE_BYTES+1024**3:
        raise ValueError('Insufficient deployment disk space')
    descriptor,path=tempfile.mkstemp(prefix='image-',suffix='.tar',dir=directory)
    try:
        total=0
        with os.fdopen(descriptor,'wb') as target,gzip.GzipFile(fileobj=stream,mode='rb') as incoming:
            while True:
                chunk=incoming.read(1024*1024)
                if not chunk:break
                total+=len(chunk)
                if total>MAX_ARCHIVE_BYTES:raise ValueError('Image archive exceeds size limit')
                target.write(chunk)
        return path
    except Exception:
        Path(path).unlink(missing_ok=True)
        raise

def activation(action,image):
    if action not in ('check','preflight','consume','revoke','status'):raise ValueError('Invalid internal activation action')
    node=Path('/opt/harbor-storage-tools/node/bin/node');helper=Path('/opt/harbor-storage-tools/app/storage-activation-deploy.mjs')
    for target in (node,helper):
        for parent in [*target.parents,target]:
            info=parent.lstat()
            if stat.S_ISLNK(info.st_mode) or info.st_uid!=0 or info.st_mode&0o022:raise ValueError('Activation helper is not root controlled')
    result=subprocess.run([str(node),str(helper)],input=json.dumps({'action':action,'image':image}),capture_output=True,text=True,timeout=150,env={'PATH':'/usr/bin:/bin','LANG':'C.UTF-8','TZ':'Asia/Shanghai'})
    if result.returncode!=0:raise RuntimeError('PostgreSQL activation operation failed; retain migration material')
    value=json.loads(result.stdout)
    if not isinstance(value,dict) or value.get('contract')!='harbor-storage-activation-deploy-v1':raise ValueError('Activation helper response is invalid')
    return value

def deploy_entry_postgres(image,config,archive=None):
    deployment_policy(config,'deploy-entry-postgres')
    local_assurance=config.get('local_assurance_release',False)
    if type(local_assurance) is not bool:raise ValueError('Local assurance flag must be a root boolean')
    if local_assurance:
        if config.get('entry_installed_release') is not True or archive is not None:raise ValueError('Local assurance requires the installed entry image')
        # A committed PG installation must never enter SQLite migration again.
        helper=Path('/usr/local/lib/harbor/postgres-upgrade-deploy.py') if os.path.lexists('/var/lib/harbor-entry/active.json') else Path(LOCAL_ASSURANCE_HELPER)
        for target in [*reversed(helper.parents),helper]:
            info=target.lstat()
            if stat.S_ISLNK(info.st_mode) or info.st_uid!=0 or info.st_mode&0o022:raise ValueError('Local assurance helper is not root controlled')
        if not stat.S_ISREG(helper.lstat().st_mode):raise ValueError('Local assurance helper must be a regular file')
        spec=importlib.util.spec_from_file_location('harbor_local_assurance_deploy',helper)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        return module.deploy(image,config,archive,globals())
    helper=Path(ENTRY_HELPER)
    for target in [*reversed(helper.parents),helper]:
        info=target.lstat()
        if stat.S_ISLNK(info.st_mode) or info.st_uid!=0 or info.st_mode&0o022:raise ValueError('Entry controller is not root controlled')
    if not stat.S_ISREG(helper.lstat().st_mode):raise ValueError('Entry controller must be a regular file')
    spec=importlib.util.spec_from_file_location('harbor_entry_controller',helper)
    controller=importlib.util.module_from_spec(spec);spec.loader.exec_module(controller)
    approval=activation('check',image)
    context=controller.authorize_deployment(image=image,approval=approval)
    def guarded_activation(action,requested_image):
        if requested_image!=image:raise ValueError('Entry activation image changed')
        if action in ('check','preflight','consume','status'):context.check()
        value=activation(action,requested_image)
        if action in ('check','preflight','consume','status'):context.check(value)
        return value
    spec=importlib.util.spec_from_file_location('harbor_postgres_deploy',Path(__file__).with_name('postgres-deploy.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    api={**globals(),'activation':guarded_activation,'http_code':lambda port,path,headers=None:context.probe(port,path,headers),'entry_deployment':context}
    result=module.deploy(image,config,archive,api)
    # Failed final CAS leaves PostgreSQL and the held entry intact for explicit
    # recovery. It must never trigger an automatic SQLite restart.
    latest=guarded_activation('status',image)
    entry=context.activate(latest)
    return {**result,'entry':entry}


def deploy(image,config,archive=None):
    deployment_policy(config,'deploy')
    engine=config.get('storage_engine','sqlite')
    if engine not in ('sqlite','postgres'):raise ValueError('Invalid storage engine')
    if engine=='postgres':
        spec=importlib.util.spec_from_file_location('harbor_postgres_deploy',Path(__file__).with_name('postgres-deploy.py'))
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        return module.deploy(image,config,archive,globals())
    if os.path.lexists(Path(STACK)/'postgres-storage.json'):raise ValueError('PostgreSQL marker prohibits a SQLite deployment or rollback')
    port=config['port']
    contract=config.get('required_data_contract')
    if contract is not None and (not isinstance(contract,str) or not re.fullmatch(r'[a-z0-9][a-z0-9._-]{0,63}',contract)):
        raise ValueError('Invalid required data contract')
    current=inspect(NAME)
    if current and any(binding.get('HostPort')=='8092' for values in (current.get('HostConfig',{}).get('PortBindings') or {}).values() for binding in (values or [])):
        raise ValueError('An existing 8092 generation cannot use stop-first deployment')
    if current and current['Config'].get('Labels',{}).get('com.aistrong.project')!=NAME:
        raise ValueError('Existing container is not owned by this service')
    if inspect(NAME+'-previous'):
        raise ValueError('Previous container requires administrator inspection')
    bindings=(current or {}).get('HostConfig',{}).get('PortBindings',{})
    owns_port=any(binding.get('HostPort')==str(port) for entries in bindings.values() for binding in (entries or []))
    if not owns_port and run('/usr/bin/ss','-lntH','sport = :'+str(port)).stdout.strip():
        raise ValueError('Service port is occupied')
    if archive:
        validate_archive(archive,image)
        docker('load','--input',archive)
    else:
        docker('pull',image)
    if contract:
        candidate_image=inspect(image)
        if not candidate_image or candidate_image['Config'].get('Labels',{}).get('com.harbor.data-contract')!=contract:
            raise ValueError('Candidate image does not meet the required data contract')
    rollback_compatible=not contract or (current or {}).get('Config',{}).get('Labels',{}).get('com.harbor.data-contract')==contract
    for directory in ['data','runtime']:
        path=Path(STACK)/directory
        path.mkdir(mode=0o700,parents=True,exist_ok=True)
        os.chown(path,1000,1000)
    previous=False
    try:
        if current:
            docker('stop','-t','30',NAME)
            docker('rename',NAME,NAME+'-previous')
            previous=True
        docker('run','-d','--name',NAME,'--label','com.aistrong.project='+NAME,
               '--restart','unless-stopped','--memory','1g','--cpus','1','--pids-limit','256',
               '--cap-drop','ALL','--security-opt','no-new-privileges',
               '--env','TZ=Asia/Shanghai','--env','HARBOR_LISTEN_HOST=0.0.0.0',
               '--env','HARBOR_PUBLIC_ORIGINS='+config['origin'],'--env','HARBOR_PORT='+str(port),
               '--publish',str(port)+':'+str(port),
               '--mount','type=bind,src='+STACK+'/data,dst=/app/data',
               '--mount','type=bind,src='+STACK+'/runtime,dst=/app/runtime',image)
        end=time.monotonic()+90
        while time.monotonic()<end:
            candidate=inspect(NAME)
            if candidate and candidate['State'].get('Health',{}).get('Status')=='healthy' and http_code(port,'/health')==200:
                break
            time.sleep(2)
        else:
            raise RuntimeError('Health check failed')
        if candidate['Config']['Image']!=image or http_code(port,'/api/admin/state')!=401 or http_code(port,'/health',{'Host':'untrusted.invalid'})!=403:
            raise RuntimeError('Image or authentication checks failed')
    except Exception:
        if previous or not current:
            docker('rm','-f',NAME,check=False)
        if previous:
            docker('rename',NAME+'-previous',NAME)
            if rollback_compatible:
                docker('start',NAME)
        elif current and rollback_compatible:
            docker('start',NAME)
        raise
    if previous:
        docker('rm',NAME+'-previous')

def receive_deployment_image(stream,payload,config,action):
    transport=config.get('image_transport','registry')
    if transport not in ('registry','ssh_archive'):raise ValueError('Invalid image transport')
    installed=config.get('entry_installed_release',False)
    if type(installed) is not bool:raise ValueError('Installed entry release must be a root boolean')
    if installed and payload.get('action')=='deploy-entry-postgres':
        deployment_policy(config,action)
        if action!='deploy-entry-postgres':raise ValueError('Installed entry release requires the explicit entry action')
        if stream.read(1):raise ValueError('Installed entry release does not accept image bytes')
        # The entry consumer still verifies the approved OCI identity and runs
        # the ordinary activation/preflight checks against the installed image.
        return None,'installed'
    return (receive_archive(stream,STACK+'/import') if transport=='ssh_archive' else None),transport

def log_image_stage_error(action,error,module=None):
    try:
        event={'event':'harbor_image_stage_error','action':action if action in ('prepare-image','image-stage-status','image-stage-plan') else 'unknown'}
        kind=type(error).__name__
        event['type']=kind if kind in ('ValueError','RuntimeError','OSError','PermissionError','FileNotFoundError','BlockingIOError','TimeoutError','CalledProcessError','TimeoutExpired','TarError','ReadError','EOFError','KeyError','TypeError','JSONDecodeError','SyntaxError') else 'Exception'
        if module is not None and type(error) is getattr(module,'Refused',None):
            event['type']='Refused'
            if len(error.args)==1 and isinstance(error.args[0],str) and re.fullmatch(r'image_stage_[a-z_]{1,64}',error.args[0]):event['code']=error.args[0]
        if isinstance(error,OSError) and type(error.errno) is int and 0<error.errno<4096:event['errno']=error.errno
        syslog.openlog('harbor-image-stage',syslog.LOG_PID,syslog.LOG_DAEMON)
        syslog.syslog(syslog.LOG_ERR,json.dumps(event,sort_keys=True,separators=(',',':')))
    except Exception:
        pass

def main():
    os.umask(0o077)
    request=sys.stdin.buffer.readline(4097)
    if len(request)>4096:raise ValueError('Deployment request is too large')
    payload=json.loads(request)
    info=Path(CONFIG).lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode) or info.st_uid!=0 or stat.S_IMODE(info.st_mode)!=0o600:raise ValueError('Deployment configuration must be root-owned 0600')
    config=json.loads(Path(CONFIG).read_text())
    if not isinstance(payload,dict):raise ValueError('Only a deployment request is allowed')
    if payload.get('action') in ('prepare-image','image-stage-status','image-stage-plan'):
        module=None
        try:
            # Transport can install verified OCI bytes, never authorize or execute a
            # release. A status read must remain available while the importer holds
            # the ordinary deployment lock.
            if type(config.get('image_staging_enabled',False)) is not bool or config.get('image_staging_enabled') is not True:raise ValueError('Image staging requires the root opt-in')
            helper=Path(IMAGE_STAGE_HELPER)
            for target in [*reversed(helper.parents),helper]:
                info=target.lstat()
                if stat.S_ISLNK(info.st_mode) or info.st_uid!=0 or info.st_mode&0o022:raise ValueError('Image stage helper is not root controlled')
            spec=importlib.util.spec_from_file_location('harbor_release_image',helper)
            module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
            module.identity(payload,config)
            if payload['action'] in ('image-stage-status','image-stage-plan'):
                result=module.handle(payload,sys.stdin.buffer,config)
            else:
                with open('/run/harbor-gateway-deploy.lock','a') as lock:
                    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    result=module.handle(payload,sys.stdin.buffer,config)
            print(json.dumps({'image_staging':result},sort_keys=True))
        except Exception as error:
            log_image_stage_error(payload['action'],error,module)
            raise
        return
    action=deployment_action(payload,config)
    if action=='deploy-static':
        # The helper and approval are independently installed by root. CI cannot select a path or command.
        helper=Path(STATIC_HELPER)
        for target in [*reversed(helper.parents),helper]:
            info=target.lstat()
            if stat.S_ISLNK(info.st_mode) or info.st_uid!=0 or info.st_mode&0o022:raise ValueError('Static helper is not root controlled')
        with open('/run/harbor-gateway-deploy.lock','w') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            spec=importlib.util.spec_from_file_location('harbor_static_release',helper)
            module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
            module.deploy(payload,sys.stdin.buffer,{**globals(),'static_port':config['port']})
        return
    image=validate(payload,config)
    with open('/run/harbor-gateway-deploy.lock','w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        archive=None
        try:
            archive,transport=receive_deployment_image(sys.stdin.buffer,payload,config,action)
            result=(deploy_entry_postgres if action=='deploy-entry-postgres' else deploy)(image,config,archive)
            if action=='deploy-entry-postgres':result={**result,'wireAction':payload['action'],'effectiveAction':action,'imageTransport':transport}
            if config.get('storage_engine')=='postgres':print(json.dumps({'postgres_deployment':result},sort_keys=True))
        finally:
            if archive:Path(archive).unlink(missing_ok=True)
    print('Harbor deployment and health/authentication checks passed')

if __name__=='__main__':
    try:
        main()
    except (ValueError,RuntimeError,subprocess.CalledProcessError,subprocess.TimeoutExpired,OSError,tarfile.TarError,EOFError) as error:
        print('Harbor deployment failed: '+type(error).__name__,file=sys.stderr)
        sys.exit(1)
