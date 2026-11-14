"""Fixed PostgreSQL deployment composition; invoked only by the root-owned wrapper."""
import json
import hashlib
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import tempfile
import time
import socket
import ssl
import http.client
from urllib.parse import urlsplit,parse_qs
CONTRACT='postgres-conversation-v2'
EXTRA_USAGE_DISPATCH_CONTRACT='v1'
SHUTDOWN_CONTRACT='postgres-shutdown-v1'
SHUTDOWN_ENV={'gateway_drain_ms':'HARBOR_GATEWAY_DRAIN_TIMEOUT_MS','cleanup_grace_ms':'HARBOR_SHUTDOWN_CLEANUP_GRACE_MS','worker_stop_ms':'HARBOR_WORKER_STOP_TIMEOUT_MS'}
SHUTDOWN_DEFAULTS={'gateway_drain_ms':300000,'cleanup_grace_ms':30000,'worker_stop_ms':30000}
SHUTDOWN_RANGES={'gateway_drain_ms':(100,3600000),'cleanup_grace_ms':(1000,300000),'worker_stop_ms':(1000,3600000)}

class ShutdownUnconfirmed(RuntimeError):
    pass

class ShutdownContractUnsupported(ValueError):
    pass

class SourcePreflightRejected(RuntimeError):
    # This failure is raised only before deploy mutates the source composition.
    pass

def shutdown_configuration(value=None):
    if value is None:value=dict(SHUTDOWN_DEFAULTS)
    if not isinstance(value,dict) or set(value)!=set(SHUTDOWN_DEFAULTS) or any(type(value[key])!=int or not SHUTDOWN_RANGES[key][0]<=value[key]<=SHUTDOWN_RANGES[key][1] for key in SHUTDOWN_DEFAULTS):raise ValueError('PostgreSQL shutdown configuration is invalid')
    return dict(value)

def preflight_timeout_seconds(value=30):
    if type(value)!=int or not 30<=value<=300:raise ValueError('PostgreSQL preflight timeout must be an integer from 30 to 300 seconds')
    return value

def confirmed_container_state(instance):
    state=instance.get('State',{})
    if type(state.get('Running'))!=bool or state.get('Paused') or state.get('Restarting') or state.get('Dead') or not instance.get('Id'):raise ShutdownUnconfirmed('PostgreSQL container state is not verified')
    if not state['Running'] and state.get('Status') not in ('exited','created'):raise ShutdownUnconfirmed('PostgreSQL stopped state is not verified')
    return state

def no_code_injection(config):
    if any(item.split('=',1)[0] in ('NODE_OPTIONS','NODE_PATH','LD_PRELOAD') and item.partition('=')[2] for item in config.get('Env',[])):raise ShutdownUnconfirmed('Container code injection environment is not recognized')

def legacy_sqlite_source(api,instance,role,config):
    if not instance or role!='gateway':return False
    actual=instance.get('Config',{})
    if actual.get('Cmd') not in (['node','server.mjs'],['node','/app/server.mjs']):return False
    if config.get('postgres_legacy_source_engine')!='sqlite':raise ShutdownUnconfirmed('SQLite source requires explicit root migration configuration')
    state=confirmed_container_state(instance)
    if state['Running']:raise ShutdownUnconfirmed('Running SQLite source requires a controlled maintenance stop')
    if actual.get('WorkingDir')!='/app' or actual.get('Entrypoint') not in (None,[],['docker-entrypoint.sh'],['/usr/local/bin/docker-entrypoint.sh']):raise ShutdownUnconfirmed('Legacy SQLite entrypoint is not recognized')
    no_code_injection(actual)
    image_name=actual.get('Image','');prefix=config.get('image_prefix')
    if not isinstance(prefix,str) or not re.fullmatch(re.escape(prefix)+r':[0-9a-f]{40}',image_name):raise ShutdownUnconfirmed('Legacy SQLite image source is not verified')
    image_id=instance.get('Image')
    if not isinstance(image_id,str) or not re.fullmatch(r'sha256:[a-f0-9]{64}',image_id):raise ShutdownUnconfirmed('Legacy SQLite image identity is invalid')
    image=image_inspection(api,image_id,[]);labels=image['Config'].get('Labels',{}) or {}
    if image['Id']!=image_id or labels.get('org.opencontainers.image.revision')!=image_name.rsplit(':',1)[-1] or labels.get('com.harbor.data-contract')!='session-metadata-only-v1' or labels.get('com.harbor.ci-validation-only')=='true':raise ShutdownUnconfirmed('Legacy SQLite image contract is not verified')
    return True

def container_shutdown(api,instance,role):
    if not instance:return None
    config=instance.get('Config',{});state=confirmed_container_state(instance)
    if config.get('Cmd')!=['node','/app/postgres-'+role+'.mjs']:raise ShutdownUnconfirmed('PostgreSQL source entrypoint is not recognized')
    labels=config.get('Labels',{})
    if state['Running'] and (labels.get('com.harbor.shutdown-contract')!=SHUTDOWN_CONTRACT or labels.get('com.harbor.shutdown-role')!=role):raise ShutdownUnconfirmed('Running legacy PostgreSQL requires a controlled maintenance stop')
    image=image_inspection(api,config.get('Image',''),[])
    if image['Id']!=instance.get('Image') or (image['Config'].get('Labels',{}) or {}).get('com.harbor.storage.postgres')!=CONTRACT:raise ShutdownUnconfirmed('PostgreSQL image identity or source engine is not verified')
    no_code_injection(config)
    if not state['Running'] and labels.get('com.harbor.shutdown-contract')!=SHUTDOWN_CONTRACT:
        if config.get('Entrypoint') not in (None,[],['docker-entrypoint.sh'],['/usr/local/bin/docker-entrypoint.sh']):raise ShutdownUnconfirmed('Legacy PostgreSQL entrypoint is not recognized')
        return None
    if labels.get('com.harbor.shutdown-role')!=role or config.get('StopSignal') not in (None,'','SIGTERM','15') or config.get('Entrypoint') not in (None,[]):raise ShutdownUnconfirmed('PostgreSQL shutdown entrypoint or role is not verified')
    values={}
    for key,name in SHUTDOWN_ENV.items():
        found=[item[len(name)+1:] for item in config.get('Env',[]) if item.startswith(name+'=')]
        if len(found)!=1 or not re.fullmatch('[0-9]+',found[0]):raise ShutdownUnconfirmed('PostgreSQL shutdown values are not verified')
        values[key]=int(found[0])
    try:return shutdown_configuration(values)
    except ValueError as error:raise ShutdownUnconfirmed('PostgreSQL shutdown values are invalid') from error

def shutdown_diagnostic(stack,rows):
    descriptor,path=tempfile.mkstemp(prefix='.postgres-shutdown-',dir=stack)
    try:
        with os.fdopen(descriptor,'w') as output:
            json.dump({'contract':SHUTDOWN_CONTRACT,'containers':rows},output);output.flush();os.fsync(output.fileno())
        os.replace(path,Path(stack)/'postgres-shutdown-diagnostics.json')
        directory=os.open(stack,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(directory)
        finally:os.close(directory)
    finally:Path(path).unlink(missing_ok=True)

def record_shutdown_diagnostic(stack,rows):
    try:shutdown_diagnostic(stack,rows)
    except Exception as error:
        failure=ShutdownUnconfirmed('PostgreSQL shutdown diagnostic unavailable; composition retained')
        failure.diagnostic_write_failed=True
        raise failure from error

def container_inspection(api,target,rows):
    try:return api['inspect_container'](target)
    except Exception as error:
        rows.append({'container':target,'outcome':'unknown','reason':'container_inspection_failed'})
        record_shutdown_diagnostic(api['STACK'],rows)
        raise ShutdownUnconfirmed('PostgreSQL container inspection failed; composition retained') from error

def image_inspection(api,target,rows):
    try:
        value=api['inspect_image'](target)
        if not value or not value.get('Id') or not isinstance(value.get('Config'),dict):raise RuntimeError('Image identity is missing')
        return value
    except Exception as error:
        rows.append({'image_role':'postgres-candidate','outcome':'unknown','reason':'image_inspection_failed'})
        record_shutdown_diagnostic(api['STACK'],rows)
        raise ShutdownUnconfirmed('PostgreSQL image inspection failed') from error

def stop_confirmed(api,target,instance,role,rows,proof_verified=True):
    row={'container':target,'role':role,'outcome':'unknown'};rows.append(row)
    if not proof_verified:
        try:actual=container_inspection(api,target,rows)
        except Exception as error:
            record_shutdown_diagnostic(api['STACK'],rows);raise ShutdownUnconfirmed('Legacy stopped state inspection failed') from error
        state=(actual or {}).get('State',{})
        if not actual or actual.get('Id')!=instance.get('Id') or state.get('Running') is not False or state.get('Paused') or state.get('Restarting') or state.get('Dead') or state.get('Status') not in ('exited','created'):
            record_shutdown_diagnostic(api['STACK'],rows);raise ShutdownUnconfirmed('Legacy container is no longer confirmed stopped')
        row['outcome']='pre_stopped_unclassified';record_shutdown_diagnostic(api['STACK'],rows);return
    try:values=container_shutdown(api,instance,role)
    except Exception as error:
        record_shutdown_diagnostic(api['STACK'],rows);raise ShutdownUnconfirmed('PostgreSQL shutdown ownership is not confirmed') from error
    if values is None:
        record_shutdown_diagnostic(api['STACK'],rows);raise ShutdownUnconfirmed('Expected PostgreSQL shutdown contract is missing')
    budget=values['gateway_drain_ms']+values['cleanup_grace_ms'] if role=='gateway' else values['worker_stop_ms']
    seconds=(budget+999)//1000
    failure=None
    try:
        if instance.get('State',{}).get('Running'):
            before=container_inspection(api,target,rows)
            if not before or before.get('Id')!=instance['Id']:raise ShutdownUnconfirmed('PostgreSQL container identity changed before stop')
            api['docker']('stop','-t',str(seconds),target,timeout=seconds+(values['cleanup_grace_ms']+999)//1000)
    except (OSError,RuntimeError,api['subprocess'].CalledProcessError,api['subprocess'].TimeoutExpired) as error:failure=error
    try:actual=container_inspection(api,target,rows)
    except Exception as error:
        record_shutdown_diagnostic(api['STACK'],rows);raise ShutdownUnconfirmed('PostgreSQL stopped state inspection failed') from error
    state=(actual or {}).get('State',{})
    if failure:
        row.update(reason='stop_command_failed',exit_code=state.get('ExitCode'));record_shutdown_diagnostic(api['STACK'],rows)
        if isinstance(failure,ShutdownUnconfirmed):raise failure
        raise ShutdownUnconfirmed('PostgreSQL stop command failed; composition retained') from failure
    if actual and actual.get('Id')==instance.get('Id') and actual.get('Image')==instance.get('Image') and state.get('Running') is False and state.get('Status')=='exited' and not state.get('Paused') and not state.get('Restarting') and not state.get('Dead') and not state.get('OOMKilled') and not state.get('Error') and type(state.get('ExitCode'))==int and state['ExitCode'] in ([0,2] if role=='gateway' else [0]):
        row.update(outcome='interrupted_cleanup_complete' if state['ExitCode']==2 else 'normal',exit_code=state['ExitCode']);record_shutdown_diagnostic(api['STACK'],rows)
        return
    row['exit_code']=state.get('ExitCode');record_shutdown_diagnostic(api['STACK'],rows)
    raise ShutdownUnconfirmed('PostgreSQL shutdown is not confirmed; containers and materials retained')

def secure(path,uid,mode,directory=False):
    info=path.lstat()
    invalid_mode=(info.st_mode&0o022)!=0 if mode is None else stat.S_IMODE(info.st_mode)!=mode
    if stat.S_ISLNK(info.st_mode) or info.st_uid!=uid or invalid_mode or (not stat.S_ISDIR(info.st_mode) if directory else not stat.S_ISREG(info.st_mode)):
        raise ValueError('PostgreSQL protected material ownership or permissions are invalid')

MATERIAL_STORAGE_CONTRACT='harbor-material-storage-v1'
MATERIAL_CONFIG=Path('/etc/harbor-data-paths.json')
MATERIAL_MAINTENANCE=Path('/etc/harbor-material-maintenance.json')
MATERIAL_HELPER=Path('/usr/local/lib/harbor/harbor-material-storage.py')
def material_storage_guard(stack):
    if not os.path.lexists(MATERIAL_CONFIG) and not os.path.lexists(MATERIAL_MAINTENANCE):return {'enabled':False}
    import importlib.util
    for parent in MATERIAL_HELPER.parents:secure(parent,0,None,True)
    secure(MATERIAL_HELPER,0,0o644)
    spec=importlib.util.spec_from_file_location('harbor_material_storage_guard',MATERIAL_HELPER)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module.guard(['application-data','application-runtime'],expected={'application-data':Path(stack)/'pg-data','application-runtime':Path(stack)/'runtime'},required=True)

def configuration(config,stack):
    material_storage_guard(stack)
    if config.get('postgres_legacy_source_engine') not in (None,'sqlite'):raise ValueError('Legacy source engine configuration is invalid')
    for parent in [*Path(stack).parents,Path(stack)]:secure(parent,0,None,True)
    secure(Path(stack)/'secrets',0,0o700,True)
    env=Path(stack)/'secrets/postgres.env';secure(env,0,0o600)
    if env.stat().st_size>65536:raise ValueError('PostgreSQL environment exceeds budget')
    values={}
    allowed={'HARBOR_DATABASE_URL','HARBOR_RECORDING_STORAGE_MODE','HARBOR_RECORDING_STORAGE_MIN_FREE_BYTES','HARBOR_RECORDING_STORAGE_MIN_FREE_RATIO','HARBOR_RECORDING_CONTEXT_ENABLED'}
    for line in env.read_text().splitlines():
        if not line or line.startswith('#'):continue
        key,separator,value=line.partition('=')
        if not separator or key not in allowed or key in values or not value or '\x00' in value:raise ValueError('PostgreSQL environment is not a fixed configuration')
        values[key]=value
    if values.get('HARBOR_RECORDING_CONTEXT_ENABLED') not in (None,'0','1'):raise ValueError('PostgreSQL context capture flag must be 0 or 1')
    if values.get('HARBOR_RECORDING_STORAGE_MODE')!='report':raise ValueError('PostgreSQL signed storage reports are required')
    address=urlsplit(values.get('HARBOR_DATABASE_URL',''));query=parse_qs(address.query)
    if address.scheme not in ('postgres','postgresql') or not address.hostname or query.get('sslmode')!=['verify-full'] or query.get('sslrootcert')!=['/run/secrets/postgres-ca.crt']:
        raise ValueError('PostgreSQL must use the fixed verified TLS trust material')
    device_port=config.get('device_tls_port');device_origin=config.get('device_origin')
    if type(device_port)!=int or not 1<=device_port<=65535 or device_port==config['port'] or not isinstance(device_origin,str):raise ValueError('PostgreSQL device TLS endpoint is required')
    endpoint=urlsplit(device_origin)
    if endpoint.scheme!='https' or not endpoint.hostname or (endpoint.port or 443)!=device_port or endpoint.path or endpoint.query or endpoint.fragment or endpoint.username or endpoint.password:raise ValueError('PostgreSQL device TLS origin is invalid')
    device=Path(stack)/'secrets/device';secure(device,1000,0o700,True)
    for name,mode in [('server.key',0o600),('ca.key',0o600),('server.pem',0o644),('ca.pem',0o644)]:secure(device/name,1000,mode)
    mounts=[]
    for name,uid,mode in [('postgres-report.key',1000,0o600),('postgres-ca.crt',0,0o644)]:
        path=Path(stack)/'secrets'/name;secure(path,uid,mode);mounts+=['--mount','type=bind,src='+str(path)+',dst=/run/secrets/'+name+',readonly']
    if query.get('sslcert') or query.get('sslkey'):
        if query.get('sslcert')!=['/run/secrets/postgres-client.crt'] or query.get('sslkey')!=['/run/secrets/postgres-client.key']:raise ValueError('PostgreSQL client TLS paths are fixed')
        for name in ['postgres-client.crt','postgres-client.key']:
            path=Path(stack)/'secrets'/name;secure(path,1000,0o600);mounts+=['--mount','type=bind,src='+str(path)+',dst=/run/secrets/'+name+',readonly']
    for name in ['pg-data','runtime']:
        secure(Path(stack)/name,1000,0o700,True)
    for name in ['key-encryption.key','recording-encryption.key','settings.json']:
        secure(Path(stack)/'pg-data'/name,1000,0o600)
    secure(Path(stack)/'pg-data/recording-spool',1000,0o700,True)
    resources=config.get('postgres_resources',{})
    if not isinstance(resources,dict) or set(resources)-{'gateway','worker'}:raise ValueError('PostgreSQL resource configuration is invalid')
    budgets={}
    for kind,cpu,memory in [('gateway',3,6144),('worker',1,2048)]:
        value=resources.get(kind,{'cpus':cpu,'memory_mib':memory,'pids':256})
        if not isinstance(value,dict) or set(value)!={'cpus','memory_mib','pids'} or isinstance(value['cpus'],bool) or not isinstance(value['cpus'],(int,float)) or not .25<=value['cpus']<=64 or type(value['memory_mib'])!=int or not 512<=value['memory_mib']<=65536 or type(value['pids'])!=int or not 128<=value['pids']<=4096:raise ValueError('PostgreSQL resource configuration is invalid')
        budgets[kind]=value
    device_environment=['--env','HARBOR_DEVICE_TLS_ENABLED=1','--env','HARBOR_DEVICE_TLS_PORT='+str(device_port),'--env','HARBOR_DEVICE_PUBLIC_ORIGIN='+device_origin,'--env','HARBOR_DEVICE_TLS_KEY_FILE=/run/harbor-device/server.key','--env','HARBOR_DEVICE_TLS_CERT_FILE=/run/harbor-device/server.pem','--env','HARBOR_DEVICE_CA_CERT_FILE=/run/harbor-device/ca.pem','--env','HARBOR_DEVICE_CA_KEY_FILE=/run/harbor-device/ca.key','--env','HARBOR_PUBLIC_ORIGINS='+config['origin']+','+device_origin,'--mount','type=bind,src='+str(device)+',dst=/run/harbor-device,readonly']
    shutdown=shutdown_configuration(config.get('postgres_shutdown'))
    shutdown_environment=[part for key,name in SHUTDOWN_ENV.items() for part in ['--env',name+'='+str(shutdown[key])]]
    network=config.get('postgres_network')
    if network is not None and (not isinstance(network,str) or not re.fullmatch(r'[-a-zA-Z0-9_]{1,63}',network)):raise ValueError('PostgreSQL network is invalid')
    network_args=['--network',network] if network else []
    common=[*network_args,'--user','1000:1000','--cap-drop','ALL','--security-opt','no-new-privileges','--env-file',str(env),'--env','TZ=Asia/Shanghai','--env','HARBOR_DATA_DIR=/app/data','--env','HARBOR_RUNTIME_DIR=/app/runtime','--env','HARBOR_RECORDING_ENABLED=1','--env','HARBOR_RECORDING_WORKER_ENABLED=1','--env','HARBOR_RECORDING_STORAGE_REPORT_KEY_FILE=/run/secrets/postgres-report.key',*mounts,*device_environment,*shutdown_environment]
    return common,budgets

def image_proof_content(api,image,common):
    timeout=preflight_timeout_seconds(api.get('postgres_preflight_timeout_seconds',30))
    candidate=image_inspection(api,image,[])
    labels=(candidate or {}).get('Config',{}).get('Labels',{})
    if labels.get('com.harbor.ci-validation-only')=='true' or labels.get('com.harbor.storage.postgres')!=CONTRACT or labels.get('com.harbor.postgres-preflight')!='v1' or labels.get('com.harbor.extra-usage-dispatch')!=EXTRA_USAGE_DISPATCH_CONTRACT or labels.get('org.opencontainers.image.revision')!=image.rsplit(':',1)[-1]:raise ValueError('Candidate does not provide the PostgreSQL image capability')
    stack=api['STACK']
    nonce=secrets.token_hex(16)
    check_name=api['NAME']+'-postgres-check-'+nonce
    if container_inspection(api,check_name,[]):raise ValueError('PostgreSQL preflight container requires administrator inspection')
    args=['run','--name',check_name,'--label','com.aistrong.project='+api['NAME'],'--label','com.harbor.preflight=true','--label','com.harbor.preflight-nonce='+nonce,'--read-only','--tmpfs','/tmp:rw,noexec,nosuid,size=64m',*common,'--mount','type=bind,src='+stack+'/pg-data,dst=/app/data,readonly','--mount','type=bind,src='+stack+'/runtime,dst=/app/runtime,readonly',image,'node','/app/ci/postgres-preflight.mjs']
    completed=False
    try:
        value=json.loads(api['docker'](*args,timeout=timeout).stdout)
        completed=True
    finally:
        check=container_inspection(api,check_name,[])
        owned=check and check.get('Name')=='/'+check_name and check.get('Image')==candidate.get('Id') and check.get('Config',{}).get('Image')==image and check.get('Config',{}).get('Labels',{}).get('com.harbor.preflight')=='true' and check['Config']['Labels'].get('com.aistrong.project')==api['NAME'] and check['Config']['Labels'].get('com.harbor.preflight-nonce')==nonce
        stopped=owned and check.get('State',{}).get('Running') is False and check['State'].get('Status')=='exited' and check['State'].get('ExitCode')==0 and not any(check['State'].get(k) for k in ['Paused','Restarting','Dead','OOMKilled'])
        if completed and not stopped:raise ShutdownUnconfirmed('PostgreSQL preflight exit identity is not confirmed')
        if owned:
            # --rm couples a valid proof to daemon filesystem cleanup. A confirmed
            # exited probe can be retained when cleanup is slow; never suppress an
            # unknown/running probe failure or force-remove the successful probe.
            try:api['docker'](*(['rm',check_name] if stopped else ['rm','-f',check_name]),check=False,timeout=10)
            except subprocess.TimeoutExpired:
                if not (completed and stopped):raise
    return validate_image_proof(value,image,common)

def validate_image_proof(value,image,common):
    if not isinstance(value,dict) or value.get('contract')!=CONTRACT or value.get('extra_usage_dispatch_contract')!=EXTRA_USAGE_DISPATCH_CONTRACT or value.get('storage_verified') is not True or value.get('credentials_verified') is not True or value.get('device_tls_verified') is not True or value.get('revision')!=image.rsplit(':',1)[-1] or any(not isinstance(value.get(key),str) or not re.fullmatch('[a-f0-9]{64}',value[key]) for key in ['schema_digest','database_digest','source_digest','device_ca_digest']):raise ValueError('Candidate PostgreSQL preflight proof is invalid')
    if 'department_isolation_contract' in value:
        if value['department_isolation_contract']!='harbor-department-isolation-v1' or type(value.get('department_isolation_installed')) is not bool:raise ValueError('Candidate department proof is invalid')
    elif 'department_isolation_installed' in value:raise ValueError('Candidate department contract is missing')
    requested={key:int(next(item.split('=',1)[1] for item in common if item.startswith(name+'='))) for key,name in SHUTDOWN_ENV.items()}
    if 'shutdown' not in value:raise ShutdownContractUnsupported('Validated legacy PostgreSQL image has no shutdown contract')
    shutdown=value['shutdown']
    if not isinstance(shutdown,dict) or not isinstance(shutdown.get('contract'),str) or not shutdown['contract']:raise ValueError('Malformed shutdown proof')
    if shutdown['contract']!=SHUTDOWN_CONTRACT:raise ShutdownContractUnsupported('Validated legacy PostgreSQL image has an unsupported shutdown contract')
    if shutdown!={'contract':SHUTDOWN_CONTRACT,**requested}:raise ValueError('Candidate PostgreSQL shutdown values mismatch')
    return value

def image_proof(api,image,common):
    try:return image_proof_content(api,image,common)
    except (ShutdownUnconfirmed,ShutdownContractUnsupported):raise
    except Exception as error:
        record_shutdown_diagnostic(api['STACK'],[{'image_role':'postgres-candidate','outcome':'unknown','reason':'image_proof_unavailable'}])
        raise ShutdownUnconfirmed('PostgreSQL capability or runtime proof unavailable') from error

def source_preflight_material_identity(stack):
    root=Path(stack);names=['secrets/postgres.env','secrets/postgres-report.key','secrets/postgres-ca.crt','secrets/device/server.key','secrets/device/server.pem','secrets/device/ca.key','secrets/device/ca.pem','pg-data/key-encryption.key','pg-data/recording-encryption.key','pg-data/settings.json']
    names += [name for name in ['secrets/postgres-client.crt','secrets/postgres-client.key'] if os.path.lexists(root/name)]
    result=[]
    for name in names:
        fd=os.open(root/name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        with os.fdopen(fd,'rb') as file:
            before=os.fstat(file.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size>65536:raise ShutdownUnconfirmed('Source preflight material is invalid')
            raw=file.read(65537);after=os.fstat(file.fileno())
            identity=lambda row:(row.st_dev,row.st_ino,row.st_uid,row.st_mode,row.st_size,row.st_mtime_ns,row.st_ctime_ns)
            if len(raw)>65536 or identity(before)!=identity(after):raise ShutdownUnconfirmed('Source preflight material changed')
            result.append((name,identity(after),hashlib.sha256(raw).digest()))
    return tuple(result)

def running_source_image_proof(api,instance,common):
    # The existing source runs the same full verifier, including fresh signed
    # storage reports, credentials and device state. No new probe container.
    expected={}
    for index,item in enumerate(common[:-1]):
        if item=='--env-file':
            for line in Path(common[index+1]).read_text().splitlines():
                if line and not line.startswith('#'):
                    key,_,value=line.partition('=');expected[key]=value
        elif item=='--env':
            key,_,value=common[index+1].partition('=');expected[key]=value
    environment=dict(item.split('=',1) for item in instance.get('Config',{}).get('Env',[]) if '=' in item)
    if not expected.get('HARBOR_DATABASE_URL') or environment.get('HARBOR_DATABASE_URL')!=expected['HARBOR_DATABASE_URL']:
        # A changed connection must use the original isolated check with the
        # requested environment; never send connection credentials as argv.
        return image_proof(api,instance['Config']['Image'],common)
    overrides={key:expected.get(key) for key in ['HARBOR_RECORDING_STORAGE_MODE','HARBOR_RECORDING_STORAGE_MIN_FREE_BYTES','HARBOR_RECORDING_STORAGE_MIN_FREE_RATIO','HARBOR_RECORDING_CONTEXT_ENABLED']}
    overrides.update({key:value for key,value in expected.items() if key!='HARBOR_DATABASE_URL'})
    timeout=preflight_timeout_seconds(api.get('postgres_preflight_timeout_seconds',30))
    program="import {postgresPreflight} from '/app/ci/postgres-preflight.mjs';const env={...process.env};for(const [key,value] of Object.entries(JSON.parse(process.argv[1]))){if(value===null)delete env[key];else env[key]=value;}const deadline=setTimeout(()=>{console.error('Source runtime preflight timed out');process.exit(1);},SOURCE_TIMEOUT_MS);try{console.log(JSON.stringify(await postgresPreflight({env})));}catch{console.error('Source runtime preflight failed');process.exitCode=1;}finally{clearTimeout(deadline);}".replace('SOURCE_TIMEOUT_MS',str(timeout*1000))
    try:
        result=api['docker']('exec',instance['Id'],'node','--input-type=module','-e',program,json.dumps(overrides),timeout=timeout)
        return validate_image_proof(json.loads(result.stdout),instance['Config']['Image'],common)
    except (ShutdownUnconfirmed,ShutdownContractUnsupported):raise
    except Exception as error:raise ShutdownUnconfirmed('Fresh source runtime proof unavailable') from error

def prepare_source_image_preflights(api,sources,common):
    # Only the same locked upgrade process receives this closure. Never persist
    # or accept a caller-supplied proof; changes reject before stopping a member.
    materials=source_preflight_material_identity(api['STACK']);proofs={};bindings={}
    for instance,role in sources:
        values=container_shutdown(api,instance,role)
        if values is None:raise ShutdownUnconfirmed('Source shutdown contract requires controlled maintenance')
        requested=[next((label+'='+str(values[key]) for key,label in SHUTDOWN_ENV.items() if item.startswith(label+'=')),item) for item in common]
        image=instance['Config']['Image'];image_id=instance['Image'];key=(image,image_id,tuple(requested))
        if (image_inspection(api,image,[]) or {}).get('Id')!=image_id:raise ShutdownUnconfirmed('Source preflight image changed')
        if key not in proofs:proofs[key]=image_proof(api,image,requested)
        bindings[instance['Id']]=key
    if source_preflight_material_identity(api['STACK'])!=materials:raise ShutdownUnconfirmed('Source preflight material changed')
    def verified(instance,requested):
        try:
            key=(instance.get('Config',{}).get('Image'),instance.get('Image'),tuple(requested))
            if bindings.get(instance.get('Id'))!=key or source_preflight_material_identity(api['STACK'])!=materials or (image_inspection(api,key[0],[]) or {}).get('Id')!=key[1]:raise ShutdownUnconfirmed('Prepared source preflight identity changed')
            fresh=running_source_image_proof(api,instance,requested)
            if any(fresh.get(name)!=proofs[key].get(name) for name in ['contract','revision','source_digest','schema_digest','database_digest','device_ca_digest','shutdown','department_isolation_contract','department_isolation_installed']):raise ShutdownUnconfirmed('Source runtime proof changed')
            return fresh
        except Exception as error:raise SourcePreflightRejected('Source preflight rejected before composition change') from error
    return verified

def marker(stack,proof):
    path=Path(stack)/'postgres-storage.json'
    expected={key:proof[key] for key in ['contract','database_digest']}
    if os.path.lexists(path):
        secure(path,0,0o600)
        if json.loads(path.read_text())!=expected:raise ValueError('PostgreSQL database identity changed')
    else:
        descriptor,name=tempfile.mkstemp(prefix='.postgres-storage-',dir=stack)
        try:
            with os.fdopen(descriptor,'w') as target:
                json.dump(expected,target);target.flush();os.fsync(target.fileno())
            os.replace(name,path)
        finally:
            Path(name).unlink(missing_ok=True)
    directory=os.open(stack,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(directory)
    finally:os.close(directory)

def device_http_code(stack,config,path,headers=None):
    endpoint=urlsplit(config['device_origin'])
    try:
        context=ssl.create_default_context(cafile=str(Path(stack)/'secrets/device/ca.pem'))
        with socket.create_connection(('127.0.0.1',config['device_tls_port']),timeout=5) as raw:
            with context.wrap_socket(raw,server_hostname=endpoint.hostname) as connection:
                values={'Host':endpoint.netloc,'Connection':'close',**(headers or {})}
                request='GET '+path+' HTTP/1.1\r\n'+''.join(key+': '+value+'\r\n' for key,value in values.items())+'\r\n'
                connection.sendall(request.encode('ascii'))
                response=http.client.HTTPResponse(connection);response.begin();return response.status
    except (OSError,ValueError,http.client.HTTPException):return 0

def context_environment_matches(instance,stack):
    key='HARBOR_RECORDING_CONTEXT_ENABLED'
    expected=[line.partition('=')[2] for line in (Path(stack)/'secrets/postgres.env').read_text().splitlines() if line.startswith(key+'=')]
    actual=[value.partition('=')[2] for value in (instance or {}).get('Config',{}).get('Env',[]) if value.startswith(key+'=')]
    # Docker environments are immutable. Missing is the supported disabled default,
    # while duplicate or malformed values must never count as an applied setting.
    same=len(expected)<=1 and len(actual)<=1 and all(value in ('0','1') for value in expected+actual) and (expected or ['0'])==(actual or ['0'])
    return same and (expected!=['1'] or context_capture_guard(stack))

def context_flag(values):
    found=[value.partition('=')[2] for value in values if value.startswith('HARBOR_RECORDING_CONTEXT_ENABLED=')]
    if len(found)>1 or any(value not in ('0','1') for value in found):raise ValueError('Context capture environment is ambiguous')
    return found==['1']

def context_capture_guard(stack,enabled=None):
    path=Path(stack)/'pg-data/settings.json';secure(path,1000,0o600)
    if path.stat().st_size>65536:raise ValueError('Startup configuration exceeds budget')
    config=json.loads(path.read_text());allowed=config.get('recordingContextAllowed')
    if 'recordingContextAllowed' in config and type(allowed) is not bool:raise ValueError('Context capture startup policy is invalid')
    if enabled is None:return allowed is not False
    if type(enabled) is not bool:raise ValueError('Context capture startup policy must be boolean')
    if allowed is enabled:return enabled
    original=path.stat();descriptor,name=tempfile.mkstemp(prefix='.context-capture-',dir=path.parent)
    try:
        with os.fdopen(descriptor,'w') as target:
            os.fchmod(target.fileno(),0o600);os.fchown(target.fileno(),original.st_uid,original.st_gid)
            config['recordingContextAllowed']=enabled;json.dump(config,target);target.flush();os.fsync(target.fileno())
        os.replace(name,path)
        directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(directory)
        finally:os.close(directory)
    finally:Path(name).unlink(missing_ok=True)
    return enabled

def rollback_environment_matches(instance,stack,guard_supported=False):
    # Existing containers keep their original Env even after an env-file changes.
    # The image proof uses the new file, so it cannot authenticate those old Env values.
    expected={}
    for line in (Path(stack)/'secrets/postgres.env').read_text().splitlines():
        key,separator,value=line.partition('=')
        if separator and key in ('HARBOR_DATABASE_URL','HARBOR_RECORDING_STORAGE_MODE'):expected[key]=value
    values=(instance or {}).get('Config',{}).get('Env',[])
    same_database=set(expected)=={'HARBOR_DATABASE_URL','HARBOR_RECORDING_STORAGE_MODE'} and all([item for item in values if item.startswith(key+'=')]==[key+'='+value] for key,value in expected.items())
    if not same_database:return False
    try:
        requested=context_flag((Path(stack)/'secrets/postgres.env').read_text().splitlines());previous=context_flag(values)
        # Rolling back an attempted enable to disabled is safe. A formerly enabled
        # container may restart after disable only when it implements the durable veto.
        return not previous or requested or guard_supported and not context_capture_guard(stack)
    except ValueError:return False

def signed_candidate_image_id(image_id,approval):
    return isinstance(image_id,str) and re.fullmatch(r'sha256:[a-f0-9]{64}',image_id) is not None and image_id in (approval.get('configDigest'),approval.get('manifestDigest'))

def verify_candidate_image(inspection,approval,image):
    identity=(inspection or {}).get('Id')
    if not signed_candidate_image_id(identity,approval):raise ValueError('Candidate config digest or manifest digest differs from activation')
    if identity!=approval['configDigest']:
        reference=image.rsplit(':',1)[0]+'@'+approval['manifestDigest']
        if not isinstance(inspection.get('Descriptor'),dict) or inspection['Descriptor'].get('digest')!=approval['manifestDigest'] or not isinstance(inspection.get('RepoDigests'),list) or reference not in inspection['RepoDigests']:raise ValueError('Candidate manifest descriptor or approved reference differs from activation')
    return identity

def verify_started_composition(api,image,config,started,approval):
    stack=api['STACK'];name=api['NAME'];worker=name+'-worker';port=config['port']
    end=time.monotonic()+90
    while time.monotonic()<end:
        candidate,background=container_inspection(api,name,[]),container_inspection(api,worker,[])
        good=all(value and signed_candidate_image_id(value.get('Image'),approval) and value.get('Config',{}).get('Image')==image and value.get('State',{}).get('Running') and value['State'].get('Health',{}).get('Status')=='healthy' for value in [candidate,background])
        if good and api['http_code'](port,'/health')==200 and api['http_code'](port,'/ready')==200 and device_http_code(stack,config,'/ready')==200:
            progress=api['docker']('exec',worker,'node','/app/ci/postgres-preflight.mjs','--progress-since',str(started),check=False,timeout=10)
            if progress.returncode==0:break
        time.sleep(2)
    else:raise RuntimeError('PostgreSQL gateway/Worker readiness or progress failed')
    if api['http_code'](port,'/api/admin/state')!=401 or api['http_code'](port,'/health',{'Host':'untrusted.invalid'})!=403 or device_http_code(stack,config,'/api/admin/state')!=401 or device_http_code(stack,config,'/health',{'Host':'untrusted.invalid'})!=403:raise RuntimeError('PostgreSQL authentication checks failed')

STORAGE_STARTUP_CONTRACT='harbor-pg-storage-boot-v1'

def storage_restart_policy(config):
    enabled=config.get('postgres_storage_startup_guard',False)
    if type(enabled) is not bool:raise ValueError('PostgreSQL storage startup guard must be a root boolean')
    if not enabled:
        state=Path('/sdc-disk/harbor-data/.pg-storage-state.json')
        if os.path.lexists(state):
            for item in [state,*state.parents]:
                info=item.lstat()
                if info.st_uid!=0 or info.st_mode&0o022 or stat.S_ISLNK(info.st_mode):raise ValueError('PostgreSQL storage guard state is not root controlled')
            info=state.stat()
            if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode)!=0o600 or info.st_nlink!=1 or info.st_size>262144:raise ValueError('PostgreSQL storage guard state is not private')
            current=json.loads(state.read_bytes())
            if current.get('scope')!='harbor-pg-storage-move-v1':raise ValueError('PostgreSQL storage guard state is unknown')
            if any(type(current.get(key,False)) is not bool for key in ['oldDataRollbackForbidden','targetBootAuthorized']):raise ValueError('PostgreSQL storage guard flags are invalid')
            legacy_phases={'planned','archive-precopying','archive-prepared','holding-intent','held','gated','writers-paused','autostart-fenced','applications-stopped','pg-stop-intent','source-cold','cold-verified','bound','recovering-source','source-restored'}
            if current.get('oldDataRollbackForbidden') is True or current.get('targetBootAuthorized') is True or current.get('phase') not in legacy_phases:raise ValueError('Migrated PostgreSQL still requires its instance startup guard')
        return 'unless-stopped'
    # Only the protected host configuration can request this. The fixed helper
    # proves the completed physical move and the current instance's boot guard.
    path=Path('/usr/local/lib/harbor/harbor-pg-storage-host.py')
    for item in [path,*path.parents]:
        info=item.lstat()
        if info.st_uid!=0 or info.st_mode&0o022 or stat.S_ISLNK(info.st_mode):raise ValueError('PostgreSQL storage startup guard is not root controlled')
    import importlib.util
    spec=importlib.util.spec_from_file_location('harbor_storage_startup_guard',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    result=module.verify_factory_guard(config)
    if type(result) is not dict or result!={'contract':STORAGE_STARTUP_CONTRACT,'completed':True,'samePostgres':True,'mountsVerified':True} or any(type(result.get(k)) is not bool for k in ['completed','samePostgres','mountsVerified']):raise ValueError('PostgreSQL storage startup guard is not ready')
    return 'no'

def deploy(image,config,archive,api):
    restart_policy=storage_restart_policy(config)
    stack=api['STACK'];name=api['NAME'];worker=name+'-worker';port=config['port'];docker=api['docker'];entry_deployment=api.get('entry_deployment');shutdown_rows=[];inspect=lambda target:container_inspection(api,target,shutdown_rows)
    if not callable(api.get('activation')):raise ValueError('PostgreSQL activation consumer is required')
    approval=api['activation']('check',image)
    if entry_deployment is not None:entry_deployment.check(approval)
    if type(api.get('runtime_upgrade',False)) is not bool or type(api.get('retain_previous',False)) is not bool:raise ValueError('Internal PostgreSQL deployment modes must be boolean')
    expected_contract='harbor-postgres-runtime-upgrade-v1' if api.get('runtime_upgrade') is True else ('harbor-local-assurance-deploy-v1' if api.get('local_assurance') is True and config.get('local_assurance_release') is True else 'harbor-storage-activation-deploy-v1')
    if approval.get('contract')!=expected_contract or not re.fullmatch(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}',approval.get('deploymentId','')) or not re.fullmatch(r'[a-f0-9]{64}',approval.get('receiptDigest','')) or not re.fullmatch(r'sha256:[a-f0-9]{64}',approval.get('configDigest','')) or not re.fullmatch(r'sha256:[a-f0-9]{64}',approval.get('manifestDigest','')) or approval.get('revision')!=image.rsplit(':',1)[-1]:raise ValueError('Activation approval is invalid')
    def activate(action):
        value=api['activation'](action,image)
        if any(value.get(key)!=approval.get(key) for key in ('contract','deploymentId','receiptDigest','configDigest','manifestDigest','revision')):raise ShutdownUnconfirmed('Activation identity changed; composition retained')
        return value
    def owned(row):
        labels=(row or {}).get('Config',{}).get('Labels',{})
        return bool(row and signed_candidate_image_id(row.get('Image'),approval) and row.get('Config',{}).get('Image')==image and labels.get('com.aistrong.project')==name and labels.get('com.harbor.activation-deployment-id')==approval['deploymentId'] and labels.get('com.harbor.activation-receipt')==approval['receiptDigest'])
    def reclaim_candidates(targets):
        identities={}
        for target in targets:
            row=inspect(target)
            if row is None:continue
            if not owned(row):
                shutdown_rows.append({'container':target,'outcome':'unknown','reason':'candidate_ownership_changed'});record_shutdown_diagnostic(stack,shutdown_rows);raise ShutdownUnconfirmed('Candidate ownership changed; composition retained')
            state=row.get('State',{});never_started=state.get('Running') is False and state.get('Status')=='created' and state.get('StartedAt')=='0001-01-01T00:00:00Z' and not any(state.get(key) for key in ('Paused','Restarting','Dead','OOMKilled'))
            if not never_started:stop_confirmed(api,target,row,'gateway' if target==name else 'worker',shutdown_rows)
            identities[target]=row['Id']
        for target,identity in identities.items():
            row=inspect(target)
            if not owned(row) or row['Id']!=identity or row.get('State',{}).get('Running') is not False:
                shutdown_rows.append({'container':target,'outcome':'unknown','reason':'candidate_changed_after_stop'});record_shutdown_diagnostic(stack,shutdown_rows);raise ShutdownUnconfirmed('Candidate changed after stop; composition retained')
        for identity in identities.values():docker('rm',identity)
        for target in identities:
            if inspect(target) is not None:raise ShutdownUnconfirmed('Candidate removal is not confirmed; entry remains held')
        return identities
    def revoke_reclaimed(identities):
        value=activate('revoke')
        if entry_deployment is not None and identities:
            entry_deployment.unbind_candidate(activation=value,removed_container_ids=list(identities.values()))
        return value
    common,budgets=configuration(config,stack)
    current,old_worker=inspect(name),inspect(worker)
    for instance in [current,old_worker]:
        if instance and instance.get('Config',{}).get('Labels',{}).get('com.aistrong.project')!=name:raise ValueError('Existing container is not owned by this service')
    sqlite_source=[legacy_sqlite_source(api,instance,role,config) for instance,role in [(current,'gateway'),(old_worker,'worker')]]
    if sqlite_source[0] and old_worker is not None:raise ShutdownUnconfirmed('Legacy SQLite source has an unexpected Worker composition')
    old_shutdown=[None if sqlite else container_shutdown(api,instance,role) for (instance,role),sqlite in zip([(current,'gateway'),(old_worker,'worker')],sqlite_source)]
    bindings=(current or {}).get('HostConfig',{}).get('PortBindings',{})
    for required_port in [port,config['device_tls_port']]:
        if not any(item.get('HostPort')==str(required_port) for entries in bindings.values() for item in (entries or [])) and api['run']('/usr/bin/ss','-lntH','sport = :'+str(required_port)).stdout.strip():raise ValueError('Service port is occupied')
    # Activation has already qualified the installed OCI image. Reimporting a
    # docker-archive can rewrite its immutable manifest under snapshotter stores.
    if archive:api['validate_archive'](archive,image)
    verify_candidate_image(image_inspection(api,image,[]),approval,image)
    proof=activate('preflight').get('proof')
    if not isinstance(proof,dict) or proof.get('revision')!=approval['revision']:raise ValueError('Real candidate preflight is required')
    previous,previous_worker=inspect(name+'-previous'),inspect(worker+'-previous')
    if previous_worker:raise ShutdownUnconfirmed('Unexpected previous Worker requires inspection')
    if owned(current) or owned(old_worker) or previous:
        source=approval.get('sourceFence') or {}
        if previous and (previous.get('Id')!=source.get('sourceContainerId') or previous.get('Image')!=source.get('sourceImageId') or not legacy_sqlite_source(api,previous,'gateway',config)):raise ShutdownUnconfirmed('Previous source generation is not approved')
        if current and not owned(current) or old_worker and not owned(old_worker):raise ShutdownUnconfirmed('Mixed deployment generations require inspection')
        if any(row and not context_environment_matches(row,stack) for row in (current,old_worker)):
            raise ShutdownUnconfirmed('Resumed candidate context configuration changed; existing composition retained')
        pair_healthy=owned(current) and owned(old_worker) and all(row.get('State',{}).get('Running') and row['State'].get('Health',{}).get('Status')=='healthy' for row in (current,old_worker))
        if entry_deployment is not None and pair_healthy:entry_deployment.bind_candidate(current)
        if pair_healthy and api['http_code'](port,'/ready')==200 and device_http_code(stack,config,'/ready')==200:
            if activate('status').get('state')!='active':raise ShutdownUnconfirmed('Healthy containers lack active authorization')
            try:verify_started_composition(api,image,config,int(time.time()*1000),approval)
            except Exception:
                try:revoke_reclaimed(reclaim_candidates([name,worker]))
                except Exception as cleanup_error:raise ShutdownUnconfirmed('Resumed candidate revocation unconfirmed; retain migration material') from cleanup_error
                raise
            return {'shutdown_contract':SHUTDOWN_CONTRACT,'shutdown':[],'activation':{'state':'active','resumed':True}}
        removed=[]
        for target,row,role in ((name,current,'gateway'),(worker,old_worker,'worker')):
            if row:
                state=row.get('State',{});never_started=state.get('Running') is False and state.get('Status')=='created' and state.get('StartedAt')=='0001-01-01T00:00:00Z' and not any(state.get(key) for key in ('Paused','Restarting','Dead','OOMKilled'))
                if not never_started:stop_confirmed(api,target,row,role,shutdown_rows,proof_verified=True)
                removed.append((target,row['Id']))
        for target,identity in removed:
            row=inspect(target)
            if not owned(row) or row['Id']!=identity or row.get('State',{}).get('Running') is not False:raise ShutdownUnconfirmed('Resumed candidate changed after stop')
        for target,identity in removed:docker('rm',identity)
        if previous:docker('rename',name+'-previous',name)
        current,old_worker=inspect(name),inspect(worker)
        sqlite_source=[legacy_sqlite_source(api,instance,role,config) for instance,role in [(current,'gateway'),(old_worker,'worker')]]
        old_shutdown=[None if sqlite else container_shutdown(api,instance,role) for (instance,role),sqlite in zip([(current,'gateway'),(old_worker,'worker')],sqlite_source)]
    if inspect(name+'-previous') or inspect(worker+'-previous'):raise ValueError('Previous PostgreSQL containers require administrator inspection')
    old_proofs=[]
    for instance,values,sqlite in zip([current,old_worker],old_shutdown,sqlite_source):
        previous=None
        if instance and not sqlite:
            old_common=[next((label+'='+str(values[key]) for key,label in SHUTDOWN_ENV.items() if values is not None and item.startswith(label+'=')),item) for item in common]
            try:previous=api['source_image_preflight'](instance,old_common) if 'source_image_preflight' in api else image_proof(api,instance['Config']['Image'],old_common)
            except ShutdownContractUnsupported as error:
                if instance.get('State',{}).get('Running'):raise ShutdownUnconfirmed('Running legacy shutdown contract requires controlled maintenance') from error
        old_proofs.append(previous)
    context_requested=context_flag((Path(stack)/'secrets/postgres.env').read_text().splitlines())
    old_context=any(context_flag((instance or {}).get('Config',{}).get('Env',[])) for instance in (current,old_worker))
    if context_requested and proof.get('context_capture_guard_contract')!='v1':raise ValueError('Candidate lacks durable context capture policy')
    if old_context and any(context_flag((instance or {}).get('Config',{}).get('Env',[])) and (old_proof or {}).get('context_capture_guard_contract')!='v1' for instance,old_proof in zip((current,old_worker),old_proofs)):
        raise ValueError('Enabled source lacks durable context capture policy; source retained')
    context_guard_before=context_capture_guard(stack) if context_requested or old_context else None
    marker(stack,proof)
    stopped=[];created=[];rollback=False
    originals={target:existing['Config']['Image'] for target,existing in [(name,current),(worker,old_worker)] if existing}
    try:
        # Persist the restrictive policy before any old member is stopped. Both
        # candidate and rollback containers read the same startup file.
        if context_requested or old_context:context_capture_guard(stack,context_requested)
        rollback=entry_deployment is None and bool(all(rollback_environment_matches(instance,stack,(old_proof or {}).get('context_capture_guard_contract')=='v1') for instance,old_proof in zip((current,old_worker),old_proofs)) and all(value is not None for value in old_shutdown) and current and old_worker and current.get('Config',{}).get('Image')==old_worker.get('Config',{}).get('Image') and all(value is not None and all(value[key]==proof[key] for key in ['contract','schema_digest','database_digest','device_ca_digest']) for value in old_proofs))
        if activate('consume').get('state')!='active':raise ValueError('Database activation was not consumed')
        for index,(target,existing) in enumerate([(name,current),(worker,old_worker)]):
            if existing:
                stop_confirmed(api,target,existing,'gateway' if target==name else 'worker',shutdown_rows,proof_verified=old_proofs[index] is not None and old_shutdown[index] is not None);docker('rename',target,target+'-previous');stopped.append(target)
        if api.get('usage_cascade_release') is not None:
            try:api['usage_cascade_release'].repair()
            except Exception as repair_error:
                if api['usage_cascade_release'].uncertain:raise ShutdownUnconfirmed('Usage cascade repair outcome is unknown; stopped composition retained') from repair_error
                raise
        if api.get('department_release') is not None:
            try:api['department_release'].install()
            except Exception as install_error:
                if api['department_release'].uncertain:raise ShutdownUnconfirmed('Department activation outcome is unknown; stopped composition retained') from install_error
                raise
        started=int(time.time()*1000)
        for target,kind,entry in [(name,'gateway','postgres-gateway.mjs'),(worker,'worker','postgres-worker.mjs')]:
            budget=budgets[kind];shutdown=shutdown_configuration(config.get('postgres_shutdown'));stop_ms=shutdown['gateway_drain_ms']+shutdown['cleanup_grace_ms'] if kind=='gateway' else shutdown['worker_stop_ms'];args=['run','-d','--entrypoint','','--stop-signal','SIGTERM','--name',target,'--label','com.aistrong.project='+name,'--label','com.harbor.activation-deployment-id='+approval['deploymentId'],'--label','com.harbor.activation-receipt='+approval['receiptDigest'],'--label','com.harbor.shutdown-contract='+SHUTDOWN_CONTRACT,'--label','com.harbor.shutdown-role='+kind,'--stop-timeout',str((stop_ms+999)//1000),'--restart',restart_policy,'--memory',str(budget['memory_mib'])+'m','--cpus',str(budget['cpus']),'--pids-limit',str(budget['pids']),*common,'--mount','type=bind,src='+stack+'/pg-data,dst=/app/data','--mount','type=bind,src='+stack+'/runtime,dst=/app/runtime']
            if kind=='gateway':args+=['--env','HARBOR_LISTEN_HOST=0.0.0.0','--env','HARBOR_PORT='+str(port),'--publish',str(port)+':'+str(port),'--publish',str(config['device_tls_port'])+':'+str(config['device_tls_port'])]
            else:args+=['--health-cmd','node /app/ci/postgres-preflight.mjs --worker-health','--health-interval','15s','--health-timeout','5s','--health-start-period','30s']
            created.append(target);docker(*args,image,'node','/app/'+entry)
        if entry_deployment is not None:entry_deployment.bind_candidate(inspect(name))
        verify_started_composition(api,image,config,started,approval)
    except Exception as error:
        # An unsuccessful enable must not undo a prior disable, including when
        # that disable restored a container whose immutable Env is still 1.
        if context_requested and context_guard_before is False:
            try:context_capture_guard(stack,False)
            except Exception as restore_error:raise ShutdownUnconfirmed('Context capture veto restoration unconfirmed; composition retained') from restore_error
        if isinstance(error,ShutdownUnconfirmed):raise
        # Finish candidate intake and durable cleanup before removing either member.
        # Unknown cleanup retains the whole composition and blocks automatic rollback.
        reclaimed=reclaim_candidates(created)
        if api.get('department_release') is not None:
            try:api['department_release'].rollback_guard()
            except Exception as rollback_error:raise ShutdownUnconfirmed('Department rollback compatibility is unconfirmed; stopped composition retained') from rollback_error
        if api.get('usage_cascade_release') is not None:
            try:api['usage_cascade_release'].rollback()
            except Exception as rollback_error:raise ShutdownUnconfirmed('Usage cascade rollback is unconfirmed; stopped composition retained') from rollback_error
        for target,previous_image in originals.items():
            previous=inspect(target+'-previous');restored=inspect(target)
            if previous and previous.get('Config',{}).get('Image')==previous_image and previous.get('Config',{}).get('Labels',{}).get('com.aistrong.project')==name and not restored:
                docker('rename',target+'-previous',target);restored=inspect(target)
            if rollback and restored and restored.get('Config',{}).get('Image')==previous_image and restored.get('Config',{}).get('Labels',{}).get('com.aistrong.project')==name and not restored.get('State',{}).get('Running'):
                docker('start',target)
        if not rollback:
            try:revoke_reclaimed(reclaimed)
            except Exception as cleanup_error:raise ShutdownUnconfirmed('Activation revocation unconfirmed; retain PG data and migration material') from cleanup_error
        raise
    if entry_deployment is None and not api.get('retain_previous',False):
        for target in stopped:docker('rm',target+'-previous')
    return {'shutdown_contract':SHUTDOWN_CONTRACT,'shutdown':shutdown_rows,'activation':{'state':'active','resumed':False}}


def activation_preflight_main():
    import importlib.util
    import sys
    if os.getuid()!=0 or sys.argv[1:2]!=['--activation-preflight'] or len(sys.argv)!=3:raise ValueError('Fixed root activation preflight required')
    config_path=Path('/etc/harbor-gateway-deploy.json');secure(config_path,0,0o600)
    if config_path.stat().st_size>65536:raise ValueError('Deployment configuration exceeds limit')
    config=json.loads(config_path.read_text())
    if config.get('storage_engine')!='postgres':raise ValueError('PostgreSQL deployment configuration required')
    spec=importlib.util.spec_from_file_location('activation_wrapper',Path(__file__).with_name('deploy-wrapper.py'));wrapper=importlib.util.module_from_spec(spec);spec.loader.exec_module(wrapper)
    request=json.loads(sys.stdin.buffer.read(4097))
    if not isinstance(request,dict) or set(request)!={'deploymentId','receiptDigest','configDigest','manifestDigest'} or not re.fullmatch(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}',request.get('deploymentId','')) or not re.fullmatch(r'[a-f0-9]{64}',request.get('receiptDigest','')) or any(not isinstance(request.get(key),str) or not re.fullmatch(r'sha256:[a-f0-9]{64}',request[key]) for key in ('configDigest','manifestDigest')):raise ValueError('Fixed preflight identity required')
    image=wrapper.validate({'action':'deploy','image':sys.argv[2]},config);api=vars(wrapper);common,_=configuration(config,api['STACK']);common+=['--label','com.harbor.activation-deployment-id='+request['deploymentId'],'--label','com.harbor.activation-receipt='+request['receiptDigest']];before=verify_candidate_image(image_inspection(api,image,[]),request,image);proof=image_proof(api,image,common);actual=image_inspection(api,image,[]);image_id=verify_candidate_image(actual,request,image)
    if image_id!=before:raise ValueError('Candidate image changed during preflight')
    print(json.dumps({'contract':'harbor-activation-preflight-v1','imageId':image_id,'proof':proof},sort_keys=True))

if __name__=='__main__':
    activation_preflight_main()
