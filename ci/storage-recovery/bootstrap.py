#!/usr/bin/env python3
"""Explicit infrastructure preparation; never migrates, fences or activates Harbor."""
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import sys
from urllib.parse import urlsplit

CONFIG = Path('/etc/harbor-postgres-bootstrap.json')
ROOT = Path('/opt/product-development/harbor-local-gateway/postgres-infra')
PROJECT = 'harbor-local-gateway'
CONTRACT = 'postgres18-report-v1'
ARCHIVE_PROFILE = Path('/etc/harbor-storage-archive.json')
ARCHIVE_BACKUP_KEY = Path('/etc/harbor-storage-backup.key')
ARCHIVE_COMMAND = '/usr/local/bin/node /opt/harbor-storage-report/storage-backup.mjs archive-wal %f'
DIGEST = re.compile(r'^[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}$')

def require(condition, code):
    if not condition: raise ValueError(code)

def protected(path, uid=0, mode=None, directory=False):
    path=Path(path); info=path.lstat()
    require(not path.is_symlink() and (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)), 'bootstrap_material_type')
    require(info.st_uid==uid and (stat.S_IMODE(info.st_mode)==mode if mode is not None else not info.st_mode&0o022), 'bootstrap_material_permissions')
    return info

def config_value(value):
    require(isinstance(value,dict) and set(value)=={'version','businessImage','infrastructureImage','sourceImageId','publicOrigin','deviceOrigin','databasePort','publisherDatabase'}, 'bootstrap_configuration')
    require(type(value['version']) is int and value['version']==1 and all(isinstance(value[k],str) and DIGEST.fullmatch(value[k]) for k in ['businessImage','infrastructureImage']) and re.fullmatch(r'sha256:[a-f0-9]{64}',value['sourceImageId'] or ''), 'bootstrap_image_reference')
    for key,schemes in [('publicOrigin',('http','https')),('deviceOrigin',('https',))]:
        url=urlsplit(value[key]);require(url.scheme in schemes and url.hostname and not url.username and not url.password and not url.path and not url.query and not url.fragment, 'bootstrap_origin')
        require(url.hostname=='localhost' or re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9.-]*',url.hostname), 'bootstrap_origin')
        require(1<=(url.port or (443 if url.scheme=='https' else 80))<=65535, 'bootstrap_origin')
    require(value['publicOrigin']!=value['deviceOrigin'] and type(value['databasePort']) is int and 1024<=value['databasePort']<=65535 and value['databasePort'] not in [urlsplit(value['publicOrigin']).port or 80,urlsplit(value['deviceOrigin']).port or 443], 'bootstrap_port')
    require(value['publisherDatabase'] is None or isinstance(value['publisherDatabase'],str) and re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]{0,62}',value['publisherDatabase']), 'bootstrap_database')
    return value

def load_config():
    require(os.geteuid()==0, 'bootstrap_root_required');protected(CONFIG,mode=0o600)
    require(CONFIG.stat().st_size<=8192,'bootstrap_configuration_budget')
    return config_value(json.loads(CONFIG.read_text()))

def execute(args, timeout=30):
    result=subprocess.run(args,capture_output=True,text=True,timeout=timeout)
    require(result.returncode==0,'bootstrap_command_failed')
    return result.stdout

def docker(*args): return execute(['/usr/bin/docker',*args])

def inspect(kind,name):
    # A failed inspect never means absence. Inventory must positively show it.
    if kind=='container':
        ids=docker('container','ls','--all','--filter','name=^/'+name+'$','--format','{{.ID}}').split()
    else:
        ids=docker('network','ls','--filter','name=^'+name+'$','--format','{{.ID}}').split()
    if not ids:return None
    require(len(ids)==1,'bootstrap_resource_collision')
    found=json.loads(docker(kind,'inspect',name));require(len(found)==1,'bootstrap_resource_collision');return found[0]

def source_observation(config,source):
    require(source and source.get('Image')==config['sourceImageId'] and source.get('Config',{}).get('Labels',{}).get('com.aistrong.project')==PROJECT,'bootstrap_source_identity')
    state=source.get('State',{});restart=source.get('HostConfig',{}).get('RestartPolicy',{}).get('Name')
    mounts=source.get('Mounts',[])
    require(any(m.get('Destination')=='/app/data' and m.get('Type')=='bind' for m in mounts) and any(m.get('Destination')=='/app/runtime' and m.get('Type')=='bind' for m in mounts),'bootstrap_source_mounts')
    for mount in mounts:
        origin=Path(mount['Source']).resolve();require(origin!=ROOT and ROOT not in origin.parents and origin not in ROOT.parents,'bootstrap_source_overlap')
    return {'scope':'observed-docker-only','observedManager':'docker-restart-policy','suggestedSupervisorKind':'docker-restart','exclusiveManagerVerified':False,'sourceFenced':False,'sourceRunning':state.get('Running') is True,'containerFenced':state.get('Running') is False and state.get('Restarting') is False and state.get('Paused') is False and state.get('Pid')==0 and state.get('ExitCode')==0 and not state.get('OOMKilled') and not state.get('Error') and restart=='no','restartPolicy':restart}

def source_material_preparations(source):
    mounts=[m for m in source.get('Mounts',[]) if m.get('Type')=='bind' and m.get('Destination')=='/app/data']
    require(len(mounts)==1,'bootstrap_source_mounts');path=Path(mounts[0]['Source'])/'recording-encryption.key'
    try:
        info=path.lstat();valid=stat.S_ISREG(info.st_mode) and not stat.S_ISLNK(info.st_mode) and info.st_uid==1000 and stat.S_IMODE(info.st_mode)==0o600 and info.st_size==32
        state='metadata_valid_content_not_verified' if valid else 'metadata_invalid'
    except FileNotFoundError:state='missing'
    return [{'material':'source-data/recording-encryption.key','observed':state,'requiredBeforeMigration':True,'createOnlyIfMissing':True,'newIndependentBytes':32,'ownerUid':1000,'mode':'0600','overwriteAllowed':False,'automaticInstallation':False,'mustNotReuse':['key-encryption.key','postgres-report.key']}]

def image_checks(config):
    business=json.loads(docker('image','inspect',config['businessImage']))[0];labels=business.get('Config',{}).get('Labels',{}) or {}
    require(config['businessImage'] in business.get('RepoDigests',[]) and labels.get('com.harbor.ci-validation-only')!='true' and labels.get('com.harbor.storage.postgres')=='postgres-conversation-v2' and labels.get('com.harbor.postgres-preflight')=='v1','bootstrap_business_image')
    infrastructure=json.loads(docker('image','inspect',config['infrastructureImage']))[0];labels=infrastructure.get('Config',{}).get('Labels',{}) or {}
    require(config['infrastructureImage'] in infrastructure.get('RepoDigests',[]) and labels.get('com.harbor.infrastructure')==CONTRACT and labels.get('com.harbor.publisher-business-image')==config['businessImage'] and labels.get('com.harbor.ci-validation-only')!='true','bootstrap_infrastructure_image')

def write_new(path,body,mode=0o600,uid=0):
    descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,mode)
    with os.fdopen(descriptor,'wb') as handle:
        os.fchmod(handle.fileno(),mode);handle.write(body.encode() if isinstance(body,str) else body);handle.flush();os.fsync(handle.fileno());os.fchown(handle.fileno(),uid,uid)

def certificate(directory,names,uid):
    directory.mkdir(mode=0o700);os.chmod(directory,0o700);os.chown(directory,uid,uid)
    # All subjects and SAN values are controlled data, never shell fragments.
    execute(['/usr/bin/openssl','req','-x509','-newkey','rsa:3072','-nodes','-sha256','-days','3650','-subj','/CN=Harbor Infrastructure CA','-addext','basicConstraints=critical,CA:TRUE','-keyout',str(directory/'ca.key'),'-out',str(directory/'ca.pem')])
    execute(['/usr/bin/openssl','req','-new','-newkey','rsa:3072','-nodes','-sha256','-subj','/CN=Harbor Server','-keyout',str(directory/'server.key'),'-out',str(directory/'server.csr')])
    sans=[]
    for name in names:
        try:ipaddress.ip_address(name);sans.append('IP:'+name)
        except ValueError:sans.append('DNS:'+name)
    write_new(directory/'server.ext','basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName='+','.join(sans)+'\n',uid=uid)
    execute(['/usr/bin/openssl','x509','-req','-in',str(directory/'server.csr'),'-CA',str(directory/'ca.pem'),'-CAkey',str(directory/'ca.key'),'-set_serial','0x'+secrets.token_hex(16),'-days','365','-sha256','-extfile',str(directory/'server.ext'),'-out',str(directory/'server.pem')])
    for name in ['ca.key','server.key','ca.pem','server.pem']:
        os.chown(directory/name,uid,uid);os.chmod(directory/name,0o600 if name.endswith('.key') else 0o644)
    (directory/'server.csr').unlink();(directory/'server.ext').unlink()

def archive_configuration(cluster=None):
    return {'version':1,'expectedCluster':cluster,'uid':999,'pgDataDirectory':str(ROOT/'db/18/docker'),'archiveDirectory':str(ROOT/'archive'),'keyFile':str(ROOT/'archive.key'),'keyVersion':'archive-v1'}

def endpoint_configuration():
    return {'version':1,'network':PROJECT+'-postgres-infra','databaseHost':PROJECT+'-postgres-infra','databasePort':5432,'sslmode':'verify-full','applicationCaFile':'/run/secrets/postgres-ca.crt','applicationSecretsSource':'app-secrets','postgresCaSource':'app-secrets/postgres-ca.crt','reportKeySource':'app-secrets/postgres-report.key','deviceSource':'app-secrets/device','pgDataDirectory':str(ROOT/'db/18/docker'),'archiveModeRequested':'on','archiveConfiguration':'awaiting_observed_cluster_binding_metadata_and_wal','archiveReadinessCommand':'prepare-archive.py','runtimeCredentialsPrepared':False}

def publisher_configuration(config):
    return {'version':1,'database':config['publisherDatabase'],'pgDataDir':'/opt/product-development/harbor-local-gateway/postgres-infra/db/18/docker','socketDirectory':'/var/run/postgresql','port':5432,'keyFile':'/run/harbor-report.key'}

def fingerprint(config):return hashlib.sha256(json.dumps({k:v for k,v in config.items() if k!='publisherDatabase'},sort_keys=True).encode()).hexdigest()

def load_state(config):
    protected(ROOT,directory=True,mode=0o711);protected(ROOT/'state.json',mode=0o600)
    value=json.loads((ROOT/'state.json').read_text());require(value.get('configuration')==fingerprint(config) and re.fullmatch('[a-f0-9]{32}',value.get('owner','')),'bootstrap_existing_directory');return value

def prepare(config):
    require(not ROOT.exists(),'bootstrap_existing_directory')
    require(not ARCHIVE_PROFILE.exists() and not ARCHIVE_PROFILE.is_symlink(),'bootstrap_archive_profile_exists')
    for parent in [*ROOT.parents]:protected(parent,directory=True)
    ROOT.mkdir(mode=0o711);os.chmod(ROOT,0o711);state={'version':1,'owner':secrets.token_hex(16),'configuration':fingerprint(config)}
    # Failed preparations are retained for explicit inspection, never guessed safe.
    write_new(ROOT/'state.json',json.dumps(state)+'\n')
    write_new(ROOT/'endpoint.json',json.dumps(endpoint_configuration())+'\n')
    for name,uid,mode in [('db',999,0o700),('control',0,0o755),('image-volume',0,0o555),('app-secrets',0,0o700),('archive',999,0o700)]:
        (ROOT/name).mkdir(mode=mode);os.chmod(ROOT/name,mode);os.chown(ROOT/name,uid,uid)
    certificate(ROOT/'pg-tls',['localhost','127.0.0.1',PROJECT+'-postgres-infra'],999)
    os.rename(ROOT/'pg-tls/ca.key',ROOT/'pg-ca.key');os.chown(ROOT/'pg-ca.key',0,0)
    certificate(ROOT/'app-secrets/device',[urlsplit(config['deviceOrigin']).hostname],1000)
    write_new(ROOT/'app-secrets/postgres-ca.crt',(ROOT/'pg-tls/ca.pem').read_bytes(),mode=0o644)
    write_new(ROOT/'postgres-password',secrets.token_urlsafe(48)+'\n',uid=999)
    report_key=secrets.token_bytes(32);write_new(ROOT/'report-pg.key',report_key,uid=999);write_new(ROOT/'app-secrets/postgres-report.key',report_key,uid=1000)
    write_new(ROOT/'archive.key',secrets.token_bytes(32),uid=999)
    write_new(ARCHIVE_PROFILE,json.dumps(archive_configuration())+'\n',mode=0o644)
    identity=protected(ARCHIVE_PROFILE,mode=0o644);state['archiveProfileIdentity']=[identity.st_dev,identity.st_ino]
    write_new(ROOT/'state.json.pending',json.dumps(state)+'\n');os.replace(ROOT/'state.json.pending',ROOT/'state.json')
    write_new(ROOT/'control/publisher.json',json.dumps(publisher_configuration(config))+'\n',mode=0o644)
    write_new(ROOT/'control/pg_hba.conf','local all postgres peer\nlocal all all scram-sha-256\nhostnossl all all all reject\nhostssl all all 0.0.0.0/0 scram-sha-256\nhostssl all all ::/0 scram-sha-256\n',mode=0o644)
    return state

def verify_materials(config,*,validate_archive_configuration=True):
    state=load_state(config)
    for name,uid,mode in [('db',999,0o700),('control',0,0o755),('pg-tls',999,0o700),('app-secrets/device',1000,0o700),('image-volume',0,0o555),('app-secrets',0,0o700),('archive',999,0o700)]:protected(ROOT/name,uid,mode,True)
    for directory,uid in [('pg-tls',999),('app-secrets/device',1000)]:
        for name in (['ca.key'] if directory.endswith('device') else [])+['ca.pem','server.key','server.pem']:protected(ROOT/directory/name,uid,0o600 if name.endswith('.key') else 0o644)
    for name,uid in [('pg-ca.key',0),('postgres-password',999),('report-pg.key',999),('app-secrets/postgres-report.key',1000),('archive.key',999)]:protected(ROOT/name,uid,0o600)
    require((ROOT/'report-pg.key').stat().st_size==32 and (ROOT/'app-secrets/postgres-report.key').read_bytes()==(ROOT/'report-pg.key').read_bytes(),'bootstrap_report_key')
    protected(ROOT/'app-secrets/postgres-ca.crt',mode=0o644);require((ROOT/'app-secrets/postgres-ca.crt').read_bytes()==(ROOT/'pg-tls/ca.pem').read_bytes(),'bootstrap_application_ca')
    require(set(p.name for p in (ROOT/'app-secrets').iterdir())=={'device','postgres-ca.crt','postgres-report.key'} and set(p.name for p in (ROOT/'app-secrets/device').iterdir())=={'ca.pem','ca.key','server.pem','server.key'},'bootstrap_application_secrets')
    protected(ROOT/'endpoint.json',mode=0o600);require(json.loads((ROOT/'endpoint.json').read_text())==endpoint_configuration(),'bootstrap_endpoint_changed')
    profile_info=protected(ARCHIVE_PROFILE,mode=0o644);require([profile_info.st_dev,profile_info.st_ino]==state.get('archiveProfileIdentity') and profile_info.st_nlink==1,'bootstrap_archive_profile_identity')
    if validate_archive_configuration:
        profile=json.loads(ARCHIVE_PROFILE.read_text());cluster=profile.get('expectedCluster');require(profile==archive_configuration(cluster) and (cluster is None or isinstance(cluster,str) and re.fullmatch(r'[0-9]{1,20}',cluster)),'bootstrap_archive_profile_configuration')
    require((ROOT/'archive.key').stat().st_size==32 and (ROOT/'archive.key').read_bytes()!=(ROOT/'report-pg.key').read_bytes(),'bootstrap_archive_key')
    protected(ROOT/'control/publisher.json',mode=0o644);protected(ROOT/'control/pg_hba.conf',mode=0o644);return state

def container_arguments(config,state):
    name=PROJECT+'-postgres-infra';network=name;labels=['--label','com.harbor.bootstrap-owner='+state['owner'],'--label','com.aistrong.project='+PROJECT+'-infra']
    args=['run','-d','--name',name,'--hostname',name,'--network',network,*labels,'--restart','unless-stopped','--stop-signal','SIGTERM','--stop-timeout','30','--memory','8g','--cpus','4','--pids-limit','256','--publish','127.0.0.1:'+str(config['databasePort'])+':5432','--env','PGDATA=/opt/product-development/harbor-local-gateway/postgres-infra/db/18/docker','--env','POSTGRES_PASSWORD_FILE=/run/harbor-postgres-password','--env','POSTGRES_INITDB_ARGS=--auth-local=peer --auth-host=scram-sha-256 --no-locale --encoding=UTF8']
    for source,destination,readonly in [('db',str(ROOT/'db'),False),('image-volume','/var/lib/postgresql',True),('pg-tls','/run/harbor-pg-tls',True),('control','/run/harbor-infra-control',True),('report-pg.key','/run/harbor-report.key',True),('postgres-password','/run/harbor-postgres-password',True),('archive',str(ROOT/'archive'),False),('archive.key',str(ROOT/'archive.key'),True),(ARCHIVE_PROFILE,str(ARCHIVE_PROFILE),True)]:
        args+=['--mount','type=bind,src='+str(ROOT/source)+',dst='+destination+(',readonly' if readonly else '')]
    return args+[config['infrastructureImage']]

def verify_container(config,current):
    name=PROJECT+'-postgres-infra';host=current.get('HostConfig',{});runtime=current.get('Config',{})
    require(runtime.get('Image')==config['infrastructureImage'] and runtime.get('Entrypoint')==['/usr/local/bin/harbor-postgres-infra'] and runtime.get('Cmd') in (None,[]) and current.get('State',{}).get('Running') is True,'bootstrap_existing_container')
    require(set(current.get('NetworkSettings',{}).get('Networks',{}))=={name} and host.get('NetworkMode')==name and host.get('PortBindings')=={'5432/tcp':[{'HostIp':'127.0.0.1','HostPort':str(config['databasePort'])}]},'bootstrap_container_configuration')
    expected={destination:(str(ROOT/source),not readonly) for source,destination,readonly in [('db',str(ROOT/'db'),False),('image-volume','/var/lib/postgresql',True),('pg-tls','/run/harbor-pg-tls',True),('control','/run/harbor-infra-control',True),('report-pg.key','/run/harbor-report.key',True),('postgres-password','/run/harbor-postgres-password',True),('archive',str(ROOT/'archive'),False),('archive.key',str(ROOT/'archive.key'),True),(ARCHIVE_PROFILE,str(ARCHIVE_PROFILE),True)]}
    mounts=current.get('Mounts',[]);require(len(mounts)==len(expected) and all(m.get('Type')=='bind' and expected.get(m.get('Destination'))==(m.get('Source'),m.get('RW')) for m in mounts),'bootstrap_container_configuration')

def start(config):
    state=verify_materials(config);name=PROJECT+'-postgres-infra'
    for kind in ['container','network']:
        existing=inspect(kind,name)
        if existing:
            require((existing.get('Labels') or existing.get('Config',{}).get('Labels',{}) or {}).get('com.harbor.bootstrap-owner')==state['owner'],'bootstrap_resource_collision')
            if kind=='network':require(existing.get('Driver')=='bridge' and existing.get('Scope')=='local','bootstrap_network_configuration')
    current=inspect('container',name)
    if current:
        verify_container(config,current);return
    if not inspect('network',name):docker('network','create','--label','com.harbor.bootstrap-owner='+state['owner'],name)
    # A startup error preserves these new resources/data for explicit recovery.
    docker(*container_arguments(config,state))

def bind_report(config):
    verify_materials(config);require(config['publisherDatabase'] is not None,'bootstrap_database_required')
    temporary=ROOT/'control/publisher.json.pending';write_new(temporary,json.dumps(publisher_configuration(config))+'\n',mode=0o644);os.replace(temporary,ROOT/'control/publisher.json')

def main():
    require(len(sys.argv)==2 and sys.argv[1] in ['check','prepare','start','bind-report'],'bootstrap_arguments')
    config=load_config();actual_source=inspect('container',PROJECT);source=source_observation(config,actual_source);preparations=source_material_preparations(actual_source);image_checks(config)
    action=sys.argv[1]
    if action=='prepare':prepare(config)
    elif action=='start':start(config)
    elif action=='bind-report':bind_report(config)
    materials=ROOT.exists()
    if materials:verify_materials(config)
    print(json.dumps({'scope':'postgres-infrastructure-only','action':action,'source':source,'sourceMaterialPreparations':preparations,'materialsPrepared':materials,'publicOriginPreserved':True,'migrationVerified':False,'activationIssued':False,'deploymentAllowed':False,'walArchiveReady':False}))

if __name__=='__main__':
    try:main()
    except Exception as error:
        code=str(error) if isinstance(error,ValueError) and re.fullmatch('bootstrap_[a-z_]{1,80}',str(error)) else 'bootstrap_unavailable'
        print('Harbor基础设施准备未完成；原服务未切换：'+code,file=sys.stderr);sys.exit(1)
