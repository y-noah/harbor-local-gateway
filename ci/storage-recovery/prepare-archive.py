#!/usr/bin/env python3
"""Bind the new dedicated infrastructure archive profile, never migrate or fence."""
import ctypes
import secrets
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import sys
import hmac

spec=importlib.util.spec_from_file_location('harbor_bootstrap',Path(__file__).with_name('bootstrap.py'))
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)

def read_private(path,uid,size):
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        info=os.fstat(descriptor);b.require(stat.S_ISREG(info.st_mode) and info.st_uid==uid and stat.S_IMODE(info.st_mode)==0o600 and info.st_size==size and info.st_nlink==1,'bootstrap_archive_private_material')
        value=os.read(descriptor,size+1);b.require(len(value)==size,'bootstrap_archive_private_material');return value
    finally:os.close(descriptor)

def control_identity():
    data=b.ROOT/'db/18/docker';b.protected(data,999,0o700,True)
    b.require(read_private(data/'PG_VERSION',999,3)==b'18\n','bootstrap_archive_pg_version')
    control=read_private(data/'global/pg_control',999,8192);return str(int.from_bytes(control[:8],sys.byteorder))

def inspect_infrastructure(config,state,expected_id=None):
    value=b.inspect('container',b.PROJECT+'-postgres-infra');b.require(value is not None,'bootstrap_archive_container_missing');b.verify_container(config,value)
    labels=value.get('Config',{}).get('Labels',{}) or {};b.require(labels.get('com.harbor.bootstrap-owner')==state['owner'] and labels.get('com.aistrong.project')==b.PROJECT+'-infra' and (expected_id is None or value.get('Id')==expected_id),'bootstrap_archive_container_identity');return value

def observe(container):
    raw=b.docker('exec','--user','999:999',container['Id'],'/usr/local/bin/node','/opt/harbor-storage-report/ci/storage-recovery/archive-observe.mjs')
    b.require(len(raw)<=16384,'bootstrap_archive_observation_budget');value=json.loads(raw)
    b.require(value.get('schema')=='harbor-infrastructure-archive-observation-v1' and value.get('uid')==999 and value.get('tlsVerified') is True and type(value.get('version')) is int and 180000<=value['version']<190000 and value.get('mode')=='on' and value.get('command')==b.ARCHIVE_COMMAND and value.get('directory')==str(b.ROOT/'db/18/docker') and re.fullmatch(r'[0-9]{1,20}',value.get('cluster','')) and isinstance(value.get('postmaster'),str),'bootstrap_archive_observation')
    return value

def sync_directory(path):
    descriptor=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)

def publish_private(path,body):
    # renameat2 preserves create-only semantics without a linked-file crash
    # window: an interruption leaves either a complete target or no target.
    temporary=path.with_name('.'+path.name+'.'+secrets.token_hex(16)+'.pending')
    try:
        b.write_new(temporary,body)
        libc=ctypes.CDLL(None,use_errno=True);rename=libc.renameat2
        rename.argtypes=[ctypes.c_int,ctypes.c_char_p,ctypes.c_int,ctypes.c_char_p,ctypes.c_uint];rename.restype=ctypes.c_int
        if rename(-100,os.fsencode(temporary),-100,os.fsencode(path),1)!=0:
            code=ctypes.get_errno()
            raise OSError(code,os.strerror(code))
        sync_directory(path.parent)
    finally:
        try:temporary.unlink()
        except FileNotFoundError:pass

def backup_key_copy(key):
    # The existing backup/operator entry consumes this same key, not the report
    # key or application encryption material. No existing value is overwritten.
    if b.ARCHIVE_BACKUP_KEY.exists() or b.ARCHIVE_BACKUP_KEY.is_symlink():
        b.require(hmac.compare_digest(read_private(b.ARCHIVE_BACKUP_KEY,0,32),key),'bootstrap_archive_backup_key_mismatch');sync_directory(b.ARCHIVE_BACKUP_KEY.parent)
    else:publish_private(b.ARCHIVE_BACKUP_KEY,key)

def bind_profile(config,state,cluster,*,after_profile_write=None):
    expected=b.archive_configuration(cluster);journal_path=b.ROOT/'archive-profile.pending.json';profile=b.ARCHIVE_PROFILE
    descriptor=os.open(profile,os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        try:fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise ValueError('bootstrap_archive_profile_busy')
        info=os.fstat(descriptor);b.require(stat.S_ISREG(info.st_mode) and info.st_uid==0 and stat.S_IMODE(info.st_mode)==0o644 and info.st_nlink==1 and [info.st_dev,info.st_ino]==state['archiveProfileIdentity'],'bootstrap_archive_profile_identity')
        key=read_private(b.ROOT/'archive.key',999,32)
        for other in [b.ROOT/'report-pg.key',b.ROOT/'app-secrets/postgres-report.key']:
            b.require(not hmac.compare_digest(key,Path(other).read_bytes()),'bootstrap_archive_key_reuse')
        for name in ['key-encryption.key','recording-encryption.key']:
            other=b.ROOT.parent/'data'/name
            if other.exists() or other.is_symlink():b.require(not hmac.compare_digest(key,read_private(other,1000,32)),'bootstrap_archive_key_reuse')
        journal={'version':1,'owner':state['owner'],'identity':state['archiveProfileIdentity'],'profile':expected}
        prior_journal=None
        if journal_path.exists() or journal_path.is_symlink():
            b.protected(journal_path,mode=0o600);b.require(journal_path.stat().st_size<=8192,'bootstrap_archive_journal');prior_journal=json.loads(journal_path.read_text());b.require(prior_journal==journal,'bootstrap_archive_journal');sync_directory(b.ROOT)
        raw=os.read(descriptor,8193);b.require(len(raw)<=8192,'bootstrap_archive_profile_budget')
        try:previous=json.loads(raw)
        except (ValueError,UnicodeDecodeError):previous=None
        b.require(previous in [b.archive_configuration(),expected] or prior_journal==journal,'bootstrap_archive_profile_conflict')
        backup_key_copy(key)
        if previous!=expected:
            if prior_journal is None:publish_private(journal_path,json.dumps(journal)+'\n')
            body=(json.dumps(expected,sort_keys=True,separators=(',',':'))+'\n').encode();os.lseek(descriptor,0,os.SEEK_SET)
            written=0
            while written<len(body):
                count=os.write(descriptor,body[written:]);b.require(count>0,'bootstrap_archive_profile_write');written+=count
            os.ftruncate(descriptor,len(body));os.fsync(descriptor)
            if after_profile_write:after_profile_write()
        path_info=profile.lstat();b.require([path_info.st_dev,path_info.st_ino]==state['archiveProfileIdentity'],'bootstrap_archive_profile_identity')
        os.lseek(descriptor,0,os.SEEK_SET);b.require(json.loads(os.read(descriptor,8193))==expected,'bootstrap_archive_profile_readback')
        os.fsync(descriptor)
        if journal_path.exists():journal_path.unlink();sync_directory(b.ROOT)
    finally:os.close(descriptor)

def prepare_archive():
    config=b.load_config();state=b.verify_materials(config,validate_archive_configuration=False);b.image_checks(config)
    container=inspect_infrastructure(config,state);before=observe(container);b.require(before['cluster']==control_identity(),'bootstrap_archive_cluster_mismatch')
    bind_profile(config,state,before['cluster'])
    inspect_infrastructure(config,state,container['Id']);after=observe(container)
    b.require(after['cluster']==before['cluster']==control_identity() and after['postmaster']==before['postmaster'],'bootstrap_archive_instance_changed')
    b.require(after.get('profileBound') is True and all(type(after.get(key)) is bool for key in ['metadataReady','walArchiveVerified']),'bootstrap_archive_profile_unverified')
    ready=after['metadataReady'] and after['walArchiveVerified'];missing=[]
    if not after['metadataReady']:missing.append('run_existing_storage_backup_prepare_archive_after_full_backup_profile')
    if not after['walArchiveVerified']:missing.append('verify_actual_wal_archiver_and_authenticated_segment')
    return {'scope':'owned-infrastructure-archive-preparation','profileBound':True,'metadataReady':after['metadataReady'],'walArchiveVerified':after['walArchiveVerified'],'ready':ready,'keyVersion':'archive-v1','next':missing,'deploymentAllowed':False}

if __name__=='__main__':
    try:b.require(len(sys.argv)==1,'bootstrap_archive_arguments');print(json.dumps(prepare_archive()))
    except Exception as error:
        code=str(error) if isinstance(error,ValueError) and re.fullmatch('bootstrap_[a-z_]{1,80}',str(error)) else 'bootstrap_archive_unavailable'
        print(json.dumps({'error':code,'ready':False,'deploymentAllowed':False}),file=sys.stderr);sys.exit(1)
