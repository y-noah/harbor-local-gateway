#!/usr/bin/env python3
"""从批准 PGinfra 提取宿主 PG18 工具；不替换宿主 glibc，不接触业务库。"""
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import pwd
import secrets
import shutil
import signal
import stat
import subprocess
import time

spec = importlib.util.spec_from_file_location('harbor_tools_install', Path(__file__).with_name('install-tools.py'))
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)
ROOT = Path('/opt/harbor-storage-tools')
DESTINATION = ROOT / 'postgres'
TOOLS = ('pg_basebackup', 'pg_verifybackup', 'pg_waldump', 'pg_controldata', 'initdb', 'postgres', 'pg_ctl')
GLIBC = re.compile(r'^(?:ld-|ld-linux|lib(?:c|m|mvec|resolv|pthread|dl|rt|anl|util|BrokenLocale|thread_db|SegFault|c_malloc_debug|memusage|pcprofile)(?:-|\.so)|libnss_|libnsl(?:-[0-9]|\.so\.1(?:$|\.)))')
LIBRARY = re.compile(r'^lib[^/]*\.so(?:\.[A-Za-z0-9_-]+)*$')


def file_digest(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1048576), b''):
            digest.update(chunk)
    return digest.hexdigest()


def node_receipt(config):
    parent = b.trusted_directory(ROOT)
    try:
        descriptor = os.open('installation.json', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1 or info.st_size > 65536:
                raise b.Refused('node_receipt_untrusted')
            value = json.loads(os.read(descriptor, 65537))
        finally:
            os.close(descriptor)
    finally:
        os.close(parent)
    if any(value.get(name) != config[name] for name in ['businessImage', 'infrastructureImage', 'revision', 'nodeVersion']):
        raise b.Refused('node_receipt_mismatch')
    return value


def libraries(source, target):
    """只保留库根普通非 glibc SONAME；不带入 ld/libc 或 gconv/NSS 插件。"""
    source_info = source.lstat()
    if not stat.S_ISDIR(source_info.st_mode) or source_info.st_uid != 0:
        raise b.Refused('library_root_untrusted')
    target.mkdir(mode=0o755)
    links = []
    for path in sorted(source.iterdir()):
        name = path.name
        if GLIBC.match(name) or not LIBRARY.fullmatch(name):
            continue
        info = path.lstat()
        if stat.S_ISREG(info.st_mode):
            if info.st_uid != 0 or info.st_nlink != 1 or info.st_mode & 0o6000:
                raise b.Refused('library_untrusted')
            shutil.copyfile(path, target / name)
            (target / name).chmod(0o644)
        elif stat.S_ISLNK(info.st_mode):
            if info.st_uid != 0:
                raise b.Refused('library_untrusted')
            destination = os.readlink(path)
            if os.path.isabs(destination):
                if Path(destination).parent not in [Path('/lib/x86_64-linux-gnu'), Path('/usr/lib/x86_64-linux-gnu')]:
                    continue
                destination = Path(destination).name
            if '/' in destination or GLIBC.match(destination) or not LIBRARY.fullmatch(destination):
                continue
            links.append((name, destination))
        elif not stat.S_ISDIR(info.st_mode):
            raise b.Refused('library_inode_forbidden')
    # Debian 运行库的常见 SONAME 链接均指向同根真实版本；不带入外部 alternatives。
    for name, destination in links:
        if (target / destination).is_file() and not (target / destination).is_symlink():
            (target / name).symlink_to(destination)
    if any(GLIBC.match(path.name) for path in target.iterdir()):
        raise b.Refused('glibc_copy_forbidden')


def pg_container(name, token, image_id):
    rows = json.loads(b.command(['/usr/bin/docker', 'inspect', name]))
    if not isinstance(rows, list) or len(rows) != 1:
        raise b.Refused('container_identity')
    row = rows[0]
    host = row.get('HostConfig', {})
    if (row.get('Image') != image_id or row.get('Config', {}).get('Labels', {}).get('com.harbor.storage-tools-install') != token or
            row.get('State', {}).get('Running') is not False or row.get('State', {}).get('Status') != 'created' or
            host.get('NetworkMode') != 'none' or host.get('Tmpfs') != {'/var/lib/postgresql': 'rw,noexec,nosuid,nodev,mode=0700'} or
            any(m.get('Type') != 'tmpfs' or m.get('Destination') != '/var/lib/postgresql' for m in row.get('Mounts', []))):
        raise b.Refused('container_identity')


def probe(stage, work, uid=65534, gid=65534):
    """新私有 Unix socket 集群实测，不读 profile 中的连接或任何业务库。"""
    try:
        account = pwd.getpwuid(uid)
    except KeyError as error:
        raise b.Refused('probe_identity_unavailable') from error
    if uid == 0 or gid == 0 or account.pw_gid != gid:
        raise b.Refused('probe_identity')
    bin_dir = stage / 'usr/lib/postgresql/18/bin'
    share_dir = stage / 'usr/share/postgresql/18'
    library_dir = stage / 'usr/lib/x86_64-linux-gnu'
    env = {**b.ENV, 'HOME': str(work), 'PGPASSFILE': str(work / '.unused-pgpass'),
           'PATH': str(bin_dir) + ':/usr/bin:/bin', 'LD_LIBRARY_PATH': str(library_dir)}
    if os.geteuid() == 0:
        identity = {'user': uid, 'group': gid, 'extra_groups': []}
    elif os.geteuid() == uid and os.getegid() == gid:
        identity = {}
    else:
        raise b.Refused('probe_identity')
    def run(name, args, timeout=120):
        b.check_cancel()
        try:
            return subprocess.run([str(bin_dir / name), *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  check=True, env=env, timeout=timeout, **identity).stdout
        except subprocess.CalledProcessError as error:
            if name == 'initdb' and error.returncode < 0:
                raise b.Refused('pg_probe_cleanup_required') from error
            raise
        except subprocess.TimeoutExpired as error:
            # initdb 可能派生后端；不假定只杀直接子进程就已清空所有使用者。
            raise b.Refused('pg_probe_cleanup_required') from error
    versions = {}
    for name in TOOLS:
        info = (bin_dir / name).lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise b.Refused('pg_tool_untrusted')
        version = run(name, ['--version'], 15).decode().strip()
        if not re.search(r'\(PostgreSQL\) 18\.\d+(?:\D|$)', version):
            raise b.Refused('pg_tool_version')
        versions[name] = version
    work.mkdir(mode=0o700)
    if os.geteuid() == 0:
        os.chown(work, uid, gid)
    data = work / 'data'
    backup = work / 'backup'
    run('initdb', ['-D', str(data), '-L', str(share_dir), '--locale=C', '--encoding=UTF8', '-U', 'harbor_storage_tools_probe', '-A', 'trust', '--no-sync'])
    server = None
    stopped = False
    try:
        log_fd = os.open(work / 'postgres.log', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        with os.fdopen(log_fd, 'wb') as output:
            server = subprocess.Popen([str(bin_dir / 'postgres'), '-D', str(data), '-c', 'listen_addresses=',
                                       '-c', 'unix_socket_directories=' + str(work), '-c', 'unix_socket_permissions=0700',
                                       '-c', 'max_wal_senders=3', '-c', 'max_replication_slots=3'],
                                      stdout=output, stderr=subprocess.STDOUT, env=env, start_new_session=True, **identity)
            deadline = time.monotonic() + 45
            while True:
                b.check_cancel()
                if server.poll() is not None:
                    raise b.Refused('pg_probe_start_failed')
                if (work / '.s.PGSQL.5432').exists():
                    try:
                        run('psql', ['-X', '-h', str(work), '-U', 'harbor_storage_tools_probe', '-d', 'postgres', '-c', 'SELECT 1'], 5)
                        break
                    except subprocess.CalledProcessError:
                        pass
                if time.monotonic() >= deadline:
                    raise b.Refused('pg_probe_start_timeout')
                time.sleep(0.05)
            run('psql', ['-X', '-h', str(work), '-U', 'harbor_storage_tools_probe', '-d', 'postgres', '-c', 'CREATE TABLE harbor_tools_probe (id integer); INSERT INTO harbor_tools_probe VALUES (1)'])
            run('pg_controldata', [str(data)], 15)
            run('pg_basebackup', ['-h', str(work), '-U', 'harbor_storage_tools_probe', '-D', str(backup), '--checkpoint=fast', '--wal-method=stream', '--no-sync', '--manifest-checksums=SHA256'])
            run('pg_verifybackup', [str(backup)])
            b.check_cancel()
    finally:
        try:
            if server is not None:
                # Popen 持有未 reap 的自有直接子进程，不按外来 pid 文件发信号。
                if server.poll() is None:
                    server.terminate()
                    try:
                        server.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        server.send_signal(signal.SIGQUIT)
                        server.wait(timeout=15)
                # 异常被杀不能证明其后代已退出，保留私有现场而非删除在用文件。
                stopped = server.returncode == 0
            else:
                stopped = True
        except BaseException as error:
            raise b.Refused('pg_probe_cleanup_required') from error
        if not stopped:
            raise b.Refused('pg_probe_cleanup_required')
    return {'versions': versions, 'privateClusterBackupVerified': True,
            'hostGlibcUsed': True, 'tools': {name: file_digest(bin_dir / name) for name in TOOLS}}


def install(raw, config):
    parent = b.trusted_directory(ROOT)
    token = secrets.token_hex(16)
    stage = ROOT / ('.postgres-tools-stage-' + token)
    work = ROOT / ('.postgres-tools-probe-' + token)
    stage_created = False
    probe_stopped = True
    name = None
    lock = None
    published = False
    cleanup_ok = True
    try:
        lock = os.open('.postgres-tools.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=parent)
        info = os.fstat(lock)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600:
            raise b.Refused('lock_untrusted')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        initial_receipt = node_receipt(config)
        if DESTINATION.exists() or DESTINATION.is_symlink():
            info = DESTINATION.lstat()
            if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022 or any(DESTINATION.iterdir()):
                raise b.Refused('destination_not_empty')
        image_ids = b.image_identity(config)
        b.check_cancel()
        stage.mkdir(mode=0o700)
        stage_created = True
        name = 'harbor-pg-tools-' + token
        b.command(['/usr/bin/docker', 'create', '--pull=never', '--network=none', '--read-only', '--tmpfs', '/var/lib/postgresql:rw,noexec,nosuid,nodev,mode=0700', '--name', name, '--label', 'com.harbor.storage-tools-install=' + token, '--entrypoint', '/bin/false', config['infrastructureImage']])
        pg_container(name, token, image_ids[1])
        # 官方镜像的版本目录可相对引用PG共享根；完整复制运行时闭包，不放宽链接校验。
        for source, destination in [('/usr/lib/postgresql/18', stage / 'usr/lib/postgresql/18'), ('/usr/share/postgresql', stage / 'usr/share/postgresql'), ('/usr/lib/x86_64-linux-gnu', stage / '_image-libraries')]:
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
            b.check_cancel()
            b.command(['/usr/bin/docker', 'cp', name + ':' + source, str(destination)])
            pg_container(name, token, image_ids[1])
        libraries(stage / '_image-libraries', stage / 'usr/lib/x86_64-linux-gnu')
        shutil.rmtree(stage / '_image-libraries')
        b.normalize_tree(stage)
        b.verify_tool_tree(stage)
        # 降权测试只能遍历只读工具；测试目录另属既有 nobody/0700，不放宽工具写权限。
        probe_stopped = False
        try:
            proof = probe(stage, work)
            probe_stopped = True
        except b.Refused as error:
            if str(error) != 'pg_probe_cleanup_required':
                probe_stopped = True
            raise
        except BaseException:
            # probe 对启动后的所有路径都等待自有 postgres 退出再抛出。
            probe_stopped = True
            raise
        shutil.rmtree(work)
        receipt = {'version': 1, 'businessImage': config['businessImage'], 'infrastructureImage': config['infrastructureImage'],
                   'revision': config['revision'], 'postgresHostToolsInstalled': True, **proof,
                   'libraryDigests': {path.name: file_digest(path) for path in (stage / 'usr/lib/x86_64-linux-gnu').iterdir() if path.is_file() and not path.is_symlink()}}
        (stage / 'installation.json').write_text(json.dumps(receipt, sort_keys=True) + '\n')
        (stage / 'installation.json').chmod(0o600)
        b.verify_tool_tree(stage)
        b.sync_tree(stage)
        if b.configuration()[0] != raw or b.image_identity(config) != image_ids or node_receipt(config) != initial_receipt:
            raise b.Refused('approval_changed')
        b.check_cancel()
        pg_container(name, token, image_ids[1])
        b.command(['/usr/bin/docker', 'rm', '-v', name])
        name = None
        b.publish(stage, DESTINATION)
        published = True
        os.fsync(parent)
        b.check_cancel()
        return receipt
    except BaseException as error:
        if published:
            raise b.Refused('published_recheck_required') from error
        raise
    finally:
        if name is not None:
            try:
                pg_container(name, token, image_ids[1])
                b.command(['/usr/bin/docker', 'rm', '-v', name])
            except BaseException:
                cleanup_ok = False
        if probe_stopped:
            if work.exists():
                shutil.rmtree(work)
            if stage_created and not published:
                shutil.rmtree(stage)
        if lock is not None:
            os.close(lock)
        os.close(parent)
        if not cleanup_ok or not probe_stopped:
            raise b.Refused('pg_tools_cleanup_recheck_required')


def main():
    import sys
    def cancel(_signum, _frame):
        b.CANCELLED = True
    signal.signal(signal.SIGINT, cancel)
    signal.signal(signal.SIGTERM, cancel)
    try:
        if len(sys.argv) != 1:
            raise b.Refused('arguments_forbidden')
        if os.getuid() != 0 or os.geteuid() != 0:
            raise b.Refused('root_required')
        if os.uname().machine != 'x86_64':
            raise b.Refused('architecture_unsupported')
        raw, config = b.configuration()
        install(raw, config)
        print(json.dumps({'ok': True, 'status': 'postgres_host_tools_installed', 'postgresHostToolsInstalled': True, 'productionClusterAccessed': False}))
        return 0
    except b.Refused as error:
        print(json.dumps({'ok': False, 'status': str(error)}))
        return 1
    except BaseException:
        print(json.dumps({'ok': False, 'status': 'pg_tools_installation_refused'}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
