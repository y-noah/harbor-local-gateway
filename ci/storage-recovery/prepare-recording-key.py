#!/usr/bin/env python3
"""固定 root 入口：只准备旧来源缺失的独立正文密钥，不读取数据库。"""
import json
import os
import re
import signal
import stat
import subprocess

PROFILE = '/etc/harbor-storage-operator.json'
KEY = 'recording-encryption.key'
CANCELLED = False


class Refused(Exception):
    pass


def check_cancel():
    if CANCELLED:
        raise Refused('cancelled')


def directory(path, uid):
    """逐级打开目录；祖先必须 root 控制，叶目录固定所有者且不可组/他人写。"""
    if not isinstance(path, str) or not path.startswith('/') or os.path.normpath(path) != path or path == '/':
        raise Refused('directory_invalid')
    fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
    try:
        parts = path.split('/')[1:]
        for index, part in enumerate(parts):
            next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = next_fd
            info = os.fstat(fd)
            if info.st_uid != (uid if index == len(parts) - 1 else 0) or info.st_mode & 0o022:
                raise Refused('directory_untrusted')
        return fd
    except BaseException:
        os.close(fd)
        raise


def material(fd, name, uid, optional=False):
    try:
        key_fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
    except FileNotFoundError:
        if optional:
            return None
        raise Refused('material_missing')
    try:
        before = os.fstat(key_fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != uid or
                stat.S_IMODE(before.st_mode) != 0o600 or before.st_size != 32 or before.st_nlink != 1):
            raise Refused('material_invalid')
        value = os.read(key_fd, 33)
        os.lseek(key_fd, 0, os.SEEK_SET)
        repeated = os.read(key_fd, 33)
        after = os.fstat(key_fd)
        named = os.stat(name, dir_fd=fd, follow_symlinks=False)
        identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns, s.st_mode, s.st_uid, s.st_nlink)
        if len(value) != 32 or repeated != value or identity(before) != identity(after) or identity(after) != identity(named):
            raise Refused('material_changed')
        return value
    finally:
        os.close(key_fd)


def has_spool(fd):
    try:
        os.stat('recording-spool', dir_fd=fd, follow_symlinks=False)
        return True
    except FileNotFoundError:
        return False


def prepare(fd, forbidden, revalidate, uid=1000):
    """实际 FD 原子机制；调用者负责固定 root 配置及 Docker 身份。"""
    def verify(value):
        if value in forbidden:
            raise Refused('material_reused')
    existing = material(fd, KEY, uid, optional=True)
    if existing is not None:
        verify(existing)
        revalidate()
        if material(fd, KEY, uid) != existing:
            raise Refused('material_changed')
        os.fsync(fd)
        check_cancel()
        return 'existing_preserved'
    if has_spool(fd):
        raise Refused('missing_key_with_spool')
    check_cancel()
    # 匿名 inode 不暴露可被 data 所有者替换的临时文件名；不支持就拒绝。
    temporary = os.open('.', os.O_RDWR | os.O_TMPFILE, 0o600, dir_fd=fd)
    published = False
    try:
        value = os.getrandom(32)
        if len(value) != 32:
            raise Refused('entropy_incomplete')
        verify(value)
        if os.write(temporary, value) != 32:
            raise Refused('write_incomplete')
        os.fchmod(temporary, 0o600)
        if os.fstat(temporary).st_uid != uid:
            os.fchown(temporary, uid, uid)
        os.fsync(temporary)
        revalidate()
        if has_spool(fd):
            raise Refused('missing_key_with_spool')
        check_cancel()
        try:
            os.link('/proc/self/fd/' + str(temporary), KEY, dst_dir_fd=fd, follow_symlinks=True)
            published = True
        except FileExistsError:
            winner = material(fd, KEY, uid)
            verify(winner)
            revalidate()
            os.fsync(fd)
            check_cancel()
            if material(fd, KEY, uid) != winner:
                raise Refused('material_changed')
            return 'concurrent_existing_preserved'
        # 一旦发布，任何错误或取消均不删除材料；后续重试重验既有值。
        os.fsync(fd)
        check_cancel()
        revalidate()
        if material(fd, KEY, uid) != value:
            raise Refused('material_changed')
        return 'created'
    except BaseException as error:
        if published:
            raise Refused('published_recheck_required') from error
        raise
    finally:
        os.close(temporary)


def profile():
    config_dir = directory('/etc', 0)
    try:
        fd = os.open('harbor-storage-operator.json', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=config_dir)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1 or info.st_size > 1048576:
                raise Refused('profile_untrusted')
            raw = os.read(fd, 1048577)
            if len(raw) != info.st_size:
                raise Refused('profile_changed')
            return raw, json.loads(raw)
        finally:
            os.close(fd)
    finally:
        os.close(config_dir)


def inspect_source(source, data_fd):
    result = subprocess.run(['/usr/bin/docker', 'inspect', source['container']], capture_output=True, timeout=20, check=True, env={'PATH': '/usr/bin:/bin', 'HOME': '/root'})
    rows = json.loads(result.stdout)
    if not isinstance(rows, list) or len(rows) != 1:
        raise Refused('source_mismatch')
    row = rows[0]
    mounts = [m for m in row.get('Mounts', []) if m.get('Destination') == '/app/data']
    if (row.get('Image') != source['imageId'] or row.get('Config', {}).get('User') != source['user'] or
            row.get('Config', {}).get('Labels', {}).get('com.aistrong.project') != source['project'] or
            len(mounts) != 1 or mounts[0].get('Type') != 'bind' or mounts[0].get('Source') != source['dataDirectory']):
        raise Refused('source_mismatch')
    fresh = directory(source['dataDirectory'], 1000)
    try:
        if (os.fstat(fresh).st_dev, os.fstat(fresh).st_ino) != (os.fstat(data_fd).st_dev, os.fstat(data_fd).st_ino):
            raise Refused('source_changed')
    finally:
        os.close(fresh)


def run():
    if os.geteuid() != 0 or os.getuid() != 0 or os.name != 'posix':
        raise Refused('root_required')
    raw, config = profile()
    source = config.get('source', {})
    if (config.get('version') != 1 or source.get('uid') != 1000 or
            not re.fullmatch(r'[-a-zA-Z0-9_]{1,63}', source.get('container', '')) or
            not re.fullmatch(r'sha256:[a-f0-9]{64}', source.get('imageId', '')) or
            not re.fullmatch(r'[-a-z0-9]{1,63}', source.get('project', '')) or
            not re.fullmatch(r'[-a-zA-Z0-9_:]{1,64}', source.get('user', ''))):
        raise Refused('profile_invalid')
    data_fd = directory(source.get('dataDirectory'), 1000)
    secrets_fd = None
    try:
        forbidden = [material(data_fd, 'key-encryption.key', 1000)]
        secrets = config.get('candidate', {}).get('secretsDirectory')
        roots = [root for root in config.get('fileRoots', []) if root.get('id') == 'secrets']
        if len(roots) != 1 or roots[0].get('source') != secrets:
            raise Refused('secrets_not_approved')
        # 只查批准 secrets 根中的固定报告密钥名，不扫描其他位置。
        secrets_fd = directory(secrets, 0)
        def materials():
            values = [material(data_fd, 'key-encryption.key', 1000)]
            for name, uid in [('report-app.key', 1000), ('report-pg.key', 999), ('postgres-report.key', 1000)]:
                value = material(secrets_fd, name, uid, optional=True)
                if value is not None:
                    values.append(value)
            return values
        forbidden = materials()
        def revalidate():
            if profile()[0] != raw:
                raise Refused('profile_changed')
            inspect_source(source, data_fd)
            fresh = directory(secrets, 0)
            try:
                if (os.fstat(fresh).st_dev, os.fstat(fresh).st_ino) != (os.fstat(secrets_fd).st_dev, os.fstat(secrets_fd).st_ino):
                    raise Refused('secrets_changed')
            finally:
                os.close(fresh)
            if materials() != forbidden:
                raise Refused('material_changed')
        revalidate()
        return prepare(data_fd, forbidden, revalidate)
    finally:
        os.close(data_fd)
        if secrets_fd is not None:
            os.close(secrets_fd)


def main():
    import sys
    global CANCELLED
    def cancel(_signum, _frame):
        global CANCELLED
        CANCELLED = True
    signal.signal(signal.SIGTERM, cancel)
    signal.signal(signal.SIGINT, cancel)
    try:
        if len(sys.argv) != 1:
            raise Refused('arguments_forbidden')
        status = run()
        print(json.dumps({'ok': True, 'status': status, 'uid': 1000, 'mode': '0600'}))
        return 0
    except Refused as error:
        print(json.dumps({'ok': False, 'status': str(error)}))
        return 1
    except BaseException:
        # 不输出异常中的路径、容器响应或材料。
        print(json.dumps({'ok': False, 'status': 'preparation_refused'}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
