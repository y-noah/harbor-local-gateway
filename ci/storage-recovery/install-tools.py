#!/usr/bin/env python3
"""固定 root、只消费本地批准镜像的 Node/app 工具安装入口；不启动容器或服务。"""
import ctypes
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import stat
import subprocess

DESTINATION = Path('/opt/harbor-storage-tools')
PROFILE = Path('/etc/harbor-storage-tools.json')
CANCELLED = False
ENV = {'PATH': '/usr/bin:/bin', 'HOME': '/root', 'LANG': 'C', 'LC_ALL': 'C'}


class Refused(Exception):
    pass


def check_cancel():
    if CANCELLED:
        raise Refused('cancelled')


def command(arguments, timeout=120, cwd=None):
    return subprocess.run(arguments, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=timeout, cwd=cwd, env=ENV).stdout


def trusted_directory(path):
    path = Path(path)
    if not path.is_absolute() or str(path) != os.path.normpath(path):
        raise Refused('directory_invalid')
    descriptor = os.open('/', os.O_DIRECTORY | os.O_RDONLY)
    try:
        for part in path.parts[1:]:
            child = os.open(part, os.O_DIRECTORY | os.O_RDONLY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
            info = os.fstat(descriptor)
            if info.st_uid != 0 or info.st_mode & 0o022:
                raise Refused('directory_untrusted')
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def configuration():
    parent = trusted_directory(PROFILE.parent)
    try:
        descriptor = os.open(PROFILE.name, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW, dir_fd=parent)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1 or info.st_size > 65536:
                raise Refused('profile_untrusted')
            raw = os.read(descriptor, 65537)
            if len(raw) != info.st_size:
                raise Refused('profile_changed')
            value = json.loads(raw)
        finally:
            os.close(descriptor)
    finally:
        os.close(parent)
    image = r'[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64}'
    if (not isinstance(value, dict) or set(value) != {'version', 'businessImage', 'infrastructureImage', 'revision', 'nodeVersion'} or
            value.get('version') != 1 or not re.fullmatch(image, value.get('businessImage', '')) or
            not re.fullmatch(image, value.get('infrastructureImage', '')) or
            not re.fullmatch(r'[a-f0-9]{40}', value.get('revision', '')) or
            not re.fullmatch(r'v24\.\d+\.\d+', value.get('nodeVersion', ''))):
        raise Refused('profile_invalid')
    return raw, value


def image_identity(config):
    rows = []
    for kind in ('businessImage', 'infrastructureImage'):
        actual = json.loads(command(['/usr/bin/docker', 'image', 'inspect', config[kind]]))
        if not isinstance(actual, list) or len(actual) != 1:
            raise Refused('image_identity')
        row = actual[0]
        labels = row.get('Config', {}).get('Labels') or {}
        if (config[kind] not in row.get('RepoDigests', []) or
                not re.fullmatch(r'sha256:[a-f0-9]{64}', row.get('Id', '')) or
                labels.get('com.harbor.ci-validation-only') == 'true'):
            raise Refused('image_identity')
        if kind == 'businessImage':
            if (labels.get('com.harbor.storage.postgres') != 'postgres-conversation-v2' or
                    labels.get('com.harbor.postgres-preflight') != 'v1' or
                    labels.get('com.harbor.extra-usage-dispatch') != 'v1' or
                    labels.get('org.opencontainers.image.revision') != config['revision']):
                raise Refused('business_contract')
        elif labels.get('com.harbor.infrastructure') != 'postgres18-report-v1' or labels.get('com.harbor.publisher-business-image') != config['businessImage']:
            raise Refused('infrastructure_contract')
        rows.append(row['Id'])
    return tuple(rows)


def normalize_tree(root):
    """只接受代码树；链接必须留在自身树内，拒绝数据、凭据文件和特殊 inode。"""
    root = Path(root)
    root_info = root.lstat()
    if not stat.S_ISDIR(root_info.st_mode) or root_info.st_uid != 0 or root_info.st_mode & 0o6000:
        raise Refused('code_root_invalid')
    forbidden_roots = {'data', 'runtime', '.git', 'secrets', 'credentials', '.ssh', '.codex'}
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            path = Path(directory) / name
            relative = path.relative_to(root)
            info = path.lstat()
            if (relative.parts[0] in forbidden_roots or name == '.env' or
                    (name.startswith('.env.') and name not in {'.env.example', '.env.sample', '.env.template'}) or
                    name.endswith(('.key', '.p12', '.pfx')) or
                    name == 'docker.sock' or info.st_mode & 0o6000):
                raise Refused('code_material_forbidden')
            if stat.S_ISLNK(info.st_mode):
                target = os.readlink(path)
                if os.path.isabs(target) or not path.resolve().is_relative_to(root.resolve()) or not path.exists():
                    raise Refused('code_link_forbidden')
                if info.st_uid != 0:
                    raise Refused('code_owner_invalid')
            elif stat.S_ISREG(info.st_mode):
                if info.st_nlink != 1 or info.st_uid != 0:
                    raise Refused('code_owner_invalid')
                path.chmod(0o755 if info.st_mode & 0o111 else 0o644)
            elif stat.S_ISDIR(info.st_mode):
                if info.st_uid != 0:
                    raise Refused('code_owner_invalid')
                path.chmod(0o755)
            else:
                raise Refused('code_inode_forbidden')
    root.chmod(0o755)


def verify_tool_tree(root):
    # 不依赖调用者 umask；发布前复核整个执行依赖树均仅 root 可写。
    paths = [root]
    for directory, dirs, files in os.walk(root, followlinks=False):
        paths.extend(Path(directory) / name for name in dirs + files)
    for path in paths:
        info = path.lstat()
        if info.st_uid != 0:
            raise Refused('tool_owner_invalid')
        if stat.S_ISLNK(info.st_mode):
            if os.path.isabs(os.readlink(path)) or not path.resolve().is_relative_to(root.resolve()) or not path.exists():
                raise Refused('tool_link_invalid')
        elif not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)) or info.st_mode & 0o022:
            raise Refused('tool_mode_invalid')


def project_operator_tools(app):
    """保留镜像原包，完整投影到宿主模块约定路径；拒绝覆盖已有执行树。"""
    source = app / 'ci/storage-operator-tools'
    saved = json.loads((app / 'ci/postgres-image-manifest.json').read_text())
    prefix = 'ci/storage-operator-tools/'
    expected = {name[len(prefix):]: digest for name, digest in saved.get('files', {}).items() if name.startswith(prefix)}
    if not expected or any(not re.fullmatch(r'[a-z0-9-]+\.(mjs|py|c)', name) or not re.fullmatch(r'[a-f0-9]{64}', digest) for name, digest in expected.items()):
        raise Refused('operator_tools_manifest')
    if not source.is_dir() or source.is_symlink() or {path.name for path in source.iterdir()} != set(expected):
        raise Refused('operator_tools_package')
    for name, digest in expected.items():
        path = source / name
        if not stat.S_ISREG(path.lstat().st_mode) or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
            raise Refused('operator_tools_package')
    tools = app / 'tools'
    if tools.is_symlink() or tools.exists() and not tools.is_dir():
        raise Refused('operator_tools_destination')
    tools.mkdir(mode=0o755, exist_ok=True)
    tools.chmod(0o755)
    destination = tools / 'capacity'
    if destination.exists() or destination.is_symlink():
        raise Refused('operator_tools_destination')
    shutil.copytree(source, destination, symlinks=False)
    for name, digest in expected.items():
        if hashlib.sha256((destination / name).read_bytes()).hexdigest() != digest:
            raise Refused('operator_tools_projection')


def verify_copy(stage, config):
    node = stage / 'node/bin/node'
    info = node.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_nlink != 1:
        raise Refused('node_identity')
    node.chmod(0o755)
    if command([str(node), '--version'], timeout=15).decode().strip() != config['nodeVersion']:
        raise Refused('node_version')
    # 只执行固定镜像自检入口，不加载服务器或任何业务配置。
    program = ("import {imageManifest,verifyOperatorToolsProjection} from './ci/postgres-image-manifest.mjs';"
               "const proof=await imageManifest();await verifyOperatorToolsProjection();"
               "for(const module of ['./storage-operator-context.mjs','./storage-activation-issuer.mjs','./storage-operator-backup.mjs','./storage-operator.mjs','./storage-backup.mjs','./storage-maintenance.mjs'])await import(module);"
               "process.stdout.write(JSON.stringify(proof));")
    proof = json.loads(command([str(node), '--input-type=module', '-e', program], timeout=60, cwd=stage / 'app'))
    if proof.get('revision') != config['revision'] or not re.fullmatch(r'[a-f0-9]{64}', proof.get('source_digest', '')):
        raise Refused('manifest_identity')
    return {'revision': proof['revision'], 'sourceDigest': proof['source_digest'],
            'nodeVersion': config['nodeVersion'], 'nodeDigest': hashlib.sha256(node.read_bytes()).hexdigest(),
            'postgresHostToolsInstalled': False}


def sync_tree(root):
    for directory, _, files in os.walk(root, topdown=False, followlinks=False):
        for name in files:
            path = Path(directory) / name
            if path.is_symlink():
                continue
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def publish(stage, destination):
    """只允许缺失/空的 root 工具目录；原子 no-replace，不覆盖任何已安装版本。"""
    try:
        info = destination.lstat()
    except FileNotFoundError:
        info = None
    if info is not None:
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise Refused('destination_untrusted')
        # rmdir 本身只移除空目录，其他安装器赢得非空目录后不会误删。
        try:
            destination.rmdir()
        except OSError as error:
            raise Refused('destination_not_empty') from error
    libc = ctypes.CDLL(None, use_errno=True)
    rename = libc.renameat2
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(-100, os.fsencode(stage), -100, os.fsencode(destination), 1) != 0:
        raise OSError(ctypes.get_errno(), 'atomic publish refused')


def stopped_container(name, token, image_id):
    rows = json.loads(command(['/usr/bin/docker', 'inspect', name]))
    if not isinstance(rows, list) or len(rows) != 1:
        raise Refused('container_identity')
    row = rows[0]
    if (row.get('Image') != image_id or row.get('Config', {}).get('Labels', {}).get('com.harbor.storage-tools-install') != token or
            row.get('State', {}).get('Running') is not False or row.get('State', {}).get('Status') != 'created' or
            row.get('Mounts') or row.get('HostConfig', {}).get('NetworkMode') != 'none'):
        raise Refused('container_identity')
    return row


def install(raw, config):
    parent_fd = trusted_directory(DESTINATION.parent)
    lock_fd = None
    stage = None
    stage_created = False
    name = None
    token = secrets.token_hex(16)
    published = False
    cleanup_ok = True
    try:
        lock_fd = os.open('.harbor-storage-tools.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=parent_fd)
        info = os.fstat(lock_fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1:
            raise Refused('lock_untrusted')
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if DESTINATION.exists() or DESTINATION.is_symlink():
            info = DESTINATION.lstat()
            if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022 or any(DESTINATION.iterdir()):
                raise Refused('destination_not_empty')
        image_ids = image_identity(config)
        check_cancel()
        stage = DESTINATION.parent / ('.harbor-storage-tools-stage-' + token)
        stage.mkdir(mode=0o700)
        stage_created = True
        (stage / 'node').mkdir(mode=0o755)
        (stage / 'node/bin').mkdir(mode=0o755)
        (stage / 'node').chmod(0o755)
        (stage / 'node/bin').chmod(0o755)
        name = 'harbor-storage-tools-' + token
        command(['/usr/bin/docker', 'create', '--pull=never', '--network=none', '--read-only', '--name', name,
                 '--label', 'com.harbor.storage-tools-install=' + token, '--entrypoint', '/bin/false', config['businessImage']])
        stopped_container(name, token, image_ids[0])
        for source, target in [('/usr/local/bin/node', stage / 'node/bin/node'), ('/app', stage / 'app')]:
            check_cancel()
            command(['/usr/bin/docker', 'cp', name + ':' + source, str(target)])
            stopped_container(name, token, image_ids[0])
        normalize_tree(stage / 'app')
        project_operator_tools(stage / 'app')
        proof = verify_copy(stage, config)
        receipt = {'version': 1, **proof, 'businessImage': config['businessImage'], 'infrastructureImage': config['infrastructureImage'],
                   'postgresHostToolsStatus': 'not-installed-needs-private-loader-libraries-and-runtime-verification'}
        receipt_path = stage / 'installation.json'
        receipt_path.write_text(json.dumps(receipt, sort_keys=True) + '\n')
        receipt_path.chmod(0o600)
        stage.chmod(0o755)
        verify_tool_tree(stage)
        sync_tree(stage)
        if configuration()[0] != raw or image_identity(config) != image_ids:
            raise Refused('approval_changed')
        check_cancel()
        # 确保未留下工具容器后才发布；不强杀被外部启动的进程。
        stopped_container(name, token, image_ids[0])
        command(['/usr/bin/docker', 'rm', '-v', name])
        name = None
        publish(stage, DESTINATION)
        published = True
        os.fsync(parent_fd)
        check_cancel()
        return proof
    except BaseException as error:
        if published:
            raise Refused('published_recheck_required') from error
        raise
    finally:
        if name is not None:
            try:
                # 只移除本次拥有、仍从未启动的独立容器。
                stopped_container(name, token, image_ids[0])
                command(['/usr/bin/docker', 'rm', '-v', name])
            except BaseException:
                cleanup_ok = False
        if stage_created and not published:
            shutil.rmtree(stage)
        if lock_fd is not None:
            os.close(lock_fd)
        os.close(parent_fd)
        if not cleanup_ok:
            raise Refused('container_cleanup_recheck_required')


def main():
    import sys
    def cancel(_signum, _frame):
        global CANCELLED
        CANCELLED = True
    signal.signal(signal.SIGINT, cancel)
    signal.signal(signal.SIGTERM, cancel)
    try:
        if len(sys.argv) != 1:
            raise Refused('arguments_forbidden')
        if os.getuid() != 0 or os.geteuid() != 0:
            raise Refused('root_required')
        raw, config = configuration()
        install(raw, config)
        print(json.dumps({'ok': True, 'status': 'node_app_installed', 'postgresHostToolsInstalled': False}))
        return 0
    except Refused as error:
        print(json.dumps({'ok': False, 'status': str(error)}))
        return 1
    except BaseException:
        print(json.dumps({'ok': False, 'status': 'installation_refused'}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
