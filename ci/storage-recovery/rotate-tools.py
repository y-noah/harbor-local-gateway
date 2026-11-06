#!/usr/bin/env python3
"""Preserve an idle host tools tree, then use the unchanged install entrypoints."""
import base64
import ctypes
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import sys
import uuid

spec = importlib.util.spec_from_file_location('harbor_install_tools', Path(__file__).with_name('install-tools.py'))
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)
APPROVAL = Path('/etc/harbor-storage-tools-rotation.json')
HISTORY = Path('/opt/harbor-storage-tools-history')
PROC = Path('/proc')
DOCKER_SOCKET = Path('/run/docker.sock')
UID = 0
LIMIT = 131072


def require(condition, code):
    if not condition:
        raise b.Refused('rotation_' + code)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def body(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


def identity(path):
    info = path.lstat()
    return [info.st_dev, info.st_ino]


def trusted(path, directory=False, mode=None):
    info = path.lstat()
    require((stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)) and info.st_uid == UID and not info.st_mode & 0o022 and (mode is None or stat.S_IMODE(info.st_mode) == mode) and (directory or info.st_nlink == 1), 'untrusted_material')
    return info


def read_control(path, mode=0o600):
    parent = b.trusted_directory(path.parent)
    try:
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        try:
            info = os.fstat(fd)
            require(stat.S_ISREG(info.st_mode) and info.st_uid == UID and stat.S_IMODE(info.st_mode) == mode and info.st_nlink == 1 and info.st_size <= LIMIT and identity(path) == [info.st_dev, info.st_ino], 'untrusted_material')
            raw = os.read(fd, LIMIT + 1)
            require(len(raw) == info.st_size, 'material_changed')
            return raw
        finally:
            os.close(fd)
    finally:
        os.close(parent)


def next_config(value):
    require(isinstance(value, dict) and set(value) == {'version', 'businessImage', 'infrastructureImage', 'revision', 'nodeVersion'} and type(value['version']) is int and value['version'] == 1, 'approval')
    require(all(isinstance(value[name], str) and re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64}', value[name]) for name in ['businessImage', 'infrastructureImage']) and re.fullmatch('[a-f0-9]{40}', value.get('revision', '')) and re.fullmatch(r'v24\.\d+\.\d+', value.get('nodeVersion', '')), 'approval')
    return value


def approval():
    require(os.getuid() == 0 and os.geteuid() == 0, 'root_required')
    raw = read_control(APPROVAL)
    value = json.loads(raw)
    require(isinstance(value, dict) and set(value) == {'version', 'id', 'expected', 'nextTools'} and type(value['version']) is int and value['version'] == 1 and str(uuid.UUID(value['id'])) == value['id'], 'approval')
    expected = value['expected']
    require(isinstance(expected, dict) and set(expected) == {'profileSha256', 'receiptSha256', 'treeIdentity'} and all(isinstance(expected[name], str) and re.fullmatch('[a-f0-9]{64}', expected[name]) for name in ['profileSha256', 'receiptSha256']) and isinstance(expected['treeIdentity'], list) and len(expected['treeIdentity']) == 2 and all(type(item) is int and item >= 0 for item in expected['treeIdentity']), 'approval')
    next_config(value['nextTools'])
    return value, sha(raw)


def sync(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def mkdir(path):
    if not path.exists() and not path.is_symlink():
        path.mkdir(mode=0o700)
        path.chmod(0o700)
        sync(path.parent)
    trusted(path, directory=True, mode=0o700)


def save(directory, journal):
    pending = directory / '.journal.next'
    if pending.exists() or pending.is_symlink():
        read_control(pending)
        pending.unlink()
    fd = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'wb', closefd=False) as handle:
            handle.write(body(journal)); handle.flush(); os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(pending, directory / 'journal.json')
    sync(directory)


def rename(source, target):
    # A crash may leave either name, never an overwritten foreign tree.
    libc = ctypes.CDLL(None, use_errno=True)
    call = libc.renameat2
    call.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    call.restype = ctypes.c_int
    if call(-100, os.fsencode(source), -100, os.fsencode(target), 1):
        raise OSError(ctypes.get_errno(), 'rotation no-replace refused')
    sync(source.parent)
    sync(target.parent)


def within(path, roots):
    return any(path == root or root in path.parents for root in roots)


def local_containers():
    """Disk supplies only a stable ID set; every state/mount comes from Docker."""
    def socket_identity():
        parent = b.trusted_directory(DOCKER_SOCKET.parent)
        os.close(parent)
        info = DOCKER_SOCKET.lstat()
        require(stat.S_ISSOCK(info.st_mode) and info.st_uid == UID and not info.st_mode & 0o002 and info.st_nlink == 1, 'docker_socket_untrusted')
        return [info.st_dev, info.st_ino, info.st_ctime_ns]
    socket_before = socket_identity()
    endpoint = ['/usr/bin/docker', '--host', 'unix://' + str(DOCKER_SOCKET)]
    raw = json.loads(b.command([*endpoint, 'info', '--format', '{{json .DockerRootDir}}'], timeout=15))
    require(isinstance(raw, str) and Path(raw).is_absolute() and os.path.normpath(raw) == raw, 'docker_data_root')
    root = Path(raw)
    root_fd = b.trusted_directory(root)
    containers = root / 'containers'
    directory_fd = None
    try:
        directory_fd = b.trusted_directory(containers)
        def unchanged_directory(path, fd):
            checked = b.trusted_directory(path)
            try:
                found = os.fstat(checked); held = os.fstat(fd)
                require([found.st_dev, found.st_ino] == [held.st_dev, held.st_ino], 'docker_directory_changed')
            finally:
                os.close(checked)
        def snapshot():
            found = {}
            with os.scandir(directory_fd) as entries:
                for entry in entries:
                    info = entry.stat(follow_symlinks=False)
                    require(re.fullmatch('[a-f0-9]{64}', entry.name) and stat.S_ISDIR(info.st_mode) and info.st_uid == UID and not info.st_mode & 0o022, 'docker_directory_entry')
                    found[entry.name] = [info.st_dev, info.st_ino]
            return found
        before = snapshot(); rows = []
        for cid in sorted(before):
            inspected = json.loads(b.command([*endpoint, 'container', 'inspect', cid], timeout=15))
            require(isinstance(inspected, list) and len(inspected) == 1 and inspected[0].get('Id') == cid and isinstance(inspected[0].get('State'), dict) and isinstance(inspected[0].get('Mounts'), list), 'container_inventory')
            rows.append(inspected[0])
        require(snapshot() == before, 'docker_directory_changed')
        unchanged_directory(root, root_fd); unchanged_directory(containers, directory_fd)
        require(socket_identity() == socket_before, 'docker_socket_changed')
        return rows
    finally:
        if directory_fd is not None: os.close(directory_fd)
        os.close(root_fd)


def container_holds_resources(row):
    state = row['State']
    require(state.get('Status') in {'created', 'exited', 'dead', 'running', 'paused', 'restarting', 'removing'} and all(type(state.get(key)) is bool for key in ['Running', 'Paused', 'Restarting']) and type(state.get('Pid')) is int and state['Pid'] >= 0, 'container_inventory')
    return any(state[key] for key in ['Running', 'Paused', 'Restarting']) or state.get('Dead') is True or state['Pid'] > 0 or state['Status'] in {'running', 'paused', 'restarting', 'removing', 'dead'}


def no_jobs(roots, owned_locks=()):
    """Observe actual paths, never substring-match a python -c source string."""
    roots = [path.resolve() for path in roots]
    known = {'storage-operator.mjs', 'storage-backup.mjs', 'storage-receiver.mjs', 'storage-maintenance.mjs', 'install-tools.py', 'install-pg-tools.py'}
    matches = []
    for process in PROC.iterdir():
        if not process.name.isdigit():
            continue
        try:
            command = (process / 'cmdline').read_bytes().split(b'\0')
            referenced = False
            executable = Path(os.fsdecode(command[0])).name if command and command[0] else ''
            entry = None
            if executable in {'node', 'nodejs'} or re.fullmatch(r'python(?:3(?:\.[0-9]+)?)?', executable):
                for raw in command[1:]:
                    arg = os.fsdecode(raw)
                    if arg in {'-c', '-m', '-e', '--eval', '-p', '--print'}:
                        break
                    if arg and not arg.startswith('-'):
                        entry = arg
                        break
            if entry is not None:
                referenced |= Path(entry).name in known
            for raw in command:
                if not raw:
                    continue
                arg = os.fsdecode(raw)
                # Only a standalone filename/path argument is an entrypoint.
                if '\n' not in arg and '\r' not in arg and not any(char.isspace() for char in arg):
                    path = Path(arg)
                    referenced |= path.is_absolute() and within(path.resolve(), roots)
            for name in ['exe', 'cwd']:
                try:
                    referenced |= within((process / name).resolve(strict=True), roots)
                except FileNotFoundError:
                    pass
            maps = (process / 'maps').read_bytes()
            require(len(maps) <= 16777216, 'process_budget')
            for line in maps.splitlines():
                fields = line.split(maxsplit=5)
                if len(fields) == 6 and fields[5].startswith(b'/'):
                    referenced |= within(Path(os.fsdecode(fields[5].removesuffix(b' (deleted)'))), roots)
            for fd in (process / 'fd').iterdir():
                if int(process.name) == os.getpid() and int(fd.name) in owned_locks:
                    continue
                try:
                    target = os.readlink(fd)
                    if target.startswith('/'):
                        referenced |= within(Path(target.removesuffix(' (deleted)')), roots)
                except FileNotFoundError:
                    pass
            if referenced:
                matches.append(int(process.name))
        except FileNotFoundError:
            if process.exists():
                require(False, 'process_observation_incomplete')
        except PermissionError:
            require(False, 'process_observation_incomplete')
    require(not matches, 'host_jobs_active')
    for row in local_containers():
        if not container_holds_resources(row):
            continue
        for mount in row['Mounts']:
            origin = Path(mount.get('Source', '')).resolve()
            require(not any(within(origin, [root]) or origin in root.parents for root in roots), 'mounted_tools_active')


def tree(path, config, expected_identity=None, receipt_sha=None, verify_runtime=False):
    trusted(path, directory=True)
    if expected_identity is not None:
        require(identity(path) == expected_identity, 'tree_identity')
    b.verify_tool_tree(path)
    raw = read_control(path / 'installation.json')
    if receipt_sha is not None:
        require(sha(raw) == receipt_sha, 'receipt_changed')
    receipt = json.loads(raw)
    require(all(receipt.get(key) == config[key] for key in ['businessImage', 'infrastructureImage', 'revision', 'nodeVersion']), 'receipt_binding')
    if verify_runtime:
        proof = b.verify_copy(path, config)
        require(all(receipt.get(key) == proof[key] for key in ['revision', 'sourceDigest', 'nodeVersion', 'nodeDigest']), 'runtime_binding')
    return identity(path), sha(raw)


def profile_bytes(journal):
    return {key: base64.b64decode(journal[key + 'Profile'], validate=True) for key in ['old', 'new']}


def verify_profile(fd, journal):
    info = os.fstat(fd)
    require(stat.S_ISREG(info.st_mode) and info.st_uid == UID and stat.S_IMODE(info.st_mode) == 0o600 and [info.st_dev, info.st_ino] == journal['profileIdentity'] == identity(b.PROFILE) and info.st_nlink == 1, 'profile_identity')
    os.lseek(fd, 0, os.SEEK_SET)
    current = os.read(fd, LIMIT + 1)
    require(len(current) <= LIMIT, 'profile_budget')
    if not journal['profileWriting']:
        require(current == profile_bytes(journal)[journal['profileTarget']], 'profile_changed')


def write_profile(fd, journal, directory, target, hook):
    verify_profile(fd, journal)
    journal['profileTarget'] = target
    journal['profileWriting'] = True
    save(directory, journal)
    raw = profile_bytes(journal)[target]
    os.lseek(fd, 0, os.SEEK_SET)
    remaining = memoryview(raw)
    while remaining:
        size = os.write(fd, remaining)
        require(size > 0, 'write_failed')
        remaining = remaining[size:]
    os.ftruncate(fd, len(raw)); os.fsync(fd)
    if hook:
        hook('profile-' + target)
    journal['profileWriting'] = False
    verify_profile(fd, journal)
    save(directory, journal)


def exists(path):
    return path.exists() or path.is_symlink()


def run(action, *, hook=None):
    require(action in ['prepare', 'accept', 'rollback'], 'arguments')
    approved, approval_sha = approval()
    parent = b.trusted_directory(b.DESTINATION.parent)
    fd = lock = None
    pg_locks = []
    try:
        lock = os.open('.harbor-storage-tools.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=parent)
        trusted(b.DESTINATION.parent / '.harbor-storage-tools.lock', mode=0o600)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        read_control(b.PROFILE)
        fd = os.open(b.PROFILE, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        directory = HISTORY / approved['id']
        previous, replaced = directory / 'previous', directory / 'replaced'
        journal = None
        if exists(directory):
            trusted(HISTORY, directory=True, mode=0o700); trusted(directory, directory=True, mode=0o700)
            require(set(path.name for path in directory.iterdir()) <= {'journal.json', '.journal.next', 'previous', 'replaced'}, 'history_unknown')
            if exists(directory / 'journal.json'):
                journal = json.loads(read_control(directory / 'journal.json'))
        if journal is None:
            require(action == 'prepare' and not exists(previous) and not exists(replaced), 'journal_missing')
            raw, old = b.configuration()
            require(sha(raw) == approved['expected']['profileSha256'], 'profile_digest')
            tree(b.DESTINATION, old, approved['expected']['treeIdentity'], approved['expected']['receiptSha256'])
            require(old != approved['nextTools'], 'no_change')
            journal = {'version': 1, 'approvalDigest': approval_sha, 'oldProfile': base64.b64encode(raw).decode(), 'newProfile': base64.b64encode(body(approved['nextTools'])).decode(), 'profileIdentity': identity(b.PROFILE), 'profileTarget': 'old', 'profileWriting': False, 'oldIdentity': approved['expected']['treeIdentity'], 'oldMoveIntent': False, 'oldRestoreIntent': False, 'nextIdentity': None, 'nextReceiptSha': None, 'nextMoveIntent': False, 'direction': 'prepare', 'status': 'pending'}
        require(journal.get('version') == 1 and journal.get('approvalDigest') == approval_sha, 'journal_binding')
        profiles = profile_bytes(journal)
        require(sha(profiles['old']) == approved['expected']['profileSha256'] and profiles['new'] == body(approved['nextTools']) and journal['oldIdentity'] == approved['expected']['treeIdentity'], 'journal_binding')
        old = next_config(json.loads(profiles['old']))
        verify_profile(fd, journal)
        # The PG installer has its own lock inside the installed tree. Hold it
        # across rename too, so a process that already opened that directory
        # cannot start installing through an old directory descriptor.
        for path in [b.DESTINATION, previous, replaced]:
            if not exists(path):
                continue
            if identity(path) == journal['oldIdentity']:
                tree(path, old, journal['oldIdentity'], approved['expected']['receiptSha256'])
            else:
                require(journal['status'] in ['prepared', 'accepted'] or journal['nextMoveIntent'], 'canonical_unexpected')
                tree(path, approved['nextTools'], journal['nextIdentity'], journal['nextReceiptSha'])
            descriptor = os.open(path / '.postgres-tools.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
            pg_locks.append(descriptor)
            trusted(path / '.postgres-tools.lock', mode=0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        def idle():
            no_jobs([b.DESTINATION, previous, replaced], owned_locks=pg_locks)
        idle()
        if action != 'rollback':
            b.image_identity(approved['nextTools'])
        mkdir(HISTORY); mkdir(directory)
        if action == 'prepare':
            require(journal['direction'] == 'prepare', 'direction_conflict')
            if exists(previous):
                require(journal['oldMoveIntent'], 'unexpected_previous')
                tree(previous, old, journal['oldIdentity'], approved['expected']['receiptSha256'])
            else:
                tree(b.DESTINATION, old, journal['oldIdentity'], approved['expected']['receiptSha256'])
                journal['oldMoveIntent'] = True; save(directory, journal)
                idle()
                rename(b.DESTINATION, previous)
                if hook: hook('old-tree-moved')
            if journal['status'] == 'pending':
                require(not exists(b.DESTINATION), 'canonical_unexpected')
                idle()
                write_profile(fd, journal, directory, 'new', hook)
                journal['status'] = 'prepared'; save(directory, journal)
            else:
                require(journal['status'] in ['prepared', 'accepted'] and journal['profileTarget'] == 'new' and not journal['profileWriting'], 'journal_state')
                if exists(b.DESTINATION):
                    tree(b.DESTINATION, approved['nextTools'], journal['nextIdentity'], journal['nextReceiptSha'], verify_runtime=True)
                else:
                    require(journal['status'] == 'prepared', 'canonical_missing')
            sync(previous.parent); sync(b.DESTINATION.parent)
        elif action == 'accept':
            require(journal['direction'] == 'prepare' and journal['status'] in ['prepared', 'accepted'] and journal['profileTarget'] == 'new' and not journal['profileWriting'], 'not_prepared')
            tree(previous, old, journal['oldIdentity'], approved['expected']['receiptSha256'])
            new_id, new_sha = tree(b.DESTINATION, approved['nextTools'], journal['nextIdentity'], journal['nextReceiptSha'], verify_runtime=True)
            journal['nextIdentity'] = new_id; journal['nextReceiptSha'] = new_sha
            journal['status'] = 'accepted'; save(directory, journal)
        else:
            journal['direction'] = 'rollback'; save(directory, journal)
            if exists(previous):
                tree(previous, old, journal['oldIdentity'], approved['expected']['receiptSha256'])
                if exists(b.DESTINATION):
                    require(not exists(replaced), 'replaced_collision')
                    new_id, new_sha = tree(b.DESTINATION, approved['nextTools'], journal['nextIdentity'], journal['nextReceiptSha'], verify_runtime=journal['nextIdentity'] is None)
                    journal['nextIdentity'] = new_id; journal['nextReceiptSha'] = new_sha; journal['nextMoveIntent'] = True
                    save(directory, journal); idle()
                    rename(b.DESTINATION, replaced)
                    if hook: hook('new-tree-moved')
                if exists(replaced):
                    require(journal['nextMoveIntent'] and journal['nextIdentity'] is not None, 'unexpected_replaced')
                    tree(replaced, approved['nextTools'], journal['nextIdentity'], journal['nextReceiptSha'])
            else:
                require(not journal['oldMoveIntent'] or journal['oldRestoreIntent'], 'previous_missing')
                tree(b.DESTINATION, old, journal['oldIdentity'], approved['expected']['receiptSha256'])
            idle()
            write_profile(fd, journal, directory, 'old', hook)
            if exists(previous):
                require(not exists(b.DESTINATION), 'canonical_unexpected')
                journal['oldRestoreIntent'] = True; save(directory, journal)
                rename(previous, b.DESTINATION)
                if hook: hook('old-tree-restored')
            tree(b.DESTINATION, old, journal['oldIdentity'], approved['expected']['receiptSha256'])
            journal['status'] = 'rolled-back'; save(directory, journal)
            sync(directory); sync(b.DESTINATION.parent)
        verify_profile(fd, journal)
        return {'scope': 'idle-host-tools-rotation', 'action': action, 'status': journal['status'], 'oldTreePreserved': True, 'oldProfilePreserved': True, 'newInstallerRequired': journal['status'] == 'prepared', 'servicesModified': False, 'deploymentAllowed': False}
    finally:
        for descriptor in pg_locks: os.close(descriptor)
        if fd is not None: os.close(fd)
        if lock is not None: os.close(lock)
        os.close(parent)


if __name__ == '__main__':
    try:
        require(len(sys.argv) == 2, 'arguments')
        print(json.dumps(run(sys.argv[1])))
    except Exception as error:
        code = str(error) if isinstance(error, b.Refused) and re.fullmatch('[a-z_]{1,100}', str(error)) else 'rotation_unavailable'
        print(json.dumps({'error': code, 'deploymentAllowed': False}), file=sys.stderr)
        sys.exit(1)
