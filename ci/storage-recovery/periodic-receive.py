#!/usr/bin/python3 -I
"""固定 SSH 接收入口；只交给密文 receiver，不提供恢复或部署权限。"""
import importlib.util
import fcntl
from contextlib import contextmanager
import json
import os
from pathlib import Path
import stat
import sys

WRAPPER = Path('/usr/local/lib/harbor/periodic-receive.py')
TOOLS = Path('/opt/harbor-storage-tools')
NODE = TOOLS / 'node/bin/node'
ENTRY = TOOLS / 'app/storage-periodic-transfer.mjs'
INSTALLER = TOOLS / 'app/ci/storage-recovery/install-tools.py'
LOCK = Path('/run/harbor-periodic-receive.lock')
ENVIRONMENT = {'PATH': '/usr/bin:/bin', 'LANG': 'C'}


class Refused(Exception):
    pass


def invocation(arguments, uid, euid, original_command, terminal):
    if terminal or uid != euid:
        raise Refused('periodic_receive_identity')
    if uid == 0:
        if arguments != ['--root-receive']:
            raise Refused('periodic_receive_command')
        return 'root'
    if arguments or original_command != 'harbor-periodic-receive':
        raise Refused('periodic_receive_command')
    return 'ssh'


def trusted_file(path, executable=False):
    for parent in path.parents:
        info = parent.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise Refused('periodic_receive_tool_path')
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_nlink != 1 or
            info.st_mode & 0o6022 or not info.st_size or
            executable and not info.st_mode & 0o111):
        raise Refused('periodic_receive_tool_file')


@contextmanager
def receive_lease():
    for parent in LOCK.parents:
        info = parent.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise Refused('periodic_receive_lock')
    fd = os.open(LOCK, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or
                stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
            raise Refused('periodic_receive_lock')
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise Refused('periodic_receive_busy') from None
        current = LOCK.lstat()
        if (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino):
            raise Refused('periodic_receive_lock')
        # Keep one receiver for the fixed profile, including pre-frame waits.
        # exec preserves this open description; process exit releases it.
        os.set_inheritable(fd, True)
        yield
    finally:
        os.close(fd)


def receive():
    for path in [NODE, ENTRY, INSTALLER]:
        trusted_file(path, executable=path == NODE)
    # Reuse the installer's complete root-ownership/link/mode verification.
    # Its own file and ancestors are trusted before loading any Python code.
    spec = importlib.util.spec_from_file_location('harbor_installed_tools', INSTALLER)
    installer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(installer)
    installer.verify_tool_tree(TOOLS)
    os.chdir('/')
    os.execve(str(NODE), [str(NODE), str(ENTRY), 'receive'], ENVIRONMENT)


def main():
    if sys.platform != 'linux' or Path(__file__) != WRAPPER:
        raise Refused('periodic_receive_fixed_entry')
    mode = invocation(sys.argv[1:], os.getuid(), os.geteuid(),
                      os.environ.get('SSH_ORIGINAL_COMMAND'),
                      any(os.isatty(fd) for fd in (0, 1, 2)))
    trusted_file(WRAPPER)
    os.umask(0o077)
    if mode == 'root':
        with receive_lease():
            receive()
    else:
        os.chdir('/')
        os.execve('/usr/bin/sudo', ['/usr/bin/sudo', '-n', '--', '/usr/bin/python3',
                                  '-I', '-B', str(WRAPPER), '--root-receive'], ENVIRONMENT)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'error': str(error) if isinstance(error, Refused) else
                          'periodic_receive_unavailable', 'deploymentAllowed': False}), file=sys.stderr)
        sys.exit(1)
