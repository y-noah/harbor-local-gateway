"""Root-controlled Harbor data paths; no filesystem operations at import time."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile
import uuid

CONFIG = Path('/etc/harbor-data-paths.json')
MAINTENANCE = Path('/etc/harbor-material-maintenance.json')
DISK = Path('/sdc-disk')
ROOT = DISK / 'harbor-data'
STACK = Path('/opt/product-development/harbor-local-gateway')
CONTRACT = 'harbor-material-storage-v1'
TARGETS = {'restore': 'restore', 'application-data': 'application/data',
           'application-runtime': 'application/runtime',
           'release-materials': 'release-materials', 'release-tmp': 'release-tmp'}
FIXED_LOGICAL = {'restore': '/var/lib/harbor-restore',
                 'release-materials': '/var/lib/harbor-release-materials',
                 'release-tmp': '/var/lib/harbor-release-tmp'}


def need(ok, code):
    if not ok:
        raise ValueError('harbor_material_' + code)


def command(args):
    result = subprocess.run(args, stdin=subprocess.DEVNULL, capture_output=True,
                            timeout=20, env={'PATH': '/usr/bin:/bin', 'LANG': 'C'})
    need(result.returncode == 0, 'observation_failed')
    return result.stdout


def identity(path):
    s = Path(path).lstat()
    return [s.st_dev, s.st_ino, s.st_mode, s.st_uid, s.st_gid, s.st_nlink,
            s.st_size, s.st_mtime_ns, s.st_ctime_ns]


def directory(path, uid=0):
    path = Path(path)
    need(path.is_absolute() and str(path) == os.path.normpath(path), 'path_invalid')
    for parent in reversed(path.parents):
        s = parent.lstat()
        need(stat.S_ISDIR(s.st_mode) and not stat.S_ISLNK(s.st_mode) and
             s.st_uid == 0 and not s.st_mode & 0o022, 'parent_untrusted')
    s = path.lstat()
    need(stat.S_ISDIR(s.st_mode) and s.st_uid == uid and not s.st_mode & 0o022,
         'directory_untrusted')
    return s


def private_json(path, limit=262144):
    directory(path.parent)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        s = os.fstat(stream.fileno())
        need(stat.S_ISREG(s.st_mode) and s.st_uid == 0 and
             stat.S_IMODE(s.st_mode) == 0o600 and s.st_nlink == 1 and
             s.st_size <= limit, 'configuration_untrusted')
        raw = stream.read(limit + 1)
        need(identity(path) == [s.st_dev, s.st_ino, s.st_mode, s.st_uid, s.st_gid,
                               s.st_nlink, s.st_size, s.st_mtime_ns, s.st_ctime_ns],
             'configuration_changed')
    return json.loads(raw)


def validate(value):
    return validate_roots(value, physical=True)


def validate_plan(value):
    return validate_roots(value, physical=False)


def validate_roots(value, *, physical):
    need(isinstance(value, dict) and set(value) == {'contract', 'version', 'diskUuid', 'roots'}
         and value['contract'] == CONTRACT and type(value['version']) is int and value['version'] == 1 and
         isinstance(value['diskUuid'], str) and re.fullmatch('[a-fA-F0-9-]{8,64}', value['diskUuid'])
         and isinstance(value['roots'], dict) and set(value['roots']) == set(TARGETS),
         'configuration_invalid')
    app_parents = set()
    for purpose, row in value['roots'].items():
        need(isinstance(row, dict) and set(row) == ({'logical', 'target', 'uid', 'targetInode'} if physical else {'logical', 'target', 'uid'})
             and row['target'] == str(ROOT / TARGETS[purpose]) and
             type(row['uid']) is int and row['uid'] == (1000 if purpose.startswith('application-') else 999 if purpose == 'restore' else 0)
             and (not physical or type(row['targetInode']) is int and row['targetInode'] > 0),
             'root_invalid')
        if purpose in FIXED_LOGICAL:
            need(row['logical'] == FIXED_LOGICAL[purpose], 'logical_scope_invalid')
        else:
            suffix = 'pg-data' if purpose == 'application-data' else 'runtime'
            pattern = re.escape(str(STACK)) + r'/local-assurance/[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}/' + suffix
            need(isinstance(row['logical'], str) and re.fullmatch(pattern, row['logical']), 'logical_scope_invalid')
            app_parents.add(str(Path(row['logical']).parent))
    need(len(app_parents) == 1, 'application_pair_mismatch')
    if physical:
        need(len({row['targetInode'] for row in value['roots'].values()}) == len(TARGETS), 'overlapping_target_roots')
    return value


def configuration():
    # Absence is compatible with an installation that has never enabled this
    # migration. The marker stays on /etc, so losing the data disk cannot hide it.
    if not os.path.lexists(CONFIG):
        return None
    return validate(private_json(CONFIG))


def disk_mount(value):
    rows = json.loads(command(['/usr/bin/findmnt', '--json', '--mountpoint', str(DISK),
                               '--output', 'TARGET,UUID,MAJ:MIN']))['filesystems']
    need(len(rows) == 1 and rows[0]['target'] == str(DISK) and
         rows[0]['uuid'] == value['diskUuid'], 'disk_unavailable')
    s = directory(DISK)
    need(s.st_dev != Path('/').stat().st_dev and directory(ROOT).st_dev == s.st_dev,
         'disk_unavailable')
    return s.st_dev


def boot_unit(value):
    disk_mount(value)
    name = command(['/usr/bin/systemd-escape', '--path', '--suffix=mount', str(DISK)]).decode().strip()
    raw = command(['/usr/bin/systemctl', 'show', name, '--property=LoadState,ActiveState,UnitFileState,FragmentPath,SourcePath,Where'])
    props = dict(line.split('=', 1) for line in raw.decode().splitlines() if '=' in line)
    need(props.get('LoadState') == 'loaded' and props.get('ActiveState') == 'active' and props.get('Where') == str(DISK), 'boot_disk_unavailable')
    fragment = Path(props.get('FragmentPath', ''))
    expected = {('UUID=' + value['diskUuid']).lower(), ('/dev/disk/by-uuid/' + value['diskUuid']).lower()}
    if fragment == Path('/run/systemd/generator') / name:
        need(props.get('SourcePath') == '/etc/fstab' and props.get('UnitFileState') == 'generated', 'boot_disk_not_persistent')
        rows = json.loads(command(['/usr/bin/findmnt', '--json', '--fstab', '--mountpoint', str(DISK), '--output', 'TARGET,SOURCE']))['filesystems']
        need(len(rows) == 1 and rows[0].get('target') == str(DISK) and rows[0].get('source', '').lower() in expected, 'boot_disk_not_uuid_bound')
    else:
        need(fragment == Path('/etc/systemd/system') / name and props.get('UnitFileState') == 'enabled', 'boot_disk_not_persistent')
        directory(fragment.parent)
        fd = os.open(fragment, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(fd, 'rb') as stream:
            info = os.fstat(stream.fileno())
            need(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022 and info.st_size <= 65536, 'boot_disk_untrusted')
            lines = stream.read(65537).decode().splitlines()
        what = [line.partition('=')[2] for line in lines if line.startswith('What=')]
        need(len(what) == 1 and what[0].lower() in expected, 'boot_disk_not_uuid_bound')
    return name


def binding(value, purpose, device):
    row = value['roots'][purpose]
    target = directory(Path(row['target']), row['uid'])
    logical = directory(Path(row['logical']), row['uid'])
    need(target.st_dev == device and target.st_ino == row['targetInode'] and
         (logical.st_dev, logical.st_ino) == (target.st_dev, target.st_ino), 'bind_unavailable')
    mounts = json.loads(command(['/usr/bin/findmnt', '--json', '--mountpoint', row['logical'],
                                 '--output', 'TARGET,UUID']))['filesystems']
    need(len(mounts) == 1 and mounts[0]['target'] == row['logical'] and
         mounts[0]['uuid'] == value['diskUuid'], 'bind_unavailable')
    return Path(row['logical'])


def guard(purposes, expected=None, required=False):
    need(purposes and set(purposes) <= set(TARGETS), 'purpose_invalid')
    need(not os.path.lexists(MAINTENANCE), 'maintenance_in_progress')
    value = configuration()
    if value is None:
        need(not required, 'configuration_required')
        return {'enabled': False}
    device = disk_mount(value)
    for purpose in purposes:
        if expected is not None:
            need(str(expected[purpose]) == value['roots'][purpose]['logical'], 'caller_path_mismatch')
        binding(value, purpose, device)
    return {'enabled': True, 'purposes': list(purposes), 'mountsVerified': True}


def allocate(purpose, name):
    need(purpose in ['release-materials', 'release-tmp'] and
         isinstance(name, str) and re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}', name), 'allocation_scope_invalid')
    guard([purpose], required=True)
    value = configuration()
    root = Path(value['roots'][purpose]['logical'])
    # A directory fd pins the verified mount across allocation. If a privileged
    # unmount races, the caller cannot publish a path on the old filesystem.
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        guard([purpose], required=True)
        need((before.st_dev, before.st_ino) == (root.stat().st_dev, root.stat().st_ino), 'allocation_raced')
        os.mkdir(name, 0o700, dir_fd=fd)
        os.fsync(fd)
        guard([purpose], required=True)
        result = root / name
        need(result.lstat().st_uid == 0 and result.lstat().st_dev == before.st_dev,
             'allocation_raced')
        return {'directory': str(result), 'purpose': purpose, 'newDiskVerified': True}
    finally:
        os.close(fd)


def provision_release(disk_uuid):
    need(isinstance(disk_uuid, str) and re.fullmatch('[a-fA-F0-9-]{8,64}', disk_uuid), 'disk_uuid_invalid')
    need(not os.path.lexists(MAINTENANCE), 'maintenance_in_progress')
    value = {'diskUuid': disk_uuid}
    boot_unit(value)
    device = disk_mount(value)
    roots = {}
    for purpose in ['release-materials', 'release-tmp']:
        if configuration() is not None:
            guard([purpose], required=True)
        target = ROOT / TARGETS[purpose]
        if not os.path.lexists(target):
            target.mkdir(mode=0o700)
            fd = os.open(ROOT, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        info = directory(target)
        need(info.st_dev == device, 'release_root_wrong_filesystem')
        roots[purpose] = [info.st_dev, info.st_ino]
    need(len({tuple(v) for v in roots.values()}) == 2, 'overlapping_target_roots')
    return {'contract': 'harbor-release-storage-allocation-v1', 'diskUuid': disk_uuid, 'roots': roots}


def allocate_approved(purpose, name, approval):
    need(purpose in ['release-materials', 'release-tmp'] and isinstance(name, str) and
         re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}', name), 'allocation_scope_invalid')
    need(isinstance(approval, dict) and set(approval) == {'contract', 'diskUuid', 'roots'} and
         approval['contract'] == 'harbor-release-storage-allocation-v1' and
         isinstance(approval['diskUuid'], str) and re.fullmatch('[a-fA-F0-9-]{8,64}', approval['diskUuid']) and
         isinstance(approval['roots'], dict) and set(approval['roots']) == {'release-materials', 'release-tmp'} and
         all(isinstance(v, list) and len(v) == 2 and all(type(x) is int and x > 0 for x in v) for v in approval['roots'].values()), 'release_approval_invalid')
    need(len({tuple(v) for v in approval['roots'].values()}) == 2, 'overlapping_target_roots')
    need(not os.path.lexists(MAINTENANCE), 'maintenance_in_progress')
    device = disk_mount(approval)
    if configuration() is not None:
        guard([purpose], required=True)
    root = ROOT / TARGETS[purpose]
    info = directory(root)
    need([info.st_dev, info.st_ino] == approval['roots'][purpose] and info.st_dev == device,
         'release_root_changed')
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        pinned = os.fstat(fd)
        need([pinned.st_dev, pinned.st_ino] == approval['roots'][purpose], 'release_root_changed')
        need(not os.path.lexists(MAINTENANCE), 'maintenance_in_progress')
        os.mkdir(name, 0o700, dir_fd=fd)
        os.fsync(fd)
        disk_mount(approval)
        need(identity(root)[:2] == approval['roots'][purpose], 'allocation_raced')
        return {'directory': str(root / name), 'purpose': purpose, 'newDiskVerified': True,
                'fullMaterialMigrationVerified': False}
    finally:
        os.close(fd)


def unit_text(logical, target, disk_unit):
    need(str(logical).startswith('/') and str(target).startswith('/') and
         not any(c in str(logical) + str(target) + disk_unit for c in '\n\r\x00') and
         disk_unit.endswith('.mount') and '/' not in disk_unit, 'unit_scope_invalid')
    return ('[Unit]\nDescription=Harbor persistent material storage\nRequires=' + disk_unit +
            '\nAfter=' + disk_unit + '\nRequiresMountsFor=' + str(target) +
            '\nBefore=harbor-pg-storage-online.service\n\n[Mount]\nWhat=' + str(target) +
            '\nWhere=' + str(logical) + '\nType=none\nOptions=bind\nTimeoutSec=30\n\n'
            '[Install]\nWantedBy=local-fs.target\n').encode()


def main(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['guard', 'guard-path', 'allocate', 'provision-release'])
    parser.add_argument('purposes', nargs='+')
    options = parser.parse_args(args)
    need(os.geteuid() == 0, 'root_required')
    if options.action == 'provision-release':
        need(len(options.purposes) == 1, 'provision_arguments_invalid')
        return provision_release(options.purposes[0])
    if options.action == 'guard':
        return guard(options.purposes)
    if options.action == 'guard-path':
        need(len(options.purposes) == 2, 'guard_arguments_invalid')
        purpose, path = options.purposes
        return guard([purpose], expected={purpose: path}, required=True)
    need(len(options.purposes) == 2, 'allocation_arguments_invalid')
    return allocate(*options.purposes)


if __name__ == '__main__':
    try:
        print(json.dumps(main()))
    except Exception as error:
        code = str(error) if isinstance(error, ValueError) and re.fullmatch('harbor_material_[a-z_]+', str(error)) else 'harbor_material_failed_closed'
        print(json.dumps({'ok': False, 'code': code}))
        raise SystemExit(1)
