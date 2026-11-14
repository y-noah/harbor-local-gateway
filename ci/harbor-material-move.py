"""Approved cold relocation for Harbor material roots; preserves original trees."""
import contextlib
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import uuid

spec = importlib.util.spec_from_file_location('material_paths', Path(__file__).with_name('harbor-material-storage.py'))
g = importlib.util.module_from_spec(spec)
spec.loader.exec_module(g)
APPROVAL = Path('/etc/harbor-material-move.json')
STATE = g.ROOT / '.material-move-state.json'
UNIT_ROOT = Path('/etc/systemd/system')
LOCKS = ['/run/harbor-gateway-deploy.lock', '/run/harbor-entry/controller-state.lock', '/run/harbor-material-move.lock']
ARCHIVE_LOCK = g.STACK / 'postgres-infra/archive/.backup-coordination.lock'
UNITS = ['harbor-postgres-backup-schedule.service', 'harbor-postgres-maintain-base.service', 'harbor-postgres-maintain-base.timer']
APPS = ['harbor-local-gateway', 'harbor-local-gateway-worker']
GUARD_FILES = {'/usr/local/lib/harbor/postgres-deploy.py': b'MATERIAL_STORAGE_CONTRACT',
               '/opt/harbor-storage-tools/app/harbor-material-storage.mjs': b'materialStorageGuardContract',
               '/opt/harbor-storage-tools/app/postgres-backup-lock.mjs': b'requireMaterialMaintenanceInactive',
               '/opt/harbor-storage-tools/app/postgres-backup-restore.mjs': b'requireInstalledMaterialStorage'}


def sync(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def save(path, value):
    g.directory(path.parent)
    raw = json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
    g.need(len(raw) <= 33554432, 'state_budget_exceeded')
    temporary = path.with_name('.' + path.name + '-' + uuid.uuid4().hex)
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def run(args, timeout=60):
    result = subprocess.run(args, stdin=subprocess.DEVNULL, capture_output=True, timeout=timeout,
                            env={'PATH': '/usr/bin:/bin', 'LANG': 'C', 'HOME': '/nonexistent', 'DOCKER_CONFIG': '/nonexistent'})
    g.need(result.returncode == 0, 'command_failed')
    return result.stdout


def guard_code(path):
    g.directory(path.parent)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        g.need(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022 and info.st_nlink == 1 and info.st_size <= 1048576, 'installed_guard_untrusted')
        raw = stream.read(1048577)
    g.need(GUARD_FILES[str(path)] in raw, 'installed_guards_missing')
    return raw


def approval():
    value = g.private_json(APPROVAL)
    g.need(isinstance(value, dict) and set(value) - {'runtimeRebuildableCaches'} == {'contract', 'approved', 'nonce', 'diskUuid', 'materialsDirectory', 'sourceRoots', 'legacyMaterials', 'legacyTemporary', 'containers', 'tools', 'releaseRoots', 'guards'}, 'approval_invalid')
    g.need(value['contract'] == 'harbor-material-move-v1' and value['approved'] is True and
           str(uuid.UUID(value['nonce'])) == value['nonce'] and set(value['sourceRoots']) == set(g.TARGETS), 'approval_invalid')
    g.need(set(value['containers']) == set(APPS) and all(re.fullmatch('[a-f0-9]{64}', v) for v in value['containers'].values()), 'container_approval_invalid')
    g.need(isinstance(value['releaseRoots'], dict) and set(value['releaseRoots']) == {'release-materials', 'release-tmp'} and all(isinstance(v, list) and len(v) == 2 and all(type(n) is int and n > 0 for n in v) for v in value['releaseRoots'].values()), 'release_roots_invalid')
    g.need(isinstance(value['guards'], dict) and set(value['guards']) == set(GUARD_FILES), 'installed_guards_missing')
    for name, expected in value['guards'].items():
        g.need(hashlib.sha256(guard_code(Path(name))).hexdigest() == expected, 'installed_guard_changed')
    expected_tools = {str(Path(__file__).resolve()), str(Path(g.__file__).resolve())}
    g.need(set(value['tools']) == expected_tools and all(hashlib.sha256(Path(p).read_bytes()).hexdigest() == digest for p, digest in value['tools'].items()), 'tools_changed')
    caches = value.get('runtimeRebuildableCaches', [])
    g.need(isinstance(caches, list) and caches in [[], ['codex-launcher-aliases']], 'runtime_cache_policy_invalid')
    rows = []
    roots = {}
    for purpose, target in g.TARGETS.items():
        source = value['sourceRoots'][purpose]
        g.need(isinstance(source, dict) and set(source) == {'logical', 'sourceId'}, 'source_invalid')
        uid = 1000 if purpose.startswith('application-') else 999 if purpose == 'restore' else 0
        row = {'purpose': purpose, 'logical': source['logical'], 'target': str(g.ROOT / target), 'uid': uid, 'sourceId': source['sourceId'], 'temporary': False}
        if purpose == 'application-runtime' and caches:
            row['runtimeRebuildableCaches'] = caches
        rows.append(row)
        roots[purpose] = {key: row[key] for key in ['logical', 'target', 'uid']}
    g.validate_plan({'contract': g.CONTRACT, 'version': 1, 'diskUuid': value['diskUuid'], 'roots': roots})
    g.need(Path(roots['application-data']['logical']).parent == Path(value['materialsDirectory']), 'application_scope_invalid')
    for kind, pattern, target, temporary in [('legacyMaterials', re.escape(str(g.STACK)) + r'/release-materials-[0-9]+(?:-infrastructure)?', 'release-materials', False), ('legacyTemporary', r'/tmp/harbor-release-[0-9]+-[A-Za-z0-9_-]+', 'release-tmp', True)]:
        g.need(isinstance(value[kind], list) and len(value[kind]) <= 256, 'legacy_inventory_invalid')
        for source in value[kind]:
            g.need(isinstance(source, dict) and set(source) in [{'logical', 'sourceId'}, {'logical', 'sourceId', 'uid'}] and re.fullmatch(pattern, source['logical']), 'legacy_scope_invalid')
            path = Path(source['logical'])
            uid = source.get('uid', 0)
            g.need(type(uid) is int and uid >= 0 and (temporary or uid == 0), 'legacy_owner_invalid')
            rows.append({'purpose': kind + '-' + path.name, 'logical': str(path), 'target': str(g.ROOT / target / path.name), 'uid': uid, 'sourceId': source['sourceId'], 'temporary': temporary})
    g.need(len({r['logical'] for r in rows}) == len(rows) and len({r['target'] for r in rows}) == len(rows), 'overlapping_roots')
    for row in rows:
        expected = row['sourceId']
        g.need(expected is None and row['purpose'] in ['release-materials', 'release-tmp'] or isinstance(expected, list) and len(expected) == 2 and all(type(n) is int and n > 0 for n in expected), 'source_identity_invalid')
    return value, rows


def quiescent(value):
    fmt = '[{{json .Id}},{{json .State.Running}},{{json .State.Paused}},{{json .State.Restarting}},{{json .HostConfig.RestartPolicy.Name}}]'
    for name in APPS:
        row = json.loads(run(['/usr/bin/docker', '--host', 'unix:///run/docker.sock', 'inspect', '--format', fmt, name]))
        g.need(row == [value['containers'][name], False, False, False, 'no'], 'applications_not_fenced')
    for unit in UNITS:
        result = run(['/usr/bin/systemctl', 'show', unit, '--property=ActiveState,MainPID'])
        prop = dict(line.split('=', 1) for line in result.decode().splitlines() if '=' in line)
        g.need(prop.get('ActiveState') in ['inactive', 'failed'] and prop.get('MainPID', '0') == '0', 'backup_writer_active')
    deploy = g.private_json(Path('/etc/harbor-gateway-deploy.json'))
    g.need(deploy.get('image_staging_enabled') is False, 'release_writer_active')


def scan(root, uid, *, digest=False, rebuildable_caches=(), skip_children=()):
    g.need(list(rebuildable_caches) in [[], ['codex-launcher-aliases']], 'runtime_cache_policy_invalid')
    root = Path(root)
    device = root.lstat().st_dev
    entries = {}
    budget = 2000000
    def walk(path):
        before = g.identity(path)
        mode = before[2]
        g.need(before[0] == device and before[3] == uid and not mode & 0o022 and
               (stat.S_ISREG(mode) and before[5] == 1 or stat.S_ISDIR(mode)), 'tree_untrusted')
        name = str(path.relative_to(root))
        row = {'identity': before, 'mode': stat.S_IMODE(mode), 'uid': before[3], 'gid': before[4]}
        if stat.S_ISDIR(mode):
            names = sorted(path.iterdir())
            for child in names:
                if path == root and child.name in skip_children:
                    continue
                relative = str(child.relative_to(root))
                if rebuildable_caches and re.fullmatch(r'accounts/[^/]+/tmp/arg0', relative):
                    info = child.lstat()
                    g.need(not stat.S_ISLNK(info.st_mode), 'tree_untrusted')
                    if stat.S_ISDIR(info.st_mode):
                        g.need(info.st_dev == device and info.st_uid == uid and not info.st_mode & 0o022, 'tree_untrusted')
                        continue
                walk(child)
            g.need(names == sorted(path.iterdir()), 'tree_changed')
        elif digest:
            h = hashlib.sha256()
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, 'rb') as stream:
                s = os.fstat(stream.fileno())
                g.need((s.st_dev, s.st_ino) == tuple(before[:2]), 'tree_changed')
                for chunk in iter(lambda: stream.read(1048576), b''):
                    h.update(chunk)
            row.update(bytes=before[6], sha256=h.hexdigest())
        else:
            row['bytes'] = before[6]
        g.need(g.identity(path) == before and len(entries) < budget, 'tree_changed')
        entries[name] = row
    walk(root)
    return entries


def semantic(tree):
    return {name: {k: v for k, v in row.items() if k != 'identity'} for name, row in tree.items()}


def scan_row(root, row, *, digest=False):
    return scan(root, row['uid'], digest=digest, rebuildable_caches=row.get('runtimeRebuildableCaches', ()))


def clean_scratch(scratch, uid):
    path = Path(scratch['path'])
    before = g.identity(path)
    g.need(before[:2] == scratch['id'] and stat.S_ISDIR(before[2]) and before[3] == os.geteuid() and stat.S_IMODE(before[2]) == 0o700, 'copy_scratch_changed')
    for child in path.iterdir():
        info = child.lstat()
        g.need(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_uid in [os.geteuid(), uid] and not info.st_mode & 0o022, 'copy_scratch_untrusted')
        child.unlink()
    sync(path)


def durable_tree(target, uid, rebuildable_caches=()):
    tree = scan(target, uid, rebuildable_caches=rebuildable_caches)
    for name, attrs in tree.items():
        if not stat.S_ISREG(attrs['identity'][2]):
            continue
        path = Path(target) / name
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            stamp = os.fstat(fd)
            g.need([stamp.st_dev, stamp.st_ino] == attrs['identity'][:2], 'copied_target_changed')
            os.fsync(fd)
            g.need(g.identity(path) == attrs['identity'], 'copied_target_changed')
        finally:
            os.close(fd)
    for name in sorted(tree, key=lambda name: len(Path(name).parts), reverse=True):
        if stat.S_ISDIR(tree[name]['identity'][2]):
            sync(Path(target) / name)
    sync(Path(target).parent)
    g.need(scan(target, uid, rebuildable_caches=rebuildable_caches) == tree, 'copied_target_changed')


def copy_cold(source, target, uid, check, *, scratch=None, rebuildable_caches=(), prechecked=False):
    if scratch is None:
        check()
        with tempfile.TemporaryDirectory(prefix='.harbor-copy-', dir=Path(target).parent) as directory:
            return copy_cold(source, target, uid, check, scratch={'path': directory, 'id': g.identity(directory)[:2]}, rebuildable_caches=rebuildable_caches, prechecked=True)
    if not prechecked:
        check()
    clean_scratch(scratch, uid)
    before = scan(source, uid, digest=True, rebuildable_caches=rebuildable_caches)
    filters = ['--exclude=/accounts/*/tmp/arg0/'] if rebuildable_caches else []
    run(['/usr/bin/rsync', '--archive', '--checksum', '--numeric-ids', '--one-file-system',
         '--no-devices', '--no-specials', '--temp-dir=' + scratch['path'], *filters,
         str(source) + '/', str(target) + '/'], timeout=3600)
    clean_scratch(scratch, uid)
    check()
    after = scan(source, uid, rebuildable_caches=rebuildable_caches)
    copied = scan(target, uid, digest=True, rebuildable_caches=rebuildable_caches)
    unchanged = {name: {key: val for key, val in row.items() if key != 'sha256'} for name, row in before.items()}
    g.need(unchanged == after and semantic(before) == semantic(copied), 'cold_copy_mismatch')
    durable_tree(target, uid, rebuildable_caches)
    return {'sourceTree': before, 'targetTree': copied}


@contextlib.contextmanager
def locks(archive=True):
    fds = []
    try:
        for path in LOCKS:
            fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            s = os.fstat(fd)
            g.need(stat.S_ISREG(s.st_mode) and s.st_uid == 0 and stat.S_IMODE(s.st_mode) == 0o600 and s.st_nlink == 1, 'lock_untrusted')
            fds.append(fd)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if archive:
            fd = os.open(ARCHIVE_LOCK, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK)
            s = os.fstat(fd)
            g.need(stat.S_ISREG(s.st_mode) and s.st_uid == 999 and stat.S_IMODE(s.st_mode) == 0o600 and s.st_nlink == 1, 'archive_lock_untrusted')
            fds.append(fd)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        def release_archive():
            if archive and fd in fds:
                fds.remove(fd)
                os.close(fd)
        yield release_archive
    finally:
        for fd in reversed(fds):
            os.close(fd)


def target_directory(path, uid):
    path = Path(path)
    if not path.exists():
        g.directory(path.parent)
        path.mkdir(mode=0o700)
        os.chown(path, uid, uid)
        sync(path.parent)
    return g.directory(path, uid)



def prepare_scratch(value, index, entry, state):
    stem = str(g.ROOT / ('.material-copy-' + value['nonce'] + '-' + str(index)))
    def planned(path):
        return isinstance(path, str) and re.fullmatch(re.escape(stem) + r'-[a-f0-9]{32}', path)
    if 'scratch' in entry:
        g.need(entry['scratch']['path'] == stem or planned(entry['scratch']['path']), 'copy_scratch_changed')
        return entry['scratch']
    previous = entry.get('scratchIntent')
    if previous is not None:
        g.need(isinstance(previous, dict) and set(previous) == {'phase', 'path'} and previous['phase'] == 'create-intent' and planned(previous['path']), 'copy_scratch_changed')
        if os.path.lexists(previous['path']):
            # Without a durably recorded inode this is not authenticated as ours.
            # Preserve it, and create a different random candidate rather than
            # adopting or deleting it on the strength of its path alone.
            retained = entry.setdefault('unprovedScratchPaths', [])
            g.need(isinstance(retained, list) and len(retained) < 32 and all(planned(path) for path in retained), 'copy_scratch_recovery_budget')
            if previous['path'] not in retained:
                retained.append(previous['path'])
    path = Path(stem + '-' + uuid.uuid4().hex)
    entry['scratchIntent'] = {'phase': 'create-intent', 'path': str(path)}
    save(STATE, state)
    path.mkdir(mode=0o700)
    sync(path.parent)
    entry['scratch'] = {'path': str(path), 'id': g.identity(path)[:2]}
    del entry['scratchIntent']
    save(STATE, state)
    return entry['scratch']


def prepare(value, rows):
    g.need(not os.path.lexists(g.CONFIG), 'already_initialized')
    g.disk_mount(value)
    quiescent(value)
    approval_hash = hashlib.sha256(APPROVAL.read_bytes()).hexdigest()
    if os.path.lexists(STATE):
        state = g.private_json(STATE, 33554432)
        g.need(state.get('nonce') == value['nonce'] and state.get('approvalSha256') == approval_hash and state.get('phase') in ['copying', 'copied'], 'copy_resume_invalid')
        g.need([r['purpose'] for r in state['rows']] == [r['purpose'] for r in rows[:len(state['rows'])]], 'copy_resume_invalid')
    else:
        state = {'contract': value['contract'], 'nonce': value['nonce'], 'approvalSha256': approval_hash, 'phase': 'copying', 'rows': []}
        save(STATE, state)
    for index, row in enumerate(rows):
        source, target = Path(row['logical']), Path(row['target'])
        if index < len(state['rows']):
            entry = state['rows'][index]
            g.need(all(entry[key] == row[key] for key in ['purpose', 'logical', 'target', 'uid', 'temporary']) and g.identity(source)[:2] == entry['sourceId'] and g.identity(target)[:2] == entry['targetId'], 'copy_resume_invalid')
            g.need(row.get('runtimeRebuildableCaches', []) == entry.get('runtimeRebuildableCaches', []) and scan_row(source, row) == entry['sourceMetadata'], 'source_changed')
            if entry['phase'] == 'copied':
                # Group roots may now contain a later, partially copied child.
                # That child's own row owns its target and validates it below.
                if row['purpose'] not in ['release-materials', 'release-tmp']:
                    target_unchanged(entry, state)
                continue
            g.need(entry['phase'] == 'copying', 'copy_resume_invalid')
        else:
            if row['sourceId'] is None:
                g.need(not os.path.lexists(source), 'source_appeared')
                g.directory(source.parent)
                source.mkdir(mode=0o700)
                sync(source.parent)
            else:
                g.need(g.identity(source)[:2] == row['sourceId'], 'source_changed')
            preexisting_group = row['purpose'] in ['release-materials', 'release-tmp'] and os.path.lexists(target)
            if preexisting_group:
                g.need(g.identity(target)[:2] == value['releaseRoots'][row['purpose']] and set(scan_row(source, row)) == {'.'}, 'release_group_changed')
            else:
                g.need(not os.path.lexists(target), 'target_exists')
            if target.parent == g.ROOT / 'application' and not target.parent.exists():
                target.parent.mkdir(mode=0o700)
            target_directory(target, row['uid'])
            entry = dict(row, sourceId=g.identity(source)[:2], targetId=g.identity(target)[:2],
                         sourceMetadata=scan_row(source, row), preexistingGroup=preexisting_group, phase='copying')
            state['rows'].append(entry)
            save(STATE, state)
        prepare_scratch(value, index, entry, state)
        if entry.get('preexistingGroup'):
            entry['proof'] = {'sourceTree': scan_row(source, row, digest=True), 'targetTree': scan_row(target, row, digest=True)}
            durable_tree(target, row['uid'], row.get('runtimeRebuildableCaches', ()))
        else:
            entry['proof'] = copy_cold(source, target, row['uid'], lambda: (g.disk_mount(value), quiescent(value)), scratch=entry['scratch'], rebuildable_caches=row.get('runtimeRebuildableCaches', ()))
        g.need(row.get('runtimeRebuildableCaches', []) == entry.get('runtimeRebuildableCaches', []) and scan_row(source, row) == entry['sourceMetadata'], 'source_changed')
        entry['phase'] = 'copied'
        save(STATE, state)
    for entry in state['rows']:
        target_unchanged(entry, state)
    state['phase'] = 'copied'
    save(STATE, state)
    return {'phase': 'copied', 'roots': len(rows), 'sourcePreserved': True, 'switchPerformed': False}


def target_unchanged(row, state, *, allow_incomplete=False):
    expected = dict(row['proof']['targetTree'])
    grouped = row['purpose'] in ['release-materials', 'release-tmp']
    children = set()
    if grouped:
        for child in state['rows']:
            target = Path(child['target'])
            if target.parent != Path(row['target']):
                continue
            stamp = g.identity(target)
            g.need(stamp[:2] == child['targetId'] and stat.S_ISDIR(stamp[2]) and stamp[3] == child['uid'] and not stamp[2] & 0o022, 'copied_target_changed')
            if 'proof' in child:
                target_unchanged(child, state)
            else:
                g.need(allow_incomplete and child.get('phase') == 'copying', 'copied_target_changed')
            children.add(target.name)
    observed = scan(row['target'], row['uid'], rebuildable_caches=row.get('runtimeRebuildableCaches', ()), skip_children=children)
    g.need(set(expected) == set(observed), 'copied_target_changed')
    for name, before in expected.items():
        actual = observed[name]
        if grouped and name == '.':
            g.need(actual['identity'][:5] == before['identity'][:5], 'copied_target_changed')
        else:
            g.need(actual == {k: v for k, v in before.items() if k != 'sha256'}, 'copied_target_changed')


def mounted(source):
    result = subprocess.run(['/usr/bin/findmnt', '--json', '--mountpoint', str(source), '--output', 'TARGET'], capture_output=True, timeout=20)
    if result.returncode == 1:
        return False
    g.need(result.returncode == 0, 'mount_observation_failed')
    return any(r.get('target') == str(source) for r in json.loads(result.stdout)['filesystems'])


def empty_mountpoint(path):
    return not any(Path(path).iterdir())


def switch(value, state):
    if state['phase'] == 'complete':
        g.guard(list(g.TARGETS), required=True)
        return {'phase': 'complete', 'alreadyComplete': True, 'applicationsStarted': False}
    if state['phase'] == 'activation-ready':
        return finish_activation(value, state)
    g.need(state['phase'] in ['copied', 'switching'], 'switch_phase_invalid')
    g.disk_mount(value)
    quiescent(value)
    state['phase'] = 'switching'
    save(STATE, state)
    disk_unit = g.boot_unit(value)
    for row in state['rows']:
        if row['temporary']:
            g.need(scan_row(row['logical'], row) == {k: {a: b for a, b in r.items() if a != 'sha256'} for k, r in row['proof']['sourceTree'].items()}, 'source_changed')
            target_unchanged(row, state)
            # Historic /tmp copies are retained without creating permanent
            # aliases beneath a world-writable boot-time parent.
            continue
        source, target = Path(row['logical']), Path(row['target'])
        target_unchanged(row, state)
        if row['phase'] == 'bound':
            unit = UNIT_ROOT / row['unit']
            g.need(mounted(source) and g.identity(source)[:2] == row['targetId'] and hashlib.sha256(unit.read_bytes()).hexdigest() == row['unitSha256'], 'bound_root_changed')
            continue
        g.need(row['phase'] in ['copied', 'rename-intent', 'renamed'] and g.identity(target)[:2] == row['targetId'], 'root_changed')
        if row['phase'] == 'copied':
            g.need(g.identity(source)[:2] == row['sourceId'], 'source_changed')
            g.need(scan_row(source, row) == {k: {a: b for a, b in r.items() if a != 'sha256'} for k, r in row['proof']['sourceTree'].items()}, 'source_changed')
            name = run(['/usr/bin/systemd-escape', '--path', '--suffix=mount', str(source)]).decode().strip()
            unit = UNIT_ROOT / name
            g.need(not os.path.lexists(unit) and not mounted(source), 'existing_unit_or_mount')
            old = source.with_name('.harbor-material-rollback-' + value['nonce'] + '-' + source.name)
            g.need(not os.path.lexists(old), 'rollback_root_exists')
            row.update(unit=name, unitSha256=hashlib.sha256(g.unit_text(source, target, disk_unit)).hexdigest(), oldRoot=str(old), phase='rename-intent')
            save(STATE, state)
        name = row['unit']
        unit = UNIT_ROOT / name
        old = Path(row['oldRoot'])
        if row['phase'] == 'rename-intent':
            if not old.exists():
                g.need(not mounted(source) and g.identity(source)[:2] == row['sourceId'], 'rename_source_changed')
                os.rename(source, old)
                sync(source.parent)
            g.need(g.identity(old)[:2] == row['sourceId'], 'rollback_source_changed')
            if not os.path.lexists(source):
                source.mkdir(mode=0o000)
                sync(source.parent)
            stamp = source.lstat()
            g.need(stat.S_ISDIR(stamp.st_mode) and stamp.st_uid == os.geteuid() and stat.S_IMODE(stamp.st_mode) == 0 and empty_mountpoint(source) and not mounted(source), 'mountpoint_changed')
            row['mountpointId'] = g.identity(source)[:2]
            row['phase'] = 'renamed'
            save(STATE, state)
        g.need(g.identity(old)[:2] == row['sourceId'], 'rollback_source_changed')
        if mounted(source):
            g.need(g.identity(source)[:2] == row['targetId'] and hashlib.sha256(unit.read_bytes()).hexdigest() == row['unitSha256'], 'foreign_bind')
            row['phase'] = 'bound'
            save(STATE, state)
            continue
        g.need(g.identity(source)[:2] == row['mountpointId'], 'mountpoint_changed')
        if os.path.lexists(unit):
            g.need(hashlib.sha256(unit.read_bytes()).hexdigest() == row['unitSha256'], 'unit_changed')
        else:
            fd = os.open(unit, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
            with os.fdopen(fd, 'wb') as stream:
                stream.write(g.unit_text(source, target, disk_unit))
                stream.flush()
                os.fsync(stream.fileno())
            sync(unit.parent)
        run(['/usr/bin/systemctl', 'daemon-reload'])
        run(['/usr/bin/systemctl', 'enable', name])
        run(['/usr/bin/systemctl', 'start', name])
        g.need(mounted(source) and g.identity(source)[:2] == row['targetId'], 'bind_failed')
        row['phase'] = 'bound'
        save(STATE, state)
    roots = {}
    for row in state['rows']:
        if row['purpose'] in g.TARGETS:
            roots[row['purpose']] = {key: row[key] for key in ['logical', 'target', 'uid']} | {'targetInode': row['targetId'][1]}
    config = g.validate({'contract': g.CONTRACT, 'version': 1, 'diskUuid': value['diskUuid'], 'roots': roots})
    quiescent(value)
    if os.path.lexists(g.CONFIG):
        g.need(g.private_json(g.CONFIG) == config, 'configuration_appeared')
    else:
        save(g.CONFIG, config)
    return finish_activation(value, state)


def final_configuration(value, state):
    roots = {}
    for row in state['rows']:
        if row['purpose'] in g.TARGETS:
            roots[row['purpose']] = {key: row[key] for key in ['logical', 'target', 'uid']} | {'targetInode': row['targetId'][1]}
    return g.validate({'contract': g.CONTRACT, 'version': 1, 'diskUuid': value['diskUuid'], 'roots': roots})


def finish_activation(value, state):
    config = final_configuration(value, state)
    g.need(g.private_json(g.CONFIG) == config, 'configuration_changed')
    device = g.disk_mount(config)
    for purpose in g.TARGETS:
        g.binding(config, purpose, device)
    # Persist the point after which the new tree is authoritative before
    # opening any startup/writer guard. A crash only permits forward finish.
    state['phase'] = 'activation-ready'
    save(STATE, state)
    clear_maintenance(value)
    g.guard(list(g.TARGETS), required=True)
    state['phase'] = 'complete'
    save(STATE, state)
    return {'phase': 'complete', 'roots': len(state['rows']), 'originalTreesRetained': True, 'applicationsStarted': False}


def rollback(value, state):
    g.need(state['phase'] in ['copying', 'copied', 'switching', 'rolled-back'], 'rollback_after_activation_forbidden')
    g.disk_mount(value)
    quiescent(value)
    # Reuse independently verified SHA only while every copied file's full
    # inode/size/mode/owner/mtime/ctime identity is unchanged.
    for row in state['rows']:
        if 'proof' in row:
            target_unchanged(row, state, allow_incomplete=True)
    marker_present = os.path.lexists(g.CONFIG)
    if marker_present:
        g.need(g.private_json(g.CONFIG) == final_configuration(value, state), 'configuration_changed')
    for row in reversed(state['rows']):
        if row['phase'] not in ['rename-intent', 'renamed', 'bound', 'restoring']:
            continue
        source, old = Path(row['logical']), Path(row['oldRoot'])
        if not old.exists():
            g.need(not mounted(source) and g.identity(source)[:2] == row['sourceId'], 'rollback_source_missing')
            row['phase'] = 'restored'
            save(STATE, state)
            continue
        g.need(g.identity(old)[:2] == row['sourceId'], 'rollback_source_changed')
        unit = UNIT_ROOT / row['unit']
        if unit.exists():
            g.need(hashlib.sha256(unit.read_bytes()).hexdigest() == row['unitSha256'], 'unit_changed')
            if mounted(source):
                g.need(g.identity(source)[:2] == row['targetId'], 'foreign_bind')
                run(['/usr/bin/systemctl', 'stop', row['unit']])
                g.need(not mounted(source), 'unmount_failed')
            run(['/usr/bin/systemctl', 'disable', row['unit']])
            g.need(hashlib.sha256(unit.read_bytes()).hexdigest() == row['unitSha256'], 'unit_changed')
            unit.unlink()
            sync(unit.parent)
            run(['/usr/bin/systemctl', 'daemon-reload'])
        else:
            g.need(not mounted(source), 'foreign_bind')
        if os.path.lexists(source):
            stamp = source.lstat()
            g.need(stat.S_ISDIR(stamp.st_mode) and stamp.st_uid == os.geteuid() and stat.S_IMODE(stamp.st_mode) == 0 and empty_mountpoint(source) and ('mountpointId' not in row or g.identity(source)[:2] == row['mountpointId']), 'mountpoint_changed')
            source.rmdir()
        row['phase'] = 'restoring'
        save(STATE, state)
        os.rename(old, source)
        sync(source.parent)
        g.need(g.identity(source)[:2] == row['sourceId'], 'restore_identity_changed')
        row['phase'] = 'restored'
        save(STATE, state)
    if marker_present:
        g.need(g.private_json(g.CONFIG) == final_configuration(value, state), 'configuration_changed')
        g.CONFIG.unlink()
        sync(g.CONFIG.parent)
    clear_maintenance(value)
    state['phase'] = 'rolled-back'
    save(STATE, state)
    return {'phase': 'rolled-back', 'sourcesRestored': True, 'targetsPreserved': True, 'applicationsStarted': False}


def maintenance_receipt(value):
    return {'contract': 'harbor-material-maintenance-v1', 'nonce': value['nonce'],
            'approvalSha256': hashlib.sha256(APPROVAL.read_bytes()).hexdigest()}


def clear_maintenance(value):
    if os.path.lexists(g.MAINTENANCE):
        g.need(g.private_json(g.MAINTENANCE) == maintenance_receipt(value), 'maintenance_changed')
        g.MAINTENANCE.unlink()
        sync(g.MAINTENANCE.parent)


def collect_plan():
    ticket = g.private_json(Path('/etc/harbor-postgres-runtime-upgrade.json'))
    materials = Path(ticket['materialsDirectory'])
    disk = json.loads(run(['/usr/bin/findmnt', '--json', '--mountpoint', str(g.DISK), '--output', 'TARGET,UUID']))['filesystems']
    g.need(len(disk) == 1 and disk[0]['target'] == str(g.DISK), 'disk_unavailable')
    value = {'contract': 'harbor-material-move-v1', 'approved': False, 'nonce': str(uuid.uuid4()),
             'diskUuid': disk[0]['uuid'], 'materialsDirectory': str(materials), 'sourceRoots': {},
             'legacyMaterials': [], 'legacyTemporary': [], 'runtimeRebuildableCaches': [], 'containers': {}, 'tools': {}, 'releaseRoots': {}, 'guards': {}}
    g.boot_unit(value)
    for purpose, target in g.TARGETS.items():
        logical = Path(g.FIXED_LOGICAL[purpose]) if purpose in g.FIXED_LOGICAL else materials / ('pg-data' if purpose == 'application-data' else 'runtime')
        source_id = g.identity(logical)[:2] if os.path.lexists(logical) else None
        value['sourceRoots'][purpose] = {'logical': str(logical), 'sourceId': source_id}
    for purpose in ['release-materials', 'release-tmp']:
        target = g.ROOT / g.TARGETS[purpose]
        g.directory(target)
        value['releaseRoots'][purpose] = g.identity(target)[:2]
    for name in APPS:
        cid = run(['/usr/bin/docker', '--host', 'unix:///run/docker.sock', 'inspect', '--format', '{{.Id}}', name]).decode().strip()
        g.need(re.fullmatch('[a-f0-9]{64}', cid), 'container_approval_invalid')
        value['containers'][name] = cid
    for root, pattern, key in [(g.STACK, r'release-materials-[0-9]+(?:-infrastructure)?', 'legacyMaterials'), (Path('/tmp'), r'harbor-release-[0-9]+-[A-Za-z0-9_-]+', 'legacyTemporary')]:
        for path in sorted(root.iterdir()):
            if not re.fullmatch(pattern, path.name):
                continue
            info = path.lstat()
            g.need(stat.S_ISDIR(info.st_mode) and (key == 'legacyTemporary' or info.st_uid == 0) and not info.st_mode & 0o022, 'legacy_root_untrusted')
            value[key].append({'logical': str(path), 'sourceId': [info.st_dev, info.st_ino], 'uid': info.st_uid})
    value['guards'] = {name: hashlib.sha256(guard_code(Path(name))).hexdigest() for name in GUARD_FILES}
    value['tools'] = {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest() for path in [Path(__file__), Path(g.__file__)]}
    return value


def main():
    g.need(os.geteuid() == 0 and len(sys.argv) == 2 and sys.argv[1] in ['plan', 'prepare', 'switch', 'rollback', 'status'], 'invocation_invalid')
    if sys.argv[1] == 'plan':
        return collect_plan()
    value, rows = approval()
    g.disk_mount(value)
    if sys.argv[1] == 'status':
        state = g.private_json(STATE, 33554432)
        g.need(state['nonce'] == value['nonce'] and state['contract'] == value['contract'], 'state_binding_changed')
        return {'phase': state['phase'], 'roots': len(state['rows']), 'readOnly': True}
    if sys.argv[1] == 'switch' and os.path.lexists(STATE):
        state = g.private_json(STATE, 33554432)
        g.need(state.get('nonce') == value['nonce'] and state.get('approvalSha256') == hashlib.sha256(APPROVAL.read_bytes()).hexdigest(), 'state_binding_changed')
        if state.get('phase') == 'complete':
            return switch(value, state)
        if state.get('phase') == 'activation-ready' and not os.path.lexists(g.MAINTENANCE):
            with locks(archive=False):
                state = g.private_json(STATE, 33554432)
                g.need(state.get('phase') == 'activation-ready' and state.get('nonce') == value['nonce'] and state.get('approvalSha256') == hashlib.sha256(APPROVAL.read_bytes()).hexdigest(), 'state_binding_changed')
                return finish_activation(value, state)
    with locks() as release_archive:
        quiescent(value)
        receipt = maintenance_receipt(value)
        if os.path.lexists(g.MAINTENANCE):
            g.need(g.private_json(g.MAINTENANCE) == receipt, 'maintenance_changed')
        elif sys.argv[1] == 'prepare':
            g.need(not os.path.lexists(STATE) and not os.path.lexists(g.CONFIG), 'already_initialized')
            save(g.MAINTENANCE, receipt)
        else:
            g.need(sys.argv[1] == 'switch' and g.private_json(STATE, 33554432).get('phase') in ['activation-ready', 'complete'], 'maintenance_missing')
        # Existing backup operations have drained under the exclusive lease.
        # The fixed /etc marker blocks their next operation; WAL archiving
        # keeps using the shared archive lock throughout the large copy.
        release_archive()
        if sys.argv[1] == 'prepare':
            return prepare(value, rows)
        state = g.private_json(STATE, 33554432)
        g.need(state['nonce'] == value['nonce'] and state['contract'] == value['contract'] and state['approvalSha256'] == hashlib.sha256(APPROVAL.read_bytes()).hexdigest(), 'state_binding_changed')
        if sys.argv[1] == 'rollback':
            return rollback(value, state)
        return switch(value, state)


if __name__ == '__main__':
    try:
        print(json.dumps(main()))
    except Exception as error:
        code = str(error) if isinstance(error, ValueError) and re.fullmatch('harbor_material_[a-z_]+', str(error)) else 'harbor_material_move_failed_closed'
        print(json.dumps({'ok': False, 'code': code, 'sourcesPreserved': True, 'applicationsStarted': False}))
        raise SystemExit(1)
