#!/usr/bin/env python3
"""Rebind retained, stopped infrastructure metadata; never start, migrate or activate."""
import base64
import contextlib
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

spec = importlib.util.spec_from_file_location('harbor_bootstrap', Path(__file__).with_name('bootstrap.py'))
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)
inventory_spec = importlib.util.spec_from_file_location('harbor_rebind_container_inventory', Path(__file__).with_name('rotate-tools.py'))
inventory = importlib.util.module_from_spec(inventory_spec)
inventory_spec.loader.exec_module(inventory)
APPROVAL = Path('/etc/harbor-postgres-rebind.json')
BUDGET = 131072
ROOT_UID = 0
PG_UID = 999


def require(condition, code):
    b.require(condition, 'bootstrap_rebind_' + code)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(raw):
    return base64.b64encode(raw).decode('ascii')


def decoded(value):
    require(isinstance(value, str), 'journal')
    return base64.b64decode(value, validate=True)


def body(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


def trusted_parents(path):
    for parent in Path(path).parents:
        b.protected(parent, directory=True)


def read_fd(fd):
    os.lseek(fd, 0, os.SEEK_SET)
    raw = os.read(fd, BUDGET + 1)
    require(len(raw) <= BUDGET, 'budget')
    return raw


def identity(info):
    return [info.st_dev, info.st_ino, info.st_uid, stat.S_IMODE(info.st_mode)]


def open_control(path, mode, writable=False):
    trusted_parents(path)
    fd = os.open(path, (os.O_RDWR if writable else os.O_RDONLY) | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == ROOT_UID and stat.S_IMODE(info.st_mode) == mode and info.st_nlink == 1, 'control_identity')
        require(identity(path.lstat()) == identity(info), 'control_identity')
        if writable:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError('bootstrap_rebind_busy')
        return fd
    except BaseException:
        os.close(fd)
        raise


def load_approval():
    require(os.geteuid() == 0, 'root_required')
    fd = open_control(APPROVAL, 0o600)
    try:
        raw = read_fd(fd)
    finally:
        os.close(fd)
    value = json.loads(raw)
    require(isinstance(value, dict) and set(value) == {'version', 'id', 'expected', 'source', 'nextBootstrap'}, 'approval')
    require(type(value['version']) is int and value['version'] == 1 and str(uuid.UUID(value['id'])) == value['id'], 'approval')
    expected = value['expected']
    require(isinstance(expected, dict) and set(expected) == {'bootstrapSha256', 'stateSha256', 'publisherSha256', 'owner', 'cluster'}, 'approval')
    require(all(isinstance(expected[k], str) and re.fullmatch('[a-f0-9]{64}', expected[k]) for k in ['bootstrapSha256', 'stateSha256', 'publisherSha256']), 'approval')
    require(re.fullmatch('[a-f0-9]{32}', expected.get('owner', '')) and re.fullmatch('[0-9]{1,20}', expected.get('cluster', '')), 'approval')
    source = value['source']
    require(isinstance(source, dict) and set(source) == {'containerId', 'startedAt', 'dataSource', 'runtimeSource'}, 'approval')
    require(re.fullmatch('[a-f0-9]{64}', source.get('containerId', '')) and isinstance(source['startedAt'], str) and 1 <= len(source['startedAt']) <= 128, 'approval')
    for key in ['dataSource', 'runtimeSource']:
        require(isinstance(source[key], str) and Path(source[key]).is_absolute() and str(Path(source[key]).resolve()) == source[key], 'approval')
    b.config_value(value['nextBootstrap'])
    require(value['nextBootstrap']['publisherDatabase'] is None, 'publisher_must_reset')
    return value, digest(raw)


def paths():
    return {'bootstrap': (b.CONFIG, 0o600), 'state': (b.ROOT / 'state.json', 0o600), 'publisher': (b.ROOT / 'control/publisher.json', 0o644)}


def cluster_identity():
    data = b.ROOT / 'db/18/docker'
    b.protected(data, PG_UID, 0o700, True)
    b.protected(data / 'global', PG_UID, 0o700, True)
    # The official entrypoint can create intermediate PGDATA parents as root.
    # Either privileged owner is valid; no symlink or group/world write is.
    for parent in [data.parent, data.parent.parent]:
        info = parent.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid in {ROOT_UID, PG_UID} and not info.st_mode & 0o022, 'cluster_material')
    values = []
    for path, size in [(data / 'PG_VERSION', 3), (data / 'global/pg_control', 8192)]:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            require(stat.S_ISREG(info.st_mode) and info.st_uid == PG_UID and stat.S_IMODE(info.st_mode) == 0o600 and info.st_nlink == 1 and info.st_size == size, 'cluster_material')
            raw = os.read(fd, size + 1)
            require(len(raw) == size, 'cluster_material')
            values.append(raw)
        finally:
            os.close(fd)
    require(values[0] == b'18\n' and not (data / 'postmaster.pid').exists() and not (data / 'postmaster.pid').is_symlink(), 'cluster_not_cold')
    return str(int.from_bytes(values[1][:8], sys.byteorder))


def observe(config, state, approval):
    """Positive Docker inventory plus exact retained owner and source generation."""
    source = b.inspect('container', b.PROJECT)
    observation = b.source_observation(approval['nextBootstrap'], source)
    require(observation['sourceRunning'] and source.get('Id') == approval['source']['containerId'] and source.get('State', {}).get('StartedAt') == approval['source']['startedAt'], 'source_generation')
    for destination, key in [('/app/data', 'dataSource'), ('/app/runtime', 'runtimeSource')]:
        mounts = [m for m in source.get('Mounts', []) if m.get('Destination') == destination]
        require(len(mounts) == 1 and mounts[0].get('Type') == 'bind' and mounts[0].get('Source') == approval['source'][key], 'source_mounts')
    infra = b.inspect('container', b.PROJECT + '-postgres-infra')
    if infra is not None:
        actual = infra.get('State', {})
        require(actual.get('Running') is False and actual.get('Restarting') is False and actual.get('Paused') is False and actual.get('Pid') == 0, 'infrastructure_running')
        labels = infra.get('Config', {}).get('Labels', {}) or {}
        require(labels.get('com.harbor.bootstrap-owner') == state['owner'] and labels.get('com.aistrong.project') == b.PROJECT + '-infra', 'infrastructure_owner')
        # Reuse the full existing mount/network/image contract for a cold instance.
        cold = {**infra, 'State': {**actual, 'Running': True}}
        b.verify_container(config, cold)
    try:
        containers = [(row, inventory.container_holds_resources(row)) for row in inventory.local_containers()]
    except Exception:
        raise ValueError('bootstrap_rebind_running_inventory') from None
    for container, held in containers:
        if not held:
            continue
        for mount in container.get('Mounts', []):
            require(type(mount.get('RW')) is bool, 'running_inventory')
            if not mount['RW']:
                continue
            origin = Path(mount.get('Source', '')).resolve()
            require(all(origin != protected and protected not in origin.parents and origin not in protected.parents for protected in [b.ROOT, b.CONFIG, b.ARCHIVE_PROFILE]), 'running_writer')
    require(cluster_identity() == approval['expected']['cluster'], 'cluster_mismatch')
    archive = json.loads(b.ARCHIVE_PROFILE.read_text())
    require(archive == b.archive_configuration(approval['expected']['cluster']), 'archive_cluster')


def material_witness():
    # Only identities and hashes stay in the root-only journal; secret bytes
    # never enter an output, snapshot, command argument or recovery approval.
    directories = ['', 'db', 'db/18', 'db/18/docker', 'db/18/docker/global', 'control', 'pg-tls', 'app-secrets', 'app-secrets/device', 'archive', 'image-volume']
    files = ['endpoint.json', 'control/pg_hba.conf', 'postgres-password', 'report-pg.key', 'app-secrets/postgres-report.key', 'pg-ca.key', 'archive.key', 'app-secrets/postgres-ca.crt', 'db/18/docker/PG_VERSION', 'db/18/docker/global/pg_control']
    files += ['pg-tls/' + name for name in ['ca.pem', 'server.key', 'server.pem']]
    files += ['app-secrets/device/' + name for name in ['ca.key', 'ca.pem', 'server.key', 'server.pem']]
    found = {}
    for name in directories:
        path = b.ROOT / name
        info = path.lstat()
        require(stat.S_ISDIR(info.st_mode), 'material_changed')
        found['directory:' + name] = identity(info)
    for name, path in [(name, b.ROOT / name) for name in files] + [('archive-profile', b.ARCHIVE_PROFILE)]:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(fd)
            require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= BUDGET and identity(info) == identity(path.lstat()), 'material_changed')
            found['file:' + name] = [identity(info), digest(read_fd(fd))]
        finally:
            os.close(fd)
    return found


def validate_change(old, state, publisher, approval):
    b.config_value(old)
    require(state.get('configuration') == b.fingerprint(old) and state.get('owner') == approval['expected']['owner'] and state.get('version') == 1, 'old_identity')
    require(isinstance(publisher, dict) and 'database' in publisher, 'publisher_changed')
    bound = b.config_value({**old, 'publisherDatabase': publisher['database']})
    require(publisher == b.publisher_configuration(bound), 'publisher_changed')
    next_config = approval['nextBootstrap']
    immutable = set(old) - {'businessImage', 'infrastructureImage', 'sourceImageId', 'publisherDatabase'}
    require(all(old[key] == next_config[key] for key in immutable), 'unsupported_change')
    require(any(old[key] != next_config[key] for key in ['businessImage', 'infrastructureImage', 'sourceImageId']) or old['publisherDatabase'] is not None, 'no_change')
    return {**state, 'configuration': b.fingerprint(next_config)}


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def save_journal(directory, value):
    pending = directory / '.journal.next'
    # Only this root-owned, fixed staging path may remain after a journal crash.
    if pending.exists() or pending.is_symlink():
        fd = open_control(pending, 0o600)
        os.close(fd)
        pending.unlink()
    b.write_new(pending, body(value))
    os.replace(pending, directory / 'journal.json')
    sync_directory(directory)


def ensure_directory(path):
    if not path.exists() and not path.is_symlink():
        path.mkdir(mode=0o700)
        os.chmod(path, 0o700)
        sync_directory(path.parent)
    b.protected(path, mode=0o700, directory=True)


def load_journal(directory):
    if not (directory / 'journal.json').exists() and not (directory / 'journal.json').is_symlink():
        return None
    fd = open_control(directory / 'journal.json', 0o600)
    try:
        return json.loads(read_fd(fd))
    finally:
        os.close(fd)


def check_entries(journal, opened):
    require(isinstance(journal, dict) and set(journal) == {'version', 'approvalDigest', 'direction', 'complete', 'entries', 'materials'} and journal['version'] == 1 and journal['direction'] in ['apply', 'rollback'] and type(journal['complete']) is bool, 'journal')
    require(set(journal['entries']) == set(paths()), 'journal')
    for name, entry in journal['entries'].items():
        require(set(entry) == {'identity', 'old', 'new', 'fromDigest', 'status'} and entry['status'] in ['pending', 'writing', 'complete'], 'journal')
        path, _ = paths()[name]
        fd = opened[name]
        require(identity(os.fstat(fd)) == entry['identity'] == identity(path.lstat()) and os.fstat(fd).st_nlink == 1, 'inode_changed')
        old, new = decoded(entry['old']), decoded(entry['new'])
        require(len(old) <= BUDGET and len(new) <= BUDGET and re.fullmatch('[a-f0-9]{64}', entry['fromDigest']), 'journal')
        current = read_fd(fd)
        if entry['status'] == 'pending':
            require(digest(current) == entry['fromDigest'], 'external_change')
        elif entry['status'] == 'complete':
            require(current == (new if journal['direction'] == 'apply' else old), 'external_change')


def write_control(fd, raw):
    os.lseek(fd, 0, os.SEEK_SET)
    remaining = memoryview(raw)
    while remaining:
        count = os.write(fd, remaining)
        require(count > 0, 'write_failed')
        remaining = remaining[count:]
    os.ftruncate(fd, len(raw))
    os.fsync(fd)
    require(read_fd(fd) == raw, 'readback')


def run(action, *, after_write=None):
    require(action in ['check', 'apply', 'rollback'], 'arguments')
    approval, approval_digest = load_approval()
    b.protected(b.ROOT, mode=0o711, directory=True)
    root_fd = os.open(b.ROOT, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    with contextlib.ExitStack() as stack:
        stack.callback(os.close, root_fd)
        try:
            fcntl.flock(root_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('bootstrap_rebind_busy')
        opened = {}
        for name, (path, mode) in paths().items():
            fd = open_control(path, mode, writable=True)
            stack.callback(os.close, fd)
            opened[name] = fd
        directory = b.ROOT / 'rebind' / approval['id']
        journal = None
        if directory.exists() or directory.is_symlink():
            b.protected(directory.parent, mode=0o700, directory=True)
            b.protected(directory, mode=0o700, directory=True)
            require(set(path.name for path in directory.iterdir()) <= {'journal.json', '.journal.next'}, 'journal_directory')
            journal = load_journal(directory)
        if journal is None:
            require(action != 'rollback', 'journal_missing')
            raw = {name: read_fd(fd) for name, fd in opened.items()}
            require(all(digest(raw[name]) == approval['expected'][name + 'Sha256'] for name in raw), 'expected_digest')
            old, state, publisher = [json.loads(raw[name]) for name in ['bootstrap', 'state', 'publisher']]
            next_state = validate_change(old, state, publisher, approval)
            b.verify_materials(old)
            replacements = {'bootstrap': approval['nextBootstrap'], 'state': next_state, 'publisher': b.publisher_configuration(approval['nextBootstrap'])}
            journal = {'version': 1, 'approvalDigest': approval_digest, 'direction': 'apply', 'complete': False, 'materials': material_witness(), 'entries': {name: {'identity': identity(os.fstat(opened[name])), 'old': encoded(raw[name]), 'new': encoded(body(replacements[name])), 'fromDigest': digest(raw[name]), 'status': 'pending'} for name in raw}}
        else:
            require(journal.get('approvalDigest') == approval_digest, 'approval_changed')
            check_entries(journal, opened)
            old = json.loads(decoded(journal['entries']['bootstrap']['old']))
            state = json.loads(decoded(journal['entries']['state']['old']))
            publisher = json.loads(decoded(journal['entries']['publisher']['old']))
            next_state = validate_change(old, state, publisher, approval)
            expected_new = {'bootstrap': approval['nextBootstrap'], 'state': next_state, 'publisher': b.publisher_configuration(approval['nextBootstrap'])}
            require(all(digest(decoded(journal['entries'][name]['old'])) == approval['expected'][name + 'Sha256'] and decoded(journal['entries'][name]['new']) == body(expected_new[name]) for name in paths()), 'journal_binding')
        require(material_witness() == journal['materials'], 'material_changed')
        # Validate both immutable image references before any metadata write.
        b.image_checks(approval['nextBootstrap'])
        observe(old, state, approval)
        if action == 'check':
            return {'scope': 'retained-postgres-bootstrap-rebind', 'action': action, 'ready': True, 'metadataWritten': False, 'deploymentAllowed': False}
        ensure_directory(directory.parent)
        ensure_directory(directory)
        if action == 'rollback' and journal['direction'] != 'rollback':
            check_entries(journal, opened)
            journal['direction'] = 'rollback'
            journal['complete'] = False
            for name, entry in journal['entries'].items():
                entry['fromDigest'] = digest(read_fd(opened[name]))
                entry['status'] = 'pending'
        require(journal['direction'] == action, 'direction_conflict')
        save_journal(directory, journal)
        for name, entry in journal['entries'].items():
            check_entries(journal, opened)
            require(material_witness() == journal['materials'], 'material_changed')
            observe(old, state, approval)
            if entry['status'] == 'complete':
                continue
            entry['status'] = 'writing'
            save_journal(directory, journal)
            write_control(opened[name], decoded(entry['new' if action == 'apply' else 'old']))
            if after_write:
                after_write(name)
            entry['status'] = 'complete'
            save_journal(directory, journal)
        check_entries(journal, opened)
        observe(old, state, approval)
        require(material_witness() == journal['materials'], 'material_changed')
        b.verify_materials(approval['nextBootstrap'] if action == 'apply' else old)
        journal['complete'] = True
        save_journal(directory, journal)
        return {'scope': 'retained-postgres-bootstrap-rebind', 'action': action, 'ready': True, 'metadataWritten': True, 'publisherReset': action == 'apply', 'snapshotsPreserved': True, 'clusterPreserved': True, 'sourceModified': False, 'deploymentAllowed': False}


if __name__ == '__main__':
    try:
        require(len(sys.argv) == 2, 'arguments')
        print(json.dumps(run(sys.argv[1])))
    except Exception as error:
        code = str(error) if isinstance(error, ValueError) and re.fullmatch('bootstrap_[a-z_]{1,80}', str(error)) else 'bootstrap_rebind_unavailable'
        print(json.dumps({'error': code, 'deploymentAllowed': False}), file=sys.stderr)
        sys.exit(1)
