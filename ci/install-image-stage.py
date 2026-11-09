#!/usr/bin/env python3
"""Root-only host update; keeps the existing release/activation history intact."""
import argparse
import copy
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import tempfile

WRAPPER = Path('/usr/local/bin/harbor-gateway-deploy')
HELPER = Path('/usr/local/lib/harbor/release-image.py')
MANIFEST = Path('/etc/harbor-postgres-runtime-tools.json')
TICKET = Path('/etc/harbor-postgres-runtime-upgrade.json')
UPGRADE = Path('/usr/local/lib/harbor/postgres-upgrade-deploy.py')
ENTRY = Path('/usr/local/lib/harbor/gateway-entry-controller.py')
HISTORY = Path('/var/lib/harbor-host-tool-installs')
LOCKS = ['/run/harbor-gateway-deploy.lock', '/run/harbor-entry/controller-state.lock']
LIMIT = 1024**2


def need(ok, code):
    if not ok:
        raise ValueError('image_stage_install_' + code)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def body(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


def parents(path):
    for p in Path(path).parents:
        s = p.lstat()
        need(stat.S_ISDIR(s.st_mode) and s.st_uid == 0 and not s.st_mode & 0o022, 'parent_trust')


def read(path, mode=None):
    parents(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        s = os.fstat(stream.fileno())
        need(stat.S_ISREG(s.st_mode) and s.st_uid == 0 and s.st_nlink == 1 and not s.st_mode & 0o022 and s.st_size <= LIMIT and (mode is None or stat.S_IMODE(s.st_mode) == mode), 'file_trust')
        raw = stream.read(LIMIT + 1)
        need(len(raw) == s.st_size, 'file_changed')
        return raw


def optional(path):
    return read(path) if os.path.lexists(path) else None


def write(path, raw, mode):
    parents(path)
    fd, name = tempfile.mkstemp(prefix='.image-stage-install-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_DIRECTORY | os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(name).unlink(missing_ok=True)


def module(path, name):
    read(path)
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def transition(old_manifest, old_ticket, files, wrapper_bytes, revision):
    """Keep both historical approval contents and the current deployment target."""
    need(re.fullmatch('[a-f0-9]{40}', revision) is not None, 'revision')
    need(old_manifest.get('contract') == 'harbor-postgres-runtime-tools-v1' and old_manifest.get('revision') == old_ticket.get('hostToolsRevision'), 'old_binding')
    need(set(old_manifest.get('files', {})) == set(files) and all(sha(raw) == old_manifest['files'][path] for path, raw in files.items()), 'old_bytes')
    need(str(WRAPPER) in files, 'wrapper_binding')
    compile(wrapper_bytes, str(WRAPPER), 'exec')
    manifest, ticket = copy.deepcopy(old_manifest), copy.deepcopy(old_ticket)
    manifest['revision'] = revision
    manifest['files'][str(WRAPPER)] = sha(wrapper_bytes)
    ticket['hostToolsRevision'] = revision
    return manifest, ticket


def current_binding(upgrade, entry, ticket):
    upgrade.verify_installed_tools(ticket)
    need(entry.entry_state().get('phase') == 'active', 'entry_not_active')
    active, _, _ = entry.active_record()
    record = upgrade.record_for(active.get('approval', {}))
    need(record.get('phase') == 'complete' and active.get('image') == ticket.get('to', {}).get('image') and active.get('approval', {}).get('deploymentId') == ticket.get('deploymentId'), 'current_release')
    same_release = lambda value: {key: item for key, item in value.items() if key != 'hostToolsRevision'}
    need(same_release(record['ticket']) == same_release(ticket), 'current_ticket')
    # Use the actual startup authorization path; a changed fixed ticket must not
    # invalidate the old durable release/approval that the current entry uses.
    before = entry.read_active_authorization(active['image'])
    need(before.get('state') == 'active' and upgrade.approval_binding(before) == active['approval'], 'current_authorization')
    return upgrade.active_identity(active), before


def install(source, revision, expected_manifest, expected_ticket, expected_wrapper, expected_helper):
    need(os.geteuid() == 0, 'root_required')
    for digest in [expected_manifest, expected_ticket, expected_wrapper, expected_helper]:
        need(re.fullmatch('[a-f0-9]{64}', digest) is not None, 'expected_digest')
    wrapper, helper = read(source / 'deploy-wrapper.py'), read(source / 'release-image.py')
    need(sha(wrapper) == expected_wrapper and sha(helper) == expected_helper, 'source_digest')
    compile(helper, str(HELPER), 'exec')
    upgrade, entry = module(UPGRADE, 'image_stage_install_upgrade'), module(ENTRY, 'image_stage_install_entry')
    fds = []
    try:
        for path in LOCKS:
            fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
            fds.append(fd)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        destination = HISTORY / ('image-stage-' + revision)
        prior = json.loads(read(destination / 'journal.json', 0o600)) if (destination / 'journal.json').exists() else None
        if prior is not None:
            need(prior.get('version') == 1 and prior.get('revision') == revision and set(prior.get('files', {})) == {str(WRAPPER), str(HELPER), str(MANIFEST), str(TICKET)}, 'existing_record')
            need(prior['files'][str(MANIFEST)]['before'] == expected_manifest and prior['files'][str(TICKET)]['before'] == expected_ticket and prior['files'][str(WRAPPER)]['after'] == expected_wrapper and prior['files'][str(HELPER)]['after'] == expected_helper, 'existing_source')
            if prior.get('state') == 'complete':
                for name, row in prior['files'].items():
                    need(sha(read(Path(name))) == row['after'], 'completed_bytes')
                current = json.loads(read(TICKET, 0o600))
                actual, _ = current_binding(upgrade, entry, current)
                need(actual == prior['active'], 'completed_active')
                return {'installed': True, 'reused': True, 'revision': revision, 'activeApprovalPreserved': True, 'businessContainersTouched': False, 'rootStagingSwitchChanged': False, 'backupDirectory': str(destination)}
            need(prior.get('state') in ['prepared', 'rolled-back'], 'existing_state')
        before_manifest, before_ticket = read(MANIFEST, 0o600), read(TICKET, 0o600)
        need(sha(before_manifest) == expected_manifest and sha(before_ticket) == expected_ticket, 'expected_current')
        old_manifest, old_ticket = json.loads(before_manifest), json.loads(before_ticket)
        active, authorization = current_binding(upgrade, entry, old_ticket)
        files = {path: read(Path(path)) for path in upgrade.HOST_FILES}
        next_manifest, next_ticket = transition(old_manifest, old_ticket, files, wrapper, revision)
        need(str(WRAPPER) in files and set(next_manifest['files']) == set(upgrade.HOST_FILES), 'fixed_tools')
        parents(HISTORY)
        HISTORY.mkdir(mode=0o700, exist_ok=True)
        parents(destination)
        destination.mkdir(mode=0o700, exist_ok=True)
        info = destination.lstat();need(stat.S_ISDIR(info.st_mode) and not info.st_mode & 0o077, 'history_directory')
        before = {WRAPPER: files[str(WRAPPER)], MANIFEST: before_manifest, TICKET: before_ticket, HELPER: optional(HELPER)}
        after = {HELPER: helper, WRAPPER: wrapper, MANIFEST: body(next_manifest), TICKET: body(next_ticket)}
        modes_before = {path: stat.S_IMODE(path.lstat().st_mode) if raw is not None else None for path, raw in before.items()}
        modes_after = {path: 0o755 if path == WRAPPER else 0o644 if path == HELPER else 0o600 for path in after}
        journal = {'version': 1, 'revision': revision, 'state': 'prepared', 'active': active, 'attempts': (prior.get('attempts', 1) + 1) if prior else 1, 'files': {str(path): {'before': None if raw is None else sha(raw), 'after': sha(after[path]), 'modeBefore': modes_before[path], 'modeAfter': modes_after[path]} for path, raw in before.items()}}
        allowed = {'journal.json', *(str(index) + '.before' for index, raw in enumerate(before.values()) if raw is not None)}
        need(set(p.name for p in destination.iterdir()) <= allowed, 'history_contents')
        for index, (path, raw) in enumerate(before.items()):
            journal['files'][str(path)]['backup'] = str(index) + '.before'
            if raw is not None:
                backup = destination / (str(index) + '.before')
                if os.path.lexists(backup):
                    need(read(backup, 0o600) == raw, 'existing_backup')
                else:
                    write(backup, raw, 0o600)
        if prior is not None:
            need(prior['files'] == journal['files'] and prior['active'] == active, 'retry_binding')
        write(destination / 'journal.json', body(journal), 0o600)
        try:
            for path, raw in after.items():
                need(optional(path) == before[path], 'changed_before_install')
                write(path, raw, modes_after[path])
            upgrade.verify_installed_tools(next_ticket)
            now, status = current_binding(upgrade, entry, next_ticket)
            need(now == active and status == authorization, 'active_changed')
            journal['state'] = 'complete'
            write(destination / 'journal.json', body(journal), 0o600)
        except BaseException:
            for path in reversed(after):
                present = optional(path)
                if present == before[path]:
                    continue
                need(present == after[path], 'rollback_changed')
                if before[path] is None:
                    path.unlink()
                else:
                    write(path, before[path], modes_before[path])
            upgrade.verify_installed_tools(old_ticket)
            journal['state'] = 'rolled-back'
            write(destination / 'journal.json', body(journal), 0o600)
            raise
        return {'installed': True, 'revision': revision, 'activeApprovalPreserved': True, 'businessContainersTouched': False, 'rootStagingSwitchChanged': False, 'backupDirectory': str(destination)}
    finally:
        for fd in reversed(fds):
            os.close(fd)


def rollback(revision):
    need(os.geteuid() == 0 and re.fullmatch('[a-f0-9]{40}', revision) is not None, 'root_required')
    directory = HISTORY / ('image-stage-' + revision)
    journal = json.loads(read(directory / 'journal.json', 0o600))
    need(journal.get('version') == 1 and journal.get('revision') == revision and journal.get('state') in ['prepared', 'complete', 'rolled-back'], 'rollback_record')
    need(set(journal.get('files', {})) == {str(WRAPPER), str(HELPER), str(MANIFEST), str(TICKET)}, 'rollback_files')
    upgrade, entry = module(UPGRADE, 'image_stage_rollback_upgrade'), module(ENTRY, 'image_stage_rollback_entry')
    fds = []
    try:
        for name in LOCKS:
            fd = os.open(name, os.O_RDWR | os.O_NOFOLLOW);fds.append(fd);fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        need(entry.entry_state().get('phase') == 'active', 'entry_not_active')
        active, _, _ = entry.active_record()
        need(upgrade.active_identity(active) == journal['active'], 'rollback_active_changed')
        originals = {}
        for name, row in journal['files'].items():
            path = Path(name)
            present = optional(path)
            need((None if present is None else sha(present)) in [row['before'], row['after']], 'rollback_changed')
            need(type(row.get('modeAfter')) is int and row['modeAfter'] in [0o600, 0o644, 0o755] and (row['modeBefore'] is None if row['before'] is None else type(row['modeBefore']) is int and 0 <= row['modeBefore'] <= 0o777 and not row['modeBefore'] & 0o022), 'rollback_modes')
            backup = None if row['before'] is None else read(directory / row['backup'], 0o600)
            need(backup is None or sha(backup) == row['before'], 'rollback_backup')
            originals[path] = backup
        for path, raw in originals.items():
            if raw is None:
                path.unlink(missing_ok=True)
            else:
                write(path, raw, journal['files'][str(path)]['modeBefore'])
        old_ticket = json.loads(originals[TICKET]);upgrade.verify_installed_tools(old_ticket)
        current_binding(upgrade, entry, old_ticket)
        journal['state'] = 'rolled-back';write(directory / 'journal.json', body(journal), 0o600)
        return {'rolledBack': True, 'revision': revision, 'activeApprovalPreserved': True, 'businessContainersTouched': False}
    finally:
        for fd in reversed(fds):
            os.close(fd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['install', 'rollback'])
    parser.add_argument('--revision', required=True)
    parser.add_argument('--source', type=Path)
    for name in ['manifest', 'ticket', 'wrapper', 'helper']:
        parser.add_argument('--expected-' + name + '-sha256')
    arguments = parser.parse_args()
    try:
        if arguments.action == 'rollback':
            result = rollback(arguments.revision)
        else:
            need(arguments.source is not None, 'source')
            result = install(arguments.source, arguments.revision, arguments.expected_manifest_sha256, arguments.expected_ticket_sha256, arguments.expected_wrapper_sha256, arguments.expected_helper_sha256)
        print(json.dumps(result))
    except BaseException:
        raise SystemExit('image_stage_install_failed_retained')
