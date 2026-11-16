#!/usr/bin/env python3
"""Root-only host update; keeps the existing release/activation history intact."""
import sys
sys.dont_write_bytecode = True
import argparse
import copy
import fcntl
import hashlib
from types import ModuleType
import json
import os
from pathlib import Path
import re
import stat
import tempfile

WRAPPER = Path('/usr/local/bin/harbor-gateway-deploy')
COMPOSE = Path('/usr/local/lib/harbor/postgres-deploy.py')
HELPER = Path('/usr/local/lib/harbor/release-image.py')
MANIFEST = Path('/etc/harbor-postgres-runtime-tools.json')
TICKET = Path('/etc/harbor-postgres-runtime-upgrade.json')
UPGRADE = Path('/usr/local/lib/harbor/postgres-upgrade-deploy.py')
ENTRY = Path('/usr/local/lib/harbor/gateway-entry-controller.py')
CONFIG = Path('/etc/harbor-gateway-deploy.json')
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


def module_bytes(raw, path, name):
    value = ModuleType(name)
    value.__file__ = str(path)
    exec(compile(raw, str(path), 'exec'), value.__dict__)
    return value


def module(path, name):
    # dont_write_bytecode does not prevent consuming stale timestamp .pyc files.
    return module_bytes(read(path), path, name)


def runtime_modules(prefix):
    module(WRAPPER, prefix + '_wrapper')
    module(COMPOSE, prefix + '_compose')
    return module(UPGRADE, prefix + '_upgrade')


def transition(old_manifest, old_ticket, files, wrapper_bytes, revision, replacements=None):
    """Keep both historical approval contents and the current deployment target."""
    need(re.fullmatch('[a-f0-9]{40}', revision) is not None, 'revision')
    need(old_manifest.get('contract') == 'harbor-postgres-runtime-tools-v1' and old_manifest.get('revision') == old_ticket.get('hostToolsRevision'), 'old_binding')
    need(set(old_manifest.get('files', {})) == set(files) and all(sha(raw) == old_manifest['files'][path] for path, raw in files.items()), 'old_bytes')
    need(str(WRAPPER) in files, 'wrapper_binding')
    replacements = {str(WRAPPER): wrapper_bytes} if replacements is None else replacements
    need(set(replacements) <= set(files), 'replacement_scope')
    for path, raw in replacements.items():
        compile(raw, path, 'exec')
    manifest, ticket = copy.deepcopy(old_manifest), copy.deepcopy(old_ticket)
    manifest['revision'] = revision
    for path, raw in replacements.items():
        manifest['files'][path] = sha(raw)
    ticket['hostToolsRevision'] = revision
    return manifest, ticket


def readonly_active_record(upgrade, entry):
    """Validate the existing PG binding without active_record's write refresh.

    Keep compatibility with already-installed controllers: runtime installation
    replaces only WRAPPER/COMPOSE/UPGRADE, not the entry controller API.
    Runtime PID/IP observations are never published by this read-only path.
    """
    active = entry.private_json(entry.ACTIVE)
    config = entry.validate(entry.private_json(entry.PROFILE))
    entry.verify_candidate_bridge(active.get('networkSnapshot', {}))
    need(active.get('version') == 1 and active.get('configDigest') == entry.fingerprint(config), 'qualification_active_configuration')
    need(active.get('approval', {}).get('contract') == upgrade.CONTRACT, 'qualification_active_contract')
    marker = upgrade.verified_marker_path(active['image'], active)
    need(active.get('storageMarkerDigest') == entry.fingerprint(entry.private_json(marker)), 'qualification_active_marker')
    temporary = object.__new__(entry.EntryDeployment)
    temporary.image, temporary.approval = active['image'], active['approval']
    row = entry.inspect_container(active['candidate']['containerId'])
    temporary.verify_candidate_row(row)
    need(row['Image'] == active['candidate']['imageId'] and entry.candidate_mounts(row, active['approval']) == active['mounts'], 'qualification_active_materials')
    address = temporary.endpoint(row)
    profile = entry.private_json(entry.CANDIDATE_PROFILE)
    need(profile.get('socketPath') == '/run/harbor-entry/candidate.sock', 'qualification_active_slot')
    need(active['candidate'].get('address') == address and profile.get('sourceAddress') == address and profile.get('sourcePort') == active['candidate']['port'], 'qualification_active_address_changed')
    return active


def current_binding(upgrade, entry, ticket, *, readonly=False):
    upgrade.verify_installed_tools(ticket)
    need(entry.entry_state().get('phase') == 'active', 'entry_not_active')
    active = readonly_active_record(upgrade, entry) if readonly else entry.active_record()[0]
    record = upgrade.record_for(active.get('approval', {}))
    need(record.get('phase') == 'complete' and active.get('image') == ticket.get('to', {}).get('image') and active.get('approval', {}).get('deploymentId') == ticket.get('deploymentId'), 'current_release')
    same_release = lambda value: {key: item for key, item in value.items() if key != 'hostToolsRevision'}
    need(same_release(record['ticket']) == same_release(ticket), 'current_ticket')
    # Use the actual startup authorization path; a changed fixed ticket must not
    # invalidate the old durable release/approval that the current entry uses.
    before = entry.read_active_authorization(active['image'])
    need(before.get('state') == 'active' and upgrade.approval_binding(before) == active['approval'], 'current_authorization')
    return upgrade.active_identity(active), before


def install(source, revision, expected_manifest, expected_ticket, expected_wrapper, expected_helper, *, runtime=False, expected_compose=None, expected_upgrade=None):
    need(os.geteuid() == 0, 'root_required')
    need(type(runtime) is bool, 'mode')
    need(isinstance(revision, str) and re.fullmatch('[a-f0-9]{40}', revision) is not None, 'revision')
    for digest in [expected_manifest, expected_ticket, expected_wrapper, expected_helper] + ([expected_compose, expected_upgrade] if runtime else []):
        need(re.fullmatch('[a-f0-9]{64}', digest) is not None, 'expected_digest')
    wrapper = read(source / 'deploy-wrapper.py')
    helper = read(HELPER) if runtime else read(source / 'release-image.py')
    replacements = {str(WRAPPER): wrapper}
    if runtime:
        compose, next_upgrade = read(source / 'postgres-deploy.py'), read(source / 'postgres-upgrade-deploy.py')
        need(sha(compose) == expected_compose and sha(next_upgrade) == expected_upgrade, 'runtime_source_digest')
        replacements.update({str(COMPOSE): compose, str(UPGRADE): next_upgrade})
    need(sha(wrapper) == expected_wrapper and sha(helper) == expected_helper, 'source_digest')
    compile(helper, str(HELPER), 'exec')
    upgrade, entry = module(UPGRADE, 'image_stage_install_upgrade'), module(ENTRY, 'image_stage_install_entry')
    if runtime:
        # Bind indirect ACTIVE/authorization checks to the same verified source
        # module; ENTRY's ordinary importer may still find an old timestamp pyc.
        entry.runtime_upgrade_module = lambda: upgrade
    fds = []
    try:
        for path in LOCKS:
            if runtime:
                read(Path(path))
            fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
            fds.append(fd)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        destination = HISTORY / (('runtime-tools-' if runtime else 'image-stage-') + revision)
        scope = {str(WRAPPER), str(HELPER), str(MANIFEST), str(TICKET)} | ({str(COMPOSE), str(UPGRADE)} if runtime else set())
        prior = json.loads(read(destination / 'journal.json', 0o600)) if (destination / 'journal.json').exists() else None
        if prior is not None:
            need(prior.get('version') == 1 and prior.get('revision') == revision and set(prior.get('files', {})) == scope, 'existing_record')
            need(prior['files'][str(MANIFEST)]['before'] == expected_manifest and prior['files'][str(TICKET)]['before'] == expected_ticket and prior['files'][str(WRAPPER)]['after'] == expected_wrapper and prior['files'][str(HELPER)]['after'] == expected_helper, 'existing_source')
            if runtime:
                need(prior['files'][str(COMPOSE)]['after'] == expected_compose and prior['files'][str(UPGRADE)]['after'] == expected_upgrade and prior['files'][str(HELPER)]['before'] == expected_helper, 'existing_runtime_source')
            if prior.get('state') == 'complete':
                for name, row in prior['files'].items():
                    need(sha(read(Path(name))) == row['after'], 'completed_bytes')
                current = json.loads(read(TICKET, 0o600))
                actual, _ = current_binding(upgrade, entry, current)
                need(actual == prior['active'], 'completed_active')
                return {'installed': True, 'reused': True, 'revision': revision, 'activeApprovalPreserved': True, 'businessContainersTouched': False, 'rootStagingSwitchChanged': False, 'hostToolsReady': False, 'qualificationRequired': True, 'backupDirectory': str(destination)}
            need(prior.get('state') in ['prepared', 'rolled-back'], 'existing_state')
        before_manifest, before_ticket = read(MANIFEST, 0o600), read(TICKET, 0o600)
        need(sha(before_manifest) == expected_manifest and sha(before_ticket) == expected_ticket, 'expected_current')
        old_manifest, old_ticket = json.loads(before_manifest), json.loads(before_ticket)
        active, authorization = current_binding(upgrade, entry, old_ticket)
        files = {path: read(Path(path)) for path in upgrade.HOST_FILES}
        if runtime:
            need(len(files) == 7 and set(replacements) == {str(WRAPPER), str(COMPOSE), str(UPGRADE)} and set(replacements) <= set(files), 'runtime_fixed_tools')
        next_manifest, next_ticket = transition(old_manifest, old_ticket, files, wrapper, revision, replacements)
        need(str(WRAPPER) in files and set(next_manifest['files']) == set(upgrade.HOST_FILES), 'fixed_tools')
        parents(HISTORY)
        HISTORY.mkdir(mode=0o700, exist_ok=True)
        parents(destination)
        destination.mkdir(mode=0o700, exist_ok=True)
        parents(destination / 'journal.json')
        info = destination.lstat();need(stat.S_ISDIR(info.st_mode) and not info.st_mode & 0o077, 'history_directory')
        before = {WRAPPER: files[str(WRAPPER)], MANIFEST: before_manifest, TICKET: before_ticket, HELPER: optional(HELPER)}
        after = {HELPER: helper, WRAPPER: wrapper, MANIFEST: body(next_manifest), TICKET: body(next_ticket)}
        if runtime:
            before.update({COMPOSE: files[str(COMPOSE)], UPGRADE: files[str(UPGRADE)]})
            after = {COMPOSE: compose, UPGRADE: next_upgrade, **after}
        modes_before = {path: stat.S_IMODE(path.lstat().st_mode) if raw is not None else None for path, raw in before.items()}
        modes_after = {path: 0o755 if path == WRAPPER else modes_before[path] if runtime and path == HELPER else 0o644 if path in [HELPER, COMPOSE, UPGRADE] else 0o600 for path in after}
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
                if raw != before[path]:
                    write(path, raw, modes_after[path])
            if runtime:
                upgrade = runtime_modules('runtime_tools_install_verified')
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
            if runtime:
                upgrade = runtime_modules('runtime_tools_install_restored')
            upgrade.verify_installed_tools(old_ticket)
            journal['state'] = 'rolled-back'
            write(destination / 'journal.json', body(journal), 0o600)
            raise
        return {'installed': True, 'revision': revision, 'activeApprovalPreserved': True, 'businessContainersTouched': False, 'rootStagingSwitchChanged': False, 'hostToolsReady': False, 'qualificationRequired': True, 'backupDirectory': str(destination)}
    finally:
        for fd in reversed(fds):
            os.close(fd)


def qualification_bytes_match(path, raw, row, side, directory):
    expected = row[side]
    if (None if raw is None else sha(raw)) == expected:
        return True
    if path not in [TICKET, MANIFEST] or raw is None or expected is None:
        return False
    # The fixed recovery writer may re-encode the same authenticated source
    # ticket. Accept its value, never altered approvals or executable bytes.
    if side == 'after':
        return sha(body(json.loads(raw))) == expected
    need(re.fullmatch('[0-9]+\.before', row.get('backup', '')) is not None, 'qualification_backup_path')
    previous = read(directory / row['backup'], 0o600)
    need(sha(previous) == expected, 'qualification_backup')
    return body(json.loads(raw)) == body(json.loads(previous))


def qualify(revision, *, runtime=False):
    """Read-only Host qualification after an explicit supervisor refresh.

    The refresh helper needs these same locks during its controlled stop/start,
    so it is never invoked from inside the file installation transaction.
    A successful result is not an image approval or a deployment action.
    """
    need(os.geteuid() == 0 and re.fullmatch('[a-f0-9]{40}', revision) is not None, 'root_required')
    need(type(runtime) is bool, 'mode')
    fds = []
    try:
        for name in LOCKS:
            read(Path(name))
            fd = os.open(name, os.O_RDWR | os.O_NOFOLLOW);fds.append(fd)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        directory = HISTORY / (('runtime-tools-' if runtime else 'image-stage-') + revision)
        journal = json.loads(read(directory / 'journal.json', 0o600))
        scope = {str(WRAPPER), str(HELPER), str(MANIFEST), str(TICKET)} | ({str(COMPOSE), str(UPGRADE)} if runtime else set())
        need(journal.get('version') == 1 and journal.get('revision') == revision and journal.get('state') in ['complete', 'rolled-back'] and set(journal.get('files', {})) == scope, 'qualification_record')
        side = 'after' if journal['state'] == 'complete' else 'before'
        observed, observed_modes = {}, {}
        for name, row in journal['files'].items():
            actual = optional(Path(name))
            need(qualification_bytes_match(Path(name), actual, row, side, directory), 'qualification_bytes')
            observed[name] = actual
            mode = None if actual is None else stat.S_IMODE(Path(name).lstat().st_mode)
            expected_mode = row['modeAfter' if side == 'after' else 'modeBefore']
            need(mode == expected_mode and (mode is None or type(expected_mode) is int), 'qualification_mode')
            if Path(name) == WRAPPER:need(mode is not None and mode & 0o111, 'qualification_wrapper_not_executable')
            observed_modes[name] = mode
        upgrade, entry = module(UPGRADE, 'host_qualification_upgrade'), module(ENTRY, 'host_qualification_entry')
        entry.runtime_upgrade_module = lambda: upgrade
        ticket = json.loads(read(TICKET, 0o600))
        active, _ = current_binding(upgrade, entry, ticket, readonly=True)
        need(active == journal['active'], 'qualification_active_changed')
        config_raw = read(CONFIG, 0o600)
        config = json.loads(config_raw)
        compose = module(COMPOSE, 'host_qualification_composition')
        policy = compose.storage_restart_policy(config)
        need(policy in ['no', 'unless-stopped'], 'qualification_restart_policy')
        # A guard check may perform slow Docker/systemd reads. Neither source
        # approval nor installed files may change while it is being observed.
        final_active, _ = current_binding(upgrade, entry, ticket, readonly=True)
        need(final_active == active and read(CONFIG, 0o600) == config_raw, 'qualification_changed')
        for name, row in journal['files'].items():
            actual = optional(Path(name))
            need(actual == observed[name], 'qualification_bytes_changed')
            mode = None if actual is None else stat.S_IMODE(Path(name).lstat().st_mode)
            need(mode == observed_modes[name], 'qualification_mode_changed')
        return {'hostToolsReady': True, 'qualificationRequired': False, 'revision': ticket['hostToolsRevision'], 'installationRevision': revision, 'storageStartupGuardEnabled': config.get('postgres_storage_startup_guard', False), 'businessContainersTouched': False, 'publicationTriggered': False}
    finally:
        for fd in reversed(fds):
            os.close(fd)


def rollback(revision, *, runtime=False):
    need(os.geteuid() == 0 and re.fullmatch('[a-f0-9]{40}', revision) is not None, 'root_required')
    need(type(runtime) is bool, 'mode')
    directory = HISTORY / (('runtime-tools-' if runtime else 'image-stage-') + revision)
    journal = json.loads(read(directory / 'journal.json', 0o600))
    need(journal.get('version') == 1 and journal.get('revision') == revision and journal.get('state') in ['prepared', 'complete', 'rolled-back'], 'rollback_record')
    need(set(journal.get('files', {})) == ({str(WRAPPER), str(HELPER), str(MANIFEST), str(TICKET)} | ({str(COMPOSE), str(UPGRADE)} if runtime else set())), 'rollback_files')
    # Runtime recovery must not execute the possibly broken new UPGRADE.
    entry = module(ENTRY, 'image_stage_rollback_entry')
    upgrade = None if runtime else module(UPGRADE, 'image_stage_rollback_upgrade')
    fds = []
    try:
        for name in LOCKS:
            if runtime:
                read(Path(name))
            fd = os.open(name, os.O_RDWR | os.O_NOFOLLOW);fds.append(fd);fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        originals = {}
        for name, row in journal['files'].items():
            path = Path(name)
            present = optional(path)
            known = (None if present is None else sha(present)) in [row['before'], row['after']]
            if not known:
                known = any(qualification_bytes_match(path, present, row, side, directory) for side in ['before', 'after'])
            need(known, 'rollback_changed')
            allowed_after = row.get('modeAfter') == row.get('modeBefore') if runtime and path == HELPER else row.get('modeAfter') in [0o600, 0o644, 0o755]
            need(type(row.get('modeAfter')) is int and allowed_after and (row['modeBefore'] is None if row['before'] is None else type(row['modeBefore']) is int and 0 <= row['modeBefore'] <= 0o777 and not row['modeBefore'] & 0o022), 'rollback_modes')
            need(re.fullmatch('[0-9]+\.before', row.get('backup', '')) is not None, 'rollback_backup_path')
            backup = None if row['before'] is None else read(directory / row['backup'], 0o600)
            need(backup is None or sha(backup) == row['before'], 'rollback_backup')
            originals[path] = backup
        if runtime:
            upgrade = module_bytes(originals[UPGRADE], UPGRADE, 'runtime_tools_rollback_previous_upgrade')
            original_manifest, original_ticket = json.loads(originals[MANIFEST]), json.loads(originals[TICKET])
            need(set(original_manifest.get('files', {})) == set(upgrade.HOST_FILES) and original_manifest.get('revision') == original_ticket.get('hostToolsRevision'), 'rollback_original_binding')
            for name, expected in original_manifest['files'].items():
                raw = originals[Path(name)] if Path(name) in originals else read(Path(name))
                need(sha(raw) == expected, 'rollback_original_tools')
            # Only this in-memory controller uses the already authenticated backup;
            # no installed helper or active history is patched.
            entry.runtime_upgrade_module = lambda: upgrade
        need(entry.entry_state().get('phase') == 'active', 'entry_not_active')
        active, _, _ = entry.active_record()
        need(upgrade.active_identity(active) == journal['active'], 'rollback_active_changed')
        for path, raw in originals.items():
            if optional(path) == raw:
                continue
            if raw is None:
                path.unlink(missing_ok=True)
            else:
                write(path, raw, journal['files'][str(path)]['modeBefore'])
        if runtime:
            upgrade = runtime_modules('runtime_tools_rollback_restored')
        old_ticket = json.loads(originals[TICKET]);upgrade.verify_installed_tools(old_ticket)
        current_binding(upgrade, entry, old_ticket)
        journal['state'] = 'rolled-back';write(directory / 'journal.json', body(journal), 0o600)
        return {'rolledBack': True, 'revision': revision, 'activeApprovalPreserved': True, 'businessContainersTouched': False, 'hostToolsReady': False, 'qualificationRequired': True}
    finally:
        for fd in reversed(fds):
            os.close(fd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['install', 'rollback', 'install-runtime', 'rollback-runtime', 'qualify', 'qualify-runtime'])
    parser.add_argument('--revision', required=True)
    parser.add_argument('--source', type=Path)
    for name in ['manifest', 'ticket', 'wrapper', 'helper', 'compose', 'upgrade']:
        parser.add_argument('--expected-' + name + '-sha256')
    arguments = parser.parse_args()
    try:
        if arguments.action in ['qualify', 'qualify-runtime']:
            result = qualify(arguments.revision, runtime=arguments.action == 'qualify-runtime')
        elif arguments.action in ['rollback', 'rollback-runtime']:
            result = rollback(arguments.revision, runtime=arguments.action == 'rollback-runtime')
        else:
            need(arguments.source is not None, 'source')
            result = install(arguments.source, arguments.revision, arguments.expected_manifest_sha256, arguments.expected_ticket_sha256, arguments.expected_wrapper_sha256, arguments.expected_helper_sha256, runtime=arguments.action == 'install-runtime', expected_compose=arguments.expected_compose_sha256, expected_upgrade=arguments.expected_upgrade_sha256)
        print(json.dumps(result))
    except BaseException:
        raise SystemExit('image_stage_install_failed_retained')
