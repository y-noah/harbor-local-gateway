#!/usr/bin/env python3
"""Prepare/import an OCI image; this module never starts a business container."""
import contextlib
import fcntl
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time

ROOT = Path('/var/lib/harbor-image-staging')
CONFIG = Path('/etc/harbor-gateway-deploy.json')
LOCK = '/run/harbor-gateway-deploy.lock'
MAX_BYTES = 2 * 1024**3
MAX_FILES = 512
METADATA_BYTES = 1024**2
RECEIVE_SECONDS = 900
IMPORT_SECONDS = 1200
DIGEST = re.compile(r'sha256:[a-f0-9]{64}\Z')
SHA = re.compile(r'[a-f0-9]{64}\Z')
MANIFEST_TYPES = {'application/vnd.oci.image.manifest.v1+json', 'application/vnd.docker.distribution.manifest.v2+json'}
CONFIG_TYPES = {'application/vnd.oci.image.config.v1+json', 'application/vnd.docker.container.image.v1+json'}
LAYER_TYPES = {'application/vnd.oci.image.layer.v1.tar+gzip', 'application/vnd.oci.image.layer.v1.tar', 'application/vnd.docker.image.rootfs.diff.tar.gzip'}


class Refused(ValueError):
    pass


def need(condition, code):
    if not condition:
        raise Refused('image_stage_' + code)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def trusted_directory(path, private=False):
    path = Path(path)
    for parent in [path, *path.parents]:
        info = parent.lstat()
        need(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022, 'directory_trust')
    if private:
        need(path.stat().st_mode & 0o777 == 0o700, 'private_directory')


def private_json(path):
    info = path.lstat()
    need(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_nlink == 1 and info.st_mode & 0o777 == 0o600 and info.st_size <= METADATA_BYTES, 'private_metadata')
    return json.loads(path.read_bytes())


def save(path, value):
    fd, name = tempfile.mkstemp(prefix='.stage-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as output:
            output.write(canonical(value) + b'\n')
            output.flush()
            os.fsync(output.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_DIRECTORY | os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(name).unlink(missing_ok=True)


def identity(payload, config):
    fields = {'action', 'image', 'role', 'manifestDigest', 'configDigest', 'parentDigest', 'archiveSha256', 'archiveBytes', 'producer'}
    wire_fields = {'transportSha256', 'transportBytes'}
    need(isinstance(payload, dict) and set(payload) in [fields, fields | wire_fields] and isinstance(payload['action'], str) and payload['action'] in {'prepare-image', 'image-stage-status', 'image-stage-plan'}, 'request')
    need(type(config.get('image_staging_enabled', False)) is bool and config.get('image_staging_enabled') is True, 'disabled')
    need(isinstance(payload['role'], str) and payload['role'] in {'gateway', 'postgres-infrastructure'}, 'role')
    need(isinstance(config.get('image_prefix'), str) and re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9.:/-]*', config['image_prefix']), 'repository')
    repository = config.get('image_prefix', '') + ('' if payload['role'] == 'gateway' else '-pg-infra')
    need(isinstance(payload['image'], str) and re.fullmatch(re.escape(repository) + r':[a-f0-9]{40}', payload['image']), 'repository')
    need(all(isinstance(payload[name], str) and DIGEST.fullmatch(payload[name]) for name in ['manifestDigest', 'configDigest']), 'digest')
    need(payload['parentDigest'] == 'none' if payload['role'] == 'gateway' else isinstance(payload['parentDigest'], str) and DIGEST.fullmatch(payload['parentDigest']), 'parent')
    need(type(payload['archiveBytes']) is int and 0 < payload['archiveBytes'] <= MAX_BYTES and isinstance(payload['archiveSha256'], str) and SHA.fullmatch(payload['archiveSha256']), 'archive_budget')
    if 'transportBytes' in payload:
        need(payload['action'] != 'image-stage-status' and type(payload['transportBytes']) is int and 0 < payload['transportBytes'] <= payload['archiveBytes'] and isinstance(payload['transportSha256'], str) and SHA.fullmatch(payload['transportSha256']), 'transport_budget')
    need(payload['action'] != 'image-stage-plan' or 'transportBytes' in payload and payload['transportBytes'] <= 3 * METADATA_BYTES, 'plan_budget')
    producer = payload['producer']
    need(isinstance(producer, dict) and set(producer) == {'project', 'pipeline', 'job'} and all(isinstance(v, str) and re.fullmatch(r'[1-9][0-9]{0,19}', v) for v in producer.values()), 'producer')
    # This is an audit binding, never evidence that CI passed or a release was approved.
    value = {key: value for key, value in payload.items() if key != 'action' and key not in wire_fields}
    return value


def stage_id(value):
    # A different transport of the same image cannot overwrite an earlier outcome.
    return sha(canonical({key: value[key] for key in ['image', 'role', 'manifestDigest', 'configDigest', 'parentDigest']}))


def descriptor(value, types, entries):
    need(isinstance(value, dict) and isinstance(value.get('digest'), str) and DIGEST.fullmatch(value['digest']) and type(value.get('size')) is int and value['size'] >= 0 and value.get('mediaType') in types, 'descriptor')
    name = 'blobs/sha256/' + value['digest'][7:]
    need(name in entries and entries[name].size == value['size'], 'descriptor_size')
    return name


def validate_archive(path, value, partial=False, metadata_only=False):
    """Hash the complete exact OCI closure without extracting any entry."""
    with tarfile.open(path, 'r:') as archive:
        entries = {}
        total = 0
        for member in archive:
            name = member.name
            need(member.isfile() and name not in entries and (name in {'index.json', 'oci-layout'} or re.fullmatch(r'blobs/sha256/[a-f0-9]{64}', name)), 'archive_entry')
            need(not member.pax_headers or set(member.pax_headers) <= {'mtime', 'atime', 'ctime'}, 'archive_metadata')
            entries[name] = member
            total += member.size
            need(len(entries) <= MAX_FILES and total <= MAX_BYTES, 'archive_budget')
        need({'index.json', 'oci-layout'} <= set(entries), 'layout_missing')
        def small(name):
            need(entries[name].size <= METADATA_BYTES, 'metadata_budget')
            return json.loads(archive.extractfile(entries[name]).read(METADATA_BYTES + 1))
        need(small('oci-layout') == {'imageLayoutVersion': '1.0.0'}, 'layout')
        index = small('index.json')
        need(isinstance(index, dict) and index.get('schemaVersion') == 2 and isinstance(index.get('manifests'), list) and len(index['manifests']) == 1, 'index')
        root = index['manifests'][0]
        name = descriptor(root, MANIFEST_TYPES, entries)
        need(root['digest'] == value['manifestDigest'] and root.get('annotations', {}).get('org.opencontainers.image.ref.name') == value['image'], 'index_binding')
        manifest = small(name)
        need(isinstance(manifest, dict) and manifest.get('schemaVersion') == 2 and isinstance(manifest.get('layers'), list) and 0 < len(manifest['layers']) <= MAX_FILES - 4, 'manifest')
        config_name = descriptor(manifest.get('config'), CONFIG_TYPES, entries)
        need(manifest['config']['digest'] == value['configDigest'], 'config_binding')
        closure = {name, config_name, 'index.json', 'oci-layout'}
        layers = {}
        for layer in manifest['layers']:
            need(isinstance(layer, dict) and isinstance(layer.get('digest'), str) and DIGEST.fullmatch(layer['digest']) and type(layer.get('size')) is int and 0 <= layer['size'] <= MAX_BYTES and layer.get('mediaType') in LAYER_TYPES, 'descriptor')
            layer_name = 'blobs/sha256/' + layer['digest'][7:]
            layers[layer_name] = layer
            if layer_name in entries:
                need(entries[layer_name].size == layer['size'], 'descriptor_size')
        required = set(closure)
        closure.update(layers)
        sizes = {name: entries[name].size for name in required}
        sizes.update({name: row['size'] for name, row in layers.items()})
        # Metadata-only and delta inputs still bind the whole final image, not
        # merely the few bytes currently on the wire. Reject before cache copy.
        need(sum(sizes.values()) <= MAX_BYTES, 'complete_budget')
        tar_bytes = sum(512 + ((size + 511) // 512) * 512 for size in sizes.values()) + 1024
        tar_bytes = ((tar_bytes + tarfile.RECORDSIZE - 1) // tarfile.RECORDSIZE) * tarfile.RECORDSIZE
        need(tar_bytes == value['archiveBytes'] and tar_bytes <= MAX_BYTES, 'complete_archive_size')
        expected = required if metadata_only else closure
        need(required <= set(entries) <= closure if partial and not metadata_only else set(entries) == expected, 'closure')
        for name, member in entries.items():
            if not name.startswith('blobs/'):
                continue
            digest = hashlib.sha256()
            with archive.extractfile(member) as source:
                for block in iter(lambda: source.read(1024**2), b''):
                    digest.update(block)
            need(digest.hexdigest() == name.rsplit('/', 1)[1], 'blob_digest')
        config = small(config_name)
        need(isinstance(config, dict) and config.get('os') == 'linux' and config.get('architecture') == 'amd64' and isinstance(config.get('rootfs'), dict) and isinstance(config.get('config'), dict), 'platform')
        diffs = config.get('rootfs', {}).get('diff_ids')
        need(config.get('rootfs', {}).get('type') == 'layers' and isinstance(diffs, list) and len(diffs) == len(manifest['layers']) and all(isinstance(v, str) and DIGEST.fullmatch(v) for v in diffs), 'rootfs')
        expanded = 0
        for layer, expected in zip(manifest['layers'], diffs):
            layer_name = 'blobs/sha256/' + layer['digest'][7:]
            if layer_name not in entries:
                need(partial, 'layer_missing')
                continue
            digest = hashlib.sha256()
            with contextlib.ExitStack() as stack:
                source = stack.enter_context(archive.extractfile(entries[layer_name]))
                if layer['mediaType'].endswith(('+gzip', '.gzip')):
                    source = stack.enter_context(gzip.GzipFile(fileobj=source))
                for block in iter(lambda: source.read(1024**2), b''):
                    expanded += len(block)
                    need(expanded <= MAX_BYTES, 'expanded_budget')
                    digest.update(block)
            need('sha256:' + digest.hexdigest() == expected, 'layer_diff_id')
        labels = config.get('config', {}).get('Labels', {})
        if value['role'] == 'gateway':
            need(labels.get('org.opencontainers.image.revision') == value['image'].rsplit(':', 1)[1] and labels.get('com.harbor.storage.postgres') == 'postgres-conversation-v2' and labels.get('com.harbor.postgres-preflight') == 'v1' and labels.get('com.harbor.extra-usage-dispatch') == 'v1', 'business_contract')
        else:
            business = value['image'].rsplit(':', 1)[0].removesuffix('-pg-infra') + '@' + value['parentDigest']
            need(labels.get('com.harbor.infrastructure') == 'postgres18-report-v1' and labels.get('com.harbor.publisher-business-image') == business, 'infrastructure_contract')
        return config


def prepare(source, target, image, manifest_digest, role, parent_digest, producer):
    """Normalize a Skopeo OCI directory to one canonical native Docker-load tar."""
    source, target = Path(source), Path(target)
    index = json.loads((source / 'index.json').read_bytes())
    need(index.get('schemaVersion') == 2 and len(index.get('manifests', [])) == 1, 'index')
    root = index['manifests'][0]
    need(root.get('digest') == manifest_digest and DIGEST.fullmatch(manifest_digest), 'manifest_binding')
    manifest_path = source / 'blobs' / 'sha256' / manifest_digest[7:]
    manifest = json.loads(manifest_path.read_bytes())
    config_digest = manifest.get('config', {}).get('digest')
    need(isinstance(config_digest, str) and DIGEST.fullmatch(config_digest), 'config_binding')
    root.setdefault('annotations', {})['org.opencontainers.image.ref.name'] = image
    names = {'oci-layout': None, 'index.json': canonical(index)}
    for digest in [manifest_digest, config_digest, *(row.get('digest') for row in manifest.get('layers', []))]:
        need(isinstance(digest, str) and DIGEST.fullmatch(digest), 'descriptor')
        names['blobs/sha256/' + digest[7:]] = None
    with target.open('xb') as output:
        os.fchmod(output.fileno(), 0o600)
        with tarfile.open(fileobj=output, mode='w', format=tarfile.USTAR_FORMAT) as archive:
            for name, raw in sorted(names.items()):
                path = source / name
                if raw is None:
                    info = path.lstat()
                    need(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, 'source_file')
                header = tarfile.TarInfo(name)
                header.mode = 0o644
                header.size = len(raw) if raw is not None else info.st_size
                with contextlib.ExitStack() as stack:
                    stream = io.BytesIO(raw) if raw is not None else stack.enter_context(path.open('rb'))
                    archive.addfile(header, stream)
        output.flush()
        os.fsync(output.fileno())
    digest = hashlib.sha256()
    with target.open('rb') as stream:
        for block in iter(lambda: stream.read(1024**2), b''):
            digest.update(block)
    value = {'image': image, 'role': role, 'manifestDigest': manifest_digest, 'configDigest': config_digest, 'parentDigest': parent_digest, 'archiveSha256': digest.hexdigest(), 'archiveBytes': target.stat().st_size, 'producer': producer}
    validate_archive(target, value)
    return {'action': 'prepare-image', **value}


def archive_hash(path, value):
    info = path.lstat()
    need(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size == value['archiveBytes'] and info.st_uid == os.geteuid() and not info.st_mode & 0o077, 'archive_identity')
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024**2), b''):
            digest.update(block)
    need(digest.hexdigest() == value['archiveSha256'], 'archive_digest')


def layer_descriptors(archive, value):
    member = archive.getmember('blobs/sha256/' + value['manifestDigest'][7:])
    need(member.isfile() and member.size <= METADATA_BYTES, 'manifest')
    manifest = json.loads(archive.extractfile(member).read(METADATA_BYTES + 1))
    return {row['digest']: row['size'] for row in manifest['layers']}


def delta_archive(source, target, missing):
    """A deterministic subset tar: metadata always travels, only missing layers do."""
    need(isinstance(missing, list) and len(missing) <= MAX_FILES and len(set(missing)) == len(missing) and all(isinstance(d, str) and DIGEST.fullmatch(d) for d in missing), 'delta_selection')
    with tarfile.open(source, 'r:') as archive, target.open('xb') as output:
        os.fchmod(output.fileno(), 0o600)
        index = json.loads(archive.extractfile('index.json').read(METADATA_BYTES + 1))
        manifest_digest = index['manifests'][0]['digest']
        manifest = json.loads(archive.extractfile('blobs/sha256/' + manifest_digest[7:]).read(METADATA_BYTES + 1))
        layers = {row['digest'] for row in manifest['layers']}
        need(set(missing) <= layers, 'delta_selection')
        selected = {'oci-layout', 'index.json', 'blobs/sha256/' + manifest_digest[7:], 'blobs/sha256/' + manifest['config']['digest'][7:], *('blobs/sha256/' + d[7:] for d in missing)}
        with tarfile.open(fileobj=output, mode='w', format=tarfile.USTAR_FORMAT) as transport:
            for name in sorted(selected):
                member = archive.getmember(name)
                need(member.isfile(), 'delta_entry')
                header = tarfile.TarInfo(name)
                header.mode, header.size = 0o644, member.size
                with archive.extractfile(member) as stream:
                    transport.addfile(header, stream)
        output.flush()
        os.fsync(output.fileno())
    checksum = hashlib.sha256()
    with target.open('rb') as stream:
        for block in iter(lambda: stream.read(1024**2), b''):
            checksum.update(block)
    return {'transportBytes': target.stat().st_size, 'transportSha256': checksum.hexdigest()}


def reusable_layers(wanted):
    """Only prior complete receipts and their exact authenticated archives count."""
    sources = {}
    candidates = []
    for path in ROOT.iterdir():
        if SHA.fullmatch(path.name) and path.is_dir() and not path.is_symlink() and (path / 'receipt.json').exists():
            trusted_directory(path, private=True)
            old = private_json(path / 'receipt.json')
            if old.get('phase') == 'complete':
                candidates.append((path, old))
    for path, old in sorted(candidates, key=lambda row: (row[0] / 'receipt.json').stat().st_mtime_ns, reverse=True)[:3]:
        value = old.get('identity', {})
        need(stage_id(value) == path.name, 'reuse_binding')
        status(value)  # Requires the original actual-load/config/layers completion proof.
        archive_path = path / 'image.oci.tar'
        if not archive_path.exists():
            continue
        archive_hash(archive_path, value)
        with tarfile.open(archive_path, 'r:') as archive:
            previous = layer_descriptors(archive, value)
            for digest, size in wanted.items():
                if digest in sources or previous.get(digest) != size:
                    continue
                member = archive.getmember('blobs/sha256/' + digest[7:])
                need(member.isfile() and member.size == size, 'reuse_descriptor')
                sources[digest] = archive_path
    return sources


def assemble_delta(source, target, value):
    validate_archive(source, value, partial=True)
    with tarfile.open(source, 'r:') as archive:
        wanted = layer_descriptors(archive, value)
        names = set(archive.getnames())
        absent = {d: size for d, size in wanted.items() if 'blobs/sha256/' + d[7:] not in names}
        sources = reusable_layers(absent)
        need(set(sources) == set(absent), 'reuse_missing')
        with target.open('xb') as output:
            os.fchmod(output.fileno(), 0o600)
            class BoundedOutput:
                def write(self, block):
                    need(output.tell() + len(block) <= value['archiveBytes'], 'complete_write_budget')
                    return output.write(block)
                def tell(self):
                    return output.tell()
            with tarfile.open(fileobj=BoundedOutput(), mode='w', format=tarfile.USTAR_FORMAT) as transport, contextlib.ExitStack() as opened:
                previous = {path: opened.enter_context(tarfile.open(path, 'r:')) for path in set(sources.values())}
                names.update('blobs/sha256/' + digest[7:] for digest in absent)
                for name in sorted(names):
                    chosen = previous[sources['sha256:' + name.rsplit('/', 1)[1]]] if name.startswith('blobs/') and 'sha256:' + name.rsplit('/', 1)[1] in absent else archive
                    member = chosen.getmember(name)
                    need(member.isfile(), 'reuse_entry')
                    header = tarfile.TarInfo(name)
                    header.mode, header.size = 0o644, member.size
                    with chosen.extractfile(member) as stream:
                        transport.addfile(header, stream)
            output.flush()
            os.fsync(output.fileno())
    archive_hash(target, value)
    validate_archive(target, value)


def read_wire(stream, target, payload):
    size = payload.get('transportBytes', payload['archiveBytes'])
    expected = payload.get('transportSha256', payload['archiveSha256'])
    with receive_deadline(), target.open('xb') as output:
        os.fchmod(output.fileno(), 0o600)
        checksum, remaining = hashlib.sha256(), size
        while remaining:
            block = stream.read(min(1024**2, remaining))
            need(bool(block), 'truncated')
            output.write(block)
            checksum.update(block)
            remaining -= len(block)
        need(not stream.read(1), 'trailing_bytes')
        need(checksum.hexdigest() == expected, 'archive_digest')
        output.flush()
        os.fsync(output.fileno())


@contextlib.contextmanager
def receive_deadline():
    previous = signal.getsignal(signal.SIGALRM)
    def expired(_signal, _frame):
        raise Refused('image_stage_receive_timeout')
    signal.signal(signal.SIGALRM, expired)
    old_timer = signal.setitimer(signal.ITIMER_REAL, RECEIVE_SECONDS)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, *old_timer)
        signal.signal(signal.SIGALRM, previous)


@contextlib.contextmanager
def cancellation_signals():
    previous = {}
    def cancelled(_signal, _frame):
        raise Refused('image_stage_cancelled')
    for name in [signal.SIGTERM, signal.SIGHUP, signal.SIGINT]:
        previous[name] = signal.getsignal(name)
        signal.signal(name, cancelled)
    try:
        yield
    finally:
        for name, handler in previous.items():
            signal.signal(name, handler)


def receipt(directory, value, phase, **fields):
    return {'contract': 'harbor-image-stage-v1', 'stageId': directory.name, 'identity': value, 'phase': phase, 'ciVerified': False, 'deploymentAllowed': False, 'businessContainersTouched': False, **fields}


def status(value):
    directory = ROOT / stage_id(value)
    if not directory.exists():
        return receipt(directory, value, 'absent')
    trusted_directory(directory, private=True)
    result = private_json(directory / 'receipt.json')
    need(result.get('contract') == 'harbor-image-stage-v1' and result.get('stageId') == directory.name and result.get('ciVerified') is False and result.get('deploymentAllowed') is False and result.get('businessContainersTouched') is False, 'receipt_binding')
    same_image = lambda v: {k: x for k, x in v.items() if k != 'producer'}
    need(same_image(result.get('identity', {})) == same_image(value), 'existing_identity')
    if result.get('phase') == 'complete':
        need(result.get('loadCompleted') is True and result.get('loadClientExitCode') == 0 and result.get('nativeImageImported') is True and result.get('allConfigAndLayersVerified') is True, 'completion_receipt')
    return result


def start_import(directory):
    unit = 'harbor-image-stage-' + directory.name + '.service'
    helper = Path(__file__).resolve()
    trusted_directory(helper.parent)
    info = helper.lstat()
    need(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022 and info.st_nlink == 1, 'helper_trust')
    result = subprocess.run(['/usr/bin/systemd-run', '--unit=' + unit, '--property=Type=exec', '--property=UMask=0077', '--property=RuntimeMaxSec=' + str(IMPORT_SECONDS + 60), '--property=KillMode=control-group', '/usr/bin/python3', str(helper), '--import', directory.name], capture_output=True, timeout=30)
    need(result.returncode == 0, 'import_start')


def plan_layers(stream, payload, value):
    trusted_directory(ROOT.parent)
    ROOT.mkdir(mode=0o700, exist_ok=True)
    trusted_directory(ROOT, private=True)
    temporary = Path(tempfile.mkdtemp(prefix='.plan-', dir=ROOT))
    try:
        target = temporary / 'metadata.tar'
        read_wire(stream, target, payload)
        validate_archive(target, value, partial=True, metadata_only=True)
        with tarfile.open(target, 'r:') as archive:
            wanted = layer_descriptors(archive, value)
        reusable = reusable_layers(wanted)
        return {**receipt(ROOT / stage_id(value), value, 'absent'), 'missingBlobs': sorted(set(wanted) - set(reusable)), 'reuseBytes': sum(wanted[d] for d in reusable)}
    finally:
        shutil.rmtree(temporary)


def receive(stream, value, payload=None):
    trusted_directory(ROOT.parent)
    ROOT.mkdir(mode=0o700, exist_ok=True)
    trusted_directory(ROOT, private=True)
    destination = ROOT / stage_id(value)
    if destination.exists():
        # Consume/verify the retried wire too; a repeated request cannot hide trailing bytes.
        existing = status(value)
    else:
        existing = None
    temporary = Path(tempfile.mkdtemp(prefix='.incoming-', dir=ROOT))
    try:
        need(shutil.disk_usage(ROOT).free > value['archiveBytes'] + MAX_BYTES, 'disk_budget')
        target = temporary / 'image.oci.tar'
        wire = temporary / 'transport.tar'
        read_wire(stream, wire, payload or value)
        if (payload or value).get('transportBytes', value['archiveBytes']) == value['archiveBytes'] and (payload or value).get('transportSha256', value['archiveSha256']) == value['archiveSha256']:
            os.rename(wire, target)
            validate_archive(target, value)
        else:
            assemble_delta(wire, target, value)
            wire.unlink()
        if existing is not None:
            return existing
        save(temporary / 'receipt.json', receipt(destination, value, 'queued'))
        os.rename(temporary, destination)
        try:
            start_import(destination)
        except Exception:
            save(destination / 'receipt.json', receipt(destination, value, 'failed', errorCode='image_stage_import_start'))
            raise
        return receipt(destination, value, 'queued')
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def native_proof(directory, value, config, timeout=60):
    pinned = value['image'].rsplit(':', 1)[0] + '@' + value['manifestDigest']
    result = subprocess.run(['/usr/bin/docker', 'image', 'inspect', pinned, value['image']], capture_output=True, timeout=timeout, env=safe_environment(directory))
    need(result.returncode == 0, 'image_query')
    rows = json.loads(result.stdout)
    need(isinstance(rows, list) and len(rows) == 2 and rows[0]['Id'] == rows[1]['Id'] and rows[0]['Id'] in {value['manifestDigest'], value['configDigest']}, 'image_identity')
    for row in rows:
        need(pinned in row.get('RepoDigests', []) and row.get('Descriptor', {}).get('digest') == value['manifestDigest'] and row.get('Os') == 'linux' and row.get('Architecture') == 'amd64', 'native_manifest')
        need(row.get('RootFS', {}).get('Layers') == config['rootfs']['diff_ids'] and all(row.get('Config', {}).get(k) == v for k, v in config.get('config', {}).items()), 'native_config_layers')
    return rows[0]['Id']


def safe_environment(directory):
    return {'PATH': '/usr/sbin:/usr/bin:/sbin:/bin', 'HOME': '/nonexistent', 'DOCKER_CONFIG': str(directory / 'empty-docker-config'), 'DOCKER_HOST': 'unix:///var/run/docker.sock', 'LANG': 'C.UTF-8'}


def import_image(identifier):
    need(os.geteuid() == 0 and SHA.fullmatch(identifier), 'worker_identity')
    trusted_directory(ROOT, private=True)
    directory = ROOT / identifier
    trusted_directory(directory, private=True)
    state = private_json(directory / 'receipt.json')
    value = state['identity']
    try:
        identity({'action': 'prepare-image', **value}, private_json(CONFIG))
        need(stage_id(value) == identifier and state['phase'] == 'queued', 'worker_identity')
        unit = 'harbor-image-stage-' + identifier + '.service'
        need(Path('/proc/self/cgroup').read_text().strip() == '0::/system.slice/' + unit, 'worker_service')
        with open(LOCK, 'a') as lock:
            _import_locked(directory, value, lock)
    except BaseException as error:
        code = str(error) if isinstance(error, Refused) else 'image_stage_import_outcome_unknown'
        save(directory / 'receipt.json', receipt(directory, value, 'failed', errorCode=code, nativeImageImported=False))
        raise


def _import_locked(directory, value, lock):
    operation_deadline = time.monotonic() + IMPORT_SECONDS
    # The caller releases this same lock immediately after systemd-run returns.
    deadline = time.monotonic() + 30
    while True:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            need(time.monotonic() < deadline, 'busy')
            time.sleep(0.1)
    path = directory / 'image.oci.tar'
    info = path.lstat()
    need(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size == value['archiveBytes'] and not info.st_mode & 0o077, 'archive_identity')
    checksum = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024**2), b''):
            checksum.update(block)
    need(checksum.hexdigest() == value['archiveSha256'], 'archive_digest')
    config = validate_archive(path, value)
    empty = directory / 'empty-docker-config'
    empty.mkdir(mode=0o700)
    save(directory / 'receipt.json', receipt(directory, value, 'importing'))
    load_process = None
    try:
        with (directory / 'load.stdout').open('xb') as stdout, (directory / 'load.stderr').open('xb') as stderr:
            need(time.monotonic() < operation_deadline - 60, 'load_budget')
            load_process = subprocess.Popen(['/usr/bin/docker', 'load', '--input', str(path)], stdout=stdout, stderr=stderr, env=safe_environment(directory))
            while load_process.poll() is None:
                need(time.monotonic() < operation_deadline - 60, 'load_timeout')
                need(all((directory / name).stat().st_size <= METADATA_BYTES for name in ['load.stdout', 'load.stderr']), 'load_output_budget')
                time.sleep(0.2)
        need(load_process.returncode == 0, 'load_failed')
    finally:
        if load_process is not None and load_process.poll() is None:
            load_process.terminate()
            try:
                load_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                load_process.kill()
                load_process.wait(timeout=5)
    remaining = operation_deadline - time.monotonic()
    need(remaining > 0, 'verification_budget')
    image_id = native_proof(directory, value, config, timeout=min(60, remaining))
    save(directory / 'receipt.json', receipt(directory, value, 'complete', loadCompleted=True, loadClientExitCode=0, imageId=image_id, nativeImageImported=True, allConfigAndLayersVerified=True))


def handle(payload, stream, config):
    value = identity(payload, config)
    with cancellation_signals():
        if payload['action'] == 'image-stage-status':
            with receive_deadline():
                need(not stream.read(1), 'status_bytes')
            return status(value)
        if payload['action'] == 'image-stage-plan':
            return plan_layers(stream, payload, value)
        return receive(stream, value, payload)


if __name__ == '__main__':
    try:
        if len(sys.argv) == 3 and sys.argv[1] == '--import':
            with cancellation_signals():
                import_image(sys.argv[2])
        elif len(sys.argv) == 11 and sys.argv[1] == 'prepare':
            source, target, image, digest, role, parent, project, pipeline, job = sys.argv[2:]
            print(json.dumps(prepare(source, target, image, digest, role, parent, {'project': project, 'pipeline': pipeline, 'job': job})))
        elif len(sys.argv) == 5 and sys.argv[1] == 'delta':
            print(json.dumps(delta_archive(Path(sys.argv[2]), Path(sys.argv[3]), json.loads(sys.argv[4]))))
        else:
            raise Refused('image_stage_arguments')
    except BaseException as error:
        print(str(error) if isinstance(error, Refused) else 'image_stage_operation_failed', file=sys.stderr)
        sys.exit(1)
