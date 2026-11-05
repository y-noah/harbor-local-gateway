"""Fixed local OCI extractor; Python >=3.10, no package installation.

Large digest/decompression/copy work belongs inside the role's real cgroup.
This module never fetches images, mounts a host filesystem, or confers trust on
an OCI layout: its caller must first bind the pinned business B CI identity.
"""
import gzip
import hashlib
import json
import os
import signal
import stat
import tarfile
import tempfile
import time


class ExtractionError(RuntimeError):
    pass


def require(condition, code):
    if not condition:
        raise ExtractionError(code)


def identity_value(value):
    if isinstance(value, str):
        require(value.isascii() and value.isdecimal() and
                (value == '0' or not value.startswith('0')) and len(value) <= 20,
                'capacity_runtime_identity')
        result = int(value)
        require(result <= 2**64 - 1, 'capacity_runtime_identity')
        return result
    require(type(value) is int and 0 <= value <= 2**53 - 1,
            'capacity_runtime_identity')
    return value


class Budget:
    def __init__(self, *, max_bytes, max_files, timeout_seconds):
        require(type(max_bytes) is int and 1 <= max_bytes <= 128 * 1024**3,
                "capacity_runtime_extract_budget")
        require(type(max_files) is int and 1 <= max_files <= 1000000,
                "capacity_runtime_extract_budget")
        require(type(timeout_seconds) is int and 1 <= timeout_seconds <= 3600,
                "capacity_runtime_extract_budget")
        self.limit = max_bytes
        self.max_files = max_files
        self.deadline = time.monotonic() + timeout_seconds
        self.bytes = 0
        self.files = 0
        self.cancelled = False

    def check(self):
        require(not self.cancelled, "capacity_runtime_extract_cancelled")
        require(time.monotonic() < self.deadline,
                "capacity_runtime_extract_timeout")

    def file(self, size):
        self.check()
        require(type(size) is int and size >= 0, 'capacity_runtime_tar_size')
        self.files += 1
        self.bytes += size
        require(self.files <= self.max_files and self.bytes <= self.limit,
                "capacity_runtime_extract_budget")


def name_parts(name, *, allow_empty_root=False):
    # Some OCI producers encode the directory root as an empty tar name.
    # Only that directory-member context may normalize it to the owned root.
    require(isinstance(name, str) and
            (0 < len(name.encode()) <= 4096 or allow_empty_root and name == "") and
            "\x00" not in name and "\\" not in name and
            not name.startswith("/"), "capacity_runtime_tar_path")
    parts = [part for part in name.split("/") if part not in ("", ".")]
    require(".." not in parts and len(parts) <= 128, "capacity_runtime_tar_path")
    return parts


def link_parts(parent, name):
    require(isinstance(name, str) and 0 < len(name.encode()) <= 4096 and
            "\x00" not in name and "\\" not in name,
            "capacity_runtime_tar_link")
    parts = [] if name.startswith("/") else list(parent)
    for part in name.split("/"):
        if part in ("", "."):
            continue
        parts.append(part)
    require(len(parts) <= 128, "capacity_runtime_tar_link_depth")
    return parts


class Root:
    """Resolve image symlinks inside a retained root fd, never through host /."""
    def __init__(self, directory):
        self.fd = (os.dup(directory) if type(directory) is int else
                   os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW))
        identity = os.fstat(self.fd)
        require(identity.st_uid == os.getuid() and identity.st_mode & 0o077 == 0,
                "capacity_runtime_extract_directory")
        require(not os.listdir(self.fd), "capacity_runtime_extract_directory")

    def close(self):
        os.close(self.fd)

    def directory(self, parts, create=False, with_parts=False):
        pending, resolved, traversed = list(parts), [], 0
        fd = os.dup(self.fd)
        try:
            while pending:
                part = pending.pop(0)
                if part == '..':
                    require(bool(resolved), 'capacity_runtime_tar_link_escape')
                    resolved.pop()
                    pending = resolved + pending
                    resolved = []
                    os.close(fd)
                    fd = os.dup(self.fd)
                    continue
                try:
                    info = os.stat(part, dir_fd=fd, follow_symlinks=False)
                except FileNotFoundError:
                    require(create, "capacity_runtime_tar_parent_missing")
                    os.mkdir(part, 0o755, dir_fd=fd)
                    info = os.stat(part, dir_fd=fd, follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    traversed += 1
                    require(traversed <= 40, "capacity_runtime_tar_link_cycle")
                    target = link_parts(resolved, os.readlink(part, dir_fd=fd))
                    pending = target + pending
                    resolved = []
                    os.close(fd)
                    fd = os.dup(self.fd)
                else:
                    require(stat.S_ISDIR(info.st_mode),
                            "capacity_runtime_tar_parent_type")
                    next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY |
                                      os.O_NOFOLLOW, dir_fd=fd)
                    os.close(fd)
                    fd = next_fd
                    resolved.append(part)
            return (fd, resolved) if with_parts else fd
        except BaseException:
            os.close(fd)
            raise

    def remove(self, directory, name):
        try:
            info = os.stat(name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            return
        if stat.S_ISDIR(info.st_mode):
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=directory)
            try:
                for item in os.listdir(child):
                    self.remove(child, item)
            finally:
                os.close(child)
            os.rmdir(name, dir_fd=directory)
        else:
            os.unlink(name, dir_fd=directory)

    def check_link_bound(self, parts):
        pending, names, stack, links = list(parts), [], [os.dup(self.fd)], 0
        try:
            while pending:
                part = pending.pop(0)
                if part == '..':
                    require(bool(names), 'capacity_runtime_tar_link_escape')
                    names.pop()
                    old = stack.pop()
                    if old is not None:
                        os.close(old)
                    continue
                current = stack[-1]
                if current is None:
                    names.append(part)
                    stack.append(None)
                    continue
                try:
                    info = os.stat(part, dir_fd=current, follow_symlinks=False)
                except FileNotFoundError:
                    names.append(part)
                    stack.append(None)
                    continue
                if stat.S_ISLNK(info.st_mode):
                    links += 1
                    require(links <= 40, 'capacity_runtime_tar_link_cycle')
                    pending = link_parts(names, os.readlink(part, dir_fd=current)) + pending
                    for fd in stack:
                        if fd is not None:
                            os.close(fd)
                    stack, names = [os.dup(self.fd)], []
                else:
                    names.append(part)
                    stack.append(os.open(part, os.O_RDONLY | os.O_DIRECTORY |
                                         os.O_NOFOLLOW, dir_fd=current)
                                 if stat.S_ISDIR(info.st_mode) else None)
        finally:
            for fd in stack:
                if fd is not None:
                    os.close(fd)

    def lower_metadata(self, parts):
        fd = os.dup(self.fd)
        try:
            for part in parts[:-1]:
                try:
                    child = os.open(part, os.O_RDONLY | os.O_DIRECTORY |
                                    os.O_NOFOLLOW, dir_fd=fd)
                except (FileNotFoundError, NotADirectoryError):
                    return None
                os.close(fd)
                fd = child
            try:
                info = os.stat(parts[-1], dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError:
                return None
            if stat.S_ISDIR(info.st_mode):
                return {'kind': 'directory', 'fresh': False}
            if stat.S_ISLNK(info.st_mode):
                return {'kind': 'link', 'target': os.readlink(parts[-1], dir_fd=fd)}
            return {'kind': 'file'}
        finally:
            os.close(fd)

    def validate_links(self, budget):
        def visit(directory, parts):
            for name in os.listdir(directory):
                budget.check()
                info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    self.check_link_bound(parts + [name])
                elif stat.S_ISDIR(info.st_mode):
                    child = os.open(name, os.O_RDONLY | os.O_DIRECTORY |
                                    os.O_NOFOLLOW, dir_fd=directory)
                    try:
                        visit(child, parts + [name])
                    finally:
                        os.close(child)
        visit(self.fd, [])

    def whiteout(self, parts):
        try:
            parent = self.directory(parts[:-1], create=False)
        except ExtractionError as error:
            if str(error) == 'capacity_runtime_tar_parent_missing':
                return
            raise
        try:
            if parts[-1] == ".wh..wh..opq":
                for item in os.listdir(parent):
                    self.remove(parent, item)
            else:
                target = parts[-1][4:]
                require(target not in ("", ".", "..") and
                        not target.startswith(".wh."),
                        "capacity_runtime_whiteout")
                self.remove(parent, target)
        finally:
            os.close(parent)

    def write(self, archive, member, parts, budget):
        parent, canonical_parent = self.directory(parts[:-1], create=True,
                                                     with_parts=True)
        name = parts[-1]
        try:
            # Directories merge; all other type changes replace the lower node.
            if member.isdir():
                try:
                    info = os.stat(name, dir_fd=parent, follow_symlinks=False)
                    if not stat.S_ISDIR(info.st_mode):
                        self.remove(parent, name)
                        os.mkdir(name, member.mode & 0o1777,
                                 dir_fd=parent)
                except FileNotFoundError:
                    os.mkdir(name, member.mode & 0o1777, dir_fd=parent)
                directory = os.open(name, os.O_RDONLY | os.O_DIRECTORY |
                                    os.O_NOFOLLOW, dir_fd=parent)
                try:
                    os.fchmod(directory, member.mode & 0o1777)
                finally:
                    os.close(directory)
                return
            self.remove(parent, name)
            if member.issym():
                # Absolute image links are rewritten into root-relative links;
                # retaining them verbatim before chroot would expose host paths.
                target = link_parts(canonical_parent, member.linkname)
                self.check_link_bound(target)
                if member.linkname.startswith('/'):
                    relative = '/'.join(['..'] * len(canonical_parent) +
                                        [part for part in member.linkname.split('/')
                                         if part not in ('', '.')]) or '.'
                else:
                    relative = '/'.join(part for part in member.linkname.split('/')
                                        if part not in ('', '.')) or '.'
                os.symlink(relative, name, dir_fd=parent)
            elif member.islnk():
                target = name_parts(member.linkname)
                require(bool(target), "capacity_runtime_tar_hardlink")
                source = self.directory(target[:-1])
                try:
                    info = os.stat(target[-1], dir_fd=source,
                                   follow_symlinks=False)
                    require(stat.S_ISREG(info.st_mode),
                            "capacity_runtime_tar_hardlink")
                    os.link(target[-1], name, src_dir_fd=source,
                            dst_dir_fd=parent, follow_symlinks=False)
                finally:
                    os.close(source)
            else:
                require(member.isreg() and not member.sparse,
                        "capacity_runtime_tar_type")
                free = os.fstatvfs(parent)
                require(free.f_bavail * free.f_frsize >= member.size,
                        "capacity_runtime_extract_space")
                output = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                                 os.O_NOFOLLOW, member.mode & 0o1777,
                                 dir_fd=parent)
                try:
                    stream = archive.extractfile(member)
                    require(stream is not None, "capacity_runtime_tar_content")
                    remaining = member.size
                    with stream:
                        while remaining:
                            budget.check()
                            chunk = stream.read(min(1048576, remaining))
                            require(bool(chunk), "capacity_runtime_tar_truncated")
                            view = memoryview(chunk)
                            while view:
                                written = os.write(output, view)
                                view = view[written:]
                            remaining -= len(chunk)
                    os.fchmod(output, member.mode & 0o1777)
                finally:
                    os.close(output)
        finally:
            os.close(parent)


class LayerNamespace:
    """Plan effective upper aliases/types without mutating the lower tree."""
    def __init__(self, root, budget, whiteouts=()):
        self.root, self.budget = root, budget
        self.mask = {"children": {}}
        for path in whiteouts:
            opaque = path[-1] == ".wh..wh..opq"
            target = path[:-1] if opaque else path[:-1] + [path[-1][4:]]
            node = self.mask
            for part in target:
                node = node["children"].setdefault(part, {"children": {}})
            node["descendants" if opaque else "subtree"] = True
        self.tree = {'children': {}, 'row': {'kind': 'directory', 'fresh': False}}

    def node(self, parts, create=False):
        node = self.tree
        for part in parts:
            if part not in node['children']:
                if not create:
                    return None
                node['children'][part] = {'children': {}, 'row': None}
            node = node['children'][part]
        return node

    def lower(self, parts):
        node = self.mask
        for part in parts:
            if node.get('subtree') or node.get('descendants'):
                return None
            node = node['children'].get(part)
            if node is None:
                return self.root.lower_metadata(parts)
        if node.get('subtree'):
            return None
        return self.root.lower_metadata(parts)

    def lookup(self, parts):
        if not parts:
            return self.tree['row']
        node, inherited = self.tree, True
        for part in parts:
            row = node['row']
            if row and (row['kind'] != 'directory' or row.get('fresh')):
                inherited = False
            node = node['children'].get(part)
            if node is None:
                return self.lower(parts) if inherited else None
        return node['row'] or (self.lower(parts) if inherited else None)

    def set(self, parts, row, merge=False):
        node = self.node(parts, create=True)
        if not merge:
            node['children'] = {}
        node['row'] = row

    def resolve(self, parts, create=False):
        pending, resolved, links = list(parts), [], 0
        while pending:
            self.budget.check()
            part = pending.pop(0)
            if part == '..':
                require(bool(resolved), 'capacity_runtime_tar_link_escape')
                resolved.pop()
                continue
            key = resolved + [part]
            row = self.lookup(key)
            if row is None:
                if not create:
                    return None
                row = {'kind': 'directory', 'fresh': True}
                self.set(key, row)
            if row['kind'] == 'link':
                links += 1
                require(links <= 40, 'capacity_runtime_tar_link_cycle')
                pending = link_parts(resolved, row['target']) + pending
                resolved = []
            else:
                require(row['kind'] == 'directory', 'capacity_runtime_tar_parent_type')
                resolved.append(part)
        return resolved

    def apply(self, member, parts):
        parent = self.resolve(parts[:-1], create=True)
        key = parent + parts[-1:]
        if member.isdir():
            previous = self.lookup(key)
            merge = previous is not None and previous['kind'] == 'directory'
            self.set(key, {'kind': 'directory',
                          'fresh': previous.get('fresh', False) if merge else True},
                     merge=merge)
        elif member.issym():
            self.set(key, {'kind': 'link', 'target': member.linkname})
        else:
            self.set(key, {'kind': 'file'})

    def whiteout_parent(self, parts):
        parent = self.resolve(parts[:-1])
        if parent is None:
            return None
        # A newly created/type-replaced upper directory has no lower children.
        for end in range(1, len(parent) + 1):
            row = self.lookup(parent[:end])
            if not row or row.get('fresh'):
                return None
        return parent


def open_regular(path, *, dir_fd=None):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                 dir_fd=dir_fd)
    try:
        require(stat.S_ISREG(os.fstat(fd).st_mode),
                'capacity_runtime_copy_source_type')
        return fd
    except BaseException:
        os.close(fd)
        raise


def open_material_directory(item):
    source = item.get('source')
    require(isinstance(source, str) and os.path.isabs(source) and
            os.path.realpath(source) == source, 'capacity_runtime_materials')
    fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        actual = os.fstat(fd)
        require(stat.S_ISDIR(actual.st_mode) and actual.st_uid == os.getuid() and
                actual.st_dev == identity_value(item.get('device')) and actual.st_ino == identity_value(item.get('inode')),
                'capacity_runtime_materials')
        return fd
    except BaseException:
        os.close(fd)
        raise


def verified_bytes(file, limit, expected, budget):
    budget.check()
    content = file.read(limit + 1)
    require(len(content) <= limit and
            hashlib.sha256(content).hexdigest() == expected,
            'capacity_runtime_copy_source')
    budget.check()
    return content


def digest_file(file, budget):
    digest = hashlib.sha256()
    while True:
        budget.check()
        block = file.read(1048576)
        if not block:
            break
        digest.update(block)
    file.seek(0)
    return "sha256:" + digest.hexdigest()


def extract_layers(*, layout, layers, diff_ids, destination, scratch, budget):
    require(len(layers) == len(diff_ids) and 0 < len(layers) <= 256,
            "capacity_runtime_oci_layers")
    root = Root(destination)
    total_tar_bytes = 0
    try:
        for layer, diff_id in zip(layers, diff_ids):
            budget.check()
            digest = layer.get("digest", "")
            require(len(digest) == 71 and digest.startswith("sha256:") and
                    all(c in "0123456789abcdef" for c in digest[7:]),
                    "capacity_runtime_oci_digest")
            media = layer.get("mediaType")
            require(media in ("application/vnd.oci.image.layer.v1.tar+gzip",
                              "application/vnd.oci.image.layer.v1.tar",
                              "application/vnd.docker.image.rootfs.diff.tar.gzip"),
                    "capacity_runtime_oci_layer_type")
            directory = os.open(layout, os.O_RDONLY | os.O_DIRECTORY |
                                os.O_NOFOLLOW)
            try:
                blobs = os.open("blobs", os.O_RDONLY | os.O_DIRECTORY |
                                os.O_NOFOLLOW, dir_fd=directory)
                try:
                    sha = os.open("sha256", os.O_RDONLY | os.O_DIRECTORY |
                                  os.O_NOFOLLOW, dir_fd=blobs)
                    try:
                        fd = open_regular(digest[7:], dir_fd=sha)
                    finally:
                        os.close(sha)
                finally:
                    os.close(blobs)
            finally:
                os.close(directory)
            with os.fdopen(fd, "rb") as compressed:
                before = os.fstat(compressed.fileno())
                require(stat.S_ISREG(before.st_mode) and
                        before.st_size == layer.get("size"),
                        "capacity_runtime_oci_blob")
                require(digest_file(compressed, budget) == digest,
                        "capacity_runtime_oci_blob_digest")
                with tempfile.TemporaryFile(dir=scratch) as tar:
                    source = gzip.GzipFile(fileobj=compressed) if media.endswith("gzip") else compressed
                    digest_tar = hashlib.sha256()
                    while True:
                        budget.check()
                        chunk = source.read(1048576)
                        if not chunk:
                            break
                        total_tar_bytes += len(chunk)
                        require(total_tar_bytes <= budget.limit,
                                "capacity_runtime_extract_expansion")
                        digest_tar.update(chunk)
                        tar.write(chunk)
                    if source is not compressed:
                        source.close()
                    require("sha256:" + digest_tar.hexdigest() == diff_id,
                            "capacity_runtime_oci_diff_id")
                    after = os.fstat(compressed.fileno())
                    require((before.st_dev, before.st_ino, before.st_size,
                             before.st_mtime_ns, before.st_ctime_ns) ==
                            (after.st_dev, after.st_ino, after.st_size,
                             after.st_mtime_ns, after.st_ctime_ns),
                            "capacity_runtime_oci_blob_changed")
                    tar.seek(0)
                    with tarfile.open(fileobj=tar, mode="r:") as archive:
                        members = []
                        for member in archive:
                            budget.file(member.size if member.isreg() else 0)
                            parts = name_parts(member.name, allow_empty_root=member.isdir())
                            if not parts:
                                require(member.isdir(), "capacity_runtime_tar_path")
                                continue
                            require(not member.sparse and
                                    (member.isdir() or member.isreg() or
                                     member.issym() or member.islnk()),
                                    "capacity_runtime_tar_type")
                            members.append((member, parts))
                        # Plan the actual effective upper namespace without
                        # prematurely changing any lower alias or directory.
                        whiteouts = []
                        for member, parts in members:
                            if parts[-1].startswith('.wh.'):
                                require(member.isreg() and member.size == 0,
                                        'capacity_runtime_whiteout')
                                require(parts[-1] == '.wh..wh..opq' or
                                        parts[-1][4:] not in ('', '.', '..') and
                                        not parts[-1][4:].startswith('.wh.'),
                                        'capacity_runtime_whiteout')
                                whiteouts.append(parts)
                        # Removing lower aliases can itself change effective
                        # parent resolution. Reach a stable lower-only plan
                        # before performing any mutation, or reject a cycle.
                        plan, seen = whiteouts, set()
                        while True:
                            budget.check()
                            marker = hashlib.sha256(json.dumps(sorted(plan)).encode()).digest()
                            require(marker not in seen, 'capacity_runtime_whiteout_namespace_cycle')
                            seen.add(marker)
                            namespace = LayerNamespace(root, budget, plan)
                            for member, parts in members:
                                if not parts[-1].startswith('.wh.'):
                                    namespace.apply(member, parts)
                            next_plan = []
                            for parts in whiteouts:
                                parent = namespace.whiteout_parent(parts)
                                if parent is not None:
                                    next_plan.append(parent + parts[-1:])
                            if sorted(next_plan) == sorted(plan):
                                break
                            plan = next_plan
                        for parts in plan:
                            root.whiteout(parts)
                        for member, parts in members:
                            budget.check()
                            if not parts[-1].startswith(".wh."):
                                root.write(archive, member, parts, budget)
        root.validate_links(budget)
        return {"layers": len(layers), "files": budget.files,
                "copiedBytes": budget.bytes, "tarBytes": total_tar_bytes,
                "scope": "oci-layout-extraction-only", "formalQualified": False}
    finally:
        root.close()

class LinuxMounts:
    """Private mount FDs, not ancestor pathnames, identify the role filesystem."""
    def __init__(self):
        import ctypes
        self.ctypes = ctypes
        self.libc = ctypes.CDLL(None, use_errno=True)
        self.libc.syscall.restype = ctypes.c_long
        self.libc.mount.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
                                   ctypes.c_ulong, ctypes.c_char_p]
        self.libc.umount2.argtypes = [ctypes.c_char_p, ctypes.c_int]

    @staticmethod
    def path(fd):
        return '/proc/self/fd/' + str(fd)

    def call(self, number, *args):
        result = self.libc.syscall(number, *args)
        require(result >= 0, 'capacity_runtime_mount_fd_api_' +
                str(number) + '_' + str(self.ctypes.get_errno()))
        return result

    def tmpfs(self, capacity, mode):
        c = self.ctypes
        context = self.call(430, c.c_char_p(b'tmpfs'), 1)
        try:
            for key, value in [(b'size', str(capacity).encode()), (b'mode', mode.encode())]:
                self.call(431, context, 1, c.c_char_p(key), c.c_char_p(value), 0)
            self.call(431, context, 6, 0, 0, 0)
            return self.call(432, context, 1, 2 | 4)
        finally:
            os.close(context)

    def attach(self, mount_fd, target_fd):
        c = self.ctypes
        self.call(429, mount_fd, c.c_char_p(b''), target_fd,
                  c.c_char_p(b''), 0x4 | 0x40)

    def clone_readonly(self, root_fd, target_fd):
        c = self.ctypes
        # A recursive private clone retains material/proc submounts. Only its
        # top mount becomes read-only; role data remains a separate RW mount.
        cloned = self.call(428, root_fd, c.c_char_p(b''),
                           1 | os.O_CLOEXEC | 0x1000 | 0x8000)
        try:
            class Attributes(c.Structure):
                _fields_ = [('set', c.c_uint64), ('clear', c.c_uint64),
                            ('propagation', c.c_uint64), ('userns', c.c_uint64)]
            attributes = Attributes(1 | 2 | 4, 0, 0, 0)
            self.call(442, cloned, c.c_char_p(b''), 0x1000,
                      c.byref(attributes), c.sizeof(attributes))
            self.attach(cloned, target_fd)
            return cloned
        except BaseException:
            os.close(cloned)
            raise

    def mount(self, source, destination, flags):
        require(self.libc.mount(os.fsencode(source) if source is not None else None,
                               os.fsencode(destination), None, flags, None) == 0,
                'capacity_runtime_namespace_mount_' + str(self.ctypes.get_errno()))

    def detach(self, mount_fd):
        # The FD names the actual mount even if its task ancestors were renamed.
        self.libc.umount2(os.fsencode(self.path(mount_fd)), 2)


def open_task_directory(configuration):
    directory = configuration.get('directory')
    require(isinstance(directory, str) and os.path.isabs(directory) and
            os.path.realpath(directory) == directory,
            'capacity_runtime_launch_directory')
    parent = os.open(os.path.dirname(directory), os.O_RDONLY | os.O_DIRECTORY |
                     os.O_NOFOLLOW)
    try:
        fd = os.open(os.path.basename(directory), os.O_RDONLY | os.O_DIRECTORY |
                     os.O_NOFOLLOW, dir_fd=parent)
    finally:
        os.close(parent)
    try:
        identity, owner = os.fstat(fd), configuration.get('owner', {})
        require(identity.st_uid == os.getuid() and identity.st_mode & 0o077 == 0 and
                not os.listdir(fd) and identity.st_dev == identity_value(owner.get('device')) and
                identity.st_ino == identity_value(owner.get('inode')),
                'capacity_runtime_launch_directory')
        return fd
    except BaseException:
        os.close(fd)
        raise


# The native launcher enters the exact role cgroup and a private user/mount
# namespace before this entry. No host mount/configuration is modified.
def native_public_origins():
    """Read the only native root authority FD; no JSON caller input can grant."""
    import errno
    import fcntl
    try:
        before = os.fstat(7)
    except OSError as error:
        if error.errno == errno.EBADF:
            return {'schema': 'capacity-native-public-origins-v1', 'origins': [],
                    'rootTrustHash': None, 'topologyHash': None}
        raise
    try:
        require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= 65536 and
                fcntl.fcntl(7, fcntl.F_GETFL) & os.O_ACCMODE == os.O_RDONLY,
                'capacity_runtime_root_grant_fd')
        with os.fdopen(os.dup(7), 'rb') as file:
            content = file.read(65537)
        after = os.fstat(7)
        require(len(content) == before.st_size and
                (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                'capacity_runtime_root_grant_changed')
        trust = json.loads(content)
        require(trust.get('schema') == 'capacity-runtime-trust-v1',
                'capacity_runtime_root_grant_schema')
        item = trust.get('topology')
        if item is None:
            return {'schema': 'capacity-native-public-origins-v1', 'origins': [],
                    'rootTrustHash': hashlib.sha256(content).hexdigest(), 'topologyHash': None}
        require(isinstance(item, dict) and isinstance(item.get('file'), str) and
                os.path.isabs(item['file']) and os.path.realpath(item['file']) == item['file'] and
                isinstance(item.get('sha256'), str) and len(item['sha256']) == 64,
                'capacity_runtime_root_topology')
        with os.fdopen(open_regular(item['file']), 'rb') as file:
            encoded = file.read(131073)
        require(len(encoded) <= 131072 and hashlib.sha256(encoded).hexdigest() == item['sha256'],
                'capacity_runtime_root_topology_changed')
        topology = json.loads(encoded)
        require(topology.get('schema') == 'capacity-formal-topology-v1',
                'capacity_runtime_root_topology')
        origin = topology.get('proxy', {}).get('publicOrigin')
        from urllib.parse import urlsplit
        parsed = urlsplit(origin) if isinstance(origin, str) else None
        require(parsed and parsed.scheme == 'https' and parsed.hostname and
                not parsed.username and not parsed.password and not parsed.path and
                not parsed.query and not parsed.fragment and not any(c.isspace() for c in origin) and
                (parsed.port is None or 1 <= parsed.port <= 65535),
                'capacity_runtime_root_topology_origin')
        return {'schema': 'capacity-native-public-origins-v1', 'origins': [origin],
                'rootTrustHash': hashlib.sha256(content).hexdigest(), 'topologyHash': item['sha256']}
    finally:
        os.close(7)


def enter_remote_pid_namespace(mounts, configuration):
    """Only the fixed broker entry gets an owned PID namespace, never host proc."""
    import select
    require(hasattr(os, 'pidfd_open'), 'capacity_runtime_remote_pidfd_unavailable')
    grace = configuration.get('brokerShutdownMs', 5000)
    require(type(grace) is int and 1000 <= grace <= 30000,
            'capacity_runtime_remote_shutdown_budget')
    parent = os.pidfd_open(os.getpid())
    previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM, signal.SIGINT})
    mask_restored = False
    try:
        require(mounts.libc.unshare(0x20000000) == 0,
                'capacity_runtime_remote_pid_namespace')
        child = os.fork()
        if child:
            stop = [None]
            def forward(number, _frame):
                if stop[0] is None:
                    stop[0] = time.monotonic() + grace / 1000
                try:
                    os.kill(child, number)
                except ProcessLookupError:
                    pass
            signal.signal(signal.SIGTERM, forward)
            signal.signal(signal.SIGINT, forward)
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            mask_restored = True
            while True:
                reaped, status = os.waitpid(child, os.WNOHANG)
                if reaped:
                    result = os.waitstatus_to_exitcode(status)
                    os._exit(result if result >= 0 else 128 - result)
                if stop[0] is not None and time.monotonic() >= stop[0]:
                    try:
                        os.kill(child, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                time.sleep(0.01)
        # Close the fork/prctl race with a real pidfd, not a reusable PID.
        require(mounts.libc.prctl(1, signal.SIGKILL, 0, 0, 0) == 0,
                'capacity_runtime_remote_parent_death')
        poll = select.poll()
        poll.register(parent, select.POLLIN | select.POLLHUP | select.POLLERR)
        require(not poll.poll(0), 'capacity_runtime_remote_parent_lost')
        require(os.getpid() == 1, 'capacity_runtime_remote_pid_namespace')
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        mask_restored = True
    finally:
        if not mask_restored:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        os.close(parent)


def remote_tool_directory(base_fd, parts, budget):
    # Per-file walk only; mount target FDs retain their separate held lifetime.
    fd = os.dup(base_fd)
    try:
        for part in parts:
            try:
                os.mkdir(part, 0o755, dir_fd=fd)
                budget.file(0)
            except FileExistsError:
                pass
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def copy_remote_tools(configuration, root_fd, target_directory, budget):
    """Independently copy the root-approved complete browser/module/DSO bundle."""
    spec = configuration.get('remoteTools')
    if spec is None:
        return
    require(isinstance(spec, dict) and set(spec) == {'manifestFile', 'manifestSha256'},
            'capacity_runtime_remote_tools')
    source = spec['manifestFile']
    require(isinstance(source, str) and os.path.isabs(source) and
            os.path.realpath(source) == source, 'capacity_runtime_remote_tools')
    with os.fdopen(open_regular(source), 'rb') as manifest:
        encoded = verified_bytes(manifest, 4 * 1024**2, spec['manifestSha256'], budget)
    try:
        plan = json.loads(encoded)
    except (ValueError, TypeError):
        raise ExtractionError('capacity_runtime_remote_tools')
    require(isinstance(plan, dict) and set(plan) == {'schema', 'files'} and
            plan['schema'] == 'capacity-remote-tools-v1' and
            isinstance(plan['files'], list) and 0 < len(plan['files']) <= 100000,
            'capacity_runtime_remote_tools')
    # Never merge into or replace an existing B directory.
    app = os.open('app', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
    try:
        os.mkdir('remote-tools', 0o755, dir_fd=app)
        budget.file(0)
        remote_base = os.open('remote-tools', os.O_RDONLY | os.O_DIRECTORY |
                              os.O_NOFOLLOW, dir_fd=app)
    finally:
        os.close(app)
    try:
        seen = set()
        for item in plan['files']:
            budget.check()
            require(isinstance(item, dict) and set(item) ==
                    {'name', 'source', 'sha256', 'bytes', 'executable'},
                    'capacity_runtime_remote_tools')
            parts = name_parts(item['name'])
            require(parts and '/'.join(parts) == item['name'] and item['name'] not in seen and
                    isinstance(item['sha256'], str) and len(item['sha256']) == 64 and
                    all(c in '0123456789abcdef' for c in item['sha256']) and
                    type(item['bytes']) is int and 0 < item['bytes'] <= 2 * 1024**3 and
                    type(item['executable']) is bool and isinstance(item['source'], str) and
                    os.path.isabs(item['source']) and os.path.realpath(item['source']) == item['source'],
                    'capacity_runtime_remote_tools')
            seen.add(item['name'])
            parent = remote_tool_directory(remote_base, parts[:-1], budget)
            source_fd = None
            try:
                source_fd = open_regular(item['source'])
                before = os.fstat(source_fd)
                require(before.st_uid == os.getuid() and before.st_mode & 0o022 == 0 and
                        before.st_size == item['bytes'], 'capacity_runtime_remote_tools')
                budget.file(before.st_size)
                target = os.open(parts[-1], os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                                 os.O_NOFOLLOW, 0o555 if item['executable'] else 0o444,
                                 dir_fd=parent)
                digest = hashlib.sha256()
                copied = 0
                with os.fdopen(target, 'wb') as output:
                    while True:
                        budget.check()
                        block = os.read(source_fd, 1048576)
                        if not block:
                            break
                        copied += len(block)
                        require(copied <= before.st_size,
                                'capacity_runtime_remote_tools_changed')
                        digest.update(block)
                        output.write(block)
                after = os.fstat(source_fd)
                require(copied == before.st_size and digest.hexdigest() == item['sha256'] and
                        (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                        (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) and
                        os.path.realpath(item['source']) == item['source'],
                        'capacity_runtime_remote_tools_changed')
            finally:
                if source_fd is not None:
                    os.close(source_fd)
                os.close(parent)
    finally:
        os.close(remote_base)


def launch(configuration):
    require(configuration.get('schema') == 'capacity-runtime-launch-v1',
            'capacity_runtime_launch_schema')
    entry = configuration.get('entry')
    require(entry in (None, 'restored-report', 'remote-broker'), 'capacity_runtime_launch_entry')
    require(entry == 'remote-broker' or 'remoteTools' not in configuration,
            'capacity_runtime_remote_tools')
    public_origins = native_public_origins()
    capacity = configuration.get('tmpfsBytes')
    require(type(capacity) is int and 1024**2 <= capacity <= 8 * 1024**3,
            'capacity_runtime_launch_budget')
    limits = configuration.get('budget', {})
    budget = Budget(max_bytes=limits.get('maxBytes'), max_files=limits.get('maxFiles'),
                    timeout_seconds=limits.get('timeoutSeconds'))
    signal.signal(signal.SIGTERM, lambda *_: setattr(budget, 'cancelled', True))
    signal.signal(signal.SIGINT, lambda *_: setattr(budget, 'cancelled', True))
    mounts, held, target_paths, tmpfs_mount = LinuxMounts(), [], {}, None
    if entry == 'remote-broker':
        enter_remote_pid_namespace(mounts, configuration)
    try:
        task_fd = open_task_directory(configuration)
        held.append(task_fd)
        tmpfs_mount = mounts.tmpfs(capacity, '0700')
        held.append(tmpfs_mount)
        mounts.attach(tmpfs_mount, task_fd)
        private = os.open('.', os.O_RDONLY | os.O_DIRECTORY, dir_fd=tmpfs_mount)
        held.append(private)
        for name in ['root', 'scratch', 'final']:
            os.mkdir(name, 0o700, dir_fd=private)
        root_fd = os.open('root', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                          dir_fd=private)
        scratch_fd = os.open('scratch', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                             dir_fd=private)
        final_target = os.open('final', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                               dir_fd=private)
        held.extend([root_fd, scratch_fd, final_target])

        def target_directory(parts):
            fd = os.dup(root_fd)
            try:
                for part in parts:
                    budget.check()
                    try:
                        os.mkdir(part, 0o755, dir_fd=fd)
                    except FileExistsError:
                        pass
                    child = os.open(part, os.O_RDONLY | os.O_DIRECTORY |
                                    os.O_NOFOLLOW, dir_fd=fd)
                    os.close(fd)
                    fd = child
                held.append(fd)
                target_paths[fd] = (list(parts), True)
                return fd
            except BaseException:
                os.close(fd)
                raise

        def target_file(parts):
            parent = target_directory(parts[:-1])
            try:
                fd = os.open(parts[-1], os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                             os.O_NOFOLLOW, 0o444, dir_fd=parent)
            except FileExistsError:
                fd = open_regular(parts[-1], dir_fd=parent)
                if os.fstat(fd).st_size:
                    os.close(fd)
                    raise ExtractionError('capacity_runtime_mount_target')
            held.append(fd)
            target_paths[fd] = (list(parts), False)
            return fd

        def reopen_target(original):
            parts, is_directory = target_paths[original]
            fd = os.dup(root_fd)
            try:
                for index, part in enumerate(parts):
                    flag = os.O_DIRECTORY if is_directory or index + 1 < len(parts) else 0
                    child = os.open(part, os.O_RDONLY | os.O_NONBLOCK |
                                    os.O_NOFOLLOW | flag, dir_fd=fd)
                    os.close(fd)
                    fd = child
                held.append(fd)
                return fd
            except BaseException:
                os.close(fd)
                raise

        def bind(source, target, readonly):
            expected = os.stat(source)
            mounts.mount(source, mounts.path(target), 4096)
            actual_fd = reopen_target(target)
            actual = os.fstat(actual_fd)
            require((actual.st_dev, actual.st_ino) == (expected.st_dev, expected.st_ino),
                    'capacity_runtime_mount_identity')
            mounts.mount(None, mounts.path(actual_fd), 4096 | 32 | 2 | 4 | 8 |
                         (1 if readonly else 0))

        extract_layers(layout=configuration['layout'], layers=configuration['layers'],
                       diff_ids=configuration['diffIds'], destination=root_fd,
                       scratch=mounts.path(scratch_fd), budget=budget)
        app_fd = os.open('app', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                         dir_fd=root_fd)
        try:
            ci_fd = os.open('ci', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=app_fd)
            try:
                fd = open_regular('postgres-image-manifest.json', dir_fd=ci_fd)
            finally:
                os.close(ci_fd)
            with os.fdopen(fd, 'rb') as file:
                data = file.read(4 * 1024**2 + 1)
            require(len(data) <= 4 * 1024**2, 'capacity_runtime_business_manifest')
            manifest = json.loads(data)
            require(manifest.get('version') == 1 and
                    manifest.get('revision') == configuration.get('revision') and
                    isinstance(manifest.get('files'), dict),
                    'capacity_runtime_business_manifest')
            for name, expected in manifest['files'].items():
                budget.check()
                parts = name_parts(name)
                require(bool(parts) and isinstance(expected, str) and len(expected) == 64,
                        'capacity_runtime_business_manifest')
                parent = os.dup(app_fd)
                try:
                    for part in parts[:-1]:
                        child = os.open(part, os.O_RDONLY | os.O_DIRECTORY |
                                        os.O_NOFOLLOW, dir_fd=parent)
                        os.close(parent)
                        parent = child
                    fd = open_regular(parts[-1], dir_fd=parent)
                    with os.fdopen(fd, 'rb') as file:
                        require(digest_file(file, budget) == 'sha256:' + expected,
                                'capacity_runtime_business_source')
                finally:
                    os.close(parent)
        finally:
            os.close(app_fd)
        harness = configuration.get('harness', {})
        require(isinstance(harness.get('files'), dict) and
                isinstance(harness.get('directory'), str) and
                os.path.realpath(harness['directory']) == harness['directory'],
                'capacity_runtime_harness_identity')
        harness_target = target_directory(['app', 'tools', 'capacity'])
        for name, expected in harness['files'].items():
            budget.check()
            parts = name_parts(name)
            helper_source = name in (
                'clients/harbor-device/HarborDevice.cs',
                'clients/harbor-device/CapacityDeliveryObservation.cs')
            require((helper_source or len(parts) == 3 and
                     parts[:2] == ['tools', 'capacity'] and
                     parts[-1].endswith(('.mjs', '.py', '.c'))) and
                    isinstance(expected, str) and len(expected) == 64,
                    'capacity_runtime_harness_identity')
            destination = (target_directory(['app', 'clients', 'harbor-device'])
                           if helper_source else harness_target)
            source = os.path.join(harness['directory'], *parts)
            require(os.path.realpath(source) == source, 'capacity_runtime_harness_identity')
            with os.fdopen(open_regular(source), 'rb') as file:
                content = verified_bytes(file, 4 * 1024**2, expected, budget)
            fd = os.open(parts[-1], os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                         os.O_NOFOLLOW, 0o444, dir_fd=destination)
            with os.fdopen(fd, 'wb') as file:
                file.write(content)
        if entry == 'remote-broker':
            copy_remote_tools(configuration, root_fd, target_directory, budget)
        public_bytes = json.dumps(public_origins, separators=(',', ':')).encode()
        budget.file(len(public_bytes))
        # Always generate, including []: a tar-supplied grant is never trusted.
        public_file = os.open('capacity-public-origins.json', os.O_CREAT | os.O_EXCL |
                              os.O_WRONLY | os.O_NOFOLLOW, 0o444, dir_fd=root_fd)
        with os.fdopen(public_file, 'wb') as file:
            file.write(public_bytes)
        native = configuration.get('native', {})
        require(isinstance(native.get('file'), str) and isinstance(native.get('sha256'), str),
                'capacity_runtime_launcher_identity')
        with os.fdopen(open_regular(native['file']), 'rb') as file:
            content = verified_bytes(file, 16 * 1024**2, native['sha256'], budget)
        fd = os.open('capacity-runtime-entry', os.O_CREAT | os.O_EXCL | os.O_WRONLY |
                     os.O_NOFOLLOW, 0o555, dir_fd=root_fd)
        with os.fdopen(fd, 'wb') as file:
            file.write(content)
            os.fchmod(file.fileno(), 0o555)
        materials = configuration.get('materials', [])
        require(isinstance(materials, list) and len(materials) <= 5, 'capacity_runtime_materials')
        allowed = {'data', 'runtime', 'certificates', 'postgres-data', 'postgres-socket'}
        seen, pending = set(), []
        for item in materials:
            name = item.get('name')
            require(name in allowed and name not in seen, 'capacity_runtime_materials')
            seen.add(name)
            target = target_directory(['capacity-material', name])
            source = open_material_directory(item)
            held.append(source)
            pending.append((source, target, name not in {'data', 'runtime'}))
        if entry == 'remote-broker':
            private_proc = target_directory(['proc'])
        else:
            proc_self = target_directory(['proc', 'self'])
            boot = target_file(['proc', 'sys', 'kernel', 'random', 'boot_id'])
        devices = [(name, target_file(['dev', name]))
                   for name in ['null', 'zero', 'random', 'urandom']]
        temporary = target_directory(['tmp'])
        for source, target, readonly in pending:
            bind(mounts.path(source), target, readonly)
        if entry == 'remote-broker':
            require(mounts.libc.mount(b'proc', os.fsencode(mounts.path(private_proc)),
                                     b'proc', 1 | 2 | 4 | 8, b'hidepid=2') == 0,
                    'capacity_runtime_remote_proc_namespace')
        else:
            bind('/proc/self', proc_self, True)
            bind('/proc/sys/kernel/random/boot_id', boot, True)
        for name, target in devices:
            mounts.mount('/dev/' + name, mounts.path(target), 4096)
        temporary_mount = mounts.tmpfs(64 * 1024**2, '1777')
        held.append(temporary_mount)
        mounts.attach(temporary_mount, temporary)
        final_mount = mounts.clone_readonly(root_fd, final_target)
        held.append(final_mount)
        final_root = os.open('.', os.O_RDONLY | os.O_DIRECTORY, dir_fd=final_mount)
        held.append(final_root)
        require((os.fstat(final_root).st_dev, os.fstat(final_root).st_ino) ==
                (os.fstat(root_fd).st_dev, os.fstat(root_fd).st_ino),
                'capacity_runtime_mount_identity')
        budget.check()
        os.fchdir(final_root)
        os.chroot('.')
        os.chdir('/')
        os.close(4)
        if entry == 'remote-broker':
            os.execv('/capacity-runtime-entry', ['/capacity-runtime-entry', '--execute', '--remote-broker'])
        elif entry == 'restored-report':
            os.execv('/capacity-runtime-entry', ['/capacity-runtime-entry', '--execute', '--restored-report'])
        else:
            os.execv('/capacity-runtime-entry', ['/capacity-runtime-entry', '--execute'])
    finally:
        if tmpfs_mount is not None:
            mounts.detach(tmpfs_mount)
        for fd in reversed(held):
            try:
                os.close(fd)
            except OSError:
                pass


if __name__ == '__main__':
    import sys
    if sys.argv[1:] != ['--launch']:
        sys.exit(126)
    try:
        with os.fdopen(os.dup(4), 'rb') as control:
            encoded = control.read(65537)
        require(len(encoded) <= 65536, 'capacity_runtime_launch_frame')
        launch(json.loads(encoded))
    except BaseException as error:
        # Only fixed internal codes; never paths or untrusted exception text.
        code = str(error) if isinstance(error, ExtractionError) else ''
        if not code.startswith('capacity_runtime_') or not all(c.islower() or c.isdigit() or c == '_' for c in code):
            code = 'capacity_runtime_launch_failed'
        sys.stderr.write(code + '\n')
        sys.exit(126)
