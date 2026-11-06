#!/usr/bin/python3
"""Root-controlled Unix-to-legacy TCP bridge preserving the actual peer IP.

The host controller must independently install and verify transparent return
routing. This bridge never edits routes, Docker state, or source supervision.
"""
import asyncio
import ipaddress
import json
import os
from pathlib import Path
import select
import re
import socket
import stat
import struct

PROFILE = Path('/etc/harbor-gateway-legacy-transparent.json')
ADDRESS_HEADER = b'x-harbor-entry-client-address'


def require(value, code):
    if not value:
        raise ValueError('legacy_transparent_' + code)


def root_directory(path):
    for parent in [path, *path.parents]:
        info = parent.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and not info.st_mode & 0o022, 'directory_untrusted')


def configuration(value):
    required = {'version', 'socketPath', 'sourceAddress', 'sourcePort', 'maxConnections', 'maxHeaderBytes', 'headerTimeoutMs', 'connectTimeoutMs'}
    require(isinstance(value, dict) and required <= set(value) <= required | {'legacyEffectivePeers'}, 'configuration')
    require(type(value['version']) is int and value['version'] == 1, 'configuration')
    path = value['socketPath']
    require(isinstance(path, str) and path.startswith('/run/') and str(Path(path)) == path and len(os.fsencode(path)) < 108 and '\0' not in path, 'socket_path')
    address = ipaddress.IPv4Address(value['sourceAddress'])
    require(not address.is_unspecified and not address.is_multicast and not address.is_loopback, 'source_address')
    require(type(value['sourcePort']) is int and 1 <= value['sourcePort'] <= 65535, 'source_port')
    for name, lower, upper in [('maxConnections', 1, 65535), ('maxHeaderBytes', 1024, 1048576), ('headerTimeoutMs', 1, 30000), ('connectTimeoutMs', 1, 30000)]:
        require(type(value[name]) is int and lower <= value[name] <= upper, 'configuration')
    rules = value.get('legacyEffectivePeers', [])
    require(isinstance(rules, list) and len(rules) <= 16, 'peer_mapping')
    networks = []
    for rule in rules:
        require(isinstance(rule, dict) and set(rule) == {'clientNetwork', 'peerAddress', 'witnessSha256'}, 'peer_mapping')
        network = ipaddress.ip_network(rule['clientNetwork'], strict=True)
        peer = ipaddress.IPv4Address(rule['peerAddress'])
        require(not peer.is_loopback and not peer.is_unspecified and not peer.is_multicast, 'peer_mapping')
        require(network.version == 6 or network.subnet_of(ipaddress.ip_network('127.0.0.0/8')), 'peer_mapping_public_ipv4')
        require(isinstance(rule['witnessSha256'], str) and re.fullmatch('[a-f0-9]{64}', rule['witnessSha256']), 'peer_mapping_witness')
        require(not any(network.version == other.version and network.overlaps(other) for other in networks), 'peer_mapping_overlap')
        networks.append(network)
    return {**value, 'legacyEffectivePeers': [dict(rule) for rule in rules]}


def load_configuration():
    require(os.geteuid() == 0, 'root_required')
    root_directory(PROFILE.parent)
    descriptor = os.open(PROFILE, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(descriptor)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o600 and info.st_nlink == 1 and info.st_size <= 8192, 'profile_untrusted')
        body = os.read(descriptor, 8193)
        require(len(body) <= 8192, 'profile_budget')
        return configuration(json.loads(body))
    finally:
        os.close(descriptor)


def client_address(head, mappings=()):
    rows = head[:-4].split(b'\r\n')
    values = []
    for row in rows[1:]:
        require(b':' in row and not row.startswith((b' ', b'\t')), 'header_invalid')
        name, value = row.split(b':', 1)
        if name.lower() == ADDRESS_HEADER:
            values.append(value.strip())
    require(len(values) == 1, 'identity_invalid')
    address = ipaddress.ip_address(values[0].decode('ascii'))
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        address = address.ipv4_mapped
    require(not address.is_unspecified and not address.is_multicast, 'identity_unroutable')
    if address.version == 6 or address.is_loopback:
        for rule in mappings:
            network = ipaddress.ip_network(rule['clientNetwork'])
            if address.version == network.version and address in network:
                # This preserves the witnessed old docker-proxy peer. It does
                # not claim the IPv4-only legacy server observed an IPv6 IP.
                return rule['peerAddress']
        require(False, 'identity_unwitnessed_proxy')
    return str(address)


class LegacyTransparentBridge:
    def __init__(self, config, reload_configuration=None):
        self.config = configuration(config)
        self.reload_configuration = reload_configuration
        self.server = None
        self.tasks = set()
        self.closed = False
        self.identity = None

    async def start(self):
        require(os.geteuid() == 0 and self.server is None and not self.closed, 'lifecycle')
        path = Path(self.config['socketPath'])
        root_directory(path.parent)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            # A stale/colliding path fails closed; never unlink another owner.
            old_mask = os.umask(0o177)
            try:
                listener.bind(str(path))
            finally:
                os.umask(old_mask)
            info = path.lstat()
            self.identity = (info.st_dev, info.st_ino)
            listener.listen(self.config['maxConnections'])
            listener.setblocking(False)
            self.server = await asyncio.start_unix_server(self.accept, sock=listener, limit=self.config['maxHeaderBytes'])
        except BaseException:
            listener.close()
            self.remove_socket()
            raise
        return self

    def accept(self, reader, writer):
        if self.closed or len(self.tasks) >= self.config['maxConnections']:
            writer.close()
            return
        try:
            selected = self.reload_configuration() if self.reload_configuration else self.config
            mutable = {'sourceAddress', 'sourcePort', 'legacyEffectivePeers'}
            require({k: v for k, v in selected.items() if k not in mutable} == {k: v for k, v in self.config.items() if k not in mutable}, 'profile_identity_changed')
        except (OSError, ValueError, KeyError):
            writer.close()
            return
        task = asyncio.create_task(self.forward(reader, writer, selected))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def forward(self, reader, writer, config=None):
        config = self.config if config is None else config
        upstream = None
        raw = None
        copies = []
        phase = None
        cancelled = None
        monitor = None
        loop = asyncio.get_running_loop()
        disconnected = asyncio.Event()
        try:
            credentials = writer.get_extra_info('socket').getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize('3i'))
            require(struct.unpack('3i', credentials)[1] == 0, 'peer_untrusted')
            # RDHUP observes EOF even if unread upload bytes have paused the
            # StreamReader transport. A parallel reader would consume the body
            # or require buffering while the upstream connection is pending.
            monitor = select.epoll()
            monitor.register(writer.get_extra_info('socket').fileno(), select.EPOLLRDHUP | select.EPOLLHUP | select.EPOLLERR)

            def entry_closed():
                if writer.is_closing() or monitor.poll(0):
                    disconnected.set()
                return disconnected.is_set()

            loop.add_reader(monitor.fileno(), entry_closed)
            cancelled = asyncio.create_task(disconnected.wait())
            head = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), config['headerTimeoutMs'] / 1000)
            require(len(head) <= config['maxHeaderBytes'], 'header_budget')
            address = client_address(head, config['legacyEffectivePeers'])
            if entry_closed():
                return
            raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw.setsockopt(socket.SOL_IP, socket.IP_TRANSPARENT, 1)
            raw.bind((address, 0))
            raw.setblocking(False)
            async def connect():
                nonlocal raw, upstream
                await asyncio.wait_for(loop.sock_connect(raw, (config['sourceAddress'], config['sourcePort'])), config['connectTimeoutMs'] / 1000)
                if entry_closed():
                    return None
                remote_reader, upstream = await asyncio.open_connection(sock=raw)
                raw = None
                return remote_reader

            phase = asyncio.create_task(connect())
            await asyncio.wait([phase, cancelled], return_when=asyncio.FIRST_COMPLETED)
            if entry_closed():
                return
            remote_reader = await phase
            if entry_closed():
                return
            upstream.write(head)
            await upstream.drain()

            async def copy(source, destination):
                while True:
                    block = await source.read(65536)
                    if not block:
                        return
                    destination.write(block)
                    await destination.drain()

            copies = [asyncio.create_task(copy(reader, upstream)), asyncio.create_task(copy(remote_reader, writer))]
            await asyncio.wait([*copies, cancelled], return_when=asyncio.FIRST_COMPLETED)
        except (OSError, ValueError, UnicodeError, asyncio.IncompleteReadError, asyncio.LimitOverrunError, asyncio.TimeoutError):
            # Never retry: the legacy application may already have committed.
            pass
        finally:
            tasks = [task for task in [*copies, phase, cancelled] if task is not None]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if monitor is not None:
                loop.remove_reader(monitor.fileno())
                monitor.close()
            for stream in [upstream, writer]:
                if stream is not None:
                    stream.close()
            if raw is not None:
                raw.close()

    def remove_socket(self):
        if self.identity is not None:
            path = Path(self.config['socketPath'])
            try:
                info = path.lstat()
                if stat.S_ISSOCK(info.st_mode) and (info.st_dev, info.st_ino) == self.identity:
                    path.unlink()
            except FileNotFoundError:
                pass
            self.identity = None

    async def close(self):
        self.closed = True
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.remove_socket()


async def main():
    import signal
    config = load_configuration()
    bridge = await LegacyTransparentBridge(config, reload_configuration=load_configuration).start()
    stopped = asyncio.Event()
    for signum in [signal.SIGINT, signal.SIGTERM]:
        asyncio.get_running_loop().add_signal_handler(signum, stopped.set)
    try:
        await stopped.wait()
    finally:
        await bridge.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception:
        raise SystemExit('Legacy transparent bridge failed closed; source unchanged')
