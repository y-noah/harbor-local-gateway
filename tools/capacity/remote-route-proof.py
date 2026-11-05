"""Fixed read-only IPv4 RTNETLINK observer; never sends a network packet."""
import ipaddress
import json
import os
import socket
import struct
import sys


class RouteError(RuntimeError):
    pass


def require(value):
    if not value:
        raise RouteError('capacity_remote_route_unproven')


def align(value):
    return (value + 3) & ~3


def attribute(kind, payload):
    return struct.pack('=HH', len(payload) + 4, kind) + payload + b'\0' * (align(len(payload) + 4) - len(payload) - 4)


def attributes(value):
    result = {}
    at = 0
    while at < len(value):
        require(at + 4 <= len(value))
        length, kind = struct.unpack_from('=HH', value, at)
        kind &= 0x3fff
        require(length >= 4 and at + length <= len(value) and kind not in result)
        result[kind] = value[at + 4:at + length]
        at += align(length)
    require(at == len(value))
    return result


def exchange(channel, message, payload, flags, sequence, reply_type):
    header = struct.pack('=IHHII', len(payload) + 16, message, flags | 1, sequence, 0)
    channel.sendto(header + payload, (0, 0))
    for _ in range(8):
        packet, peer = channel.recvfrom(65536)
        require(peer[0] == 0 and len(packet) <= 65536)
        at = 0
        while at < len(packet):
            require(at + 16 <= len(packet))
            length, kind, reply_flags, reply_sequence, _pid = struct.unpack_from('=IHHII', packet, at)
            require(length >= 16 and at + length <= len(packet) and reply_sequence == sequence)
            value = packet[at + 16:at + length]
            if kind == 2:
                require(len(value) >= 4 and struct.unpack_from('=i', value)[0] == 0)
            elif kind == reply_type:
                return value
            elif kind == 3:
                raise RouteError('capacity_remote_route_unproven')
            at += align(length)
    raise RouteError('capacity_remote_route_unproven')


def ipv4(value):
    require(value is not None and len(value) == 4)
    return socket.inet_ntop(socket.AF_INET, value)


def number(value):
    require(value is not None and len(value) == 4)
    return struct.unpack('=I', value)[0]


def route(channel, address, sequence, fib=False, uid=None, source_address=None):
    flags = 0x1000 | (0x2000 if fib else 0)
    request = struct.pack('=BBBBBBBBI', socket.AF_INET, 32, 32 if source_address is not None else 0, 0, 0, 0, 0, 0, flags)
    request += attribute(1, socket.inet_pton(socket.AF_INET, address))
    if source_address is not None:
        request += attribute(2, socket.inet_pton(socket.AF_INET, source_address))
    request += attribute(25, struct.pack('=I', os.geteuid() if uid is None else uid))
    value = exchange(channel, 26, request, 0, sequence, 24)
    require(len(value) >= 12)
    family, prefix, _src, _tos, table, protocol, scope, kind, route_flags = struct.unpack_from('=BBBBBBBBI', value)
    require(family == socket.AF_INET and 0 <= prefix <= 32)
    attrs = attributes(value[12:])
    selected_source = ipv4(attrs[2]) if 2 in attrs else ipv4(attrs[7]) if 7 in attrs else None
    require(source_address is None or fib or selected_source == source_address)
    return {'prefix': prefix, 'table': number(attrs[15]) if 15 in attrs else table,
            'scope': scope, 'type': kind, 'flags': route_flags, 'protocol': protocol,
            'interface': number(attrs[4]), 'source': selected_source,
            'gateway': ipv4(attrs[5]) if 5 in attrs else None,
            'destination': ipv4(attrs[1]) if 1 in attrs else '0.0.0.0',
            'complex': any(key in attrs for key in [9, 18, 21, 22, 30])}


def inspect(address, uid=None, source_address=None):
    require(uid is None or type(uid) is int and 0 <= uid <= 0xffffffff)
    require(isinstance(address, str) and (source_address is None or isinstance(source_address, str)))
    target = ipaddress.IPv4Address(address)
    if source_address is not None:
        source_address = str(ipaddress.IPv4Address(source_address))
    with socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, socket.NETLINK_ROUTE) as channel:
        channel.bind((0, 0))
        channel.settimeout(2)
        selected = route(channel, str(target), 1, uid=uid, source_address=source_address)
        matched = route(channel, str(target), 2, fib=True, uid=uid, source_address=source_address)
        require(selected['interface'] == matched['interface'] and selected['table'] == matched['table'] and
                selected['gateway'] == matched['gateway'] and selected['source'] is not None)
        link_request = struct.pack('=BBHiII', 0, 0, 0, selected['interface'], 0, 0)
        link = exchange(channel, 18, link_request, 0, 3, 16)
        require(len(link) >= 16)
        _family, _pad, _type, index, flags, _change = struct.unpack_from('=BBHiII', link)
        require(index == selected['interface'])
        attrs = attributes(link[16:])
        require(3 in attrs)
        name = attrs[3].rstrip(b'\0').decode('ascii')
        require(name == socket.if_indextoname(index) and 0 < len(name) <= 15)
        info = attributes(attrs[18]) if 18 in attrs else {}
        link_kind = info.get(1, b'').rstrip(b'\0').decode('ascii')
        network = ipaddress.IPv4Network((matched['destination'], matched['prefix']), strict=False)
        source = ipaddress.IPv4Address(selected['source'])
        on_link = bool(flags & 1 and not flags & 8 and matched['type'] == 1 and
                       matched['scope'] == 253 and matched['prefix'] > 0 and
                       not selected['complex'] and not matched['complex'] and
                       selected['gateway'] in (None, '0.0.0.0') and target in network and source in network and
                       link_kind in ('', 'vlan', 'bond', 'team'))
        return {'targetAddress': str(target), 'sourceAddress': str(source), 'interface': name,
                'destination': str(network.network_address), 'prefixLength': network.prefixlen,
                'gateway': selected['gateway'], 'onLink': on_link,
                'scope': 'actual-rtnetlink-selected-ipv4-route'}


if __name__ == '__main__':
    try:
        raw = sys.stdin.buffer.read(4097)
        require(len(raw) <= 4096)
        value = json.loads(raw)
        require(isinstance(value, dict) and set(value) == {'targetAddress'} and
                isinstance(value['targetAddress'], str))
        result = inspect(value['targetAddress'])
        sys.stdout.write(json.dumps({'ok': True, 'network': result}, separators=(',', ':')))
    except Exception:
        sys.stdout.write('{"ok":false,"code":"capacity_remote_route_unproven"}')
        sys.exit(1)
