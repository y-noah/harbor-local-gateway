"""Signal exact same-UID process instances; never fall back to numeric kill."""
import errno
import json
import os
import re
import signal
import sys

LIMIT = 524288
ALLOWED = {name: getattr(signal, name) for name in ('SIGSTOP', 'SIGCONT', 'SIGKILL')}


def open_targets(rows, *, expected_uid=None):
    if expected_uid is None:
        expected_uid = os.getuid()
    if (type(expected_uid) is not int or expected_uid < 0 or
            expected_uid != os.getuid() and os.getuid() != 0):
        raise ValueError('capacity_process_signal_identity')
    if not isinstance(rows, list) or len(rows) > 4096:
        raise ValueError('capacity_process_signal_frame')
    handles = []
    try:
        seen = set()
        for row in rows:
            if (not isinstance(row, dict) or set(row) != {'pid', 'uid', 'start'} or
                    type(row['pid']) is not int or row['pid'] < 1 or
                    type(row['uid']) is not int or row['uid'] != expected_uid or
                    not isinstance(row['start'], str) or
                    re.fullmatch(r'[0-9]{1,30}', row['start']) is None or
                    row['pid'] in seen):
                raise ValueError('capacity_process_signal_identity')
            seen.add(row['pid'])
            try:
                fd = os.pidfd_open(row['pid'], 0)
            except ProcessLookupError:
                continue
            handles.append(fd)
            try:
                path = '/proc/' + str(row['pid'])
                uid = os.stat(path, follow_symlinks=False).st_uid
                with open(path + '/stat', encoding='ascii') as source:
                    text = source.read(65536)
                fields = text[text.rfind(')') + 2:].split()
            except (FileNotFoundError, ProcessLookupError):
                os.close(handles.pop())
                continue
            if uid != row['uid'] or len(fields) < 20 or fields[19] != row['start']:
                raise ValueError('capacity_process_signal_identity_changed')
        return handles
    except BaseException:
        for fd in handles:
            os.close(fd)
        raise


def send_targets(handles, selected):
    for fd in handles:
        try:
            signal.pidfd_send_signal(fd, selected)
        except ProcessLookupError:
            pass


def main():
    value = sys.stdin.buffer.read(LIMIT + 1)
    if len(value) > LIMIT:
        raise ValueError('capacity_process_signal_frame')
    frame = json.loads(value)
    if (not isinstance(frame, dict) or set(frame) != {'signal', 'rows'} or
            not isinstance(frame['signal'], str) or frame['signal'] not in ALLOWED):
        raise ValueError('capacity_process_signal_frame')
    handles = open_targets(frame['rows'])
    try:
        send_targets(handles, ALLOWED[frame['signal']])
    finally:
        for fd in handles:
            os.close(fd)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, AttributeError, KeyError):
        # No process IDs, arguments, paths or environment are printed.
        sys.stderr.write('capacity_process_signal_refused\n')
        sys.exit(1)
