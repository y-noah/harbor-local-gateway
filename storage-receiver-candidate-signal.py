"""Fixed receiver root wrapper; reuse pidfd identity checks, never numeric kill."""
import json
import os
from pathlib import Path
import runpy
import signal
import sys

LIMIT = 524288
ALLOWED = {name: getattr(signal, name) for name in
           ('SIGCONT', 'SIGTERM', 'SIGKILL', 'SIGINT')}


def main():
    if os.getuid() != 0:
        raise ValueError('receiver_candidate_signal_root_required')
    encoded = sys.stdin.buffer.read(LIMIT + 1)
    if len(encoded) > LIMIT:
        raise ValueError('receiver_candidate_signal_frame')
    frame = json.loads(encoded)
    if (not isinstance(frame, dict) or
            set(frame) != {'signal', 'rows', 'expectedUid'} or
            type(frame['expectedUid']) is not int or
            not 1 <= frame['expectedUid'] <= 4294967294 or
            frame['signal'] not in ALLOWED):
        raise ValueError('receiver_candidate_signal_frame')
    shared = runpy.run_path(str(Path(__file__).resolve().parent /
                               'tools' / 'capacity' / 'process-signal.py'))
    handles = shared['open_targets'](frame['rows'], expected_uid=frame['expectedUid'])
    try:
        shared['send_targets'](handles, ALLOWED[frame['signal']])
    finally:
        for fd in handles:
            os.close(fd)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, AttributeError, KeyError, TypeError):
        sys.stderr.write('receiver_candidate_signal_refused\n')
        sys.exit(1)
