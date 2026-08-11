#!/usr/bin/env python3
"""Kill one already-attested Linux process through a pidfd, never os.kill(pid)."""
import json
import os
import pathlib
import select
import signal
import sys


def start_token(pid: int) -> str:
    return pathlib.Path(f"/proc/{pid}/stat").read_text().split()[21]


if len(sys.argv) != 4 or not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
    raise SystemExit("usage: chaos-pidfd-kill.py PID QEMU_BINARY START_TOKEN (Linux pidfd required)")
pid, binary, expected_start = int(sys.argv[1]), sys.argv[2], sys.argv[3]
fd = os.pidfd_open(pid)
try:
    if os.path.realpath(f"/proc/{pid}/exe") != binary or start_token(pid) != expected_start:
        raise SystemExit("pidfd target identity changed")
    signal.pidfd_send_signal(fd, signal.SIGKILL)
    if not select.select([fd], [], [], 2.0)[0]:
        raise SystemExit("pidfd target did not exit after SIGKILL")
    print(json.dumps({"schema_version": 1, "kind": "pidfd-sigkill", "pid": pid, "qemu_binary": binary, "start_token": expected_start}))
finally:
    os.close(fd)
