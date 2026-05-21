#!/usr/bin/env -S uv run --no-project --script
# /// script
# requires-python = ">=3.11"
# ///
"""ZMTP version probe — connect to a PUSH endpoint and report what
ZMTP version the server is willing to speak.

Default behaviour ("drag-feet"): connect and wait briefly without
writing anything. libzmq-based servers write their 10-byte ZMTP/2.0+
signature eagerly on accept(), so we usually learn "v1 or v2+?"
without committing to a greeting ourselves.

With --probe-all: also open three more connections that send v1, v2,
and v3 greetings respectively, to determine exactly which versions the
server will complete a handshake on (and which it closes on).
"""

from __future__ import annotations

import argparse
import socket
import struct
import sys
from dataclasses import dataclass, field

# === Greetings we can send ===
V1_GREETING = b"\x01\x00"  # anonymous identity frame: length=1, flags=0
V2_GREETING = (
    b"\xff" + b"\x00" * 8 + b"\x7f"  # signature
    + b"\x01"  # revision = 1 (ZMTP/2.0)
    + b"\x07"  # socket type = PULL
)
V3_GREETING = (
    b"\xff" + b"\x00" * 8 + b"\x7f"   # signature
    + b"\x03\x00"                      # major=3 minor=0
    + b"NULL".ljust(20, b"\x00")       # mechanism, null-padded to 20
    + b"\x00"                          # as-server = 0 (we're connecting)
    + b"\x00" * 31                     # filler
)
assert len(V3_GREETING) == 64, len(V3_GREETING)

_V2_SOCKET_TYPES = {
    0: "PAIR", 1: "PUB", 2: "SUB", 3: "REQ", 4: "REP",
    5: "DEALER", 6: "ROUTER", 7: "PULL", 8: "PUSH",
}


@dataclass
class ProbeResult:
    label: str
    status: str
    detail: str = ""
    raw: bytes = field(default_factory=bytes)

    def format(self) -> str:
        parts = [self.status]
        if self.detail:
            parts.append(self.detail)
        if self.raw:
            parts.append(f"bytes={self.raw.hex()}")
        return " | ".join(parts)


def recv_some(conn: socket.socket, want: int, first_timeout: float,
              follow_timeout: float = 0.05) -> bytes:
    """Read up to `want` bytes. First byte may take up to first_timeout;
    subsequent bytes only wait follow_timeout between reads, so once the
    server has stopped writing we return promptly rather than hanging
    for the full first_timeout."""
    buf = b""
    conn.settimeout(first_timeout)
    try:
        chunk = conn.recv(want)
    except (TimeoutError, socket.timeout):
        return buf
    except OSError:
        return buf
    if not chunk:
        return buf
    buf += chunk
    conn.settimeout(follow_timeout)
    while len(buf) < want:
        try:
            chunk = conn.recv(want - len(buf))
        except (TimeoutError, socket.timeout):
            return buf
        except OSError:
            return buf
        if not chunk:
            return buf
        buf += chunk
    return buf


def classify(raw: bytes) -> tuple[str, str]:
    """Identify the ZMTP version from server greeting bytes.
    Returns (status, detail)."""
    if not raw:
        return "no data", "server closed or never wrote"
    if raw[0:1] != b"\xff":
        return "ZMTP/1.0", f"short-length identity (length={raw[0]})"
    if len(raw) < 10:
        return "partial", f"only {len(raw)} bytes; signature incomplete"
    if raw[9:10] != b"\x7f":
        length = struct.unpack(">Q", raw[1:9])[0]
        return "ZMTP/1.0", f"long-length identity (length={length})"
    # ZMTP/2.0+ signature
    if len(raw) < 11:
        return "ZMTP/2.0+", "signature only (revision byte withheld — peer is waiting on us)"
    rev = raw[10]
    if rev == 0x01:
        if len(raw) < 12:
            return "ZMTP/2.0", "revision=1 (socket-type withheld)"
        st = raw[11]
        return "ZMTP/2.0", f"socket-type={st}={_V2_SOCKET_TYPES.get(st, '?')}"
    if len(raw) < 12:
        return f"ZMTP/{rev}.x", f"major={rev} (minor withheld)"
    minor = raw[11]
    bits = [f"major={rev}", f"minor={minor}"]
    if len(raw) >= 32:
        mech = raw[12:32].rstrip(b"\x00").decode("ascii", errors="replace")
        bits.append(f"mechanism={mech!r}")
    if len(raw) >= 33:
        bits.append(f"as-server={raw[32]}")
    return f"ZMTP/{rev}.{minor}", ", ".join(bits)


def probe(
    host: str,
    port: int,
    label: str,
    greeting: bytes,
    connect_timeout: float,
    read_timeout: float,
    read_max: int = 64,
) -> ProbeResult:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(connect_timeout)
    try:
        s.connect((host, port))
    except OSError as e:
        return ProbeResult(label=label, status=f"connect failed: {e}")
    try:
        if greeting:
            try:
                s.sendall(greeting)
            except OSError as e:
                return ProbeResult(label=label, status=f"send failed: {e}")
        raw = recv_some(s, read_max, first_timeout=read_timeout)
        status, detail = classify(raw)
        return ProbeResult(label=label, status=status, detail=detail, raw=raw)
    finally:
        try:
            s.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        s.close()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("host")
    ap.add_argument("port", type=int)
    ap.add_argument(
        "--probe-all", action="store_true",
        help="run silent + send-v1 + send-v2 + send-v3 probes (4 connections)",
    )
    ap.add_argument(
        "--silent-timeout", type=float, default=0.5,
        help="seconds to wait for the server to write in the silent probe "
             "(default: 0.5)",
    )
    ap.add_argument(
        "--probe-timeout", type=float, default=2.0,
        help="seconds to wait for the server's reply after sending a v1/v2/v3 "
             "greeting (default: 2.0)",
    )
    ap.add_argument(
        "--connect-timeout", type=float, default=3.0,
        help="seconds to wait for TCP connect (default: 3.0)",
    )
    args = ap.parse_args(argv)

    probes: list[tuple[str, bytes, float]] = [
        ("silent", b"", args.silent_timeout),
    ]
    if args.probe_all:
        probes.extend([
            ("send v1", V1_GREETING, args.probe_timeout),
            ("send v2", V2_GREETING, args.probe_timeout),
            ("send v3", V3_GREETING, args.probe_timeout),
        ])

    print(f"{args.host}:{args.port} probe results:")
    width = max(len(label) for label, _, _ in probes)
    for label, greeting, read_to in probes:
        r = probe(args.host, args.port, label, greeting,
                  args.connect_timeout, read_to)
        print(f"  {label:<{width}} → {r.format()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
