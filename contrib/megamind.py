#!/usr/bin/env -S uv run --no-project --script
# /// script
# requires-python = ">=3.11"
# ///
"""ZMTP/1.0 PUSH server for testing client-library compatibility.

Speaks raw ZMTP/1.0 on the wire — deliberately not via pyzmq, which uses
whatever libzmq it links against (usually 4.x / ZMTP 3.x).

Per connection:

  1. Sends a ZMTP/1.0 greeting: an anonymous identity frame (`\\x01\\x00`).
  2. Reads enough bytes from the client to identify what ZMTP version it
     was trying to negotiate, and logs it. Anything other than 1.0 is
     warned about — that is the question this script exists to answer.
  3. Pushes frames. Default payload is 256 single-frame messages of 512
     bytes each, body filled with byte N for N in 0..255. 512 is well
     past the v1 short/long length boundary (254), so every frame
     exercises the long-length encoding.

Always speaks 1.0 in both directions, regardless of what the peer claims.
Whether the client gracefully downgrades is part of what we're testing.
"""

from __future__ import annotations

import argparse
import logging
import socket
import struct
import sys
import threading
import time
from dataclasses import dataclass

LOG = logging.getLogger("megamind")

# ZMTP/1.0 frame: length + flags + body
#   length: 1 byte if < 255, else 0xff + 8-byte big-endian uint64
#   length includes the flags byte, so body of N bytes ⇒ length = N + 1
#   flags byte: bit 0 = MORE
FLAG_MORE = 0x01


def encode_v1_frame(body: bytes, more: bool = False) -> bytes:
    flags = FLAG_MORE if more else 0x00
    length = len(body) + 1
    if length < 0xFF:
        return bytes([length, flags]) + body
    return b"\xff" + struct.pack(">Q", length) + bytes([flags]) + body


@dataclass
class PeerInfo:
    label: str
    raw: bytes

    def __str__(self) -> str:
        return f"{self.label} (greeting bytes: {self.raw.hex()})"


def recv_exact(conn: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = conn.recv(n - len(buf))
        if not chunk:
            raise ConnectionError(
                f"peer closed mid-greeting (got {len(buf)}/{n} bytes: {buf.hex()})"
            )
        buf += chunk
    return buf


def detect_peer_version(conn: socket.socket) -> PeerInfo:
    """Classify the peer's ZMTP version from its first bytes.

    ZMTP/2.0+ signature is `\\xff <8 zero bytes> \\x7f <revision> ...`.
    Anything else is 1.0. We read only as many bytes as needed to
    classify; the rest of the peer's handshake (if any) is left in the
    socket — we don't care about it, since we're going to keep speaking
    1.0 regardless.
    """
    b0 = recv_exact(conn, 1)
    if b0 != b"\xff":
        return PeerInfo(label="ZMTP/1.0 (short-length identity)", raw=b0)
    rest = recv_exact(conn, 9)
    raw = b0 + rest
    if rest[8:9] != b"\x7f":
        return PeerInfo(label="ZMTP/1.0 (long-length identity)", raw=raw)
    rev = recv_exact(conn, 1)
    raw += rev
    if rev == b"\x01":
        return PeerInfo(label="ZMTP/2.0", raw=raw)
    minor = recv_exact(conn, 1)
    raw += minor
    return PeerInfo(label=f"ZMTP/{rev[0]}.{minor[0]}", raw=raw)


def default_payloads(frame_size: int):
    for n in range(256):
        yield bytes([n]) * frame_size


def serve_client(
    conn: socket.socket,
    addr,
    frame_size: int,
    interval: float,
    hold: float,
) -> None:
    LOG.info("[%s] connected", addr)
    try:
        greeting = encode_v1_frame(b"", more=False)
        conn.sendall(greeting)
        LOG.debug("[%s] sent v1 greeting: %s", addr, greeting.hex())

        peer = detect_peer_version(conn)
        level = logging.INFO if peer.label.startswith("ZMTP/1.0") else logging.WARNING
        LOG.log(level, "[%s] peer attempted %s", addr, peer)

        sent = 0
        for body in default_payloads(frame_size):
            frame = encode_v1_frame(body, more=False)
            try:
                conn.sendall(frame)
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                LOG.warning(
                    "[%s] peer hung up after %d/256 frames: %s", addr, sent, e
                )
                return
            sent += 1
            LOG.debug(
                "[%s] -> frame %d/256 (%d-byte body, fill=0x%02x)",
                addr,
                sent,
                len(body),
                body[0],
            )
            if interval > 0:
                time.sleep(interval)

        LOG.info("[%s] sent all 256 frames", addr)
        if hold > 0:
            time.sleep(hold)
    except ConnectionError as e:
        LOG.warning("[%s] %s", addr, e)
    except Exception:
        LOG.exception("[%s] unexpected error", addr)
    finally:
        try:
            conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        conn.close()
        LOG.info("[%s] disconnected", addr)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--bind", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5555)
    ap.add_argument(
        "--frame-size",
        type=int,
        default=512,
        help="bytes per frame body (default: 512 — past the v1 "
        "short-length cutoff of 254 so every frame is long-encoded)",
    )
    ap.add_argument(
        "--interval",
        type=float,
        default=0.0,
        help="seconds between frames (default: 0)",
    )
    ap.add_argument(
        "--hold",
        type=float,
        default=0.5,
        help="seconds to hold the socket open after the last frame so "
        "the client can drain (default: 0.5)",
    )
    ap.add_argument(
        "--once",
        action="store_true",
        help="serve one client, then exit",
    )
    ap.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="-v: INFO, -vv: DEBUG",
    )
    args = ap.parse_args(argv)

    level = logging.WARNING
    if args.verbose == 1:
        level = logging.INFO
    elif args.verbose >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((args.bind, args.port))
    srv.listen(8)
    LOG.warning("listening on %s:%d (ZMTP/1.0 PUSH)", args.bind, args.port)

    try:
        while True:
            conn, addr = srv.accept()
            t = threading.Thread(
                target=serve_client,
                args=(conn, addr, args.frame_size, args.interval, args.hold),
                daemon=True,
            )
            t.start()
            if args.once:
                t.join()
                return 0
    except KeyboardInterrupt:
        LOG.warning("interrupted")
    finally:
        srv.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
