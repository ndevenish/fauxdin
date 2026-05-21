#!/usr/bin/env -S uv run --no-project --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyzmq~=27.1"]
# ///
"""Minimal pyzmq PUSH server for testing — sends 10 64-byte frames
filled with bytes 0x00..0x09, then exits."""

import argparse
import time

import zmq

ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, default=5555)
ap.add_argument("--bind", default="127.0.0.1")
ap.add_argument("--frames", type=int, default=10)
ap.add_argument("--frame-size", type=int, default=64)
ap.add_argument("--startup-delay", type=float, default=1.0,
                help="seconds to wait after bind before sending, so peers can connect")
ap.add_argument("--hold", type=float, default=2.0,
                help="seconds to keep socket open after last frame")
args = ap.parse_args()

ctx = zmq.Context()
sock = ctx.socket(zmq.PUSH)
sock.bind(f"tcp://{args.bind}:{args.port}")
print(f"PUSH bound on {args.bind}:{args.port} (libzmq {zmq.zmq_version()}, pyzmq {zmq.pyzmq_version()})",
      flush=True)

time.sleep(args.startup_delay)

for i in range(args.frames):
    body = bytes([i & 0xff]) * args.frame_size
    sock.send(body)
    print(f"sent frame {i}: {args.frame_size} bytes of 0x{i & 0xff:02x}", flush=True)
    time.sleep(0.05)

time.sleep(args.hold)
print("done", flush=True)
