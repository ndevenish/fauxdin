#!/usr/bin/env -S uv run --no-project --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pyzmq ~=27.1",
# ]
# ///
import zmq

context = zmq.Context()
socket = context.socket(zmq.PUSH)
socket.bind("tcp://127.0.0.1:9999")
socket.send(b"somthing")
