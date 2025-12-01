#!/usr/bin/env -S uv run --no-project --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "tqdm",
#     "pyzmq ~=27.1",
#     "rich",
# ]
# ///
import json
import zmq
import time
import sys
import os
from pathlib import Path
from argparse import ArgumentParser

from rich import print
from tqdm import tqdm


def process_headers(series, headers):
    """Process headers from 0mq stream: simplon API 1.8"""

    for j, h in enumerate(headers):
        with open(f"{series}{os.sep}headers.{j}", "wb") as f:
            f.write(h)


def process_image(series, image, frame_id):
    """Process image packet"""

    for j, h in enumerate(image):
        with open(f"{series}{os.sep}{frame_id:06d}.{j}", "wb") as f:
            f.write(h)


def process_term(series, headers):
    with open(f"{series}{os.sep}end", "wb") as f:
        f.write(headers[0])


def main():
    parser = ArgumentParser()
    parser.add_argument(
        "endpoint",
        help="The detector endpoint to connect to",
        default="tcp://127.0.0.1:9998",
        nargs="?",
    )
    parser.add_argument(
        "-d",
        "--dest",
        help="Target folder to write subfolders into",
        type=Path,
        default=Path("dumps"),
    )
    args = parser.parse_args()

    context = zmq.Context()

    print(f"Connecting to data source: {args.endpoint}")

    socket = context.socket(zmq.PULL)
    socket.connect(args.endpoint)

    if args.dest:
        args.dest.mkdir(exist_ok=True)
        os.chdir(args.dest)
        print(f"Writing output to {args.dest}/")

    frames = 0
    expected_images = 0
    progress = None
    series = None
    while True:
        messages = socket.recv_multipart()

        m0 = json.loads(messages[0].decode())

        if "htype" not in m0 or "series" not in m0:
            print("Warning: Got invalid first packet, did we start mid-stream?")

        htype = m0["htype"]

        if series != m0["series"]:
            # Ensure that we make the series dir, even if we start mid-stream
            series = m0["series"]
            Path(f"{series}").mkdir(exist_ok=True)
        series = m0["series"]

        if htype.startswith("dheader"):
            # If we were in the middle of processing something and didn't finish
            if progress is not None:
                print(
                    f"Warning: Early termination of series {series} with {frames} frames and without end packet"
                )
                progress.close()
                progress = None
            t0 = time.time()
            Path(f"{series}").mkdir(exist_ok=True)
            frames = 0
            process_headers(series, messages)
            # Manage progress indicator
            m1 = json.loads(messages[1].decode().replace("'", '"'))
            expected_images = m1["nimages"]
            if progress:
                progress.close()
            progress = tqdm(total=expected_images, leave=False)
        elif htype.startswith("dimage"):
            process_image(series, messages, m0["frame"])
            frames += 1
            if progress:
                progress.update()
        elif htype.startswith("dseries"):
            process_term(series, messages)
            if progress:
                progress.close()
                progress = None
            print(
                f"Series {series} acquisition time: {time.time() - t0:.2f}s for {frames} images"
            )


main()
