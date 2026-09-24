# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Copy files or directories to and from S3, with a progress line.

python examples/cp.py ./local-dir s3://bucket/prefix/ --recursive
python examples/cp.py s3://bucket/key ./local-file
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import TYPE_CHECKING, Any

from aws_s3_transfer_manager import MiB, TransferError, TransferManager

if TYPE_CHECKING:
    from aws_s3_transfer_manager import Transfer


def split_s3_uri(uri: str) -> tuple[str, str] | None:
    """Return ``(bucket, key)`` for an ``s3://`` URI, else ``None``."""
    if not uri.startswith("s3://"):
        return None
    bucket, _, key = uri[len("s3://") :].partition("/")
    return bucket, key


def start(tm: TransferManager, source: str, destination: str, *, recursive: bool) -> Transfer[Any]:
    """Start the transfer from ``source`` to ``destination``."""
    match (split_s3_uri(source), split_s3_uri(destination)):
        case (None, (bucket, key)):
            if recursive:
                return tm.upload_directory(source, bucket, key_prefix=key)
            return tm.upload_file(source, bucket, key)
        case ((bucket, key), None):
            if recursive:
                return tm.download_directory(bucket, destination, key_prefix=key)
            return tm.download_file(bucket, key, destination)
        case _:
            sys.exit("exactly one of SOURCE and DESTINATION must be an s3:// URI")


def main() -> None:
    """Parse arguments and run the copy."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("source")
    parser.add_argument("destination")
    parser.add_argument("--recursive", action="store_true", help="copy a directory or prefix")
    parser.add_argument("--part-size-mib", type=int, help="part size in MiB")
    args = parser.parse_args()

    part_size = args.part_size_mib * MiB if args.part_size_mib else None
    with TransferManager(part_size=part_size) as tm:
        transfer = start(tm, args.source, args.destination, recursive=args.recursive)
        while not transfer.done():
            metrics = transfer.metrics
            seconds = max(metrics.elapsed.total_seconds(), 1e-9)
            rate = metrics.bytes_transferred / MiB / seconds
            total = f" of {metrics.total_bytes / MiB:.0f}" if metrics.total_bytes else ""
            print(
                f"\r{metrics.bytes_transferred / MiB:.0f}{total} MiB ({rate:.0f} MiB/s)",
                end="",
                flush=True,
            )
            time.sleep(0.25)
        try:
            transfer.result()
        except TransferError as error:
            sys.exit(f"\ncopy failed: {error}")
        print(f"\ndone in {transfer.metrics.elapsed.total_seconds():.2f}s")


if __name__ == "__main__":
    main()
