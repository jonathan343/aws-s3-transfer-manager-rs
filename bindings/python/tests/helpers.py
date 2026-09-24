# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Test helpers shared across modules."""

from __future__ import annotations

import threading
import time
from typing import Any


def get_object(s3: Any, bucket: str, key: str) -> bytes:
    body: bytes = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return body


class SlowReader:
    """A non-seekable binary stream that yields data slowly, to keep uploads in flight."""

    def __init__(self, total: int, chunk: int = 256 * 1024, delay: float = 0.01) -> None:
        self.remaining = total
        self.chunk = chunk
        self.delay = delay
        self.reads = 0

    def read(self, size: int = -1) -> bytes:
        time.sleep(self.delay)
        n = min(self.remaining, self.chunk if size < 0 else min(size, self.chunk))
        self.remaining -= n
        self.reads += 1
        return b"x" * n


class SlowWriter:
    """A binary sink whose first write blocks until released, to keep downloads in flight."""

    def __init__(self) -> None:
        self.released = threading.Event()
        self.writing = threading.Event()
        self.written = bytearray()

    def write(self, data: bytes) -> int:
        self.writing.set()
        self.released.wait(timeout=10)
        self.written += data
        return len(data)
