# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any

import pytest

from aws_s3_transfer_manager import (
    DownloadStream,
    NotFoundError,
    TransferManager,
)


@pytest.fixture
def key(s3: Any, bucket: str, payload: bytes) -> str:
    s3.put_object(Bucket=bucket, Key="streamed", Body=payload, ContentType="video/mp4")
    return "streamed"


def test_iterate_chunks_in_order(
    tm: TransferManager, bucket: str, key: str, payload: bytes
) -> None:
    stream = tm.download_stream(bucket, key)
    assert isinstance(stream, DownloadStream)

    chunks = list(stream)

    assert len(chunks) > 1, "a 12 MiB object at 5 MiB parts arrives in several chunks"
    assert all(isinstance(chunk, bytes) for chunk in chunks)
    assert b"".join(chunks) == payload
    assert stream.result is not None
    assert stream.result.metadata.size == len(payload)
    assert list(stream) == [], "an exhausted stream stays exhausted"


def test_context_manager_waits_for_metadata(
    tm: TransferManager, bucket: str, key: str, payload: bytes
) -> None:
    with tm.download_stream(bucket, key) as stream:
        assert stream.metadata.size == len(payload)
        assert stream.metadata.content_type == "video/mp4"
        assert stream.result is None
        first = next(stream)
        assert payload.startswith(first)
    assert stream.closed
    assert stream.result is None, "leaving early stops the download"


def test_iterating_a_closed_stream_raises(tm: TransferManager, bucket: str, key: str) -> None:
    stream = tm.download_stream(bucket, key)
    stream.close()
    stream.close()  # idempotent
    with pytest.raises(ValueError, match="closed"):
        next(stream)


def test_missing_object_raises_on_metadata(tm: TransferManager, bucket: str) -> None:
    stream = tm.download_stream(bucket, "missing")
    with pytest.raises(NotFoundError):
        _ = stream.metadata
    with pytest.raises(NotFoundError):
        next(stream)


def test_missing_object_raises_on_enter(tm: TransferManager, bucket: str) -> None:
    with pytest.raises(NotFoundError), tm.download_stream(bucket, "missing"):
        pytest.fail("the block must not run")


def test_ranged_stream(tm: TransferManager, bucket: str, key: str, payload: bytes) -> None:
    data = b"".join(tm.download_stream(bucket, key, range="bytes=10-19"))
    assert data == payload[10:20]


def test_metrics_and_priority(tm: TransferManager, bucket: str, key: str, payload: bytes) -> None:
    with tm.download_stream(bucket, key, priority=200) as stream:
        stream.set_priority(10)
        for _ in stream:
            pass
        assert stream.metrics.bytes_received == len(payload)


def test_repr(tm: TransferManager, bucket: str, key: str) -> None:
    stream = tm.download_stream(bucket, key)
    assert repr(stream) == f"<DownloadStream download stream of s3://{bucket}/{key} (open)>"
    stream.close()
    assert repr(stream).endswith("(closed)>")
