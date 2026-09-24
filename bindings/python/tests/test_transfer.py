# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""The Transfer handle: waiting, status, progress, priority, cancellation; and manager lifecycle."""

from __future__ import annotations

import _thread
import concurrent.futures
import threading
import time
import typing
from typing import TYPE_CHECKING, Any

import pytest

from aws_s3_transfer_manager import (
    InvalidInputError,
    MiB,
    NotFoundError,
    Transfer,
    TransferCancelledError,
    TransferManager,
    TransferStatus,
    UploadResult,
)

from helpers import SlowReader, SlowWriter

if TYPE_CHECKING:
    from pathlib import Path


def wait_until(condition: Any, timeout: float = 10) -> None:
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            pytest.fail("condition not reached in time")
        time.sleep(0.01)


def test_lifecycle_of_a_successful_transfer(tm: TransferManager, bucket: str) -> None:
    transfer = tm.upload_bytes(b"data", bucket, "k")
    assert isinstance(transfer, Transfer)

    result = transfer.result(timeout=30)

    assert transfer.done()
    assert not transfer.cancelled()
    assert transfer.status is TransferStatus.COMPLETED
    assert transfer.exception() is None
    assert transfer.result() is result, "the result is computed once"
    assert transfer.cancel() is False, "a finished transfer cannot be cancelled"
    assert transfer.metrics.bytes_sent == 4
    assert transfer.metrics.total_bytes == 4
    assert transfer.metrics.elapsed.total_seconds() > 0


def test_failed_transfer(tm: TransferManager, bucket: str) -> None:
    transfer = tm.download_bytes(bucket, "missing")
    error = transfer.exception(timeout=30)
    assert isinstance(error, NotFoundError)
    assert transfer.status is TransferStatus.FAILED
    with pytest.raises(NotFoundError) as excinfo:
        transfer.result()
    assert excinfo.value is error


def test_result_timeout_leaves_transfer_running(tm: TransferManager, bucket: str) -> None:
    transfer = tm.upload_fileobj(SlowReader(total=40 * MiB), bucket, "slow")
    with pytest.raises(TimeoutError):
        transfer.result(timeout=0.05)
    assert not transfer.done()
    assert transfer.status is TransferStatus.ACTIVE
    assert transfer.cancel()


def test_cancel_upload(tm: TransferManager, s3: Any, bucket: str) -> None:
    reader = SlowReader(total=200 * MiB)
    transfer = tm.upload_fileobj(reader, bucket, "cancelled")
    wait_until(lambda: reader.reads > 0)

    assert transfer.cancel() is True

    with pytest.raises(TransferCancelledError, match="cancelled"):
        transfer.result(timeout=30)
    assert transfer.cancelled()
    assert transfer.status is TransferStatus.CANCELLED
    assert s3.list_multipart_uploads(Bucket=bucket).get("Uploads", []) == [], "the MPU is aborted"
    assert "Contents" not in s3.list_objects_v2(Bucket=bucket)


def test_cancel_download_to_fileobj(
    tm: TransferManager, s3: Any, bucket: str, payload: bytes, tmp_path: Path
) -> None:
    s3.put_object(Bucket=bucket, Key="big", Body=payload * 4)
    writer = SlowWriter()
    transfer = tm.download_fileobj(bucket, "big", writer)
    try:
        assert writer.writing.wait(timeout=10)
        assert transfer.cancel()
    finally:
        writer.released.set()
    with pytest.raises(TransferCancelledError):
        transfer.result(timeout=30)
    assert len(writer.written) < len(payload) * 4


def test_cancel_download_file_removes_temporary_file(
    tm: TransferManager, s3: Any, bucket: str, payload: bytes, tmp_path: Path
) -> None:
    s3.put_object(Bucket=bucket, Key="big", Body=payload * 8)
    transfer = tm.download_file(bucket, "big", tmp_path / "out.bin")
    transfer.cancel()
    with pytest.raises(TransferCancelledError):
        transfer.result(timeout=30)
    assert list(tmp_path.iterdir()) == []


def test_metrics_progress(tm: TransferManager, bucket: str) -> None:
    reader = SlowReader(total=16 * MiB, delay=0.002)
    transfer = tm.upload_fileobj(reader, bucket, "progress")
    seen = []
    while not transfer.done():
        seen.append(transfer.metrics.bytes_sent)
        time.sleep(0.01)
    transfer.result()
    assert seen == sorted(seen), "progress never goes backwards"
    assert transfer.metrics.bytes_sent == 16 * MiB


def test_set_priority(tm: TransferManager, bucket: str) -> None:
    transfer = tm.upload_bytes(b"x", bucket, "k", priority=255)
    transfer.set_priority(1)
    with pytest.raises(InvalidInputError):
        transfer.set_priority(0)
    transfer.result()


def test_many_concurrent_transfers(tm: TransferManager, s3: Any, bucket: str) -> None:
    transfers = [tm.upload_bytes(f"object {i}".encode(), bucket, f"many/{i}") for i in range(200)]
    results = [t.result(timeout=60) for t in transfers]
    assert all(isinstance(r, UploadResult) for r in results)
    listed = s3.list_objects_v2(Bucket=bucket, Prefix="many/")
    assert listed["KeyCount"] == 200


def test_transfers_from_many_threads(tm: TransferManager, bucket: str) -> None:
    def roundtrip(i: int) -> bytes:
        tm.upload_bytes(str(i).encode(), bucket, f"threads/{i}").result()
        return tm.download_bytes(bucket, f"threads/{i}").result()

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
        assert list(pool.map(roundtrip, range(64))) == [str(i).encode() for i in range(64)]


def test_dropped_transfers_still_complete(tm_kwargs: dict[str, Any], s3: Any, bucket: str) -> None:
    with TransferManager(**tm_kwargs) as tm:
        for i in range(10):
            tm.upload_bytes(b"fire and forget", bucket, f"dropped/{i}")
    # Leaving the block waited for them.
    assert s3.list_objects_v2(Bucket=bucket, Prefix="dropped/")["KeyCount"] == 10


def test_exiting_on_error_cancels_transfers(tm_kwargs: dict[str, Any], bucket: str) -> None:
    tm = TransferManager(**tm_kwargs)
    transfer = tm.upload_fileobj(SlowReader(total=200 * MiB), bucket, "interrupted")
    with pytest.raises(LookupError), tm:
        raise LookupError
    assert transfer.cancelled()


def test_closed_manager_rejects_transfers(tm_kwargs: dict[str, Any], bucket: str) -> None:
    tm = TransferManager(**tm_kwargs)
    tm.close()
    tm.close()  # idempotent
    assert tm.closed
    assert repr(tm).endswith(", closed>")
    with pytest.raises(RuntimeError, match="closed"):
        tm.upload_bytes(b"x", bucket, "k")


def test_close_with_cancel(tm_kwargs: dict[str, Any], bucket: str) -> None:
    tm = TransferManager(**tm_kwargs)
    transfer = tm.upload_fileobj(SlowReader(total=200 * MiB), bucket, "k")
    tm.close(cancel=True)
    assert transfer.cancelled()


def test_repr_and_generic_alias(tm: TransferManager, bucket: str) -> None:
    transfer = tm.upload_bytes(b"abc", bucket, "k")
    transfer.result()
    assert repr(transfer) == f"<Transfer upload of 3 bytes to s3://{bucket}/k (completed)>"
    alias = Transfer[UploadResult]
    assert typing.get_origin(alias) is Transfer
    assert typing.get_args(alias) == (UploadResult,)


def test_keyboard_interrupt_while_waiting(tm: TransferManager, bucket: str) -> None:
    transfer = tm.upload_fileobj(SlowReader(total=200 * MiB), bucket, "interrupted")
    timer = threading.Timer(0.2, _thread.interrupt_main)
    timer.start()
    started = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        transfer.result(timeout=30)
    assert time.monotonic() - started < 5, "Ctrl+C is handled promptly, not after the transfer"
    assert transfer.cancel()
