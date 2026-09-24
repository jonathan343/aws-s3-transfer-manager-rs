# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""asyncio integration: awaiting transfers, async context managers, and async iteration."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from aws_s3_transfer_manager import (
    MiB,
    NotFoundError,
    TransferCancelledError,
    TransferManager,
    TransferStatus,
    UploadResult,
)

from helpers import SlowReader

pytestmark = pytest.mark.asyncio


async def test_await_transfers(tm: TransferManager, bucket: str, payload: bytes) -> None:
    result = await tm.upload_bytes(payload, bucket, "awaited")
    assert isinstance(result, UploadResult)
    assert await tm.download_bytes(bucket, "awaited") == payload


async def test_await_twice_and_mix_with_result(tm: TransferManager, bucket: str) -> None:
    transfer = tm.upload_bytes(b"x", bucket, "twice")
    first = await transfer
    assert await transfer is first
    assert transfer.result() is first


async def test_gather(tm: TransferManager, bucket: str) -> None:
    uploads = [tm.upload_bytes(str(i).encode(), bucket, f"gather/{i}") for i in range(50)]
    await asyncio.gather(*uploads)
    downloads = await asyncio.gather(*(tm.download_bytes(bucket, f"gather/{i}") for i in range(50)))
    assert downloads == [str(i).encode() for i in range(50)]


async def test_await_raises_transfer_errors(tm: TransferManager, bucket: str) -> None:
    with pytest.raises(NotFoundError):
        await tm.download_bytes(bucket, "missing")


async def test_timeout_cancels_the_transfer(tm: TransferManager, bucket: str) -> None:
    transfer = tm.upload_fileobj(SlowReader(total=200 * MiB), bucket, "timed-out")
    # `asyncio.TimeoutError` is the builtin `TimeoutError` from Python 3.11 on.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(transfer, timeout=0.2)
    with pytest.raises(TransferCancelledError):
        await asyncio.wait_for(asyncio.shield(transfer), timeout=30)
    assert transfer.status is TransferStatus.CANCELLED


async def test_shield_protects_the_transfer(tm: TransferManager, bucket: str) -> None:
    transfer = tm.upload_fileobj(SlowReader(total=6 * MiB, delay=0.005), bucket, "shielded")
    # `asyncio.TimeoutError` is the builtin `TimeoutError` from Python 3.11 on.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(transfer), timeout=0.05)
    assert isinstance(await transfer, UploadResult)


async def test_async_context_manager(tm_kwargs: dict[str, Any], s3: Any, bucket: str) -> None:
    async with TransferManager(**tm_kwargs) as tm:
        for i in range(5):
            tm.upload_bytes(b"async", bucket, f"async-cm/{i}")
    assert tm.closed
    assert s3.list_objects_v2(Bucket=bucket, Prefix="async-cm/")["KeyCount"] == 5


async def test_async_context_manager_cancels_on_error(
    tm_kwargs: dict[str, Any], bucket: str
) -> None:
    tm = TransferManager(**tm_kwargs)
    transfer = tm.upload_fileobj(SlowReader(total=200 * MiB), bucket, "k")
    with pytest.raises(KeyError):
        async with tm:
            raise KeyError
    assert transfer.cancelled()


async def test_aclose(tm_kwargs: dict[str, Any], bucket: str) -> None:
    tm = TransferManager(**tm_kwargs)
    transfer = tm.upload_bytes(b"x", bucket, "k")
    await tm.aclose()
    assert transfer.done()
    assert tm.closed


async def test_async_iteration(tm: TransferManager, s3: Any, bucket: str, payload: bytes) -> None:
    s3.put_object(Bucket=bucket, Key="streamed", Body=payload)
    async with tm.download_stream(bucket, "streamed") as stream:
        assert stream.metadata.size == len(payload)
        chunks = [chunk async for chunk in stream]
    assert b"".join(chunks) == payload
    assert stream.result is not None


async def test_async_stream_errors(tm: TransferManager, bucket: str) -> None:
    with pytest.raises(NotFoundError):
        async with tm.download_stream(bucket, "missing"):
            pytest.fail("the block must not run")


async def test_event_loop_stays_responsive(tm: TransferManager, bucket: str) -> None:
    ticks = 0

    async def tick() -> None:
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0.005)

    ticker = asyncio.create_task(tick())
    await tm.upload_fileobj(SlowReader(total=6 * MiB, delay=0.005), bucket, "responsive")
    ticker.cancel()
    assert ticks > 10
