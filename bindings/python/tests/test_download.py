# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime
import io
from typing import TYPE_CHECKING, Any

import pytest

from aws_s3_transfer_manager import (
    DownloadResult,
    NotFoundError,
    PreconditionFailedError,
    TransferManager,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def uploaded(s3: Any, bucket: str, payload: bytes) -> str:
    """Key of a 12 MiB multipart object, uploaded with boto3 and some metadata."""
    s3.put_object(
        Bucket=bucket,
        Key="object.bin",
        Body=payload,
        ContentType="application/octet-stream",
        Metadata={"origin": "boto3"},
    )
    return "object.bin"


def test_download_file(
    tm: TransferManager, bucket: str, uploaded: str, payload: bytes, tmp_path: Path
) -> None:
    destination = tmp_path / "out.bin"

    result = tm.download_file(bucket, uploaded, destination).result()

    assert isinstance(result, DownloadResult)
    assert destination.read_bytes() == payload
    assert sorted(p.name for p in tmp_path.iterdir()) == ["out.bin"], "no temporary files remain"
    assert result.metadata.size == len(payload)
    assert result.metadata.content_type == "application/octet-stream"
    assert result.metadata.metadata == {"origin": "boto3"}
    assert result.metadata.etag
    assert isinstance(result.metadata.last_modified, datetime.datetime)
    assert result.metadata.last_modified.tzinfo is not None
    assert result.metrics.bytes_received == len(payload)
    assert result.metrics.disk_bytes_written == len(payload)


def test_download_file_replaces_existing_file(
    tm: TransferManager, bucket: str, uploaded: str, payload: bytes, tmp_path: Path
) -> None:
    destination = tmp_path / "out.bin"
    destination.write_bytes(b"stale")
    tm.download_file(bucket, uploaded, str(destination)).result()
    assert destination.read_bytes() == payload


def test_download_file_not_found_leaves_nothing(
    tm: TransferManager, bucket: str, tmp_path: Path
) -> None:
    transfer = tm.download_file(bucket, "missing", tmp_path / "out.bin")
    with pytest.raises(NotFoundError) as excinfo:
        transfer.result()
    assert list(tmp_path.iterdir()) == []
    error = excinfo.value
    assert error.code in {"NoSuchKey", "NotFound"}
    assert error.operation in {"GetObject", "HeadObject"}
    assert error.request_id


def test_download_file_missing_directory_raises_immediately(
    tm: TransferManager, bucket: str, tmp_path: Path
) -> None:
    with pytest.raises(FileNotFoundError):
        tm.download_file(bucket, "k", tmp_path / "no-such-dir" / "out.bin")


def test_download_file_to_directory_raises_immediately(
    tm: TransferManager, bucket: str, tmp_path: Path
) -> None:
    with pytest.raises(IsADirectoryError):
        tm.download_file(bucket, "k", tmp_path)


def test_download_bytes(tm: TransferManager, bucket: str, uploaded: str, payload: bytes) -> None:
    data = tm.download_bytes(bucket, uploaded).result()
    assert isinstance(data, bytes)
    assert data == payload


def test_download_bytes_result_is_stable(tm: TransferManager, bucket: str, uploaded: str) -> None:
    transfer = tm.download_bytes(bucket, uploaded)
    assert transfer.result() is transfer.result()


def test_download_range(tm: TransferManager, bucket: str, uploaded: str, payload: bytes) -> None:
    data = tm.download_bytes(bucket, uploaded, range="bytes=100-6291555").result()
    assert data == payload[100:6291556]


def test_download_fileobj(tm: TransferManager, bucket: str, uploaded: str, payload: bytes) -> None:
    sink = io.BytesIO()
    result = tm.download_fileobj(bucket, uploaded, sink).result()
    assert sink.getvalue() == payload
    assert result.metadata.size == len(payload)


def test_download_fileobj_propagates_write_errors(
    tm: TransferManager, bucket: str, uploaded: str
) -> None:
    class Full(io.RawIOBase):
        def writable(self) -> bool:
            return True

        def write(self, data: Any) -> int:
            raise OSError(28, "No space left on device")

    with pytest.raises(OSError, match="No space left"):
        tm.download_fileobj(bucket, uploaded, Full()).result()


def test_download_fileobj_handles_short_writes(
    tm: TransferManager, bucket: str, uploaded: str, payload: bytes
) -> None:
    class Trickle(io.RawIOBase):
        def __init__(self) -> None:
            self.data = bytearray()

        def writable(self) -> bool:
            return True

        def write(self, data: Any) -> int:
            chunk = bytes(data)[: 1024 * 1024]
            self.data += chunk
            return len(chunk)

    sink = Trickle()
    tm.download_fileobj(bucket, uploaded, sink).result()
    assert bytes(sink.data) == payload


def test_download_fileobj_requires_write(tm: TransferManager, bucket: str) -> None:
    with pytest.raises(TypeError, match="writable"):
        tm.download_fileobj(bucket, "k", io.StringIO().read)  # type: ignore[arg-type]


def test_download_if_match(tm: TransferManager, s3: Any, bucket: str, uploaded: str) -> None:
    etag = s3.head_object(Bucket=bucket, Key=uploaded)["ETag"]
    assert tm.download_bytes(bucket, uploaded, if_match=etag).result()
    with pytest.raises(PreconditionFailedError):
        tm.download_bytes(bucket, uploaded, if_match='"not-the-etag"').result()


def test_download_version_id_not_found(tm: TransferManager, bucket: str, uploaded: str) -> None:
    with pytest.raises(NotFoundError):
        tm.download_bytes(bucket, uploaded, version_id="nonexistent-version").result()


def test_download_integrity_checks(tm: TransferManager, bucket: str, tmp_path: Path) -> None:
    uploaded = tm.upload_bytes(b"checked", bucket, "checked").result()
    integrity = tm.download_file(bucket, "checked", tmp_path / "checked").result().integrity

    # The object fit in one response, so its checksum describes the whole object.
    assert integrity.checksum_crc64nvme == uploaded.checksum_crc64nvme
    # The Rust SDK validates response checksums but does not yet report the outcome back, so
    # the transfer manager reports validation as unavailable rather than claim it.
    assert not integrity.validated
    assert integrity.algorithm is None
    assert integrity.not_validated_reason == "unavailable"


def test_download_integrity_checks_disabled(
    tm_kwargs: dict[str, Any], bucket: str, tmp_path: Path
) -> None:
    with TransferManager(**tm_kwargs, response_checksum_validation="when_required") as tm:
        tm.upload_bytes(b"unchecked", bucket, "unchecked").result()
        result = tm.download_file(bucket, "unchecked", tmp_path / "unchecked").result()
    assert result.integrity.not_validated_reason == "disabled"
