# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime
import io
from typing import TYPE_CHECKING, Any

import pytest

from aws_s3_transfer_manager import (
    InvalidInputError,
    MiB,
    PreconditionFailedError,
    TransferManager,
    UploadResult,
)

from helpers import SlowReader, get_object

if TYPE_CHECKING:
    from pathlib import Path


def test_upload_file(tm: TransferManager, s3: Any, bucket: str, local_file: Path) -> None:
    result = tm.upload_file(local_file, bucket, "file.bin").result()

    assert isinstance(result, UploadResult)
    assert result.etag
    assert result.upload_id is not None, "12 MiB at a 5 MiB threshold is a multipart upload"
    assert get_object(s3, bucket, "file.bin") == local_file.read_bytes()
    assert result.metrics.bytes_sent == local_file.stat().st_size
    assert result.metrics.disk_bytes_read == local_file.stat().st_size


def test_upload_file_accepts_str_paths(
    tm: TransferManager, s3: Any, bucket: str, tmp_path: Path
) -> None:
    path = tmp_path / "small.txt"
    path.write_bytes(b"small")

    result = tm.upload_file(str(path), bucket, "small.txt").result()

    assert result.upload_id is None, "a small file is sent with a single PutObject"
    assert result.metrics.bytes_sent == 5
    assert get_object(s3, bucket, "small.txt") == b"small"


def test_upload_file_missing_raises_immediately(
    tm: TransferManager, bucket: str, tmp_path: Path
) -> None:
    missing = tmp_path / "missing.bin"
    with pytest.raises(FileNotFoundError) as excinfo:
        tm.upload_file(missing, bucket, "k")
    assert excinfo.value.filename == str(missing)


def test_upload_file_rejects_directories(tm: TransferManager, bucket: str, tmp_path: Path) -> None:
    with pytest.raises(IsADirectoryError):
        tm.upload_file(tmp_path, bucket, "k")


@pytest.mark.parametrize("wrap", [bytes, bytearray, memoryview], ids=lambda f: f.__name__)
def test_upload_bytes_like(
    tm: TransferManager, s3: Any, bucket: str, payload: bytes, wrap: Any
) -> None:
    tm.upload_bytes(wrap(payload), bucket, "bytes").result()
    assert get_object(s3, bucket, "bytes") == payload


def test_upload_bytes_copies_mutable_buffers(tm: TransferManager, s3: Any, bucket: str) -> None:
    data = bytearray(b"before")
    transfer = tm.upload_bytes(data, bucket, "mutable")
    data[:] = b"after!"
    transfer.result()
    assert get_object(s3, bucket, "mutable") == b"before"


def test_upload_bytes_rejects_non_buffers(tm: TransferManager, bucket: str) -> None:
    with pytest.raises(TypeError, match="bytes-like"):
        tm.upload_bytes("text", bucket, "k")  # type: ignore[arg-type]


def test_upload_empty_object(tm: TransferManager, s3: Any, bucket: str) -> None:
    tm.upload_bytes(b"", bucket, "empty").result()
    assert get_object(s3, bucket, "empty") == b""


def test_upload_fileobj_seekable_starts_at_current_position(
    tm: TransferManager, s3: Any, bucket: str
) -> None:
    buffer = io.BytesIO(b"skip:keep")
    buffer.seek(5)
    tm.upload_fileobj(buffer, bucket, "fileobj").result()
    assert get_object(s3, bucket, "fileobj") == b"keep"


def test_upload_fileobj_large_seekable(
    tm: TransferManager, s3: Any, bucket: str, payload: bytes
) -> None:
    result = tm.upload_fileobj(io.BytesIO(payload), bucket, "large").result()
    assert result.upload_id is not None
    assert get_object(s3, bucket, "large") == payload


def test_upload_fileobj_unseekable_stream(tm: TransferManager, s3: Any, bucket: str) -> None:
    reader = SlowReader(total=11 * MiB + 17, delay=0)

    result = tm.upload_fileobj(reader, bucket, "stream").result()

    assert result.upload_id is not None
    assert get_object(s3, bucket, "stream") == b"x" * (11 * MiB + 17)


def test_upload_fileobj_propagates_read_errors(tm: TransferManager, bucket: str) -> None:
    class Broken(io.RawIOBase):
        def readable(self) -> bool:
            return True

        def read(self, size: int = -1) -> bytes:
            raise ConnectionResetError("source went away")

    transfer = tm.upload_fileobj(Broken(), bucket, "broken")
    with pytest.raises(ConnectionResetError, match="source went away"):
        transfer.result()


def test_upload_fileobj_requires_binary_mode(tm: TransferManager, bucket: str) -> None:
    with pytest.raises(TypeError, match="binary mode"):
        tm.upload_fileobj(io.StringIO("text"), bucket, "k")  # type: ignore[arg-type]


def test_upload_fileobj_requires_read(tm: TransferManager, bucket: str) -> None:
    with pytest.raises(TypeError, match="readable"):
        tm.upload_fileobj(object(), bucket, "k")  # type: ignore[arg-type]


def test_upload_options(tm: TransferManager, s3: Any, bucket: str) -> None:
    tm.upload_bytes(
        b"{}",
        bucket,
        "options.json",
        content_type="application/json",
        cache_control="no-cache",
        metadata={"owner": "tests"},
        tagging={"team": "storage", "env": "test"},
        storage_class="STANDARD_IA",
        content_language=None,
    ).result()

    head = s3.head_object(Bucket=bucket, Key="options.json")
    assert head["ContentType"] == "application/json"
    assert head["CacheControl"] == "no-cache"
    assert head["Metadata"] == {"owner": "tests"}
    assert head["StorageClass"] == "STANDARD_IA"
    tags = s3.get_object_tagging(Bucket=bucket, Key="options.json")["TagSet"]
    assert {t["Key"]: t["Value"] for t in tags} == {"team": "storage", "env": "test"}


def test_upload_checksum_algorithm(tm: TransferManager, bucket: str) -> None:
    result = tm.upload_bytes(b"checksummed", bucket, "sha", checksum_algorithm="SHA256").result()
    assert result.checksum_sha256


def test_upload_default_checksum_is_crc64nvme(tm: TransferManager, bucket: str) -> None:
    result = tm.upload_bytes(b"checksummed", bucket, "crc").result()
    assert result.checksum_crc64nvme


def test_upload_if_none_match(tm: TransferManager, bucket: str) -> None:
    tm.upload_bytes(b"first", bucket, "once", if_none_match="*").result()
    with pytest.raises(PreconditionFailedError) as excinfo:
        tm.upload_bytes(b"second", bucket, "once", if_none_match="*").result()
    assert excinfo.value.code == "PreconditionFailed"


@pytest.mark.parametrize(
    ("options", "match"),
    [
        ({"priority": 0}, "priority must be 1-255"),
        ({"priority": 256}, "priority must be 1-255"),
        ({"checksum_type": "COMPOSITE"}, "require checksum_algorithm"),
        ({"checksum_algorithm": "SHA1", "checksum_type": "FULL_OBJECT"}, "does not support"),
        ({"failed_multipart_upload_policy": "keep"}, "'abort' or 'retain'"),
        ({"expires": datetime.datetime(2030, 1, 1)}, "timezone-aware"),
    ],
)
def test_upload_invalid_options(
    tm: TransferManager, bucket: str, options: dict[str, Any], match: str
) -> None:
    with pytest.raises(InvalidInputError, match=match):
        tm.upload_bytes(b"x", bucket, "k", **options)


def test_upload_rejects_unknown_options(tm: TransferManager, bucket: str) -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument 'contenttype'"):
        tm.upload_bytes(b"x", bucket, "k", contenttype="text/plain")  # type: ignore[call-arg]


def test_upload_option_type_errors_name_the_option(tm: TransferManager, bucket: str) -> None:
    with pytest.raises(TypeError, match="argument 'metadata'"):
        tm.upload_bytes(b"x", bucket, "k", metadata={"n": 1})  # type: ignore[dict-item]


def test_upload_accepts_aware_datetimes(tm: TransferManager, s3: Any, bucket: str) -> None:
    expires = datetime.datetime(2030, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
    tm.upload_bytes(b"x", bucket, "expiring", expires=expires).result()
    assert s3.head_object(Bucket=bucket, Key="expiring")["Expires"] == expires
