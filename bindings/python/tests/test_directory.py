# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import pytest

from aws_s3_transfer_manager import (
    BulkTransferError,
    DirectoryDownloadResult,
    DirectoryUploadResult,
    FailedUpload,
    InvalidInputError,
    ObjectSummary,
    TransferIOError,
    TransferManager,
)

from helpers import get_object

FILES = {
    "a.txt": b"a",
    "b.log": b"bb",
    "nested/c.txt": b"ccc",
    "nested/deeper/d.txt": b"dddd",
}


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    root = tmp_path / "tree"
    for name, content in FILES.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return root


def keys(s3: Any, bucket: str, prefix: str = "") -> dict[str, int]:
    listed = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    return {obj["Key"]: obj["Size"] for obj in listed}


def test_upload_directory(tm: TransferManager, s3: Any, bucket: str, tree: Path) -> None:
    result = tm.upload_directory(tree, bucket, key_prefix="backup/").result()

    assert isinstance(result, DirectoryUploadResult)
    assert result.objects_uploaded == len(FILES)
    assert result.failures == []
    assert result.metrics.bytes_sent == sum(len(c) for c in FILES.values())
    assert keys(s3, bucket) == {f"backup/{name}": len(c) for name, c in FILES.items()}
    assert get_object(s3, bucket, "backup/nested/deeper/d.txt") == b"dddd"


def test_upload_directory_non_recursive(
    tm: TransferManager, s3: Any, bucket: str, tree: Path
) -> None:
    result = tm.upload_directory(str(tree), bucket, recursive=False).result()
    assert result.objects_uploaded == 2
    assert set(keys(s3, bucket)) == {"a.txt", "b.log"}


def test_upload_directory_filter(tm: TransferManager, s3: Any, bucket: str, tree: Path) -> None:
    seen: list[Path] = []

    def only_text(path: Path) -> bool:
        seen.append(path)
        return path.suffix == ".txt"

    tm.upload_directory(tree, bucket, filter=only_text).result()

    assert set(keys(s3, bucket)) == {"a.txt", "nested/c.txt", "nested/deeper/d.txt"}
    assert all(isinstance(p, Path) for p in seen)
    assert sorted(p.relative_to(tree).as_posix() for p in seen) == sorted(FILES)


def test_upload_directory_filter_exceptions_exclude_the_file(
    tm: TransferManager, s3: Any, bucket: str, tree: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reported: list[BaseException | None] = []
    monkeypatch.setattr(sys, "unraisablehook", lambda info: reported.append(info.exc_value))

    def broken(path: Path) -> bool:
        if path.name == "a.txt":
            raise RuntimeError("bad filter")
        return True

    tm.upload_directory(tree, bucket, filter=broken).result()

    assert [str(e) for e in reported] == ["bad filter"]
    assert "a.txt" not in keys(s3, bucket)
    assert len(keys(s3, bucket)) == len(FILES) - 1


def test_upload_directory_delimiter(tm: TransferManager, s3: Any, bucket: str, tree: Path) -> None:
    tm.upload_directory(tree, bucket, delimiter="|").result()
    assert "nested|deeper|d.txt" in keys(s3, bucket)


@pytest.mark.skipif(
    sys.platform == "win32" or os.geteuid() == 0, reason="needs POSIX permissions, not root"
)
def test_upload_directory_failure_policies(
    tm: TransferManager, s3: Any, bucket: str, tree: Path
) -> None:
    unreadable = tree / "nested" / "c.txt"
    unreadable.chmod(0)
    try:
        result = tm.upload_directory(tree, bucket, failure_policy="continue").result()
        assert result.objects_uploaded == len(FILES) - 1
        [failure] = result.failures
        assert isinstance(failure, FailedUpload)
        assert failure.path == unreadable
        assert isinstance(failure.error, TransferIOError)
        assert isinstance(failure.error, OSError)
        assert "Permission denied" in str(failure.error)

        with pytest.raises(BulkTransferError) as excinfo:
            tm.upload_directory(tree, bucket, key_prefix="abort/").result()
        [aborted] = excinfo.value.failures
        assert isinstance(aborted, FailedUpload)
        assert aborted.path == unreadable
    finally:
        unreadable.chmod(0o644)


def test_upload_directory_missing_source(tm: TransferManager, bucket: str, tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        tm.upload_directory(tmp_path / "missing", bucket)
    (tmp_path / "file").write_bytes(b"")
    with pytest.raises(NotADirectoryError):
        tm.upload_directory(tmp_path / "file", bucket)


def test_upload_directory_invalid_arguments(tm: TransferManager, bucket: str, tree: Path) -> None:
    with pytest.raises(InvalidInputError, match="failure_policy"):
        tm.upload_directory(tree, bucket, failure_policy="retry")  # type: ignore[arg-type]
    with pytest.raises(InvalidInputError, match="priority"):
        tm.upload_directory(tree, bucket, priority=1000)


@pytest.fixture
def uploaded_tree(tm: TransferManager, bucket: str, tree: Path) -> str:
    tm.upload_directory(tree, bucket, key_prefix="data/").result()
    return "data/"


def test_download_directory(
    tm: TransferManager, bucket: str, uploaded_tree: str, tmp_path: Path
) -> None:
    destination = tmp_path / "restored" / "creates" / "parents"

    result = tm.download_directory(bucket, destination, key_prefix=uploaded_tree).result()

    assert isinstance(result, DirectoryDownloadResult)
    assert result.objects_downloaded == len(FILES)
    assert result.failures == []
    restored = {
        p.relative_to(destination).as_posix(): p.read_bytes()
        for p in destination.rglob("*")
        if p.is_file()
    }
    assert restored == FILES


def test_download_directory_filter(
    tm: TransferManager, bucket: str, uploaded_tree: str, tmp_path: Path
) -> None:
    seen: list[ObjectSummary] = []

    def small(obj: ObjectSummary) -> bool:
        seen.append(obj)
        return obj.size is not None and obj.size <= 2

    destination = tmp_path / "restored"
    result = tm.download_directory(
        bucket, destination, key_prefix=uploaded_tree, filter=small
    ).result()

    assert result.objects_downloaded == 2
    assert sorted(p.name for p in destination.rglob("*") if p.is_file()) == ["a.txt", "b.log"]
    assert {obj.key for obj in seen} == {f"data/{name}" for name in FILES}
    assert all(obj.etag and obj.last_modified for obj in seen)


def test_download_directory_skips_folder_markers(
    tm: TransferManager, s3: Any, bucket: str, tmp_path: Path
) -> None:
    s3.put_object(Bucket=bucket, Key="folder/", Body=b"")
    s3.put_object(Bucket=bucket, Key="folder/file", Body=b"f")
    result = tm.download_directory(bucket, tmp_path, filter=lambda _: True).result()
    assert result.objects_downloaded == 1
    assert (tmp_path / "folder" / "file").read_bytes() == b"f"


def test_download_directory_rejects_keys_escaping_the_destination(
    tm: TransferManager, s3: Any, bucket: str, tmp_path: Path
) -> None:
    s3.put_object(Bucket=bucket, Key="../escape", Body=b"x")
    destination = tmp_path / "dest"
    result = tm.download_directory(bucket, destination, failure_policy="continue").result()
    [failure] = result.failures
    assert failure.key == "../escape"
    assert isinstance(failure.error, InvalidInputError)
    assert not (tmp_path / "escape").exists()
