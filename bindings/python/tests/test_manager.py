# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""TransferManager construction, configuration, logging, and the package surface."""

from __future__ import annotations

import logging
import os
import sys
import warnings
from typing import Any

import pytest

import aws_s3_transfer_manager
from aws_s3_transfer_manager import GiB, KiB, MiB, TransferManager, TransferStatus


def test_public_api() -> None:
    for name in aws_s3_transfer_manager.__all__:
        assert hasattr(aws_s3_transfer_manager, name), name
    assert aws_s3_transfer_manager.__version__.count(".") == 2
    assert TransferManager.__module__ == "aws_s3_transfer_manager"
    assert (KiB, MiB, GiB) == (2**10, 2**20, 2**30)
    assert TransferStatus("active") is TransferStatus.ACTIVE
    assert str(TransferStatus.FAILED) == "failed"


def test_region_and_repr(tm: TransferManager) -> None:
    assert tm.region == "us-east-1"
    assert repr(tm) == '<TransferManager region="us-east-1">'
    assert not tm.closed


def test_region_from_environment(
    tm_kwargs: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_REGION", "eu-west-3")
    kwargs = {k: v for k, v in tm_kwargs.items() if k != "region"}
    with TransferManager(**kwargs) as tm:
        assert tm.region == "eu-west-3"


def test_missing_region(tm_kwargs: dict[str, Any]) -> None:
    kwargs = {k: v for k, v in tm_kwargs.items() if k != "region"}
    with pytest.raises(ValueError, match="no AWS region"):
        TransferManager(**kwargs)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"concurrency": 8, "target_throughput_gbps": 10}, "mutually exclusive"),
        ({"memory_limit": GiB, "memory_limit_fraction": 0.5}, "mutually exclusive"),
        ({"memory_limit_fraction": 1.5}, r"in \(0, 1\]"),
        ({"concurrency": 0}, "at least 1"),
        ({"aws_access_key_id": "only-the-id", "aws_secret_access_key": None}, "together"),
        ({"request_checksum_calculation": "always"}, "'when_supported' or 'when_required'"),
    ],
)
def test_invalid_configuration(
    tm_kwargs: dict[str, Any], kwargs: dict[str, Any], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        TransferManager(**{**tm_kwargs, **kwargs})


def test_tuning_options_are_accepted(tm_kwargs: dict[str, Any], bucket: str) -> None:
    options = {
        "concurrency": 4,
        "memory_limit": 64 * MiB,
        "read_ahead_parts": 2,
        "request_checksum_calculation": "when_required",
        "response_checksum_validation": "when_required",
    }
    with TransferManager(**{**tm_kwargs, **options}) as tm:
        result = tm.upload_bytes(b"tuned", bucket, "tuned").result()
        assert result.checksum_crc64nvme is None, "no checksum unless required"
        assert tm.download_bytes(bucket, "tuned").result() == b"tuned"


def test_positional_arguments_are_rejected() -> None:
    with pytest.raises(TypeError):
        TransferManager("us-east-1")  # type: ignore[call-arg]


def test_rust_logs_reach_python_logging(
    tm_kwargs: dict[str, Any], bucket: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="aws_s3_transfer_manager")
    # Logging configuration is read when a manager is created.
    with TransferManager(**tm_kwargs) as tm:
        tm.upload_bytes(b"logged", bucket, "logged").result()
    names = {record.name for record in caplog.records}
    assert any(
        name.startswith("aws_s3_transfer_manager.aws_sdk_s3_transfer_manager") for name in names
    )


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="macOS system frameworks (used for TLS certificates) crash in forked children",
)
def test_fork(tm_kwargs: dict[str, Any], bucket: str) -> None:
    parent = TransferManager(**tm_kwargs)
    parent.upload_bytes(b"from the parent", bucket, "forked").result()
    with warnings.catch_warnings():
        # Python 3.12+ warns that forking a multi-threaded process may deadlock.
        warnings.simplefilter("ignore", DeprecationWarning)
        pid = os.fork()
    if pid == 0:  # pragma: no cover - runs in the child
        status = 1
        try:
            try:
                parent.upload_bytes(b"x", bucket, "k")
            except RuntimeError:
                with TransferManager(**tm_kwargs) as child:
                    data = child.download_bytes(bucket, "forked").result(timeout=30)
                status = 0 if data == b"from the parent" else 3
            else:
                status = 2
        finally:
            os._exit(status)
    _, wait_status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(wait_status) == 0
    assert parent.download_bytes(bucket, "forked").result() == b"from the parent"
    parent.close()
