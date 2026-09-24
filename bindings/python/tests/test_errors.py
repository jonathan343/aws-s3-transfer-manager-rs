# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pickle

import pytest

from aws_s3_transfer_manager import (
    BulkTransferError,
    IntegrityError,
    InvalidInputError,
    NotFoundError,
    ObjectDiscoveryError,
    PreconditionFailedError,
    ServiceError,
    TransferCancelledError,
    TransferError,
    TransferIOError,
    TransferManager,
)


@pytest.mark.parametrize(
    ("error", "bases"),
    [
        (InvalidInputError, (TransferError, ValueError)),
        (TransferIOError, (TransferError, OSError)),
        (ServiceError, (TransferError,)),
        (NotFoundError, (ServiceError,)),
        (PreconditionFailedError, (ServiceError,)),
        (IntegrityError, (TransferError,)),
        (ObjectDiscoveryError, (TransferError,)),
        (BulkTransferError, (TransferError,)),
        (TransferCancelledError, (TransferError,)),
    ],
)
def test_hierarchy(error: type[Exception], bases: tuple[type[Exception], ...]) -> None:
    assert issubclass(error, bases)
    assert error.__module__ == "aws_s3_transfer_manager.exceptions"


def test_io_error_is_an_oserror_with_a_message() -> None:
    error = TransferIOError("connection reset")
    assert str(error) == "connection reset"
    assert error.errno is None


def test_service_error_attributes_survive_pickling(tm: TransferManager, bucket: str) -> None:
    error = tm.download_bytes(bucket, "missing").exception()
    assert isinstance(error, NotFoundError)

    copy = pickle.loads(pickle.dumps(error))

    assert type(copy) is NotFoundError
    assert str(copy) == str(error)
    for attribute in ("operation", "code", "message", "request_id", "extended_request_id"):
        assert getattr(copy, attribute) == getattr(error, attribute)


def test_service_error_message(tm: TransferManager, bucket: str) -> None:
    error = tm.download_bytes(bucket, "missing").exception()
    assert isinstance(error, ServiceError)
    assert error.message
    assert str(error).startswith(f"S3 {error.operation} failed with {error.code}: {error.message}")
    assert f"request id: {error.request_id}" in str(error)


def test_missing_bucket(tm: TransferManager) -> None:
    with pytest.raises(NotFoundError) as excinfo:
        tm.upload_bytes(b"x", "no-such-bucket-for-tests", "k").result()
    assert excinfo.value.code == "NoSuchBucket"


def test_connection_failure_is_an_io_error(tm_kwargs: dict[str, object]) -> None:
    kwargs = {**tm_kwargs, "endpoint_url": "http://127.0.0.1:9"}
    with TransferManager(**kwargs) as tm, pytest.raises(TransferIOError) as excinfo:  # type: ignore[arg-type]
        tm.upload_bytes(b"x", "bucket", "k").result(timeout=60)
    assert isinstance(excinfo.value, OSError)
