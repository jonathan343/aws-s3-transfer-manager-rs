# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Exceptions raised by transfers.

Every exception a transfer raises derives from :class:`TransferError`, except those raised for
local files before a transfer starts (for example :class:`FileNotFoundError` from
``upload_file``) and exceptions raised by file objects passed to ``upload_fileobj`` and
``download_fileobj``, which propagate unchanged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from ._core import FailedDownload, FailedUpload

__all__ = [
    "BulkTransferError",
    "IntegrityError",
    "InvalidInputError",
    "NotFoundError",
    "ObjectDiscoveryError",
    "PreconditionFailedError",
    "ServiceError",
    "TransferCancelledError",
    "TransferError",
    "TransferIOError",
]


class TransferError(Exception):
    """Base class for errors raised by a transfer."""


class InvalidInputError(TransferError, ValueError):
    """A transfer's arguments are invalid."""


class TransferIOError(TransferError, OSError):
    """Reading or writing local data failed, or the connection to S3 failed mid-transfer."""


class ServiceError(TransferError):
    """Amazon S3 returned an error.

    Attributes:
        operation: The S3 API operation that failed, e.g. ``"GetObject"``.
        code: The S3 error code, e.g. ``"AccessDenied"``.
        message: The S3 error message.
        request_id: The ``x-amz-request-id`` of the failed request.
        extended_request_id: The ``x-amz-id-2`` of the failed request.
    """

    def __init__(
        self,
        description: str,
        /,
        *,
        operation: str | None = None,
        code: str | None = None,
        message: str | None = None,
        request_id: str | None = None,
        extended_request_id: str | None = None,
    ) -> None:
        super().__init__(description)
        self.operation = operation
        self.code = code
        self.message = message
        self.request_id = request_id
        self.extended_request_id = extended_request_id


class NotFoundError(ServiceError):
    """The bucket, key, or multipart upload does not exist."""


class PreconditionFailedError(ServiceError):
    """A conditional request's precondition did not hold.

    Raised, for example, by an upload with ``if_none_match="*"`` when the key already exists.
    """


class IntegrityError(TransferError):
    """Downloaded bytes did not match the object's checksum.

    Attributes:
        algorithm: The checksum algorithm, e.g. ``"CRC64NVME"``.
        expected: The base64-encoded checksum S3 reported.
        computed: The base64-encoded checksum of the bytes received.
    """

    def __init__(
        self,
        description: str,
        /,
        *,
        algorithm: str | None = None,
        expected: str | None = None,
        computed: str | None = None,
    ) -> None:
        super().__init__(description)
        self.algorithm = algorithm
        self.expected = expected
        self.computed = computed


class ObjectDiscoveryError(TransferError):
    """The size or metadata of an object to download could not be determined."""


class BulkTransferError(TransferError):
    """One or more objects of a directory transfer failed.

    Raised by ``upload_directory`` and ``download_directory`` with ``failure_policy="abort"``.

    Attributes:
        failures: The objects that failed, each with the exception that failed it.
    """

    failures: Sequence[FailedUpload] | Sequence[FailedDownload]

    def __init__(
        self,
        description: str,
        /,
        *,
        failures: Sequence[FailedUpload] | Sequence[FailedDownload] = (),
    ) -> None:
        super().__init__(description)
        self.failures = failures


class TransferCancelledError(TransferError):
    """The transfer was cancelled."""
