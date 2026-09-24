# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""High-throughput Amazon S3 transfers, powered by the AWS S3 Transfer Manager for Rust.

>>> from aws_s3_transfer_manager import TransferManager
>>> with TransferManager() as tm:                                     # doctest: +SKIP
...     tm.upload_file("model.bin", "my-bucket", "models/model.bin").result()
...     data = tm.download_bytes("my-bucket", "config.json").result()

Every transfer method starts the transfer and returns a :class:`Transfer`. Call
:meth:`Transfer.result` to wait for it, or ``await`` it in asyncio code.
"""

from ._core import (
    DirectoryDownloadResult,
    DirectoryUploadResult,
    DownloadResult,
    DownloadStream,
    FailedDownload,
    FailedUpload,
    IntegrityChecks,
    ObjectMetadata,
    ObjectSummary,
    Transfer,
    TransferManager,
    TransferMetrics,
    UploadResult,
    __version__,
)
from .exceptions import (
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
)
from .types import GiB, KiB, MiB, TransferStatus

__all__ = [
    "BulkTransferError",
    "DirectoryDownloadResult",
    "DirectoryUploadResult",
    "DownloadResult",
    "DownloadStream",
    "FailedDownload",
    "FailedUpload",
    "GiB",
    "IntegrityChecks",
    "IntegrityError",
    "InvalidInputError",
    "KiB",
    "MiB",
    "NotFoundError",
    "ObjectDiscoveryError",
    "ObjectMetadata",
    "ObjectSummary",
    "PreconditionFailedError",
    "ServiceError",
    "Transfer",
    "TransferCancelledError",
    "TransferError",
    "TransferIOError",
    "TransferManager",
    "TransferMetrics",
    "TransferStatus",
    "UploadResult",
    "__version__",
]
