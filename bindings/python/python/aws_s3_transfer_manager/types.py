# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Enumerations, size units, and the keyword options accepted by transfer methods.

S3 enumerated values (storage classes, ACLs, ...) are given as the strings S3 uses on the wire,
as with boto3. The ``Literal`` types below list the values known to this release; newer values
are passed through unchanged.
"""

from __future__ import annotations

import datetime
import enum
from collections.abc import Mapping
from typing import Literal, TypedDict

__all__ = [
    "ChecksumAlgorithm",
    "ChecksumMode",
    "ChecksumType",
    "DownloadOptions",
    "FailurePolicy",
    "GiB",
    "KiB",
    "MiB",
    "ObjectCannedACL",
    "ObjectLockLegalHoldStatus",
    "ObjectLockMode",
    "RequestPayer",
    "ServerSideEncryption",
    "StorageClass",
    "TransferStatus",
    "UploadOptions",
]

KiB = 1024
"""One kibibyte (2**10 bytes)."""
MiB = 1024 * KiB
"""One mebibyte (2**20 bytes)."""
GiB = 1024 * MiB
"""One gibibyte (2**30 bytes)."""


class TransferStatus(str, enum.Enum):
    """The state of a transfer."""

    ACTIVE = "active"
    """The transfer is running."""
    COMPLETED = "completed"
    """The transfer finished successfully."""
    FAILED = "failed"
    """The transfer finished with an error."""
    CANCELLED = "cancelled"
    """The transfer was cancelled."""

    def __str__(self) -> str:
        return self.value


StorageClass = Literal[
    "STANDARD",
    "REDUCED_REDUNDANCY",
    "STANDARD_IA",
    "ONEZONE_IA",
    "INTELLIGENT_TIERING",
    "GLACIER",
    "DEEP_ARCHIVE",
    "OUTPOSTS",
    "GLACIER_IR",
    "SNOW",
    "EXPRESS_ONEZONE",
    "FSX_OPENZFS",
    "FSX_ONTAP",
]
ObjectCannedACL = Literal[
    "private",
    "public-read",
    "public-read-write",
    "authenticated-read",
    "aws-exec-read",
    "bucket-owner-read",
    "bucket-owner-full-control",
]
ServerSideEncryption = Literal["AES256", "aws:kms", "aws:kms:dsse", "aws:fsx"]
RequestPayer = Literal["requester"]
ObjectLockMode = Literal["GOVERNANCE", "COMPLIANCE"]
ObjectLockLegalHoldStatus = Literal["ON", "OFF"]
ChecksumAlgorithm = Literal["CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"]
ChecksumType = Literal["FULL_OBJECT", "COMPOSITE"]
ChecksumMode = Literal["ENABLED"]
FailurePolicy = Literal["abort", "continue"]
"""What a directory transfer does when one object fails: stop everything (and raise
``BulkTransferError``), or carry on and report failures in the result."""


class UploadOptions(TypedDict, total=False):
    """Keyword options of ``upload_file``, ``upload_bytes`` and ``upload_fileobj``.

    These mirror the S3 ``PutObject`` / ``CreateMultipartUpload`` parameters. An option given as
    ``None`` is the same as leaving it out.
    """

    acl: ObjectCannedACL | None
    cache_control: str | None
    content_disposition: str | None
    content_encoding: str | None
    content_language: str | None
    content_type: str | None
    checksum_algorithm: ChecksumAlgorithm | None
    """Checksum to compute while uploading; defaults to ``CRC64NVME`` unless the client's
    ``request_checksum_calculation`` is ``"when_required"``."""
    checksum_type: ChecksumType | None
    """Checksum type for multipart uploads: ``FULL_OBJECT`` (default for CRCs) or
    ``COMPOSITE`` (default, and only option, for SHA algorithms)."""
    full_object_checksum: str | None
    """A precomputed, base64-encoded full-object checksum to send instead of computing one."""
    if_match: str | None
    """Only write if the existing object's ETag matches."""
    if_none_match: str | None
    """``"*"`` to only write if the key does not exist yet."""
    expires: datetime.datetime | None
    grant_full_control: str | None
    grant_read: str | None
    grant_read_acp: str | None
    grant_write_acp: str | None
    metadata: Mapping[str, str] | None
    """User-defined metadata, stored as ``x-amz-meta-*`` headers."""
    server_side_encryption: ServerSideEncryption | None
    storage_class: StorageClass | None
    website_redirect_location: str | None
    sse_customer_algorithm: str | None
    sse_customer_key: str | None
    sse_customer_key_md5: str | None
    sse_kms_key_id: str | None
    sse_kms_encryption_context: str | None
    bucket_key_enabled: bool | None
    request_payer: RequestPayer | None
    tagging: str | Mapping[str, str] | None
    """Tags, as a mapping or as S3's query-string form (``"k1=v1&k2=v2"``)."""
    object_lock_mode: ObjectLockMode | None
    object_lock_retain_until_date: datetime.datetime | None
    object_lock_legal_hold_status: ObjectLockLegalHoldStatus | None
    expected_bucket_owner: str | None
    failed_multipart_upload_policy: Literal["abort", "retain"] | None
    """Whether a failed multipart upload is aborted (the default) or its parts kept."""
    priority: int | None
    """Scheduling priority from 1 to 255 (default 128); see ``Transfer.set_priority``."""


class DownloadOptions(TypedDict, total=False):
    """Keyword options of the ``download_*`` methods.

    These apply to ``download_file``, ``download_bytes``, ``download_fileobj`` and
    ``download_stream``, and mirror the S3 ``GetObject`` parameters. An option given as
    ``None`` is the same as leaving it out.
    """

    version_id: str | None
    range: str | None
    """An HTTP byte range, e.g. ``"bytes=0-1023"``."""
    if_match: str | None
    if_none_match: str | None
    if_modified_since: datetime.datetime | None
    if_unmodified_since: datetime.datetime | None
    sse_customer_algorithm: str | None
    sse_customer_key: str | None
    sse_customer_key_md5: str | None
    request_payer: RequestPayer | None
    expected_bucket_owner: str | None
    checksum_mode: ChecksumMode | None
    response_cache_control: str | None
    response_content_disposition: str | None
    response_content_encoding: str | None
    response_content_language: str | None
    response_content_type: str | None
    response_expires: datetime.datetime | None
    read_ahead_parts: int | None
    """How many parts to fetch ahead of the consumer (default: as memory allows)."""
    priority: int | None
    """Scheduling priority from 1 to 255 (default 128); see ``Transfer.set_priority``."""
