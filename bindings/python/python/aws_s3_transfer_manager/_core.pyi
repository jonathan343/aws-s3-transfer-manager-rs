# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Type stubs for the native extension module (re-exported by ``aws_s3_transfer_manager``)."""

import datetime
from collections.abc import Awaitable, Callable, Generator, Sequence
from pathlib import Path
from types import GenericAlias, TracebackType
from typing import Any, Generic, TypeVar, final

from _typeshed import ReadableBuffer, StrPath, SupportsRead, SupportsWrite
from typing_extensions import Self, Unpack

from .types import DownloadOptions, FailurePolicy, TransferStatus, UploadOptions

__all__ = [
    "DirectoryDownloadResult",
    "DirectoryUploadResult",
    "DownloadResult",
    "DownloadStream",
    "FailedDownload",
    "FailedUpload",
    "IntegrityChecks",
    "ObjectMetadata",
    "ObjectSummary",
    "Transfer",
    "TransferManager",
    "TransferMetrics",
    "UploadResult",
    "__version__",
]

__version__: str

_T_co = TypeVar("_T_co", covariant=True)

@final
class TransferManager:
    """High-throughput transfers between local storage and Amazon S3.

    Objects are split into parts that are transferred in parallel on the transfer manager's own
    worker threads. Create one instance and share it; it is safe to use from any thread and
    from asyncio.

    Every transfer method starts its transfer immediately and returns a :class:`Transfer`.
    Used as a context manager (``with`` or ``async with``), the manager waits for outstanding
    transfers on exit, cancelling them first if the block raised.

    AWS settings that are not given (credentials, region, retries, ...) are resolved as by any
    AWS SDK: from environment variables, the shared config and credentials files, and instance
    metadata.

    Args:
        region: The AWS region, e.g. ``"us-west-2"``.
        profile: The named profile to load from the shared AWS config files.
        endpoint_url: A custom S3 endpoint, e.g. for an S3-compatible service.
        force_path_style: Address buckets in the URL path instead of the hostname.
        aws_access_key_id: Static credentials; must be given with ``aws_secret_access_key``.
        aws_secret_access_key: Static credentials.
        aws_session_token: The session token of temporary static credentials.
        part_size: Target part size in bytes (minimum 5 MiB). Chosen automatically by default.
        multipart_threshold: Size in bytes from which uploads use multipart upload (minimum
            5 MiB; default 16 MiB).
        concurrency: Maximum number of concurrent requests across all transfers. Chosen from
            the machine's network capacity by default.
        target_throughput_gbps: Derive the concurrency from a throughput target, in gigabits
            per second, instead. Mutually exclusive with ``concurrency``.
        memory_limit: Upper bound, in bytes, on memory for in-flight and buffered data. At the
            limit, transfers slow down rather than fail. A fraction of RAM by default.
        memory_limit_fraction: The memory limit as a fraction of RAM, in ``(0, 1]``. Mutually
            exclusive with ``memory_limit``.
        read_ahead_parts: How many parts a download may fetch ahead of its consumer. As many
            as memory allows by default.
        request_checksum_calculation: ``"when_supported"`` (default) computes a checksum for
            every upload; ``"when_required"`` only when S3 requires one.
        response_checksum_validation: ``"when_supported"`` (default) validates downloads
            against the object's checksum when it has one; ``"when_required"`` only when
            requested with ``checksum_mode="ENABLED"``.

    Raises:
        ValueError: The arguments are inconsistent, or no AWS region is configured.
    """

    def __new__(
        cls,
        *,
        region: str | None = None,
        profile: str | None = None,
        endpoint_url: str | None = None,
        force_path_style: bool | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        part_size: int | None = None,
        multipart_threshold: int | None = None,
        concurrency: int | None = None,
        target_throughput_gbps: int | None = None,
        memory_limit: int | None = None,
        memory_limit_fraction: float | None = None,
        read_ahead_parts: int | None = None,
        request_checksum_calculation: str | None = None,
        response_checksum_validation: str | None = None,
    ) -> Self: ...
    @property
    def region(self) -> str:
        """The AWS region requests are sent to."""

    @property
    def closed(self) -> bool:
        """Whether :meth:`close` has been called."""

    def upload_file(
        self, path: StrPath, bucket: str, key: str, **options: Unpack[UploadOptions]
    ) -> Transfer[UploadResult]:
        """Upload a local file to ``s3://bucket/key``.

        Raises:
            FileNotFoundError: ``path`` does not exist (raised immediately).
        """

    def upload_bytes(
        self, data: ReadableBuffer, bucket: str, key: str, **options: Unpack[UploadOptions]
    ) -> Transfer[UploadResult]:
        """Upload a bytes-like object to ``s3://bucket/key``.

        ``bytes`` are uploaded without being copied. Other buffers (``bytearray``,
        ``memoryview``, ...) are copied first, so they may be modified once this returns.
        """

    def upload_fileobj(
        self,
        fileobj: SupportsRead[bytes],
        bucket: str,
        key: str,
        **options: Unpack[UploadOptions],
    ) -> Transfer[UploadResult]:
        """Upload the rest of a readable binary file object to ``s3://bucket/key``.

        Reading starts at the file's current position. A seekable file no larger than the
        multipart threshold is read before this returns; anything else is read part by part
        while the upload runs, so the file must stay open until it finishes. An exception raised
        by ``fileobj.read()`` fails the transfer and is raised by ``result()``.
        """

    def download_file(
        self, bucket: str, key: str, path: StrPath, **options: Unpack[DownloadOptions]
    ) -> Transfer[DownloadResult]:
        """Download ``s3://bucket/key`` to a local file.

        The object is written to a temporary file next to ``path``, which replaces ``path``
        only once the download succeeds, and is removed on failure or cancellation.

        Raises:
            FileNotFoundError: The directory containing ``path`` does not exist (raised
                immediately).
        """

    def download_bytes(
        self, bucket: str, key: str, **options: Unpack[DownloadOptions]
    ) -> Transfer[bytes]:
        """Download ``s3://bucket/key`` into memory."""

    def download_fileobj(
        self,
        bucket: str,
        key: str,
        fileobj: SupportsWrite[bytes],
        **options: Unpack[DownloadOptions],
    ) -> Transfer[DownloadResult]:
        """Download ``s3://bucket/key``, writing it in order to a writable binary file object.

        Writes happen on a background thread while the download runs, so the file must stay open
        until it finishes. An exception raised by ``fileobj.write()`` fails the transfer and is
        raised by ``result()``.
        """

    def download_stream(
        self, bucket: str, key: str, **options: Unpack[DownloadOptions]
    ) -> DownloadStream:
        """Stream ``s3://bucket/key`` as an iterator of ``bytes`` chunks.

        See :class:`DownloadStream`.
        """

    def upload_directory(
        self,
        directory: StrPath,
        bucket: str,
        *,
        key_prefix: str | None = None,
        delimiter: str | None = None,
        recursive: bool = True,
        follow_symlinks: bool = False,
        filter: Callable[[Path], bool] | None = None,
        failure_policy: FailurePolicy = "abort",
        max_concurrent_uploads: int | None = None,
        priority: int | None = None,
    ) -> Transfer[DirectoryUploadResult]:
        """Upload the files in a local directory to ``bucket``.

        Each file's key is its path relative to ``directory``, with components joined by
        ``delimiter`` (default ``"/"``) and prefixed with ``key_prefix``.

        Args:
            directory: The directory to upload.
            bucket: The destination bucket.
            key_prefix: Prefix for every key, e.g. ``"backups/2026/"``.
            delimiter: Separator between path components in keys.
            recursive: Include files in subdirectories.
            follow_symlinks: Follow symbolic links to files and directories.
            filter: Called with each file's path; the file is skipped if it returns false. An
                exception raised by the filter skips the file and is reported through
                ``sys.unraisablehook``. It is called from a transfer manager thread.
            failure_policy: ``"abort"`` stops at the first failed file and raises
                :class:`BulkTransferError`; ``"continue"`` uploads the rest and reports failures
                in the result.
            max_concurrent_uploads: Maximum number of files in flight at once (default 512).
            priority: Scheduling priority from 1 to 255 (default 128).

        Raises:
            FileNotFoundError: ``directory`` does not exist (raised immediately).
            NotADirectoryError: ``directory`` is not a directory (raised immediately).
        """

    def download_directory(
        self,
        bucket: str,
        directory: StrPath,
        *,
        key_prefix: str | None = None,
        delimiter: str | None = None,
        filter: Callable[[ObjectSummary], bool] | None = None,
        failure_policy: FailurePolicy = "abort",
        max_concurrent_downloads: int | None = None,
        priority: int | None = None,
    ) -> Transfer[DirectoryDownloadResult]:
        """Download the objects under ``key_prefix`` in ``bucket`` to a local directory.

        Each object's local path is its key with ``key_prefix`` removed, split into components on
        ``delimiter`` (default ``"/"``). ``directory`` is created if it does not exist. Keys that
        would resolve outside ``directory`` fail rather than being written.

        Args:
            bucket: The bucket to download from.
            directory: The destination directory.
            key_prefix: Only download keys starting with this prefix.
            delimiter: Separator between path components in keys.
            filter: Called with an :class:`ObjectSummary` of each listed object; the object is
                skipped if it returns false. An exception raised by the filter skips the object
                and is reported through ``sys.unraisablehook``. It is called from a transfer
                manager thread.
            failure_policy: ``"abort"`` stops at the first failed object and raises
                :class:`BulkTransferError`; ``"continue"`` downloads the rest and reports
                failures in the result.
            max_concurrent_downloads: Maximum number of objects in flight at once (default 512).
            priority: Scheduling priority from 1 to 255 (default 128).
        """

    def close(self, *, cancel: bool = False) -> None:
        """Wait for every transfer started by this manager to finish, then release resources.

        New transfers cannot be started afterwards. Idempotent.

        Args:
            cancel: Cancel transfers still in flight instead of waiting for them to complete.
        """

    def aclose(self, *, cancel: bool = False) -> Awaitable[None]:
        """Asynchronous :meth:`close`."""

    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None: ...
    def __aenter__(self) -> Awaitable[Self]: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Awaitable[None]: ...

@final
class Transfer(Generic[_T_co]):
    """A transfer in progress, returned (already running) by every transfer method.

    Wait for it with :meth:`result`, or ``await`` it from asyncio code; both return the
    transfer's result or raise its exception. The transfer keeps running if this object is
    discarded; call :meth:`cancel` to stop it. Cancelling a task that is awaiting the transfer
    (for example through ``asyncio.timeout``) cancels the transfer too, like awaiting an
    :class:`asyncio.Task`.
    """

    def result(self, timeout: float | None = None) -> _T_co:
        """Wait for the transfer to finish and return its result.

        Args:
            timeout: Seconds to wait; ``None`` waits indefinitely.

        Raises:
            TransferError: The transfer failed; the subclass says why.
            TransferCancelledError: The transfer was cancelled.
            TimeoutError: ``timeout`` elapsed first. The transfer keeps running.
        """

    def exception(self, timeout: float | None = None) -> BaseException | None:
        """Wait for the transfer to finish and return its exception, or ``None`` on success."""

    def done(self) -> bool:
        """Whether the transfer has finished, by succeeding, failing, or being cancelled."""

    def cancel(self) -> bool:
        """Request cancellation.

        Cancellation completes in the background, after which :meth:`result` raises
        :class:`TransferCancelledError`. A cancelled multipart upload is aborted and a cancelled
        ``download_file`` leaves no file behind. As with :meth:`asyncio.Task.cancel`, a transfer
        that finishes before the request takes effect keeps its result.

        Returns:
            ``False`` if the transfer had already finished.
        """

    def cancelled(self) -> bool:
        """Whether the transfer finished by being cancelled."""

    @property
    def status(self) -> TransferStatus:
        """``TransferStatus.ACTIVE`` until the transfer has finished, then how it finished."""

    @property
    def metrics(self) -> TransferMetrics:
        """A snapshot of the transfer's progress."""

    def set_priority(self, priority: int) -> None:
        """Change the transfer's scheduling priority, from 1 (lowest) to 255 (highest).

        Concurrent transfers share throughput in proportion to their priority; the default is
        128.
        """

    def __await__(self) -> Generator[Any, None, _T_co]: ...
    def __class_getitem__(cls, item: object) -> GenericAlias: ...

@final
class DownloadStream:
    """The content of an S3 object, delivered in order as ``bytes`` chunks.

    Returned, already downloading, by :meth:`TransferManager.download_stream`. Iterate it with
    ``for`` or ``async for``. Used as a context manager (``with`` or ``async with``), entering
    waits for the object's :attr:`metadata` and leaving stops the download if it has not been
    read to the end. The object is fetched in parallel ranged requests ahead of the consumer,
    within the transfer manager's memory limit.
    """

    @property
    def metadata(self) -> ObjectMetadata:
        """Metadata of the object, waiting for it if necessary."""

    @property
    def result(self) -> DownloadResult | None:
        """The download's result once the whole object has been read, else ``None``."""

    @property
    def metrics(self) -> TransferMetrics:
        """A snapshot of the download's progress."""

    @property
    def closed(self) -> bool:
        """Whether :meth:`close` has been called."""

    def set_priority(self, priority: int) -> None:
        """Change the download's scheduling priority, from 1 (lowest) to 255 (highest)."""

    def close(self) -> None:
        """Stop the download if it has not finished. Idempotent."""

    def aclose(self) -> Awaitable[None]:
        """Asynchronous :meth:`close`."""

    def __iter__(self) -> Self: ...
    def __next__(self) -> bytes: ...
    def __aiter__(self) -> Self: ...
    def __anext__(self) -> Awaitable[bytes]: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None: ...
    def __aenter__(self) -> Awaitable[Self]: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Awaitable[None]: ...

@final
class TransferMetrics:
    """A snapshot of a transfer's progress."""

    @property
    def bytes_sent(self) -> int:
        """Payload bytes sent to S3 (uploads)."""

    @property
    def bytes_received(self) -> int:
        """Payload bytes received from S3 (downloads)."""

    @property
    def bytes_transferred(self) -> int:
        """Payload bytes moved over the network in either direction."""

    @property
    def disk_bytes_read(self) -> int:
        """Bytes read from local disk."""

    @property
    def disk_bytes_written(self) -> int:
        """Bytes written to local disk."""

    @property
    def total_bytes(self) -> int | None:
        """The expected payload size in bytes, once known."""

    @property
    def elapsed(self) -> datetime.timedelta:
        """Time from the start of the transfer until it finished, or until this snapshot."""

@final
class UploadResult:
    """The outcome of a single-object upload."""

    @property
    def etag(self) -> str | None: ...
    @property
    def version_id(self) -> str | None: ...
    @property
    def checksum_crc32(self) -> str | None: ...
    @property
    def checksum_crc32c(self) -> str | None: ...
    @property
    def checksum_crc64nvme(self) -> str | None: ...
    @property
    def checksum_sha1(self) -> str | None: ...
    @property
    def checksum_sha256(self) -> str | None: ...
    @property
    def checksum_type(self) -> str | None: ...
    @property
    def expiration(self) -> str | None: ...
    @property
    def server_side_encryption(self) -> str | None: ...
    @property
    def sse_customer_algorithm(self) -> str | None: ...
    @property
    def sse_customer_key_md5(self) -> str | None: ...
    @property
    def sse_kms_key_id(self) -> str | None: ...
    @property
    def sse_kms_encryption_context(self) -> str | None: ...
    @property
    def bucket_key_enabled(self) -> bool | None: ...
    @property
    def request_charged(self) -> str | None: ...
    @property
    def upload_id(self) -> str | None:
        """The multipart upload ID, if the object was uploaded in parts."""

    @property
    def metrics(self) -> TransferMetrics:
        """Transfer metrics at completion."""

@final
class DownloadResult:
    """The outcome of a single-object download."""

    @property
    def metadata(self) -> ObjectMetadata:
        """Metadata of the downloaded object."""

    @property
    def integrity(self) -> IntegrityChecks:
        """Checksum validation of the downloaded bytes."""

    @property
    def metrics(self) -> TransferMetrics:
        """Transfer metrics at completion."""

@final
class ObjectMetadata:
    """Metadata of an object, as reported by S3."""

    @property
    def size(self) -> int:
        """Total size of the object in bytes (not only of a requested range)."""

    @property
    def etag(self) -> str | None: ...
    @property
    def last_modified(self) -> datetime.datetime | None: ...
    @property
    def content_type(self) -> str | None: ...
    @property
    def content_encoding(self) -> str | None: ...
    @property
    def content_language(self) -> str | None: ...
    @property
    def content_disposition(self) -> str | None: ...
    @property
    def cache_control(self) -> str | None: ...
    @property
    def expires(self) -> str | None:
        """The ``Expires`` header, verbatim."""

    @property
    def metadata(self) -> dict[str, str]:
        """User-defined metadata (``x-amz-meta-*``), without the prefix."""

    @property
    def version_id(self) -> str | None: ...
    @property
    def storage_class(self) -> str | None:
        """The storage class; S3 omits it for ``STANDARD``."""

    @property
    def server_side_encryption(self) -> str | None: ...
    @property
    def sse_kms_key_id(self) -> str | None: ...
    @property
    def bucket_key_enabled(self) -> bool | None: ...
    @property
    def sse_customer_algorithm(self) -> str | None: ...
    @property
    def sse_customer_key_md5(self) -> str | None: ...
    @property
    def delete_marker(self) -> bool | None: ...
    @property
    def expiration(self) -> str | None: ...
    @property
    def restore(self) -> str | None: ...
    @property
    def website_redirect_location(self) -> str | None: ...
    @property
    def missing_meta(self) -> int | None: ...
    @property
    def parts_count(self) -> int | None: ...
    @property
    def replication_status(self) -> str | None: ...
    @property
    def request_charged(self) -> str | None: ...
    @property
    def object_lock_mode(self) -> str | None: ...
    @property
    def object_lock_retain_until_date(self) -> datetime.datetime | None: ...
    @property
    def object_lock_legal_hold_status(self) -> str | None: ...

@final
class IntegrityChecks:
    """Checksum information for a completed download.

    A checksum *mismatch* is never reported here: it fails the download with
    :class:`IntegrityError`.
    """

    @property
    def validated(self) -> bool:
        """Whether every downloaded byte was validated against a checksum."""

    @property
    def algorithm(self) -> str | None:
        """The algorithm used, when :attr:`validated` is true."""

    @property
    def not_validated_reason(self) -> str | None:
        """Why the download was not validated, when :attr:`validated` is false.

        One of ``"disabled"``, ``"composite_checksum"``, ``"partial_coverage"``, or
        ``"unavailable"``.
        """

    @property
    def checksum_crc32(self) -> str | None: ...
    @property
    def checksum_crc32c(self) -> str | None: ...
    @property
    def checksum_crc64nvme(self) -> str | None: ...
    @property
    def checksum_sha1(self) -> str | None: ...
    @property
    def checksum_sha256(self) -> str | None: ...
    @property
    def checksum_type(self) -> str | None: ...

@final
class DirectoryUploadResult:
    """The outcome of :meth:`TransferManager.upload_directory`."""

    @property
    def objects_uploaded(self) -> int:
        """Number of files uploaded successfully."""

    @property
    def failures(self) -> Sequence[FailedUpload]:
        """Files that failed (only ever non-empty with ``failure_policy="continue"``)."""

    @property
    def metrics(self) -> TransferMetrics:
        """Aggregated transfer metrics at completion."""

@final
class DirectoryDownloadResult:
    """The outcome of :meth:`TransferManager.download_directory`."""

    @property
    def objects_downloaded(self) -> int:
        """Number of objects downloaded successfully."""

    @property
    def failures(self) -> Sequence[FailedDownload]:
        """Objects that failed (only ever non-empty with ``failure_policy="continue"``)."""

    @property
    def metrics(self) -> TransferMetrics:
        """Aggregated transfer metrics at completion."""

@final
class FailedUpload:
    """A local file that :meth:`TransferManager.upload_directory` could not upload."""

    @property
    def path(self) -> Path | None:
        """The local file, when the failure is attributable to one."""

    @property
    def key(self) -> str | None:
        """The destination key, if the upload got as far as having one."""

    @property
    def error(self) -> Exception:
        """The exception describing why the upload failed."""

@final
class FailedDownload:
    """An object that :meth:`TransferManager.download_directory` could not download."""

    @property
    def key(self) -> str | None:
        """The key of the object."""

    @property
    def error(self) -> Exception:
        """The exception describing why the download failed."""

@final
class ObjectSummary:
    """An object listed by :meth:`TransferManager.download_directory`, as passed to its filter."""

    @property
    def key(self) -> str: ...
    @property
    def size(self) -> int | None: ...
    @property
    def last_modified(self) -> datetime.datetime | None: ...
    @property
    def etag(self) -> str | None: ...
    @property
    def storage_class(self) -> str | None: ...
