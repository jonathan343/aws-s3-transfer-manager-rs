# Changelog

All notable changes to the Python bindings are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

Initial developer preview of `aws-s3-transfer-manager` for Python.

### Added
- `TransferManager`, configured from keyword arguments and the standard AWS configuration
  chain (environment, shared config files, SSO, instance metadata).
- Single-object transfers: `upload_file`, `upload_bytes` (zero-copy for `bytes`),
  `upload_fileobj` (including unseekable streams of unknown length), `download_file` (atomic
  replace), `download_bytes`, `download_fileobj`, and `download_stream` (in-order chunks, sync
  and async iteration).
- Directory transfers: `upload_directory` and `download_directory`, with filters and
  abort/continue failure policies.
- `Transfer` handles that work with both `result()` and `await`, with live metrics, priority,
  cancellation, and asyncio cancellation propagation.
- An exception hierarchy rooted at `TransferError`, with S3 error details on `ServiceError`.
- Typed keyword options (`UploadOptions` / `DownloadOptions`), full type stubs, and `py.typed`.
- Rust log events forwarded to Python `logging` under `aws_s3_transfer_manager`.
- Support for free-threaded CPython 3.14t, and for creating managers in forked children.
