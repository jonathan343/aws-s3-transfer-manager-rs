# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - Unreleased

A ground-up rearchitecture of the transfer manager. The public API keeps its shape; the machinery
beneath it is new.

### Added
- Adaptive concurrency: the number of in-flight requests is discovered at runtime — seeded from the
  instance and ramped toward the throughput the network sustains — rather than fixed at a constant.
- Bounded memory: a global memory budget and an occupancy-paced receive buffer cap resident memory
  independently of concurrency and consumer speed, so a fast network draining to a slow disk cannot
  grow memory without limit.
- Fair scheduling across concurrent transfers, so `upload_objects`/`download_objects` calls share
  throughput by transfer rather than by object count.
- Data integrity: checksum validation on upload and download, with a corrupt body failing the
  transfer rather than being silently retried.
- Resilience: recovery from download body-stream failures the SDK's own retry does not cover,
  throttle-storm recovery with per-bucket retry isolation, and speculative hedging of slow requests
  under a self-limiting budget.
- `TransferMonitor`, returned by every transfer handle's `monitor()`: a cloneable view of a running
  transfer's status, metrics and scheduling controls, whose `finished()` waits for the transfer to end
  without consuming the handle, so progress can be reported while another task joins it.

### Fixed
- Cancelling a transfer (dropping its handle or calling `abort()`) now interrupts its in-flight
  work; previously work already dispatched ran to completion, so cancelling a large upload could
  keep uploading every part in flight.
- `UploadHandle::abort()` no longer panics when the client uses the managed runtime's HTTP
  transport: requests issued from outside the managed threads (such as `AbortMultipartUpload`)
  use a separate connection pool.
- Dropping the last `Client` after it ran transfers now shuts down its worker threads. Finished
  transfers were kept alive by scheduler queue entries awaiting epoch-based reclamation, which
  kept the client and its threads alive; and when the last reference was released by a worker
  thread, shutdown tried to join that thread with itself.

### Changed
- Execution model: the transfer manager now runs its own per-core threads and dispatches work to
  them, replacing the shared general-purpose thread pool. This gives the client direct control over
  request ordering and placement for tighter latency behavior.

## [0.1.3] - 2025-09-08

### Added
- Validate the content range and request count of ranged GET requests.
- Validate field mappings between transfer manager and S3 input/output types.
- Validate the content length and part-number alignment of `UploadPart` requests.

## [0.1.1] - 2025-03-05

### Fixed
- Publishing on crates.io: add the crate README, fix the repository URL, and add the description,
  categories, and keywords.

## [0.1.0] - 2025-03-05

### Added
- Initial developer-preview release of a high-performance Amazon S3 client for Rust.
