# aws-s3-transfer-manager

High-throughput Amazon S3 uploads and downloads for Python, powered by the
[AWS S3 Transfer Manager for Rust](https://github.com/awslabs/aws-s3-transfer-manager-rs).

Objects are split into parts that are transferred in parallel on the transfer manager's own
worker threads, outside the GIL, with concurrency sized to the machine's network and memory
bounded by a budget. The API works the same from threads and from `asyncio`.

> [!WARNING]
> Developer preview: not recommended for production use yet. The API may change.

```python
from aws_s3_transfer_manager import TransferManager

with TransferManager() as tm:
    tm.upload_file("model.safetensors", "my-bucket", "models/v3.safetensors").result()
    tm.download_directory("my-bucket", "./datasets", key_prefix="datasets/2026/").result()
```

## Installation

```sh
pip install aws-s3-transfer-manager
```

Wheels are built for CPython 3.10+ (including free-threaded 3.14t) on Linux (x86-64, ARM64),
macOS, and Windows. There are no Python dependencies.

## Transfers

Every transfer method starts its transfer immediately and returns a `Transfer`. Wait for it with
`result()`, or `await` it:

```python
transfer = tm.upload_file("big.iso", "my-bucket", "images/big.iso")
result = transfer.result()  # blocks; raises the transfer's exception on failure
print(result.etag, result.metrics.elapsed)


async def main() -> None:
    result = await tm.upload_file("big.iso", "my-bucket", "images/big.iso")
```

Because transfers run in the background, starting several and then waiting is all it takes to
run them concurrently, with no threads or tasks of your own:

```python
transfers = [tm.upload_file(path, "my-bucket", path.name) for path in Path("out").glob("*.parquet")]
for transfer in transfers:
    transfer.result()

# or, in asyncio code
await asyncio.gather(*(tm.download_bytes("my-bucket", key) for key in keys))
```

A `Transfer` also reports progress, controls priority, and can be cancelled:

| | |
|---|---|
| `result(timeout=None)` | Wait and return the result, or raise its exception (`TimeoutError` if `timeout` elapses). |
| `await transfer` | The same, from asyncio. Cancelling the awaiting task cancels the transfer. |
| `exception(timeout=None)` | Wait and return the exception, or `None`. |
| `done()`, `status` | Whether it finished; `TransferStatus.ACTIVE` / `COMPLETED` / `FAILED` / `CANCELLED`. |
| `metrics` | A `TransferMetrics` snapshot: `bytes_transferred`, `total_bytes`, `elapsed`, ... |
| `set_priority(n)` | 1–255 (default 128); concurrent transfers share throughput in proportion. |
| `cancel()`, `cancelled()` | Request cancellation; a cancelled multipart upload is aborted. |

Dropping a `Transfer` does not stop it. Leaving a `with TransferManager()` block waits for the
transfers it started, and cancels them if the block raised.

## Uploads

```python
tm.upload_file("report.pdf", "my-bucket", "reports/q3.pdf")
tm.upload_bytes(b'{"ok": true}', "my-bucket", "status.json", content_type="application/json")
with open("archive.tar", "rb") as f:
    tm.upload_fileobj(f, "my-bucket", "archive.tar").result()  # keep f open until done
```

`upload_fileobj` reads any binary file-like object, including unseekable streams (pipes, sockets,
`gzip` streams) whose size is not known in advance. `bytes` given to `upload_bytes` are uploaded
without being copied.

S3 request options are keyword arguments, named like boto3's but in `snake_case`; enumerated
values are the strings S3 uses (`"STANDARD_IA"`, `"aws:kms"`, ...). They are fully typed, so your
editor completes and checks them:

```python
tm.upload_file(
    "data.csv",
    "my-bucket",
    "exports/data.csv",
    content_type="text/csv",
    metadata={"source": "nightly-export"},
    tagging={"team": "analytics"},
    storage_class="INTELLIGENT_TIERING",
    server_side_encryption="aws:kms",
    checksum_algorithm="SHA256",
    if_none_match="*",  # only create; raises PreconditionFailedError if the key exists
)
```

## Downloads

```python
result = tm.download_file("my-bucket", "images/big.iso", "big.iso").result()
print(result.metadata.size, result.metadata.content_type, result.metadata.last_modified)

data: bytes = tm.download_bytes("my-bucket", "config.json").result()

with open("big.iso", "wb") as f:
    tm.download_fileobj("my-bucket", "images/big.iso", f).result()

tm.download_bytes("my-bucket", "video.mp4", range="bytes=0-1048575", version_id="...")
```

`download_file` writes to a temporary file next to the destination and moves it into place only
once the download succeeds, so readers never see a partial file.

To process an object while it downloads, stream it. Chunks arrive in order while later parts are
fetched in parallel:

```python
with tm.download_stream("my-bucket", "logs/2026-09-23.jsonl") as stream:
    print(stream.metadata.size)
    for chunk in stream:
        process(chunk)

async with tm.download_stream("my-bucket", "logs/2026-09-23.jsonl") as stream:
    async for chunk in stream:
        process(chunk)
```

Leaving the `with` block early stops the download.

## Directories

```python
result = tm.upload_directory(
    "./site",
    "my-bucket",
    key_prefix="www/",
    filter=lambda path: path.suffix != ".map",  # called with each file's pathlib.Path
).result()
print(result.objects_uploaded)

result = tm.download_directory(
    "my-bucket",
    "./backup",
    key_prefix="www/",
    filter=lambda obj: obj.size < 100 * MiB,  # called with an ObjectSummary
    failure_policy="continue",  # report failures instead of stopping
).result()
for failure in result.failures:
    print(failure.key, failure.error)
```

With the default `failure_policy="abort"`, the first failure stops the transfer and raises
`BulkTransferError`, whose `failures` lists what failed.

## Configuration

AWS settings are resolved like any AWS SDK does: from environment variables (`AWS_REGION`,
`AWS_PROFILE`, ...), the shared `~/.aws/config` and `~/.aws/credentials` files, SSO, container
and instance credentials. Anything can be overridden explicitly:

```python
from aws_s3_transfer_manager import GiB, MiB, TransferManager

tm = TransferManager(
    region="us-west-2",
    profile="analytics",
    part_size=16 * MiB,  # default: chosen automatically
    multipart_threshold=64 * MiB,  # default: 16 MiB
    target_throughput_gbps=100,  # or concurrency=...; default: sized to the instance
    memory_limit=8 * GiB,  # or memory_limit_fraction=...; default: a share of RAM
)
```

For S3-compatible services, pass `endpoint_url=` (and usually `force_path_style=True`); if the
service does not support the default CRC64NVME checksums, pass
`request_checksum_calculation="when_required"`.

Create one `TransferManager` and share it: it is safe to use from many threads and from asyncio
at once, and each instance owns a pool of worker threads. After `os.fork()` (for example in
`multiprocessing` workers on Linux), create a new one in the child; instances inherited from the
parent raise `RuntimeError`. On macOS, use the `spawn` start method (Python's default there):
system frameworks the TLS stack relies on cannot be used in a forked child.

## Errors

Transfer failures raise subclasses of `TransferError`:

| Exception | Raised when |
|---|---|
| `ServiceError` | S3 returned an error; see `.code`, `.message`, `.operation`, `.request_id`. |
| `NotFoundError` | ... the bucket, key, version, or upload does not exist (`ServiceError`). |
| `PreconditionFailedError` | ... an `if_match` / `if_none_match` condition failed (`ServiceError`). |
| `TransferIOError` | Local I/O or the network connection failed (also an `OSError`). |
| `IntegrityError` | Downloaded bytes did not match the object's checksum. |
| `InvalidInputError` | An argument was invalid (also a `ValueError`). |
| `BulkTransferError` | A directory transfer failed; see `.failures`. |
| `TransferCancelledError` | The transfer was cancelled. |

Problems detected before a transfer starts raise the usual built-in exceptions immediately, for
example `FileNotFoundError` from `upload_file`, and exceptions raised by your own file objects
propagate unchanged.

## Logging

Diagnostics from the transfer manager and the AWS SDK go to the `aws_s3_transfer_manager`
logger:

```python
import logging

logging.basicConfig()
logging.getLogger("aws_s3_transfer_manager").setLevel(logging.DEBUG)
```

Configure logging before creating a `TransferManager`; changes made later take effect for
managers created afterwards.

## Development

The package is built with [maturin](https://www.maturin.rs) and developed with
[uv](https://docs.astral.sh/uv/). From this directory:

```sh
uv sync                                   # build the extension (in dev mode) and install deps
uv run pytest                             # tests run against an in-process moto S3 server
uv run ruff check && uv run ruff format --check
uv run mypy                               # strict type checking of the package and tests
uv run python -m mypy.stubtest aws_s3_transfer_manager._core   # stubs match the extension
cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

`uv sync` rebuilds the extension whenever the Rust sources change. Published wheels use the
`dist` Cargo profile (fat LTO); build one locally with
`uvx maturin build --profile dist` for benchmarking.

The native module lives in `src/` (Rust, [PyO3](https://pyo3.rs)); the pure-Python package
(exceptions, enums, typing) and the type stub for the native module live in
`python/aws_s3_transfer_manager/`.

## License

Apache-2.0
