/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! `TransferManager`: the Python entry point.

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use aws_sdk_s3_transfer_manager::io::walk::{FsWalker, S3Walker};
use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::operation::download::builders::DownloadFluentBuilder;
use aws_sdk_s3_transfer_manager::types::FailedTransferPolicy;
use aws_sdk_s3_transfer_manager::Client;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::Settings;
use crate::errors::{invalid_input, to_pyerr};
use crate::io::{self, PyErrorSlot, PyReader};
use crate::options::{self, DownloadOptions, UploadOptions};
use crate::runtime::{self, ProcessLocal};
use crate::stream::DownloadStream;
use crate::transfer::{self, Launcher, Sink, Transfer};
use crate::types::ObjectSummary;

struct Inner {
    /// `None` once the manager is closed.
    client: RwLock<Option<Client>>,
    /// Every transfer's driver task, so `close()` can wait for them.
    tracker: TaskTracker,
    /// Parent of every transfer's cancellation token.
    cancel: CancellationToken,
    region: String,
    multipart_threshold: u64,
}

impl Inner {
    fn client(&self) -> PyResult<Client> {
        self.client
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| PyRuntimeError::new_err("the TransferManager is closed"))
    }

    fn launcher(&self) -> Launcher<'_> {
        Launcher {
            tracker: &self.tracker,
            cancel: &self.cancel,
        }
    }

    fn is_closed(&self) -> bool {
        self.client
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
    }

    /// Stop accepting transfers and, optionally, cancel the ones in flight.
    fn begin_close(&self, cancel: bool) {
        if cancel {
            self.cancel.cancel();
        }
        self.tracker.close();
    }

    /// Release the client once in-flight transfers are done.
    fn finish_close(&self) {
        let client = self
            .client
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        // The last reference joins the client's worker threads; keep that off this thread.
        if let Some(client) = client {
            runtime::handle().spawn_blocking(move || drop(client));
        }
    }
}

fn failure_policy(py: Python<'_>, value: &str) -> PyResult<FailedTransferPolicy> {
    match value {
        "abort" => Ok(FailedTransferPolicy::Abort),
        "continue" => Ok(FailedTransferPolicy::Continue),
        other => Err(invalid_input(
            py,
            format!("failure_policy must be 'abort' or 'continue', got {other:?}"),
        )),
    }
}

/// Fail fast, with the `OSError` Python would raise, if `path` is not an existing directory.
fn require_dir(py: Python<'_>, path: &Path) -> PyResult<()> {
    let metadata = std::fs::metadata(path).map_err(|err| io::os_error(py, &err, path))?;
    if !metadata.is_dir() {
        return Err(io::os_error(
            py,
            &std::io::Error::from(std::io::ErrorKind::NotADirectory),
            path,
        ));
    }
    Ok(())
}

/// Call a user filter from a transfer manager thread. A filter that raises excludes the entry;
/// the exception is reported through `sys.unraisablehook`.
fn call_filter<F>(filter: &Py<PyAny>, make_arg: F) -> bool
where
    F: for<'py> FnOnce(Python<'py>) -> PyResult<Bound<'py, PyAny>>,
{
    Python::try_attach(|py| {
        let filter = filter.bind(py);
        let keep = make_arg(py)
            .and_then(|arg| filter.call1((arg,)))
            .and_then(|verdict| verdict.is_truthy());
        keep.unwrap_or_else(|err| {
            err.write_unraisable(py, Some(filter));
            false
        })
    })
    .unwrap_or(false)
}

/// Zero-byte keys ending in `/` are "folders" created by the S3 console, not files.
fn is_folder_marker(object: &aws_sdk_s3::types::Object) -> bool {
    object.key().is_some_and(|key| key.ends_with('/')) && object.size() == Some(0)
}

/// Run a (fast, synchronous) initiation with the bindings runtime entered, detached from Python.
fn initiate<T: Send>(
    py: Python<'_>,
    f: impl FnOnce() -> Result<T, aws_sdk_s3_transfer_manager::error::Error> + Send,
) -> PyResult<T> {
    let handle = runtime::handle();
    py.detach(|| {
        let _entered = handle.enter();
        f()
    })
    .map_err(|err| to_pyerr(py, &err))
}

/// High-throughput transfers between local storage and Amazon S3.
///
/// Objects are split into parts that are transferred in parallel on the transfer manager's own
/// worker threads, so a single instance can saturate the network of large machines. Create one
/// instance and share it: it is safe to use from multiple threads and from asyncio.
///
/// Every transfer method starts the transfer immediately and returns a `Transfer` handle;
/// `result()` waits for it, or it can be `await`-ed. Use the manager as a context manager (sync
/// or async) to wait for outstanding transfers on exit, cancelling them if the block raised.
///
/// AWS settings not given explicitly (credentials, region, retries, ...) are resolved like any
/// AWS SDK: from environment variables, the shared config and credentials files, and instance
/// metadata.
#[pyclass(frozen, module = "aws_s3_transfer_manager")]
pub struct TransferManager {
    inner: ProcessLocal<Arc<Inner>>,
}

impl TransferManager {
    fn inner(&self) -> PyResult<&Arc<Inner>> {
        self.inner.get()
    }

    #[allow(clippy::too_many_arguments)]
    fn launch_upload(
        &self,
        py: Python<'_>,
        body: InputStream,
        bucket: String,
        key: String,
        options: UploadOptions,
        source: String,
        errors: PyErrorSlot,
    ) -> PyResult<Transfer> {
        let inner = self.inner()?;
        let priority = options.priority(py)?;
        let builder = options.apply(
            py,
            inner
                .client()?
                .upload()
                .bucket(bucket.as_str())
                .key(key.as_str())
                .body(body),
        )?;
        let handle = initiate(py, || builder.initiate())?;
        let monitor = handle.monitor();
        let description = format!("upload of {source} to s3://{bucket}/{key}");
        Ok(inner
            .launcher()
            .launch(description.clone(), monitor.clone(), priority, |cancel| {
                transfer::drive(handle, monitor, cancel, description, errors)
            }))
    }

    fn download_builder(
        &self,
        py: Python<'_>,
        method: &str,
        bucket: &str,
        key: &str,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<(DownloadFluentBuilder, Option<u8>)> {
        let options = DownloadOptions::parse(method, kwargs)?;
        let priority = options.priority(py)?;
        let builder = options.apply(self.inner()?.client()?.download().bucket(bucket).key(key));
        Ok((builder, priority))
    }

    fn launch_body(
        &self,
        py: Python<'_>,
        (builder, priority): (DownloadFluentBuilder, Option<u8>),
        sink: Sink,
        description: String,
    ) -> PyResult<Transfer> {
        let handle = initiate(py, || builder.initiate())?;
        let monitor = handle.monitor();
        Ok(self
            .inner()?
            .launcher()
            .launch(description.clone(), monitor, priority, |cancel| {
                transfer::drive_body(handle, sink, cancel, description)
            }))
    }

    fn close_inner(&self, py: Python<'_>, cancel: bool) -> PyResult<()> {
        let inner = self.inner()?;
        inner.begin_close(cancel);
        runtime::block_on_forever(py, inner.tracker.wait())?;
        inner.finish_close();
        Ok(())
    }

    fn aclose_inner<'py>(&self, py: Python<'py>, cancel: bool) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(self.inner()?);
        inner.begin_close(cancel);
        runtime::future_into_py(py, async move {
            inner.tracker.wait().await;
            inner.finish_close();
            Ok(())
        })
    }
}

#[pymethods]
impl TransferManager {
    /// Create a transfer manager.
    ///
    /// All arguments are keyword-only and optional. `part_size`, `multipart_threshold`,
    /// `memory_limit` are in bytes (see `KiB`, `MiB`, `GiB`).
    #[new]
    #[pyo3(signature = (
        *,
        region=None,
        profile=None,
        endpoint_url=None,
        force_path_style=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
        part_size=None,
        multipart_threshold=None,
        concurrency=None,
        target_throughput_gbps=None,
        memory_limit=None,
        memory_limit_fraction=None,
        read_ahead_parts=None,
        request_checksum_calculation=None,
        response_checksum_validation=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        region: Option<String>,
        profile: Option<String>,
        endpoint_url: Option<String>,
        force_path_style: Option<bool>,
        aws_access_key_id: Option<String>,
        aws_secret_access_key: Option<String>,
        aws_session_token: Option<String>,
        part_size: Option<u64>,
        multipart_threshold: Option<u64>,
        concurrency: Option<usize>,
        target_throughput_gbps: Option<u64>,
        memory_limit: Option<usize>,
        memory_limit_fraction: Option<f64>,
        read_ahead_parts: Option<usize>,
        request_checksum_calculation: Option<String>,
        response_checksum_validation: Option<String>,
    ) -> PyResult<Self> {
        let settings = Settings {
            region,
            profile,
            endpoint_url,
            force_path_style,
            aws_access_key_id,
            aws_secret_access_key,
            aws_session_token,
            part_size,
            multipart_threshold,
            concurrency,
            target_throughput_gbps,
            memory_limit,
            memory_limit_fraction,
            read_ahead_parts,
            request_checksum_calculation,
            response_checksum_validation,
        };
        settings.validate()?;
        // Pick up any logging configuration made since the module was imported.
        crate::logging::refresh();

        let resolved =
            runtime::block_on_forever(py, settings.resolve())?.map_err(PyValueError::new_err)?;
        let runtime = runtime::handle();
        let client = py.detach(|| {
            let _entered = runtime.enter();
            Client::new(resolved.config)
        });
        Ok(Self {
            inner: ProcessLocal::new(Arc::new(Inner {
                client: RwLock::new(Some(client)),
                tracker: TaskTracker::new(),
                cancel: CancellationToken::new(),
                region: resolved.region,
                multipart_threshold: resolved.multipart_threshold,
            })),
        })
    }

    /// The AWS region requests are sent to.
    #[getter]
    fn region(&self) -> PyResult<String> {
        Ok(self.inner()?.region.clone())
    }

    /// Whether `close()` has been called.
    #[getter]
    fn closed(&self) -> PyResult<bool> {
        Ok(self.inner()?.is_closed())
    }

    /// Upload a local file to `s3://bucket/key`.
    #[pyo3(signature = (path, bucket, key, **options))]
    fn upload_file(
        &self,
        py: Python<'_>,
        path: PathBuf,
        bucket: String,
        key: String,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Transfer> {
        let options = UploadOptions::parse("upload_file", options)?;
        let metadata = std::fs::metadata(&path).map_err(|err| io::os_error(py, &err, &path))?;
        if metadata.is_dir() {
            return Err(io::os_error(
                py,
                &std::io::Error::from(std::io::ErrorKind::IsADirectory),
                &path,
            ));
        }
        let body = InputStream::from_path(&path)
            .map_err(|err| to_pyerr(py, &aws_sdk_s3_transfer_manager::error::Error::from(err)))?;
        let source = format!("{:?}", path.display().to_string());
        self.launch_upload(
            py,
            body,
            bucket,
            key,
            options,
            source,
            PyErrorSlot::default(),
        )
    }

    /// Upload a bytes-like object to `s3://bucket/key`.
    ///
    /// `bytes` are uploaded without being copied; other buffers (`bytearray`, `memoryview`, ...)
    /// are copied first, so they may be modified once this returns.
    #[pyo3(signature = (data, bucket, key, **options))]
    fn upload_bytes(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        bucket: String,
        key: String,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Transfer> {
        let options = UploadOptions::parse("upload_bytes", options)?;
        let data = io::bytes_from_buffer(data)?;
        let source = format!("{} bytes", data.len());
        self.launch_upload(
            py,
            InputStream::from(data),
            bucket,
            key,
            options,
            source,
            PyErrorSlot::default(),
        )
    }

    /// Upload the rest of a readable binary file object to `s3://bucket/key`.
    ///
    /// Reading starts at the file's current position. A seekable file no larger than the
    /// multipart threshold is read before this returns; anything else is read part by part while
    /// the upload runs, so the file must stay open until it finishes.
    #[pyo3(signature = (fileobj, bucket, key, **options))]
    fn upload_fileobj(
        &self,
        py: Python<'_>,
        fileobj: &Bound<'_, PyAny>,
        bucket: String,
        key: String,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Transfer> {
        let options = UploadOptions::parse("upload_fileobj", options)?;
        if !fileobj.hasattr(intern!(py, "read"))? {
            return Err(PyTypeError::new_err(
                "fileobj must be a readable binary file object",
            ));
        }
        let remaining = io::remaining_len(fileobj)?;
        let errors = PyErrorSlot::default();
        let body = match remaining {
            Some(len) if len <= self.inner()?.multipart_threshold => {
                InputStream::from(io::read_exact_or_eof(fileobj, len as usize)?)
            }
            _ => PyReader::input_stream(fileobj.clone().unbind(), remaining, errors.clone()),
        };
        let source = fileobj.repr()?.to_string();
        self.launch_upload(py, body, bucket, key, options, source, errors)
    }

    /// Download `s3://bucket/key` to a local file.
    ///
    /// The object is written to a temporary file next to `path`, which replaces `path` only once
    /// the download succeeds; on failure or cancellation it is removed.
    #[pyo3(signature = (bucket, key, path, **options))]
    fn download_file(
        &self,
        py: Python<'_>,
        bucket: String,
        key: String,
        path: PathBuf,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Transfer> {
        let (builder, priority) =
            self.download_builder(py, "download_file", &bucket, &key, options)?;
        if path.is_dir() {
            return Err(io::os_error(
                py,
                &std::io::Error::from(std::io::ErrorKind::IsADirectory),
                &path,
            ));
        }
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        require_dir(py, parent)?;
        let destination = path.clone();
        let handle =
            runtime::block_on_forever(py, async move { builder.write_to_path(destination).await })?
                .map_err(|err| to_pyerr(py, &err))?;
        let monitor = handle.monitor();
        let description = format!(
            "download of s3://{bucket}/{key} to {:?}",
            path.display().to_string()
        );
        Ok(self.inner()?.launcher().launch(
            description.clone(),
            monitor.clone(),
            priority,
            |cancel| transfer::drive(handle, monitor, cancel, description, PyErrorSlot::default()),
        ))
    }

    /// Download `s3://bucket/key` into memory; the result is `bytes`.
    #[pyo3(signature = (bucket, key, **options))]
    fn download_bytes(
        &self,
        py: Python<'_>,
        bucket: String,
        key: String,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Transfer> {
        let builder = self.download_builder(py, "download_bytes", &bucket, &key, options)?;
        let description = format!("download of s3://{bucket}/{key}");
        self.launch_body(py, builder, Sink::Memory, description)
    }

    /// Download `s3://bucket/key`, writing it in order to a writable binary file object.
    ///
    /// Writes happen on a background thread while the download runs, so the file must stay open
    /// until it finishes.
    #[pyo3(signature = (bucket, key, fileobj, **options))]
    fn download_fileobj(
        &self,
        py: Python<'_>,
        bucket: String,
        key: String,
        fileobj: &Bound<'_, PyAny>,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Transfer> {
        if !fileobj.hasattr(intern!(py, "write"))? {
            return Err(PyTypeError::new_err(
                "fileobj must be a writable binary file object",
            ));
        }
        let builder = self.download_builder(py, "download_fileobj", &bucket, &key, options)?;
        let description = format!("download of s3://{bucket}/{key} to {}", fileobj.repr()?);
        let sink = Sink::FileObj(Arc::new(fileobj.clone().unbind()));
        self.launch_body(py, builder, sink, description)
    }

    /// Stream `s3://bucket/key` as an iterator of `bytes` chunks; see `DownloadStream`.
    #[pyo3(signature = (bucket, key, **options))]
    fn download_stream(
        &self,
        py: Python<'_>,
        bucket: String,
        key: String,
        options: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<DownloadStream> {
        let (builder, priority) =
            self.download_builder(py, "download_stream", &bucket, &key, options)?;
        let handle = initiate(py, || builder.initiate())?;
        Ok(DownloadStream::new(
            handle,
            format!("download stream of s3://{bucket}/{key}"),
            self.inner()?.cancel.child_token(),
            priority,
        ))
    }

    /// Upload the files in a local directory to `bucket`, under `key_prefix`.
    ///
    /// Keys are the files' paths relative to `directory`, joined with `delimiter` (default
    /// `"/"`) and prefixed with `key_prefix`. `filter` is called with each file's path and
    /// excludes the file when it returns false.
    #[pyo3(signature = (
        directory,
        bucket,
        *,
        key_prefix=None,
        delimiter=None,
        recursive=true,
        follow_symlinks=false,
        filter=None,
        failure_policy="abort",
        max_concurrent_uploads=None,
        priority=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn upload_directory(
        &self,
        py: Python<'_>,
        directory: PathBuf,
        bucket: String,
        key_prefix: Option<String>,
        delimiter: Option<String>,
        recursive: bool,
        follow_symlinks: bool,
        filter: Option<Py<PyAny>>,
        failure_policy: &str,
        max_concurrent_uploads: Option<usize>,
        priority: Option<i64>,
    ) -> PyResult<Transfer> {
        let policy = self::failure_policy(py, failure_policy)?;
        let priority = options::priority(py, priority)?;
        require_dir(py, &directory)?;
        let mut walker = FsWalker::builder()
            .recursive(recursive)
            .follow_symlinks(follow_symlinks);
        if let Some(filter) = filter {
            let filter = Arc::new(filter);
            walker = walker
                .filter(move |entry| call_filter(&filter, |py| entry.path().into_pyobject(py)));
        }
        let description = format!(
            "upload of directory {:?} to s3://{bucket}/{}",
            directory.display().to_string(),
            key_prefix.as_deref().unwrap_or_default()
        );
        let builder = self
            .inner()?
            .client()?
            .upload_objects()
            .bucket(bucket)
            .source(directory)
            .walker(walker.build())
            .set_key_prefix(key_prefix)
            .set_delimiter(delimiter)
            .failure_policy(policy)
            .set_max_concurrent_uploads(max_concurrent_uploads);
        let handle = initiate(py, || builder.initiate())?;
        let monitor = handle.monitor();
        Ok(self.inner()?.launcher().launch(
            description.clone(),
            monitor.clone(),
            priority,
            |cancel| transfer::drive(handle, monitor, cancel, description, PyErrorSlot::default()),
        ))
    }

    /// Download the objects under `key_prefix` in `bucket` to a local directory.
    ///
    /// Local paths are the keys with `key_prefix` removed, split on `delimiter` (default `"/"`).
    /// `directory` is created if needed. `filter` is called with an `ObjectSummary` for each
    /// listed object and excludes it when it returns false.
    #[pyo3(signature = (
        bucket,
        directory,
        *,
        key_prefix=None,
        delimiter=None,
        filter=None,
        failure_policy="abort",
        max_concurrent_downloads=None,
        priority=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn download_directory(
        &self,
        py: Python<'_>,
        bucket: String,
        directory: PathBuf,
        key_prefix: Option<String>,
        delimiter: Option<String>,
        filter: Option<Py<PyAny>>,
        failure_policy: &str,
        max_concurrent_downloads: Option<usize>,
        priority: Option<i64>,
    ) -> PyResult<Transfer> {
        let policy = self::failure_policy(py, failure_policy)?;
        let priority = options::priority(py, priority)?;
        std::fs::create_dir_all(&directory).map_err(|err| io::os_error(py, &err, &directory))?;
        let description = format!(
            "download of s3://{bucket}/{} to directory {:?}",
            key_prefix.as_deref().unwrap_or_default(),
            directory.display().to_string()
        );
        let mut builder = self
            .inner()?
            .client()?
            .download_objects()
            .bucket(bucket)
            .destination(directory)
            .set_key_prefix(key_prefix.clone())
            .set_delimiter(delimiter)
            .failure_policy(policy)
            .set_max_concurrent_downloads(max_concurrent_downloads);
        if let Some(filter) = filter {
            let filter = Arc::new(filter);
            let mut walker = S3Walker::builder().filter(move |object| {
                !is_folder_marker(object)
                    && call_filter(&filter, |py| {
                        Ok(Bound::new(py, ObjectSummary::new(object))?.into_any())
                    })
            });
            if let Some(prefix) = key_prefix {
                walker = walker.prefix(prefix);
            }
            builder = builder.walker(walker.build());
        }
        let handle = initiate(py, || builder.initiate())?;
        let monitor = handle.monitor();
        Ok(self.inner()?.launcher().launch(
            description.clone(),
            monitor.clone(),
            priority,
            |cancel| transfer::drive(handle, monitor, cancel, description, PyErrorSlot::default()),
        ))
    }

    /// Wait for every transfer started by this manager to finish, then release its resources.
    ///
    /// With `cancel=True`, transfers still in flight are cancelled first. New transfers cannot
    /// be started once this is called. Idempotent.
    #[pyo3(signature = (*, cancel=false))]
    fn close(&self, py: Python<'_>, cancel: bool) -> PyResult<()> {
        self.close_inner(py, cancel)
    }

    /// Asynchronous `close()`.
    #[pyo3(signature = (*, cancel=false))]
    fn aclose<'py>(&self, py: Python<'py>, cancel: bool) -> PyResult<Bound<'py, PyAny>> {
        self.aclose_inner(py, cancel)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Wait for outstanding transfers, cancelling them if the block raised.
    #[pyo3(signature = (exc_type, _exc_value, _traceback, /))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        self.close_inner(py, exc_type.is_some())
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let this: Py<Self> = slf.into();
        runtime::future_into_py(py, async move { Ok(this) })
    }

    #[pyo3(signature = (exc_type, _exc_value, _traceback, /))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<&Bound<'py, PyAny>>,
        _exc_value: Option<&Bound<'py, PyAny>>,
        _traceback: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.aclose_inner(py, exc_type.is_some())
    }

    fn __repr__(&self) -> PyResult<String> {
        let inner = self.inner()?;
        let closed = if inner.is_closed() { ", closed" } else { "" };
        Ok(format!(
            "<TransferManager region={:?}{closed}>",
            inner.region
        ))
    }
}
