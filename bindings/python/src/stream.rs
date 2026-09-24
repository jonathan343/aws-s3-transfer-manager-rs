/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! `DownloadStream`: an object's content as an (async) iterator of in-order chunks.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use aws_sdk_s3_transfer_manager::operation::download::{
    DownloadHandle, DownloadOutput, ObjectMetadata as RustObjectMetadata,
};
use aws_sdk_s3_transfer_manager::TransferMonitor;
use bytes::{Buf, Bytes};
use pyo3::exceptions::{PyStopAsyncIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio_util::sync::CancellationToken;

use crate::errors::ErrorInfo;
use crate::runtime::{self, ProcessLocal};
use crate::transfer::{concat_bytes, Failure};
use crate::types::{self, DownloadResult, ObjectMetadata};

struct StreamState {
    description: String,
    monitor: TransferMonitor,
    cancel: CancellationToken,
    /// `None` once the body is exhausted, the download failed, or the stream is closed.
    handle: tokio::sync::Mutex<Option<DownloadHandle>>,
    metadata: OnceLock<RustObjectMetadata>,
    /// Set once the whole body has been delivered.
    output: OnceLock<DownloadOutput>,
    /// Set if the download failed; raised again by later reads.
    error: OnceLock<ErrorInfo>,
    closed: AtomicBool,
}

/// One delivered chunk, or the end of the stream.
enum Next {
    Chunk(Vec<Bytes>, usize),
    End,
}

impl StreamState {
    fn fail(&self, failure: Failure) -> Failure {
        if let Failure::Transfer(info) = &failure {
            let _ = self.error.set(info.clone());
        }
        failure
    }

    fn check_readable(&self) -> Result<(), Failure> {
        if let Some(info) = self.error.get() {
            return Err(Failure::Transfer(info.clone()));
        }
        Ok(())
    }

    /// Wait for the object's metadata (available once the download has discovered the object).
    async fn metadata(&self) -> Result<RustObjectMetadata, Failure> {
        if let Some(metadata) = self.metadata.get() {
            return Ok(metadata.clone());
        }
        self.check_readable()?;
        let mut guard = self.handle.lock().await;
        if let Some(metadata) = self.metadata.get() {
            return Ok(metadata.clone());
        }
        let Some(handle) = guard.as_ref() else {
            return Err(self.closed_error());
        };
        match handle.object_meta().await {
            Ok(metadata) => Ok(self.metadata.get_or_init(|| metadata.clone()).clone()),
            // Discovery errors are generic; the handle's `join()` has the cause.
            Err(_) => {
                self.finish(&mut guard).await?;
                self.metadata
                    .get()
                    .cloned()
                    .ok_or_else(|| self.closed_error())
            }
        }
    }

    async fn next(&self) -> Result<Next, Failure> {
        self.check_readable()?;
        let mut guard = self.handle.lock().await;
        let Some(handle) = guard.as_mut() else {
            if self.output.get().is_some() {
                return Ok(Next::End);
            }
            self.check_readable()?;
            return Err(self.closed_error());
        };
        let next = tokio::select! {
            biased;
            () = self.cancel.cancelled() => {
                if let Some(handle) = guard.take() {
                    handle.abort().await;
                }
                return Err(self.fail(Failure::cancelled(&self.description)));
            }
            next = handle.body_mut().next() => next,
        };
        match next {
            Some(Ok(chunk)) => {
                if self.metadata.get().is_none() {
                    if let Ok(metadata) = handle.object_meta().await {
                        let _ = self.metadata.set(metadata.clone());
                    }
                }
                let len = chunk.data.remaining();
                Ok(Next::Chunk(chunk.data.into_segments().collect(), len))
            }
            // The end of the body, or a generic error whose cause `join()` reports.
            _ => self.finish(&mut guard).await.map(|()| Next::End),
        }
    }

    /// Join the finished (or failed) download, recording its output or error.
    async fn finish(&self, guard: &mut Option<DownloadHandle>) -> Result<(), Failure> {
        let Some(handle) = guard.take() else {
            return Err(self.closed_error());
        };
        match handle.join().await {
            Ok(output) => {
                let _ = self.metadata.set(output.object_meta.clone());
                let _ = self.output.set(output);
                Ok(())
            }
            Err(err) => Err(self.fail(Failure::from_error(&err))),
        }
    }

    fn closed_error(&self) -> Failure {
        Failure::Python(PyValueError::new_err(format!(
            "I/O operation on closed {}",
            self.description
        )))
    }

    /// Stop the download if it is still running, and release it.
    async fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.cancel.cancel();
        if let Some(handle) = self.handle.lock().await.take() {
            handle.abort().await;
        }
    }
}

fn chunk_to_py(py: Python<'_>, next: Next) -> PyResult<Option<Py<PyBytes>>> {
    match next {
        Next::Chunk(segments, len) => Ok(Some(concat_bytes(py, &segments, len)?.unbind())),
        Next::End => Ok(None),
    }
}

/// The content of an S3 object, delivered in order as `bytes` chunks.
///
/// Returned by `TransferManager.download_stream()`, with the download already running. Iterate
/// it with `for` or `async for`. Use it as a context manager (`with` / `async with`) so that
/// leaving the block early stops the download; entering the block waits until the object's
/// `metadata` is available. After the last chunk, `result` holds the download's result.
///
/// The object is fetched in parallel ranged requests, prefetching ahead of the consumer within
/// the transfer manager's memory budget.
#[pyclass(frozen, module = "aws_s3_transfer_manager")]
pub struct DownloadStream {
    state: ProcessLocal<Arc<StreamState>>,
}

impl DownloadStream {
    pub(crate) fn new(
        handle: DownloadHandle,
        description: String,
        cancel: CancellationToken,
        priority: Option<u8>,
    ) -> Self {
        let monitor = handle.monitor();
        if let Some(priority) = priority {
            monitor.scheduling().set_priority(priority);
        }
        Self {
            state: ProcessLocal::new(Arc::new(StreamState {
                description,
                monitor,
                cancel,
                handle: tokio::sync::Mutex::new(Some(handle)),
                metadata: OnceLock::new(),
                output: OnceLock::new(),
                error: OnceLock::new(),
                closed: AtomicBool::new(false),
            })),
        }
    }

    fn state(&self) -> PyResult<&Arc<StreamState>> {
        self.state.get()
    }
}

#[pymethods]
impl DownloadStream {
    /// Metadata of the object being downloaded, waiting for it if necessary.
    #[getter]
    fn metadata(&self, py: Python<'_>) -> PyResult<ObjectMetadata> {
        let state = self.state()?;
        match runtime::block_on_forever(py, state.metadata())? {
            Ok(metadata) => Ok(ObjectMetadata::new(metadata)),
            Err(failure) => Err(failure.into_pyerr(py)),
        }
    }

    /// The download's result once the whole body has been read, else `None`.
    #[getter]
    fn result(&self, py: Python<'_>) -> PyResult<Option<DownloadResult>> {
        self.state()?
            .output
            .get()
            .map(|output| DownloadResult::new(py, output.clone()))
            .transpose()
    }

    /// A snapshot of the download's progress.
    #[getter]
    fn metrics(&self) -> PyResult<types::TransferMetrics> {
        Ok(types::TransferMetrics::new(self.state()?.monitor.metrics()))
    }

    /// Whether the stream has been closed.
    #[getter]
    fn closed(&self) -> PyResult<bool> {
        Ok(self.state()?.closed.load(Ordering::Acquire))
    }

    /// Change the download's scheduling priority, from 1 (lowest) to 255 (highest).
    fn set_priority(&self, py: Python<'_>, priority: i64) -> PyResult<()> {
        let priority = u8::try_from(priority)
            .ok()
            .filter(|p| *p >= 1)
            .ok_or_else(|| {
                crate::errors::invalid_input(py, format!("priority must be 1-255, got {priority}"))
            })?;
        self.state()?.monitor.scheduling().set_priority(priority);
        Ok(())
    }

    /// Stop the download if it has not finished, waiting for it to wind down. Idempotent.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let state = self.state()?;
        runtime::block_on_forever(py, state.close())
    }

    /// Asynchronous `close()`.
    fn aclose<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = Arc::clone(self.state()?);
        runtime::future_into_py(py, async move {
            state.close().await;
            Ok(())
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
        let state = self.state()?;
        match runtime::block_on_forever(py, state.next())? {
            Ok(next) => chunk_to_py(py, next),
            Err(failure) => Err(failure.into_pyerr(py)),
        }
    }

    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = Arc::clone(self.state()?);
        runtime::future_into_py(py, async move {
            let next = state.next().await;
            Python::attach(|py| match next {
                Ok(next) => chunk_to_py(py, next)?.ok_or_else(|| PyStopAsyncIteration::new_err(())),
                Err(failure) => Err(failure.into_pyerr(py)),
            })
        })
    }

    fn __enter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<PyRef<'py, Self>> {
        slf.metadata(py)?;
        Ok(slf)
    }

    #[pyo3(signature = (_exc_type, _exc_value, _traceback, /))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.close(py)
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = Arc::clone(slf.state()?);
        let this: Py<Self> = slf.into();
        runtime::future_into_py(py, async move {
            let metadata = state.metadata().await;
            Python::attach(|py| match metadata {
                Ok(_) => Ok(this),
                Err(failure) => Err(failure.into_pyerr(py)),
            })
        })
    }

    #[pyo3(signature = (_exc_type, _exc_value, _traceback, /))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: &Bound<'py, PyAny>,
        _exc_value: &Bound<'py, PyAny>,
        _traceback: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.aclose(py)
    }

    fn __repr__(&self) -> PyResult<String> {
        let state = self.state()?;
        let status = if state.closed.load(Ordering::Acquire) {
            "closed"
        } else if state.output.get().is_some() {
            "exhausted"
        } else if state.error.get().is_some() {
            "failed"
        } else {
            "open"
        };
        Ok(format!("<DownloadStream {} ({status})>", state.description))
    }
}
