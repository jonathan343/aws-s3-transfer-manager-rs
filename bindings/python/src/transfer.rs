/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! `Transfer`: the future-like handle every transfer method returns.
//!
//! A transfer is driven to completion by a task on the bindings runtime, which owns the transfer
//! manager's handle, joins it (or aborts it when cancelled), and publishes the outcome. The Python
//! object only observes: it waits for the outcome (`result()` or `await`), reads progress through a
//! [`TransferMonitor`], and requests cancellation. Dropping it does not stop the transfer, the same
//! as a `concurrent.futures.Future`.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use aws_sdk_s3_transfer_manager::error::Error;
use aws_sdk_s3_transfer_manager::operation::download::{DownloadHandle, DownloadOutput};
use aws_sdk_s3_transfer_manager::operation::upload::UploadOutput;
use aws_sdk_s3_transfer_manager::types::TransferStatus;
use aws_sdk_s3_transfer_manager::TransferMonitor;
use bytes::Bytes;
use futures_util::FutureExt;
use pyo3::exceptions::PyTimeoutError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyType};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::errors::{invalid_input, ErrorInfo};
use crate::io::PyErrorSlot;
use crate::runtime::{self, ProcessLocal};
use crate::types::{
    self, DirectoryDownloadResult, DirectoryUploadResult, DownloadResult, UploadResult,
};

/// A successful transfer's value, before conversion to Python.
pub(crate) enum Outcome {
    Upload(Box<UploadOutput>),
    Download(Box<DownloadOutput>),
    Bytes { segments: Vec<Bytes>, len: usize },
    UploadDirectory(DirectoryUploadResult),
    DownloadDirectory(DirectoryDownloadResult),
}

impl Outcome {
    fn into_py(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(match self {
            Self::Upload(output) => Py::new(py, UploadResult::new(*output))?.into_any(),
            Self::Download(output) => Py::new(py, DownloadResult::new(py, *output)?)?.into_any(),
            Self::Bytes { segments, len } => concat_bytes(py, &segments, len)?.into_any().unbind(),
            Self::UploadDirectory(result) => Py::new(py, result)?.into_any(),
            Self::DownloadDirectory(result) => Py::new(py, result)?.into_any(),
        })
    }
}

/// Copy byte segments into a single Python `bytes`.
pub(crate) fn concat_bytes<'py>(
    py: Python<'py>,
    segments: &[Bytes],
    len: usize,
) -> PyResult<Bound<'py, PyBytes>> {
    if let [segment] = segments {
        return Ok(PyBytes::new(py, segment));
    }
    PyBytes::new_with(py, len, |buf| {
        let mut offset = 0;
        for segment in segments {
            buf[offset..offset + segment.len()].copy_from_slice(segment);
            offset += segment.len();
        }
        Ok(())
    })
}

/// Why a transfer did not produce a value.
pub(crate) enum Failure {
    /// The transfer manager reported an error.
    Transfer(ErrorInfo),
    /// Python code the transfer called into (a file object) raised.
    Python(PyErr),
}

impl Failure {
    pub(crate) fn from_error(err: &Error) -> Self {
        Self::Transfer(ErrorInfo::from_error(err))
    }

    /// Prefer the file object's own exception, when it is what failed the transfer.
    fn from_error_or(err: &Error, errors: &PyErrorSlot) -> Self {
        match errors.take() {
            Some(err) => Self::Python(err),
            None => Self::from_error(err),
        }
    }

    pub(crate) fn cancelled(description: &str) -> Self {
        Self::Transfer(ErrorInfo::cancelled(description))
    }

    fn is_cancelled(&self) -> bool {
        matches!(self, Self::Transfer(info) if info.is_cancelled())
    }

    pub(crate) fn into_pyerr(self, py: Python<'_>) -> PyErr {
        match self {
            Self::Transfer(info) => info.to_pyerr(py),
            Self::Python(err) => err,
        }
    }
}

pub(crate) type TransferResult = Result<Outcome, Failure>;

/// State shared between a [`Transfer`] and the task driving it.
pub(crate) struct TransferState {
    description: String,
    monitor: TransferMonitor,
    cancel: CancellationToken,
    /// The outcome, until it is converted to Python (which moves it into `converted`).
    outcome: Mutex<Option<TransferResult>>,
    final_status: OnceLock<TransferStatus>,
    done: tokio::sync::watch::Sender<bool>,
    converted: PyOnceLock<Result<Py<PyAny>, PyErr>>,
}

impl TransferState {
    fn complete(&self, result: TransferResult) {
        let status = match &result {
            Ok(_) => TransferStatus::Completed,
            Err(failure) if failure.is_cancelled() => TransferStatus::Cancelled,
            Err(_) => TransferStatus::Failed,
        };
        *self.outcome.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
        let _ = self.final_status.set(status);
        self.done.send_replace(true);
    }

    fn is_done(&self) -> bool {
        *self.done.borrow()
    }

    async fn wait(&self) {
        let mut done = self.done.subscribe();
        let _ = done.wait_for(|done| *done).await;
    }

    /// Request cancellation; `false` if the transfer has already finished.
    ///
    /// Like `asyncio.Task.cancel()`, a request that races with the transfer finishing may lose:
    /// the driver aborts only work that is still running.
    fn cancel(&self) -> bool {
        if self.is_done() {
            return false;
        }
        self.cancel.cancel();
        true
    }

    /// The outcome as a Python value or exception, converted once and then shared.
    fn outcome_py(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let converted = self.converted.get_or_init(py, || {
            let outcome = self
                .outcome
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
                .expect("a finished transfer has an outcome");
            match outcome {
                Ok(outcome) => outcome.into_py(py),
                Err(failure) => Err(failure.into_pyerr(py)),
            }
        });
        match converted {
            Ok(value) => Ok(value.clone_ref(py)),
            Err(err) => Err(err.clone_ref(py)),
        }
    }
}

/// Everything needed to start a driver task for a transfer.
pub(crate) struct Launcher<'a> {
    pub(crate) tracker: &'a TaskTracker,
    pub(crate) cancel: &'a CancellationToken,
}

impl Launcher<'_> {
    /// Spawn `drive` (given the transfer's cancellation token) and return the Python handle.
    pub(crate) fn launch<F, Fut>(
        &self,
        description: String,
        monitor: TransferMonitor,
        priority: Option<u8>,
        drive: F,
    ) -> Transfer
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = TransferResult> + Send + 'static,
    {
        if let Some(priority) = priority {
            monitor.scheduling().set_priority(priority);
        }
        let cancel = self.cancel.child_token();
        let state = Arc::new(TransferState {
            description,
            monitor,
            cancel: cancel.clone(),
            outcome: Mutex::new(None),
            final_status: OnceLock::new(),
            done: tokio::sync::watch::Sender::new(false),
            converted: PyOnceLock::new(),
        });
        let driver = AssertUnwindSafe(drive(cancel)).catch_unwind();
        let driven = Arc::clone(&state);
        self.tracker.spawn_on(
            async move {
                // A panic must still finish the transfer, or `result()` would wait forever.
                let result = driver.await.unwrap_or_else(|panic| {
                    let reason = panic
                        .downcast_ref::<&str>()
                        .map(|s| (*s).to_owned())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_default();
                    Err(Failure::Transfer(ErrorInfo::internal(format!(
                        "internal error while running the transfer: {reason}"
                    ))))
                });
                driven.complete(result);
            },
            &runtime::handle(),
        );
        Transfer {
            state: ProcessLocal::new(state),
        }
    }
}

/// The parts of the transfer manager's handles a driver needs.
pub(crate) trait JoinHandle: Send + 'static {
    fn join(self) -> impl Future<Output = Result<Outcome, Error>> + Send;
    fn abort(self) -> impl Future<Output = ()> + Send;
}

impl JoinHandle for aws_sdk_s3_transfer_manager::operation::upload::UploadHandle {
    async fn join(self) -> Result<Outcome, Error> {
        self.join()
            .await
            .map(|output| Outcome::Upload(Box::new(output)))
    }

    async fn abort(self) {
        // An `AbortMultipartUpload` failure leaves nothing more to do: the transfer is cancelled
        // regardless, and S3 lifecycle rules reclaim orphaned parts.
        let _ = self.abort().await;
    }
}

impl JoinHandle for aws_sdk_s3_transfer_manager::operation::download::ManagedDownloadHandle {
    async fn join(self) -> Result<Outcome, Error> {
        self.join()
            .await
            .map(|output| Outcome::Download(Box::new(output)))
    }

    async fn abort(self) {
        self.abort().await
    }
}

impl JoinHandle for aws_sdk_s3_transfer_manager::operation::upload_objects::UploadObjectsHandle {
    async fn join(self) -> Result<Outcome, Error> {
        self.join()
            .await
            .map(|output| Outcome::UploadDirectory(DirectoryUploadResult::new(&output)))
    }

    async fn abort(self) {
        self.abort().await
    }
}

impl JoinHandle
    for aws_sdk_s3_transfer_manager::operation::download_objects::DownloadObjectsHandle
{
    async fn join(self) -> Result<Outcome, Error> {
        self.join()
            .await
            .map(|output| Outcome::DownloadDirectory(DirectoryDownloadResult::new(&output)))
    }

    async fn abort(self) {
        self.abort().await
    }
}

/// Drive a handle to completion: join it once it finishes, or abort it if cancelled first.
pub(crate) async fn drive<H: JoinHandle>(
    handle: H,
    monitor: TransferMonitor,
    cancel: CancellationToken,
    description: String,
    errors: PyErrorSlot,
) -> TransferResult {
    tokio::select! {
        biased;
        () = monitor.finished() => {}
        () = cancel.cancelled() => {
            if monitor.status() == TransferStatus::Active {
                handle.abort().await;
                return Err(Failure::cancelled(&description));
            }
        }
    }
    let mut outcome = handle
        .join()
        .await
        .map_err(|err| Failure::from_error_or(&err, &errors))?;
    if let Outcome::Upload(output) = &mut outcome {
        // The upload's own snapshot can predate its final byte counts; the transfer is over
        // now, so a fresh one is complete.
        output.metrics = monitor.metrics();
    }
    Ok(outcome)
}

/// Where a streamed download's body goes.
pub(crate) enum Sink {
    /// Collect it into memory (`download_bytes`).
    Memory,
    /// Write it to a Python file object (`download_fileobj`).
    FileObj(Arc<Py<PyAny>>),
}

/// Drive a streamed download, delivering its body to `sink` in order.
pub(crate) async fn drive_body(
    mut handle: DownloadHandle,
    sink: Sink,
    cancel: CancellationToken,
    description: String,
) -> TransferResult {
    let mut segments = Vec::new();
    let mut len = 0;
    loop {
        let next = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                handle.abort().await;
                return Err(Failure::cancelled(&description));
            }
            next = handle.body_mut().next() => next,
        };
        // A body error is generic; `join()` below reports the actual cause.
        let Some(Ok(chunk)) = next else { break };
        let chunk_segments = chunk.data.into_segments();
        match &sink {
            Sink::Memory => {
                for segment in chunk_segments {
                    len += segment.len();
                    segments.push(segment);
                }
            }
            Sink::FileObj(fileobj) => {
                let written =
                    crate::io::write_all(Arc::clone(fileobj), chunk_segments.collect()).await;
                if let Err(err) = written {
                    handle.abort().await;
                    return Err(Failure::Python(err));
                }
                // A write can outlast the download itself; honor a cancellation made meanwhile.
                if cancel.is_cancelled() {
                    handle.abort().await;
                    return Err(Failure::cancelled(&description));
                }
            }
        }
    }
    let output = handle
        .join()
        .await
        .map_err(|err| Failure::from_error(&err))?;
    Ok(match sink {
        Sink::Memory => Outcome::Bytes { segments, len },
        Sink::FileObj(_) => Outcome::Download(Box::new(output)),
    })
}

/// Cancels the transfer if an `await` on it is abandoned (e.g. by `asyncio.timeout`), matching
/// how awaiting an `asyncio.Task` propagates cancellation into it.
struct CancelOnDrop {
    state: Arc<TransferState>,
    armed: bool,
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.state.cancel();
        }
    }
}

fn seconds(py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Duration>> {
    timeout
        .map(|t| {
            Duration::try_from_secs_f64(t.max(0.0))
                .map_err(|_| invalid_input(py, format!("invalid timeout: {t}")))
        })
        .transpose()
}

/// A transfer in progress.
///
/// Returned by every `TransferManager` transfer method, with the transfer already running. Use
/// `result()` to block until it finishes, or `await` it from asyncio code; both return the
/// transfer's result or raise its exception. The transfer keeps running if this object is
/// discarded; use `cancel()` to stop it.
#[pyclass(frozen, module = "aws_s3_transfer_manager")]
pub struct Transfer {
    state: ProcessLocal<Arc<TransferState>>,
}

impl Transfer {
    fn state(&self) -> PyResult<&Arc<TransferState>> {
        self.state.get()
    }

    fn wait(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<&Arc<TransferState>> {
        let state = self.state()?;
        if !state.is_done() {
            let timeout = seconds(py, timeout)?;
            if runtime::block_on(py, state.wait(), timeout)?.is_none() {
                return Err(PyTimeoutError::new_err(format!(
                    "{} did not finish within the timeout",
                    state.description
                )));
            }
        }
        Ok(state)
    }
}

#[pymethods]
impl Transfer {
    /// Wait for the transfer to finish and return its result.
    ///
    /// Raises the transfer's exception if it failed, `TransferCancelledError` if it was
    /// cancelled, and `TimeoutError` if `timeout` seconds pass first (the transfer keeps
    /// running).
    #[pyo3(signature = (timeout=None))]
    fn result(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<PyAny>> {
        self.wait(py, timeout)?.outcome_py(py)
    }

    /// Wait for the transfer to finish and return its exception, or `None` if it succeeded.
    #[pyo3(signature = (timeout=None))]
    fn exception(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Py<PyAny>>> {
        match self.wait(py, timeout)?.outcome_py(py) {
            Ok(_) => Ok(None),
            Err(err) => Ok(Some(err.into_value(py).into_any())),
        }
    }

    /// Whether the transfer has finished (successfully, with an error, or cancelled).
    fn done(&self) -> PyResult<bool> {
        Ok(self.state()?.is_done())
    }

    /// Request cancellation of the transfer.
    ///
    /// Returns `False` if the transfer has already finished. Cancellation completes in the
    /// background; `result()` then raises `TransferCancelledError`, unless the transfer finished
    /// before the request took effect. A cancelled multipart upload is aborted.
    fn cancel(&self) -> PyResult<bool> {
        Ok(self.state()?.cancel())
    }

    /// Whether the transfer finished by being cancelled.
    fn cancelled(&self) -> PyResult<bool> {
        Ok(self.state()?.final_status.get() == Some(&TransferStatus::Cancelled))
    }

    /// The transfer's status: `TransferStatus.ACTIVE` until it has finished.
    #[getter]
    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let status = self
            .state()?
            .final_status
            .get()
            .copied()
            .unwrap_or(TransferStatus::Active);
        types::transfer_status(py, status)
    }

    /// A snapshot of the transfer's progress.
    #[getter]
    fn metrics(&self) -> PyResult<types::TransferMetrics> {
        Ok(types::TransferMetrics::new(self.state()?.monitor.metrics()))
    }

    /// Change the transfer's scheduling priority, from 1 (lowest) to 255 (highest).
    ///
    /// Transfers share throughput in proportion to their priority; the default is 128.
    fn set_priority(&self, py: Python<'_>, priority: i64) -> PyResult<()> {
        let priority = u8::try_from(priority)
            .ok()
            .filter(|p| *p >= 1)
            .ok_or_else(|| invalid_input(py, format!("priority must be 1-255, got {priority}")))?;
        self.state()?.monitor.scheduling().set_priority(priority);
        Ok(())
    }

    fn __await__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = Arc::clone(self.state()?);
        let awaitable = runtime::future_into_py(py, async move {
            let mut guard = CancelOnDrop {
                state: Arc::clone(&state),
                armed: true,
            };
            state.wait().await;
            guard.armed = false;
            Python::attach(|py| state.outcome_py(py))
        })?;
        awaitable.call_method0(pyo3::intern!(py, "__await__"))
    }

    #[classmethod]
    fn __class_getitem__<'py>(
        cls: &Bound<'py, PyType>,
        item: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = cls.py();
        py.import(pyo3::intern!(py, "types"))?
            .getattr(pyo3::intern!(py, "GenericAlias"))?
            .call1((cls, item))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let state = self.state()?;
        let status = self.status(py)?.getattr(pyo3::intern!(py, "value"))?;
        Ok(format!("<Transfer {} ({status})>", state.description))
    }
}
