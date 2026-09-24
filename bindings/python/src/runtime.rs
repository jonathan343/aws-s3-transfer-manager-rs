/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The tokio runtime that drives the bindings, and the helpers Python threads use to wait on it.
//!
//! Transfers themselves run on the transfer manager's own worker threads. This runtime only
//! carries the bindings' bookkeeping: loading AWS configuration, the per-transfer driver tasks,
//! the asyncio bridge, and the blocking pool used to call back into Python (file objects,
//! filters).

use std::future::Future;
use std::mem::ManuallyDrop;
use std::pin::{pin, Pin};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3_async_runtimes::generic::{ContextExt, JoinError, Runtime as GenericRuntime};
use pyo3_async_runtimes::TaskLocals;

/// How often a Python thread blocked on a transfer wakes to service signals (e.g. Ctrl+C).
const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(100);

/// Upper bound on the runtime's worker threads; the work it carries is light.
const MAX_WORKER_THREADS: usize = 4;

struct ProcessRuntime {
    pid: u32,
    handle: tokio::runtime::Handle,
}

static RUNTIME: Mutex<Option<ProcessRuntime>> = Mutex::new(None);

/// Handle to the bindings runtime for the current process, starting it on first use.
///
/// `fork()` copies memory but not threads, so a child cannot use the runtime it inherited. The
/// child starts its own instead, and the inherited one is leaked rather than dropped: dropping it
/// would wait on worker threads that do not exist in the child.
pub(crate) fn handle() -> tokio::runtime::Handle {
    let pid = std::process::id();
    let mut current = RUNTIME
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(rt) = current.as_ref().filter(|rt| rt.pid == pid) {
        return rt.handle.clone();
    }
    let workers = std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .clamp(1, MAX_WORKER_THREADS);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("aws-s3-tm-python")
        .enable_all()
        .build()
        .expect("failed to start the tokio runtime for aws_s3_transfer_manager");
    let handle = runtime.handle().clone();
    // The runtime lives for the rest of the process.
    std::mem::forget(runtime);
    *current = Some(ProcessRuntime {
        pid,
        handle: handle.clone(),
    });
    handle
}

/// Block the calling Python thread until `fut` resolves or `timeout` elapses (`Ok(None)`).
///
/// The thread detaches from the interpreter while it waits, so other Python threads keep running,
/// and wakes every [`SIGNAL_CHECK_INTERVAL`] to run signal handlers: a `KeyboardInterrupt` is
/// raised promptly instead of after the transfer finishes. The future is only polled, never
/// dropped early, when that happens between waits.
pub(crate) fn block_on<F>(
    py: Python<'_>,
    fut: F,
    timeout: Option<Duration>,
) -> PyResult<Option<F::Output>>
where
    F: Future + Send,
    F::Output: Send,
{
    let runtime = handle();
    let deadline = timeout.map(|t| Instant::now() + t);
    let mut fut = pin!(fut);
    loop {
        let slice = deadline.map_or(SIGNAL_CHECK_INTERVAL, |deadline| {
            deadline
                .saturating_duration_since(Instant::now())
                .min(SIGNAL_CHECK_INTERVAL)
        });
        let polled = py.detach(|| {
            runtime.block_on(async { tokio::time::timeout(slice, fut.as_mut()).await.ok() })
        });
        if let Some(output) = polled {
            return Ok(Some(output));
        }
        py.check_signals()?;
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Ok(None);
        }
    }
}

/// [`block_on`] without a timeout.
pub(crate) fn block_on_forever<F>(py: Python<'_>, fut: F) -> PyResult<F::Output>
where
    F: Future + Send,
    F::Output: Send,
{
    Ok(block_on(py, fut, None)?.expect("no timeout was set"))
}

/// Convert a Rust future into a Python awaitable bound to the running asyncio loop.
pub(crate) fn future_into_py<F, T>(py: Python<'_>, fut: F) -> PyResult<Bound<'_, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    pyo3_async_runtimes::generic::future_into_py::<AsyncioBridge, F, T>(py, fut)
}

/// A value tied to the process that created it.
///
/// Objects such as the transfer manager's client own threads, which a forked child does not
/// have. In a child this refuses access with a clear error instead of hanging, and leaks the
/// value on drop instead of joining threads that do not exist.
///
/// Dropping the value in its own process happens on the runtime's blocking pool. The last
/// reference to a client joins its worker threads, which must not happen on a Python thread:
/// those threads may themselves be waiting to call into Python.
pub(crate) struct ProcessLocal<T: Send + 'static> {
    pid: u32,
    value: ManuallyDrop<T>,
}

impl<T: Send + 'static> ProcessLocal<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            pid: std::process::id(),
            value: ManuallyDrop::new(value),
        }
    }

    pub(crate) fn get(&self) -> PyResult<&T> {
        if self.pid == std::process::id() {
            Ok(&self.value)
        } else {
            Err(PyRuntimeError::new_err(
                "this object was created before the process forked and cannot be used in the \
                 child; create a new TransferManager in the child process instead",
            ))
        }
    }
}

impl<T: Send + 'static> Drop for ProcessLocal<T> {
    fn drop(&mut self) {
        if self.pid != std::process::id() {
            return;
        }
        // SAFETY: `value` is never used again; `drop` runs once.
        let value = unsafe { ManuallyDrop::take(&mut self.value) };
        handle().spawn_blocking(move || drop(value));
    }
}

/// The glue `pyo3-async-runtimes` needs to spawn onto (and scope asyncio task locals within)
/// the bindings runtime.
struct AsyncioBridge;

tokio::task_local! {
    static TASK_LOCALS: OnceLock<TaskLocals>;
}

struct TaskJoinError(tokio::task::JoinError);

impl JoinError for TaskJoinError {
    fn is_panic(&self) -> bool {
        self.0.is_panic()
    }

    fn into_panic(self) -> Box<dyn std::any::Any + Send + 'static> {
        self.0.into_panic()
    }
}

struct TaskHandle(tokio::task::JoinHandle<()>);

impl Future for TaskHandle {
    type Output = Result<(), TaskJoinError>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map_err(TaskJoinError)
    }
}

impl GenericRuntime for AsyncioBridge {
    type JoinError = TaskJoinError;
    type JoinHandle = TaskHandle;

    fn spawn<F>(fut: F) -> Self::JoinHandle
    where
        F: Future<Output = ()> + Send + 'static,
    {
        TaskHandle(handle().spawn(fut))
    }

    fn spawn_blocking<F>(f: F) -> Self::JoinHandle
    where
        F: FnOnce() + Send + 'static,
    {
        TaskHandle(handle().spawn_blocking(f))
    }
}

impl ContextExt for AsyncioBridge {
    fn scope<F, R>(locals: TaskLocals, fut: F) -> Pin<Box<dyn Future<Output = R> + Send>>
    where
        F: Future<Output = R> + Send + 'static,
    {
        let cell = OnceLock::new();
        let _ = cell.set(locals);
        Box::pin(TASK_LOCALS.scope(cell, fut))
    }

    fn get_task_locals() -> Option<TaskLocals> {
        TASK_LOCALS
            .try_with(|cell| cell.get().cloned())
            .unwrap_or_default()
    }
}
