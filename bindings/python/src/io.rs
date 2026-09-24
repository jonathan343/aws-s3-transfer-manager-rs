/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Moving bytes between Python objects and the transfer manager.
//!
//! Python file objects are only touched on the runtime's blocking pool, attached to the
//! interpreter, so the transfer manager's worker threads never wait on Python.

use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{ready, Context, Poll};

use aws_sdk_s3_transfer_manager::io::{InputStream, PartData, PartStream, SizeHint, StreamContext};
use bytes::{Bytes, BytesMut};
use pyo3::exceptions::{PyOSError, PyTypeError};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes};
use tokio::task::JoinHandle;

use crate::runtime;

/// The `OSError` Python itself would raise for `err` on `path`: `OSError(errno, strerror,
/// filename)`, which Python turns into the matching subclass (`FileNotFoundError`, ...).
pub(crate) fn os_error(py: Python<'_>, err: &io::Error, path: &Path) -> PyErr {
    let build = || -> PyResult<PyErr> {
        let errno_name = match err.kind() {
            io::ErrorKind::NotFound => "ENOENT",
            io::ErrorKind::PermissionDenied => "EACCES",
            io::ErrorKind::AlreadyExists => "EEXIST",
            io::ErrorKind::IsADirectory => "EISDIR",
            io::ErrorKind::NotADirectory => "ENOTDIR",
            _ => {
                return Ok(PyOSError::new_err((
                    err.to_string(),
                    path.as_os_str().to_owned(),
                )))
            }
        };
        let code: i32 = match err.raw_os_error() {
            Some(code) => code,
            None => py
                .import(intern!(py, "errno"))?
                .getattr(errno_name)?
                .extract()?,
        };
        let strerror: String = py
            .import(intern!(py, "os"))?
            .call_method1(intern!(py, "strerror"), (code,))?
            .extract()?;
        Ok(PyOSError::new_err((
            code,
            strerror,
            path.as_os_str().to_owned(),
        )))
    };
    build().unwrap_or_else(|failure| failure)
}

/// A Python `bytes` object lent to the transfer manager without copying it.
struct PyBytesOwner {
    // Keeps the object, and so the buffer below, alive. `bytes` is immutable.
    _object: Py<PyBytes>,
    data: *const u8,
    len: usize,
}

// SAFETY: the pointer refers to the immutable buffer of a `bytes` object that `_object` keeps
// alive; reading it needs no interpreter state. `Py<T>` itself is `Send + Sync`.
unsafe impl Send for PyBytesOwner {}
unsafe impl Sync for PyBytesOwner {}

impl AsRef<[u8]> for PyBytesOwner {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: see the `Send`/`Sync` impls above.
        unsafe { std::slice::from_raw_parts(self.data, self.len) }
    }
}

/// Copy (or, for `bytes`, borrow) a bytes-like object for an upload.
pub(crate) fn bytes_from_buffer(data: &Bound<'_, PyAny>) -> PyResult<Bytes> {
    if let Ok(bytes) = data.cast::<PyBytes>() {
        let slice = bytes.as_bytes();
        return Ok(Bytes::from_owner(PyBytesOwner {
            data: slice.as_ptr(),
            len: slice.len(),
            _object: bytes.clone().unbind(),
        }));
    }
    // Any other object supporting the buffer protocol (`bytearray`, `memoryview`, `array`, ...).
    let py = data.py();
    let copy = py
        .import(intern!(py, "builtins"))?
        .getattr(intern!(py, "memoryview"))?
        .call1((data,))
        .map_err(|_| {
            PyTypeError::new_err(format!(
                "expected a bytes-like object, got {}",
                data.get_type()
                    .name()
                    .map_or_else(|_| "?".into(), |name| name.to_string())
            ))
        })?
        .call_method0(intern!(py, "tobytes"))?;
    bytes_from_buffer(copy.cast::<PyBytes>()?)
}

/// The number of bytes left to read from a seekable file object, if it is one.
pub(crate) fn remaining_len(fileobj: &Bound<'_, PyAny>) -> PyResult<Option<u64>> {
    let py = fileobj.py();
    let seekable = match fileobj.call_method0(intern!(py, "seekable")) {
        Ok(seekable) => seekable.is_truthy()?,
        Err(_) => false,
    };
    if !seekable {
        return Ok(None);
    }
    let position: u64 = fileobj.call_method0(intern!(py, "tell"))?.extract()?;
    let end: u64 = fileobj
        .call_method1(intern!(py, "seek"), (0, 2))?
        .extract()?;
    fileobj.call_method1(intern!(py, "seek"), (position,))?;
    Ok(Some(end.saturating_sub(position)))
}

/// Read up to `len` bytes, retrying short reads until `len` is reached or the stream ends.
pub(crate) fn read_exact_or_eof(fileobj: &Bound<'_, PyAny>, len: usize) -> PyResult<Bytes> {
    let py = fileobj.py();
    let mut buf = BytesMut::new();
    while buf.len() < len {
        let chunk = fileobj.call_method1(intern!(py, "read"), (len - buf.len(),))?;
        let before = buf.len();
        if let Ok(bytes) = chunk.cast::<PyBytes>() {
            // The common case: one read fills the whole part.
            if before == 0 && bytes.as_bytes().len() == len {
                return bytes_from_buffer(bytes);
            }
            buf.extend_from_slice(bytes.as_bytes());
        } else if let Ok(array) = chunk.cast::<PyByteArray>() {
            buf.extend_from_slice(&array.to_vec());
        } else if chunk.is_none() {
            // A non-blocking raw stream with nothing available yet.
            continue;
        } else {
            return Err(PyTypeError::new_err(format!(
                "fileobj.read() must return bytes, not {} (is the file open in binary mode?)",
                chunk.get_type().name()?
            )));
        }
        if buf.len() == before {
            break;
        }
    }
    Ok(buf.freeze())
}

/// Where a file object's own exception is kept when it fails a transfer, so the transfer can
/// raise it rather than a generic I/O error.
#[derive(Clone, Default)]
pub(crate) struct PyErrorSlot(Arc<Mutex<Option<PyErr>>>);

impl PyErrorSlot {
    fn set(&self, err: PyErr) {
        let mut slot = self.0.lock().unwrap_or_else(|e| e.into_inner());
        slot.get_or_insert(err);
    }

    pub(crate) fn take(&self) -> Option<PyErr> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).take()
    }
}

/// A [`PartStream`] over a readable Python file object.
///
/// Each part is read with one blocking `read()` (retried on short reads) on the runtime's
/// blocking pool.
pub(crate) struct PyReader {
    fileobj: Arc<Py<PyAny>>,
    size_hint: SizeHint,
    next_part_number: u64,
    in_flight: Option<JoinHandle<PyResult<Bytes>>>,
    finished: bool,
    errors: PyErrorSlot,
}

impl PyReader {
    pub(crate) fn input_stream(
        fileobj: Py<PyAny>,
        remaining: Option<u64>,
        errors: PyErrorSlot,
    ) -> InputStream {
        let size_hint = match remaining {
            Some(len) => SizeHint::exact(len),
            None => SizeHint::default(),
        };
        InputStream::from_part_stream(Self {
            fileobj: Arc::new(fileobj),
            size_hint,
            next_part_number: 1,
            in_flight: None,
            finished: false,
            errors,
        })
    }
}

impl PartStream for PyReader {
    fn poll_part(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        stream_cx: &StreamContext,
    ) -> Poll<Option<io::Result<PartData>>> {
        if self.finished {
            return Poll::Ready(None);
        }
        let part_size = stream_cx.part_size();
        let this = &mut *self;
        let read = this.in_flight.get_or_insert_with(|| {
            let fileobj = Arc::clone(&this.fileobj);
            runtime::handle().spawn_blocking(move || {
                Python::try_attach(|py| read_exact_or_eof(fileobj.bind(py), part_size))
                    .unwrap_or_else(|| {
                        Err(PyOSError::new_err(
                            "the Python interpreter is shutting down",
                        ))
                    })
            })
        });
        let result = ready!(Pin::new(read).poll(cx));
        self.in_flight = None;
        let data = match result {
            Ok(Ok(data)) => data,
            Ok(Err(err)) => {
                self.finished = true;
                self.errors.set(err);
                return Poll::Ready(Some(Err(io::Error::other(
                    "reading from the Python file object failed",
                ))));
            }
            Err(join_err) => {
                self.finished = true;
                return Poll::Ready(Some(Err(io::Error::other(join_err))));
            }
        };
        if data.len() < part_size {
            self.finished = true;
            if data.is_empty() {
                return Poll::Ready(None);
            }
        }
        let part_number = self.next_part_number;
        self.next_part_number += 1;
        Poll::Ready(Some(Ok(PartData::new(part_number, data))))
    }

    fn size_hint(&self) -> SizeHint {
        self.size_hint
    }
}

/// Write `data` to a Python file object on the blocking pool, honoring short writes.
pub(crate) async fn write_all(fileobj: Arc<Py<PyAny>>, data: Vec<Bytes>) -> PyResult<()> {
    let written = runtime::handle().spawn_blocking(move || {
        Python::try_attach(|py| -> PyResult<()> {
            let fileobj = fileobj.bind(py);
            for segment in &data {
                write_segment(fileobj, segment)?;
            }
            Ok(())
        })
        .unwrap_or_else(|| {
            Err(PyOSError::new_err(
                "the Python interpreter is shutting down",
            ))
        })
    });
    written
        .await
        .map_err(|err| PyOSError::new_err(err.to_string()))?
}

fn write_segment(fileobj: &Bound<'_, PyAny>, segment: &[u8]) -> PyResult<()> {
    let py = fileobj.py();
    let mut rest = segment;
    while !rest.is_empty() {
        let written = fileobj.call_method1(intern!(py, "write"), (PyBytes::new(py, rest),))?;
        // `write()` returning `None` (as some file-likes do) is taken as having written it all.
        let Ok(written) = written.extract::<usize>() else {
            return Ok(());
        };
        if written == 0 {
            return Err(PyOSError::new_err(
                "fileobj.write() wrote no bytes; is it a non-blocking stream?",
            ));
        }
        rest = &rest[written.min(rest.len())..];
    }
    Ok(())
}
