/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Mapping transfer manager errors onto the exception hierarchy in
//! `aws_s3_transfer_manager.exceptions`.
//!
//! The exception classes are defined in Python (so they have ordinary constructors, pickle, and
//! read as `aws_s3_transfer_manager.exceptions.ServiceError` in tracebacks). Errors produced on
//! the runtime are captured as [`ErrorInfo`], which holds no Python objects, and become exceptions
//! only on a thread attached to the interpreter.

use std::fmt::Write as _;
use std::sync::Arc;

use aws_sdk_s3_transfer_manager::error::{Error, ErrorKind};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyType};

use crate::types::{FailedDownload, FailedUpload};

/// Import (once) a class from the pure-Python half of the package.
pub(crate) fn python_class<'py>(
    py: Python<'py>,
    cell: &'static PyOnceLock<Py<PyType>>,
    module: &str,
    name: &str,
) -> PyResult<&'py Bound<'py, PyType>> {
    cell.import(py, module, name)
}

macro_rules! exception_class {
    ($py:expr, $name:literal) => {{
        static CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
        python_class($py, &CLASS, "aws_s3_transfer_manager.exceptions", $name)
    }};
}

/// `err` followed by each of its sources, separated by `": "`.
fn error_chain(err: &dyn std::error::Error) -> String {
    let mut message = err.to_string();
    let mut source = err.source();
    while let Some(err) = source {
        let text = err.to_string();
        // Wrappers often repeat their source's message; skip exact repeats.
        if !message.ends_with(&text) {
            message.push_str(": ");
            message.push_str(&text);
        }
        source = err.source();
    }
    message
}

/// `S3 GetObject failed with NoSuchKey: The specified key does not exist. (request id: ...)`.
///
/// The SDK's own rendering of service errors embeds `Debug` output, so it is only used when S3
/// did not return an error code.
fn service_message(err: &Error, details: &ServiceDetails) -> String {
    let Some(code) = &details.code else {
        return error_chain(err);
    };
    let mut message = match &details.operation {
        Some(operation) => format!("S3 {operation} failed with {code}"),
        None => format!("S3 request failed with {code}"),
    };
    if let Some(text) = &details.message {
        message.push_str(": ");
        message.push_str(text);
    }
    if let Some(chunk) = err.chunk() {
        match chunk.byte_range() {
            Some(range) => {
                let _ = write!(message, " [bytes {}-{}]", range.start(), range.end());
            }
            None => {
                let _ = write!(message, " [part {}]", chunk.seq() + 1);
            }
        }
    }
    match (&details.request_id, &details.extended_request_id) {
        (Some(id), Some(extended)) => {
            let _ = write!(
                message,
                " (request id: {id}, extended request id: {extended})"
            );
        }
        (Some(id), None) => {
            let _ = write!(message, " (request id: {id})");
        }
        _ => {}
    }
    message
}

/// Service error codes that mean the bucket, key, version, or upload does not exist.
const NOT_FOUND_CODES: &[&str] = &[
    "NotFound",
    "NoSuchBucket",
    "NoSuchKey",
    "NoSuchUpload",
    "NoSuchVersion",
];

/// A transfer error, detached from both the transfer manager and Python.
#[derive(Debug, Clone)]
pub(crate) struct ErrorInfo {
    kind: Kind,
    message: String,
}

#[derive(Debug, Clone)]
enum Kind {
    InvalidInput,
    Io,
    Internal,
    Discovery,
    Service(Box<ServiceDetails>),
    Integrity {
        algorithm: Option<String>,
        expected: Option<String>,
        computed: Option<String>,
    },
    Bulk(Arc<Failures>),
    Cancelled,
}

#[derive(Debug, Clone)]
struct ServiceDetails {
    operation: Option<String>,
    code: Option<String>,
    message: Option<String>,
    request_id: Option<String>,
    extended_request_id: Option<String>,
}

#[derive(Debug)]
pub(crate) enum Failures {
    Uploads(Vec<FailedUpload>),
    Downloads(Vec<FailedDownload>),
}

impl ErrorInfo {
    pub(crate) fn from_error(err: &Error) -> Self {
        let kind = match err.kind() {
            ErrorKind::InputInvalid => Kind::InvalidInput,
            ErrorKind::IOError => Kind::Io,
            ErrorKind::ObjectNotDiscoverable => Kind::Discovery,
            // No error code means S3 never answered: the request failed in transport (or reading
            // the local body), which is an I/O failure rather than a service error.
            ErrorKind::ServiceError if err.code().is_none() => Kind::Io,
            ErrorKind::ServiceError => Kind::Service(Box::new(ServiceDetails {
                operation: err.operation_name().map(str::to_owned),
                code: err.code().map(str::to_owned),
                message: err.message().map(str::to_owned),
                request_id: err.request_id().map(str::to_owned),
                extended_request_id: err.extended_request_id().map(str::to_owned),
            })),
            ErrorKind::IntegrityError(detail) => Kind::Integrity {
                algorithm: detail.algorithm().map(|a| a.as_str().to_owned()),
                expected: detail.expected().map(str::to_owned),
                computed: detail.computed().map(str::to_owned),
            },
            ErrorKind::ChildOperationFailed => Kind::Bulk(Arc::new(Failures::from_error(err))),
            ErrorKind::OperationCancelled => Kind::Cancelled,
            _ => Kind::Internal,
        };
        let mut message = match &kind {
            Kind::Service(details) => service_message(err, details),
            _ => error_chain(err),
        };
        if let Kind::Bulk(failures) = &kind {
            if let Some(first) = failures.first_error() {
                message.push_str("; first failure: ");
                message.push_str(&first.message);
            }
        }
        Self { kind, message }
    }

    pub(crate) fn cancelled(what: &str) -> Self {
        Self {
            kind: Kind::Cancelled,
            message: format!("{what} was cancelled"),
        }
    }

    /// A bug in the bindings or the transfer manager, reported instead of hanging the caller.
    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: Kind::Internal,
            message: message.into(),
        }
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        matches!(self.kind, Kind::Cancelled)
    }

    /// Build the Python exception for this error.
    pub(crate) fn to_pyerr(&self, py: Python<'_>) -> PyErr {
        self.instantiate(py).unwrap_or_else(|err| err)
    }

    fn instantiate(&self, py: Python<'_>) -> Result<PyErr, PyErr> {
        let kwargs = PyDict::new(py);
        let class = match &self.kind {
            Kind::InvalidInput => exception_class!(py, "InvalidInputError")?,
            Kind::Io => exception_class!(py, "TransferIOError")?,
            Kind::Internal => exception_class!(py, "TransferError")?,
            Kind::Discovery => exception_class!(py, "ObjectDiscoveryError")?,
            Kind::Cancelled => exception_class!(py, "TransferCancelledError")?,
            Kind::Integrity {
                algorithm,
                expected,
                computed,
            } => {
                kwargs.set_item("algorithm", algorithm)?;
                kwargs.set_item("expected", expected)?;
                kwargs.set_item("computed", computed)?;
                exception_class!(py, "IntegrityError")?
            }
            Kind::Service(details) => {
                kwargs.set_item("operation", &details.operation)?;
                kwargs.set_item("code", &details.code)?;
                kwargs.set_item("message", &details.message)?;
                kwargs.set_item("request_id", &details.request_id)?;
                kwargs.set_item("extended_request_id", &details.extended_request_id)?;
                match details.code.as_deref() {
                    Some(code) if NOT_FOUND_CODES.contains(&code) => {
                        exception_class!(py, "NotFoundError")?
                    }
                    Some("PreconditionFailed") => exception_class!(py, "PreconditionFailedError")?,
                    _ => exception_class!(py, "ServiceError")?,
                }
            }
            Kind::Bulk(failures) => {
                kwargs.set_item("failures", failures.to_list(py)?)?;
                exception_class!(py, "BulkTransferError")?
            }
        };
        let exception = class.call((&self.message,), Some(&kwargs))?;
        Ok(PyErr::from_value(exception))
    }
}

impl Failures {
    fn from_error(err: &Error) -> Self {
        match (err.failed_uploads(), err.failed_downloads()) {
            (Some(uploads), _) => Self::Uploads(uploads.iter().map(FailedUpload::new).collect()),
            (None, Some(downloads)) => {
                Self::Downloads(downloads.iter().map(FailedDownload::new).collect())
            }
            (None, None) => Self::Uploads(Vec::new()),
        }
    }

    fn first_error(&self) -> Option<&ErrorInfo> {
        match self {
            Self::Uploads(failures) => failures.first().map(FailedUpload::error_info),
            Self::Downloads(failures) => failures.first().map(FailedDownload::error_info),
        }
    }

    fn to_list<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyList>> {
        match self {
            Self::Uploads(failures) => pyo3::types::PyList::new(py, failures.iter().cloned()),
            Self::Downloads(failures) => pyo3::types::PyList::new(py, failures.iter().cloned()),
        }
    }
}

/// Convert a transfer manager error raised synchronously (while initiating a transfer).
pub(crate) fn to_pyerr(py: Python<'_>, err: &Error) -> PyErr {
    ErrorInfo::from_error(err).to_pyerr(py)
}

/// An `InvalidInputError` for bad arguments detected by the bindings themselves.
pub(crate) fn invalid_input(py: Python<'_>, message: impl Into<String>) -> PyErr {
    ErrorInfo {
        kind: Kind::InvalidInput,
        message: message.into(),
    }
    .to_pyerr(py)
}
