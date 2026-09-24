/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Python bindings for the Amazon S3 Transfer Manager.
//!
//! This is the `aws_s3_transfer_manager._core` extension module. The public API is the
//! `aws_s3_transfer_manager` package, which re-exports these types next to its pure-Python parts
//! (exceptions, enums, and typing helpers).

mod client;
mod config;
mod errors;
mod io;
mod logging;
mod options;
mod runtime;
mod stream;
mod transfer;
mod types;

use pyo3::prelude::*;

#[pymodule]
mod _core {
    #[pymodule_export]
    use crate::client::TransferManager;
    #[pymodule_export]
    use crate::stream::DownloadStream;
    #[pymodule_export]
    use crate::transfer::Transfer;
    #[pymodule_export]
    use crate::types::{
        DirectoryDownloadResult, DirectoryUploadResult, DownloadResult, FailedDownload,
        FailedUpload, IntegrityChecks, ObjectMetadata, ObjectSummary, TransferMetrics,
        UploadResult,
    };

    use super::*;

    #[pymodule_init]
    fn init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add("__version__", env!("CARGO_PKG_VERSION"))?;
        crate::logging::install(m.py())
    }
}
