/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Forwarding Rust log events to Python's `logging`.
//!
//! Events from the transfer manager and the AWS SDK are emitted under the
//! `aws_s3_transfer_manager` logger, e.g. `aws_s3_transfer_manager.aws_sdk_s3_transfer_manager`
//! and `aws_s3_transfer_manager.aws_smithy_runtime`.

use std::sync::OnceLock;

use log::LevelFilter;
use pyo3::prelude::*;

static RESET: OnceLock<pyo3_log::ResetHandle> = OnceLock::new();

/// Crates whose debug output is too chatty to be useful; only their warnings are forwarded.
const QUIET_TARGETS: &[&str] = &[
    "h2",
    "hickory_proto",
    "hickory_resolver",
    "hyper",
    "hyper_util",
    "rustls",
    // `tracing` span enter/exit records.
    "tracing",
];

pub(crate) fn install(py: Python<'_>) -> PyResult<()> {
    // TRACE is left out: at that level the transfer manager logs on per-request hot paths, and
    // each forwarded record costs a call into Python.
    let mut logger = pyo3_log::Logger::new(py, pyo3_log::Caching::LoggersAndLevels)?
        .set_prefix("aws_s3_transfer_manager")
        .filter(LevelFilter::Debug);
    for target in QUIET_TARGETS {
        logger = logger.filter_target((*target).to_owned(), LevelFilter::Warn);
    }
    // A logger is already installed if the module is initialized more than once in a process.
    if let Ok(reset) = logger.install() {
        let _ = RESET.set(reset);
    }
    Ok(())
}

/// Drop cached logger levels, so that logging configuration changes take effect.
pub(crate) fn refresh() {
    if let Some(reset) = RESET.get() {
        reset.reset();
    }
}
