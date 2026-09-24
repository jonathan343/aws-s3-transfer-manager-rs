/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Building a transfer manager `Config` from `TransferManager(...)` keyword arguments.
//!
//! Anything not given explicitly is resolved the way every AWS SDK resolves it: environment
//! variables, the shared config and credentials files, then instance metadata.

use std::borrow::Cow;

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_runtime::user_agent::FrameworkMetadata;
use aws_sdk_s3_transfer_manager::types::{
    ConcurrencyMode, MemoryBudgetConfig, PartSize, ReadAhead, TargetThroughput,
};
use aws_sdk_s3_transfer_manager::{Config, S3ClientConfig};
use aws_smithy_types::checksum_config::{RequestChecksumCalculation, ResponseChecksumValidation};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

/// Name reported in the `lib/` section of the user agent.
const USER_AGENT_NAME: &str = "aws-s3-transfer-manager-python";

/// The transfer manager's multipart threshold when none is configured. Mirrors the crate's
/// `PartSize::Auto` resolution; used to decide whether a file object is small enough to read
/// into memory and send in a single request.
pub(crate) const DEFAULT_MULTIPART_THRESHOLD: u64 = 16 * 1024 * 1024;

/// `TransferManager` constructor arguments.
#[derive(Default)]
pub(crate) struct Settings {
    pub(crate) region: Option<String>,
    pub(crate) profile: Option<String>,
    pub(crate) endpoint_url: Option<String>,
    pub(crate) force_path_style: Option<bool>,
    pub(crate) aws_access_key_id: Option<String>,
    pub(crate) aws_secret_access_key: Option<String>,
    pub(crate) aws_session_token: Option<String>,
    pub(crate) part_size: Option<u64>,
    pub(crate) multipart_threshold: Option<u64>,
    pub(crate) concurrency: Option<usize>,
    pub(crate) target_throughput_gbps: Option<u64>,
    pub(crate) memory_limit: Option<usize>,
    pub(crate) memory_limit_fraction: Option<f64>,
    pub(crate) read_ahead_parts: Option<usize>,
    pub(crate) request_checksum_calculation: Option<String>,
    pub(crate) response_checksum_validation: Option<String>,
}

/// A resolved configuration, ready to build a client from.
pub(crate) struct Resolved {
    pub(crate) config: Config,
    pub(crate) region: String,
    pub(crate) multipart_threshold: u64,
}

fn checksum_setting(name: &str, value: &str) -> PyResult<bool> {
    match value {
        "when_supported" => Ok(true),
        "when_required" => Ok(false),
        other => Err(PyValueError::new_err(format!(
            "{name} must be 'when_supported' or 'when_required', got {other:?}"
        ))),
    }
}

fn exclusive<A, B>(a: &Option<A>, a_name: &str, b: &Option<B>, b_name: &str) -> PyResult<()> {
    if a.is_some() && b.is_some() {
        return Err(PyValueError::new_err(format!(
            "{a_name} and {b_name} are mutually exclusive"
        )));
    }
    Ok(())
}

impl Settings {
    /// Check the arguments that can be checked without loading anything.
    pub(crate) fn validate(&self) -> PyResult<()> {
        exclusive(
            &self.concurrency,
            "concurrency",
            &self.target_throughput_gbps,
            "target_throughput_gbps",
        )?;
        exclusive(
            &self.memory_limit,
            "memory_limit",
            &self.memory_limit_fraction,
            "memory_limit_fraction",
        )?;
        if self.aws_access_key_id.is_some() != self.aws_secret_access_key.is_some() {
            return Err(PyValueError::new_err(
                "aws_access_key_id and aws_secret_access_key must be given together",
            ));
        }
        if self.aws_session_token.is_some() && self.aws_access_key_id.is_none() {
            return Err(PyValueError::new_err(
                "aws_session_token requires aws_access_key_id and aws_secret_access_key",
            ));
        }
        if self.concurrency == Some(0) {
            return Err(PyValueError::new_err("concurrency must be at least 1"));
        }
        if self.target_throughput_gbps == Some(0) {
            return Err(PyValueError::new_err(
                "target_throughput_gbps must be at least 1",
            ));
        }
        if let Some(fraction) = self.memory_limit_fraction {
            if !(fraction > 0.0 && fraction <= 1.0) {
                return Err(PyValueError::new_err(format!(
                    "memory_limit_fraction must be in (0, 1], got {fraction}"
                )));
            }
        }
        for (name, value) in [
            (
                "request_checksum_calculation",
                &self.request_checksum_calculation,
            ),
            (
                "response_checksum_validation",
                &self.response_checksum_validation,
            ),
        ] {
            if let Some(value) = value {
                checksum_setting(name, value)?;
            }
        }
        Ok(())
    }

    /// Resolve AWS configuration (region, credentials provider, ...) and assemble the config.
    /// Expects [`validate`](Self::validate) to have passed.
    ///
    /// Returns an error message rather than a `PyErr` so that it can run off the interpreter.
    pub(crate) async fn resolve(self) -> Result<Resolved, String> {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = self.region {
            loader = loader.region(Region::new(region));
        }
        if let Some(profile) = self.profile {
            loader = loader.profile_name(profile);
        }
        if let Some(endpoint_url) = self.endpoint_url {
            loader = loader.endpoint_url(endpoint_url);
        }
        if let (Some(access_key_id), Some(secret_access_key)) =
            (self.aws_access_key_id, self.aws_secret_access_key)
        {
            loader = loader.credentials_provider(Credentials::new(
                access_key_id,
                secret_access_key,
                self.aws_session_token,
                None,
                "TransferManager",
            ));
        }
        if let Some(value) = &self.request_checksum_calculation {
            loader = loader.request_checksum_calculation(if value == "when_supported" {
                RequestChecksumCalculation::WhenSupported
            } else {
                RequestChecksumCalculation::WhenRequired
            });
        }
        if let Some(value) = &self.response_checksum_validation {
            loader = loader.response_checksum_validation(if value == "when_supported" {
                ResponseChecksumValidation::WhenSupported
            } else {
                ResponseChecksumValidation::WhenRequired
            });
        }
        let sdk_config = loader.load().await;
        let region = sdk_config
            .region()
            .map(|r| r.as_ref().to_owned())
            .ok_or_else(|| {
                "no AWS region is configured: pass region=... or set AWS_REGION (or a region in \
                 your AWS config file)"
                    .to_owned()
            })?;

        let mut s3_config = aws_sdk_s3::config::Builder::from(&sdk_config);
        if let Some(force_path_style) = self.force_path_style {
            s3_config = s3_config.force_path_style(force_path_style);
        }

        let mut builder = Config::builder()
            .s3_config(S3ClientConfig::new(s3_config))
            .framework_metadata(
                FrameworkMetadata::new(
                    USER_AGENT_NAME,
                    Some(Cow::Borrowed(env!("CARGO_PKG_VERSION"))),
                )
                .ok(),
            );
        if let Some(part_size) = self.part_size {
            builder = builder.part_size(PartSize::Target(part_size));
        }
        if let Some(threshold) = self.multipart_threshold {
            builder = builder.multipart_threshold(PartSize::Target(threshold));
        }
        if let Some(concurrency) = self.concurrency {
            builder = builder.concurrency(ConcurrencyMode::Explicit(concurrency));
        }
        if let Some(gbps) = self.target_throughput_gbps {
            builder = builder.concurrency(ConcurrencyMode::TargetThroughput(
                TargetThroughput::new_gigabits_per_sec(gbps),
            ));
        }
        if let Some(limit) = self.memory_limit {
            builder = builder.memory_budget(MemoryBudgetConfig::Limit(limit));
        }
        if let Some(fraction) = self.memory_limit_fraction {
            builder = builder.memory_budget(MemoryBudgetConfig::Fraction(fraction));
        }
        if let Some(parts) = self.read_ahead_parts {
            builder = builder.read_ahead(ReadAhead::Parts(parts));
        }
        let config = builder.build();
        let multipart_threshold = match config.multipart_threshold() {
            PartSize::Target(threshold) => *threshold,
            _ => DEFAULT_MULTIPART_THRESHOLD,
        };
        Ok(Resolved {
            config,
            region,
            multipart_threshold,
        })
    }
}
