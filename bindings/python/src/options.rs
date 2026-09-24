/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Keyword options accepted by the single-object operations.
//!
//! Enumerated S3 values (storage classes, ACLs, ...) are taken as their wire strings, as boto3
//! does, and passed through without validation so that values newer than this release still work.
//! The Python typing (`aws_s3_transfer_manager.types`) lists the known values.

use std::collections::HashMap;
use std::time::SystemTime;

use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ChecksumType, ObjectCannedAcl, ObjectLockLegalHoldStatus,
    ObjectLockMode, RequestPayer, ServerSideEncryption, StorageClass,
};
use aws_sdk_s3_transfer_manager::operation::download::builders::DownloadFluentBuilder;
use aws_sdk_s3_transfer_manager::operation::upload::builders::UploadFluentBuilder;
use aws_sdk_s3_transfer_manager::operation::upload::ChecksumStrategy;
use aws_sdk_s3_transfer_manager::types::{FailedMultipartUploadPolicy, ReadAhead};
use aws_smithy_types::DateTime;
use pyo3::exceptions::PyTypeError;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};

use crate::errors::invalid_input;

/// Declares an options struct parsed from `**kwargs`: every field optional, `None` treated as
/// unset, and unknown keys rejected the way a Python function rejects them.
macro_rules! options {
    ($(#[$doc:meta])* $name:ident { $($field:ident: $ty:ty $(=> $parse:path)?,)* }) => {
        $(#[$doc])*
        #[derive(Default)]
        pub(crate) struct $name {
            $($field: Option<$ty>,)*
        }

        impl $name {
            pub(crate) fn parse(
                method: &str,
                kwargs: Option<&Bound<'_, PyDict>>,
            ) -> PyResult<Self> {
                let mut options = Self::default();
                let Some(kwargs) = kwargs else {
                    return Ok(options);
                };
                for (key, value) in kwargs.iter() {
                    let key = key.cast_into::<PyString>()?;
                    let key = key.to_str()?;
                    if value.is_none() {
                        continue;
                    }
                    match key {
                        $(stringify!($field) => {
                            options.$field = Some(
                                options!(@parse value $($parse)?)
                                    .map_err(|err| with_context(value.py(), method, key, err))?,
                            );
                        })*
                        _ => {
                            return Err(PyTypeError::new_err(format!(
                                "{method}() got an unexpected keyword argument '{key}'"
                            )))
                        }
                    }
                }
                Ok(options)
            }
        }
    };
    (@parse $value:ident $parse:path) => { $parse(&$value) };
    (@parse $value:ident) => { $value.extract() };
}

/// Name the offending argument in a conversion `TypeError`.
fn with_context(py: Python<'_>, method: &str, key: &str, err: PyErr) -> PyErr {
    if err.is_instance_of::<PyTypeError>(py) {
        PyTypeError::new_err(format!("{method}() argument '{key}': {}", err.value(py)))
    } else {
        err
    }
}

/// Parse a timezone-aware `datetime`; a naive one is ambiguous and is rejected.
fn aware_datetime(value: &Bound<'_, PyAny>) -> PyResult<SystemTime> {
    let py = value.py();
    if value.getattr(intern!(py, "tzinfo"))?.is_none() {
        return Err(invalid_input(
            py,
            "datetime values must be timezone-aware (for example `datetime.now(timezone.utc)`)",
        ));
    }
    value.extract()
}

/// `tagging` is either S3's query-string form (`"k1=v1&k2=v2"`) or a mapping of tags.
fn tagging(value: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(tags) = value.cast::<PyString>() {
        return Ok(tags.to_str()?.to_owned());
    }
    let py = value.py();
    py.import(intern!(py, "urllib.parse"))?
        .call_method1(intern!(py, "urlencode"), (value,))?
        .extract()
}

pub(crate) fn priority(py: Python<'_>, value: Option<i64>) -> PyResult<Option<u8>> {
    value
        .map(|p| {
            u8::try_from(p)
                .ok()
                .filter(|p| *p >= 1)
                .ok_or_else(|| invalid_input(py, format!("priority must be 1-255, got {p}")))
        })
        .transpose()
}

fn to_datetime(value: Option<SystemTime>) -> Option<DateTime> {
    value.map(DateTime::from)
}

options! {
    /// Options for `upload_file`, `upload_bytes` and `upload_fileobj`.
    UploadOptions {
        acl: String,
        cache_control: String,
        content_disposition: String,
        content_encoding: String,
        content_language: String,
        content_type: String,
        checksum_algorithm: String,
        checksum_type: String,
        full_object_checksum: String,
        if_match: String,
        if_none_match: String,
        expires: SystemTime => aware_datetime,
        grant_full_control: String,
        grant_read: String,
        grant_read_acp: String,
        grant_write_acp: String,
        metadata: HashMap<String, String>,
        server_side_encryption: String,
        storage_class: String,
        website_redirect_location: String,
        sse_customer_algorithm: String,
        sse_customer_key: String,
        sse_customer_key_md5: String,
        sse_kms_key_id: String,
        sse_kms_encryption_context: String,
        bucket_key_enabled: bool,
        request_payer: String,
        tagging: String => tagging,
        object_lock_mode: String,
        object_lock_retain_until_date: SystemTime => aware_datetime,
        object_lock_legal_hold_status: String,
        expected_bucket_owner: String,
        failed_multipart_upload_policy: String,
        priority: i64,
    }
}

impl UploadOptions {
    pub(crate) fn priority(&self, py: Python<'_>) -> PyResult<Option<u8>> {
        priority(py, self.priority)
    }

    fn checksum_strategy(&self, py: Python<'_>) -> PyResult<Option<ChecksumStrategy>> {
        let Some(algorithm) = &self.checksum_algorithm else {
            if self.checksum_type.is_some() || self.full_object_checksum.is_some() {
                return Err(invalid_input(
                    py,
                    "checksum_type and full_object_checksum require checksum_algorithm",
                ));
            }
            return Ok(None);
        };
        let mut builder = ChecksumStrategy::builder().algorithm(ChecksumAlgorithm::from(
            algorithm.to_ascii_uppercase().as_str(),
        ));
        if let Some(checksum_type) = &self.checksum_type {
            builder = builder.type_if_multipart(ChecksumType::from(
                checksum_type.to_ascii_uppercase().as_str(),
            ));
        }
        if let Some(checksum) = &self.full_object_checksum {
            builder = builder.full_object_checksum(checksum);
        }
        builder
            .build()
            .map(Some)
            .map_err(|err| invalid_input(py, err.to_string()))
    }

    fn failed_multipart_upload_policy(
        &self,
        py: Python<'_>,
    ) -> PyResult<Option<FailedMultipartUploadPolicy>> {
        match self.failed_multipart_upload_policy.as_deref() {
            None => Ok(None),
            Some("abort") => Ok(Some(FailedMultipartUploadPolicy::AbortUpload)),
            Some("retain") => Ok(Some(FailedMultipartUploadPolicy::Retain)),
            Some(other) => Err(invalid_input(
                py,
                format!(
                    "failed_multipart_upload_policy must be 'abort' or 'retain', got {other:?}"
                ),
            )),
        }
    }

    pub(crate) fn apply(
        self,
        py: Python<'_>,
        builder: UploadFluentBuilder,
    ) -> PyResult<UploadFluentBuilder> {
        let checksum_strategy = self.checksum_strategy(py)?;
        let failed_multipart_upload_policy = self.failed_multipart_upload_policy(py)?;
        Ok(builder
            .set_acl(self.acl.as_deref().map(ObjectCannedAcl::from))
            .set_cache_control(self.cache_control)
            .set_content_disposition(self.content_disposition)
            .set_content_encoding(self.content_encoding)
            .set_content_language(self.content_language)
            .set_content_type(self.content_type)
            .set_checksum_strategy(checksum_strategy)
            .set_if_match(self.if_match)
            .set_if_none_match(self.if_none_match)
            .set_expires(to_datetime(self.expires))
            .set_grant_full_control(self.grant_full_control)
            .set_grant_read(self.grant_read)
            .set_grant_read_acp(self.grant_read_acp)
            .set_grant_write_acp(self.grant_write_acp)
            .set_metadata(self.metadata)
            .set_server_side_encryption(
                self.server_side_encryption
                    .as_deref()
                    .map(ServerSideEncryption::from),
            )
            .set_storage_class(self.storage_class.as_deref().map(StorageClass::from))
            .set_website_redirect_location(self.website_redirect_location)
            .set_sse_customer_algorithm(self.sse_customer_algorithm)
            .set_sse_customer_key(self.sse_customer_key)
            .set_sse_customer_key_md5(self.sse_customer_key_md5)
            .set_sse_kms_key_id(self.sse_kms_key_id)
            .set_sse_kms_encryption_context(self.sse_kms_encryption_context)
            .set_bucket_key_enabled(self.bucket_key_enabled)
            .set_request_payer(self.request_payer.as_deref().map(RequestPayer::from))
            .set_tagging(self.tagging)
            .set_object_lock_mode(self.object_lock_mode.as_deref().map(ObjectLockMode::from))
            .set_object_lock_retain_until_date(to_datetime(self.object_lock_retain_until_date))
            .set_object_lock_legal_hold_status(
                self.object_lock_legal_hold_status
                    .as_deref()
                    .map(ObjectLockLegalHoldStatus::from),
            )
            .set_expected_bucket_owner(self.expected_bucket_owner)
            .set_failed_multipart_upload_policy(failed_multipart_upload_policy))
    }
}

options! {
    /// Options for `download_file`, `download_bytes`, `download_fileobj` and `download_stream`.
    DownloadOptions {
        version_id: String,
        range: String,
        if_match: String,
        if_none_match: String,
        if_modified_since: SystemTime => aware_datetime,
        if_unmodified_since: SystemTime => aware_datetime,
        sse_customer_algorithm: String,
        sse_customer_key: String,
        sse_customer_key_md5: String,
        request_payer: String,
        expected_bucket_owner: String,
        checksum_mode: String,
        response_cache_control: String,
        response_content_disposition: String,
        response_content_encoding: String,
        response_content_language: String,
        response_content_type: String,
        response_expires: SystemTime => aware_datetime,
        read_ahead_parts: usize,
        priority: i64,
    }
}

impl DownloadOptions {
    pub(crate) fn priority(&self, py: Python<'_>) -> PyResult<Option<u8>> {
        priority(py, self.priority)
    }

    pub(crate) fn apply(self, builder: DownloadFluentBuilder) -> DownloadFluentBuilder {
        builder
            .set_version_id(self.version_id)
            .set_range(self.range)
            .set_if_match(self.if_match)
            .set_if_none_match(self.if_none_match)
            .set_if_modified_since(to_datetime(self.if_modified_since))
            .set_if_unmodified_since(to_datetime(self.if_unmodified_since))
            .set_sse_customer_algorithm(self.sse_customer_algorithm)
            .set_sse_customer_key(self.sse_customer_key)
            .set_sse_customer_key_md5(self.sse_customer_key_md5)
            .set_request_payer(self.request_payer.as_deref().map(RequestPayer::from))
            .set_expected_bucket_owner(self.expected_bucket_owner)
            .set_checksum_mode(self.checksum_mode.as_deref().map(ChecksumMode::from))
            .set_response_cache_control(self.response_cache_control)
            .set_response_content_disposition(self.response_content_disposition)
            .set_response_content_encoding(self.response_content_encoding)
            .set_response_content_language(self.response_content_language)
            .set_response_content_type(self.response_content_type)
            .set_response_expires(to_datetime(self.response_expires))
            .set_read_ahead(self.read_ahead_parts.map(ReadAhead::Parts))
    }
}
