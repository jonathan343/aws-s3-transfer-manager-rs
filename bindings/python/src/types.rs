/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Immutable result and metadata types returned to Python.

use std::fmt::Write as _;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

use aws_sdk_s3_transfer_manager::operation::download::{
    DownloadOutput, ObjectMetadata as RustObjectMetadata,
};
use aws_sdk_s3_transfer_manager::operation::upload::UploadOutput;
use aws_sdk_s3_transfer_manager::types::{
    ChecksumValidation, IntegrityChecks as RustIntegrityChecks, NotValidatedReason,
    TransferMetrics as RustTransferMetrics,
};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyType;

use crate::errors::{python_class, ErrorInfo};

fn to_system_time(value: Option<&aws_smithy_types::DateTime>) -> Option<SystemTime> {
    value.and_then(|dt| SystemTime::try_from(*dt).ok())
}

/// `repr()` for the result types: `Name(field=value, ...)`, skipping unset fields.
struct Repr {
    out: String,
    first: bool,
}

impl Repr {
    fn new(name: &str) -> Self {
        Self {
            out: format!("{name}("),
            first: true,
        }
    }

    fn field(mut self, name: &str, value: impl std::fmt::Display) -> Self {
        if !self.first {
            self.out.push_str(", ");
        }
        self.first = false;
        let _ = write!(self.out, "{name}={value}");
        self
    }

    fn str_field(self, name: &str, value: Option<&str>) -> Self {
        match value {
            Some(v) => self.field(name, format_args!("{v:?}")),
            None => self,
        }
    }

    fn finish(mut self) -> String {
        self.out.push(')');
        self.out
    }
}

/// A snapshot of a transfer's progress.
#[pyclass(frozen, skip_from_py_object, module = "aws_s3_transfer_manager")]
#[derive(Clone, Debug)]
pub struct TransferMetrics {
    inner: RustTransferMetrics,
    /// When the snapshot was taken, so `elapsed` does not move for an unfinished transfer.
    taken_at: Instant,
}

impl TransferMetrics {
    pub(crate) fn new(inner: RustTransferMetrics) -> Self {
        Self {
            inner,
            taken_at: Instant::now(),
        }
    }
}

#[pymethods]
impl TransferMetrics {
    /// Payload bytes sent to S3 (uploads).
    #[getter]
    fn bytes_sent(&self) -> u64 {
        self.inner.network_tx
    }

    /// Payload bytes received from S3 (downloads).
    #[getter]
    fn bytes_received(&self) -> u64 {
        self.inner.network_rx
    }

    /// Payload bytes moved over the network in either direction.
    #[getter]
    fn bytes_transferred(&self) -> u64 {
        self.inner.network_tx + self.inner.network_rx
    }

    /// Bytes read from local disk.
    #[getter]
    fn disk_bytes_read(&self) -> u64 {
        self.inner.disk_read
    }

    /// Bytes written to local disk.
    #[getter]
    fn disk_bytes_written(&self) -> u64 {
        self.inner.disk_write
    }

    /// Expected payload size in bytes, once known.
    #[getter]
    fn total_bytes(&self) -> Option<u64> {
        self.inner.total_bytes
    }

    /// Time from the start of the transfer until it finished, or until this snapshot.
    #[getter]
    fn elapsed(&self) -> Duration {
        self.inner
            .finished_at
            .unwrap_or(self.taken_at)
            .saturating_duration_since(self.inner.started_at)
    }

    fn __repr__(&self) -> String {
        Repr::new("TransferMetrics")
            .field("bytes_transferred", self.bytes_transferred())
            .field(
                "total_bytes",
                self.total_bytes()
                    .map_or_else(|| "None".to_owned(), |t| t.to_string()),
            )
            .field(
                "elapsed",
                format_args!("{:.3}s", self.elapsed().as_secs_f64()),
            )
            .finish()
    }
}

/// The outcome of a completed single-object upload.
#[pyclass(frozen, skip_from_py_object, module = "aws_s3_transfer_manager")]
#[derive(Clone)]
pub struct UploadResult {
    inner: UploadOutput,
}

impl UploadResult {
    pub(crate) fn new(inner: UploadOutput) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl UploadResult {
    /// The entity tag of the uploaded object.
    #[getter]
    fn etag(&self) -> Option<&str> {
        self.inner.e_tag()
    }

    /// The version ID of the object, if the bucket is versioned.
    #[getter]
    fn version_id(&self) -> Option<&str> {
        self.inner.version_id()
    }

    /// The base64-encoded CRC-32 checksum of the object, if one was computed.
    #[getter]
    fn checksum_crc32(&self) -> Option<&str> {
        self.inner.checksum_crc32()
    }

    /// The base64-encoded CRC-32C checksum of the object, if one was computed.
    #[getter]
    fn checksum_crc32c(&self) -> Option<&str> {
        self.inner.checksum_crc32_c()
    }

    /// The base64-encoded CRC-64/NVME checksum of the object, if one was computed.
    #[getter]
    fn checksum_crc64nvme(&self) -> Option<&str> {
        self.inner.checksum_crc64_nvme()
    }

    /// The base64-encoded SHA-1 checksum of the object, if one was computed.
    #[getter]
    fn checksum_sha1(&self) -> Option<&str> {
        self.inner.checksum_sha1()
    }

    /// The base64-encoded SHA-256 checksum of the object, if one was computed.
    #[getter]
    fn checksum_sha256(&self) -> Option<&str> {
        self.inner.checksum_sha256()
    }

    /// `"FULL_OBJECT"` or `"COMPOSITE"`, if the object has a checksum.
    #[getter]
    fn checksum_type(&self) -> Option<&str> {
        self.inner.checksum_type().map(|t| t.as_str())
    }

    /// The object's expiration rule, if a lifecycle configuration applies.
    #[getter]
    fn expiration(&self) -> Option<&str> {
        self.inner.expiration()
    }

    /// The server-side encryption algorithm used to store the object.
    #[getter]
    fn server_side_encryption(&self) -> Option<&str> {
        self.inner.server_side_encryption().map(|t| t.as_str())
    }

    /// The algorithm of the customer-provided encryption key, if one was used.
    #[getter]
    fn sse_customer_algorithm(&self) -> Option<&str> {
        self.inner.sse_customer_algorithm()
    }

    /// The MD5 of the customer-provided encryption key, if one was used.
    #[getter]
    fn sse_customer_key_md5(&self) -> Option<&str> {
        self.inner.sse_customer_key_md5()
    }

    /// The ID of the KMS key used to encrypt the object, if any.
    #[getter]
    fn sse_kms_key_id(&self) -> Option<&str> {
        self.inner.sse_kms_key_id()
    }

    /// The KMS encryption context, if any.
    #[getter]
    fn sse_kms_encryption_context(&self) -> Option<&str> {
        self.inner.sse_kms_encryption_context()
    }

    /// Whether an S3 Bucket Key was used for SSE-KMS encryption.
    #[getter]
    fn bucket_key_enabled(&self) -> Option<bool> {
        self.inner.bucket_key_enabled()
    }

    /// `"requester"` if the requester was charged for the request.
    #[getter]
    fn request_charged(&self) -> Option<&str> {
        self.inner.request_charged().map(|t| t.as_str())
    }

    /// The multipart upload ID, if the object was uploaded in parts.
    #[getter]
    fn upload_id(&self) -> Option<&str> {
        self.inner.upload_id().map(String::as_str)
    }

    /// Transfer metrics at completion.
    #[getter]
    fn metrics(&self) -> TransferMetrics {
        TransferMetrics::new(self.inner.metrics)
    }

    fn __repr__(&self) -> String {
        Repr::new("UploadResult")
            .str_field("etag", self.etag())
            .str_field("version_id", self.version_id())
            .str_field("checksum_type", self.checksum_type())
            .str_field("upload_id", self.upload_id())
            .finish()
    }
}

/// Metadata of a downloaded object, as reported by S3.
#[pyclass(frozen, skip_from_py_object, module = "aws_s3_transfer_manager")]
#[derive(Clone)]
pub struct ObjectMetadata {
    inner: RustObjectMetadata,
}

impl ObjectMetadata {
    pub(crate) fn new(inner: RustObjectMetadata) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl ObjectMetadata {
    /// Total size of the object in bytes (not only of a requested range).
    #[getter]
    fn size(&self) -> u64 {
        self.inner.total_object_size()
    }

    /// The entity tag of the object.
    #[getter]
    fn etag(&self) -> Option<&str> {
        self.inner.e_tag.as_deref()
    }

    /// When the object was last modified.
    #[getter]
    fn last_modified(&self) -> Option<SystemTime> {
        to_system_time(self.inner.last_modified.as_ref())
    }

    /// The MIME type of the object.
    #[getter]
    fn content_type(&self) -> Option<&str> {
        self.inner.content_type.as_deref()
    }

    /// The content encodings applied to the object.
    #[getter]
    fn content_encoding(&self) -> Option<&str> {
        self.inner.content_encoding.as_deref()
    }

    /// The language of the object's content.
    #[getter]
    fn content_language(&self) -> Option<&str> {
        self.inner.content_language.as_deref()
    }

    /// Presentational information for the object.
    #[getter]
    fn content_disposition(&self) -> Option<&str> {
        self.inner.content_disposition.as_deref()
    }

    /// Caching behavior for the object.
    #[getter]
    fn cache_control(&self) -> Option<&str> {
        self.inner.cache_control.as_deref()
    }

    /// The `Expires` header of the object, verbatim.
    #[getter]
    fn expires(&self) -> Option<&str> {
        self.inner.expires_string.as_deref()
    }

    /// User-defined metadata (`x-amz-meta-*`), without the prefix.
    #[getter]
    fn metadata(&self) -> std::collections::HashMap<String, String> {
        self.inner.metadata.clone().unwrap_or_default()
    }

    /// The version ID of the object.
    #[getter]
    fn version_id(&self) -> Option<&str> {
        self.inner.version_id.as_deref()
    }

    /// The storage class of the object; S3 omits it for `STANDARD`.
    #[getter]
    fn storage_class(&self) -> Option<&str> {
        self.inner.storage_class.as_ref().map(|t| t.as_str())
    }

    /// The server-side encryption algorithm used to store the object.
    #[getter]
    fn server_side_encryption(&self) -> Option<&str> {
        self.inner
            .server_side_encryption
            .as_ref()
            .map(|t| t.as_str())
    }

    /// The ID of the KMS key used to encrypt the object, if any.
    #[getter]
    fn sse_kms_key_id(&self) -> Option<&str> {
        self.inner.ssekms_key_id.as_deref()
    }

    /// Whether an S3 Bucket Key was used for SSE-KMS encryption.
    #[getter]
    fn bucket_key_enabled(&self) -> Option<bool> {
        self.inner.bucket_key_enabled
    }

    /// The algorithm of the customer-provided encryption key, if one was used.
    #[getter]
    fn sse_customer_algorithm(&self) -> Option<&str> {
        self.inner.sse_customer_algorithm.as_deref()
    }

    /// The MD5 of the customer-provided encryption key, if one was used.
    #[getter]
    fn sse_customer_key_md5(&self) -> Option<&str> {
        self.inner.sse_customer_key_md5.as_deref()
    }

    /// Whether the object is a delete marker.
    #[getter]
    fn delete_marker(&self) -> Option<bool> {
        self.inner.delete_marker
    }

    /// The object's expiration rule, if a lifecycle configuration applies.
    #[getter]
    fn expiration(&self) -> Option<&str> {
        self.inner.expiration.as_deref()
    }

    /// The restoration status of an archived object.
    #[getter]
    fn restore(&self) -> Option<&str> {
        self.inner.restore.as_deref()
    }

    /// The website redirect location of the object.
    #[getter]
    fn website_redirect_location(&self) -> Option<&str> {
        self.inner.website_redirect_location.as_deref()
    }

    /// The number of metadata entries S3 could not return as headers.
    #[getter]
    fn missing_meta(&self) -> Option<i32> {
        self.inner.missing_meta
    }

    /// The number of parts of a multipart object, when S3 reports it.
    #[getter]
    fn parts_count(&self) -> Option<i32> {
        self.inner.parts_count
    }

    /// The replication status of the object.
    #[getter]
    fn replication_status(&self) -> Option<&str> {
        self.inner.replication_status.as_ref().map(|t| t.as_str())
    }

    /// `"requester"` if the requester was charged for the request.
    #[getter]
    fn request_charged(&self) -> Option<&str> {
        self.inner.request_charged.as_ref().map(|t| t.as_str())
    }

    /// The Object Lock mode in place for the object.
    #[getter]
    fn object_lock_mode(&self) -> Option<&str> {
        self.inner.object_lock_mode.as_ref().map(|t| t.as_str())
    }

    /// When the object's Object Lock retention expires.
    #[getter]
    fn object_lock_retain_until_date(&self) -> Option<SystemTime> {
        to_system_time(self.inner.object_lock_retain_until_date.as_ref())
    }

    /// Whether the object has an Object Lock legal hold (`"ON"` or `"OFF"`).
    #[getter]
    fn object_lock_legal_hold_status(&self) -> Option<&str> {
        self.inner
            .object_lock_legal_hold_status
            .as_ref()
            .map(|t| t.as_str())
    }

    fn __repr__(&self) -> String {
        Repr::new("ObjectMetadata")
            .field("size", self.size())
            .str_field("etag", self.etag())
            .str_field("content_type", self.content_type())
            .str_field("version_id", self.version_id())
            .finish()
    }
}

/// Checksum information for a completed download.
#[pyclass(frozen, skip_from_py_object, module = "aws_s3_transfer_manager")]
#[derive(Clone)]
pub struct IntegrityChecks {
    inner: RustIntegrityChecks,
}

impl IntegrityChecks {
    pub(crate) fn new(inner: RustIntegrityChecks) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl IntegrityChecks {
    /// Whether every downloaded byte was validated against a checksum.
    ///
    /// A checksum *mismatch* is never reported here; it fails the download with an
    /// `IntegrityError`.
    #[getter]
    fn validated(&self) -> bool {
        matches!(
            self.inner.checksum_validation(),
            ChecksumValidation::Validated { .. }
        )
    }

    /// The algorithm used for validation, when `validated` is true.
    #[getter]
    fn algorithm(&self) -> Option<&str> {
        match self.inner.checksum_validation() {
            ChecksumValidation::Validated { algorithm, .. } => Some(algorithm.as_str()),
            _ => None,
        }
    }

    /// Why the download was not validated, when `validated` is false: `"disabled"`,
    /// `"composite_checksum"`, `"partial_coverage"`, or `"unavailable"`.
    #[getter]
    fn not_validated_reason(&self) -> Option<&'static str> {
        match self.inner.checksum_validation() {
            ChecksumValidation::NotValidated { reason, .. } => Some(match reason {
                NotValidatedReason::Disabled => "disabled",
                NotValidatedReason::CompositeChecksum => "composite_checksum",
                NotValidatedReason::PartialCoverage => "partial_coverage",
                _ => "unavailable",
            }),
            _ => None,
        }
    }

    /// The object's base64-encoded CRC-32 checksum, as reported by S3.
    #[getter]
    fn checksum_crc32(&self) -> Option<&str> {
        self.inner.checksum_crc32()
    }

    /// The object's base64-encoded CRC-32C checksum, as reported by S3.
    #[getter]
    fn checksum_crc32c(&self) -> Option<&str> {
        self.inner.checksum_crc32c()
    }

    /// The object's base64-encoded CRC-64/NVME checksum, as reported by S3.
    #[getter]
    fn checksum_crc64nvme(&self) -> Option<&str> {
        self.inner.checksum_crc64_nvme()
    }

    /// The object's base64-encoded SHA-1 checksum, as reported by S3.
    #[getter]
    fn checksum_sha1(&self) -> Option<&str> {
        self.inner.checksum_sha1()
    }

    /// The object's base64-encoded SHA-256 checksum, as reported by S3.
    #[getter]
    fn checksum_sha256(&self) -> Option<&str> {
        self.inner.checksum_sha256()
    }

    /// `"FULL_OBJECT"` or `"COMPOSITE"`, as reported by S3.
    #[getter]
    fn checksum_type(&self) -> Option<&str> {
        self.inner.checksum_type().map(|t| t.as_str())
    }

    fn __repr__(&self) -> String {
        let repr = Repr::new("IntegrityChecks")
            .field("validated", if self.validated() { "True" } else { "False" });
        match (self.algorithm(), self.not_validated_reason()) {
            (Some(algorithm), _) => repr.str_field("algorithm", Some(algorithm)),
            (_, reason) => repr.str_field("not_validated_reason", reason),
        }
        .finish()
    }
}

/// The outcome of a completed single-object download.
#[pyclass(frozen, skip_from_py_object, module = "aws_s3_transfer_manager")]
pub struct DownloadResult {
    /// Metadata of the downloaded object.
    #[pyo3(get)]
    metadata: Py<ObjectMetadata>,
    /// Checksum validation of the downloaded bytes.
    #[pyo3(get)]
    integrity: Py<IntegrityChecks>,
    /// Transfer metrics at completion.
    #[pyo3(get)]
    metrics: Py<TransferMetrics>,
}

impl DownloadResult {
    pub(crate) fn new(py: Python<'_>, output: DownloadOutput) -> PyResult<Self> {
        Ok(Self {
            metadata: Py::new(py, ObjectMetadata::new(output.object_meta.clone()))?,
            integrity: Py::new(py, IntegrityChecks::new(output.integrity_checks().clone()))?,
            metrics: Py::new(py, TransferMetrics::new(output.metrics))?,
        })
    }
}

#[pymethods]
impl DownloadResult {
    fn __repr__(&self, py: Python<'_>) -> String {
        format!(
            "DownloadResult(metadata={}, integrity={})",
            self.metadata.get().__repr__(),
            self.integrity.bind(py).get().__repr__()
        )
    }
}

/// A local file that could not be uploaded by `upload_directory`.
#[pyclass(frozen, skip_from_py_object, module = "aws_s3_transfer_manager")]
#[derive(Clone, Debug)]
pub struct FailedUpload {
    /// The local file, when the failure is attributable to one.
    #[pyo3(get)]
    path: Option<PathBuf>,
    /// The destination key, when the upload got as far as having one.
    #[pyo3(get)]
    key: Option<String>,
    error: ErrorInfo,
}

impl FailedUpload {
    pub(crate) fn new(failure: &aws_sdk_s3_transfer_manager::types::FailedUpload) -> Self {
        Self {
            path: failure.source_path().map(ToOwned::to_owned),
            key: failure
                .input()
                .and_then(|input| input.key())
                .map(str::to_owned),
            error: ErrorInfo::from_error(failure.error()),
        }
    }

    pub(crate) fn error_info(&self) -> &ErrorInfo {
        &self.error
    }
}

#[pymethods]
impl FailedUpload {
    /// The exception describing why the upload failed.
    #[getter]
    fn error(&self, py: Python<'_>) -> Py<PyAny> {
        self.error.to_pyerr(py).into_value(py).into_any()
    }

    fn __repr__(&self) -> String {
        Repr::new("FailedUpload")
            .field(
                "path",
                format_args!(
                    "{:?}",
                    self.path.as_deref().map(|p| p.display().to_string())
                ),
            )
            .str_field("key", self.key.as_deref())
            .field("error", format_args!("{:?}", self.error.message()))
            .finish()
    }
}

/// An object that could not be downloaded by `download_directory`.
#[pyclass(frozen, skip_from_py_object, module = "aws_s3_transfer_manager")]
#[derive(Clone, Debug)]
pub struct FailedDownload {
    /// The key of the object.
    #[pyo3(get)]
    key: Option<String>,
    error: ErrorInfo,
}

impl FailedDownload {
    pub(crate) fn new(failure: &aws_sdk_s3_transfer_manager::types::FailedDownload) -> Self {
        Self {
            key: failure.input().key().map(str::to_owned),
            error: ErrorInfo::from_error(failure.error()),
        }
    }

    pub(crate) fn error_info(&self) -> &ErrorInfo {
        &self.error
    }
}

#[pymethods]
impl FailedDownload {
    /// The exception describing why the download failed.
    #[getter]
    fn error(&self, py: Python<'_>) -> Py<PyAny> {
        self.error.to_pyerr(py).into_value(py).into_any()
    }

    fn __repr__(&self) -> String {
        Repr::new("FailedDownload")
            .str_field("key", self.key.as_deref())
            .field("error", format_args!("{:?}", self.error.message()))
            .finish()
    }
}

/// The outcome of `upload_directory`.
#[pyclass(frozen, skip_from_py_object, module = "aws_s3_transfer_manager")]
pub struct DirectoryUploadResult {
    /// Number of files uploaded successfully.
    #[pyo3(get)]
    objects_uploaded: u64,
    /// Files that failed to upload (only non-empty with `failure_policy="continue"`).
    #[pyo3(get)]
    failures: Vec<FailedUpload>,
    metrics: RustTransferMetrics,
}

impl DirectoryUploadResult {
    pub(crate) fn new(
        output: &aws_sdk_s3_transfer_manager::operation::upload_objects::UploadObjectsOutput,
    ) -> Self {
        Self {
            objects_uploaded: output.objects_uploaded(),
            failures: output
                .failed_transfers()
                .iter()
                .map(FailedUpload::new)
                .collect(),
            metrics: *output.metrics(),
        }
    }
}

#[pymethods]
impl DirectoryUploadResult {
    /// Aggregated transfer metrics at completion.
    #[getter]
    fn metrics(&self) -> TransferMetrics {
        TransferMetrics::new(self.metrics)
    }

    fn __repr__(&self) -> String {
        Repr::new("DirectoryUploadResult")
            .field("objects_uploaded", self.objects_uploaded)
            .field("failures", self.failures.len())
            .finish()
    }
}

/// The outcome of `download_directory`.
#[pyclass(frozen, skip_from_py_object, module = "aws_s3_transfer_manager")]
pub struct DirectoryDownloadResult {
    /// Number of objects downloaded successfully.
    #[pyo3(get)]
    objects_downloaded: u64,
    /// Objects that failed to download (only non-empty with `failure_policy="continue"`).
    #[pyo3(get)]
    failures: Vec<FailedDownload>,
    metrics: RustTransferMetrics,
}

impl DirectoryDownloadResult {
    pub(crate) fn new(
        output: &aws_sdk_s3_transfer_manager::operation::download_objects::DownloadObjectsOutput,
    ) -> Self {
        Self {
            objects_downloaded: output.objects_downloaded(),
            failures: output
                .failed_transfers()
                .iter()
                .map(FailedDownload::new)
                .collect(),
            metrics: *output.metrics(),
        }
    }
}

#[pymethods]
impl DirectoryDownloadResult {
    /// Aggregated transfer metrics at completion.
    #[getter]
    fn metrics(&self) -> TransferMetrics {
        TransferMetrics::new(self.metrics)
    }

    fn __repr__(&self) -> String {
        Repr::new("DirectoryDownloadResult")
            .field("objects_downloaded", self.objects_downloaded)
            .field("failures", self.failures.len())
            .finish()
    }
}

/// An object listed by `download_directory`, as passed to its `filter`.
#[pyclass(frozen, skip_from_py_object, module = "aws_s3_transfer_manager")]
pub struct ObjectSummary {
    /// The object's key.
    #[pyo3(get)]
    key: String,
    /// The object's size in bytes.
    #[pyo3(get)]
    size: Option<i64>,
    /// When the object was last modified.
    #[pyo3(get)]
    last_modified: Option<SystemTime>,
    /// The object's entity tag.
    #[pyo3(get)]
    etag: Option<String>,
    /// The object's storage class.
    #[pyo3(get)]
    storage_class: Option<String>,
}

impl ObjectSummary {
    pub(crate) fn new(object: &aws_sdk_s3::types::Object) -> Self {
        Self {
            key: object.key().unwrap_or_default().to_owned(),
            size: object.size(),
            last_modified: to_system_time(object.last_modified()),
            etag: object.e_tag().map(str::to_owned),
            storage_class: object.storage_class().map(|s| s.as_str().to_owned()),
        }
    }
}

#[pymethods]
impl ObjectSummary {
    fn __repr__(&self) -> String {
        Repr::new("ObjectSummary")
            .str_field("key", Some(&self.key))
            .field(
                "size",
                self.size
                    .map_or_else(|| "None".to_owned(), |s| s.to_string()),
            )
            .finish()
    }
}

/// `aws_s3_transfer_manager.TransferStatus` for a transfer manager status.
pub(crate) fn transfer_status<'py>(
    py: Python<'py>,
    status: aws_sdk_s3_transfer_manager::types::TransferStatus,
) -> PyResult<Bound<'py, PyAny>> {
    use aws_sdk_s3_transfer_manager::types::TransferStatus as S;
    static CLASS: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    let class = python_class(
        py,
        &CLASS,
        "aws_s3_transfer_manager.types",
        "TransferStatus",
    )?;
    let value = match status {
        S::Active => "active",
        S::Completed => "completed",
        S::Failed => "failed",
        _ => "cancelled",
    };
    class.call1((value,))
}
