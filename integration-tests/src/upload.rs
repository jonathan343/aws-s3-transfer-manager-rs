/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Upload integration tests.

use aws_sdk_s3_transfer_manager::io::InputStream;
use aws_sdk_s3_transfer_manager::metrics::unit::ByteUnit;
use aws_sdk_s3_transfer_manager::types::RuntimeMode;

use crate::harness::{mock_tm, MockTm};

async fn setup() -> MockTm {
    mock_tm(RuntimeMode::Managed).await
}

#[tokio::test]
async fn test_mpu_upload_small_file() {
    let m = setup().await;

    let content = vec![0u8; 16 * ByteUnit::Mebibyte.as_bytes_usize()]; // 16MB = 2 parts at 8MB default
    let expected_content = content.clone();

    let upload_handle = m
        .client
        .upload()
        .bucket("test-bucket")
        .key("test-key")
        .body(InputStream::from(content))
        .initiate()
        .expect("initiate upload");

    let result = upload_handle.join().await.expect("upload complete");
    assert!(result.e_tag().is_some(), "should have etag");
    assert!(
        result.upload_id().is_some(),
        "should have upload_id for MPU"
    );

    let s3_client = m.handle.client().await;
    let get_result = s3_client
        .get_object()
        .bucket("test-bucket")
        .key("test-key")
        .send()
        .await
        .expect("get object");

    let body = get_result.body.collect().await.expect("collect body");
    assert_eq!(body.to_vec(), expected_content);

    m.handle.shutdown().await.expect("shutdown");
}

async fn test_mpu_upload_concurrent(rt: RuntimeMode) {
    let m = mock_tm(rt).await;

    let mut handles = Vec::new();

    // Start multiple concurrent uploads
    for i in 0..5 {
        let content = vec![i as u8; 8 * ByteUnit::Mebibyte.as_bytes_usize()];
        let key = format!("concurrent-key-{}", i);

        let upload_handle = m
            .client
            .upload()
            .bucket("test-bucket")
            .key(&key)
            .body(InputStream::from(content))
            .initiate()
            .expect("initiate upload");

        handles.push((key, upload_handle));
    }

    // Wait for all uploads to complete
    for (key, handle) in handles {
        let result = handle.join().await;
        assert!(
            result.is_ok(),
            "upload {} should succeed: {:?}",
            key,
            result
        );
    }

    m.handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_mpu_upload_concurrent_mock_gp() {
    test_mpu_upload_concurrent(RuntimeMode::Managed).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mpu_upload_concurrent_tokio_mt() {
    test_mpu_upload_concurrent(RuntimeMode::MultiThreadTokio).await;
}

#[tokio::test]
async fn test_upload_verify_data_integrity() {
    let m = setup().await;

    // Create content with recognizable pattern
    let content: Vec<u8> = (0..24 * ByteUnit::Mebibyte.as_bytes_usize()) // 24MB = 3 parts
        .map(|i| (i % 256) as u8)
        .collect();
    let expected_content = content.clone();

    let upload_handle = m
        .client
        .upload()
        .bucket("test-bucket")
        .key("integrity-test")
        .body(InputStream::from(content))
        .initiate()
        .expect("initiate upload");

    upload_handle.join().await.expect("upload complete");

    let s3_client = m.handle.client().await;
    let get_result = s3_client
        .get_object()
        .bucket("test-bucket")
        .key("integrity-test")
        .send()
        .await
        .expect("get object");

    let body = get_result.body.collect().await.expect("collect body");
    assert_eq!(
        body.to_vec(),
        expected_content,
        "data integrity check failed"
    );

    m.handle.shutdown().await.expect("shutdown");
}

/// Aborting a multipart upload whose parts are stuck in flight must interrupt them rather than
/// wait for them, and must work when the client uses the managed runtime's HTTP transport:
/// `abort()` sends `AbortMultipartUpload` from the caller's task, not a managed thread.
#[tokio::test]
async fn test_abort_stalled_upload_with_runtime_http() {
    use aws_sdk_s3_transfer_manager::io::adapters::TokioIo;
    use aws_sdk_s3_transfer_manager::io::SizeHint;
    use aws_sdk_s3_transfer_manager::types::PartSize;
    use aws_sdk_s3_transfer_manager::{Config, S3ClientConfig};
    use tokio::io::AsyncWriteExt;

    let server = s3_mock_server::S3MockServer::builder()
        .with_in_memory_store()
        .build()
        .expect("build mock server");
    let handle = server.start().await.expect("start mock server");
    let s3_client = handle.client().await;
    let part_size = 5 * ByteUnit::Mebibyte.as_bytes_usize();
    // `s3_config` (rather than a finished client) lets the runtime inject its own transport.
    let tm = aws_sdk_s3_transfer_manager::Client::new(
        Config::builder()
            .s3_config(S3ClientConfig::new(s3_client.config().to_builder()))
            .part_size(PartSize::Target(part_size as u64))
            .build(),
    );

    // A stream of unknown length that delivers one part and then stalls without ending.
    let (mut writer, reader) = tokio::io::duplex(64 * 1024);
    let feeder = tokio::spawn(async move {
        writer.write_all(&vec![0u8; part_size]).await.unwrap();
        std::future::pending::<()>().await;
    });
    let upload = tm
        .upload()
        .bucket("test-bucket")
        .key("stalled")
        .body(InputStream::from_part_stream(TokioIo::new(
            reader,
            SizeHint::default(),
        )))
        .initiate()
        .expect("initiate upload");
    let monitor = upload.monitor();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while monitor.metrics().network_tx == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the first part is uploaded");

    let aborted = tokio::time::timeout(std::time::Duration::from_secs(10), upload.abort())
        .await
        .expect("abort must interrupt parts stuck on the stream, not wait for them")
        .expect("abort succeeds");
    let upload_id = aborted.upload_id().expect("a multipart upload was started");
    let orphan = s3_client
        .upload_part()
        .bucket("test-bucket")
        .key("stalled")
        .upload_id(upload_id)
        .part_number(2)
        .body(vec![0u8; 1].into())
        .send()
        .await;
    assert!(
        orphan.is_err(),
        "the multipart upload must have been aborted"
    );

    feeder.abort();
    handle.shutdown().await.expect("shutdown");
}
