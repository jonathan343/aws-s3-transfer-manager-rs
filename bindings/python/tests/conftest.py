# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Fixtures: an in-process S3 (moto) server, boto3 for setup/inspection, and a TransferManager."""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING, Any

import boto3
import pytest
from moto.server import ThreadedMotoServer

from aws_s3_transfer_manager import MiB, TransferManager

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

REGION = "us-east-1"
CREDENTIALS = {"aws_access_key_id": "testing", "aws_secret_access_key": "testing"}


@pytest.fixture(scope="session", autouse=True)
def _isolated_aws_environment(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    """Keep the developer's AWS configuration and instance metadata out of the tests."""
    empty = tmp_path_factory.mktemp("aws") / "empty"
    empty.touch()
    with pytest.MonkeyPatch.context() as mp:
        for name in list(os.environ):
            if name.startswith("AWS_"):
                mp.delenv(name)
        mp.setenv("AWS_CONFIG_FILE", str(empty))
        mp.setenv("AWS_SHARED_CREDENTIALS_FILE", str(empty))
        mp.setenv("AWS_EC2_METADATA_DISABLED", "true")
        yield


@pytest.fixture(scope="session")
def endpoint_url(_isolated_aws_environment: None) -> Iterator[str]:
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    yield f"http://{host}:{port}"
    server.stop()


@pytest.fixture(scope="session")
def s3(endpoint_url: str) -> Any:
    return boto3.client("s3", endpoint_url=endpoint_url, region_name=REGION, **CREDENTIALS)


@pytest.fixture
def bucket(s3: Any) -> str:
    name = f"test-{uuid.uuid4().hex[:16]}"
    s3.create_bucket(Bucket=name)
    return name


@pytest.fixture
def tm_kwargs(endpoint_url: str) -> dict[str, Any]:
    """Arguments for a TransferManager against the test server, with small (5 MiB) parts."""
    return {
        "region": REGION,
        "endpoint_url": endpoint_url,
        "force_path_style": True,
        "part_size": 5 * MiB,
        "multipart_threshold": 5 * MiB,
        **CREDENTIALS,
    }


@pytest.fixture
def tm(tm_kwargs: dict[str, Any]) -> Iterator[TransferManager]:
    with TransferManager(**tm_kwargs) as manager:
        yield manager


@pytest.fixture
def payload() -> bytes:
    """12 MiB of random data: three parts at the test part size."""
    return os.urandom(12 * MiB)


@pytest.fixture
def local_file(tmp_path: Path, payload: bytes) -> Path:
    path = tmp_path / "payload.bin"
    path.write_bytes(payload)
    return path
