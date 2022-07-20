import json
import os

import boto3
import moto
import pandas as pd
import pytest
from pytest import fixture

from app import BUCKET_OPTIMISED, BUCKET_SOURCE, get_key, handler


@fixture
def event_s3():
    with open("s3-event.json") as f:
        return json.load(f)


@fixture
def event_sns():
    with open("sns-event.json") as f:
        return json.load(f)


def test_get_key_s3(event_s3):
    records = event_s3["Records"]
    for record in records:
        key = get_key(record)
        assert key is not None


def test_get_key_sns(event_sns):
    records = event_sns["Records"]
    for record in records:
        key = get_key(record)
        assert key is not None


@moto.mock_s3
@pytest.mark.skip("Can't mock s3 properly, skipping")
def test_handler(event):
    # Set up some fake infra for testing
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    client = boto3.client("s3")
    client.create_bucket(Bucket=BUCKET_SOURCE)
    client.create_bucket(Bucket=BUCKET_OPTIMISED)

    key = event.get("Records")[0]["s3"]["object"]["key"]
    # upload local file to s3
    client.upload_file("./IOSS01_AMMC_20200901134700_D5LR9.csv", BUCKET_SOURCE, key)
    csv_path = f"s3://{BUCKET_SOURCE}/{key}"
    pd.read_csv(csv_path, header=None, engine="python", error_bad_lines=False)

    # Try running the thing. Doesn't work because pandas doesn't
    # like working with mocked S3...
    handler(event, {})

    # # Should check we can read the parquet file...
    # head = wr.s3.read_parquet("s3://imos-data-lab-optimised/TEST_DATA/XBT-WRANGLER/").head()

    # assert head is not None
