# Loading SOOP XBT NRT data into Parquet

This is a repo that holds code for reading `bufr` formatted CSV files
and loading them into a reasonably optimally partitioned Parquet
file.

The Dockerfile contains the required code and is used for a Lambda
function, which is triggered from a SNS notification from a S3 creation
event.

Unfortunately, the Lambda deployment was manual, so there's no infra
code.

There are also some notebooks for exploration of
the parquet creation and consumption.

Tests exist, but the important one doesn't pass because moto can't
mock S3 the way aiobotocore expects... I think.
