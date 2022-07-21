import io
import json
import logging
import os
from typing import Optional

import awswrangler as wr
import boto3
import pandas as pd
import xarray as xr

# Get some destinations for the results
BUCKET_SOURCE = os.environ.get("BUCKET_SOURCE", "imos-data-lab-raw")
BUCKET_OPTIMISED = os.environ.get("BUCKET", "imos-data-lab-optimised")

# Set up a tidy logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info("Starting NetCDF Wrangler")


class SingleDimensionalHandler:
    def __init__(self, input_object_key):
        self.raw_bucket_name = BUCKET_SOURCE
        self.optimised_bucket_name = BUCKET_OPTIMISED
        self.input_object_key = input_object_key
        self.database_name = "single_dimensional"

    def create_df_ds(self):
        logger.info(f"Starting to create df and ds for key {self.input_object_key}")
        with io.BytesIO() as inmemoryfile:
            s3 = boto3.client("s3")
            s3.download_fileobj(
                self.raw_bucket_name, self.input_object_key, inmemoryfile
            )
            inmemoryfile.seek(0)
            ds = xr.open_dataset(inmemoryfile)

            # create dataframe
            df = ds.to_dataframe()
            df = df.reset_index()

            return df, ds

    def create_parquet(self):
        logger.info(f"Starting to create parquet for key {self.input_object_key}")
        df, ds = self.create_df_ds()

        # create data for athena keys(?)
        df["date"] = pd.DatetimeIndex(df["TIME"]).date
        df["filename"] = os.path.basename(self.input_object_key)

        # get info for table name and parquet path
        if "cdm_data_type" in ds.attrs.keys():
            table_name = ds.attrs[
                "cdm_data_type"
            ].lower()  # for example station or trajectory
        else:
            raise ValueError()

        description = ds.attrs["cdm_data_type"]
        column_comments = dict()
        for var in ds.variables.keys():
            column_comments[var] = ds.variables[var].attrs["long_name"]

        if "deployment_code" in ds.attrs.keys():
            df["deployment_code"] = ds.attrs.get("deployment_code", "NO_DEPLOYMENT_CODE")
        else:
            raise ValueError()

        parameter_list = ["filename", "deployment_code", "date"]

        # create and push parquet
        logger.info("Preparing to write...")
        result = wr.s3.to_parquet(
            df=df,
            database=self.database_name,
            dataset=True,
            table=table_name,
            mode="overwrite_partitions",
            description=description,
            columns_comments=column_comments,
            path=f"s3://{self.optimised_bucket_name}/parquet/{table_name}/",
            partition_cols=parameter_list,
            concurrent_partitioning=True,
        )

        return result


def get_key(record: str) -> Optional[str]:
    if record.get("s3") is not None:
        return record["s3"]["object"]["key"]
    elif record.get("Sns") is not None:
        message = json.loads(record["Sns"]["Message"])
        return message["Records"][0]["s3"]["object"]["key"]


def handler(event, _):
    logger.info(f"Event: {event}")

    for record in event["Records"]:
        key = get_key(record)
        if ".nc" in key:
            result = SingleDimensionalHandler(key).create_parquet()
            logger.info(f"Processed file {key} with the result {result}")
        else:
            logger.info(f"Skipping file {key}")
