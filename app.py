import awswrangler as wr
import pandas as pd
from aodndata.soop.soop_xbt_nrt import parse_bufr_file

import os
import logging


# Get some destinations for the results
BUCKET_SOURCE = os.environ.get("BUCKET_SOURCE", "imos-data-lab-raw")
BUCKET_OPTIMISED = os.environ.get("BUCKET", "imos-data-lab-optimised")
PATH_OPTIMISED = os.environ.get("DEST_PATH", "parquet/xbt")
TABLE_NAME = os.environ.get("TABLE_NAME", "soop_xbt_nrt")
DATABASE_NAME = os.environ.get("DATABASE_NAME", "profile")

# Set up a tidy logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info("Starting XBT-WRANGLER")


def process_file(key):
    logger.info(f"Processing file {key} for bucket {BUCKET_SOURCE}")
    file = f"s3://{BUCKET_SOURCE}/{key}"

    # Parse the file. Returns list of dicts each with three dicts in it
    profiles = parse_bufr_file(file)

    for n, profile in enumerate(profiles, start=1):
        logger.info(f"Processing profile {n}")

        # Probably should format this better and do something with it...
        metadata = {
            "profile_geotime": profile["profile_geotime"],
            "profile_metadata": profile["profile_metadata"],
        }
        metadata["profile_geotime"][
            "date_utc"
        ] = f'{metadata["profile_geotime"]["date_utc"]:%Y-%m-%dT%H:%M:00}'

        p = profile["profile_data"]
        # These are floats
        data_headers = ["depth", "temp"]
        # These are ints
        data_headers_array = ["glob_gtspp", "glob_gtspp_depth", "glob_gtspp_temp"]

        # Merge them together into a list of lists with shape [[x,x,x,x,x],[x,x,x,x,x],...]
        to_zip = [list(p[var].values) for var in data_headers] + [
            list(p[var]) for var in data_headers_array
        ]
        data = list(zip(*to_zip))

        # Make a nice WKT point
        longitude = metadata["profile_geotime"]["longitude"]
        latitude = metadata["profile_geotime"]["latitude"]
        point = f'POINT ({longitude} {latitude})'

        # Get time
        time = metadata["profile_geotime"]["date_utc"]

        # Get a UID (assumed to be unique cause it says it is)
        uid = metadata["profile_metadata"]["XBT_uniqueid"]

        # Now we want a dataframe...
        df = pd.DataFrame(data, columns=data_headers + data_headers_array)
        df["uid"] = uid
        df["geom"] = point
        df["datetime"] = time
        df["datetime"] = pd.to_datetime(df.datetime)
        for var in data_headers_array:
            df[var] = df[var].astype(int)

        path = f"s3://{BUCKET_OPTIMISED}/{PATH_OPTIMISED}/"
        logger.info(f"Writing to S3 at {path}")
        result = wr.s3.to_parquet(
            df=df,
            path=path,
            dataset=True,
            database=DATABASE_NAME,
            table=TABLE_NAME,
            mode="append",
            partition_cols=["uid"],
        )
        if not result:
            logger.error(f"Failed to write {file} to parquet")
        else:
            return result


def handler(event, _):
    logger.info(f"Event: {event}")

    for record in event["Records"]:
        key = record["s3"]["object"]["key"]
        if "XBT" in key:
            result = process_file(key)
            logger.info(f"Processed file {key} with the result {result}")
        else:
            logger.info(f"Skipping file {key}")
