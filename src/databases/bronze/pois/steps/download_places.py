"""
This script is used to download files from an S3 bucket with a given prefix.

The script defines a function `list_and_download_files` that lists and 
    downloads files from an S3 bucket.
The function takes three arguments:
- `download_dir`: The local directory where the files will be downloaded.
- `bucket`: The name of the S3 bucket.
- `prefix`: The prefix used to filter the files in the S3 bucket.

The function returns a list of file keys that were downloaded.

The script also defines a `main` function that sets the download directory, 
bucket name, and prefix, and calls the `list_and_download_files` function.
The downloaded files are saved in a JSON file named "downloaded_files.json" 
in the download directory.

"""

import os
import boto3
from tqdm import tqdm
from botocore import UNSIGNED
from botocore.config import Config

from src.tools.data_contract.pois_data_contract import get_pois_contracts
from src.tools.utils.common import write_log

POIS_CONTRACTS = get_pois_contracts("bronze")


def list_and_download_files_omf(download_dir, bucket, prefix):
    """
    Lists and downloads files from an S3 bucket with the given prefix.

    Args:
        download_dir (str): The local directory where the files will be downloaded.
        bucket (str): The name of the S3 bucket.
        prefix (str): The prefix used to filter the files in the S3 bucket.
    """
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    continuation_token = None
    while True:
        if continuation_token:
            response = s3.list_objects_v2(
                Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token
            )
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" in response:
            for obj in tqdm(response["Contents"], desc="Downloading files"):
                key = obj["Key"]
                file_name = key.split("/")[-1]
                file_path = os.path.join(download_dir, file_name)
                s3.download_file(bucket, key, file_path)
                write_log(f"Downloaded {file_name}")
        if response.get("IsTruncated"):  # More pages to fetch
            continuation_token = response.get("NextContinuationToken")
        else:
            break


def main():
    """
    Downloads files from a specified bucket and prefix, and saves the
    downloaded files information in a JSON file.
    """
    download_dir = POIS_CONTRACTS["datalake"]["physicalPath"]
    bucket = "overturemaps-us-west-2"
    prefix = "release/2024-07-22.0/theme=places/"
    os.makedirs(download_dir, exist_ok=True)
    list_and_download_files_omf(download_dir, bucket, prefix)
