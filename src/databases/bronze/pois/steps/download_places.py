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

import json
import os
import boto3
from botocore import UNSIGNED
from botocore.config import Config

BRAZILIAN_BBOX = [
    [
        [-74.0703974804, -34.2545333765],
        [-33.1129000343, -34.2545333765],
        [-33.1129000343, 5.6600149373],
        [-74.0703974804, 5.6600149373],
        [-74.0703974804, -34.2545333765],
    ]
]


# Function to list and download files
def list_and_download_files(download_dir, bucket, prefix):
    """
    Lists and downloads files from an S3 bucket with the given prefix.

    Args:
        download_dir (str): The local directory where the files will be downloaded.
        bucket (str): The name of the S3 bucket.
        prefix (str): The prefix used to filter the files in the S3 bucket.

    Returns:
        list: A list of file keys that were downloaded.

    """
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    continuation_token = None
    files = []
    while True:
        if continuation_token:
            response = s3.list_objects_v2(
                Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token
            )
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" in response:
            for obj in response["Contents"]:
                key = obj["Key"]
                file_name = key.split("/")[-1]
                file_path = os.path.join(download_dir, file_name)
                s3.download_file(bucket, key, file_path)
                print(f"Downloaded {file_name} to {file_path}")
                files.append(key)
        if response.get("IsTruncated"):  # More pages to fetch
            continuation_token = response.get("NextContinuationToken")
        else:
            break
    return files


def main():
    """
    Downloads files from a specified bucket and prefix, and saves the
    downloaded files information in a JSON file.
    """
    download_dir = "downloaded_files"
    bucket = "overturemaps-us-west-2"
    prefix = "release/2024-07-22.0/theme=places/"
    os.makedirs(download_dir, exist_ok=True)
    files_downloaded = list_and_download_files(download_dir, bucket, prefix)
    output = {"bucket": bucket, "prefix": prefix, "files": files_downloaded}
    with open("downloaded_files.json", "w", encoding="utf-8") as output_file:
        output_file.write(json.dumps(output, indent=4))
