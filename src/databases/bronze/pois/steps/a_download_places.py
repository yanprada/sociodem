"""
This script is used to download files from an S3 bucket with 
a given prefix.

"""

from src.tools.databases.data_request.drivers.http_requester import (
    HttpRequesterOvertureMaps,
)
from src.tools.data_contract.pois_data_contract import get_pois_contracts


POIS_CONTRACTS = get_pois_contracts("bronze")


def main():
    """
    Downloads files from a specified bucket and prefix, and saves the
    downloaded files information in a JSON file.
    """
    download_path = POIS_CONTRACTS["datalake"]["physicalPath"]
    prefix = "release/2024-07-22.0/theme=places/"
    requester = HttpRequesterOvertureMaps(prefix, download_path)
    requester.download_files_omf()
