"""
This script is used to download files from an S3 bucket with 
a given prefix.

"""

from src.tools.databases.data_request.drivers.http_requester import (
    HttpRequesterOvertureMaps,
)
from src.tools.data_contract.transport_data_contract import get_transportation_contracts


TRANSPORT_CONTRACTS = get_transportation_contracts("bronze")


def main():
    """
    Downloads files from a specified bucket and prefix, and saves the
    downloaded files information in a JSON file.
    """
    download_path = TRANSPORT_CONTRACTS["datalake"]["physicalPath"]
    prefix = "release/2024-07-22.0/theme=transportation/"
    requester = HttpRequesterOvertureMaps(prefix, download_path)
    requester.download_files_omf()
