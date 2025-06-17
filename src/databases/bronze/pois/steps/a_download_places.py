"""
This script is used to download files from an S3 bucket with
a given prefix.

"""

import os

from src.tools.databases.data_request.drivers.http_requester import (
    HttpRequesterOvertureMaps,
)
from src.databases.bronze.pois.config import (
    CONTRACTS_RAW,
    manager,
)

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


def main():
    """
    Downloads files from a specified bucket and prefix, and saves the
    downloaded files information in a JSON file.
    """
    download_path = CONTRACTS_RAW["pois"]["physicalPath"]
    prefix = "places"
    requester = HttpRequesterOvertureMaps(prefix, download_path)
    requester.download_places_omf()
