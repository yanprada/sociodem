"""
This script is used to download files from an S3 bucket with
a given prefix.

"""

import os

from src.tools.databases.data_request.drivers.http_requester import (
    HttpRequesterOvertureMaps,
)
from src.tools.managers.execution import ExecutionManager
from src.databases.bronze.pois.config import EXECUTION_ID, BASE_PARAMS


manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, overwrite=True)
module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)

POIS_CONTRACTS = execution_parameters["data_contracts"]["pois_bronze"]


def main():
    """
    Downloads files from a specified bucket and prefix, and saves the
    downloaded files information in a JSON file.
    """
    download_path = POIS_CONTRACTS["raw_data"]["physicalPath"]
    prefix = "places"
    requester = HttpRequesterOvertureMaps(prefix, download_path)
    requester.download_places_omf()
    manager.update_last_run()
