"""
This module provides functions to download data for buildings from different sources.

The module includes the following functions:
- download_data(source): Downloads data from a specified source.
- main(): The main function that downloads data for buildings from both "omf" and "google" sources.
"""

import os
from src.tools.databases.data_request.drivers.http_requester import (
    HttpRequesterBuildings,
)
from src.tools.utils.constants import BUILDING_PARTITIONS
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.buildings.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
CONTRACTS = execution_parameters["data_contracts"][0]
manager.update_status("running_step_1")


def download_data(source) -> None:
    """
    Downloads data from a specified source.

    Args:
        source (str): The data source to download from.
    """
    omf_request = HttpRequesterBuildings(source)
    path = os.path.join(CONTRACTS[source]["physicalPath"])
    files = BUILDING_PARTITIONS[source]
    omf_request.request_from_page(
        files,
        path,
    )


def main() -> None:
    """
    This is the main function that downloads data for buildings.
    It calls the download_data function twice, once for "omf" and once for "google".
    """
    download_data("omf")
    download_data("google")
    manager.update_status("finished_step_1")
    manager.update_last_run()
