"""
This script is used to download Mapbiomas data for a specified range of years.

It imports the necessary modules and defines the CONTRACT variable, which contains the contract
information for downloading Mapbiomas data. The main() function is the entry point of the script,
which initializes an instance of HttpRequesterMapbiomas and makes a request to download data
for the specified range of years using the contract defined in CONTRACT.
"""

import os

from src.tools.databases.data_request.drivers.http_requester import (
    HttpRequesterMapbiomas,
)
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.mapbiomas.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(execution_parameters, module_name)

CONTRACTS_BRONZE = execution_parameters["data_contracts"]["mapbiomas_bronze"]
CONTRACTS_RAW = execution_parameters["data_contracts"]["mapbiomas_raw"]


def main() -> None:
    """
    This function is the entry point of the script for downloading Mapbiomas data.
    It initializes an instance of HttpRequesterMapbiomas and makes a request to download data
    for the specified range of years using the contract defined in CONTRACT.
    """
    year_init, year_end = CONTRACTS_RAW["raw_data"]["queryYears"]
    requester = HttpRequesterMapbiomas()
    requester.request_from_page(
        range(year_init, year_end), CONTRACTS_RAW["raw_data"]["physicalPath"]
    )
    manager.update_last_run()


if __name__ == "__main__":
    main()
