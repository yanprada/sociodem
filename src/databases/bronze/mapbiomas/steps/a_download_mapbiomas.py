"""
This script is used to download Mapbiomas data for a specified range of years.

It imports the necessary modules and defines the CONTRACT variable, which contains the contract
information for downloading Mapbiomas data. The main() function is the entry point of the script,
which initializes an instance of HttpRequesterMapbiomas and makes a request to download data
for the specified range of years using the contract defined in CONTRACT.
"""

from src.tools.databases.data_request.drivers.http_requester import (
    HttpRequesterMapbiomas,
)
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.mapbiomas.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
manager.update_status("running_step_1")

CONTRACTS = execution_parameters["data_contracts"][0]


def main() -> None:
    """
    This function is the entry point of the script for downloading Mapbiomas data.
    It initializes an instance of HttpRequesterMapbiomas and makes a request to download data
    for the specified range of years using the contract defined in CONTRACT.
    """
    requester = HttpRequesterMapbiomas()
    requester.request_from_page(
        range(2018, 2023), CONTRACTS["mapbiomas_2022"]["physicalPath"]
    )
    manager.update_last_run()


if __name__ == "__main__":
    main()
