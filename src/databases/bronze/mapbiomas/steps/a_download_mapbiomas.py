"""
This script is used to download Mapbiomas data for a specified range of years.

It imports the necessary modules and defines the CONTRACT variable, which contains the contract
information for downloading Mapbiomas data. The main() function is the entry point of the script,
which initializes an instance of HttpRequesterMapbiomas and makes a request to download data
for the specified range of years using the contract defined in CONTRACT.
"""

import os

from src.tools.managers.http_requester import (
    HttpRequesterMapbiomas,
)

from src.databases.bronze.mapbiomas.config import manager, CONTRACTS_RAW, YEARS

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


def main() -> None:
    """
    This function is the entry point of the script for downloading Mapbiomas data.
    It initializes an instance of HttpRequesterMapbiomas and makes a request to download data
    for the specified range of years using the contract defined in CONTRACT.
    """
    random_year = YEARS[0]  # random year, just to get the path
    requester = HttpRequesterMapbiomas()
    requester.request_from_page(
        YEARS, CONTRACTS_RAW[f"brasil_coverage_{random_year}"]["physicalPath"]
    )
    manager.update_last_run()


if __name__ == "__main__":
    main()
