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
from src.tools.data_contract.mapbiomas_data_contract import get_mapbiomas_contracts

CONTRACT = get_mapbiomas_contracts("bronze")["mapbiomas_2022"]


def main() -> None:
    """
    This function is the entry point of the script for downloading Mapbiomas data.
    It initializes an instance of HttpRequesterMapbiomas and makes a request to download data
    for the specified range of years using the contract defined in CONTRACT.
    """
    requester = HttpRequesterMapbiomas()
    requester.request_from_page(range(2018, 2023), CONTRACT["physicalPath"])
