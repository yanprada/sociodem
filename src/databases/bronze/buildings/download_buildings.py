"""
This module provides functions to download data for buildings from different sources.

The module includes the following functions:
- download_data(source): Downloads data from a specified source.
- main(): The main function that downloads data for buildings from both "omf" and "google" sources.
"""

import os
from src.tools.data_contract.aneel_data_contract import get_contract
from src.tools.databases.data_request.drivers.http_requester import (
    HttpRequesterBuildings,
)
from src.tools.utils.constants import BUILDING_PARTITIONS

CONTRACTS = {
    "omf": get_contract("buildings/contract_omf_buildings.yaml", "bronze"),
    "google": get_contract("buildings/contract_google_buildings.yaml", "bronze"),
}


def download_data(source) -> None:
    """
    Downloads data from a specified source.

    Args:
        source (str): The data source to download from.
    """
    omf_request = HttpRequesterBuildings(source)
    path = os.path.join(
        CONTRACTS[source]["physicalPath"]
        .replace("databases", "datalake")
        .replace("bronze/", "")
    )
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
