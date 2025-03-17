"""
This module provides functions to download data for buildings from different sources.

The module includes the following functions:
- download_data(source): Downloads data from a specified source.
- main(): The main function that downloads data for buildings from both "omf" and "google" sources.
"""

import os

from src.tools.databases.data_request.drivers.http_requester import (
    HttpRequesterOvertureMaps,
)
from src.databases.bronze.buildings.omf.config import manager, CONTRACT_RAW_OMF

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


def download_buildings_omf():
    """
    Downloads data for buildings from the "omf" source.
    """
    download_path = CONTRACT_RAW_OMF["omf"]["physicalPath"]
    theme = "buildings"
    requester = HttpRequesterOvertureMaps(theme, download_path)
    requester.download_buildings_omf()


def main():
    """
    Downloads files from a specified bucket and prefix, and saves the
    downloaded files information in a JSON file.
    """
    download_buildings_omf()
    manager.update_last_run()
