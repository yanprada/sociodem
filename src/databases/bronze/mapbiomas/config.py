"""
This module contains the configuration settings for the bronze 
    database related to the Mapbiomas data.

Attributes:
    EXECUTION_ID (None): The execution ID.
    PARAMS (dict): A dictionary containing the parameters for the bronze database.
        - "medallon" (str): The medallon value for the bronze database.
        - "data_name" (str): The data name for the Mapbiomas data.

"""

import os
from src.tools.data_contract.mapbiomas_data_contract import get_mapbiomas_contracts
from config.run_mode import DEBUG

EXECUTION_ID = "bronze-mapbiomas-nezY66KEh4bQ68C"
BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "mapbiomas",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "mapbiomas_raw": get_mapbiomas_contracts("raw"),
        "mapbiomas_bronze": get_mapbiomas_contracts("bronze"),
    },
    "run_mode": "single_file" if DEBUG else "pipeline",
    "last_run": None,
}
