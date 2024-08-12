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
from src.tools.data_contract.validation_data_contract import get_validation_partitions
from config.run_mode import DEBUG

EXECUTION_ID = None
BASE_PARAMS = {
    "medallon": "silver",
    "data_name": "mapbiomas",
    "config_path": os.path.abspath(__file__),
    "data_contracts": [
        get_mapbiomas_contracts("bronze"),
        get_mapbiomas_contracts("silver"),
        get_validation_partitions(),
    ],
    "run_mode": "single_file" if DEBUG else "pipeline",
    "last_run": None,
}
