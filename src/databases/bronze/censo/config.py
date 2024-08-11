"""
This module contains the configuration settings for the bronze 
    database related to the Buildings data.

Attributes:
    EXECUTION_ID (None): The execution ID.
    PARAMS (dict): A dictionary containing the parameters for the bronze database.
        - "medallon" (str): The medallon value for the bronze database.
        - "data_name" (str): The data name for the buildings data.

"""

import os

from src.tools.data_contract.censo_data_contract import get_censo_contracts
from src.tools.data_contract.validation_data_contract import get_validation_partitions
from config.run_mode import DEBUG


EXECUTION_ID = None

BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "censo",
    "config_path": os.path.abspath(__file__),
    "data_contracts": [
        get_censo_contracts("bronze"),
        get_validation_partitions(),
    ],
    "run_mode": "single_file" if DEBUG else "pipeline",
    "last_run": None,
}
