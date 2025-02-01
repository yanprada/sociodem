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
from config.run_mode import DEBUG


EXECUTION_ID = "silver-censo-RDzIMAQkgi4kJFr"

BASE_PARAMS = {
    "medallon": "silver",
    "data_name": "censo",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "censo_bronze": get_censo_contracts("bronze"),
        "censo_silver": get_censo_contracts("silver"),
    },
    "run_mode": "single_file" if DEBUG else "pipeline",
    "last_run": None,
}
