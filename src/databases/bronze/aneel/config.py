"""
This module contains the configuration settings for the bronze database related to the ANEEL data.

Attributes:
    EXECUTION_ID (None): The execution ID.
    PARAMS (dict): A dictionary containing the parameters for the bronze database.
        - "medallon" (str): The medallon value for the bronze database.
        - "data_name" (str): The data name for the ANEEL data.

"""

import os

from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from config.run_mode import DEBUG

EXECUTION_ID = "bronze-aneel-CyD8lAfmAw1kJUF"

BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "aneel",
    "config_path": os.path.abspath(__file__),
    "data_contracts": [get_aneel_contracts("bronze")],
    "run_mode": "single_file" if DEBUG else "pipeline",
    "last_run": None,
}
