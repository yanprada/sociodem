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

from src.tools.data_contract.pois_data_contract import get_pois_contracts
from src.tools.data_contract.censo_data_contract import get_censo_contracts

from config.run_mode import DEBUG

EXECUTION_ID = None

BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "pois",
    "config_path": os.path.abspath(__file__),
    "data_contracts": [get_pois_contracts("bronze"), get_censo_contracts("bronze")],
    "run_mode": "single_file" if DEBUG else "pipeline",
    "last_run": None,
}
