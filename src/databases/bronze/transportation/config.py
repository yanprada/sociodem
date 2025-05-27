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

from src.tools.data_contract.transport_data_contract import get_transportation_contracts
from src.tools.data_contract.ibge_data_contract import get_ibge_contracts


EXECUTION_ID = None

BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "transportation",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "transportation_bronze": get_transportation_contracts("bronze"),
        "ibge_bronze": get_ibge_contracts("bronze"),
    },
    "run_mode": "dev",
    "last_run": None,
}
