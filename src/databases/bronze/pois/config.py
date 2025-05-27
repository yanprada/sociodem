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
from src.tools.data_contract.ibge_data_contract import get_ibge_contracts


EXECUTION_ID = "bronze-pois-Ioz9Kgv4r8LFXUD"

BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "pois",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "pois_bronze": get_pois_contracts("bronze"),
        "ibge_bronze": get_ibge_contracts("bronze"),
    },
    "run_mode": "dev",
    "last_run": None,
}
