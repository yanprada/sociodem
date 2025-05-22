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

from config.run_mode import DEBUG

from src.tools.utils.execution_manager import ExecutionManagerWrapper

EXECUTION_ID = "silver-ibge-2025-05-17-15h21m18s"
BASE_PARAMS = {
    "medallon": "silver",
    "data_name": "ibge",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "bronze_data_2022": ["bronze", "ibge", "censo_2022"],
        "bronze_data_2010": ["bronze", "ibge", "censo_2010"],
        "silver_data_2022": ["silver", "ibge", "censo_2022"],
    },
    "run_mode": "single_file" if DEBUG else "pipeline",
    "last_run": None,
    "materialized_views": {
        "hex_unique_sc_2022": "hex_unique_sc_2022",
        "hex_unique_sc_2022_sc_info": "hex_unique_sc_2022_sc_info",
    },
}

_manager_wrapper = ExecutionManagerWrapper(BASE_PARAMS, EXECUTION_ID, DEBUG)
manager = _manager_wrapper.manager

CONTRACT_BRONZE_2010 = manager.execution_details["data_contracts"]["bronze_data_2010"]
CONTRACT_BRONZE_2022 = manager.execution_details["data_contracts"]["bronze_data_2022"]
CONTRACTS_BRONZE = {**CONTRACT_BRONZE_2010, **CONTRACT_BRONZE_2022}

CONTRACTS_SILVER = manager.execution_details["data_contracts"]["silver_data_2022"]


EXPERIMENT_NAME = manager.execution_details["mlflow_experiment"]

PATHS_MV = manager.execution_details["materialized_views"]
