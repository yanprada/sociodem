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


from src.tools.managers.execution import ExecutionManagerWrapper

EXECUTION_ID = "bronze-buildings-2025-05-22-19h18m42s"

BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "buildings",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "raw_google": ["raw_data", "buildings", "google"],
        "raw_state_ibge": ["raw_data", "ibge", "ibge_2022"],
        "bronze_google": ["bronze", "buildings", "google"],
    },
    "run_mode": "dev",
    "last_run": None,
    "materialized_views": {},
}

_manager_wrapper = ExecutionManagerWrapper(BASE_PARAMS, EXECUTION_ID, overwrite=True)
manager = _manager_wrapper.manager


BUILDING_CONTRACTS_RAW = manager.execution_details["data_contracts"]["raw_google"]
STATE_CONTRACTS_RAW = manager.execution_details["data_contracts"]["raw_state_ibge"][
    "states_2022"
]
BUILDING_CONTRACTS_BRONZE = manager.execution_details["data_contracts"]["bronze_google"]

YEARS = manager.execution_details["info"]["running_years"]

EXPERIMENT_NAME = manager.execution_details["mlflow_experiment"]
