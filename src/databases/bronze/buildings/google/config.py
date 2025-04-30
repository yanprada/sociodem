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

EXECUTION_ID = "bronze-buildings-2025-03-18-07h19m49s"

BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "buildings",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "raw_google": ["raw_data", "buildings", "google"],
        "raw_state_censo": ["raw_data", "censo", "states_2022"],
        "bronze_google": ["bronze", "buildings", "google"],
    },
    "run_mode": "single_file" if DEBUG else "pipeline",
    "last_run": None,
    "materialized_views": {},
}

_manager_wrapper = ExecutionManagerWrapper(BASE_PARAMS, EXECUTION_ID, DEBUG)
manager = _manager_wrapper.manager


BUILDING_CONTRACTS_RAW = manager.execution_details["data_contracts"]["raw_google"]
STATE_CONTRACTS_RAW = manager.execution_details["data_contracts"]["raw_state_censo"]
BUILDING_CONTRACTS_BRONZE = manager.execution_details["data_contracts"]["bronze_google"]

YEARS = manager.execution_details["info"]["running_years"]

EXPERIMENT_NAME = manager.execution_details["mlflow_experiment"]
