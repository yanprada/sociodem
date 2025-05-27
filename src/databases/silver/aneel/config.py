"""
This module contains the configuration settings for the bronze database related to the ANEEL data.

Attributes:
    EXECUTION_ID (None): The execution ID.
    PARAMS (dict): A dictionary containing the parameters for the bronze database.
        - "medallon" (str): The medallon value for the bronze database.
        - "data_name" (str): The data name for the ANEEL data.

"""

import os

from config.run_mode import DEBUG

from src.tools.managers.execution_manager import ExecutionManagerWrapper


EXECUTION_ID = "silver-aneel-2025-05-18-15h46m31s"
BASE_PARAMS = {
    "medallon": "silver",
    "data_name": "aneel",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "aneel_bronze": ["bronze", "aneel", "energy"],
        "aneel_silver": ["silver", "aneel", "energy"],
    },
    "run_mode": "single_file" if DEBUG else "pipeline",
    "last_run": None,
    "materialized_views": {},
}

_manager_wrapper = ExecutionManagerWrapper(BASE_PARAMS, EXECUTION_ID, DEBUG)
manager = _manager_wrapper.manager


CONTRACT_BRONZE_ENERGY = manager.execution_details["data_contracts"]["aneel_bronze"]
CONTRACT_SILVER_ENERGY = manager.execution_details["data_contracts"]["aneel_silver"]

YEARS = manager.execution_details["info"]["running_years"]

EXPERIMENT_NAME = manager.execution_details["mlflow_experiment"]

PATHS_MV = manager.execution_details["materialized_views"]
