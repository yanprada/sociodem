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

from src.tools.managers.execution import ExecutionManagerWrapper

EXECUTION_ID = None

BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "buildings",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "omf_raw_data": ["raw_data", "buildings", "omf"],
        "omf_bronze": ["bronze", "buildings", "omf"],
    },
    "run_mode": "single_file" if DEBUG else "pipeline",
    "last_run": None,
}

_manager_wrapper = ExecutionManagerWrapper(BASE_PARAMS, EXECUTION_ID, DEBUG)
manager = _manager_wrapper.manager


CONTRACT_BRONZE_OMF = manager.execution_details["data_contracts"]["omf_bronze"]
CONTRACT_RAW_OMF = manager.execution_details["data_contracts"]["omf_raw_data"]

YEARS = manager.execution_details["info"]["running_years"]

EXPERIMENT_NAME = manager.execution_details["mlflow_experiment"]
