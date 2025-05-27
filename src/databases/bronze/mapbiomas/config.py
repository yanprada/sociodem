"""
This module contains the configuration settings for the bronze
    database related to the Mapbiomas data.

Attributes:
    EXECUTION_ID (None): The execution ID.
    PARAMS (dict): A dictionary containing the parameters for the bronze database.
        - "medallon" (str): The medallon value for the bronze database.
        - "data_name" (str): The data name for the Mapbiomas data.

"""

import os

from config.run_mode import DEBUG

from src.tools.managers.execution_manager import ExecutionManagerWrapper


# main_run = "bronze-mapbiomas-2025-03-13-15h51m53s"
EXECUTION_ID = "bronze-mapbiomas-2025-03-17-10h32m24s"
BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "mapbiomas",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "mapbiomas_raw": ["raw_data", "mapbiomas", "brazil_coverage"],
        "mapbiomas_bronze": ["bronze", "mapbiomas", "brazil_coverage"],
    },
    "run_mode": "single_file" if DEBUG else "pipeline",
    "last_run": None,
    "materialized_views": {},
}

_manager_wrapper = ExecutionManagerWrapper(BASE_PARAMS, EXECUTION_ID, DEBUG)
manager = _manager_wrapper.manager

CONTRACTS_BRONZE = manager.execution_details["data_contracts"]["mapbiomas_bronze"]
CONTRACTS_RAW = manager.execution_details["data_contracts"]["mapbiomas_raw"]

YEARS = manager.execution_details["info"]["running_years"]

EXPERIMENT_NAME = manager.execution_details["mlflow_experiment"]
