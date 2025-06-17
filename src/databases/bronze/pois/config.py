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


EXECUTION_ID = "bronze-pois-2025-06-16-11h34m45s"

BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "pois",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "pois_raw": ["raw_data", "pois", "places"],
        "pois_bronze": ["bronze", "pois", "places"],
    },
    "run_mode": "dev",
    "last_run": None,
    "materialized_views": {},
}


_manager_wrapper = ExecutionManagerWrapper(BASE_PARAMS, EXECUTION_ID, overwrite=True)
manager = _manager_wrapper.manager

CONTRACTS_BRONZE = manager.execution_details["data_contracts"]["pois_bronze"]
CONTRACTS_RAW = manager.execution_details["data_contracts"]["pois_raw"]

YEARS = manager.execution_details["info"]["running_years"]

EXPERIMENT_NAME = manager.execution_details["mlflow_experiment"]
