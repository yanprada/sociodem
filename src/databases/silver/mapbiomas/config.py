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


from src.tools.managers.execution import ExecutionManagerWrapper

EXECUTION_ID = "silver-mapbiomas-2025-05-28-15h39m46s"
BASE_PARAMS = {
    "medallon": "silver",
    "data_name": "mapbiomas",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "mapbiomas_bronze": ["bronze", "mapbiomas", "brazil_coverage"],
        "mapbiomas_silver": ["silver", "mapbiomas", "brazil_coverage"],
    },
    "run_mode": "dev",
    "last_run": None,
    "materialized_views": {},
}


_manager_wrapper = ExecutionManagerWrapper(BASE_PARAMS, EXECUTION_ID, overwrite=True)
manager = _manager_wrapper.manager

CONTRACTS_BRONZE = manager.execution_details["data_contracts"]["mapbiomas_bronze"]
CONTRACTS_SILVER = manager.execution_details["data_contracts"]["mapbiomas_silver"]

YEARS = manager.execution_details["info"]["running_years"]

EXPERIMENT_NAME = manager.execution_details["mlflow_experiment"]

PATHS_MV = manager.execution_details["materialized_views"]
