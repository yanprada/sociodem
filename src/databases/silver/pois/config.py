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


EXECUTION_ID = "silver-pois-2025-06-17-16h47m19s"

BASE_PARAMS = {
    "medallon": "silver",
    "data_name": "pois",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "pois_bronze": ["bronze", "pois", "places"],
        "pois_silver": ["silver", "pois", "places"],
        "silver_data_2022": ["silver", "ibge", "ibge_2022"],
    },
    "run_mode": "dev",
    "last_run": None,
    "materialized_views": {},
}


_manager_wrapper = ExecutionManagerWrapper(BASE_PARAMS, EXECUTION_ID, overwrite=True)
manager = _manager_wrapper.manager

CONTRACTS_BRONZE = manager.execution_details["data_contracts"]["pois_bronze"]
CONTRACTS_SILVER = manager.execution_details["data_contracts"]["pois_silver"]
CONTRACTS_CENSO_SILVER = manager.execution_details["data_contracts"]["silver_data_2022"]
YEARS = manager.execution_details["info"]["running_years"]

EXPERIMENT_NAME = manager.execution_details["mlflow_experiment"]
