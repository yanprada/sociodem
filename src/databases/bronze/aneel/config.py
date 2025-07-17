"""
This module contains the configuration settings for the bronze database related to the ANEEL data.

Attributes:
    EXECUTION_ID (None): The execution ID.
    PARAMS (dict): A dictionary containing the parameters for the bronze database.
        - "medallon" (str): The medallon value for the bronze database.
        - "data_name" (str): The data name for the ANEEL data.

"""

import os


from src.tools.managers.execution import ExecutionManagerWrapper

# main run = "bronze-aneel-2025-03-13-15h51m53s"

EXECUTION_ID = "bronze-aneel-2025-06-30-12h15m55s"


BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "aneel",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "aneel_bronze": ["bronze", "aneel", "energy"],
        "aneel_raw": ["raw_data", "aneel", "energy"],
        "aneel_company_ids": ["raw_data", "aneel", "company_ids"],
    },
    "run_mode": "dev",
    "last_run": None,
    "materialized_views": {
        "common": "{path}_companies_already_processed_v2",
        "step_c": "{path}_sum_energy_per_companies_v2",
        "step_e": "{path}_test_join_v2",
    },
}

_manager_wrapper = ExecutionManagerWrapper(BASE_PARAMS, EXECUTION_ID, overwrite=True)
manager = _manager_wrapper.manager


CONTRACT_BRONZE_ENERGY = manager.execution_details["data_contracts"]["aneel_bronze"]
CONTRACT_RAW_ENERGY = manager.execution_details["data_contracts"]["aneel_raw"]
CONTRACT_RAW_IDS = manager.execution_details["data_contracts"]["aneel_company_ids"]

YEARS = manager.execution_details["info"]["running_years"]

EXPERIMENT_NAME = manager.execution_details["mlflow_experiment"]

PATHS_MV = manager.execution_details["materialized_views"]
