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

from src.tools.utils.execution_manager import ExecutionManager

EXECUTION_ID = "bronze-aneel-qStOVo6wDOk3obC"

BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "aneel",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "aneel_bronze": ["bronze", "aneel", "energy"],
        "aneel_raw": ["raw_data", "aneel", "energy"],
        "aneel_company_ids": ["raw_data", "aneel", "company_ids"],
    },
    "run_mode": "single_file" if DEBUG else "pipeline",
    "last_run": None,
}

MANAGER = ExecutionManager(BASE_PARAMS)
MANAGER.initialize_execution(EXECUTION_ID, DEBUG)

CONTRACT_BRONZE_ENERGY = MANAGER.execution_details["data_contracts"]["aneel_bronze"]
CONTRACT_RAW_ENERGY = MANAGER.execution_details["data_contracts"]["aneel_raw"]
CONTRACT_RAW_IDS = MANAGER.execution_details["data_contracts"]["aneel_company_ids"]

EXPERIMENT_NAME = MANAGER.execution_details["mlflow_experiment"]
