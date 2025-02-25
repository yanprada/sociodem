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
from src.tools.utils.execution_manager import ExecutionManager

EXECUTION_ID = "bronze-buildings-KgcKlXbQ5P85l7X"

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
}

MANAGER = ExecutionManager(BASE_PARAMS)
MANAGER.initialize_execution(EXECUTION_ID, DEBUG)

BUILDING_CONTRACTS_RAW = MANAGER.execution_details["data_contracts"]["raw_google"]
STATE_CONTRACTS_RAW = MANAGER.execution_details["data_contracts"]["raw_state_censo"]
BUILDING_CONTRACTS_BRONZE = MANAGER.execution_details["data_contracts"]["bronze_google"]

EXPERIMENT_ID = MANAGER.execution_details["mlflow_experiment"]
