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

EXECUTION_ID = "bronze-ibge-2025-05-23-11h54m35s"

BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "ibge",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "raw_data_2010": ["raw_data", "ibge", "ibge_2010"],
        "raw_data_2022": ["raw_data", "ibge", "ibge_2022"],
        "bronze_data_2022": ["bronze", "ibge", "ibge_2022"],
        "bronze_data_2010": ["bronze", "ibge", "ibge_2010"],
    },
    "run_mode": "dev",
    "last_run": None,
    "materialized_views": {},
}

_manager_wrapper = ExecutionManagerWrapper(BASE_PARAMS, EXECUTION_ID, overwrite=True)
manager = _manager_wrapper.manager

CONTRACT_RAW_2010 = manager.execution_details["data_contracts"]["raw_data_2010"]
CONTRACT_RAW_2022 = manager.execution_details["data_contracts"]["raw_data_2022"]
CONTRACTS_RAW = {**CONTRACT_RAW_2010, **CONTRACT_RAW_2022}

CONTRACT_BRONZE_2010 = manager.execution_details["data_contracts"]["bronze_data_2010"]
CONTRACT_BRONZE_2022 = manager.execution_details["data_contracts"]["bronze_data_2022"]
CONTRACTS_BRONZE = {**CONTRACT_BRONZE_2010, **CONTRACT_BRONZE_2022}

EXPERIMENT_NAME = manager.execution_details["mlflow_experiment"]

PATHS_MV = manager.execution_details["materialized_views"]
