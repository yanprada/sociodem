"""
This module contains the configuration settings for the bronze
    database related to the AlphaEarth embeddings data.

Attributes:
    EXECUTION_ID (None): The execution ID.
    PARAMS (dict): A dictionary containing the parameters for the bronze database.
        - "medallon" (str): The medallon value for the bronze database.
        - "data_name" (str): The data name for the alphaearth data.

"""

import os


from src.tools.managers.execution import ExecutionManagerWrapper


EXECUTION_ID = "bronze-alphaearth-2025-12-18-11h20m51s"
BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "alphaearth",
    "config_path": os.path.abspath(__file__),
    "data_contracts": {
        "hex_per_mun": ["silver", "ibge", "ibge_2022"],
        "layers_bronze": ["bronze", "ibge", "ibge_2022"],
        "alphaearth_raw": ["raw_data", "alphaearth", "brazil_embeddings"],
        "alphaearth_bronze": ["bronze", "alphaearth", "brazil_embeddings"],
    },
    "run_mode": "dev",
    "last_run": None,
    "materialized_views": {},
}

_manager_wrapper = ExecutionManagerWrapper(BASE_PARAMS, EXECUTION_ID, overwrite=True)
manager = _manager_wrapper.manager

CONTRACTS_BRONZE_GEE = manager.execution_details["data_contracts"]["alphaearth_bronze"]
COTRACTS_BRONZE_LAYERS = manager.execution_details["data_contracts"]["layers_bronze"]
CONTRACTS_BRONZE = {**CONTRACTS_BRONZE_GEE, **COTRACTS_BRONZE_LAYERS}

CONTRACTS_SILVER = manager.execution_details["data_contracts"]["hex_per_mun"]
CONTRACTS_RAW = manager.execution_details["data_contracts"]["alphaearth_raw"]

YEARS = manager.execution_details["info"]["running_years"]

EXPERIMENT_NAME = manager.execution_details["mlflow_experiment"]

GEE_SERVICE_ACCOUNT = "utopian-trilogy-305721@appspot.gserviceaccount.com"
GEE_KEY_FILE = ".private-key.json"
