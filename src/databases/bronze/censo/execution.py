"""
This module contains the pipeline for processing buildings data.
"""

from src.databases.bronze.censo.config import EXECUTION_ID, BASE_PARAMS
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.censo.steps import (
    a_download_layers,
    b_make_bronze_dataset_censo,
)
from config.run_mode import DEBUG


def main():
    """
    Main function for executing the bronze Buildings data pipeline.
    """
    mode = not DEBUG
    BASE_PARAMS["execution_details"] = {
        "steps": [
            {"run": False, "function": a_download_layers.main},
            {"run": True, "function": b_make_bronze_dataset_censo.main},
        ],
    }
    execution_manager = ExecutionManager(BASE_PARAMS)
    execution_manager.start_execution(EXECUTION_ID, mode)
