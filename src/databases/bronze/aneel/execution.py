"""
This module contains the execution manager for the bronze ANEEL data pipeline.
"""

from config.run_mode import DEBUG
from src.databases.bronze.aneel.config import EXECUTION_ID, BASE_PARAMS
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.aneel.steps import (
    a_download_aneel_companies,
    b_make_bronze_aneel_dataset,
)


def main():
    """
    Main function for executing the bronze ANEEL data pipeline.
    """
    mode = not DEBUG
    BASE_PARAMS["execution_details"] = {
        "steps": [
            {"run": False, "function": a_download_aneel_companies.main},
            {"run": True, "function": b_make_bronze_aneel_dataset.main},
        ],
    }
    execution_manager = ExecutionManager(BASE_PARAMS)
    execution_manager.start_execution(EXECUTION_ID, mode)
