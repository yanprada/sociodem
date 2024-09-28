"""
This module contains the pipeline for processing buildings data.
"""

from src.databases.bronze.buildings.google.config import EXECUTION_ID, BASE_PARAMS
from src.tools.utils.execution_manager import ExecutionManager

from src.databases.bronze.buildings.google.steps import (
    a_download_data,
    b_process_image_to_hex,
)
from config.run_mode import DEBUG


def main():
    """
    Main function for executing the bronze Buildings data pipeline.
    """
    mode = not DEBUG
    BASE_PARAMS["execution_details"] = {
        "steps": [
            {"run": False, "function": a_download_data.main},
            {"run": True, "function": b_process_image_to_hex.main},
        ],
    }
    execution_manager = ExecutionManager(BASE_PARAMS)
    execution_manager.start_execution(EXECUTION_ID, mode)
