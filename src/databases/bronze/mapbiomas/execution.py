"""
This module contains the pipeline for processing buildings data.
"""

from src.databases.bronze.mapbiomas.config import EXECUTION_ID, BASE_PARAMS
from src.databases.bronze.mapbiomas.steps import (
    a_download_mapbiomas,
    b_add_hexagon_to_mapbiomas,
    c_create_grouped_by_hex_mapbiomas,
)
from src.tools.utils.execution_manager import ExecutionManager
from config.run_mode import DEBUG


def main():
    """
    Main function for executing the bronze mapbiomas data pipeline.
    """
    mode = not DEBUG
    BASE_PARAMS["execution_details"] = {
        "steps": [
            {"run": False, "function": a_download_mapbiomas.main},
            {"run": True, "function": b_add_hexagon_to_mapbiomas.main},
            {"run": True, "function": c_create_grouped_by_hex_mapbiomas.main},
        ],
    }
    execution_manager = ExecutionManager(BASE_PARAMS)
    execution_manager.start_execution(EXECUTION_ID, mode)
