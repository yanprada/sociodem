"""
This module contains the pipeline for processing buildings data.
"""

from src.databases.bronze.buildings.omf.config import EXECUTION_ID, BASE_PARAMS
from src.tools.utils.execution_manager import ExecutionManager

from src.databases.bronze.buildings.omf.steps import (
    a_download_buildings,
    b_add_h3_and_sc_index,
    c_group_buildings_by_hex_sc,
)
from config.run_mode import DEBUG


def main():
    """
    Main function for executing the bronze Buildings data pipeline.
    """
    mode = not DEBUG
    BASE_PARAMS["execution_details"] = {
        "steps": [
            {"run": False, "function": a_download_buildings.main},
            {"run": True, "function": b_add_h3_and_sc_index.main},
            {"run": True, "function": c_group_buildings_by_hex_sc.main},
        ],
    }
    execution_manager = ExecutionManager(BASE_PARAMS)
    execution_manager.start_execution(EXECUTION_ID, mode)
