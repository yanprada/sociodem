"""
This module contains the pipeline for processing buildings data.
"""

from src.databases.bronze.pois.config import EXECUTION_ID, BASE_PARAMS
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.pois.steps import (
    a_download_places,
    b_process_places_bbox,
    c_process_places_brazil,
)
from config.run_mode import DEBUG


def main():
    """
    Main function for executing the bronze Buildings data pipeline.
    """
    mode = not DEBUG
    BASE_PARAMS["execution_details"] = {
        "steps": [
            {"run": False, "function": a_download_places.main},
            {"run": True, "function": b_process_places_bbox.main},
            {"run": True, "function": c_process_places_brazil.main},
        ],
    }
    execution_manager = ExecutionManager(BASE_PARAMS)
    execution_manager.start_execution(EXECUTION_ID, mode)
