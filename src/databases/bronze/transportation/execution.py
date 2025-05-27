"""
This module contains the pipeline for processing buildings data.
"""

from src.databases.bronze.pois.config import EXECUTION_ID, BASE_PARAMS
from src.tools.managers.execution import ExecutionManager
from src.databases.bronze.transportation.steps import (
    a_download_transportation,
    b_process_transportation_bbox,
    c_process_transportation_brazil,
)
from config.run_mode import DEBUG


def main():
    """
    Main function for executing the bronze Buildings data pipeline.
    """
    mode = not DEBUG
    BASE_PARAMS["execution_details"] = {
        "steps": [
            {"run": False, "function": a_download_transportation.main},
            {"run": True, "function": b_process_transportation_bbox.main},
            {"run": True, "function": c_process_transportation_brazil.main},
        ],
    }
    execution = ExecutionManager(BASE_PARAMS)
    execution.start_execution(EXECUTION_ID, mode)
