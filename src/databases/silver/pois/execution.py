"""
This module contains the pipeline for processing buildings data.
"""

from src.databases.silver.pois.config import EXECUTION_ID, BASE_PARAMS
from src.tools.managers.execution import ExecutionManager
from src.databases.silver.pois.steps import a_create_places_hex
from config.run_mode import DEBUG


def main():
    """
    Main function for executing the silver Pois data pipeline.
    """
    mode = not DEBUG
    BASE_PARAMS["execution_details"] = {
        "steps": [
            {"run": False, "function": a_create_places_hex.main},
        ],
    }
    execution = ExecutionManager(BASE_PARAMS)
    execution.start_execution(EXECUTION_ID, mode)
