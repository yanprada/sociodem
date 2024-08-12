"""
This module contains the pipeline for processing buildings data.
"""

from src.databases.silver.mapbiomas.config import EXECUTION_ID, BASE_PARAMS
from src.databases.silver.mapbiomas.steps import a_create_silver_mapbiomas
from src.tools.utils.execution_manager import ExecutionManager
from config.run_mode import DEBUG


def main():
    """
    Main function for executing the silver mapbiomas data pipeline.
    """
    mode = not DEBUG
    BASE_PARAMS["execution_details"] = {
        "steps": [{"run": False, "function": a_create_silver_mapbiomas.main}],
    }
    execution_manager = ExecutionManager(BASE_PARAMS)
    execution_manager.start_execution(EXECUTION_ID, mode)
