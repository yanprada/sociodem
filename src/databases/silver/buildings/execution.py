"""
This module contains the pipeline for processing buildings data.
"""

from src.databases.silver.buildings.config import EXECUTION_ID, BASE_PARAMS
from src.tools.managers.execution import ExecutionManager

from src.databases.silver.buildings.steps import a_join_google_omf
from config.run_mode import DEBUG


def main():
    """
    Main function for executing the silver Buildings data pipeline.
    """
    mode = not DEBUG
    BASE_PARAMS["execution_details"] = {
        "steps": [{"run": False, "function": a_join_google_omf.main}],
    }
    execution = ExecutionManager(BASE_PARAMS)
    execution.start_execution(EXECUTION_ID, mode)
