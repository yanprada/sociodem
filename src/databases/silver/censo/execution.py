"""
This module contains the pipeline for processing buildings data.
"""

from src.databases.silver.censo.config import EXECUTION_ID, BASE_PARAMS
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.silver.censo.steps import (
    a_dompp_add_hex_sc,
    b_dompp_pct_sc_by_hex,
    c_sc_add_hex,
)
from config.run_mode import DEBUG


def main():
    """
    Main function for executing the silver Buildings data pipeline.
    """
    mode = not DEBUG
    BASE_PARAMS["execution_details"] = {
        "steps": [
            {"run": False, "function": a_dompp_add_hex_sc.main},
            {"run": True, "function": b_dompp_pct_sc_by_hex.main},
            {"run": True, "function": c_sc_add_hex.main},
        ],
    }
    execution_manager = ExecutionManager(BASE_PARAMS)
    execution_manager.start_execution(EXECUTION_ID, mode)
