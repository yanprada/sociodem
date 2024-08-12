"""
This module contains the execution manager for the bronze ANEEL data pipeline.
"""

from config.run_mode import DEBUG
from src.databases.silver.aneel.config import EXECUTION_ID, BASE_PARAMS
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.silver.aneel.steps import (
    a_join_ucbt_and_ponnot,
    b_fix_after_join_ponnot_without_match,
    c_process_aneel_after_fixes,
    d_process_hex_ids_aneel,
    e_group_by_hex,
)


def main():
    """
    Main function for executing the silver ANEEL data pipeline.
    """
    mode = not DEBUG
    BASE_PARAMS["execution_details"] = {
        "steps": [
            {"run": False, "function": a_join_ucbt_and_ponnot.main},
            {"run": True, "function": b_fix_after_join_ponnot_without_match.main},
            {"run": True, "function": c_process_aneel_after_fixes.main},
            {"run": True, "function": d_process_hex_ids_aneel.main},
            {"run": True, "function": e_group_by_hex.main},
        ],
    }
    execution_manager = ExecutionManager(BASE_PARAMS)
    execution_manager.start_execution(EXECUTION_ID, mode)
