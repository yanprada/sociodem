"""
This module contains the execution manager for the bronze ANEEL data pipeline.
"""

from src.databases.bronze.aneel.config import EXECUTION_ID, BASE_PARAMS
from src.tools.utils.execution_manager import ExecutionManager
from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from src.databases.bronze.aneel.steps import (
    a_download_aneel_companies,
    b_make_bronze_aneel_dataset,
)


def main():
    """
    Main function for executing the bronze ANEEL data pipeline.
    """
    BASE_PARAMS["execution_details"] = {
        "steps": [
            {"run": False, "function": a_download_aneel_companies.main},
            {"run": True, "function": b_make_bronze_aneel_dataset.main},
        ],
        "data_contracts": get_aneel_contracts("bronze"),
    }
    execution_manager = ExecutionManager(BASE_PARAMS)
    execution_manager.start_execution(EXECUTION_ID)


if __name__ == "__main__":
    main()
