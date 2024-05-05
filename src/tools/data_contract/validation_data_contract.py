"""
This module provides functions for retrieving ANEEL contracts for different companies.

The main function, get_validation_contracts, retrieves the ANEEL contracts 
based on the given parameters.

Example usage:
    contracts = get_validation_contracts("bronze", 1)

"""

from src.tools.utils.data_contract import get_contract


def get_validation_contracts(medallon: str, contract_id: int):
    """
    Retrieves the ANEEL contracts for different companies.

    Args:
        medallon (str): The type of medallon (e.g., "bronze", "silver", "gold").
        contract_id (int): The ID of the contract.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    if medallon == "bronze":
        return get_contract("validation/contract_validation.yaml", "bronze")[
            contract_id
        ]
    return get_contract("contract_template.yaml", medallon)
