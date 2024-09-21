"""
This module provides functions to retrieve ANEEL contracts for different companies.

Functions:
    - get_aneel_bronze_contracts(): Retrieves the ANEEL contracts for different companies
      with bronze medallons.
    - get_aneel_contracts(medallon: str): Retrieves the ANEEL contracts for different companies
      based on the given medallon.

"""

from easydict import EasyDict
from src.tools.utils.data_contract import get_contract


def get_transportation_bronze_contracts():
    """
    Retrieves the transportation contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    contract_transportation = get_contract(
        "transportation/contract_transportation.yaml", "bronze"
    )
    contract_transportation_dl = get_contract(
        "transportation/contract_transportation.yaml", "raw_data"
    )
    contracts = {
        "transportation": contract_transportation,
        "raw_data": contract_transportation_dl,
    }
    return EasyDict(contracts)


def get_transportation_silver_contracts():
    """
    Retrieves the transportation contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """


def get_transportation_contracts(medallon: str):
    """
    Retrieves the ANEEL contracts for different companies.

    Args:
        medallon (str): The medallon type.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    if medallon == "bronze":
        return get_transportation_bronze_contracts()
    if medallon == "silver":
        return get_transportation_silver_contracts()
    return get_contract("contract_template.yaml", medallon)
