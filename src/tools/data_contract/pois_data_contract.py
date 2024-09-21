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


def get_pois_bronze_contracts():
    """
    Retrieves the pois contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    contract_pois = get_contract("pois/contract_pois.yaml", "bronze")
    contract_pois_dl = get_contract("pois/contract_pois.yaml", "raw_data")
    contracts = {
        "pois": contract_pois,
        "raw_data": contract_pois_dl[0],
        "categories": contract_pois_dl[1],
    }
    return EasyDict(contracts)


def get_pois_silver_contracts():
    """
    Retrieves the pois contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    contract_pois = get_contract("pois/contract_pois.yaml", "silver")
    contracts = {"pois_hex": contract_pois}
    return EasyDict(contracts)


def get_pois_contracts(medallon: str):
    """
    Retrieves the ANEEL contracts for different companies.

    Args:
        medallon (str): The medallon type.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    if medallon == "bronze":
        return get_pois_bronze_contracts()
    if medallon == "silver":
        return get_pois_silver_contracts()
    return get_contract("contract_template.yaml", medallon)
