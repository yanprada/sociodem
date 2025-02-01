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


def get_mapbiomas_raw_contracts():
    """
    Retrieves the MAPBIOMAS contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    raw_data = get_contract("mapbiomas/contract_mapbiomas.yaml", "raw_data")
    contracts = {
        "raw_data": raw_data,
    }
    return EasyDict(contracts)


def get_mapbiomas_bronze_contracts():
    """
    Retrieves the MAPBIOMAS contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    contract_mapbiomas = get_contract("mapbiomas/contract_mapbiomas.yaml", "bronze")
    contracts = {
        "mapbiomas": contract_mapbiomas[0],
        "grouped_by_hex_mapbiomas": contract_mapbiomas[1],
    }
    return EasyDict(contracts)


def get_mapbiomas_silver_contracts():
    """
    Retrieves the CENSO contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    contract_mapbiomas = get_contract("mapbiomas/contract_mapbiomas.yaml", "silver")
    raw_data = get_contract("mapbiomas/contract_mapbiomas.yaml", "raw_data")
    contracts = {
        "mapbiomas": contract_mapbiomas,
        "raw_data": raw_data,
    }
    return EasyDict(contracts)


def get_mapbiomas_contracts(medallon: str):
    """
    Retrieves the ANEEL contracts for different companies.

    Args:
        medallon (str): The medallon type.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    if medallon == "raw":
        return get_mapbiomas_raw_contracts()
    if medallon == "bronze":
        return get_mapbiomas_bronze_contracts()
    if medallon == "silver":
        return get_mapbiomas_silver_contracts()
    return get_contract("contract_template.yaml", medallon)
