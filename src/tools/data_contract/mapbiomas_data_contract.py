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


def get_mapbiomas_bronze_contracts():
    """
    Retrieves the MAPBIOMAS contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    contract_mapbiomas_2022 = get_contract(
        "mapbiomas/contract_mapbiomas_2022.yaml", "bronze"
    )
    datalake = get_contract("mapbiomas/contract_mapbiomas_2022.yaml", "datalake")
    contracts = {
        "mapbiomas_2022": contract_mapbiomas_2022[0],
        "grouped_by_hex_mapbiomas_2022": contract_mapbiomas_2022[1],
        "unique_hex_ids": contract_mapbiomas_2022[2],
        "datalake": datalake,
    }
    return EasyDict(contracts)


def get_mapbiomas_silver_contracts():
    """
    Retrieves the CENSO contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    contract_mun_2022 = get_contract("mapbiomas/contract_mapbiomas_2022.yaml", "silver")
    datalake = get_contract("mapbiomas/contract_mapbiomas_2022.yaml", "datalake")
    contracts = {
        "mapbiomas_2022": contract_mun_2022,
        "datalake": datalake,
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
    if medallon == "bronze":
        return get_mapbiomas_bronze_contracts()
    if medallon == "silver":
        return get_mapbiomas_silver_contracts()
    return get_contract("contract_template.yaml", medallon)
