"""
This module provides functions to retrieve ANEEL contracts for different 
companies related to buildings.

The contracts can be retrieved based on the medallon type, which can be 
either "raw_data" or "bronze".

The module includes the following functions:
- get_buildings_raw_data_contracts: Retrieves the buildings contracts for 
    different companies from the raw_data.
- get_buildings_bronze_contracts: Retrieves the buildings contracts for 
    different companies from the bronze level.
- get_buildings_contracts: Retrieves the ANEEL contracts for different 
    companies based on the medallon type.

"""

from easydict import EasyDict
from src.tools.utils.data_contract import get_contract


def get_buildings_raw_data_contracts():
    """
    Retrieves the buildings contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    contract_buildings_google = get_contract(
        "buildings/contract_google_buildings.yaml", "raw_data"
    )
    contract_buildings_omf = get_contract(
        "buildings/contract_omf_buildings.yaml", "raw_data"
    )
    contracts = {
        "buildings_google": contract_buildings_google,
        "buildings_omf": contract_buildings_omf,
    }
    return EasyDict(contracts)


def get_buildings_bronze_contracts():
    """
    Retrieves the buildings contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    contract_buildings_google = get_contract(
        "buildings/contract_google_buildings.yaml", "bronze"
    )
    contract_buildings_omf = get_contract(
        "buildings/contract_omf_buildings.yaml", "bronze"
    )
    contracts = {
        "buildings_google": contract_buildings_google[0],
        "buildings_google_grouped_by_hex": contract_buildings_google[1],
        "buildings_omf": contract_buildings_omf[0],
        "buildings_omf_grouped_by_hex": contract_buildings_omf[1],
    }
    return EasyDict(contracts)


def get_buildings_contracts(medallon: str):
    """
    Retrieves the ANEEL contracts for different companies.

    Args:
        medallon (str): The medallon type.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    if medallon == "raw_data":
        return get_buildings_raw_data_contracts()
    if medallon == "bronze":
        return get_buildings_bronze_contracts()
    return get_contract("contract_template.yaml", medallon)
