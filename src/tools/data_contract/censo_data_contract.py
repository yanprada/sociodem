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


def get_censo_bronze_contracts():
    """
    Retrieves the CENSO contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    contract_mun_2010 = get_contract("censo/contract_mun_censo_2010.yaml", "bronze")
    contract_mun_2022 = get_contract("censo/contract_mun_censo_2022.yaml", "bronze")

    contract_sector_2010 = get_contract(
        "censo/contract_sectors_censo_2010.yaml", "bronze"
    )
    contract_sector_2022 = get_contract(
        "censo/contract_sectors_censo_2022.yaml", "bronze"
    )
    contract_dompp_2022 = get_contract("censo/contract_dompp_censo_2022.yaml", "bronze")
    contract_layer_2010 = get_contract("censo/contract_mun_censo_2010.yaml", "raw_data")
    contract_layer_2022 = get_contract("censo/contract_mun_censo_2022.yaml", "raw_data")
    contracts = {
        "municipalities_2010": contract_mun_2010,
        "mun_2022": contract_mun_2022,
        "sectors_2010": contract_sector_2010,
        "sectors_2022": contract_sector_2022,
        "dompp_2022": contract_dompp_2022,
        "raw_data_2010": contract_layer_2010,
        "raw_data_2022": contract_layer_2022,
    }
    return EasyDict(contracts)


def get_censo_silver_contracts():
    """
    Retrieves the CENSO contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    contract_dompp_2022 = get_contract("censo/contract_dompp_censo_2022.yaml", "silver")
    contract_mun_2022 = get_contract("censo/contract_mun_censo_2022.yaml", "silver")
    contract_sector_2022 = get_contract(
        "censo/contract_sectors_censo_2022.yaml", "silver"
    )
    contracts = {
        "dompp_2022": contract_dompp_2022[0],
        "dompp_pct_2022": contract_dompp_2022[1],
        "mun_2022": contract_mun_2022,
        "sectors_2022": contract_sector_2022,
    }
    return EasyDict(contracts)


def get_censo_contracts(medallon: str):
    """
    Retrieves the ANEEL contracts for different companies.

    Args:
        medallon (str): The medallon type.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    if medallon == "bronze":
        return get_censo_bronze_contracts()
    if medallon == "silver":
        return get_censo_silver_contracts()
    return get_contract("contract_template.yaml", medallon)
