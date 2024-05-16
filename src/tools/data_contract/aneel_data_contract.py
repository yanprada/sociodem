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


def get_aneel_bronze_contracts():
    """
    Retrieves the ANEEL contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    contract_company_id = get_contract(
        "aneel/contract_aneel_companies_id.yaml", "bronze"
    )
    contract_aneel_bronze = get_contract(
        "aneel/contract_aneel_companies.yaml", "bronze"
    )
    contract_aneel_datalake = get_contract(
        "aneel/contract_aneel_companies.yaml", "datalake"
    )
    contracts = {
        "datalake": contract_aneel_datalake,
        "company_id": contract_company_id,
        "ponnot": contract_aneel_bronze[0],
        "ucbt": contract_aneel_bronze[1],
        "ramlig": contract_aneel_bronze[2],
        "conj": contract_aneel_bronze[3],
    }
    return EasyDict(contracts)


def get_aneel_silver_contracts():
    """
    Retrieves the ANEEL silver contracts.

    Returns:
        EasyDict: A dictionary-like object containing the ANEEL silver contracts.
    """
    contract_company_id = get_contract(
        "aneel/contract_aneel_companies_id.yaml", "bronze"
    )
    contract_aneel_silver = get_contract(
        "aneel/contract_aneel_companies.yaml", "silver"
    )
    contracts = {"company_id": contract_company_id, "aneel": contract_aneel_silver}
    return EasyDict(contracts)


def get_aneel_contracts(medallon: str):
    """
    Retrieves the ANEEL contracts for different companies.

    Returns:
        contracts (dict): A dictionary containing the ANEEL contracts for different companies.
            The keys are the names of the companies and the values are the corresponding contracts.
    """
    if medallon == "bronze":
        return get_aneel_bronze_contracts()
    if medallon == "silver":
        return get_aneel_silver_contracts()
    return get_contract("contract_template.yaml", medallon)
