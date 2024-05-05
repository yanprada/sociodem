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
        "aneel/contract_aneel_companies_id.yaml", "silver"
    )
    contract_ponnot = get_contract(
        "aneel/contract_aneel_companies_ponnot.yaml", "bronze"
    )
    contract_ucbt = get_contract("aneel/contract_aneel_companies_ucbt.yaml", "bronze")
    contract_ramlig = get_contract(
        "aneel/contract_aneel_companies_ramlig.yaml", "bronze"
    )
    contract_aneel_datalake = get_contract(
        "aneel/contract_aneel_datalake.yaml", "datalake"
    )
    contracts = {
        "datalake": contract_aneel_datalake,
        "company_id": contract_company_id,
        "ponnot": contract_ponnot,
        "ucbt": contract_ucbt,
        "ramlig": contract_ramlig,
    }
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
    return get_contract("contract_template.yaml", medallon)
