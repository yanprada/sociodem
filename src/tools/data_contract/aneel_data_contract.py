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
    contract_company_id_raw_data = get_contract(
        "aneel/contract_aneel_companies_id.yaml", "raw_data"
    )
    contract_aneel_bronze = get_contract(
        "aneel/contract_aneel_companies.yaml", "bronze"
    )
    contract_aneel_raw_data = get_contract(
        "aneel/contract_aneel_companies.yaml", "raw_data"
    )
    contracts = {
        "raw_data": contract_aneel_raw_data,
        "company_id_raw_data": contract_company_id_raw_data,
        "company_id": contract_company_id,
        "ponnot": contract_aneel_bronze[0],
        "ucbt": contract_aneel_bronze[1],
        "ucbt_agg": contract_aneel_bronze[2],
        "ucbt_no_join": contract_aneel_bronze[3],
        "ramlig": contract_aneel_bronze[4],
        "conj": contract_aneel_bronze[5],
        "aneel_join": contract_aneel_bronze[6],
        "ucbt_clean": contract_aneel_bronze[7],
        "ponnot_clean": contract_aneel_bronze[8],
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
    aneel_contracts = get_contract("aneel/contract_aneel_companies.yaml", "silver")
    contract_aneel_neighbors_list = []
    for i in range(1, len(aneel_contracts) - 4):
        contract_aneel_neighbors_list.append(aneel_contracts[i])
    contracts = {
        f"neighbors_lvl{i}": contract_aneel_neighbors_list[i]
        for i in range(len(contract_aneel_neighbors_list))
    }
    contracts["company_id"] = contract_company_id
    contracts["aneel"] = aneel_contracts[0]
    contracts["temp_join"] = aneel_contracts[-4]
    contracts["final_aneel"] = aneel_contracts[-3]
    contracts["aneel_hexagon"] = aneel_contracts[-2]
    contracts["aneel_hexagon_grouped"] = aneel_contracts[-1]
    return EasyDict(contracts)


def get_aneel_gold_contracts():
    """
    Retrieves the gold contracts from the ANEEL data contract.

    Returns:
        An EasyDict object containing the gold contracts from the ANEEL data contract.
    """
    contract_aneel = get_contract("aneel/contract_aneel_companies.yaml", "gold")
    return EasyDict({"aneel": contract_aneel})


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
    return get_aneel_gold_contracts()
