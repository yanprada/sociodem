"""
This module contains utility functions for working with configuration files.
"""

import os
from functools import lru_cache
from pathlib import Path
import yaml
from easydict import EasyDict


@lru_cache(maxsize=1)
def get_path_contracts() -> str:
    """
    Returns the absolute path to the 'data_contract' directory.

    This function retrieves the absolute path of the current file,
    then navigates to the parent directory and then to the 'databases/data_contract' directory.
    The resulting absolute path is returned.

    Returns:
        str: The absolute path to the 'data_contract' directory.
    """
    file_path = os.path.abspath(__file__)
    directory = os.path.dirname(file_path)
    path_src = Path(directory).parent.parent.absolute()
    contract_path = os.path.join(path_src, "databases/data_contract/")
    return contract_path


@lru_cache(maxsize=1)
def get_contract(
    contract: str = "contract_template.yaml", medallon: str = "bronze"
) -> EasyDict:
    """
    Get the configuration file for contract databases.

    Args:
        contract (str): The name of the contract file to be loaded.
                        Default is "contract_template.yaml".
        medallon (str): The type of medallon. Default is "bronze".

    Returns:
        EasyDict: A dictionary-like object that allows attribute access to its keys.

    Contract return example:
        {
            'tableName': None,
            'path': None,
            'description': None,
            'queryYear': None,
            'columns':
            [
                {
                    'column': None,
                    'isPrimary': None,
                    'businessName': None,
                    'description': None,
                    'logicalType': None,
                    'physicalType': None,
                    'isNullable': None,
                    'sampleValues': [None, None]
                },
            ]
        }
    """
    contract_path = get_path_contracts()
    contract_path = os.path.join(contract_path, contract)
    with open(contract_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    return EasyDict(config)[medallon]


def post_contract(contract: dict):
    """
    Overrides the contract column datatypes and saves the updated data to a YAML file.

    Args:
        contract (dict): The dictionary containing the contract column datatypes.
    """
    contract_path = get_path_contracts()
    contract_path = os.path.join(contract_path, contract["contractName"])
    with open(contract_path, "w", encoding="utf-8") as file:
        yaml.dump(contract, file)
