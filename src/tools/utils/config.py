"""
This module contains utility functions for working with configuration files.
"""

import os
from functools import lru_cache
from pathlib import Path
import yaml
from easydict import EasyDict


@lru_cache(maxsize=1)
def get_contract(contract: str = "contract_template.yaml") -> EasyDict:
    """
    Get the configuration file for contract databases.

    Args:
        contract (str): The name of the contract file to be loaded.
                        Default is "contract_template.yaml".
    Returns:
        EasyDict: A dictionary-like object that allows attribute access to its keys.
        ------------------------------------------------
        Contract return example:
            {  'bronze':
                [
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
                                'logicalTye': None,
                                'physicalType': None,
                                'isNullable': None,
                                'sampleValues': [None, None]
                            },
                        ]
                    }
                ]
            }
    """
    file_path = os.path.abspath(__file__)
    directory = os.path.dirname(file_path)
    path_src = Path(directory).parent.parent.absolute()
    config_file = os.path.join(path_src, "databases/data_contract/", contract)
    with open(config_file, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    return EasyDict(config)
