"""
This module contains utility functions for working with configuration files.
"""

import os
from functools import lru_cache
import yaml
from easydict import EasyDict


@lru_cache(maxsize=1)
def get_config(file_name: str = "testes.yaml") -> EasyDict:
    """
    Get the configuration file.

    Args:
        file_name (str): The name of the file to be loaded. Default is "testes.yaml".

    Returns:
        EasyDict: A dictionary-like object that allows attribute access to its keys.
    """
    file_path = os.path.abspath(__file__)
    directory = os.path.dirname(file_path)
    config_file = os.path.join(directory, file_name)
    with open(config_file, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
    return EasyDict(config)
