"""
This module contains the configuration settings for the bronze database related to the ANEEL data.

Attributes:
    EXECUTION_ID (None): The execution ID.
    PARAMS (dict): A dictionary containing the parameters for the bronze database.
        - "medallon" (str): The medallon value for the bronze database.
        - "data_name" (str): The data name for the ANEEL data.

"""

import os

EXECUTION_ID = None

BASE_PARAMS = {
    "medallon": "bronze",
    "data_name": "aneel",
    "config_path": os.path.abspath(__file__),
}
