"""
This script retrieves data from the Censo 2010 and 
Censo 2022 datasets and saves it as parquet files and to database.

The script contains the following functions:
- get_municipalities_2010: Retrieves the municipalities data from the Censo 2010 dataset.
- get_municipalities_2022: Retrieves the municipalities data from the Censo 2022 dataset.
- get_sectors_2010: Retrieves the census sectors data from the Censo 2010 dataset.
- main: The main function that executes the script.

The script also imports the following modules:
- os: Provides a way to interact with the operating system.
- pandas: A powerful data manipulation library.
- src.tools.utils.read: A module that provides functions for reading data.
- src.tools.utils.config: A module that provides functions for retrieving configuration data.
- src.tools.utils.save: A module that provides functions for saving data.
- src.tools.utils.constants: A module that contains constants.

Note: The script assumes the existence of certain contract files and directories.
"""

import os
import pandas as pd
from src.tools.utils.read import Reader
from src.tools.utils.config import get_contract
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.constants import STATES

CONTRACT_MUN_CENSO_2010 = get_contract("censo/contract_mun_censo_2010.yaml", "bronze")
CONTRACT_MUN_CENSO_2022 = get_contract("censo/contract_mun_censo_2022.yaml", "bronze")
CONTRACT_SECTORS_CENSO_2010 = get_contract(
    "censo/contract_sectors_censo_2010.yaml", "bronze"
)
# CONTRACT_SECTORS_CENSO_2022 = get_contract(
#     "censo/contract_sectors_censo_2022.yaml", "bronze"
# )


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_MUN_CENSO_2010)
def get_municipalities_2010(**kwargs):
    """
    Retrieves the municipalities data from the Censo 2010 dataset.

    Returns:
        pandas.DataFrame: A DataFrame containing the municipalities data.
    """
    reader = Reader(CONTRACT_MUN_CENSO_2010)
    dfs = []
    for state in STATES:
        filepath = os.path.join(
            CONTRACT_MUN_CENSO_2010["physicalPath"], "".join([state, ".zip"])
        )
        df = reader.read_geofile(filepath)
        dfs.append(df)
    dfs = pd.concat(dfs)
    return dfs


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_MUN_CENSO_2022)
def get_municipalities_2022(**kwargs):
    """
    Retrieves the municipalities data from the Censo 2022 dataset.

    Returns:
        pandas.DataFrame: A DataFrame containing the municipalities data.
    """
    reader = Reader(CONTRACT_MUN_CENSO_2022)
    dfs = []
    for state in STATES:
        filepath = os.path.join(
            CONTRACT_MUN_CENSO_2022["physicalPath"], "".join([state, ".zip"])
        )
        df = reader.read_geofile(filepath)
        dfs.append(df)
    dfs = pd.concat(dfs)
    return dfs


def get_sectors_2010(**kwargs):
    """
    Retrieves the census sectors data from the Censo 2010 dataset.

    Returns:
        pandas.DataFrame: A DataFrame containing the census sectors data.
    """
    reader = Reader(CONTRACT_SECTORS_CENSO_2010)
    dfs = []
    for state in STATES:
        filepath = os.path.join(
            CONTRACT_SECTORS_CENSO_2010["physicalPath"], "".join([state, ".zip"])
        )
        df = reader.read_geofile(filepath)
        dfs.append(df)
    dfs = pd.concat(dfs)
    return dfs


def main():
    """
    The main function that executes the script.
    """
    kwargs = {"filename": "municipalities"}
    _ = get_municipalities_2010(**kwargs)
    _ = get_municipalities_2022(**kwargs)
