"""
This script retrieves data from the Censo 2010 and 
Censo 2022 datasets and saves it as parquet files and to database.

The script contains the following functions:
- get_municipalities_2010: Retrieves the municipalities data from the Censo 2010 dataset.
- get_municipalities_2022: Retrieves the municipalities data from the Censo 2022 dataset.
- get_sectors_2010: Retrieves the census sectors data from the Censo 2010 dataset.
- main: The main function that executes the script.

Note: The script assumes the existence of certain contract files and directories.
"""

import os
import zipfile
import pandas as pd
from tqdm import tqdm
from src.tools.utils.read import Reader
from src.tools.utils.data_contracts import get_contract
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.constants import STATES
from src.tools.utils.common import write_log, check_data_consistency

CONTRACT_MUN_CENSO_2010 = get_contract("censo/contract_mun_censo_2010.yaml", "bronze")

CONTRACT_SECTORS_CENSO_2010 = get_contract(
    "censo/contract_sectors_censo_2010.yaml", "bronze"
)
CONTRACT_SECTORS_CENSO_2022 = get_contract(
    "censo/contract_sectors_censo_2022.yaml", "bronze"
)
CONTRACT_DOMPP_CENSO_2022 = get_contract(
    "censo/contract_dompp_censo_2022.yaml", "bronze"
)
CONTRACT_PARTITIONS = get_contract("contract_partitions.yaml")
CONTRACTS = {
    "municipalities_2010": CONTRACT_MUN_CENSO_2010,
    "sectors_2010": CONTRACT_SECTORS_CENSO_2010,
    "sectors_2022": CONTRACT_SECTORS_CENSO_2022,
    "dompp_2022": CONTRACT_DOMPP_CENSO_2022,
}


def get_censo_data(contract_key):
    """
    Retrieves the data from the Censo dataset.

    Args:
        contract_key (str): The key of the contract to use.

    Returns:
        pandas.DataFrame: A DataFrame containing the data.
    """
    reader = Reader(CONTRACTS[contract_key])
    dfs = []
    for state in tqdm(STATES):
        filepath = os.path.join(
            CONTRACTS[contract_key]["physicalPath"]
            .replace("databases", "datalake")
            .replace("bronze/", ""),
            "".join([state, ".zip"]),
        )
        df = reader.read_geofile(filepath)
        dfs.append(df)
    dfs = pd.concat(dfs)
    return dfs


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_MUN_CENSO_2010)
def get_municipalities_2010(**kwargs):
    """
    Retrieves the municipalities data from the Censo 2010 dataset.

    Returns:
        pandas.DataFrame: A DataFrame containing the municipalities data.
    """
    dfs = get_censo_data("municipalities_2010")
    return dfs


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_SECTORS_CENSO_2010)
def get_sectors_2010(**kwargs):
    """
    Retrieves the census sectors data from the Censo 2010 dataset.

    Returns:
        pandas.DataFrame: A DataFrame containing the census sectors data.
    """
    dfs = get_censo_data("sectors_2010")
    return dfs


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_SECTORS_CENSO_2022)
def get_sectors_2022(**kwargs):
    """
    Retrieves the census sectors data from the Censo 2022 dataset.

    Returns:
        pandas.DataFrame: A DataFrame containing the census sectors data.
    """
    dfs = get_censo_data("sectors_2022")
    return dfs


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_DOMPP_CENSO_2022)
def get_dompp_per_state_2022(state, **kwargs):
    """
    Retrieves the DOMPP data for a specific state in 2022.

    Args:
        state (str): The abbreviation of the state for which the data is requested.
        **kwargs: Additional keyword arguments.

    Returns:
        pandas.DataFrame: The DOMPP data for the specified state in 2022.

    Raises:
        FileNotFoundError: If the file for the specified state is not found.

    """
    reader = Reader(CONTRACT_DOMPP_CENSO_2022)
    filepath = os.path.join(
        CONTRACT_DOMPP_CENSO_2022["physicalPath"]
        .replace("databases", "datalake")
        .replace("bronze/", ""),
        "".join([state, ".zip"]),
    )
    with zipfile.ZipFile(filepath, "r") as zip_ref:
        csv_filename = zip_ref.namelist()[0]
        with zip_ref.open(csv_filename) as csv_file:
            df = reader.read_csv(csv_file, sep=";")
            df = (
                df.groupby(df.columns.tolist(), as_index=False)
                .size()
                .rename(columns={"size": "count"})
                .astype({"cod_uf": "category", "cod_mun": "category"})
            )
            return df


def get_dompp_2022():
    """
    Retrieves the DOMPP data from the Censo 2022 dataset.
    """
    for state in tqdm(STATES):
        kwargs = {"filename": state}
        _ = get_dompp_per_state_2022(state, **kwargs)


def upload_municipalities_2010():
    """
    Uploads municipalities data for the year 2010.

    This function processes the municipalities data and checks if the data already exists.
    If the data does not exist, it calls the `get_municipalities_2010` function to retrieve it.
    """
    write_log("Processing municipalities data...")
    kwargs = {"filename": "municipalities"}
    if os.path.exists(
        os.path.join(CONTRACT_MUN_CENSO_2010["physicalPath"], "municipalities.parquet")
    ):
        write_log("Municipalities data already exists.")
    else:
        _ = get_municipalities_2010(**kwargs)


def upload_sectors_2010():
    """
    Uploads sectors data for the year 2010.

    This function processes sectors data and checks if the data already exists.
    If the data does not exist, it calls the `get_sectors_2010` function to retrieve it.
    """
    write_log("Processing sectors data...")
    kwargs = {"filename": ""}
    path = CONTRACT_SECTORS_CENSO_2010["physicalPath"]
    if os.path.exists(path):
        data_wrong = check_data_consistency(path)
        if any(data_wrong.values()):
            _ = get_sectors_2010(**kwargs)
    else:
        _ = get_sectors_2010(**kwargs)


def upload_sectors_2022():
    """
    Uploads sectors data for the year 2022.

    This function processes sectors data and checks if the data already exists.
    If the data does not exist, it calls the `get_sectors_2022` function to retrieve it.
    """
    write_log("Processing sectors data...")
    kwargs = {"filename": ""}
    path = CONTRACT_SECTORS_CENSO_2022["physicalPath"]
    if os.path.exists(path):
        data_wrong = check_data_consistency(path)
        if any(data_wrong.values()):
            _ = get_sectors_2022(**kwargs)
    else:
        _ = get_sectors_2022(**kwargs)


def upload_dompp_2022():
    """
    Uploads DOMPP data for the year 2022.

    This function processes DOMPP data and checks if the data already exists.
    If the data does not exist, it calls the `get_dompp_2022` function to retrieve it.
    """
    write_log("Processing dompp data...")
    path = CONTRACT_DOMPP_CENSO_2022["physicalPath"]
    if os.path.exists(path):
        data_wrong = check_data_consistency(path)
        if any(data_wrong.values()):
            get_dompp_2022()
    else:
        get_dompp_2022()


def main():
    """
    The main function that executes the script.
    """
    upload_municipalities_2010()
    upload_sectors_2010()
    upload_sectors_2022()
    upload_dompp_2022()
