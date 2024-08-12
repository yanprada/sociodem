"""
This script retrieves data from the Censo 2010 and 
Censo 2022 datasets and saves it as parquet files and to database.

The script contains the following functions:
- get_municipalities_2010: Retrieves the municipalities data from the Censo 2010 dataset.
- get_mun_2022: Retrieves the municipalities data from the Censo 2022 dataset.
- get_sectors_2010: Retrieves the census sectors data from the Censo 2010 dataset.
- main: The main function that executes the script.

Note: The script assumes the existence of certain contract files and directories.
"""

import os
import zipfile
import pandas as pd
from tqdm import tqdm
from src.tools.utils.reader import Reader
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.constants import STATES
from src.tools.utils.common import write_log, check_data_consistency
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.censo.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG


manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
CONTRACTS = execution_parameters["data_contracts"][0]
CONTRACT_PARTITIONS = execution_parameters["data_contracts"][1]
manager.update_status("running_step_2")


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS["dompp_2022"])
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
    reader = Reader()
    filepath = os.path.join(
        CONTRACTS["dompp_2022"]["physicalPath"]
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


def upload_dompp_2022():
    """
    Uploads DOMPP data for the year 2022.

    This function processes DOMPP data and checks if the data already exists.
    If the data does not exist, it calls the `get_dompp_2022` function to retrieve it.
    """
    write_log("Processing dompp data...")
    path = CONTRACTS["dompp_2022"]["physicalPath"]
    if os.path.exists(path):
        data_wrong = check_data_consistency(path)
        if any(data_wrong.values()):
            get_dompp_2022()
    else:
        get_dompp_2022()


def upload_censo_data(layer_key):
    """
    Uploads municipalities data for the year 2010.

    This function processes the municipalities data and checks if the data already exists.
    If the data does not exist, it calls the `get_municipalities_2010` function to retrieve it.
    """

    @save_parquet_decorator(medallon="bronze", contract=CONTRACTS[layer_key])
    def get_censo_data(layer_key, **kwargs):
        """
        Retrieves the data from the Censo dataset.

        Args:
            layer_key (str): The key of the layer to use.
            **kwargs: Additional keyword arguments.

        Returns:
            pandas.DataFrame: A DataFrame containing the data.
        """
        reader = Reader()
        dfs = []
        for state in tqdm(STATES):
            filepath = os.path.join(
                CONTRACTS[layer_key]["physicalPath"]
                .replace("databases", "datalake")
                .replace("bronze/", ""),
                "".join([state, ".zip"]),
            )
            df = reader.read_geofile(filepath)
            dfs.append(df)
        dfs = pd.concat(dfs)
        return dfs

    write_log(f"Processing {layer_key} data...")
    kwargs = {"filename": layer_key}
    path = CONTRACTS[layer_key]["physicalPath"]
    if os.path.exists(os.path.join(path, f"{layer_key}.parquet")):
        data_wrong = check_data_consistency(path)
        if any(data_wrong.values()):
            _ = get_censo_data(layer_key, **kwargs)
        write_log(f"{layer_key} data already exists.")
    else:

        _ = get_censo_data(layer_key, **kwargs)


def main():
    """
    The main function that executes the script.
    """
    for layer_key in [
        "municipalities_2010",
        "mun_2022",
        "sectors_2010",
        "sectors_2022",
    ]:
        upload_censo_data(layer_key)
    upload_dompp_2022()
    manager.update_status("finished_step_2")
    manager.update_last_run()
