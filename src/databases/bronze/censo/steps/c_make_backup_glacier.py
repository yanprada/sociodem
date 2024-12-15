"""
This module is responsible for creating a backup of census data and 
saving it to the "bronze" database as Parquet files.
It utilizes the ExecutionManager to manage the execution details and status updates, 
and the DBConnection class to query
data from the database. The main function iterates over a list of data layers, 
queries the corresponding data from the
database, and saves it as Parquet files using a decorator.
Functions:
    save_data_wraper(df: pd.DataFrame, contract: dict, **kwargs) -> pd.DataFrame:
    main():
        The main function that orchestrates the process of querying 
        data from the database and saving it as Parquet files.

"""

from tqdm import tqdm
import pandas as pd

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.common import get_db_path
from src.tools.utils.save import save_parquet_decorator

from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.censo.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG


manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
CONTRACTS_BRONZE = execution_parameters["data_contracts"]["bronze"]
CONTRACTS_RAW_DATA = execution_parameters["data_contracts"]["raw_data"]
manager.update_status("running_step_3")


def save_data_wraper(df: pd.DataFrame, contract: dict, **kwargs) -> pd.DataFrame:
    """
    A wrapper function to save a DataFrame using a decorator that saves it as a Parquet file.
    This function uses the `save_parquet_decorator` to save the DataFrame to the "bronze" database
    as a Parquet file. The decorator is configured to not save to the database (`save_db=False`)
    but to save as a Parquet file (`save_pq=True`).
    Args:
        df (pandas.DataFrame): The DataFrame to be saved.
        contract (str): The contract identifier used by the decorator.
        **kwargs: Additional keyword arguments to be passed to the `save_data` function.
    Returns:
        pandas.DataFrame: The same DataFrame that was passed in,
                        after being processed by the decorator.
    """

    @save_parquet_decorator("bronze", contract, save_db=False, save_pq=True)
    def save_data(df, **kwargs):
        return df

    return save_data(df, **kwargs)


def main():
    """
    The main function that orchestrates the process of querying data from the database
    and saving it as Parquet files. The function iterates over a list of data layers,
    queries the corresponding data from the database, and saves it as Parquet
    files using a decorator.
    """
    conn = DBConnection("bronze")
    for layer_key in tqdm(
        [
            "mun_2010",
            "mun_2022",
            "sectors_2010",
            "sectors_2022",
            "districts_2010",
            "districts_2022",
            "subdistricts_2010",
            "subdistricts_2022",
            "states_2022",
        ],
        "Saving data to bronze",
    ):
        kwargs = {"filename": layer_key}
        data_contract = CONTRACTS_BRONZE[layer_key]
        path = get_db_path(data_contract)
        physical_path = data_contract["physicalPath"]
        df = conn.query_database(f"SELECT * FROM {path}")
        df = save_data_wraper(df, data_contract, **kwargs)
        print(f"Saved {layer_key} to {physical_path}")
    manager.update_status("finished_step_3")
    manager.update_last_run()
