"""
This script saves ANEEL data to Glacier by connecting to the "bronze" database,
retrieving data for each year within the specified range, and processing the data
for each company and municipality. The processed data is then saved using the
`save_ucbt_data` function.

Functions:
    save_data_in_glacier(contract: dict):

    create_indexes(conn: DBConnection, contract: dict, year: str) -> None:
"""

import os
from typing import Optional, Union
import pandas as pd
from tqdm import tqdm

from src.tools.databases.data_connection.connection import DBConnection

from src.tools.utils.common import get_db_path, write_log
from src.tools.utils.save import save_parquet_decorator

from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.aneel.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
manager.update_status("running_step_6")

ANEEL_BRONZE_CONTRACTS = execution_parameters["data_contracts"]["aneel_bronze"]


def save_data_in_glacier(contract: dict, after_join: Optional[bool] = False) -> None:
    """
    Save ANEEL data to Glacier.
    This function connects to the "bronze" database, retrieves data for each year
    within the specified range, and processes the data for each company and municipality.
    The processed data is then saved using the `save_ucbt_data` function.
    Steps:
    1. Connect to the "bronze" database.
    2. Retrieve the path and query years for data from the
        ANEEL_BRONZE_CONTRACTS configuration.
    3. For each year in the specified range:
        a. Retrieve distinct company IDs for the given year.
        b. For each company, retrieve distinct municipalities.
        c. For each municipality, query the data for the given company and municipality.
        d. Process and save the data using the `save_data` function.
    4. Close the database connection.
    """

    @save_parquet_decorator("bronze", contract, save_db=False, save_pq=True)
    def save_data(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return df

    conn = DBConnection("bronze")
    for year in tqdm(
        range(contract["queryYears"][0], contract["queryYears"][1] + 1), desc="Years"
    ):
        path = get_db_path(contract)
        if after_join:
            create_indexes(conn, contract, str(year))
            path = "_".join([path, str(year)])
        else:
            create_indexes(conn, contract)
        companies = conn.query_database(
            f"SELECT DISTINCT company_file FROM {path} WHERE year = '{year}'"
        )
        for companie in tqdm(companies["company_file"], desc=f"Companies for {year}"):
            physical_path = contract["physicalPath"]
            folder_path = f"{physical_path}{year}/{companie}/"
            if not os.path.exists(folder_path):
                muns = conn.query_database(
                    f"""
                        SELECT DISTINCT mun 
                        FROM {path} 
                        WHERE company_file = '{companie}' 
                        AND year = '{year}'
                    """
                )
                for mun in tqdm(
                    muns["mun"], desc=f"Municipalities for {companie} {year}"
                ):
                    query = f"""
                                SELECT * 
                                FROM {path} 
                                WHERE company_file = '{companie}' 
                                AND mun = '{mun}'
                                AND year = '{year}'
                            """
                    kwargs = {"filename": f"{year}/{companie}/{mun}"}
                    df = conn.query_database(query)
                    df = save_data(df, **kwargs)
            else:
                write_log(
                    f"Folder {folder_path} already exists. Skipping data retrieval."
                )
    conn.close()


def create_indexes(
    conn: DBConnection, contract: dict, year: Optional[Union[str, None]] = None
) -> None:
    """
    Creates indexes on the specific table.

    Args:
        conn (DBConnection): The connection object to the database.
        contract (dict): The contract dictionary containing the schema and table name.
        year (Union[str, None], optional): The year to create the index for. Defaults to None.
    """
    schema = contract["schema"]
    if year is None:
        table_name = contract["tableName"]
    else:
        table_name = "_".join([contract["tableName"], year])
    conn.create_index(schema, table_name, ["year", "mun", "company_file"])
    conn.create_index(schema, table_name, ["year", "company_file"])
    conn.create_index(schema, table_name, ["year"])


def main():
    """
    Main function to save ANEEL bronze contracts data to Glacier.
    This function calls the save_data_in_glacier function for each of the
    specified ANEEL bronze contracts: "ucbt" and "ponnot".
    It is assumed that ANEEL_BRONZE_CONTRACTS is a dictionary containing
    the necessary data for these contracts.
    """
    save_data_in_glacier(ANEEL_BRONZE_CONTRACTS["ucbt"])
    save_data_in_glacier(ANEEL_BRONZE_CONTRACTS["ponnot"])
    save_data_in_glacier(ANEEL_BRONZE_CONTRACTS["aneel_join"])
    save_data_in_glacier(ANEEL_BRONZE_CONTRACTS["ucbt_no_join"])
