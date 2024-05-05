"""
This module contains functions for testing and re-uploading data 
to Postgres database if failed in test
"""

import os
from typing import Tuple
import pandas as pd
from tqdm import tqdm
from src.databases.bronze.aneel.make_bronze_aneel_dataset_postgres import (
    upload_ponnot,
    upload_ucbt,
    upload_ramlig,
    get_cols_ucbt,
)
from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.common import write_log, add_test_to_yaml, get_test_yaml

CONTRACTS = get_aneel_contracts("bronze")

FUNCTIONS = {
    "ponnot": upload_ponnot,
    "ucbt": upload_ucbt,
    "ramlig": upload_ramlig,
}


def re_upload_data(
    conn: DBConnection,
    dataframes: Tuple[pd.DataFrame, pd.DataFrame],
    table_name: str,
    company_info: Tuple[int, str],
    saved_columns_ucbt: list,
):
    """
    Re-uploads data to the database after deleting existing rows for a specific company.

    Args:
        conn (DBConnection): The database connection object.
        dataframes (Tuple[pd.DataFrame, pd.DataFrame]): A Tuple of two pandas DataFrames
                                                    containing the data to be uploaded.
        table_name (str): The name of the DataFrame being uploaded.
        company_info (Tuple[int, str]): A tuple containing the company ID and
                                        title for which the data is being uploaded.
        saved_columns_ucbt (list): A list of saved columns for the "ucbt" table.
    """
    company_id, company_title = company_info
    write_log(f"Re-uploading data from company {company_title} to Postgres")
    schema = CONTRACTS[table_name]["schema"]
    df_pq, df_db = dataframes
    conn.delete_rows_table((schema, table_name), condition=f"dist = {company_id}")
    func = FUNCTIONS[table_name]
    if table_name == "ucbt":
        _ = func(company_title, saved_columns_ucbt)
    else:
        _ = func(company_title)
    df_db = conn.query_database(
        f"SELECT * FROM {schema}.{table_name} WHERE dist = {company_id}"
    )
    yaml_path = "src/tests/bronze/aneel/tests_aneel.yaml"
    add_test_to_yaml(yaml_path, company_title, df_pq.shape[0] == df_db.shape[0])


def test_data(
    table_name: str, company_id: int, company_title: str, saved_columns_ucbt: list
):
    """
    Test the data loaded in Postgres by comparing it with the data in a Parquet file.

    Args:
        table_name (str): The name of the dataframe to be tested.
        company_id (int): The company ID to filter the data.
        company_title (str): The title of the company for which the data is being tested.
        saved_columns_ucbt (list): A list of saved columns for the "ucbt" table.
    """
    path = os.path.join(
        CONTRACTS[table_name]["physicalPath"],
        company_title,
    )
    try:
        df_pq = pd.read_parquet("".join([path, ".parquet"]))
    except FileNotFoundError:
        df_pq = pd.read_parquet("".join([path, "/"]))
    conn = DBConnection("bronze")
    df_db = conn.query_database(
        f"SELECT * FROM infrastructure.{table_name} WHERE dist = {company_id}"
    )
    if df_pq.shape[0] != df_db.shape[0]:
        write_log(
            f"File {company_title} from table {table_name} have equal number of rows."
            + f"Parquet rows: {df_pq.shape[0]}, Postgres rows: {df_db.shape[0]}"
        )
        re_upload_data(
            conn,
            (df_pq, df_db),
            table_name,
            (company_id, company_title),
            saved_columns_ucbt,
        )
    else:
        yaml_path = "src/tests/bronze/aneel/tests_aneel.yaml"
        add_test_to_yaml(yaml_path, company_title, True)


def load_aneel_ids() -> pd.DataFrame:
    """
    Loads ANEEL IDs from a CSV file and returns them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing ANEEL IDs.
    """
    conn = DBConnection("silver")
    path = ".".join(
        [CONTRACTS["company_id"]["schema"], CONTRACTS["company_id"]["tableName"]]
    )
    df = conn.query_database(
        f"""
            SELECT * FROM {path} 
            WHERE year = '{CONTRACTS["company_id"]["queryYear"]}' 
            AND company NOT LIKE '%tab%' 
            AND title LIKE '%_V%'
        """
    )
    return df


def main():
    """
    This is the main function that performs the data loading and testing operations.
    It loads ANEEL IDs, filters out a specific title, and then performs tests on the data.
    """
    df_aneel_ids = load_aneel_ids()
    df_aneel_ids = df_aneel_ids.query(
        "title != 'EAC_26_2022-12-31_V11_20230725-1759.gdb.zip'"
    ).astype({"company_id": int})
    saved_columns_ucbt = get_cols_ucbt()
    for _, row in tqdm(df_aneel_ids.iterrows()):
        company_id = row["company_id"]
        company_title = row["title"]
        write_log(f"Processing row title: {company_title}")
        for table_name in tqdm(["ponnot", "ucbt", "ramlig"]):
            company_title_data = company_title.replace(
                ".gdb.zip", "".join(["_", table_name])
            )
            test_yaml = get_test_yaml("src/tests/bronze/aneel/tests_aneel.yaml")
            if (
                test_yaml is None
                or test_yaml.get(company_title_data, None) is None
                or test_yaml[company_title_data] is False
            ):
                test_data(
                    table_name, company_id, company_title_data, saved_columns_ucbt
                )
