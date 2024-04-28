"""
This script is used to upload ANEEL (Agência Nacional de Energia Elétrica) 
dataset files to the database.
It contains functions to upload different types of files (PONNOT, UCBT, RAMLIG) 
and process each row title from the 'df_aneel_ids' DataFrame.

Functions:
- upload_ponnot(row_title): Uploads ANEEL PONNOT files to the database.
- save_df_problematic(df_ucbt): Filters the given DataFrame to remove rows 
        where the 'pn_con' column is empty.
- upload_ucbt(row_title): Uploads ANEEL UCBT files to the database.
- upload_ramlig(row_title): Uploads ANEEL RAMLIG files to the database.
- main(): Processes each row title from the 'df_aneel_ids' DataFrame, 
        writes a log message, and uploads data to various services.
"""

import os
from shapely import wkt
import geopandas as gpd
from tqdm import tqdm
from src.tools.utils.read import Reader
from src.tools.utils.common import write_log
from src.tools.utils.config import get_contract
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.constants import CRS
from src.tools.databases.data_connection.connection import DBConnection
from src.databases.bronze.aneel.make_bronze_aneel_dataset_parquet import load_aneel_ids


CONTRACT_ID = get_contract("aneel/contract_aneel_companies_id.yaml", "silver")
CONTRACT_PONNOT = get_contract("aneel/contract_aneel_companies_ponnot.yaml", "bronze")
CONTRACT_UCBT = get_contract("aneel/contract_aneel_companies_ucbt.yaml", "bronze")
CONTRACT_RAMLIG = get_contract("aneel/contract_aneel_companies_ramlig.yaml", "bronze")


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_PONNOT, save_pq=False)
def upload_ponnot(row_title):
    """
    Uploads ANEEL PONNOT files to the database.
    """
    file_name = "_".join([row_title.split(".")[0], "ponnot"])
    file_path_large_files = os.path.join(CONTRACT_PONNOT["physicalPath"], file_name)
    file_path_small_files = os.path.join(
        CONTRACT_PONNOT["physicalPath"], "".join([file_name, ".parquet"])
    )
    exist_large_file = os.path.exists(file_path_large_files)
    reader = Reader(CONTRACT_PONNOT)
    if exist_large_file:
        df_ponnot = reader.read_parquet(file_path=file_path_large_files)
        df_ponnot = gpd.GeoDataFrame(
            df_ponnot,
            geometry=df_ponnot.filter(like="geom").map(wkt.loads).squeeze(),
            crs=CRS,
        )
    else:
        df_ponnot = reader.read_geoparquet(file_path=file_path_small_files)
    df_ponnot = df_ponnot.astype({"conj": int})
    return df_ponnot


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_UCBT, save_pq=False)
def upload_ucbt(row_title, saved_columns_ucbt):
    """
    Uploads the UCBT dataset.

    Args:
        row_title (str): The title of the row.
        saved_columns_ucbt (list): The list of columns to be saved.

    Returns:
        pandas.DataFrame: The UCBT dataset.

    """
    file_name = "_".join([row_title.split(".")[0], "ucbt"])
    file_path_large_files = os.path.join(CONTRACT_UCBT["physicalPath"], file_name)
    file_path_small_files = os.path.join(
        CONTRACT_UCBT["physicalPath"], "".join([file_name, ".parquet"])
    )
    exist_large_file = os.path.exists(file_path_large_files)
    file = file_path_large_files if exist_large_file else file_path_small_files
    reader = Reader(CONTRACT_UCBT)
    df_ucbt = reader.read_parquet(file_path=file, columns=saved_columns_ucbt)
    return df_ucbt


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_RAMLIG, save_pq=False)
def upload_ramlig(row_title):
    """
    Uploads ANEEL RAMLIG files to the database.
    """
    cols = [col["column"] for col in CONTRACT_RAMLIG["columns"]]
    file_name = "_".join([row_title.split(".")[0], "ramlig"])
    file_path_large_files = os.path.join(CONTRACT_RAMLIG["physicalPath"], file_name)
    file_path_small_files = os.path.join(
        CONTRACT_RAMLIG["physicalPath"], "".join([file_name, ".parquet"])
    )
    exist_large_file = os.path.exists(file_path_large_files)
    file = file_path_large_files if exist_large_file else file_path_small_files
    reader = Reader(CONTRACT_RAMLIG)
    df_ramlig = reader.read_parquet(file_path=file, columns=cols)
    return df_ramlig


def check_in_postgres(table_name: str):
    """
    Check if a table exists in the PostgreSQL database and return the distinct
    values of the 'dist' column.

    Parameters:
    table_name (str): The name of the table to check.

    Returns:
    pandas.DataFrame: A DataFrame containing the distinct values of the 'dist' column.

    """
    conn = DBConnection("bronze")
    df = conn.query_database(f"SELECT DISTINCT dist FROM infrastructure.{table_name}")
    return df


def run_ponnot(row_title, company_id, ponnot_in_db):
    """
    Runs the 'ponnot' process for a given row title, company ID, and 'ponnot_in_db' data.

    Args:
        row_title (str): The title of the row.
        company_id (int): The ID of the company.
        ponnot_in_db (pd.DataFrame): The 'ponnot' data already present in the database.
    """
    if company_id in ponnot_in_db.values:
        write_log(f"Row title {row_title} already exists in the 'ponnot' table.")
    else:
        _ = upload_ponnot(row_title)


def run_ucbt(row_title, company_id, ucbt_in_db, saved_columns_ucbt):
    """
    Runs the UCBT process for a given row title and company ID.

    Args:
        row_title (str): The title of the row.
        company_id (int): The ID of the company.
        ucbt_in_db (dict): A dictionary containing the existing UCBT values in the database.
        saved_columns_ucbt (list): A list of saved columns for the UCBT.
    """
    if company_id in ucbt_in_db.values:
        write_log(f"Row title {row_title} already exists in the 'ucbt' table.")
    else:
        _ = upload_ucbt(row_title, saved_columns_ucbt)


def run_ramlig(row_title, company_id, ramlig_in_db):
    """
    Runs the 'ramlig' process for a given row title and company ID.

    Args:
        row_title (str): The title of the row.
        company_id (int): The ID of the company.
        ramlig_in_db (pd.DataFrame): The DataFrame containing the existing 'ramlig' data.
    """
    if company_id in ramlig_in_db.values:
        write_log(f"Row title {row_title} already exists in the 'ramlig' table.")
    else:
        _ = upload_ramlig(row_title)


def get_cols_ucbt():
    """
    Retrieves the columns of the 'infrastructure.ucbt' table from the 'bronze' database.

    Returns:
        list: A list of column names.
    """
    conn = DBConnection("bronze")
    return conn.query_database("SELECT * FROM infrastructure.ucbt LIMIT 1").columns


def main(do_check=False):
    """
    This function processes each row title from the 'df_aneel_ids' DataFrame,
    writes a log message, and uploads data to various services.
    """
    df_aneel_ids = load_aneel_ids()
    # retirar arquivo que não tem camada ucbt
    df_aneel_ids = df_aneel_ids.query(
        "title != 'EAC_26_2022-12-31_V11_20230725-1759.gdb.zip'"
    ).astype({"company_id": int})
    saved_columns_ucbt = get_cols_ucbt()
    if do_check:
        ponnot_in_db = check_in_postgres("ponnot")
        ucbt_in_db = check_in_postgres("ucbt")
        ramlig_in_db = check_in_postgres("ramlig")

        for row_title, company_id in tqdm(
            zip(df_aneel_ids["title"], df_aneel_ids["company_id"])
        ):
            write_log(f"Processing row title: {row_title}")
            run_ponnot(row_title, company_id, ponnot_in_db)
            run_ucbt(row_title, company_id, ucbt_in_db, saved_columns_ucbt)
            run_ramlig(row_title, company_id, ramlig_in_db)
    else:
        for row_title in tqdm(df_aneel_ids["title"]):
            write_log(f"Processing row title: {row_title}")
            _ = upload_ponnot(row_title)
            _ = upload_ucbt(row_title, saved_columns_ucbt)
            _ = upload_ramlig(row_title)
