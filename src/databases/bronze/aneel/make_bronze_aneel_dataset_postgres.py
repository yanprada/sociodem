"""
This script is used to upload ANEEL (Agência Nacional de Energia Elétrica) 
dataset files to the database.
It contains functions to upload different types of files (PONNOT, UCBT, RAMLIG) 
and process each company id from the 'df_aneel_ids' DataFrame.

Functions:
- upload_ponnot(company_id: str) -> gpd.GeoDataFrame: Uploads ANEEL PONNOT files to the database.
- save_df_problematic(df_ucbt: pd.DataFrame) -> pd.DataFrame: Filters the given DataFrame 
    to remove rows where the 'pn_con' column is empty.
- upload_ucbt(company_id: str, saved_columns_ucbt: List[str]) -> pd.DataFrame: Uploads ANEEL
     UCBT files to the database.
- upload_ramlig(company_id: str) -> pd.DataFrame: Uploads ANEEL RAMLIG files to the database.
- main(check_in_db: bool = True) -> None: Processes each company id from the 'df_aneel_ids' 
    DataFrame, writes a log message, and uploads data to various services.
"""

import os
from typing import List
from shapely import wkt
import geopandas as gpd
from tqdm import tqdm
import pandas as pd
from src.tools.utils.read import Reader
from src.tools.utils.common import write_log
from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.constants import CRS
from src.tools.databases.data_connection.connection import DBConnection
from src.databases.bronze.aneel.make_bronze_aneel_dataset_parquet import load_aneel_ids


CONTRACTS = get_aneel_contracts("bronze")


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS["ponnot"], save_pq=False)
def upload_ponnot(file_name: int) -> gpd.GeoDataFrame:
    """
    Uploads ANEEL PONNOT files to the database.
    """
    file_path_large_files = os.path.join(CONTRACTS["ponnot"]["physicalPath"], file_name)
    file_path_small_files = os.path.join(
        CONTRACTS["ponnot"]["physicalPath"], "".join([file_name, ".parquet"])
    )
    exist_large_file = os.path.exists(file_path_large_files)
    reader = Reader(CONTRACTS["ponnot"])
    if exist_large_file:
        df_ponnot = reader.read_parquet(file_path=file_path_large_files)
        df_ponnot = gpd.GeoDataFrame(
            df_ponnot,
            geometry=df_ponnot.filter(like="geom").map(wkt.loads).squeeze(),
            crs=CRS,
        )
    else:
        df_ponnot = reader.read_geoparquet(file_path=file_path_small_files)
    df_ponnot["conj"] = df_ponnot["conj"].fillna(-1)
    df_ponnot = df_ponnot.astype({"conj": int})
    return df_ponnot


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS["ucbt"], save_pq=False)
def upload_ucbt(file_name: str, saved_columns_ucbt: List[str] = None) -> pd.DataFrame:
    """
    Uploads the UCBT dataset.

    Args:
    file_name (str): The title of the row.
    saved_columns_ucbt (list): The list of columns to be saved.

    Returns:
    pandas.DataFrame: The UCBT dataset.

    """
    file_path_large_files = os.path.join(CONTRACTS["ucbt"]["physicalPath"], file_name)
    file_path_small_files = os.path.join(
        CONTRACTS["ucbt"]["physicalPath"], "".join([file_name, ".parquet"])
    )
    exist_large_file = os.path.exists(file_path_large_files)
    file = file_path_large_files if exist_large_file else file_path_small_files
    reader = Reader(CONTRACTS["ucbt"])
    if saved_columns_ucbt is None:
        df_ucbt = reader.read_parquet(file_path=file)
    else:
        df_ucbt = reader.read_parquet(file_path=file, columns=saved_columns_ucbt)
    return df_ucbt


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS["ramlig"], save_pq=False)
def upload_ramlig(file_name: str) -> pd.DataFrame:
    """
    Uploads ANEEL RAMLIG files to the database.
    """
    file_path_large_files = os.path.join(CONTRACTS["ramlig"]["physicalPath"], file_name)
    file_path_small_files = os.path.join(
        CONTRACTS["ramlig"]["physicalPath"], "".join([file_name, ".parquet"])
    )
    exist_large_file = os.path.exists(file_path_large_files)
    file = file_path_large_files if exist_large_file else file_path_small_files
    reader = Reader(CONTRACTS["ramlig"])
    df_ramlig = reader.read_parquet(file_path=file)
    return df_ramlig


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS["conj"], save_pq=False)
def upload_conj(file_name: int) -> gpd.GeoDataFrame:
    """
    Uploads ANEEL CONJ files to the database.
    """
    file_path = os.path.join(
        CONTRACTS["conj"]["physicalPath"], "".join([file_name, ".parquet"])
    )
    reader = Reader(CONTRACTS["conj"])
    df_conj = reader.read_geoparquet(file_path=file_path)
    return df_conj


def check_in_postgres(table_name: str) -> pd.DataFrame:
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


def run_ponnot(company_id: int, ponnot_in_db: pd.DataFrame) -> None:
    """
    Runs the 'ponnot' process for a given company id, company ID, and 'ponnot_in_db' data.

    Args:
    company_id (int): The ID of the company.
    ponnot_in_db (pd.DataFrame): The 'ponnot' data already present in the database.
    """
    if company_id in ponnot_in_db.values:
        write_log(f"Company ID {company_id} already exists in the 'ponnot' table.")
    else:
        _ = upload_ponnot(company_id)


def run_ucbt(
    company_id: int, ucbt_in_db: dict, saved_columns_ucbt: List[str] = None
) -> None:
    """
    Runs the UCBT process for a given company id and company ID.

    Args:
    company_id (int): The ID of the company.
    ucbt_in_db (dict): A dictionary containing the existing UCBT values in the database.
    saved_columns_ucbt (list): A list of saved columns for the UCBT.
    """
    if company_id in ucbt_in_db.values:
        write_log(f"company id {company_id} already exists in the 'ucbt' table.")
    else:
        _ = upload_ucbt(company_id, saved_columns_ucbt)


def run_ramlig(company_id: int, ramlig_in_db: pd.DataFrame) -> None:
    """
    Runs the 'ramlig' process for a given company ID.

    Args:
    company_id (str): The title of the row.
    company_id (int): The ID of the company.
    ramlig_in_db (pd.DataFrame): The DataFrame containing the existing 'ramlig' data.
    """
    if company_id in ramlig_in_db.values:
        write_log(f"company id {company_id} already exists in the 'ramlig' table.")
    else:
        _ = upload_ramlig(company_id)


def run_conj(company_id: int, conj_in_db: pd.DataFrame) -> None:
    """
    Runs the 'conj' process for a given company ID.

    Args:
    company_id (str): The title of the row.
    company_id (int): The ID of the company.
    conj_in_db (pd.DataFrame): The DataFrame containing the existing 'conj' data.
    """
    if company_id in conj_in_db.values:
        write_log(f"company id {company_id} already exists in the 'conj' table.")
    else:
        _ = upload_conj(company_id)


def get_cols_ucbt() -> List[str]:
    """
    Retrieves the columns of the 'infrastructure.ucbt' table from the 'bronze' database.

    Returns:
    list: A list of column names.
    """
    conn = DBConnection("bronze")
    return conn.query_database("SELECT * FROM infrastructure.ucbt LIMIT 1").columns


def main(check_in_db: bool = True) -> None:
    """
    This function processes each company id from the 'df_aneel_ids' DataFrame,
    writes a log message, and uploads data to various services.
    """
    df_aneel_ids = load_aneel_ids().astype({"company_id": int})
    saved_columns_ucbt = get_cols_ucbt()
    if check_in_db:
        ponnot_in_db = check_in_postgres("ponnot")
        ucbt_in_db = check_in_postgres("ucbt")
        ramlig_in_db = check_in_postgres("ramlig")
        conj_in_db = check_in_postgres("conj")

        for company_id in tqdm(df_aneel_ids["company_ids"]):
            write_log(f"Processing row id: {company_id}")
            run_ponnot(company_id, ponnot_in_db)
            run_ucbt(company_id, ucbt_in_db, saved_columns_ucbt)
            run_ramlig(company_id, ramlig_in_db)
            run_conj(company_id, conj_in_db)
    else:
        for company_id in tqdm(df_aneel_ids["company_ids"]):
            write_log(f"Processing row id: {company_id}")
            _ = upload_ponnot(company_id)
            _ = upload_ucbt(company_id, saved_columns_ucbt)
            _ = upload_ramlig(company_id)
            _ = upload_conj(company_id)
