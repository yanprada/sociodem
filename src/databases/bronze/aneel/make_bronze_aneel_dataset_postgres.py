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
from functools import lru_cache
from typing import List
from shapely import wkt, wkb
import geopandas as gpd
from tqdm import tqdm
import pandas as pd
import numpy as np
from pyarrow.lib import ArrowInvalid

from src.tools.utils.read import Reader
from src.tools.utils.common import write_log
from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.constants import CRS
from src.tools.databases.data_connection.connection import DBConnection
from src.databases.bronze.aneel.make_bronze_aneel_dataset_parquet import load_aneel_ids


CONTRACTS = get_aneel_contracts("bronze")


def add_missing_columns(df: pd.DataFrame, saved_columns: List[str]) -> pd.DataFrame:
    """
    Adds missing columns to a DataFrame and fills them with NaN values.

    Args:
        df (pd.DataFrame): The DataFrame to add missing columns to.
        saved_columns (List[str]): The list of columns that should be present in the DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with missing columns added and filled with NaN values.
    """
    cols_not_in_saved_cols = list(set(saved_columns).difference(set(df.columns)))
    df[cols_not_in_saved_cols] = [np.nan] * len(cols_not_in_saved_cols)
    return df[saved_columns]


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS["ponnot"], save_pq=False)
def upload_ponnot(
    file_name: int, saved_columns_ponnot: List[str] = None
) -> gpd.GeoDataFrame:
    """
    Uploads ANEEL PONNOT files to the database.

    Args:
        file_name (int): The name of the file to upload.
        saved_columns_ponnot (List[str], optional): List of column names
                            to be saved. Defaults to None.

    Returns:
        gpd.GeoDataFrame: The uploaded ANEEL PONNOT data as a GeoDataFrame.
    """
    file_path_large_files = os.path.join(CONTRACTS["ponnot"]["physicalPath"], file_name)
    file_path_small_files = os.path.join(
        CONTRACTS["ponnot"]["physicalPath"], "".join([file_name, ".parquet"])
    )
    exist_large_file = os.path.exists(file_path_large_files)
    reader = Reader(CONTRACTS["ponnot"])
    if exist_large_file:
        df_ponnot = reader.read_parquet(file_path=file_path_large_files)
        geom_test = df_ponnot.filter(like="geom").squeeze().iloc[0]
        mapper = wkb.loads if isinstance(geom_test, bytes) else wkt.loads
        df_ponnot = gpd.GeoDataFrame(
            df_ponnot,
            geometry=df_ponnot.filter(like="geom").map(mapper).squeeze(),
            crs=CRS,
        )
    else:
        df_ponnot = reader.read_geoparquet(file_path=file_path_small_files)
    df_ponnot = add_missing_columns(df_ponnot, saved_columns_ponnot)
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
        return reader.read_parquet(file_path=file)
    try:
        df_ucbt = reader.read_parquet(file_path=file, columns=saved_columns_ucbt)
    except ArrowInvalid as e:
        write_log(f"Error reading parquet file: {e}")
        df_ucbt = reader.read_parquet(file_path=file)
        df_ucbt = add_missing_columns(df_ucbt, saved_columns_ucbt)
    return df_ucbt


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS["ramlig"], save_pq=False)
def upload_ramlig(
    file_name: str, saved_columns_ramlig: List[str] = None
) -> pd.DataFrame:
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
    df_ramlig = add_missing_columns(df_ramlig, saved_columns_ramlig)
    return df_ramlig


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS["conj"], save_pq=False)
def upload_conj(
    file_name: int, saved_columns_conj: List[str] = None
) -> gpd.GeoDataFrame:
    """
    Uploads ANEEL CONJ files to the database.
    """
    file_path = os.path.join(
        CONTRACTS["conj"]["physicalPath"], "".join([file_name, ".parquet"])
    )
    reader = Reader(CONTRACTS["conj"])
    df_conj = reader.read_geoparquet(file_path=file_path)
    df_conj = add_missing_columns(df_conj, saved_columns_conj)
    return df_conj


@lru_cache(maxsize=10)
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


def run_ponnot(
    company_id: str,
    id_dict: dict,
    ponnot_in_db: pd.DataFrame,
    saved_columns_ponnot: List[str] = None,
) -> None:
    """
    Runs the 'ponnot' process for a given company id, company ID, and 'ponnot_in_db' data.

    Args:
        company_id (str): The ID of the company.
        id_dict (dict): A dictionary containing the mapping of company IDs to their
                        corresponding values.
        ponnot_in_db (pd.DataFrame): The 'ponnot' data already present in the database.
        saved_columns_ponnot (list, optional): A list of saved columns for the 'ponnot'
                            table. Defaults to None.
    """
    if id_dict[company_id] in ponnot_in_db.values:
        write_log(f"Company ID {company_id} already exists in the 'ponnot' table.")
    else:
        _ = upload_ponnot(company_id, saved_columns_ponnot)


def run_ucbt(
    company_id: str,
    id_dict: dict,
    ucbt_in_db: dict,
    saved_columns_ucbt: List[str] = None,
) -> None:
    """
    Runs the UCBT process for a given company id and company ID.

    Args:
        company_id (str): The ID of the company.
        id_dict (dict): A dictionary containing the mapping of company IDs to their
                        corresponding values.
        ucbt_in_db (dict): A dictionary containing the existing UCBT values in the database.
        saved_columns_ucbt (list, optional): A list of saved columns for the UCBT. Defaults to None.
    """
    if id_dict[company_id] in ucbt_in_db.values:
        write_log(f"company id {company_id} already exists in the 'ucbt' table.")
    else:
        _ = upload_ucbt(company_id, saved_columns_ucbt)


def run_ramlig(
    company_id: str,
    id_dict: dict,
    ramlig_in_db: pd.DataFrame,
    saved_columns_ramlig: List[str] = None,
) -> None:
    """
    Runs the 'ramlig' process for a given company ID.

    Args:
        company_id (str): The ID of the company.
        id_dict (dict): A dictionary containing company IDs as keys and their corresponding values.
        ramlig_in_db (pd.DataFrame): The DataFrame containing the existing 'ramlig' data.
        saved_columns_ramlig (list): The list of columns to be saved.
    """
    if id_dict[company_id] in ramlig_in_db.values:
        write_log(f"company id {company_id} already exists in the 'ramlig' table.")
    else:
        _ = upload_ramlig(company_id, saved_columns_ramlig)


def run_conj(
    company_id: str,
    id_dict: dict,
    conj_in_db: pd.DataFrame,
    saved_columns_conj: List[str] = None,
) -> None:
    """
    Runs the 'conj' process for a given company ID.

    Args:
    company_id (str): The title of the row.
    company_id (int): The ID of the company.
    conj_in_db (pd.DataFrame): The DataFrame containing the existing 'conj' data.
    saved_columns_conj (list): The list of columns to be saved.
    """
    if id_dict[company_id] in conj_in_db.values:
        write_log(f"company id {company_id} already exists in the 'conj' table.")
    else:
        _ = upload_conj(company_id, saved_columns_conj)


@lru_cache(maxsize=10)
def get_cols_in_db(table_name) -> List[str]:
    """
    Retrieves the columns of the 'infrastructure.ucbt' table from the 'bronze' database.

    Returns:
    list: A list of column names.
    """
    schema = CONTRACTS[table_name]["schema"]
    table = CONTRACTS[table_name]["tableName"]
    conn = DBConnection("bronze")
    return conn.query_database(f"SELECT * FROM {schema}.{table} LIMIT 1").columns


def get_dictionary_id_dist(df_aneel_ids: pd.DataFrame) -> dict:
    """
    Retrieves a dictionary mapping company IDs to their corresponding dist values.

    Args:
        df_aneel_ids (pd.DataFrame): A DataFrame containing company IDs.

    Returns:
        dict: A dictionary mapping company IDs to their corresponding dist values.
    """
    id_dict = {}
    for company_id in df_aneel_ids["company_ids"]:
        temp = pd.read_parquet(f"databases/bronze/aneel/conj/{company_id}.parquet")
        id_dict[company_id] = temp.dist.unique()[0]
    return id_dict


def run_with_check_in_db(df_aneel_ids, id_dict) -> None:
    """
    Runs the data processing functions for each company ID in the given DataFrame.

    Args:
        df_aneel_ids (pandas.DataFrame): DataFrame containing the company IDs.
        id_dict (dict): Dictionary containing ID mappings.
    """
    saved_columns_ucbt = get_cols_in_db("ucbt")
    saved_columns_ponnot = get_cols_in_db("ponnot")
    saved_columns_ramlig = get_cols_in_db("ramlig")
    saved_columns_conj = get_cols_in_db("conj")

    ponnot_in_db = check_in_postgres("ponnot")
    ucbt_in_db = check_in_postgres("ucbt")
    ramlig_in_db = check_in_postgres("ramlig")
    conj_in_db = check_in_postgres("conj")

    for company_id in tqdm(df_aneel_ids["company_ids"]):
        write_log(f"Processing row id: {company_id}")
        run_ponnot(company_id, id_dict, ponnot_in_db, saved_columns_ponnot)
        run_ucbt(company_id, id_dict, ucbt_in_db, saved_columns_ucbt)
        run_ramlig(company_id, id_dict, ramlig_in_db, saved_columns_ramlig)
        run_conj(company_id, id_dict, conj_in_db, saved_columns_conj)


def run_without_checking_in_db(df_aneel_ids: pd.DataFrame) -> None:
    """
    Runs the data processing pipeline for the given DataFrame of ANEEL IDs without
    checking in the database.

    Args:
        df_aneel_ids (pd.DataFrame): DataFrame containing ANEEL IDs.
    """
    for company_id in tqdm(df_aneel_ids["company_ids"]):
        write_log(f"Processing row id: {company_id}")
        _ = upload_ponnot(company_id)
        _ = upload_ucbt(company_id, None)
        _ = upload_ramlig(company_id)
        _ = upload_conj(company_id)


def main(check_in_db: bool = True) -> None:
    """
    This function processes each company id from the 'df_aneel_ids' DataFrame,
    writes a log message, and uploads data to various services.

    Our first run do not ckheck in the database, but we can change this behavior after uploading
    """
    df_aneel_ids = load_aneel_ids()
    id_dict = get_dictionary_id_dist(df_aneel_ids)
    if check_in_db:
        run_with_check_in_db(df_aneel_ids, id_dict)
    else:
        run_without_checking_in_db(df_aneel_ids)
