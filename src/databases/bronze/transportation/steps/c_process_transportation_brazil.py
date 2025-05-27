"""
This module contains functions for processing places in Brazil.

The main function `main` reads the files in the specified path,
divides the workload among the available workers,
and processes the files using the `process_files` function.
It also handles any exceptions that occur
during the processing and logs them using the `write_log` function.

Other functions in this module include:
- `read_files`: Reads a Parquet file and returns a DataFrame.
- `get_brazil_geom`: Reads the Brazil geometry from a geojson file.
- `save_file`: Saves a DataFrame to a Parquet file.
- `process_files`: Process the given files and perform spatial join with df_brazil_geom.
"""

import os
import ast
from tqdm import tqdm
from shapely import wkt, wkb
import numpy as np
import pandas as pd
import geopandas as gpd


from src.tools.utils.reader import Reader
from src.tools.managers.saver_manager import save_parquet_decorator
from src.tools.utils.common import generate_random_string, get_db_path
from src.tools.utils.constants import CRS_GLOBAL
from src.tools.databases.data_connection.connection import DBConnection

from src.tools.managers.execution_manager import ExecutionManager
from src.databases.bronze.transportation.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)

TRANSPORT_CONTRACTS = execution_parameters["data_contracts"]["transportation_bronze"]


def read_files(file: str) -> pd.DataFrame:
    """
    Reads a Parquet file and returns a DataFrame.

    Args:
        file (str): The name of the Parquet file to read.

    Returns:
        pd.DataFrame: The DataFrame containing the data read from the file.
    """
    reader = Reader()
    path = os.path.join(
        TRANSPORT_CONTRACTS["pois"]["physicalPath"].replace("processed", "tmp"), file
    )
    df = reader.read_parquet(
        path,
        columns=[
            "geometry",
            "sources",
            "names",
            "categories",
            "confidence",
            "websites",
            "socials",
            "emails",
            "phones",
            "brand",
            "addresses",
        ],
    )
    df["geometry"] = df["geometry"].apply(wkt.loads)
    df = gpd.GeoDataFrame(df, geometry="geometry")
    df = df.set_crs(CRS_GLOBAL)
    return df


def get_pois_categories_map() -> dict:
    """
    Retrieves the points of interest (POIs) categories from a CSV file.

    Returns:
        pandas.DataFrame: A DataFrame containing the POIs categories.

    Raises:
        AssertionError: If the primary category is not equal to the GPT category.

    Returns:
        dict: A dictionary containing the POIs categories, with the primary category
        as the key and the GPT category as the value.
    """
    path = TRANSPORT_CONTRACTS["categories"]["physicalPath"]
    reader = Reader()
    df = reader.read_csv(path)
    assert all(
        df["category_primary"] == df["gpt"]
    ), "Primary category is not equal to GPT category"
    pois_map = (
        df.set_index("category_primary")
        .drop(columns="gpt")
        .to_dict()["general_categories"]
    )
    pois_map[""] = "None"
    return pois_map


def get_brazil_geom():
    """
    Reads the Brazil geometry from a geojson file.

    Returns:
        GeoDataFrame: A GeoDataFrame containing the Brazil geometry.
    """
    conn = DBConnection("bronze")
    contract = TRANSPORT_CONTRACTS["mun_2022"]
    path = get_db_path(contract)
    df = conn.query_database(f"SELECT cd_mun, nm_mun, sigla_uf, geometry FROM {path}")
    df["geometry"] = df["geometry"].apply(wkb.loads)
    df = gpd.GeoDataFrame(df, geometry="geometry")
    df = df.set_crs(CRS_GLOBAL)
    return df


def get_update_info(record_str: str) -> str:
    """
    Extracts the 'update' information from a record string.

    Args:
        record_str (str): The record string containing the update information.

    Returns:
        str: The 'update' information extracted from the record.

    """

    record = ast.literal_eval(
        str(record_str).split("\n", maxsplit=1)[0].replace("[", "").replace("]", "")
    )
    if isinstance(record, list):
        record = record[0]
    return record.get("update_time", "")


def get_source_info(record_str: str) -> str:
    """
    Extracts the source information from a record string.

    Args:
        record_str (str): The record string to extract the source information from.

    Returns:
        str: The source information extracted from the record.

    """
    record = ast.literal_eval(
        str(record_str).split("\n", maxsplit=1)[0].replace("[", "").replace("]", "")
    )
    if isinstance(record, list):
        record = record[0]
    return record.get("dataset", "")


def process_source_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Gets the source data from the given DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the source data.

    Returns:
        pd.DataFrame: The DataFrame containing the source data.
    """
    df["update_time"] = df["sources"].apply(get_update_info)
    df["update_time"] = pd.to_datetime(
        df["update_time"], format="%Y-%m-%dT%H:%M:%S.%fZ"
    )
    df["source"] = df["sources"].apply(get_source_info)
    return df.drop(columns=["sources"])


def get_name_info(record_str: str) -> str:
    """
    Extracts the source information from a record string.

    Args:
        record_str (str): The record string to extract the source information from.

    Returns:
        str: The source information extracted from the record.

    """
    record = ast.literal_eval(str(record_str))
    return record.get("primary", "")


def process_name_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes the name column in the given DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the name column.

    Returns:
        pd.DataFrame: The DataFrame with the name column processed.
    """
    df["name"] = df["names"].apply(get_name_info)
    return df.drop(columns=["names"])


def change_column_dtypes(df):
    """
    Change the data types of object columns in a DataFrame to string.

    Args:
        df (pandas.DataFrame): The DataFrame to modify.

    Returns:
        pandas.DataFrame: The modified DataFrame with object columns converted to string.

    """
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str)
    return df


@save_parquet_decorator("bronze", TRANSPORT_CONTRACTS["pois"])
def save_file(df: pd.DataFrame, **kwargs):
    """
    Saves a DataFrame to a Parquet file.


    Args:
        df (pd.DataFrame): The DataFrame to save.
        kwargs : Additional keyword arguments.
    """
    return df


def join_data(df: pd.DataFrame, df_brazil_geom: pd.DataFrame) -> pd.DataFrame:
    """
    Joins the given DataFrame `df` with the DataFrame `df_brazil_geom`
    based on spatial intersection.

    Parameters:
        df (pd.DataFrame): The DataFrame to be joined.
        df_brazil_geom (pd.DataFrame): The DataFrame containing Brazil-specific data.

    Returns:
        pd.DataFrame: The joined DataFrame.

    """
    df = gpd.sjoin(df, df_brazil_geom, how="inner", op="intersects")
    df = df.drop(columns=["index_right"])
    return df


def process_brand_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes the brand column in the given DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the brand column.

    Returns:
        pd.DataFrame: The DataFrame with the brand column processed.
    """
    df["brand"] = df["brand"].fillna("")
    return df


def get_primary_cat_info(record_str: str) -> str:
    """
    Extracts the source information from a record string.

    Args:
        record_str (str): The record string to extract the source information from.

    Returns:
        str: The source information extracted from the record.

    """
    if record_str is np.NA:
        return ""
    record = ast.literal_eval(
        str(record_str)
        .replace("dtype=object", "")
        .replace("array", "")
        .replace("\n", "")
    )
    if record is None:
        return ""
    return record.get("primary", "")


def get_secondary_cat_info(record_str: str) -> str:
    """
    Extracts the source information from a record string.

    Args:
        record_str (str): The record string to extract the source information from.

    Returns:
        str: The source information extracted from the record.

    """
    if record_str is np.NA:
        return ""
    record = ast.literal_eval(
        str(record_str)
        .replace("dtype=object", "")
        .replace("array", "")
        .replace("\n", "")
    )
    if record is None:
        return ""
    record = record.get("alternate", "")
    if isinstance(record, tuple):
        record = record[0]
    if isinstance(record, list):
        return record[0]
    return ""


def get_tertiary_cat_info(record_str: str) -> str:
    """
    Extracts the source information from a record string.

    Args:
        record_str (str): The record string to extract the source information from.

    Returns:
        str: The source information extracted from the record.

    """
    if record_str is np.NA:
        return ""
    record = ast.literal_eval(
        str(record_str)
        .replace("dtype=object", "")
        .replace("array", "")
        .replace("\n", "")
    )
    if record is None:
        return ""
    record = record.get("alternate", "")
    if isinstance(record, tuple):
        record = record[0]
    if isinstance(record, list):
        if len(record) > 1:
            return record[1]
    return ""


def process_categories_column(df: pd.DataFrame, pois_cat_map: dict) -> pd.DataFrame:
    """
    Processes the categories column in the given DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the categories column.
        pois_cat_map (dict): Dictionary mapping POIs categories.

    Returns:
        pd.DataFrame: The DataFrame with the categories column processed.
    """
    df["category_primary"] = df["categories"].apply(get_primary_cat_info)
    df["category_secondary"] = df["categories"].apply(get_secondary_cat_info)
    df["category_tertiary"] = df["categories"].apply(get_tertiary_cat_info)
    df["general_category"] = df["category_primary"].apply(pois_cat_map.__getitem__)
    return df.drop(columns=["categories"])


def process_files(files: list, df_brazil_geom: pd.DataFrame, pois_cat_map: dict):
    """
    Process the given files and perform spatial join with df_brazil_geom.

    Args:
        files (list): List of file paths to be processed.
        df_brazil_geom (DataFrame): DataFrame representing the boundaries of Brazil.
        pois_cat_map (dict): Dictionary mapping POIs categories.
    """
    for file in tqdm(files, desc="Processing files"):
        df = read_files(file)
        df = join_data(df, df_brazil_geom)
        df = (
            process_source_column(df)
            .pipe(process_name_column)
            .pipe(process_brand_column)
            .pipe(process_categories_column, pois_cat_map)
            .pipe(change_column_dtypes)
        )
        random_string = generate_random_string(10)
        kwargs = {"filename": f"{random_string}"}
        _ = save_file(df, **kwargs)


def main():
    """
    Main function for processing places in Brazil.

    This function reads the files in the specified path,
    divides the workload among the available workers,
    and processes the files using the `process_files` function.
    It also handles any exceptions that occur
    during the processing and logs them using the `write_log` function.
    """
    path = TRANSPORT_CONTRACTS["pois"]["physicalPath"].replace("processed", "tmp")
    files = os.listdir(path)
    df_brazil_geom_geom = get_brazil_geom()
    pois_cat_map = get_pois_categories_map()
    process_files(files, df_brazil_geom_geom, pois_cat_map)
    manager.update_last_run()
