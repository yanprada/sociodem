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
from shapely import wkt
import numpy as np
import pandas as pd
import geopandas as gpd


from src.tools.managers.reader import Reader
from src.tools.utils.h3 import create_hex_col_from_dot
from src.tools.managers.saver import save_parquet_decorator
from src.tools.utils.common import generate_random_string
from src.tools.utils.constants import CRS_GLOBAL
from src.databases.bronze.pois.config import (
    CONTRACTS_BRONZE,
    CONTRACTS_RAW,
    manager,
)

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


def read_files(file: str) -> gpd.GeoDataFrame:
    """
    Reads a Parquet file and returns a DataFrame.

    Args:
        file (str): The name of the Parquet file to read.

    Returns:
        gpd.GeoDataFrame: The DataFrame containing the data read from the file.
    """
    reader = Reader()
    path = os.path.join(CONTRACTS_RAW["pois"]["physicalPath"], file)
    df = reader.read_parquet(path)
    df["geometry"] = df["geometry"].apply(wkt.loads)  # type: ignore
    df = gpd.GeoDataFrame(df, geometry="geometry", crs=CRS_GLOBAL)
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
    path = CONTRACTS_RAW["pois_categories"]["physicalPath"]
    reader = Reader()
    df = reader.read_csv(path)
    pois_map = df.set_index("category_primary").to_dict()["general_categories"]
    pois_map[""] = "None"
    return pois_map


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


@save_parquet_decorator("bronze")
def save_file(df: pd.DataFrame, **kwargs):
    """
    Saves a DataFrame to a Parquet file.


    Args:
        df (pd.DataFrame): The DataFrame to save.
        kwargs : Additional keyword arguments.
    """
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


def get_cat_info(record_str: np.ndarray, idx: int) -> str:
    """
    Extracts the primary category information from a record string.

    Args:
        record_str (np.ndarray): The record string to extract the primary category from.
        idx (int): The index of the primary category in the record string.
    Returns:
        str: The primary category extracted from the record, or empty string if not found.
    """
    if np.all(pd.isna(record_str)):
        return ""

    try:
        if isinstance(record_str, str):
            # Clean and parse the string
            cleaned_str = (
                str(record_str)
                .replace("dtype=object", "")
                .replace("array", "")
                .replace("\n", "")
            )
            record = ast.literal_eval(cleaned_str)
        elif isinstance(record_str, np.ndarray):
            record = record_str[idx] if len(record_str) > idx else ""
        else:
            # Handle other array-like objects
            record = record_str[idx] if len(record_str) > idx else ""

        return str(record) if record is not None else ""

    except (ValueError, SyntaxError, IndexError):
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
    df["category_primary_alt"] = df["category_alternate"].apply(get_cat_info, idx=0)
    df["category_secondary"] = df["category_alternate"].apply(get_cat_info, idx=1)
    df["category_tertiary"] = df["category_alternate"].apply(get_cat_info, idx=2)
    df["general_category"] = df["category_primary"].apply(pois_cat_map.__getitem__)
    return df.drop(columns=["category_alternate"])


def add_h3_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds the H3 index to the given DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to add the H3 index to.

    Returns:
        pd.DataFrame: The DataFrame with the H3 index added.
    """
    df["longitude"] = df.geometry.x
    df["latitude"] = df.geometry.y
    df = create_hex_col_from_dot(df)  # type: ignore
    return df.drop(columns=["latitude", "longitude"])


def process_files(files: list, pois_cat_map: dict):
    """
    Process the given files and perform spatial join with df_brazil_geom.

    Args:
        files (list): List of file paths to be processed.
        pois_cat_map (dict): Dictionary mapping POIs categories.
    """
    for file in tqdm(files, desc="Processing files"):
        df = read_files(file)
        df = (
            process_name_column(df)
            .pipe(process_brand_column)
            .pipe(process_categories_column, pois_cat_map)
            .pipe(add_h3_index)
            .pipe(change_column_dtypes)
        )
        random_string = generate_random_string(10)
        kwargs = {"filename": f"{random_string}", "contract": CONTRACTS_BRONZE["pois"]}
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
    path = CONTRACTS_RAW["pois"]["physicalPath"]
    files = os.listdir(path)
    pois_cat_map = get_pois_categories_map()
    process_files(files, pois_cat_map)
