"""
This script is used to download files from the ANEEL website based on ANEEL IDs.

It connects to a database, retrieves ANEEL IDs for a specific year, and then uses 
those IDs to download files from the ANEEL website.

The downloaded files are saved to a specified output path.

Usage:
    - Ensure that the necessary database connection and contract 
        configurations are set up correctly.
    - Run the script to download the files.

"""

import os
import gc
import multiprocessing
import concurrent.futures
from typing import List, Tuple
from tqdm import tqdm
import fiona
import pandas as pd
import geopandas as gpd

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.read import Reader
from src.tools.utils.config import get_contract
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.common import write_log

CONTRACT_ID = get_contract("contract_aneel_companies_id.yaml", "silver")
CONTRACT_PONNOT = get_contract("contract_aneel_companies_ponnot.yaml", "bronze")
CONTRACT_UCBT = get_contract("contract_aneel_companies_ucbt.yaml", "bronze")
CONTRACT_RAMLIG = get_contract("contract_aneel_companies_ramlig.yaml", "bronze")


def load_aneel_ids() -> pd.DataFrame:
    """Loads ANEEL IDs from a CSV file and returns them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing ANEEL IDs.

    Examples:
        >>> load_aneel_ids()
           id  year  company  title
        0   1  2021  CompanyA  TitleA
        1   2  2021  CompanyB  TitleB
        2   3  2021  CompanyC  TitleC
    """
    conn = DBConnection("silver")
    path = ".".join([CONTRACT_ID["schema"], CONTRACT_ID["tableName"]])
    df = conn.query_database(
        f"""
            SELECT * FROM {path} 
            WHERE year = '{CONTRACT_ID["queryYear"]}' 
            AND company NOT LIKE '%tab%' 
            AND title LIKE '%_V%'
        """
    )
    return df


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_PONNOT, save_db=False)
def read_aneel_ponnot(row_title: str, **kwargs) -> gpd.GeoDataFrame:
    """
    Reads ANEEL PONNOT files and returns a GeoDataFrame.

    This function reads the downloaded ANEEL PONNOT files and returns a GeoDataFrame
    containing the data.

    Parameters:
    - row_title (str): The title of the row to read.

    Returns:
    - df_ponnot (GeoDataFrame): A GeoDataFrame containing the data from the ANEEL PONNOT files.

    Example:
    >>> df = read_aneel_ponnot("example_row_title")
    >>> print(df.head())
       column1  column2  column3
    0        1        2        3
    1        4        5        6
    2        7        8        9
    """
    reader = Reader(CONTRACT_PONNOT)
    path = os.path.join(
        CONTRACT_PONNOT["physicalPath"].replace("ponnot", "zip_files"), row_title
    )
    layers = fiona.listlayers(path)
    assert "PONNOT" in layers, f"PONNOT not found in the file {path}"
    df_ponnot = reader.read_geofile(
        file_path=path,
        driver="FileGDB",
        layer="PONNOT",
    )
    return df_ponnot


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_UCBT, save_db=False)
def read_aneel_ucbt(row_title: str, **kwargs) -> gpd.GeoDataFrame:
    """
    Reads ANEEL UCBT files and save it.

    This function reads the downloaded ANEEL UCBT files.
    """
    reader = Reader(CONTRACT_UCBT)
    path = os.path.join(
        CONTRACT_UCBT["physicalPath"].replace("ucbt", "zip_files"),
        row_title,
    )
    layers = fiona.listlayers(path)
    assert "UCBT_tab" in layers, f"UCBT_tab not found in the file {path}"
    df_ucbt = reader.read_geofile(
        file_path=path,
        driver="FileGDB",
        layer="UCBT_tab",
        exclude_fields=["geometry"],
    )
    return df_ucbt


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_RAMLIG, save_db=False)
def read_aneel_ramlig(row_title: str, **kwargs) -> gpd.GeoDataFrame:
    """
    Reads ANEEL RAMLIG files and save it.

    This function reads the downloaded ANEEL RAMLIG files.
    """
    reader = Reader(CONTRACT_RAMLIG)
    path = os.path.join(
        CONTRACT_RAMLIG["physicalPath"].replace("ramlig", "zip_files"), row_title
    )
    layers = fiona.listlayers(path)
    assert "RAMLIG" in layers, f"RAMLIG not found in the file {path}"
    df_ramlig = reader.read_geofile(
        file_path=path,
        driver="FileGDB",
        layer="RAMLIG",
    )
    del reader
    del path
    gc.collect()
    return df_ramlig


def read_ponnot(row_title: str) -> None:
    """
    Reads the 'ponnot' file associated with the given row title.

    Args:
        row_title (str): The title of the row.
    """
    file_name = "_".join([row_title.split(".")[0], "ponnot"])
    exist_small_file = os.path.exists(
        os.path.join(CONTRACT_PONNOT["physicalPath"], "".join([file_name, ".parquet"]))
    )
    exist_large_file = os.path.exists(
        os.path.join(CONTRACT_PONNOT["physicalPath"], file_name)
    )
    exist_file = exist_small_file or exist_large_file
    if not exist_file:
        kwargs = {"filename": file_name}
        df_ponnot = read_aneel_ponnot(row_title, **kwargs)
        del df_ponnot
        gc.collect()


def read_ucbt(row_title: str) -> None:
    """
    Reads the UCBT data for a given row title.

    Args:
        row_title (str): The row title.
    """
    file_name = "_".join([row_title.split(".")[0], "ucbt"])
    exist_small_file = os.path.exists(
        os.path.join(CONTRACT_UCBT["physicalPath"], "".join([file_name, ".parquet"]))
    )
    exist_large_file = os.path.exists(
        os.path.join(CONTRACT_UCBT["physicalPath"], file_name)
    )
    exist_file = exist_small_file or exist_large_file
    if not exist_file:
        kwargs = {"filename": file_name}
        df_ucbt = read_aneel_ucbt(row_title, **kwargs)
        del df_ucbt
        gc.collect()


def read_ramlig(row_title: str) -> None:
    """
    Reads the RAMLIG data for a given row title.

    Args:
        row_title (str): The title of the row.

    """
    file_name = "_".join([row_title.split(".")[0], "ramlig"])
    exist_small_file = os.path.exists(
        os.path.join(CONTRACT_RAMLIG["physicalPath"], "".join([file_name, ".parquet"]))
    )
    exist_large_file = os.path.exists(
        os.path.join(CONTRACT_RAMLIG["physicalPath"], file_name)
    )
    exist_file = exist_small_file or exist_large_file
    if not exist_file:
        kwargs = {"filename": file_name}
        df_ramlig = read_aneel_ramlig(row_title, **kwargs)
        del df_ramlig
        gc.collect()


def read_aneel_company_files(row_title: str) -> None:
    """
    Reads ANEEL company files.

    This function reads the downloaded ANEEL company files.
    """
    read_ponnot(row_title)
    read_ramlig(row_title)
    read_ucbt(row_title)


def split_file_sizes(df_aneel_ids) -> Tuple[List[str], List[str]]:
    """
    Splits the file sizes into two lists based on their sizes.

    Args:
        df_aneel_ids (DataFrame): The DataFrame containing the file titles.

    Returns:
        tuple: A tuple containing two lists - large_files and small_files.
               large_files: List of file titles with sizes greater than 800MB.
               small_files: List of file titles with sizes less than 800KB.
    """
    large_files = []
    medium_files = []
    small_files = []
    split_size = 800 * 1024 * 1024
    for row_title in df_aneel_ids["title"]:
        file_path = os.path.join(
            CONTRACT_PONNOT["physicalPath"].replace("ponnot", "zip_files"), row_title
        )
        file_size = os.path.getsize(file_path)
        if file_size >= split_size:
            large_files.append(row_title)
        elif (split_size / 8) <= file_size < split_size:
            medium_files.append(row_title)
        else:
            small_files.append(row_title)
    return large_files, medium_files, small_files


def process_small_files(small_files: list) -> None:
    """
    Process small files using multiprocessing.

    Args:
        small_files (list): A list of small file paths to be processed.
    """
    write_log("Processing small files")
    num_cores = multiprocessing.cpu_count()
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = [
            executor.submit(read_aneel_company_files, row_title)
            for row_title in small_files
        ]
        concurrent.futures.wait(futures)


def proccess_medium_files(medium_files: list) -> None:
    """
    Process medium files using concurrent.futures.ProcessPoolExecutor.

    Args:
        medium_files (list): A list of medium files to be processed.
    """
    write_log("Processing medium files")
    num_cores = min(3, multiprocessing.cpu_count())
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = [
            executor.submit(read_aneel_company_files, row_title)
            for row_title in medium_files
        ]
        concurrent.futures.wait(futures)


def process_large_files(large_files) -> None:
    """
    Process a list of large files.

    Args:
        large_files (list): A list of file titles.
    """
    write_log("Processing large files")
    for row_title in tqdm(large_files):
        write_log(
            f"Reading large file: {row_title}",
        )
        read_aneel_company_files(row_title)


def main():
    """
    Main function for making dataset of ANEEL companies.

    This function loads ANEEL IDs, splits file sizes, and reads ANEEL
    company files using multiprocessing.
    It utilizes concurrent.futures.ProcessPoolExecutor to parallelize the
    file reading process.
    """
    df_aneel_ids = load_aneel_ids()
    large_files, medium_files, small_files = split_file_sizes(df_aneel_ids)
    process_small_files(small_files)
    proccess_medium_files(medium_files)
    process_large_files(large_files)
