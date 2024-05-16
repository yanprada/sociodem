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
from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from src.tools.data_contract.validation_data_contract import get_validation_partitions
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.common import write_log, check_file_exists

CONTRACTS = get_aneel_contracts("bronze")
VALIDATION_PARTITIONS = get_validation_partitions()


def load_aneel_ids() -> pd.DataFrame:
    """Loads ANEEL IDs from a CSV file and returns them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing ANEEL IDs.

    Examples:
        >>> load_aneel_ids()
           id
        0   18729481353
        1   29873689872
        2   39048776183
    """
    conn = DBConnection("bronze")
    path = ".".join(
        [CONTRACTS["company_id"]["schema"], CONTRACTS["company_id"]["tableName"]]
    )
    df = conn.query_database(
        f"""
            SELECT * FROM {path} 
        """
    )
    return df


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS["ponnot"], save_db=False)
def read_aneel_ponnot(company_id: str, **kwargs) -> gpd.GeoDataFrame:
    """
    Reads ANEEL PONNOT files and returns a GeoDataFrame.

    This function reads the downloaded ANEEL PONNOT files and returns a GeoDataFrame
    containing the data.

    Parameters:
    - company_id (str): The id of the row to read.

    Returns:
    - df_ponnot (GeoDataFrame): A GeoDataFrame containing the data from the ANEEL PONNOT files.

    Example:
    >>> df = read_aneel_ponnot("example_company_id")
    >>> print(df.head())
       column1  column2  column3
    0        1        2        3
    1        4        5        6
    2        7        8        9
    """
    reader = Reader(CONTRACTS["ponnot"])
    path = os.path.join(CONTRACTS["datalake"]["physicalPath"], company_id)
    layers = fiona.listlayers(path)
    assert "PONNOT" in layers, f"PONNOT not found in the file {path}"
    df_ponnot = reader.read_geofile(
        file_path=path,
        driver="FileGDB",
        layer="PONNOT",
    )
    return df_ponnot


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS["ucbt"], save_db=False)
def read_aneel_ucbt(company_id: str, **kwargs) -> gpd.GeoDataFrame:
    """
    Reads ANEEL UCBT files and save it.

    This function reads the downloaded ANEEL UCBT files.
    """
    reader = Reader(CONTRACTS["ucbt"])
    path = os.path.join(
        CONTRACTS["datalake"]["physicalPath"],
        company_id,
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


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS["ramlig"], save_db=False)
def read_aneel_ramlig(company_id: str, **kwargs) -> gpd.GeoDataFrame:
    """
    Reads ANEEL RAMLIG files and save it.

    This function reads the downloaded ANEEL RAMLIG files.
    """
    reader = Reader(CONTRACTS["ramlig"])
    path = os.path.join(CONTRACTS["datalake"]["physicalPath"], company_id)
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


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS["conj"], save_db=False)
def read_aneel_conj(company_id: str, **kwargs) -> gpd.GeoDataFrame:
    """
    Reads ANEEL CONJ files and save it.

    This function reads the downloaded ANEEL CONJ files.
    """
    reader = Reader(CONTRACTS["conj"])
    path = os.path.join(CONTRACTS["datalake"]["physicalPath"], company_id)
    layers = fiona.listlayers(path)
    assert "CONJ" in layers, f"CONJ not found in the file {path}"
    df_conj = reader.read_geofile(
        file_path=path,
        driver="FileGDB",
        layer="CONJ",
    )
    del reader
    del path
    gc.collect()
    return df_conj


def read_conj(company_id: str) -> None:
    """
    Reads the 'conj' file associated with the given row id.

    Args:
        company_id (str): The id of the row.
    """
    file_name = company_id.split(".")[0]
    file_path = CONTRACTS["conj"]["physicalPath"]
    exist_file = check_file_exists(file_name, file_path)
    if not exist_file:
        kwargs = {"filename": file_name}
        df_conj = read_aneel_conj(company_id, **kwargs)
        del df_conj
        gc.collect()


def read_ponnot(company_id: str) -> None:
    """
    Reads the 'ponnot' file associated with the given row id.

    Args:
        company_id (str): The id of the row.
    """
    file_name = company_id.split(".")[0]
    file_path = CONTRACTS["ponnot"]["physicalPath"]
    exist_file = check_file_exists(file_name, file_path)
    if not exist_file:
        kwargs = {"filename": file_name}
        df_ponnot = read_aneel_ponnot(company_id, **kwargs)
        del df_ponnot
        gc.collect()


def read_ucbt(company_id: str) -> None:
    """
    Reads the UCBT data for a given row id.

    Args:
        company_id (str): The row id.
    """
    file_name = company_id.split(".")[0]
    file_path = CONTRACTS["ucbt"]["physicalPath"]
    exist_file = check_file_exists(file_name, file_path)
    if not exist_file:
        kwargs = {"filename": file_name}
        df_ucbt = read_aneel_ucbt(company_id, **kwargs)
        del df_ucbt
        gc.collect()


def read_ramlig(company_id: str) -> None:
    """
    Reads the RAMLIG data for a given row id.

    Args:
        company_id (str): The id of the row.

    """
    file_name = company_id.split(".")[0]
    file_path = CONTRACTS["ramlig"]["physicalPath"]
    exist_file = check_file_exists(file_name, file_path)
    if not exist_file:
        kwargs = {"filename": file_name}
        df_ramlig = read_aneel_ramlig(company_id, **kwargs)
        del df_ramlig
        gc.collect()


def read_aneel_company_files(company_id: str) -> None:
    """
    Reads ANEEL company files.

    This function reads the downloaded ANEEL company files.
    """
    read_ponnot(company_id)
    read_ramlig(company_id)
    read_ucbt(company_id)
    read_conj(company_id)


def split_file_sizes(df_aneel_ids) -> Tuple[List[str], List[str]]:
    """
    Splits the file sizes into two lists based on their sizes.

    Args:
        df_aneel_ids (DataFrame): The DataFrame containing the file ids.

    Returns:
        tuple: A tuple containing two lists - large_files and small_files.
               large_files: List of file ids with sizes greater than 800MB.
               small_files: List of file ids with sizes less than 800KB.
    """
    large_files = []
    medium_files = []
    small_files = []
    split_size = 800 * 1024 * 1024
    for company_id in df_aneel_ids["company_ids"]:
        company_id = "".join([company_id, ".gdb.zip"])
        file_path = os.path.join(
            CONTRACTS["datalake"]["physicalPath"],
            company_id,
        )
        file_size = os.path.getsize(file_path)
        if file_size >= split_size:
            large_files.append(company_id)
        elif (split_size / 8) <= file_size < split_size:
            medium_files.append(company_id)
        else:
            small_files.append(company_id)
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
            executor.submit(read_aneel_company_files, company_id)
            for company_id in tqdm(small_files, desc="Processing small files")
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
            executor.submit(read_aneel_company_files, company_id)
            for company_id in tqdm(medium_files, desc="Processing medium files")
        ]
        concurrent.futures.wait(futures)


def process_large_files(large_files) -> None:
    """
    Process a list of large files.

    Args:
        large_files (list): A list of file ids.
    """
    for company_id in tqdm(large_files, desc="Processing large files"):
        write_log(
            f"Reading large file: {company_id}",
        )
        read_aneel_company_files(company_id)


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
    gc.collect()
    proccess_medium_files(medium_files)
    gc.collect()
    process_large_files(large_files)
    gc.collect()
