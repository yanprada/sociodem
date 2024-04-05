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
import logging
import multiprocessing
import concurrent.futures
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

from src.tools.databases.connection import DBConnection
from src.tools.utils.read import Reader
from src.tools.utils.config import get_contract
from src.tools.utils.save import save_parquet_decorator


logging.basicConfig(level=logging.INFO)

CONTRACT_ID = get_contract("contract_aneel_companies_id.yaml", "silver")
CONTRACT_PONNOT = get_contract("contract_aneel_companies_ponnot.yaml", "bronze")
CONTRACT_UCBT = get_contract("contract_aneel_companies_ucbt.yaml", "bronze")
CONTRACT_RAMLIG = get_contract("contract_aneel_companies_ramlig.yaml", "bronze")


def load_aneel_ids() -> pd.DataFrame:
    """Loads ANEEL IDs from a CSV file and returns them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing ANEEL IDs.
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
    Reads ANEEL PONNOT files and save it.

    This function reads the downloaded ANEEL PONNOT files.
    """
    reader = Reader(CONTRACT_PONNOT)
    path = os.path.join(
        CONTRACT_PONNOT["physicalPath"].replace("ponnot", "zip_files"), row_title
    )
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
    df_ucbt = reader.read_geofile(
        file_path=path,
        driver="FileGDB",
        layer="UCBT_tab",
    )
    df_ucbt = df_ucbt.drop(columns=["geometry"])
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
    df_ramlig = reader.read_geofile(
        file_path=path,
        driver="FileGDB",
        layer="RAMLIG",
    )
    return df_ramlig


def read_aneel_company_files(row_title: str):
    """
    Reads ANEEL company files.

    This function reads the downloaded ANEEL company files.
    """
    if not os.path.exists(os.path.join(CONTRACT_PONNOT["physicalPath"], row_title)):
        kwargs = {"filename": "_".join([row_title.split(".")[0], "ponnot"])}
        df_ponnot = read_aneel_ponnot(row_title, **kwargs)
        del df_ponnot
        gc.collect()
    if not os.path.exists(os.path.join(CONTRACT_UCBT["physicalPath"], row_title)):
        kwargs = {"filename": "_".join([row_title.split(".")[0], "ucbt"])}
        df_ucbt = read_aneel_ucbt(row_title, **kwargs)
        del df_ucbt
        gc.collect()
    if not os.path.exists(os.path.join(CONTRACT_RAMLIG["physicalPath"], row_title)):
        kwargs = {"filename": "_".join([row_title.split(".")[0], "ramlig"])}
        df_ramlig = read_aneel_ramlig(row_title, **kwargs)
        del df_ramlig
        gc.collect()


def main():
    """
    Main function for making dataset for ANEEL companies.
    """
    df_aneel_ids = load_aneel_ids()
    num_cores = multiprocessing.cpu_count()
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = [
            executor.submit(read_aneel_company_files, row_title)
            for row_title in tqdm(df_aneel_ids["title"])
        ]
        concurrent.futures.wait(futures)
