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

import gc
import logging
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

from src.tools.utils.config import get_contract
from src.tools.databases.connection import DBConnection
from src.tools.databases.data_request.drivers.http_requester import HttpRequesterAneel
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.read import Reader

logging.basicConfig(level=logging.INFO)

CONTRACT_INPUT = get_contract("contract_aneel_companies_id.yaml", "silver")
TABLE_INPUT_PATH = ".".join([CONTRACT_INPUT["schema"], CONTRACT_INPUT["tableName"]])


CONTRACT_PONNOT = get_contract("contract_aneel_companies_ponnot.yaml", "bronze")


CONTRACT_UCBT = get_contract("contract_aneel_companies_ucbt.yaml", "bronze")


def load_aneel_ids() -> pd.DataFrame:
    """
    Loads ANEEL IDs from a CSV file and returns them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing ANEEL IDs.
    """
    conn = DBConnection("silver")
    df = conn.query_database(
        f"""
            SELECT * FROM {TABLE_INPUT_PATH} 
            WHERE year = '{CONTRACT_INPUT["queryYear"]}' 
            AND company NOT LIKE '%tab%' 
            AND title LIKE '%_V%'
        """
    )
    return df


def download_files(df: pd.DataFrame) -> None:
    """
    Downloads files from the ANEEL website.

    Args:
        df (pd.DataFrame): The DataFrame containing the ANEEL IDs.

    """
    aneel_request = HttpRequesterAneel()
    aneel_request.request_from_page(
        df["id"],
        df["title"],
        CONTRACT_PONNOT["physicalPath"],
    )


def download_aneel_company_files(df_aneel_ids: pd.DataFrame) -> None:
    """
    Downloads ANEEL company files.

    This function loads ANEEL IDs and downloads the corresponding files for each company.
    """

    download_files(df_aneel_ids)


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_PONNOT)
def read_aneel_ponnot(row_title: str, **kwargs) -> gpd.GeoDataFrame:
    """
    Reads ANEEL PONNOT files and save it.

    This function reads the downloaded ANEEL PONNOT files.
    """
    reader = Reader(CONTRACT_PONNOT)
    df_ponnot = reader.read_geofile(
        "/".join([CONTRACT_PONNOT["physicalPath"], row_title]),
        driver="FileGDB",
        layer="PONNOT",
    )
    return df_ponnot


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_UCBT)
def read_aneel_ucbt(row_title: str, **kwargs) -> gpd.GeoDataFrame:
    """
    Reads ANEEL UCBT files and save it.

    This function reads the downloaded ANEEL UCBT files.
    """
    reader = Reader(CONTRACT_UCBT)
    df_ucbt = reader.read_geofile(
        "/".join([CONTRACT_UCBT["physicalPath"], row_title]),
        driver="FileGDB",
        layer="UCBT_tab",
    )
    df_ucbt = df_ucbt.drop(columns=["geometry"])
    return df_ucbt


def read_aneel_company_files(row_title: str):
    """
    Reads ANEEL company files.

    This function reads the downloaded ANEEL company files.
    """
    kwargs = {"filename": "_".join([row_title.split(".")[0], "ponnot"])}
    df_ponnot = read_aneel_ponnot(row_title, **kwargs)
    del df_ponnot
    gc.collect()
    kwargs = {"filename": "_".join([row_title.split(".")[0], "ucbt"])}
    df_ucbt = read_aneel_ucbt(row_title, **kwargs)
    del df_ucbt
    gc.collect()


def main(download: bool = False):
    """
    Main function for making dataset for ANEEL companies.

    Args:
        download (bool): Flag indicating whether to download ANEEL company files.
    """
    df_aneel_ids = load_aneel_ids()
    if download:
        download_aneel_company_files(df_aneel_ids)
    for row_title in tqdm(df_aneel_ids["title"]):
        logging.info("Reading %s", row_title)
        read_aneel_company_files(row_title)
