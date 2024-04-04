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
import pandas as pd
import geopandas as gpd
from tqdm import tqdm
from src.tools.utils.config import get_contract
from src.tools.databases.connection import DBConnectionHandler
from src.tools.databases.data_request.drivers.http_requester import HttpRequesterAneel
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.read import Reader


MEDALLON_INPUT_CONTRACT = "silver"
CONTRACT_INPUT = get_contract("contract_aneel_companies_id.yaml")
DATABASE_INPUT_CONTRACT = CONTRACT_INPUT[MEDALLON_INPUT_CONTRACT]
TABLE_INPUT_PATH = ".".join(
    [DATABASE_INPUT_CONTRACT["schema"], DATABASE_INPUT_CONTRACT["tableName"]]
)

MEDALLON_OUTPUT_CONTRACT_PONNOT = "bronze"
CONTRACT_OUTPUT_PONNOT = get_contract("contract_aneel_companies_ponnot.yaml")
DATABASE_OUTPUT_CONTRACT_PONNOT = CONTRACT_OUTPUT_PONNOT[
    MEDALLON_OUTPUT_CONTRACT_PONNOT
]
PATH_OUTPUT_PONNOT = DATABASE_OUTPUT_CONTRACT_PONNOT["physicalPath"].format(
    medallon=MEDALLON_OUTPUT_CONTRACT_PONNOT
)

MEDALLON_OUTPUT_CONTRACT_UCBT = "bronze"
CONTRACT_OUTPUT_UCBT = get_contract("contract_aneel_companies_ucbt.yaml")
DATABASE_OUTPUT_CONTRACT_UCBT = CONTRACT_OUTPUT_UCBT[MEDALLON_OUTPUT_CONTRACT_UCBT]
PATH_OUTPUT_UCBT = DATABASE_OUTPUT_CONTRACT_UCBT["physicalPath"].format(
    medallon=MEDALLON_OUTPUT_CONTRACT_UCBT
)


def load_aneel_ids() -> pd.DataFrame:
    """
    Loads ANEEL IDs from a CSV file and returns them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing ANEEL IDs.
    """
    conn = DBConnectionHandler(MEDALLON_INPUT_CONTRACT)
    df = conn.query_database(
        f"""
            SELECT * FROM {TABLE_INPUT_PATH} 
            WHERE year = '{DATABASE_INPUT_CONTRACT["queryYear"]}' 
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
        PATH_OUTPUT_PONNOT,
    )


def download_aneel_company_files(df_aneel_ids: pd.DataFrame):
    """
    Downloads ANEEL company files.

    This function loads ANEEL IDs and downloads the corresponding files for each company.
    """

    download_files(df_aneel_ids)


@save_parquet_decorator(
    MEDALLON_OUTPUT_CONTRACT_PONNOT, DATABASE_OUTPUT_CONTRACT_PONNOT
)
def read_aneel_ponnot(row_title: str, **kwargs) -> gpd.GeoDataFrame:
    """
    Reads ANEEL PONNOT files and save it.

    This function reads the downloaded ANEEL PONNOT files.
    """
    reader = Reader(DATABASE_OUTPUT_CONTRACT_PONNOT)
    df_ponnot = reader.read_geofile(
        "/".join([PATH_OUTPUT_PONNOT, row_title]),
        driver="FileGDB",
        layer="PONNOT",
    )
    return df_ponnot


@save_parquet_decorator(MEDALLON_OUTPUT_CONTRACT_UCBT, DATABASE_OUTPUT_CONTRACT_UCBT)
def read_aneel_ucbt(row_title: str, **kwargs) -> gpd.GeoDataFrame:
    """
    Reads ANEEL UCBT files and save it.

    This function reads the downloaded ANEEL UCBT files.
    """
    reader = Reader(DATABASE_OUTPUT_CONTRACT_UCBT)
    df_ucbt = reader.read_geofile(
        "/".join([PATH_OUTPUT_UCBT, row_title]),
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
        read_aneel_company_files(row_title)
