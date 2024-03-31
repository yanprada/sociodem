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

import pandas as pd
from src.tools.utils.config import get_contract
from src.tools.databases.connection import DBConnectionHandler
from src.tools.databases.data_request.drivers.http_requester import HttpRequesterAneel

# from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.read import Reader


MEDALLON_INPUT_CONTRACT = "silver"
CONTRACT_INPUT = get_contract("contract_aneel_companies_id.yaml")
DATABASE_INPUT_CONTRACT = CONTRACT_INPUT[MEDALLON_INPUT_CONTRACT]
TABLE_INPUT_NAME = DATABASE_INPUT_CONTRACT["tableName"]
TABLE_INPUT_SCHEMA = DATABASE_INPUT_CONTRACT["schema"]
TABLE_INPUT_PATH = ".".join([TABLE_INPUT_SCHEMA, TABLE_INPUT_NAME])
YEAR = DATABASE_INPUT_CONTRACT["queryYear"]

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
            WHERE year = '{YEAR}' 
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


# @save_parquet_decorator(MEDALLON_OUTPUT_CONTRACT, DATABASE_OUTPUT_CONTRACT)
def read_aneel_company_files(row: pd.DataFrame):
    """
    Reads ANEEL company files.

    This function reads the downloaded ANEEL company files.
    """
    reader = Reader(DATABASE_OUTPUT_CONTRACT_PONNOT)
    df_ponnot = reader.read_geofile(
        "/".join([PATH_OUTPUT_PONNOT, row["title"].squeeze()]),
        driver="FileGDB",
        layer="PONNOT",
    )
    reader = Reader(DATABASE_OUTPUT_CONTRACT_UCBT)
    df_ucbt = reader.read_geofile(
        "/".join([PATH_OUTPUT_UCBT, row["title"].squeeze()]),
        driver="FileGDB",
        layer="UCBT_tab",
    )
    return df_ponnot, df_ucbt


def main(download: bool = False):
    """
    Main function for making dataset for ANEEL companies.

    Args:
        download (bool): Flag indicating whether to download ANEEL company files.
    """
    df_aneel_ids = load_aneel_ids()
    if download:
        download_aneel_company_files(df_aneel_ids)
    # df_ponnot = read_aneel_company_files(df_aneel_ids)
