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


MEDALLON_INPUT_CONTRACT = "silver"
CONTRACT_INPUT = get_contract("contract_aneel_companies_id.yaml")
DATABASE_INPUT_CONTRACT = CONTRACT_INPUT[MEDALLON_INPUT_CONTRACT]
TABLE_INPUT_NAME = DATABASE_INPUT_CONTRACT["tableName"]
TABLE_INPUT_SCHEMA = DATABASE_INPUT_CONTRACT["schema"]
TABLE_INPUT_PATH = ".".join([TABLE_INPUT_SCHEMA, TABLE_INPUT_NAME])
YEAR = DATABASE_INPUT_CONTRACT["queryYear"]

MEDALLON_OUTPUT_CONTRACT = "bronze"
CONTRACT_OUTPUT = get_contract("contract_aneel_companies.yaml")
DATABASE_OUTPUT_CONTRACT = CONTRACT_OUTPUT[MEDALLON_OUTPUT_CONTRACT]
PATH_OUTPUT = DATABASE_OUTPUT_CONTRACT["physicalPath"].format(
    medallon=MEDALLON_OUTPUT_CONTRACT
)


def load_aneel_ids() -> pd.DataFrame:
    """
    Loads ANEEL IDs from a CSV file and returns them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing ANEEL IDs.
    """
    conn = DBConnectionHandler(MEDALLON_INPUT_CONTRACT)
    df = conn.query_database(
        f"SELECT id, title FROM {TABLE_INPUT_PATH} WHERE year = '{YEAR}'"
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
        PATH_OUTPUT,
    )


def download_aneel_company_files():
    """
    Downloads ANEEL company files.

    This function loads ANEEL IDs and downloads the corresponding files for each company.
    """
    df_aneel_ids = load_aneel_ids()
    download_files(df_aneel_ids)
