"""
This module provides functions to load ANEEL IDs from a CSV file,
split the tags column into multiple columns,
and filter the DataFrame by the year 2023.
"""

import os
import pandas as pd

from src.tools.managers.reader import Reader
from src.tools.managers.http_requester import HttpRequesterAneel
from src.databases.bronze.aneel.config import (
    manager,
    CONTRACT_RAW_IDS,
    CONTRACT_RAW_ENERGY,
)


module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


def load_aneel_ids() -> pd.DataFrame:
    """
    Loads ANEEL IDs from a CSV file and returns them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing ANEEL IDs.
    """
    reader = Reader()
    return (
        reader.read_csv(CONTRACT_RAW_IDS["aneel_companies_id"]["physicalPath"])
        .rename(columns={"id": "company_ids"})
        .drop_duplicates()
    )


def download_aneel_company_files(df_aneel_ids: pd.DataFrame) -> None:
    """
    Downloads ANEEL company files.

    This function loads ANEEL IDs and downloads the corresponding files for each company.

    Args:
        df_aneel_ids (pd.DataFrame): A DataFrame containing ANEEL company IDs.

    """

    aneel_request = HttpRequesterAneel()
    path = os.path.join(CONTRACT_RAW_ENERGY["raw_data"]["physicalPath"])
    aneel_request.request_from_page(
        df_aneel_ids["company_ids"], path, df_aneel_ids["title"]
    )


def main():
    """
    This function loads ANEEL IDs, splits tags, and returns a DataFrame filtered by selected year.

    Example:
        >>> main()
               id
        0      123456789
        1      987654321
        2      456789123

    """
    df = load_aneel_ids()
    download_aneel_company_files(df)
    manager.update_last_run()
