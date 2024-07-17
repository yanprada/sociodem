"""
This module provides functions to load ANEEL IDs from a CSV file, 
split the tags column into multiple columns,
and filter the DataFrame by the year 2023.
"""

import os
import pandas as pd
from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.read import Reader
from src.tools.databases.data_request.drivers.http_requester import HttpRequesterAneel

CONTRACTS_BRONZE = get_aneel_contracts("bronze")


def load_aneel_ids() -> pd.DataFrame:
    """
    Loads ANEEL IDs from a CSV file and returns them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing ANEEL IDs.
    """
    reader = Reader()
    df = reader.read_csv(CONTRACTS_BRONZE["company_id"]["physicalPath"])
    return df.drop_duplicates()


def download_aneel_company_files(df_aneel_ids: pd.DataFrame) -> None:
    """
    Downloads ANEEL company files.

    This function loads ANEEL IDs and downloads the corresponding files for each company.

    Args:
        df_aneel_ids (pd.DataFrame): A DataFrame containing ANEEL company IDs.

    """

    aneel_request = HttpRequesterAneel()
    path = os.path.join(CONTRACTS_BRONZE["datalake"]["physicalPath"])
    aneel_request.request_from_page(
        df_aneel_ids["company_ids"],
        path,
    )


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS_BRONZE["company_id"])
def main() -> pd.DataFrame:
    """
    This function loads ANEEL IDs, splits tags, and returns a DataFrame filtered by selected year.

    Example:
        >>> main()
               id
        0      123456789
        1      987654321
        2      456789123

    Returns:
        pandas.DataFrame: A DataFrame containing ANEEL Company IDs filtered by selected year.

    """
    df = load_aneel_ids()
    download_aneel_company_files(df)
    return df
