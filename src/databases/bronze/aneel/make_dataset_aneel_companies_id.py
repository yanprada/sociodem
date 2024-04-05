"""
This module provides functions to load ANEEL IDs from a CSV file, 
split the tags column into multiple columns,
and filter the DataFrame by the year 2023.
"""

import os
import pandas as pd
from src.tools.utils.config import get_contract
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.read import Reader
from src.tools.databases.data_request.drivers.http_requester import HttpRequesterAneel

CONTRACT_ID = get_contract("contract_aneel_companies_id.yaml", "bronze")
CONTRACT_PONNOT = get_contract("contract_aneel_companies_ponnot.yaml", "bronze")


def load_aneel_ids() -> pd.DataFrame:
    """
    Loads ANEEL IDs from a CSV file and returns them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing ANEEL IDs.
    """
    columns = [col["column"] for col in CONTRACT_ID.columns]
    reader = Reader(CONTRACT_ID)
    df = reader.read_csv(CONTRACT_ID.physicalPath, usecols=columns)
    return df


def split_tags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Splits the tags column into multiple columns.

    Args:
        df (pd.DataFrame): The DataFrame to split.

    Returns:
        pd.DataFrame: The DataFrame with the tags column split.
    """
    tags = (
        df["tags"].str.split(",", expand=True).rename(columns={0: "company", 1: "year"})
    )
    df = pd.concat([df, tags], axis=1)
    df = df.drop(columns=["tags", 2])
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
        os.path.join(CONTRACT_PONNOT["physicalPath"], "zip_files"),
    )


def download_aneel_company_files(df_aneel_ids: pd.DataFrame) -> None:
    """
    Downloads ANEEL company files.

    This function loads ANEEL IDs and downloads the corresponding files for each company.
    """

    download_files(df_aneel_ids)


@save_parquet_decorator(medallon="silver", contract=CONTRACT_ID)
def main() -> pd.DataFrame:
    """
    This function loads ANEEL IDs, splits tags, and returns a DataFrame filtered by selected year.

    Example:
        >>> main()
               id         title                            company    year
        0      123456789  SSDAT_2022-12-31.gdb.zip         SSDAT      2023
        1      987654321  PONNOT_2022-12-31.gdb.zip        PONNOT     2023
        2      456789123  COORSEL_7016-12-31_V11.gdb.zip   COORSEL    2023

    Returns:
        pandas.DataFrame: A DataFrame containing ANEEL Company IDs filtered by selected year.

    """
    df = (
        load_aneel_ids()
        .pipe(split_tags)
        .query(f"year == '{CONTRACT_ID.queryYear}'")
        .drop_duplicates()
    )
    download_aneel_company_files(df)
    return df
