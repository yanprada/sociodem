"""
This module provides functions to load ANEEL IDs from a CSV file, 
split the tags column into multiple columns,
and filter the DataFrame by the year 2023.
"""

import pandas as pd
from src.tools.utils.config import get_contract
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.read import Reader

DATABASE_CONTRACT = get_contract("contract_aneel_companies_id.yaml", "bronze")
TABLE_NAME = DATABASE_CONTRACT["tableName"]
YEAR = DATABASE_CONTRACT["queryYear"]


def load_aneel_ids() -> pd.DataFrame:
    """
    Loads ANEEL IDs from a CSV file and returns them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing ANEEL IDs.
    """
    columns = [col["column"] for col in DATABASE_CONTRACT.columns]
    reader = Reader(DATABASE_CONTRACT)
    df = reader.read_csv(DATABASE_CONTRACT.physicalPath, usecols=columns)
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


@save_parquet_decorator(medallon="silver", contract=DATABASE_CONTRACT)
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
    df = load_aneel_ids().pipe(split_tags).query(f"year == '{YEAR}'").drop_duplicates()
    return df
