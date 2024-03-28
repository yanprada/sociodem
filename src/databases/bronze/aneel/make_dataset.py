"""
This module provides functions to load ANEEL IDs from a CSV file, 
split the tags column into multiple columns,
and filter the DataFrame by the year 2023.
"""

import pandas as pd
from src.tools.utils.config import get_contract
from src.tools.utils.common import save_parquet_decorator

CONTRACT = get_contract("contract_aneel.yaml")
DATABASE = CONTRACT["bronze"][0]
TABLE_NAME = DATABASE["tableName"]
YEAR = DATABASE["queryYear"]


def load_aneel_ids() -> pd.DataFrame:
    """
    Loads ANEEL IDs from a CSV file and returns them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing ANEEL IDs.
    """
    columns = [col["column"] for col in DATABASE.columns]
    df = pd.read_csv(DATABASE.path, usecols=columns)
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


@save_parquet_decorator(medallon="silver", filename=TABLE_NAME)
def main() -> pd.DataFrame:
    """
    This function loads ANEEL IDs, splits tags, and returns a DataFrame filtered by the year 2023.

    Returns:
        pandas.DataFrame: A DataFrame containing ANEEL Company IDs filtered by the year 2023.
    """
    df = load_aneel_ids().pipe(split_tags).query(f"year == '{YEAR}'")
    return df
