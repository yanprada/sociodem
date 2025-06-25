"""
This module processes the brands data from the raw database and saves it to the bronze database.
It retrieves the brands, cleans them by removing unwanted characters, and formats them
for consistency.
"""

import os
import pandas as pd
from unidecode import unidecode

from src.tools.managers.db_connector import DBConnection
from src.tools.managers.saver import save_parquet_decorator
from src.tools.utils.common import get_db_path

from src.databases.bronze.pois.config import (
    CONTRACTS_BRONZE,
    manager,
)

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


@save_parquet_decorator("bronze")
def get_brands(**kwargs) -> pd.DataFrame:
    """
    Gets the brands data from the raw database and saves it to the bronze database.
    """
    path = get_db_path(CONTRACTS_BRONZE["pois"])
    query = f"""
    SELECT
        *
    FROM
        {path}
    WHERE
         brand is not null
    """
    with DBConnection("bronze") as conn:
        df = conn.query_database(query)
    df["brand"] = (
        df["brand"]
        .str.lower()
        .str.split(",")
        .str[1]
        .str.split(": ")
        .str[-1]
        .apply(unidecode)
        .str.replace("'", "")
        .str.replace('"', "")
        .str.replace("- ", "")
        .str.replace(" ", "_")
        .str.replace("__", "_")
    )
    return df


def main():
    """
    Main function to execute the script.
    """
    kwargs = {"filename": "brands", "contract": CONTRACTS_BRONZE["brands"]}
    _ = get_brands(**kwargs)
