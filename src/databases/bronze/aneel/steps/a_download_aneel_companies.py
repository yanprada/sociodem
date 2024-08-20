"""
This module provides functions to load ANEEL IDs from a CSV file, 
split the tags column into multiple columns,
and filter the DataFrame by the year 2023.
"""

import os
import pandas as pd

from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.reader import Reader
from src.tools.databases.data_request.drivers.http_requester import HttpRequesterAneel
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.aneel.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
manager.update_status("running_step_1")
CONTRACTS_BRONZE = execution_parameters["data_contracts"]["bronze"]


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS_BRONZE["company_id"])
def load_aneel_ids() -> pd.DataFrame:
    """
    Loads ANEEL IDs from a CSV file and returns them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing ANEEL IDs.
    """
    reader = Reader()
    return (
        reader.read_csv(CONTRACTS_BRONZE["company_id_datalake"]["physicalPath"])
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
    path = os.path.join(CONTRACTS_BRONZE["datalake"]["physicalPath"])
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
    manager.update_status("finished_step_1")
    manager.update_last_run()
