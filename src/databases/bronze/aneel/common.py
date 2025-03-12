"""
This module provides utility functions for handling and processing file sizes
from a specified directory. It includes functions to flatten a list of lists
and to split files into categories based on their sizes.

Functions:
    flat_list(files_dict: DefaultDict[str, List[str]]) -> List[str]:
        Flattens a dictionary of lists into a single list.

    split_file_sizes() -> Tuple[List[str], List[str], List[str]]:
        Splits files into three categories based on their sizes: large, medium, and small.

"""

import os
import glob
from itertools import product
from collections import defaultdict
from typing import List, Tuple, DefaultDict, Union
from tqdm import tqdm
import pandas as pd

from src.tools.utils.common import write_log, get_db_path, get_ml_flow_data
from src.tools.databases.data_connection.connection import DBConnection
from src.databases.bronze.aneel.config import (
    CONTRACT_RAW_ENERGY,
    CONTRACT_BRONZE_ENERGY,
    EXPERIMENT_NAME,
    YEARS,
)


def split_file_sizes(
    df_processed: Union[pd.DataFrame, None] = None,
) -> tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    """
    Splits the file sizes into two lists based on their sizes.

    Args:
        df_processed (Union[pd.DataFrame, None]): A DataFrame containing the processed
                                                    data from MLflow, or None.

    Returns:
        tuple: A tuple containing two lists - large_files and small_files.
               large_files: List of tuples with file ids and their databases for files with
                            sizes greater than or equal to 100MB.
               small_files: List of tuples with file ids and their databases for files with
                            sizes less than 100MB.
    """

    def flat_list(files_dict: DefaultDict[str, List[str]]) -> List[Tuple[str, str]]:
        """
        Flattens a list of lists.

        Args:
            files_dict (dict): A dictionary containing lists of files.

        Returns:
            list: The flattened list.
        """
        files = []
        if not files_dict:
            return files
        for _, f in files_dict.items():
            if isinstance(f, list):
                files.extend(f)
        return list(product(files, ["ponnot", "ucbt", "conj"]))  # type: ignore

    def generate_dataframe() -> pd.DataFrame:
        """
        Generates a DataFrame containing company ids from the raw data directory.

        Returns:
            pd.DataFrame: DataFrame with company ids.
        """
        return pd.DataFrame(
            {
                "company_ids": [
                    os.path.basename(f)
                    for f in glob.glob(
                        os.path.join(
                            CONTRACT_RAW_ENERGY["raw_data"]["physicalPath"], "*.gdb.zip"
                        )
                    )
                ]
            }
        )

    def categorize_files(
        df_aneel_ids: pd.DataFrame, split_size: int
    ) -> Tuple[DefaultDict[str, List[str]], DefaultDict[str, List[str]]]:
        """
        Categorizes files into large and small based on their sizes.

        Args:
            df_aneel_ids (pd.DataFrame): DataFrame containing company ids.
            split_size (int): Size threshold to categorize files.

        Returns:
            tuple: Two dictionaries containing large and small files categorized by year.
        """
        large_files = defaultdict(list)
        small_files = defaultdict(list)
        for company_id in df_aneel_ids["company_ids"]:
            if " - " not in company_id:
                continue
            year = company_id.split(" - ")[1].split("-")[0]
            file_path = os.path.join(
                CONTRACT_RAW_ENERGY["raw_data"]["physicalPath"],
                company_id,
            )
            file_size = os.path.getsize(file_path)
            if file_size >= split_size:
                large_files[year].append(company_id)
            else:
                small_files[year].append(company_id)
        return large_files, small_files

    def filter_processed_files(
        df_processed: pd.DataFrame, files: List[Tuple[str, str]]
    ) -> List[Tuple[str, str]]:
        """
        Filters out already processed files from the list.

        Args:
            df_processed (pd.DataFrame): DataFrame containing processed data.
            files (List[Tuple[str, str]]): List of tuples containing file ids and their databases.

        Returns:
            List[Tuple[str, str]]: List of tuples that are not processed yet.
        """
        df_files = pd.DataFrame(files, columns=["company_id", "database"])
        df_merged = pd.merge(
            df_files,
            df_processed,
            on=["company_id", "database"],
            how="left",
            indicator=True,
        )
        df_unprocessed = df_merged[df_merged["_merge"] == "left_only"]
        return list(
            df_unprocessed[["company_id", "database"]].itertuples(
                index=False, name=None
            )
        )

    split_size = 100 * 1024 * 1024
    df_aneel_ids = generate_dataframe()
    large_files, small_files = categorize_files(df_aneel_ids, split_size)
    large_files_flatten = flat_list(large_files)
    small_files_flatten = flat_list(small_files)

    if df_processed is not None:
        large_files_flatten = filter_processed_files(df_processed, large_files_flatten)
        small_files_flatten = filter_processed_files(df_processed, small_files_flatten)

    return large_files_flatten, small_files_flatten


def get_data_processed_from_mlflow():
    """
    Get the processed data from MLflow.
    """
    write_log("Getting processed data from MLflow")
    df_processed = get_ml_flow_data(EXPERIMENT_NAME)
    if df_processed.empty:
        return pd.DataFrame({"company_id": [], "database": []})
    df_processed = df_processed[
        (df_processed["status"] == "FINISHED")
        & (~df_processed["company_id"].isin(["ponnot", "ucbt", "conj"]))
    ]
    return df_processed[["company_id", "database", "sum_energy"]]


def get_data_processed_from_db(refresh_materialized_view=False) -> pd.DataFrame:
    """
    Retrieves and processes data from the bronze database for specified years and databases.
    This function connects to the bronze database, queries distinct company files for each
    specified year and database, and concatenates the results into a single DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the concatenated results of distinct company files
                      from the specified years and databases.
    """

    def query_or_create_view(conn: DBConnection, path: str) -> pd.DataFrame:
        """
        Queries the database for distinct company files or creates a
        materialized view if not exists.

        Args:
            conn (DBConnection): The database connection object.
            path (str): The database path.

        Returns:
            pd.DataFrame: A DataFrame containing the distinct company files.
        """
        path_mv = f"{path}_companies_already_processed"
        df = conn.query_database(f"SELECT * FROM {path_mv}")
        if df.empty:
            conn.create_materialized_view(
                f"SELECT DISTINCT(company_file) as company_id FROM {path}", path_mv
            )
            df = conn.query_database(f"SELECT * FROM {path_mv}")
        if refresh_materialized_view:
            conn.execute_query(f"REFRESH MATERIALIZED VIEW {path_mv}")
            df = conn.query_database(f"SELECT * FROM {path_mv}")
        return df

    write_log("Getting processed data from the database")
    conn = DBConnection("bronze")
    company_files = []

    for year, database in tqdm(
        product(YEARS, ["ponnot", "ucbt", "conj"]), desc="Years/database"
    ):
        path = get_db_path(CONTRACT_BRONZE_ENERGY[f"{database}_{year}"])
        df = query_or_create_view(conn, path)
        if not df.empty:
            df["database"] = database
            company_files.append(df)
    if not company_files:
        return pd.DataFrame({"company_id": [], "database": []})
    return pd.concat(company_files)


def get_df_already_processed(refresh_materialized_view=False) -> pd.DataFrame:
    """
    Get the processed data from MLflow.

    Returns:
        pd.DataFrame: A DataFrame containing the processed data from MLflow.
    """
    df_mlflow = get_data_processed_from_mlflow()
    df_db = get_data_processed_from_db()
    diff_mlflow_db = set(df_mlflow["company_id"]).difference(set(df_db["company_id"]))
    diff_db_mlflow = set(df_db["company_id"]).difference(set(df_mlflow["company_id"]))
    if diff_mlflow_db and not diff_db_mlflow:
        write_log(
            f"Mlflow has {len(diff_mlflow_db)} files that are not in the database"
        )
        return df_mlflow
    if diff_db_mlflow and not diff_mlflow_db:
        write_log(f"Database has {len(diff_db_mlflow)} files that are not in Mlflow")
        return df_db
    return df_db
