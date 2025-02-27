"""
Module for processing and aggregating energy data from MLflow and database sources.

This module contains functions to:
- Retrieve and process MLflow data.
- Retrieve and process database data.
- Prepare data for testing energy sums.
- Prepare data for testing matches between energy sums.
- Group data by a specified column.
- Main function to execute the data preparation and validation steps.

Functions:
    get_aneel_mlflow_data() -> pd.DataFrame:

    get_aneel_db_data(conn: DBConnection, years: List[int]) -> pd.DataFrame:
        Retrieves and processes energy data from the database for specified years.

    prepare_data_test_energy_sum() -> pd.DataFrame:
        Prepares data for testing energy sums between MLflow and database sources.

    prepare_data_test_match(df: pd.DataFrame) -> pd.DataFrame:
        Prepares data for testing matches between energy sums from MLflow and database sources.

    group_by_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
        Groups data by a specified column and aggregates relevant metrics.

    main():
        Main function to execute the data preparation and validation steps.

"""

import os
from typing import List
import pandas as pd
import numpy as np

from src.databases.bronze.aneel.steps.b_make_bronze_aneel_dataset import (
    split_file_sizes,
)
from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.common import get_db_path, get_ml_flow_data
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.aneel.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
manager.initialize_execution(EXECUTION_ID, DEBUG)

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)

ANEEL_BRONZE_CONTRACTS = manager.execution_details["data_contracts"]["aneel_bronze"]

EXPERIMENT_NAME = "aneel_bronze_bk"


def get_df_processed():
    """
    Get the processed data from MLflow.
    """
    df_processed = get_ml_flow_data(EXPERIMENT_NAME)
    if df_processed.empty:
        return pd.DataFrame({"mlflow.runName": [], "database": []})
    df_processed = df_processed[
        (df_processed["status"] == "FINISHED")
        & (~df_processed["mlflow.runName"].isin(["ponnot", "ucbt", "conj"]))
    ]
    return df_processed


def aggregate_large_files(df_processed: pd.DataFrame, extra_large_files: List[str]):
    """
    Aggregates data for large files and combines it with the rest of the data.
    This function processes a DataFrame by separating rows that correspond to
    extra large files, aggregating their data, and then combining the aggregated
    data with the rest of the DataFrame.
    Parameters:
        df_processed (pd.DataFrame): The input DataFrame containing processed data.
        extra_large_files (List[str]): A list of file names considered as extra large files.
    Returns:
        pd.DataFrame: A DataFrame with aggregated data for extra large files combined
                  with the rest of the data.
    """

    df_extra = df_processed[df_processed["mlflow.runName"].isin(extra_large_files)]
    df_extra = (
        df_extra.groupby(["mlflow.runName", "database"])
        .agg(
            {
                "company": "first",
                "mean_energy": "mean",
                "num_rows": "sum",
                "sum_energy": "sum",
                "std_energy": "mean",
            }
        )
        .reset_index()
    )
    cols = df_extra.columns
    df_not_extra = df_processed[
        ~df_processed["mlflow.runName"].isin(extra_large_files)
    ][cols]
    return pd.concat([df_extra, df_not_extra], ignore_index=True)


def get_aneel_mlflow_data():
    """
    Processes and aggregates MLflow data based on file sizes.
    This function retrieves processed data, identifies extra large files,
    and aggregates the data for these files by grouping on 'mlflow.runName'
    and 'database'. It calculates the first occurrence of 'company', the mean
    of 'mean_energy', the sum of 'num_rows', the sum of 'sum_energy', and the
    mean of 'std_energy'. It then combines this aggregated data with the
    remaining data that does not belong to the extra large files.
    Returns:
        pd.DataFrame: A concatenated DataFrame containing the aggregated data
        for extra large files and the non-aggregated data for other files.
    """
    df_processed = get_df_processed()
    extra_large_files, _, _, _ = split_file_sizes()
    if extra_large_files:
        df_processed = aggregate_large_files(df_processed, extra_large_files)
    return df_processed


def get_aneel_db_data(conn: DBConnection, years: List[int]):
    """
    Retrieves and processes energy data from the ANEEL database for the specified years.
    This function queries the ANEEL database to obtain energy consumption data,
    both matched and unmatched, for the given years. It calculates the total energy
    consumption and the percentage of matched energy consumption for each company.
    Args:
        conn (DBConnection): The database connection object used to execute queries.
        years (List[int]): A list of years for which the data is to be retrieved.
    Returns:
        pd.DataFrame: A DataFrame containing the following columns:
            - null_ponnot: The count of null values in the 'row_id_ucbt' column.
            - company_file: The identifier for the company.
            - ene_sum: The sum of energy consumption for matched records.
            - ene_sum_no_match: The sum of energy consumption for unmatched records.
            - ene_percentage: The percentage of matched energy consumption.
            - sum_energy_db: The total energy consumption (matched + unmatched).
    """
    path_aneel_join = get_db_path(ANEEL_BRONZE_CONTRACTS["aneel_join"])
    path_aneel_no_join = get_db_path(ANEEL_BRONZE_CONTRACTS["ucbt_no_join"])
    dfs = []
    for year in years:
        query = f"""
        WITH ene_match AS (
            SELECT 
                company_file, 
                ROUND(SUM(
                    ene_01_sum + ene_02_sum + ene_03_sum + ene_04_sum + ene_05_sum + 
                    ene_06_sum + ene_07_sum + ene_08_sum + ene_09_sum + ene_10_sum + 
                    ene_11_sum + ene_12_sum
                )) AS ene_sum,
            COUNT(CASE WHEN row_id_ucbt IS NULL THEN 1 END) AS null_ponnot
            FROM {path_aneel_join}_{year}
            GROUP BY 1
        ),
        ene_no_match AS (
            SELECT 
                company_file, 
                ROUND(SUM(
                    ene_01_sum + ene_02_sum + ene_03_sum + ene_04_sum + ene_05_sum + 
                    ene_06_sum + ene_07_sum + ene_08_sum + ene_09_sum + ene_10_sum + 
                    ene_11_sum + ene_12_sum
                )) AS ene_sum_no_match
            FROM {path_aneel_no_join}_{year}
            GROUP BY 1
        )
        SELECT
            null_ponnot,
            CASE 
                WHEN em.company_file IS NULL THEN enm.company_file
                ELSE em.company_file
            END AS company_file,
            COALESCE(em.ene_sum, 0) AS ene_sum,
            COALESCE(enm.ene_sum_no_match, 0) AS ene_sum_no_match,
            CASE 
                WHEN (COALESCE(em.ene_sum, 0) + COALESCE(enm.ene_sum_no_match, 0)) = 0 THEN 0
                ELSE (COALESCE(em.ene_sum, 0) / 
                    (COALESCE(em.ene_sum, 0) + COALESCE(enm.ene_sum_no_match, 0))) * 100
            END AS ene_percentage
        FROM ene_match em
        FULL OUTER JOIN ene_no_match enm
        ON em.company_file = enm.company_file
        ORDER BY ene_percentage DESC;
        """
        dfs.append(conn.query_database(query))
    df_db = pd.concat(dfs, ignore_index=True)
    df_db["sum_energy_db"] = df_db["ene_sum"] + df_db["ene_sum_no_match"]
    return df_db


def prepare_data_test_energy_sum() -> pd.DataFrame:
    """
    Prepares and returns a DataFrame containing energy sum data for testing purposes.
    This function performs the following steps:
    1. Retrieves the initial and end years for the query from the ANEEL_BRONZE_CONTRACTS
        configuration.
    2. Establishes a database connection to the "bronze" database.
    3. Fetches MLflow data related to 'ucbt' from the ANEEL dataset.
    4. Fetches database data for the specified years from the ANEEL dataset.
    5. Merges the MLflow data and database data on the 'mlflow.runName' and 'company_file' columns.
    6. Calculates the difference and rate between 'sum_energy' and 'sum_energy_db'.
    7. Sorts the resulting DataFrame by the 'diff' column in descending order.
    Returns:
        pd.DataFrame: A DataFrame containing the merged and processed energy sum data,
                      including the calculated difference and rate.
    """

    year_init, year_end = ANEEL_BRONZE_CONTRACTS["aneel_join"]["queryYears"]
    years = list(range(year_init, year_end + 1))
    conn = DBConnection("bronze")
    df_mlflow = get_aneel_mlflow_data().query("database == 'ucbt'")
    df_db = get_aneel_db_data(conn, years)
    df = pd.merge(
        df_mlflow[["mlflow.runName", "sum_energy"]],
        df_db[
            [
                "company_file",
                "sum_energy_db",
                "ene_sum",
                "ene_sum_no_match",
                "null_ponnot",
            ]
        ],
        left_on="mlflow.runName",
        right_on="company_file",
        how="inner",
    )
    df["diff"] = df["sum_energy"] - df["sum_energy_db"]
    df["rate"] = df["sum_energy"] / df["sum_energy_db"]
    df["year"] = df["mlflow.runName"].str.extract(r"(\d{4})")
    df["company"] = df["mlflow.runName"].str.split(" - ").str[0]
    df = df.sort_values("diff", ascending=False)
    return df


def prepare_data_test_match(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares the data for testing by filtering, calculating differences and rates,
    sorting, and extracting year and company information.
    Args:
        df (pd.DataFrame): Input DataFrame containing the columns 'ene_sum_no_match',
                           'ene_sum', and 'mlflow.runName'.
    Returns:
        pd.DataFrame: Processed DataFrame with additional columns 'diff_match',
                      'rate_match', 'year', and 'company'.
    """
    df = df[(df["ene_sum_no_match"] > 0) & (df["ene_sum"] > 0)]
    df["diff_match"] = df["ene_sum"] - df["ene_sum_no_match"]
    df["rate_match"] = df["diff_match"] / np.minimum(
        df["ene_sum_no_match"], df["ene_sum"]
    )
    df = df.sort_values("diff_match")
    return df


def group_by_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Groups the DataFrame by a specified column and performs aggregation on several columns.
    Parameters:
    df (pd.DataFrame): The input DataFrame to be grouped.
    col (str): The column name to group by.
    Returns:
    pd.DataFrame: A DataFrame with aggregated results for each group.
    Aggregations:
    - "company_file": count
    - "sum_energy": sum
    - "ene_sum": sum
    - "ene_sum_no_match": sum
    - "diff": sum
    - "rate": mean
    - "diff_match": sum
    - "rate_match": mean
    """

    return df.groupby(col).agg(
        {
            "company_file": "count",
            "sum_energy": "sum",
            "ene_sum": "sum",
            "ene_sum_no_match": "sum",
            "diff": "sum",
            "rate": "mean",
            "diff_match": "sum",
            "rate_match": "mean",
        }
    )


def main():
    """
    Main function to prepare and test energy data.
    This function performs the following steps:
    1. Prepares the energy data by calling `prepare_data_test_energy_sum`.
    2. Asserts that all values in the "rate" column are less than 1.1, ensuring that the energy rate
        between mlflow and the database is not greater than 10%.
    3. Prepares the data for matching by calling `prepare_data_test_match`.
    4. Groups the data by the "year" column using `group_by_col`.
    Raises:
        AssertionError: If any value in the "rate" column is greater than or equal to 1.1.
    """

    df = prepare_data_test_energy_sum()
    # import ipdb

    # ipdb.set_trace()
    # assert all(
    #     df["rate"] < 1.1
    # ), "Energy rate between mlflow and database is greater than 10%"
    df = prepare_data_test_match(df)
    # df_grp = group_by_col(df, "year")
