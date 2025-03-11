"""
This module contains functions for processing and analyzing energy consumption data
from the ANEEL website and database. It includes functions to calculate combined scores,
retrieve and process data, test total energy consumption, check if previous modules need to
be rerun, and delete duplicated rows from the database.

Functions:
    calculate_combine_scores(df: pd.DataFrame) -> pd.DataFrame:
        Calculates the combined scores for each company based on the difference in
        total energy consumption between the website and the database.

    get_aneel_total_values_from_website() -> pd.DataFrame:
        Retrieves and processes energy consumption data from ANEEL website and database,
        merging and aggregating the data.

    test_total_energy_consumption() -> pd.DataFrame:
        Tests the total energy consumption by comparing processed data from MLflow with total
        values from the ANEEL website, identifying problematic cases.

    check_if_need_to_rerun_previous_module() -> None:
        Checks if the previous module needs to be re-run based on the number of files processed
        per company, logging an error and raising a ValueError if necessary.

    delete_duplicated() -> None:
        Deletes duplicated rows from specified tables in the database, creating new
        tables with distinct rows.

    main() -> pd.DataFrame:
        Main function to execute specific tests and checks, including testing total energy
        consumption and checking if the previous module needs to be rerun.

"""

from tqdm import tqdm
import pandas as pd
import numpy as np

from src.tools.utils.reader import Reader
from src.tools.utils.common import write_log, get_db_path
from src.tools.databases.data_connection.connection import DBConnection
from src.databases.bronze.aneel.config import (
    YEARS,
    CONTRACT_RAW_ENERGY,
    CONTRACT_BRONZE_ENERGY,
)
from src.databases.bronze.aneel.common import (
    get_df_already_processed,
    get_data_processed_from_mlflow,
    split_file_sizes,
)


def calculate_combine_scores(df: pd.DataFrame):
    """
    Calculates the combined scores for each company.

    Args:
        df (pd.DataFrame): A DataFrame containing the difference in
            total energy consumption between the website and the database.

    Returns:
        pd.DataFrame: A DataFrame containing the combined scores for each company.
    """
    df["difference"] = np.absolute(
        df["total_energy_mwh_db"] - df["total_energy_mwh_website"]
    )
    df["rate"] = np.where(
        df["total_energy_mwh_website"] == 0,
        100,
        df["total_energy_mwh_db"] / df["total_energy_mwh_website"],
    )
    diff_threshold_upper = df["difference"].quantile(0.85)
    rate_threshold_upper = df["rate"].quantile(0.85)
    diff_threshold_lower = df["difference"].quantile(0.15)
    rate_threshold_lower = df["rate"].quantile(0.15)
    worst_cases = df[
        (
            (df["difference"] >= diff_threshold_upper)
            & (df["rate"] >= rate_threshold_upper)
        )
        | (
            (df["difference"] >= diff_threshold_upper)
            & (df["rate"] <= rate_threshold_lower)
        )
        | (
            (df["difference"] <= diff_threshold_lower)
            & (df["rate"] >= rate_threshold_upper)
        )
        | (
            (df["difference"] <= diff_threshold_lower)
            & (df["rate"] <= rate_threshold_lower)
        )
    ]
    df["problem"] = np.where(
        df["empresa_map"].isin(worst_cases["empresa_map"]), "worst_cases", "normal"
    )
    return df


def get_aneel_total_values_from_website() -> pd.DataFrame:
    """
    Retrieves and processes energy consumption data from ANEEL website and database.

    This function performs the following steps:
    1. Reads energy consumption data from Excel files for specified years.
    2. Merges the data with a mapping of companies.
    3. Formats the company names with the corresponding year.
    4. Aggregates the total energy consumption by company.
    5. Queries the database for energy consumption data for the same years.
    6. Merges the website data with the database data.
    7. Calculates combined scores for the merged data.

    Returns:
        pd.DataFrame: A DataFrame containing the combined energy consumption data from
                        the website and database.
    """

    conn = DBConnection("bronze")
    reader = Reader()
    path = CONTRACT_RAW_ENERGY["raw_data"]["physicalPath"]
    dfs = []
    for year in YEARS:
        df = reader.read_excel("".join([path, f"aneel_{year}.xlsx"]))
        df["year"] = year
        dfs.append(df[["empresa", "year", "consumo_de_energia_eletrica_(mwh)"]])
    df = pd.concat(dfs).rename(
        columns={"consumo_de_energia_eletrica_(mwh)": "total_energy_mwh"}
    )
    df_map = reader.read_csv("".join([path, "map_empresas.csv"])).drop(
        columns="tem_anos"
    )
    df = pd.merge(df, df_map, on="empresa")
    df["empresa_map"] = df[["empresa_map", "year"]].apply(  # type: ignore
        lambda x: (  # type: ignore
            str(x["empresa_map"]).format(year=str(x["year"]))
            if pd.notna(x["empresa_map"])
            else None
        ),
        axis=1,
    )
    df = df.groupby("empresa_map", as_index=False).agg({"total_energy_mwh": "sum"})
    dfs = []
    for year in tqdm(YEARS, desc="Getting data from database"):
        path = get_db_path(CONTRACT_BRONZE_ENERGY[f"ucbt_{year}"])
        path_mv = f"{path}_sum_energy_per_companies"
        df_ucbt = conn.query_database(f"SELECT * FROM {path_mv}")
        if df_ucbt.empty:
            conn.create_materialized_view(
                f"""select u.company_file as empresa_map, sum(
                        u.ene_01_sum + u.ene_02_sum+
                        u.ene_03_sum + u.ene_04_sum+
                        u.ene_05_sum + u.ene_06_sum+
                        u.ene_07_sum + u.ene_08_sum+
                        u.ene_09_sum + u.ene_10_sum+
                        u.ene_11_sum + u.ene_12_sum
                    )/1000 as total_energy_mwh 
                from {path} u 
                group by u.company_file""",
                path_mv,
            )
            df_ucbt = conn.query_database(f"SELECT * FROM {path_mv}")
        else:
            conn.execute_query(f"REFRESH MATERIALIZED VIEW {path_mv}")
            df_ucbt = conn.query_database(f"SELECT * FROM {path_mv}")

        df_test = pd.merge(
            df, df_ucbt, on="empresa_map", how="outer", suffixes=("_website", "_db")
        ).fillna(0)
        df_test = calculate_combine_scores(df_test)
        dfs.append(df_test)
    return pd.concat(dfs)


def test_total_energy_consumption() -> pd.DataFrame:
    """
    Tests the total energy consumption by comparing processed data from MLflow with
    total values from the ANEEL website.
    The function performs the following steps:
    1. Retrieves and processes data from MLflow, filtering for the 'ucbt' database and
       converting the 'sum_energy' column to float and scaling it.
    2. Retrieves total energy values from the ANEEL website.
    3. Merges the two datasets on 'empresa_map' and 'company_id'.
    4. Calculates the rate of total energy from the database to the sum of energy from MLflow.
    5. Identifies companies with normal rates (between 0.9 and 1.01).
    6. Splits files into large and small sizes and identifies those with problems.
    7. Flags companies with problematic large or small files.
    8. Filters for the worst cases based on specific rate and difference criteria.
    Returns:
        pd.DataFrame: A DataFrame containing the problematic cases
                                            with specific criteria.
    """

    df_mlflow = (
        get_data_processed_from_mlflow()
        .query("database == 'ucbt'")
        .astype({"sum_energy": float})
    )
    df_mlflow["sum_energy"] = df_mlflow["sum_energy"] / 1000
    df = get_aneel_total_values_from_website()
    df_merged = pd.merge(
        df, df_mlflow, left_on="empresa_map", right_on="company_id", how="outer"
    ).fillna(0)
    df_merged["rate_db_mlflow"] = np.where(
        df_merged["sum_energy"] == 0,
        100,
        df_merged["total_energy_mwh_db"] / df_merged["sum_energy"],
    )
    mask = (df_merged["rate_db_mlflow"] >= 0.9) & (df_merged["rate_db_mlflow"] <= 1.01)
    normal = df_merged[mask].company_id.unique()
    large_files, small_files = split_file_sizes()
    large_files_with_problem = list(set(large_files).difference(set(normal)))
    small_files_with_problem = list(set(small_files).difference(set(normal)))
    df_merged["problem_db_mlflow"] = np.where(
        df_merged["company_id"].isin(large_files_with_problem), "large_files", "normal"
    )
    df_merged["problem_db_mlflow"] = np.where(
        df_merged["company_id"].isin(small_files_with_problem),
        "small_files",
        df_merged["problem_db_mlflow"],
    )
    df_problem = df_merged[df_merged["problem"] == "worst_cases"]
    mask_rate = (df_problem["rate"] <= 0.85) | (df_problem["rate"] >= 1.15)
    mask_diff = df_problem["difference"] >= 1000000
    df_problem = df_problem[mask_rate]
    df_problem = df_problem[mask_diff]
    df_merged["need_to_correct"] = np.where(
        df_merged["company_id"].isin(df_problem["company_id"]), True, False
    )
    return df_merged


def check_if_need_to_rerun_previous_module() -> None:
    """
    Checks if the previous module needs to be re-run based on the number
    of files processed per company.
    This function retrieves a DataFrame of already processed files,
    counts the number of files per company, and identifies companies with less
    than 3 files. If any such companies are found, it logs an error message
    and raises a ValueError indicating that the previous module needs to be re-run.

    Raises:
        ValueError: If there are companies with less than 3 files processed.
    """
    df = get_df_already_processed()
    files_per_company = df["company_id"].value_counts()
    wrong_companies = files_per_company[files_per_company < 3].index
    if len(wrong_companies) > 0:
        write_log(f"Companies with less than 3 files: {wrong_companies}", level="error")
        raise ValueError(
            "You need to re-run the previous module again. no need to change the code,"
            "just re-run the previous module"
        )


def delete_duplicated() -> None:
    """
    *USE JUST IF NEEDED* And first need to rename tables to "_duplicated"
    Deletes duplicated rows from specified tables in the database.
    This function connects to the "bronze" database and iterates over
    a list of years and table names.
    For each combination of year and table, it retrieves the columns
    of the table with duplicated rows,
    constructs a query to create a new table with distinct rows based on those
    columns, and executes the query.
    The tables processed are:
    - ucbt
    - ponnot
    - conj
    The function assumes the existence of tables named in the format
    "infrastructure.{table}_{year}_duplicated"
    and creates new tables named "infrastructure.{table}_{year}"
    with distinct rows.
    Note:
        - The function uses the `tqdm` library to display a progress bar for
            the years being processed.
        - The `DBConnection` class is assumed to be defined elsewhere in the codebase.
    Raises:
        Exception: If there is an issue with the database connection or query execution.
    """

    conn = DBConnection("bronze")
    for year in tqdm(YEARS):
        for table in ["ucbt", "ponnot", "conj"]:
            cols = conn.query_database(
                f"SELECT * FROM infrastructure.{table}_{year}_duplicated limit 1"
            ).columns
            cols_str = ", ".join(cols)
            conn.execute_query(
                f"""CREATE TABLE infrastructure.{table}_{year} AS
                SELECT DISTINCT ON ({cols_str})
                    *
                FROM infrastructure.{table}_{year}_duplicated;"""
            )


def main():
    """
    Main function to execute specific tests and checks.
    This function performs the following steps:
    1. Executes the `test_total_energy_consumption` function to test the total energy
        consumption.
    2. Executes the `check_if_need_to_rerun_previous_module` function to determine if the
        previous module needs to be rerun.
    """
    check_if_need_to_rerun_previous_module()
    df = test_total_energy_consumption()
    return df
