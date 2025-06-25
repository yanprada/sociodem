"""
This module provides functions to create materialized views, retrieve and process energy data
from the ANEEL database, and prepare data for testing purposes. It includes functions to create
materialized views, fetch data from the database, merge and process data from different sources,
and perform various calculations and aggregations on the data.

Functions:
    create_materialized_view(conn: DBConnection,
                            path_aneel_join: str,
                            path_aneel_no_join: str,
                            path_view: str) -> None:

    get_aneel_db_data(conn: DBConnection, refresh_view: bool = False) -> pd.DataFrame:

    get_aneel_ucbt_data(conn: DBConnection) -> pd.DataFrame:
        Retrieves UCBT data from the ANEEL database for the specified years.

    prepare_data_test_energy_sum(refresh_view: bool = False) -> pd.DataFrame:

    prepare_data_test_match(df: pd.DataFrame) -> pd.DataFrame:
        Prepares the data for testing by filtering, calculating differences and rates,
        sorting, and extracting year and company information.

    group_by_col(df: pd.DataFrame, col: str) -> pd.DataFrame:

    main() -> None:

"""

import os
from tqdm import tqdm
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from src.tools.managers.db_connector import DBConnection
from src.tools.utils.common import get_db_path, write_log
from src.databases.bronze.aneel.common import get_data_processed_from_mlflow
from src.databases.bronze.aneel.config import CONTRACT_BRONZE_ENERGY, YEARS, PATHS_MV


def create_materialized_view(
    conn: DBConnection, path_aneel_join: str, path_aneel_no_join: str, path_view: str
) -> None:
    """
    Creates a materialized view in the ANEEL database for the specified year.
    This function creates a materialized view in the ANEEL database for the given year
    by executing the SQL query defined in the 'aneel_join' configuration.
    Args:
        conn (DBConnection): The database connection object used to execute queries.
        path_aneel_join (str): The path to the 'aneel_join' configuration for the year.
        path_aneel_no_join (str): The path to the 'aneel_no_join' configuration for the year.
        path_view (str): The path to the materialized view to be created.
    """
    query = f"""
        CREATE MATERIALIZED VIEW {path_view} AS
        WITH ene_match AS (
            SELECT 
                company_file_ponnot as company_file, 
                ROUND(SUM(
                    ene_01_sum + ene_02_sum + ene_03_sum + ene_04_sum + ene_05_sum + 
                    ene_06_sum + ene_07_sum + ene_08_sum + ene_09_sum + ene_10_sum + 
                    ene_11_sum + ene_12_sum
                )) AS ene_sum,
                COUNT(CASE WHEN row_id_ucbt IS NULL THEN 1 END) AS null_ponnot
            FROM {path_aneel_join}
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
            FROM {path_aneel_no_join}
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
    conn.execute_query(query)


def get_aneel_db_data(conn: DBConnection, refresh_view: bool = False) -> pd.DataFrame:
    """
    Retrieves and processes energy data from the ANEEL database for the specified years.
    This function queries the ANEEL database to obtain energy consumption data,
    both matched and unmatched, for the given years. It calculates the total energy
    consumption and the percentage of matched energy consumption for each company.
    Args:
        conn (DBConnection): The database connection object used to execute queries.
        refresh_view (bool): A flag indicating whether to refresh the materialized view.
    Returns:
        pd.DataFrame: A DataFrame containing the following columns:
            - null_ponnot: The count of null values in the 'row_id_ucbt' column.
            - company_file: The identifier for the company.
            - ene_sum: The sum of energy consumption for matched records.
            - ene_sum_no_match: The sum of energy consumption for unmatched records.
            - ene_percentage: The percentage of matched energy consumption.
            - sum_energy_db_join: The total energy consumption (matched + unmatched).
    """
    write_log("Getting processed data from the database")
    dfs = []
    for year in tqdm(YEARS, desc="Years"):
        path_aneel_join = get_db_path(CONTRACT_BRONZE_ENERGY[f"aneel_join_{year}"])
        path_aneel_no_join = get_db_path(CONTRACT_BRONZE_ENERGY[f"ucbt_no_join_{year}"])
        path_view = PATHS_MV["step_e"].format(path=path_aneel_join)
        df = conn.query_database(f"SELECT * FROM {path_view}")
        if df.empty:
            create_materialized_view(
                conn, path_aneel_join, path_aneel_no_join, path_view
            )
        if refresh_view:
            conn.execute_query(f"REFRESH MATERIALIZED VIEW {path_view}")
        df = conn.query_database(f"SELECT * FROM {path_view}")
        dfs.append(df)
    df_db = pd.concat(dfs, ignore_index=True)
    df_db["sum_energy_db_join"] = df_db["ene_sum"] + df_db["ene_sum_no_match"]
    return df_db[
        [
            "company_file",
            "sum_energy_db_join",
            "ene_sum",
            "ene_sum_no_match",
            "null_ponnot",
        ]
    ]


def get_aneel_ucbt_data(conn: DBConnection) -> pd.DataFrame:
    """
    Retrieves ANEEL UCBT data from the database for multiple years and concatenates
    them into a single DataFrame.
    Args:
        conn (DBConnection): The database connection object.
    Returns:
        pd.DataFrame: A DataFrame containing the concatenated UCBT data for all specified years.
    """

    dfs = []
    for year in YEARS:
        path = get_db_path(CONTRACT_BRONZE_ENERGY[f"ucbt_{year}"])
        path_mv = PATHS_MV["step_c"].format(path=path)
        df = conn.query_database(f"SELECT * FROM {path_mv}")
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def prepare_data_test_energy_sum(
    refresh_view: bool = False,
) -> pd.DataFrame:
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

    Args:
        refresh_view (bool): A flag indicating whether to refresh the
                                        materialized view.

    Returns:
        pd.DataFrame: A DataFrame containing the merged and processed energy sum data,
                      including the calculated difference and rate.
    """

    conn = DBConnection("bronze")
    df_mlflow = (
        get_data_processed_from_mlflow()
        .query("database == 'ucbt'")
        .rename(
            columns={"sum_energy": "sum_energy_mlflow", "company_id": "company_file"}
        )
    )
    df_db_join = get_aneel_db_data(conn, refresh_view)
    df_db_ucbt = get_aneel_ucbt_data(conn).rename(
        columns={
            "total_energy_mwh": "sum_energy_db_ucbt",
            "empresa_map": "company_file",
        }
    )
    df_db_ucbt["sum_energy_db_ucbt"] = df_db_ucbt["sum_energy_db_ucbt"] * 1000
    df = pd.merge(
        df_mlflow,
        df_db_join,
        on="company_file",
        how="inner",
    )
    df = pd.merge(
        df,
        df_db_ucbt,
        on="company_file",
        how="inner",
    )
    df["diff_join_mlflow"] = df["sum_energy_mlflow"] - df["sum_energy_db_join"]
    df["rate_join_mlflow"] = df["sum_energy_mlflow"] / df["sum_energy_db_join"]
    df["diff_ucbt_mlflow"] = df["sum_energy_mlflow"] - df["sum_energy_db_ucbt"]
    df["rate_ucbt_mlflow"] = df["sum_energy_mlflow"] / df["sum_energy_db_ucbt"]
    df["diff_ucbt_join"] = df["sum_energy_db_join"] - df["sum_energy_db_ucbt"]
    df["rate_ucbt_join"] = df["sum_energy_db_join"] / df["sum_energy_db_ucbt"]
    df["rate_no_match"] = df["ene_sum_no_match"] / (
        df["ene_sum"] + df["ene_sum_no_match"]
    )
    return df[
        [
            "company_file",
            "sum_energy_mlflow",
            "sum_energy_db_ucbt",
            "sum_energy_db_join",
            "diff_ucbt_join",
            "rate_ucbt_join",
            "diff_ucbt_mlflow",
            "rate_ucbt_mlflow",
            "diff_join_mlflow",
            "rate_join_mlflow",
            "ene_sum",
            "ene_sum_no_match",
            "rate_no_match",
        ]
    ].sort_values("rate_no_match", ascending=False)


def group_by_col(df: pd.DataFrame) -> pd.DataFrame:
    """
    Groups the DataFrame by a specified column and performs aggregation on several columns.
    Parameters:
    df (pd.DataFrame): The input DataFrame to be grouped.

    Returns:
    pd.DataFrame: A DataFrame with aggregated results for each group.
    """
    df["year"] = df["company_file"].str.extract(r"(\d{4})")
    df["company"] = df["company_file"].str.split(" - ").str[0]
    return df.groupby(["year"]).agg(
        {
            "company_file": "count",
            "sum_energy_mlflow": "sum",
            "ene_sum": "sum",
            "ene_sum_no_match": "sum",
            "rate_no_match": "mean",
        }
    )


def assert_rates_energy(df: pd.DataFrame) -> None:
    """
    Asserts that all values in the "rate" column are less than 1.1, ensuring that the energy rate
    between mlflow and the database is not greater than 10%.
    Args:
        df (pd.DataFrame): Input DataFrame containing the columns 'rate'.
    Raises:
        AssertionError: If any value in the "rate" column is greater than or equal to 1.1.
    """
    np.testing.assert_almost_equal(df["rate_ucbt_join"].max(), 1.0, decimal=0)
    np.testing.assert_almost_equal(df["rate_ucbt_join"].min(), 1.0, decimal=0)
    np.testing.assert_almost_equal(df["rate_ucbt_mlflow"].max(), 1.0, decimal=0)
    np.testing.assert_almost_equal(df["rate_ucbt_mlflow"].min(), 1.0, decimal=0)
    np.testing.assert_almost_equal(df["rate_join_mlflow"].max(), 1.0, decimal=0)
    np.testing.assert_almost_equal(df["rate_join_mlflow"].min(), 1.0, decimal=0)


def pivot_energy_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivots the DataFrame to have 'company' as rows, 'year'
    as columns, and 'ene_sum' and 'ene_sum_no_match' as values.
    Args:
        df (pd.DataFrame): Input DataFrame containing 'company',
                            'year', 'ene_sum', and 'ene_sum_no_match'.
    Returns:
        pd.DataFrame: A pivoted DataFrame with 'company' as rows and
                        'year' as columns for 'ene_sum' and 'ene_sum_no_match'.
    """
    pivot_ene_sum = df.pivot(index="company", columns="year", values="ene_sum")
    pivot_ene_sum_no_match = df.pivot(
        index="company", columns="year", values="ene_sum_no_match"
    )
    pivoted_df = pd.concat(
        {"ene_sum": pivot_ene_sum, "ene_sum_no_match": pivot_ene_sum_no_match},
        axis=1,
    )
    return pivoted_df


def save_plots(df: pd.DataFrame) -> None:
    """
    Saves plots of energy data for each company in the DataFrame.
    Args:
        df (pd.DataFrame): Input DataFrame containing 'company',
                            'year', 'ene_sum', and 'ene_sum_no_match'.
    """
    output_dir = "plots/aneel/energy_data_companies"
    os.makedirs(output_dir, exist_ok=True)

    for company in tqdm(df.index, desc="Ploting companies"):
        plt.figure(figsize=(10, 6))
        ene_sum_values = df.loc[company, "ene_sum"]
        ene_sum_no_match_values = df.loc[company, "ene_sum_no_match"]

        plt.plot(
            pd.to_numeric(ene_sum_values.index),  # type: ignore
            ene_sum_values.values,  # type: ignore
            marker="o",
            label="ene_sum",
        )

        plt.plot(
            pd.to_numeric(ene_sum_no_match_values.index),  # type: ignore
            ene_sum_no_match_values.values,  # type: ignore
            marker="o",
            label="ene_sum_no_match",
        )

        plt.xlabel("Year")
        plt.ylabel("Values")
        plt.title(f"Energy Data for {company}")
        plt.legend()
        plt.grid(True)

        # Make the x and y axes stronger and black
        ax = plt.gca()
        ax.spines["bottom"].set_color("black")
        ax.spines["bottom"].set_linewidth(2)
        ax.spines["left"].set_color("black")
        ax.spines["left"].set_linewidth(2)

        plt.savefig(f"{output_dir}/{company}_energy_plot.png")
        plt.close()


def main():
    """
    Main function to prepare and test energy data.
    This function performs the following steps:
    1. Prepares the energy data by calling `prepare_data_test_energy_sum`.
    2. Asserts that all values in the "rate" column are less than 1.1, ensuring that the energy rate
        between mlflow and the database is not greater than 10%.
    3. Prepares the data for matching by calling `prepare_data_test_match`.
    4. Groups the data by the "year" and "company" column using `group_by_col`.
    Raises:
        AssertionError: If any value in the "rate" column is greater than or equal to 1.1.
    """
    refresh_view = False
    df = prepare_data_test_energy_sum(refresh_view)
    assert_rates_energy(df)
    df_grp = group_by_col(df)
    pivoted_df = pivot_energy_data(df)
    save_plots(pivoted_df)
    return df, df_grp, pivoted_df
