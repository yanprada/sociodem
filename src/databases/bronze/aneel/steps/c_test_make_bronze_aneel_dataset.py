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
from shapely import wkb
import geopandas as gpd

from src.tools.managers.reader import Reader
from src.tools.utils.common import write_log, get_db_path
from src.tools.databases.data_connection.connection import DBConnection
from src.databases.bronze.aneel.config import (
    YEARS,
    CONTRACT_RAW_ENERGY,
    CONTRACT_BRONZE_ENERGY,
    PATHS_MV,
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


def get_data_from_website():
    """
    Retrieves and processes energy consumption data from the ANEEL website.

    Returns:
        pd.DataFrame: A DataFrame containing the processed energy consumption data.
    """
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
    df["empresa_map"] = df["empresa_map"].str.replace("–", "-")
    df = personalized_empresa_map_col(df)
    df = df.groupby("empresa_map", as_index=False).agg({"total_energy_mwh": "sum"})
    return df


def append_with_database_data(
    df: pd.DataFrame, refresh_view: bool = False
) -> pd.DataFrame:
    """
    Appends data from a database to the given DataFrame.
    This function retrieves data from a database for each year specified in the YEARS
    list, processes it, and appends it to the provided DataFrame. It also handles the
    creation and refreshing of materialized views in the database.
    Parameters:
    df (pd.DataFrame): The input DataFrame to which the database data will be appended.
    refresh_view (bool): If True, refreshes the materialized view before
                                      querying the database. Default is False.
    Returns:
    pd.DataFrame: A DataFrame with the original data and the appended database data.
    """
    conn = DBConnection("bronze")
    dfs = []
    for year in tqdm(YEARS, desc="Getting data from database"):
        path = get_db_path(CONTRACT_BRONZE_ENERGY[f"ucbt_{year}"])
        path_mv = PATHS_MV["step_c"].format(path=path)
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
        if refresh_view:
            conn.execute_query(f"REFRESH MATERIALIZED VIEW {path_mv}")
            df_ucbt = conn.query_database(f"SELECT * FROM {path_mv}")
        df_ucbt["empresa_map"] = df_ucbt["empresa_map"].str.replace("–", "-")
        df_ucbt = personalized_empresa_map_col(df_ucbt)
        df_ucbt = df_ucbt.groupby("empresa_map", as_index=False).agg(
            {"total_energy_mwh": "sum"}
        )
        df_test = pd.merge(
            df, df_ucbt, on="empresa_map", how="right", suffixes=("_website", "_db")
        ).fillna(0)
        df_test = calculate_combine_scores(df_test)
        dfs.append(df_test)
    df_test = pd.concat(dfs)
    df_web_not_found = df[~df["empresa_map"].isin(df_test["empresa_map"])].rename(
        columns={"total_energy_mwh": "total_energy_mwh_website"}
    )
    df_web_not_found["total_energy_mwh_db"] = 0
    df_web_not_found["rate"] = 100
    df_web_not_found["difference"] = df_web_not_found["total_energy_mwh_website"]
    df_web_not_found["problem"] = "worst_cases"
    dfs.append(df_web_not_found)
    return pd.concat(dfs)


def personalized_empresa_map_col(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace specific values in the 'empresa_map' column of the given DataFrame with
    personalized mappings.

    This function creates a dictionary of mappings for the 'empresa_map' column,
    replacing certain values
    with new ones based on predefined rules. The mappings include:
    - Replacing "ENEL_GO - {year}-12-31.gdb.zip" with "EQUATORIAL_GO - {year}-12-31.gdb.zip"
        for years 2017 to 2021.
    - Replacing "CPFL_SUL_PAULISTA - 2017-12-31.gdb.zip" with
        "CPFL_SANTA_CRUZ - 2017-12-31.gdb.zip".
    - Replacing "CPFL_LESTE_PAULISTA - 2017-12-31.gdb.zip" with
        "CPFL_SANTA_CRUZ - 2017-12-31.gdb.zip".

    Args:
        df (pd.DataFrame): The input DataFrame containing the 'empresa_map' column to be modified.

    Returns:
        pd.DataFrame: The DataFrame with the 'empresa_map' column values replaced according to the
                        personalized mappings.
    """
    personalized_map = {
        f"ENEL_GO - {year}-12-31.gdb.zip": f"EQUATORIAL_GO - {year}-12-31.gdb.zip"
        for year in range(2017, 2022)
    }
    personalized_map["CPFL_SUL_PAULISTA - 2017-12-31.gdb.zip"] = (
        "CPFL_SANTA_CRUZ - 2017-12-31.gdb.zip"
    )
    personalized_map["CPFL_LESTE_PAULISTA - 2017-12-31.gdb.zip"] = (
        "CPFL_SANTA_CRUZ - 2017-12-31.gdb.zip"
    )
    df["empresa_map"] = df["empresa_map"].replace(personalized_map)
    return df


def append_with_mlflow_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Append data from MLflow to the given DataFrame.
    This function retrieves processed data from MLflow, filters it for the 'ucbt' database,
    converts the 'sum_energy' column to float, renames the 'company_id' column to 'empresa_map',
    and performs additional processing on the 'empresa_map' column. The 'sum_energy' values are
    converted from Wh to kWh. The processed MLflow data is then merged with the input DataFrame
    on the 'empresa_map' column using an outer join, and missing values are filled with 0.
    A new column 'rate_db_mlflow' is calculated as the ratio of 'total_energy_mwh_db'
    to 'sum_energy', with a default value of 100 when 'sum_energy' is 0.
    Args:
        df (pd.DataFrame): The input DataFrame to which MLflow data will be appended.
    Returns:
        pd.DataFrame: The merged DataFrame with additional data from MLflow and the calculated
                        'rate_db_mlflow' column.
    """

    df_mlflow = (
        get_data_processed_from_mlflow()
        .query("database == 'ucbt'")
        .astype({"sum_energy": float})
        .rename(columns={"company_id": "empresa_map"})
    )
    df_mlflow = personalized_empresa_map_col(df_mlflow)
    df_mlflow = df_mlflow.groupby("empresa_map", as_index=False).agg(
        {"sum_energy": "sum"}
    )
    df_mlflow["sum_energy"] = df_mlflow["sum_energy"] / 1000
    df_merged = pd.merge(df, df_mlflow, on="empresa_map", how="outer").fillna(0)
    df_merged["rate_db_mlflow"] = np.where(
        df_merged["sum_energy"] == 0,
        100,
        df_merged["total_energy_mwh_db"] / df_merged["sum_energy"],
    )
    return df_merged


def add_need_to_correct_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds columns to the DataFrame indicating whether there is a need to correct
    based on certain conditions.

    Parameters:
    df (pd.DataFrame): The input DataFrame containing the columns 'rate_db_mlflow',
                       'empresa_map', 'problem', 'rate', and 'difference'.

    Returns:
    pd.DataFrame: The modified DataFrame with additional columns 'problem_db_mlflow',
                  'need_to_correct', and 'year'.

    The function performs the following steps:
    1. Identifies 'empresa_map' values that are considered normal based on 'rate_db_mlflow'.
    2. Splits files into large and small sizes and identifies those with problems.
    3. Adds a 'problem_db_mlflow' column indicating whether the file is a
        large or small file with problems.
    4. Filters the DataFrame for rows with 'problem' as 'worst_cases' and specific 'rate'
        and 'difference' conditions.
    5. Adds a 'need_to_correct' column indicating whether the 'empresa_map' needs correction.
    6. Extracts the year from 'empresa_map' and adds it as a 'year' column.
    """
    mask = (df["rate_db_mlflow"] >= 0.9) & (df["rate_db_mlflow"] <= 1.01)
    normal = df[mask]["empresa_map"].unique()
    large_files, small_files = split_file_sizes()
    large_files_with_problem = list(set(large_files).difference(set(normal)))
    small_files_with_problem = list(set(small_files).difference(set(normal)))
    df["problem_db_mlflow"] = np.where(
        df["empresa_map"].isin(large_files_with_problem), "large_files", "normal"
    )
    df["problem_db_mlflow"] = np.where(
        df["empresa_map"].isin(small_files_with_problem),
        "small_files",
        df["problem_db_mlflow"],
    )
    df_problem = df[df["problem"] == "worst_cases"]
    mask_rate = (df_problem["rate"] <= 0.85) | (df_problem["rate"] >= 1.15)
    mask_diff = df_problem["difference"] >= 1000000
    df_problem = df_problem[mask_rate]
    df_problem = df_problem[mask_diff]
    df["need_to_correct"] = np.where(
        df["empresa_map"].isin(df_problem["empresa_map"]), True, False
    )
    df["year"] = df["empresa_map"].str.extract(r"(\d{4})-\d{2}-\d{2}")
    return df


def fetch_geometry_data(companies):
    """
    Fetches geometry data for a list of companies from a database.

    This function connects to a database, queries geometry data for each company
    for each year specified in the global YEARS variable, and returns a concatenated
    DataFrame of the results.

    Args:
        companies (list of str): A list of company names with their respective years
                                 in the format "company_name - year".

    Returns:
        pandas.DataFrame: A DataFrame containing the geometry data for the specified companies.
                          The DataFrame includes a 'join_col' column derived from the
                          'company_file' column.
    """
    conn = DBConnection("bronze")
    dfs = []
    for company in companies:
        company_year = company.split(" - ")[1].split("-")[0]
        for year in YEARS:
            company_str = company.replace(company_year, str(year))
            query = f"""
                    SELECT company_file, geometry 
                    FROM infrastructure.conj_{year} 
                    WHERE company_file = '{company_str}'
                """
            df_geom = conn.query_database(query)
            df_geom["join_col"] = df_geom["company_file"].str.split(" - ").str[0]
            if not df_geom.empty:
                dfs.append(df_geom)
                break
    return pd.concat(dfs, ignore_index=True)


def check_geometry_correspondence(df: pd.DataFrame) -> pd.DataFrame:
    """
    Checks the geometry correspondence for companies with mismatched energy data.

    Args:
        df (pd.DataFrame): DataFrame containing energy data with columns
                           'total_energy_mwh_website', 'total_energy_mwh_db', and 'empresa_map'.

    Returns:
        pd.DataFrame: DataFrame containing the geometry information for companies
                        with mismatched data.
    """
    df_db = df.query("total_energy_mwh_website == 0")
    df_db["join_col"] = df_db["empresa_map"].str.split(" - ").str[0]
    df_website = df.query("total_energy_mwh_db == 0")
    df_website["join_col"] = df_website["empresa_map"].str.split(" - ").str[0]

    db_not_website = df_db["empresa_map"].unique()
    website_not_db = df_website["empresa_map"].unique()

    df_db_geom = fetch_geometry_data(db_not_website)
    df_website_geom = fetch_geometry_data(website_not_db)

    df_db = pd.merge(df_db, df_db_geom, on="join_col", how="outer").drop(
        columns=["join_col", "company_file"]
    )
    df_website = pd.merge(df_website, df_website_geom, on="join_col", how="left").drop(
        columns=["join_col", "company_file"]
    )
    dfs = []
    for year in df_db["year"].unique():
        df_db_year = df_db.query(f"year == '{year}'")
        df_website_year = df_website.query(f"year == '{year}'")
        df_db_year["geometry"] = df_db_year["geometry"].apply(wkb.loads)  # type: ignore
        df_website_year["geometry"] = df_website_year["geometry"].apply(wkb.loads)  # type: ignore
        gdf_db = gpd.GeoDataFrame(df_db_year, geometry="geometry")
        gdf_website = gpd.GeoDataFrame(df_website_year, geometry="geometry")
        gdf_merged = gdf_db.sjoin(
            gdf_website,
            how="inner",
            predicate="intersects",
            rsuffix="website",
            lsuffix="db",
        )
        if not gdf_merged.empty:
            dfs.append(gdf_merged)
    if dfs:
        result = pd.concat(dfs)
        result = result[["empresa_map_db", "empresa_map_website"]].drop_duplicates()
        return result
    return pd.DataFrame()


def test_total_energy_consumption(refresh_view) -> pd.DataFrame:
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

    df = (
        get_data_from_website()
        .pipe(
            append_with_database_data,
            refresh_view=refresh_view,
        )
        .pipe(append_with_mlflow_data)
        .pipe(add_need_to_correct_column)
    )
    result = check_geometry_correspondence(df)
    if not result.empty:
        write_log(
            "Companies with geometry correspondence issues. Check result DataFrame for details.",
            level="error",
        )
    return df


def check_if_need_to_rerun_previous_module(refresh_view: bool) -> None:
    """
    Checks if the previous module needs to be re-run based on the number
    of files processed per company.
    This function retrieves a DataFrame of already processed files,
    counts the number of files per company, and identifies companies with less
    than 3 files. If any such companies are found, it logs an error message
    and raises a ValueError indicating that the previous module needs to be re-run.

    Args:
        refresh_view (bool): If True, refreshes the materialized view before
                                          querying the database. Default is False.


    Raises:
        ValueError: If there are companies with less than 3 files processed.
    """
    df = get_df_already_processed(refresh_view)
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

    Important:
            -> refresh_view (bool): set to True to refresh the materialized view before querying
                the database. It is usually set to False, but when you run the first time,
                or changed the data in the database, set to True.
                This will refresh the sum of energy per company.
    """
    refresh_view = True
    check_if_need_to_rerun_previous_module(refresh_view=refresh_view)
    df = test_total_energy_consumption(refresh_view=refresh_view)
    return df
