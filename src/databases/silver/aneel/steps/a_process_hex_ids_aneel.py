"""
This module contains functions to process hex IDs for ANEEL data.

The functions in this module retrieve data from the ANEEL database,
perform calculations on the data,
and group the data by hexagon.
"""

import os
from functools import lru_cache
from itertools import product
from typing import List, Set
import gc
import multiprocessing
from dask.distributed import Client, as_completed, LocalCluster
import duckdb
import pandas as pd
from shapely import wkb
from tqdm import tqdm
import ipdb


from src.tools.managers.db_connector import DBConnection
from src.tools.utils.constants import ANEEL_CLASSES
from src.tools.managers.saver import save_parquet_decorator
from src.tools.utils.common import get_db_path, write_log, trim_memory
from src.tools.utils.h3 import create_hex_col_from_dot

from src.databases.silver.aneel.config import (
    manager,
    CONTRACT_BRONZE_ENERGY,
    CONTRACT_SILVER_ENERGY,
    YEARS,
)


@lru_cache(maxsize=1)
def get_table_lenth(path: str) -> int:
    """
    Retrieves the length of a table in the database.

    Args:
        path (str): The path to the table.
    Returns:
        int: The number of rows in the table.
    """
    conn = DBConnection("bronze")
    query = f"""
    SELECT COUNT(*) as count FROM {path}
    """
    result = conn.query_database(query)
    conn.close()
    return result["count"].iloc[0]


def calculate_h3_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the H3 index for each point in the given DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame containing the points.

    Returns:
        pandas.DataFrame: The DataFrame with the H3 index calculated
            for each point.
    """
    write_log(
        "Calculating H3 index for each point in the DataFrame",
    )
    df["geometry"] = df["geometry"].apply(wkb.loads)  # type: ignore
    df["latitude"] = df["geometry"].apply(lambda point: point.y)
    df["longitude"] = df["geometry"].apply(lambda point: point.x)
    df = create_hex_col_from_dot(df)
    return df


def calculate_energy_consumption(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the energy consumption metrics for the given DataFrame.

    Parameters:
    df (pandas.DataFrame): The input DataFrame containing energy consumption data.

    Returns:
    pandas.DataFrame: The input DataFrame with additional columns for
        energy consumption metrics.
    """
    write_log("Calculating energy consumption metrics for the DataFrame")
    df_energy_sum = df.filter(regex=r"ene_0[1-9]_sum|ene_1[0-2]_sum")
    df_energy_mean = df.filter(regex=r"ene_0[1-9]_mean|ene_1[0-2]_mean")
    df_energy_median = df.filter(regex=r"ene_0[1-9]_median|ene_1[0-2]_median")
    df["energy_consumption"] = df_energy_sum.sum(axis=1)
    df["mean_energy_consumption"] = df_energy_mean.mean(axis=1)
    df["median_energy_consumption"] = df_energy_median.mean(axis=1)
    return df


def group_columns(df: pd.DataFrame, grouped_cols: List[str]) -> pd.DataFrame:
    """
    Group columns in the DataFrame based on specified columns,
    and perform aggregation operations on other columns.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        grouped_cols (List[str]): The columns to group by.

    Returns:
        pandas.DataFrame: The resulting DataFrame with grouped and aggregated columns.
    """
    write_log("Grouping columns in the DataFrame")
    con = duckdb.connect()
    con.register("df_table", df)
    query = f"""
        SELECT
            {', '.join(grouped_cols)},
            SUM(energy_consumption) AS energy_consumption,
            AVG(mean_energy_consumption) AS mean_energy_consumption,
            AVG(median_energy_consumption) AS median_energy_consumption,
            MODE(mun) AS mun,
            MODE(brr_most_frequent) AS brr_most_frequent,
            MODE(mat) AS mat,
            MODE(are_loc) AS are_loc,
            MIN(dat_con_oldest) AS dat_con_oldest,
            MAX(dat_con_latest) AS dat_con_latest,
            MODE(dat_con_most_frequent) AS dat_con_most_frequent,
            MODE(company_file) AS company_file,
        FROM df_table
        GROUP BY {', '.join(grouped_cols)}
        """
    result_df = con.execute(query).fetchdf()
    con.unregister("df_table")
    con.close()
    return result_df.set_index("hex_col")


def pivot_energy_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Pivot energy columns in the given DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame containing the energy columns.
        cols (List[str]): The list of energy columns to pivot.

    Returns:
        pandas.DataFrame: The DataFrame with energy columns pivoted.

    """
    write_log("Pivoting energy columns in the DataFrame")
    pivot_df = df.pivot(columns="clas_sub", values=cols)
    pivot_df.columns = [
        f"{col}" if isinstance(col, str) else "_".join(list(col))
        for col in pivot_df.columns
    ]
    df = group_columns(df.reset_index(), ["hex_col"])
    df = df.drop(columns=cols).join(pivot_df).fillna(0)
    return df


def add_missing_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Adds missing columns to the given DataFrame.

    Parameters:
    df (pandas.DataFrame): The DataFrame to add missing columns to.

    Returns:
    pandas.DataFrame: The DataFrame with missing columns added.
    """
    write_log("Adding missing columns to the DataFrame")
    set_values = list(set(ANEEL_CLASSES.values()))
    for col in product(cols, set_values):
        col = "_".join(col)
        if col not in df.columns:
            df[col] = 0
    return df


def group_by_hexagon(df: pd.DataFrame) -> pd.DataFrame:
    """
    Groups the given DataFrame by hexagon.

    Args:
        df (pandas.DataFrame): The DataFrame to be grouped.

    Returns:
        pandas.DataFrame: The grouped DataFrame.
    """
    write_log("Grouping DataFrame by hexagon")
    df["clas_sub"] = df["clas_sub"].apply(lambda x: ANEEL_CLASSES.get(x, "outros"))
    cols = [
        "energy_consumption",
        "mean_energy_consumption",
        "median_energy_consumption",
    ]
    df = (
        group_columns(df, ["hex_col", "clas_sub"])
        .pipe(pivot_energy_columns, cols)
        .pipe(add_missing_columns, cols)
    )
    return df


def get_data(conn: DBConnection, path: str, batch: int, i: int) -> pd.DataFrame:
    """
    Retrieves data from the specified database connection based on the given parameters.

    Args:
        conn (DBConnection): The database connection object.
        path (str): The path of the table in the database.
        batch (int): The number of rows to retrieve.
        i (int): The starting row index.

    Returns:
        pd.DataFrame: The retrieved data as a pandas DataFrame.
    """
    write_log(
        f"Retrieving data from {path} for rows {round(i/1e6,2)}"
        f"millions to {round((i + batch)/1e6,2)} millions",
    )
    rows_id = range(i, i + batch)

    query = f"""
    SELECT * FROM {path} WHERE row_id IN {tuple(rows_id)}
    """
    return conn.query_database(query)


@save_parquet_decorator("silver")
def save_data(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Save the DataFrame to the database.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        **kwargs: Additional keyword arguments.
    Returns:
        pd.DataFrame: The saved DataFrame.
    """
    write_log("Saving DataFrame to the database")
    return df


def engeneering_data(df: pd.DataFrame, start: int) -> pd.DataFrame:
    """
    Engeneering the DataFrame for saving.
    Args:
        df (pd.DataFrame): The DataFrame to be engineered.
        start (int): The starting index for the batch.
    Returns:
        pd.DataFrame: The engineered DataFrame.
    """
    write_log("Engeneering DataFrame for saving")
    df["mun"] = df["mun"].astype(str)
    df["brr_most_frequent"] = df["brr_most_frequent"].astype(str)
    df["dat_con_oldest"] = pd.to_datetime(df["dat_con_oldest"], errors="coerce")
    df["dat_con_latest"] = pd.to_datetime(df["dat_con_latest"], errors="coerce")
    df["dat_con_most_frequent"] = pd.to_datetime(
        df["dat_con_most_frequent"], errors="coerce"
    )
    df["company_file"] = df["company_file"].astype(str)
    df["are_loc"] = df["are_loc"].astype(str)
    df = df.reset_index()
    df["hex_col"] = df["hex_col"].astype(str)
    df["batch"] = start
    return df


def process_batch(start: int, batch: int, path: str, year: int) -> str:
    """
    Process a single batch of data.

    Args:
        start (int): The starting index for the batch.
        batch (int): The size of the batch.
        path (str): The database table path.
        year (int): The year of the data.

    Returns:
        str: A message indicating the completion of the batch processing.
    """
    conn = DBConnection("bronze")
    df = (
        get_data(conn, path, batch, start)
        .pipe(calculate_h3_index)
        .pipe(calculate_energy_consumption)
        .pipe(group_by_hexagon)
    )
    conn.close()
    if not df.index.is_unique:
        write_log("Duplicated hex cols")
        ipdb.set_trace()
    kwargs = {
        "filename": f"{year}_{start}",
        "contract": CONTRACT_SILVER_ENERGY[f"aneel_hexagon_{year}"],
    }
    df = engeneering_data(df, start)
    df = save_data(df, **kwargs)
    del df
    gc.collect()
    return f"Saved batch {start} to {start + batch}"


def get_batches_processed_db_data(path_db: str) -> Set[int]:
    """
    Get the data from the database.

    Args:
        path_db (str): The path to the database.

    Returns:
        List[int]: A list of batches processed.
    """
    conn = DBConnection("silver")
    query = f"""
    SELECT DISTINCT(batch) FROM {path_db}
    """
    df = conn.query_database(query)
    conn.close()
    if df.empty:
        return set()
    return set(sorted(df.loc[:, "batch"].to_list()))


def get_batches_processed_physical_data(path: str) -> Set[int]:
    """
    Get the processed data from the physical path.
    Args:
        path (str): The path to the physical data.

    Returns:
        Set[int]: A set of processed data IDs.
    """
    if not os.path.exists(path):
        return set()
    files = os.listdir(path)
    files = [f.split(".")[0] for f in files]
    files = [int(f.split("_")[1]) for f in files]
    return set(sorted(files))


def get_previous_processed_data(year: int) -> List[int]:
    """
    Get the previously processed data from the database.

    Returns:
        List[int]: A list of previously processed data IDs.
    """
    contract_aneel_silver = CONTRACT_SILVER_ENERGY[f"aneel_hexagon_{year}"]
    path_db = get_db_path(contract_aneel_silver)
    path = contract_aneel_silver["physicalPath"]
    batches_processed_in_db = get_batches_processed_db_data(path_db)
    batches_processed_in_physiscal_data = get_batches_processed_physical_data(path)

    if len(batches_processed_in_db) != len(batches_processed_in_physiscal_data):
        files_in_db_not_in_physiscal = batches_processed_in_db.difference(
            batches_processed_in_physiscal_data
        )
        write_log(f"Files in DB not in physical data: {files_in_db_not_in_physiscal}")
        files_in_physiscal_not_in_db = batches_processed_in_physiscal_data.difference(
            batches_processed_in_db
        )
        write_log(f"Files in physical data not in DB: {files_in_physiscal_not_in_db}")
        batches_processed_in_db = batches_processed_in_db.union(
            batches_processed_in_physiscal_data
        )

    return sorted(batches_processed_in_db)


def run_process_in_parallel(
    batch: int, path: str, year: int, batch_loop: List[int]
) -> None:
    """
    Run the process in parallel using Dask.
    Args:
        batch (int): The size of the batch.
        path (str): The database table path.
        year (int): The year of the data.
        batch_loop (List[int]): The list of batch indices to process.
    """
    write_log("Running process in parallel using Dask")
    n_workers = min(3, multiprocessing.cpu_count())
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1)
    client = Client(cluster)
    futures = [client.submit(process_batch, i, batch, path, year) for i in batch_loop]
    for i, future in tqdm(
        enumerate(as_completed(futures)),
        total=len(futures),
        desc="Processing batches",
    ):
        try:
            _ = future.result()  # type: ignore
            client.run(trim_memory)
        except Exception as e:
            write_log(f"Error processing batch: {e}", level="error")

    client.close()
    cluster.close()


def main() -> None:
    """
    Process the hex IDs for ANEEL data.

    This function retrieves data from the ANEEL database, performs calculations on the data,
    and groups the data by hexagon.
    """
    process_in_parallel = True
    module_name = os.path.basename(__file__).replace(".py", "")
    manager.update_status(module_name)

    for year in tqdm(YEARS, desc="Processing years"):
        contract_aneel_bronze = CONTRACT_BRONZE_ENERGY[f"aneel_join_{year}"]
        path_bronze = get_db_path(contract_aneel_bronze)
        table_length = get_table_lenth(path_bronze)

        batch = int(7e5)

        files_to_process = list(range(0, table_length, batch))
        files_already_processed = get_previous_processed_data(year)

        if files_already_processed:
            files_to_process = [
                f for f in files_to_process if f not in files_already_processed
            ]
            if not files_to_process:
                write_log(f"All files already processed for {year}")
                continue
        else:
            # Get the first batch to create the schema and table in database
            # to avoid errors when saving the first batch
            _ = process_batch(0, batch, path_bronze, year)
            files_to_process = list(range(batch, table_length, batch))
        if process_in_parallel:
            run_process_in_parallel(
                batch,
                path_bronze,
                year,
                files_to_process,
            )
        else:
            for file in files_to_process:
                _ = process_batch(file, batch, path_bronze, year)


if __name__ == "__main__":
    main()
