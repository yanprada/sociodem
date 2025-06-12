"""
This module contains functions for creating and processing MapBiomas data in the silver database.

Functions:
- save_single_file: Saves the MapBiomas partition DataFrame.
- save_mapbiomas: Saves the MapBiomas DataFrame.
- calculate_percentage: Calculates the percentage of each value per hex_col.
- pivot_table: Pivots the DataFrame.
- load_mapbiomas: Loads data from the bronze database.
- add_classes_mapbiomas: Transforms the 'value' column in the DataFrame to MapBiomas classes names.
- main: Retrieves data from the bronze database, processes it, and returns a DataFrame.m
"""

import os
import gc
import multiprocessing
from typing import List
import dask
from dask.distributed import Client, LocalCluster, as_completed
import pandas as pd
from tqdm import tqdm
from src.tools.databases.data_connection.connection import DBConnection

from src.tools.utils.constants import MAPBIOMAS_CLASSES
from src.tools.managers.saver import save_parquet_decorator
from src.tools.utils.common import write_log, get_db_path, trim_memory
from src.databases.silver.mapbiomas.config import (
    manager,
    CONTRACTS_SILVER,
    CONTRACTS_BRONZE,
    YEARS,
)


dask.config.set({"distributed.worker.memory.target": 0.8})  # type: ignore
dask.config.set({"distributed.worker.memory.spill": 0.85})  # type: ignore
dask.config.set({"distributed.worker.memory.pause": 0.9})  # type: ignore


@save_parquet_decorator("silver")
def save_single_file(df: pd.DataFrame, **kwargs):
    """
    Saves the MapBiomas partition DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the MapBiomas partition data.
        kwargs: Additional keyword arguments.

    Returns:
        pd.DataFrame: The input DataFrame.

    """
    return df


def save_mapbiomas(
    df: pd.DataFrame,
    year: int,
    external_partition: int,
    partition: int,
    minibatch: int,
):
    """
    Saves the MapBiomas DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the MapBiomas data.
        year (int): The MapBiomas year being processed.
        external_partition (int): The large partition number.
        partition (int): The partition number.
        minibatch (int): The minibatch size.
    """
    external_partition = int(external_partition / minibatch)
    partition = int(partition / minibatch)
    kwargs = {
        "filename": f"mapbiomas_{external_partition}_{partition}",
        "contract": CONTRACTS_SILVER[f"brasil_coverage_{year}"],
    }
    _ = save_single_file(df, **kwargs)


def calculate_percentage(df: pd.DataFrame):
    """
    This function calculates the percentage of each value per hex_col.

    Args:
        df (pandas.DataFrame): The input DataFrame containing the MapBiomas data.

    Returns:
        pandas.DataFrame: The processed DataFrame containing the percentage of
        each value per hex_col.
    """
    total_counts_per_hex_col = df.groupby("hex_col")["total_count"].transform("sum")
    # transformamos em int16 para economizar memória
    df["pct"] = (
        ((df["total_count"] / total_counts_per_hex_col) * 100).round().astype("int16")
    )
    return df.drop(columns="total_count")


def add_missing_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add missing columns to the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame containing the MapBiomas data.

    Returns:
        pd.DataFrame: The DataFrame with the missing columns added.
    """
    for col in MAPBIOMAS_CLASSES.values():
        if col not in df.columns:
            df[col] = 0
        df[col] = df[col].astype("int16")
    return df


def pivot_mapbiomas_table(df: pd.DataFrame):
    """
    This function pivots the DataFrame.

    Args:
        df (pandas.DataFrame): The input DataFrame containing the MapBiomas data.

    Returns:
        pandas.DataFrame: The pivoted DataFrame.
    """

    df = df.pivot_table(
        index="hex_col", columns="value", values="pct", aggfunc="sum"
    ).fillna(0)
    df = add_missing_columns(df)
    return df.reset_index()


def load_mapbiomas(condition: str, year: int) -> pd.DataFrame:
    """
    Load data from the bronze database.

    Args:
        condition (str): The condition to apply to the database query.
        year(int): Mapbiomas year.

    Returns:
        pd.DataFrame: The loaded data from the bronze database.
    """
    with DBConnection("bronze") as conn:
        contract_mapbiomas = CONTRACTS_BRONZE[f"grouped_by_hex_brasil_coverage_{year}"]
        path = get_db_path(contract_mapbiomas)
        query = f"SELECT * FROM {path} {condition}"
        df = conn.query_database(query)
    return df


def add_classes_mapbiomas(df: pd.DataFrame):
    """
    Transforms the 'value' column in the DataFrame to mapbiomas classes names.

    Args:
        df (pd.DataFrame): The DataFrame containing the 'value' column.

    Returns:
        pd.DataFrame: The DataFrame with the 'value' column transformed to mapbiomas classes names.
    """
    df["value"] = df["value"].apply(MAPBIOMAS_CLASSES.__getitem__)
    return df


def process_batch(
    hex_ids: list,
    year: int,
    i: int,
    minibatch: int,
    external_partition: int,
):
    """
    Process a batch of hex IDs to create the silver mapbiomas.

    Args:
        hex_ids (list): List of hex IDs to process.
        year (int): The MapBiomas year being processed.
        i (int): Starting index of the batch.
        minibatch (int): Number of hex IDs to process in each minibatch.
        external_partition (int): External partition to save the mapbiomas.
    """
    try:
        query_hex_ids = hex_ids[i : i + minibatch]
        contract = CONTRACTS_SILVER[f"brasil_coverage_{year}"]
        path_already_processed = get_db_path(contract)
        with DBConnection("silver") as conn:
            df_already_processed = conn.query_database(
                f"""SELECT * FROM {path_already_processed} WHERE hex_col = '{query_hex_ids[0]}' """
            )
        if df_already_processed.empty:

            condition = f"WHERE hex_col IN {tuple(query_hex_ids)}"
            df = (
                load_mapbiomas(condition, year)
                .pipe(add_classes_mapbiomas)
                .pipe(calculate_percentage)
                .pipe(pivot_mapbiomas_table)
            )

            save_mapbiomas(df, year, external_partition, i, minibatch)
            del df
            gc.collect()
        else:
            write_log(
                f"Skipping processing for {i} in year {year} "
                f"as it has already been processed."
            )
    except Exception as e:
        write_log(
            f"Error processing batch {i} in year {year}: {e}",
            "error",
        )
        raise e


def get_hex_ids(year: int, external_partition: int, batch: int) -> List[str]:
    """
    Retrieves the length of distinct hex values from the specified database connection.
    Args:
        year (int): The MapBiomas year being processed.
        external_partition (int): The external partition number.
        batch (int): The batch size.
    Returns:
        List[str]: A list of distinct hex values for the specified year.

    """
    hex_col_list = []

    row_numbers = range(external_partition, external_partition + batch)
    with DBConnection("bronze") as conn:
        contract_unique_hex = CONTRACTS_BRONZE[f"unique_hex_{year}"]
        write_log("Querying distinct hex_col values")
        path = get_db_path(contract_unique_hex)
        df = conn.query_database(
            f"""SELECT hex_col FROM {path} WHERE row_number IN {tuple(row_numbers)}"""
        )
        hex_col_list = df["hex_col"].to_list()
        del df
        gc.collect()
    write_log("Finished retrieving hex_col values")
    return hex_col_list


def create_hex_unique_ids_table(year: int) -> None:
    """
    Creates a table with distinct hex_col values for the specified year.
    Args:
        year (int): The MapBiomas year being processed.
    """
    with DBConnection("bronze") as conn:
        contract_hex = CONTRACTS_BRONZE[f"grouped_by_hex_brasil_coverage_{year}"]
        contract_unique_hex = CONTRACTS_BRONZE[f"unique_hex_{year}"]
        new_path = get_db_path(contract_unique_hex)
        write_log("Creating distinct hex_col table")
        path = get_db_path(contract_hex)
        query = f"""
                SELECT 
                    ROW_NUMBER() OVER () AS row_number,
                    hex_col
                FROM (
                    SELECT DISTINCT hex_col
                    FROM {path}
                    ) AS distinct_hexes;
            """
        conn.create_table_from_sql(query, new_path)
        conn.create_index(
            contract_unique_hex["schema"],
            contract_unique_hex["tableName"],
            ["row_number"],
        )


def get_hex_counts(year: int) -> int:
    """
    Retrieves the number of distinct hex values for the specified year.

    Args:
        year (int): The MapBiomas year being processed.

    Returns:
        int: The number of distinct hex values for the specified year.
    """
    with DBConnection("bronze") as conn:
        contract_unique_hex = CONTRACTS_BRONZE[f"grouped_by_hex_brasil_coverage_{year}"]
        schema = contract_unique_hex["schema"]
        table_name = contract_unique_hex["tableName"]
        write_log("Creating index for hex_col")
        conn.create_index(schema, table_name, ["hex_col"])
        write_log("Querying sum distinct hex_col values")
        path = get_db_path(contract_unique_hex)
        hex_count = int(
            conn.query_database(
                f"""SELECT COUNT(DISTINCT(hex_col)) as count FROM {path}"""
            )["count"].values[0]
        )
    return hex_count


def partitions_saved(year: int, external_partition: int) -> bool:
    """
    Check if the partitions are already saved in the database.

    Args:
        year (int): The MapBiomas year being processed.
        external_partition (int): The external partition number.

    Returns:
        bool: True if the partitions are already saved, False otherwise.
    """
    path_saved = CONTRACTS_SILVER[f"brasil_coverage_{year}"]["physicalPath"]
    if not os.path.exists(path_saved):
        return False
    files = [
        int(f.split("_")[1])
        for f in os.listdir(path_saved)
        if os.path.isfile(os.path.join(path_saved, f))
    ]
    return external_partition in files


def main():
    """
    This function retrieves data from the "bronze" database, processes it, and returns a DataFrame.
    """

    module_name = os.path.basename(__file__).replace(".py", "")
    manager.update_status(module_name)

    batch = int(5e6)
    minibatch = int(batch / 10)
    for year in tqdm(YEARS, desc="Processing Years"):
        if year <= 2022:
            write_log(f"Skipping year {year} as it is already processed.")
            continue
        hex_len = get_hex_counts(year)
        create_hex_unique_ids_table(year)
        # Ensure that there is the table and schema in the database
        # befor processing the data in parallel
        hex_ids = get_hex_ids(year, 0, minibatch)
        process_batch(hex_ids, year, 0, minibatch, 0)
        for external_partition in tqdm(
            range(minibatch, hex_len, batch),
            desc="Processing data in batch",
        ):
            if partitions_saved(year, int(external_partition / minibatch)):
                write_log(
                    f"{year} external partition {int(external_partition / minibatch)}"
                    "already saved."
                )
                continue
            hex_ids = get_hex_ids(year, external_partition, batch)
            num_cores = min(4, multiprocessing.cpu_count())
            cluster = LocalCluster(
                n_workers=num_cores, threads_per_worker=1, processes=True
            )
            with Client(cluster) as client:
                futures = [
                    client.submit(
                        process_batch,
                        hex_ids,
                        year,
                        i,
                        minibatch,
                        external_partition,
                    )
                    for i in range(minibatch, len(hex_ids), minibatch)
                ]

                for future in tqdm(
                    as_completed(futures),  # type: ignore
                    total=len(futures),
                    desc="Processing data in minibatch",
                ):
                    try:
                        future.result()  # type: ignore
                        trim_memory()

                    except Exception as e:
                        write_log(e, "error")
            cluster.close()
            trim_memory()
        trim_memory()
