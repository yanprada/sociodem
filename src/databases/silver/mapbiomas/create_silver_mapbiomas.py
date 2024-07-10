"""
This module contains functions for creating and processing MapBiomas data in the silver database.

Functions:
- save_mapbiomas_partition: Saves the MapBiomas partition DataFrame.
- save_mapbiomas: Saves the MapBiomas DataFrame.
- calculate_percentage: Calculates the percentage of each value per hex_col.
- pivot_table: Pivots the DataFrame.
- load_mapbiomas: Loads data from the bronze database.
- add_classes_mapbiomas: Transforms the 'value' column in the DataFrame to MapBiomas classes names.
- main: Retrieves data from the bronze database, processes it, and returns a DataFrame.
"""

from functools import lru_cache
import gc
import multiprocessing
import concurrent.futures
from typing import List
import pandas as pd
from tqdm import tqdm
from src.tools.databases.data_connection.connection import DBConnection
from src.tools.data_contract.mapbiomas_data_contract import get_mapbiomas_contracts
from src.tools.utils.constants import MAPBIOMAS_CLASSES
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.common import write_log

CONTRACT_BRONZE = get_mapbiomas_contracts("bronze")["grouped_by_hex_mapbiomas_2022"]
CONTRACT_SILVER = get_mapbiomas_contracts("silver")["mapbiomas_2022"]


@save_parquet_decorator("silver", CONTRACT_SILVER)
def save_mapbiomas_partition(df: pd.DataFrame, **kwargs):
    """
    Saves the MapBiomas partition DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the MapBiomas partition data.
        kwargs: Additional keyword arguments.

    Returns:
        pd.DataFrame: The input DataFrame.

    """
    return df


def save_mapbiomas(df: pd.DataFrame, large_partition: int, partition: int, batch: int):
    """
    Saves the MapBiomas DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing the MapBiomas data.
        large_partition (int): The large partition number.
        partition (int): The partition number.
        batch (int): The batch size.
    """
    large_partition = int(large_partition / int(1e8))
    partition = int(partition / batch)
    write_log("Saving data")
    kwargs = {"filename": f"mapbiomas_{large_partition}_{partition}"}
    _ = save_mapbiomas_partition(df, **kwargs)
    write_log("All data saved successfully")


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
    df["pct"] = round((df["total_count"] / total_counts_per_hex_col) * 100, 2).astype(
        "float16"
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
    return df


def pivot_table(df: pd.DataFrame):
    """
    This function pivots the DataFrame.

    Args:
        df (pandas.DataFrame): The input DataFrame containing the MapBiomas data.

    Returns:
        pandas.DataFrame: The pivoted DataFrame.
    """
    write_log("Pivoting the DataFrame")
    df = df.pivot_table(
        index="hex_col", columns="value", values="pct", aggfunc="sum"
    ).fillna(0)
    return add_missing_columns(df).reset_index()


def load_mapbiomas(conn: DBConnection, condition: str) -> pd.DataFrame:
    """
    Load data from the bronze database.

    Args:
        conn (DBConnection): The database connection object.
        condition (str): The condition to apply to the database query.

    Returns:
        pd.DataFrame: The loaded data from the bronze database.
    """
    write_log("Reading data from bronze database")
    path = ".".join([CONTRACT_BRONZE["schema"], CONTRACT_BRONZE["tableName"]])
    query = f"SELECT * FROM {path} {condition}"
    return conn.query_database(query)


def add_classes_mapbiomas(df: pd.DataFrame):
    """
    Transforms the 'value' column in the DataFrame to mapbiomas classes names.

    Args:
        df (pd.DataFrame): The DataFrame containing the 'value' column.

    Returns:
        pd.DataFrame: The DataFrame with the 'value' column transformed to mapbiomas classes names.
    """
    write_log("Transforming value to mapbiomas classes names")
    df["value"] = df["value"].apply(MAPBIOMAS_CLASSES.__getitem__)
    return df


def get_hex_ids(batch: int, i: int) -> List[str]:
    """
    Get all hex ids from the bronze database.

    Args:
        batch (int): The number of hex ids to retrieve in each batch.
        i (int): The offset value to start retrieving hex ids.

    Returns:
        List[str]: The list of all hex ids.
    """
    conn = DBConnection("bronze")
    write_log("Getting all hex ids from the bronze database")
    path = ".".join([CONTRACT_BRONZE["schema"], CONTRACT_BRONZE["tableName"]])
    query = f"SELECT DISTINCT hex_col FROM {path}"
    query_batch = f"{query} LIMIT {batch} OFFSET {i}"
    return conn.query_database(query_batch)["hex_col"].tolist()


@lru_cache()
def get_hex_len() -> int:
    """
    Retrieves the length of distinct hex values from the specified database connection.
    Returns:
        The length of distinct hex values.

    """
    conn = DBConnection("bronze")
    path = ".".join([CONTRACT_BRONZE["schema"], CONTRACT_BRONZE["tableName"]])
    return int(
        conn.query_database(f"SELECT COUNT(DISTINCT hex_col) FROM {path}")[
            "count"
        ].values[0]
    )


def process_batch(hex_ids, i, batch, external_partition):
    """
    Process a batch of hex IDs to create the silver mapbiomas.

    Args:
        hex_ids (list): List of hex IDs to process.
        i (int): Starting index of the batch.
        batch (int): Number of hex IDs to process in each batch.
        external_partition (str): External partition to save the mapbiomas.

    """
    conn = DBConnection("bronze")
    query_hex_ids = hex_ids[i : i + batch]
    condition = f"WHERE hex_col IN {tuple(query_hex_ids)}"
    df = load_mapbiomas(conn, condition)
    df = add_classes_mapbiomas(df)
    df = calculate_percentage(df)
    df = pivot_table(df)
    save_mapbiomas(df, external_partition, i, batch)
    del df
    gc.collect()


def add_pk(col: str):
    """
    Adds a primary key constraint to a column in the 'silver' database table.

    Args:
        col (str): The name of the column to add the primary key constraint to.
    """
    conn = DBConnection("silver")
    schema = CONTRACT_SILVER["schema"]
    table_name = CONTRACT_SILVER["tableName"]
    conn.add_pk_to_table(schema, table_name, col)


def main():
    """
    This function retrieves data from the "bronze" database, processes it, and returns a DataFrame.
    """
    batch = int(1e6)
    external_batch = int(1e8)
    hex_len = get_hex_len()
    for external_partition in tqdm(
        range(0, hex_len, external_batch),
        desc="Processing data in external batch",
    ):
        if external_partition == 0:
            continue
        hex_ids = get_hex_ids(external_batch, external_partition)
        num_cores = min(2, multiprocessing.cpu_count())
        with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
            futures = [
                executor.submit(process_batch, hex_ids, i, batch, external_partition)
                for i in range(0, len(hex_ids), batch)
            ]

            for future in tqdm(
                concurrent.futures.as_completed(futures),
                total=len(futures),
                desc="Processing data in batch",
            ):
                try:
                    future.result()

                except Exception as e:
                    write_log(e, "error")
        del hex_ids
        gc.collect()
    add_pk("hex_col")
