"""
This module contains functions for processing municipality data and 
adding hexagon IDs and geometry information to the data.

Functions:
- get_muns(): Retrieves a list of distinct municipality codes from the 
    specified database table.
- create_hex_col(df): Creates a new column in the DataFrame containing 
    the hexagon ID of each point.
- create_geom_col(df): Creates a new column in the DataFrame containing
    the geometry of each point.
- group_by_hex(df): Groups the DataFrame by hexagon ID and species code, 
    aggregating the number of points, the total DOMPP, and the maximum 
    number of points.
- process_mun(mun): Process the municipality data for a given municipality code.
- main(): This is the main function that processes municipalities. 
    It retrieves a list of municipalities and processes each one using 
    the `process_mun` function.
"""

from functools import lru_cache
import math
import h3
from shapely import Point
from tqdm import tqdm
from dask.distributed import Client, LocalCluster, as_completed

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.data_contract.censo_data_contract import get_censo_contracts
from src.tools.utils.common import get_db_path, write_log
from src.tools.utils.constants import HEX_RESOLUTION, DOMPP_CLASSES
from src.tools.utils.save import save_parquet_decorator

CONTRACT_CENSO_BRONZE = get_censo_contracts("bronze")
CONTRACT_CENSO_SILVER = get_censo_contracts("silver")


@lru_cache(maxsize=1)
def get_muns():
    """
    Retrieves a list of distinct municipality codes from the specified database table.

    Returns:
        list: A list of distinct municipality codes.
    """
    contract_dompp = CONTRACT_CENSO_BRONZE["dompp_2022"]
    path = get_db_path(contract_dompp)
    conn = DBConnection("bronze")
    query = f"SELECT DISTINCT cod_mun FROM {path}"
    return conn.query_database(query)["cod_mun"].tolist()


def create_hex_col(df):
    """
    Creates a new column in the DataFrame containing the hexagon ID of each point.

    Args:
        df (DataFrame): The DataFrame containing the points.

    Returns:
        DataFrame: The DataFrame with the new column.
    """
    df["hex_id"] = df.apply(
        lambda row: h3.geo_to_h3(row["latitude"], row["longitude"], HEX_RESOLUTION),
        axis=1,
    )
    return df


def create_geom_col(df):
    """
    Creates a new column in the DataFrame containing the geometry of each point.

    Args:
        df (DataFrame): The DataFrame containing the points.

    Returns:
        DataFrame: The DataFrame with the new column.
    """
    df["geometry"] = df.apply(
        lambda row: Point(row["longitude"], row["latitude"]), axis=1
    )
    return df


def group_by_hex(df):
    """
    Groups the DataFrame by hexagon ID and species code,
    aggregating the number of points, the total DOMPP and
    the maximum number of points.

    Args:
        df (DataFrame): The DataFrame containing the points.

    Returns:
        DataFrame: The DataFrame grouped by hexagon ID and species code.
    """
    return df.groupby(["hex_id", "cod_especie"], as_index=False).agg(
        num_points=("cod_especie", "size"),
        dompp_total=("count", "sum"),
        max_points=("count", "max"),
    )


def pivot_table(df):
    """
    Pivot the DataFrame to have the species codes as columns.

    Args:
        df (DataFrame): The DataFrame containing the points.

    Returns:
        DataFrame: The pivoted DataFrame.
    """
    df["cod_especie"] = df["cod_especie"].apply(DOMPP_CLASSES.__getitem__)
    df = df.pivot_table(
        index="hex_id",
        columns="cod_especie",
        values=["num_points", "dompp_total", "max_points"],
        fill_value=0,
    )
    df.columns = [f"{col[0]}_{col[1]}" for col in df.columns]
    return df.reset_index()


@save_parquet_decorator("silver", CONTRACT_CENSO_SILVER["dompp_2022"], save_pq=False)
def process_mun(conn, mun):
    """
    Process the municipality data for a given municipality code.

    Args:
        conn (Connection): The database connection object.
        mun (str): The municipality code.

    Returns:
        DataFrame: The processed data for the municipality.
    """
    contract_dompp = CONTRACT_CENSO_BRONZE["dompp_2022"]
    path = get_db_path(contract_dompp)
    df = (
        conn.query_database(f"SELECT * FROM {path} WHERE cod_mun = '{mun}'")
        .pipe(create_geom_col)
        .pipe(create_hex_col)
        .pipe(group_by_hex)
        .pipe(pivot_table)
    )
    return df


def process_muns(muns):
    """
    Process the municipality data for a given list of municipality codes.

    Args:
        muns (list): A list of municipality codes.
    """
    conn = DBConnection("bronze")
    for mun in tqdm(muns, desc="Processing batch"):
        _ = process_mun(conn, mun)


def main():
    """
    This is the main function that processes municipalities.
    It retrieves a list of municipalities and processes each one using the `process_mun` function.
    """
    cluster = LocalCluster(n_workers=10)
    client = Client(cluster)
    num_workers = len(client.scheduler_info()["workers"])
    muns = get_muns()
    steps = math.ceil(len(muns) / num_workers)
    futures = [
        client.submit(process_muns, muns[i * steps : i * steps + steps])
        for i in range(num_workers)
    ]
    for future in tqdm(
        as_completed(futures), total=len(futures), desc="Processing municipalities"
    ):
        try:
            future.result()
        except Exception as e:
            write_log(f"An error occurred: {e}")
