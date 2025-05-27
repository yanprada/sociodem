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
- group_by_hex_sc(df): Groups the DataFrame by hexagon ID and species code,
    aggregating the number of points, the total DOMPP, and the maximum
    number of points.
- process_mun(mun): Process the municipality data for a given municipality code.
- main(): This is the main function that processes municipalities.
    It retrieves a list of municipalities and processes each one using
    the `process_mun` function.
"""

import os
import math
from typing import List
import mlflow
import pandas as pd
import geopandas as gpd
from shapely import Point
from tqdm import tqdm
from dask.distributed import Client, LocalCluster, as_completed

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.managers.loader import Loader

from src.tools.utils.common import get_db_path, write_log
from src.tools.utils.h3 import create_hex_col_from_dot
from src.tools.utils.constants import (
    DOMPP_CLASSES,
    CRS_GLOBAL,
)
from src.tools.managers.saver import save_parquet_decorator

from src.databases.silver.ibge.config import (
    manager,
    EXPERIMENT_NAME,
    CONTRACTS_SILVER,
    CONTRACTS_BRONZE,
)


module_name = os.path.basename(__file__).replace(".py", "")


EXPERIMENT_NAME = "_".join([EXPERIMENT_NAME, module_name])
mlflow.set_experiment(EXPERIMENT_NAME)


def create_geom_col(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Creates a new column in the DataFrame containing the geometry of each point.

    Args:
        df (DataFrame): The DataFrame containing the points.

    Returns:
        DataFrame: The DataFrame with the new column.
    """
    df["geometry"] = df.apply(
        lambda row: Point(row["longitude"], row["latitude"]), axis=1  # type: ignore
    )
    return gpd.GeoDataFrame(df, geometry="geometry", crs=CRS_GLOBAL)


def merge_with_sc(df: gpd.GeoDataFrame, df_sc: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Merges the DataFrame with the sector data.

    Args:
        df (DataFrame): The DataFrame containing the points.
        df_sc (DataFrame): The DataFrame containing the sector data.

    Returns:
        DataFrame: The merged DataFrame.
    """
    join_df = gpd.sjoin(df, df_sc, how="inner", predicate="intersects").drop(
        columns="index_right"
    )
    assert len(join_df) - len(df) < 0.01 * len(
        df
    ), "The length of the DataFrame is different from the original dompp DataFrame"
    mlflow.log_metric("num_dompp_after_join", join_df["count"].sum())
    return join_df


def group_by_hex_sc(df: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Groups the DataFrame by hexagon ID and species code,
    aggregating the number of points, the total DOMPP and
    the maximum number of points.

    Args:
        df (DataFrame): The DataFrame containing the points.

    Returns:
        DataFrame: The DataFrame grouped by hexagon ID and species code.
    """
    df = pd.DataFrame(df.drop(columns=["geometry"]))  # type: ignore
    return df.groupby(["hex_col", "cd_setor", "cod_especie"], as_index=False).agg(
        num_points=("cod_especie", "size"),
        dompp_total=("count", "sum"),
        max_points=("count", "max"),
    )


def pivot_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot the DataFrame to have the species codes as columns.

    Args:
        df (DataFrame): The DataFrame containing the points.

    Returns:
        DataFrame: The pivoted DataFrame.
    """
    df["cod_especie"] = df["cod_especie"].apply(DOMPP_CLASSES.__getitem__)
    df = df.pivot_table(
        index=["hex_col", "cd_setor"],
        columns="cod_especie",
        values=["num_points", "dompp_total", "max_points"],
        fill_value=0,
    )
    df.columns = [f"{col[0]}_{col[1]}" for col in df.columns]
    return df.reset_index()


@save_parquet_decorator("silver")
def process_mun(
    conn: DBConnection, mun: str, df_sc: gpd.GeoDataFrame, **kwargs
) -> pd.DataFrame:
    """
    Process the municipality data for a given municipality code.

    Args:
        conn (Connection): The database connection object.
        mun (str): The municipality code.
        df_sc (DataFrame): The DataFrame containing the sector data.
        **kwargs: Additional keyword arguments.
    Returns:
        DataFrame: The processed data for the municipality.
    """
    contract_dompp = CONTRACTS_BRONZE["dompp_2022"]
    path = get_db_path(contract_dompp)
    df = conn.query_database(f"SELECT * FROM {path} WHERE cod_mun = '{mun}'")
    mlflow.log_metric("num_dompp", df["count"].sum())
    df = (
        create_geom_col(df)
        .pipe(create_hex_col_from_dot)
        .pipe(merge_with_sc, df_sc)
        .pipe(group_by_hex_sc)
        .pipe(pivot_table)
    )
    return df


def process_muns(muns: List[str], df_sc: gpd.GeoDataFrame) -> None:
    """
    Process the municipality data for a given list of municipality codes.

    Args:
        muns (list): A list of municipality codes.
        df_sc (DataFrame): The DataFrame containing the sector data.
    """
    conn = DBConnection("bronze")
    for mun in tqdm(muns, desc="Processing batch"):
        with mlflow.start_run(run_name=str(mun), nested=True):
            kwargs = {
                "filename": mun,
                "contract": CONTRACTS_SILVER["dompp_per_hex_sc_2022"],
            }
            _ = process_mun(conn, mun, df_sc, **kwargs)


def main():
    """
    This is the main function that processes municipalities.
    It retrieves a list of municipalities and processes each one using the `process_mun` function.
    """
    manager.update_status(module_name)
    date = pd.Timestamp.now().strftime("%d-%m-%Y %H:%M:%S")
    manager.update_mlflow_runs(date)
    with mlflow.start_run(run_name=date):
        loader = Loader()
        cluster = LocalCluster(n_workers=8)
        client = Client(cluster)
        num_workers = len(client.scheduler_info()["workers"])
        muns = loader.get_muns_cod()
        steps = math.ceil(len(muns) / num_workers)
        df_sc = loader.get_sc_2022()
        futures = [
            client.submit(process_muns, muns[i * steps : i * steps + steps], df_sc)
            for i in range(num_workers)
        ]
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Processing municipalities"
        ):
            try:
                future.result()  # type: ignore
            except Exception as e:
                write_log(f"An error occurred: {e}")
        client.close()
