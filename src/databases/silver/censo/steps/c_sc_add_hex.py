"""
This module contains functions related to the processing of census data for municipalities.

Functions:
- get_hex_from_geom(df: pd.DataFrame) -> pd.DataFrame: Get the hexagon ID from the geometry column.
- get_muns(): Get the file from the physical path.
- main(): Main function.

"""

import logging
import gc
import mlflow
from tqdm import tqdm
import pandas as pd
import geopandas as gpd
from src.tools.data_contract.censo_data_contract import get_censo_contracts
from src.tools.utils.common import get_db_path
from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.loader import Loader
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.h3 import add_h3_index_to_large_geom

logging.getLogger("distributed").setLevel(logging.WARNING)

mlflow.set_experiment("sc silver hex")

CONTRACT_SCS_CENSO_BRONZE = get_censo_contracts("bronze")
CONTRACT_SCS_CENSO_SILVER = get_censo_contracts("silver")


def get_pct_dompp_hex_sc():
    """
    Get the file from the physical path.
    """
    conn = DBConnection("silver")
    path = get_db_path(CONTRACT_SCS_CENSO_SILVER["dompp_pct_2022"])
    df = conn.query_database(f"SELECT * FROM {path}")
    return df


@save_parquet_decorator(
    "silver", CONTRACT_SCS_CENSO_SILVER["sectors_2022"], save_pq=False
)
def add_hex_from_geom(
    df: gpd.GeoDataFrame, df_pct_sc_hex: pd.DataFrame, **kwargs
) -> gpd.GeoDataFrame:
    """
    Adds a hex index column to the GeoDataFrame based on the geometry column.
    Parameters:
    - df (gpd.GeoDataFrame): The input GeoDataFrame.
    - **kwargs: Additional arguments.
    Returns:
    - gpd.GeoDataFrame: The modified GeoDataFrame with the hex index column added.
    """
    df = add_h3_index_to_large_geom(df, "cd_setor")
    df = df.merge(
        df_pct_sc_hex.rename(columns={"hex_id": "hex_col"}),
        on=["hex_col", "cd_setor"],
        how="left",
    ).fillna(0)
    return df


def main():
    """
    Main function.
    """
    df_sc = Loader().get_sc()
    df_pct_sc_hex = get_pct_dompp_hex_sc()
    batch = 10000
    date = pd.Timestamp.now().strftime("%d/%m/%Y %H:%M")
    with mlflow.start_run(run_name=date):
        for partition in tqdm(range(0, len(df_sc), batch), desc="Processing hexagons"):
            with mlflow.start_run(run_name=str(partition), nested=True):
                kwargs = {"filename": str(partition)}
                df = df_sc.iloc[partition : partition + batch]
                df = add_hex_from_geom(df, df_pct_sc_hex, **kwargs)
                mlflow.log_metric("num_sc", df["cd_setor"].nunique())
                mlflow.log_metric("num_hex", df["hex_col"].nunique())
                del df
                gc.collect()
