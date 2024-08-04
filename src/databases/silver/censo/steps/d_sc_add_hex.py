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
import geopandas as gpd
from src.tools.data_contract.censo_data_contract import get_censo_contracts
from src.tools.utils.common import get_db_path
from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.h3 import add_h3_index_to_large_geom
from src.tools.utils.constants import CRS_GLOBAL

logging.getLogger("distributed").setLevel(logging.WARNING)

mlflow.set_experiment("aneel silver hex")

CONTRACT_SCS_CENSO_BRONZE = get_censo_contracts("bronze")["sectors_2022"]
CONTRACT_SCS_CENSO_SILVER = get_censo_contracts("silver")["sectors_2022"]


def get_scs():
    """
    Get the file from the physical path.
    """
    conn = DBConnection("bronze")
    path = get_db_path(CONTRACT_SCS_CENSO_BRONZE)
    df = conn.query_database(f"SELECT cd_setor, geometry FROM {path}", geo=False)
    df["geometry"] = gpd.GeoSeries.from_wkb(df["geometry"])
    df = gpd.GeoDataFrame(df, geometry="geometry", crs=CRS_GLOBAL)
    return df


@save_parquet_decorator("silver", CONTRACT_SCS_CENSO_SILVER)
def add_hex_from_geom(df: gpd.GeoDataFrame, **kwargs) -> gpd.GeoDataFrame:
    """
    Adds a hex index column to the GeoDataFrame based on the geometry column.
    Parameters:
    - df (gpd.GeoDataFrame): The input GeoDataFrame.
    - **kwargs: Additional arguments.
    Returns:
    - gpd.GeoDataFrame: The modified GeoDataFrame with the hex index column added.
    """

    return add_h3_index_to_large_geom(df, "cd_setor")


def main():
    """
    Main function.
    """
    df_sc = get_scs()
    batch = 10000
    for partition in tqdm(range(0, len(df_sc), batch), desc="Processing hexagons"):
        with mlflow.start_run(run_name=str(partition)):
            kwargs = {"filename": str(partition)}
            df = df_sc.iloc[partition : partition + batch]
            df = add_hex_from_geom(df, **kwargs)
            mlflow.log_metric("num_sc", df["cd_setor"].nunique())
            mlflow.log_metric("num_hex", df["hex_col"].nunique())
            del df
            gc.collect()
