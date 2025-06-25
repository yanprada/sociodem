"""
This module contains functions for creating and processing Places data in the silver database.
"""

import os
from typing import List
import geopandas as gpd
import pandas as pd

from src.tools.managers.loader import Loader
from src.tools.managers.saver import save_parquet_decorator
from src.tools.managers.db_connector import DBConnection
from src.tools.utils.common import get_db_path
from src.tools.utils.constants import CRS_GLOBAL
from src.tools.utils.h3 import get_h3_geom

from src.databases.silver.pois.config import (
    CONTRACTS_SILVER,
    CONTRACTS_CENSO_SILVER,
    manager,
)

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


def group_pois_per_cols(df_pois: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Groups the pois data per cols.

    Args:
        df_pois (pd.DataFrame): The pois data.
        cols (List[str]): The columns to group by.
    Returns:
        pd.DataFrame: The pois data grouped per cols.
    """
    return (
        df_pois.groupby(cols + ["general_category"])
        .size()
        .reset_index(name="pois_count")
    )


def pivot_pois(df_pois: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Pivots the pois data.

    Args:
        df_pois (pd.DataFrame): The pois data.
        cols (List[str]): The columns to pivot by.
    Returns:
        pd.DataFrame: The pivoted pois data.
    """
    return pd.pivot_table(
        df_pois,
        index=cols,
        columns=["general_category"],
        values="pois_count",
    ).fillna(0)


def calculate_stats(df_pois: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the statistics of the pois data.

    Args:
        df_pois (pd.DataFrame): The pois data.

    Returns:
        pd.DataFrame: The pois data with statistics.
    """
    df_pois["total_pois"] = df_pois.sum(axis=1)
    df_pois["total_pois"] = df_pois["total_pois"].fillna(0)
    return df_pois


def get_hex_mun_map(hex_ids: List[str]) -> pd.DataFrame:
    """
    Queries the municipalities data for the given hexagon IDs.

    Args:
        hex_ids (List[str]): The list of hexagon IDs.

    Returns:
        pd.DataFrame: The municipalities data.
    """
    path = get_db_path(CONTRACTS_CENSO_SILVER["hex_unique_sc_2022_sc_info"])
    query = f"""
    SELECT hex_col, cd_mun, nm_mun FROM {path}
    WHERE hex_col IN ({', '.join(f"'{hex_id}'" for hex_id in hex_ids)})
    """
    with DBConnection("silver") as conn:
        df_hex_mun = conn.query_database(query)
    return df_hex_mun.set_index("hex_col")


def calculate_pct_pois_per_hex(df_pois_mun, df_pois_hex):
    """
    Calculates the percentage of points of interest (POIs) per hexagon.
    Args:
        df_pois_mun (pd.DataFrame): DataFrame containing POIs aggregated by municipality.
        df_pois_hex (pd.DataFrame): DataFrame containing POIs aggregated by hexagon.
    Returns:
        pd.DataFrame: DataFrame with the percentage of POIs per hexagon.
    """
    df_pois_mun = df_pois_mun.reset_index()
    df_pois_hex = df_pois_hex.reset_index()
    cols_to_normalize = [
        col for col in df_pois_hex.columns if col not in ["cd_mun", "nm_mun", "hex_col"]
    ]
    df_merged = df_pois_hex.merge(
        df_pois_mun, on=["cd_mun", "nm_mun"], suffixes=("_hex", "_mun")
    )
    for col in cols_to_normalize:
        df_merged[col + "_pct"] = df_merged[col + "_hex"] / df_merged[col + "_mun"]
    df_merged["geometry"] = df_merged["hex_col"].apply(get_h3_geom)
    df_merged = gpd.GeoDataFrame(df_merged, geometry="geometry", crs=CRS_GLOBAL)
    return df_merged


@save_parquet_decorator("silver")
def transform_pois(df_pois: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Transforms the pois data.

    Args:
        df_pois (pd.DataFrame): The pois data.
        **kwargs: Additional arguments, including filename and contract.

    Returns:
        pd.DataFrame: The transformed pois data.
    """
    cols_hex = ["hex_col"]
    cols_mun = ["cd_mun", "nm_mun"]
    df_pois_hex = group_pois_per_cols(df_pois, cols_hex).pipe(pivot_pois, cols_hex)
    hex_ids = df_pois_hex.index.tolist()
    df_hex_mun = get_hex_mun_map(hex_ids)
    df_pois_hex = (
        df_pois_hex.join(df_hex_mun, how="inner").reset_index().set_index(cols_mun)
    )
    df_pois = df_pois.set_index("hex_col").join(df_hex_mun, how="inner")
    df_pois_mun = group_pois_per_cols(df_pois, cols_mun).pipe(pivot_pois, cols_mun)
    return calculate_pct_pois_per_hex(df_pois_mun, df_pois_hex)


def main():
    """
    This function is the entry point of the program.
    It loads places using the Loader class, groups the points of interest per hex,
    and pivots the points of interest.
    """

    loader = Loader()
    df_pois = loader.get_places()
    kwargs = {"filename": "pois", "contract": CONTRACTS_SILVER["pois"]}
    _ = transform_pois(df_pois, **kwargs)
