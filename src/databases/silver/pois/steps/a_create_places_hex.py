"""
This module contains functions for creating and processing Places data in the silver database.
"""

from typing import List
import pandas as pd

from src.tools.managers.loader import Loader
from src.tools.managers.saver import save_parquet_decorator
from src.tools.managers.execution import ExecutionManager
from src.databases.silver.pois.config import EXECUTION_ID, BASE_PARAMS


manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, overwrite=True)
manager.update_status("running_step_1")

CONTRACT_BRONZE = execution_parameters["data_contracts"][0]
CONTRACT_SILVER = execution_parameters["data_contracts"][1]


def group_pois_per_hex(df_pois: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Groups the pois data per hexagon.

    Args:
        df_pois (pd.DataFrame): The pois data.
        cols (List[str]): The columns to group by.
    Returns:
        pd.DataFrame: The pois data grouped per hexagon.
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


@save_parquet_decorator("silver", CONTRACT_SILVER["pois_hex"])
def transform_pois(df_pois: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the pois data.

    Args:
        df_pois (pd.DataFrame): The pois data.

    Returns:
        pd.DataFrame: The transformed pois data.
    """
    cols_hex = ["cd_mun", "nm_mun", "hex_col"]
    cols_mun = ["cd_mun", "nm_mun"]
    df_pois_hex = group_pois_per_hex(df_pois, cols_hex).pipe(pivot_pois, cols_hex)
    df_pois_mun = group_pois_per_hex(df_pois, cols_mun).pipe(pivot_pois, cols_mun)
    df_pois = df_pois_hex.div(df_pois_mun, axis=0).fillna(0)
    # revisar: check if it is correct
    return df_pois


def main():
    """
    This function is the entry point of the program.
    It loads places using the Loader class, groups the points of interest per hex,
    and pivots the points of interest.
    """

    loader = Loader()
    df_pois = loader.get_places()
    _ = transform_pois(df_pois)
    manager.update_status("finished_step_1")
    manager.update_last_run()
