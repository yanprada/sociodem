"""Get land use information for a given municipality code.
This module provides a function to retrieve land use data from the
database based on the municipality code and specified years.
"""

from typing import List
import h3
import pandas as pd

from src.tools.managers.db_connector import DBConnection
from src.tools.utils.constants import MAPBIOMAS_CLASSES
from src.tools.utils.h3 import get_h3_geom
from src.tools.utils.common import write_log


def get_land_use(cd_mun: str, years: List[int], col=None) -> pd.DataFrame:
    """
    Get land use for a given municipality code.

    Args:
        cd_mun (str): Municipality code.
        years (List[int]): List of years to retrieve land use data for.
        col (str, optional): Optional column to filter land use data. If provided,
            only rows where this column is greater than 0 will be included.

    Returns:
        pd.DataFrame: DataFrame containing land use information for the specified municipality.
    """
    base_table_alias = "h"
    str_join = ""
    str_cols = f"{base_table_alias}.cd_setor, {base_table_alias}.hex_col, "
    str_where = ""
    query = ""
    for year in years:
        table_alias = f"lu{year}"
        str_cols += "".join(
            [
                f"{table_alias}.{col_land_use} as {col_land_use}_{year},"
                for col_land_use in MAPBIOMAS_CLASSES.values()
            ]
        )
        if col is not None:
            str_where += f"{table_alias}.{col} > 0"
        str_join = " \n".join(
            [
                f"JOIN land_use.brasil_coverage_{year} {table_alias} "
                f"ON {base_table_alias}.hex_col = {table_alias}.hex_col",
                str_join,
            ]
        )
    str_cols = str_cols[:-1]
    query = f"""
                WITH {base_table_alias} as (SELECT hex_col, cd_setor 
                                            FROM layers.hex_unique_sc_2022_sc_info a 
                                            WHERE a.cd_mun = '{cd_mun}'
                                            )
                SELECT {str_cols}
                FROM {base_table_alias}
                {str_join}
                {str_where}
            """
    with DBConnection("silver") as conn:
        df = conn.query_database(query, display=True)
    df["geometry"] = df["hex_col"].apply(get_h3_geom)  # type: ignore
    for year in years:
        df[f"land_use_{year}"] = (
            df.filter(regex=str(year)).idxmax(axis=1).str.replace(f"_{year}", "")
        )
    return df


def aggregate_h3_by_category(df: pd.DataFrame, cat_col: str) -> pd.DataFrame:
    """
    Recursively aggregate H3 hexagons by category. If all children of a parent hexagon
    have the same category, replace them with the parent hexagon and its category.

    Args:
        df (pd.DataFrame): DataFrame with H3 hexagon ids and a categorical column.
        cat_col (str): Name of the categorical column.

    Returns:
        pd.DataFrame: Aggregated DataFrame.
    """

    def _aggregate(df, res=None):
        if res is None:
            res = h3.get_resolution(df["hex_col"].iloc[0])
        else:
            res -= 1
        if res == 0:
            return df

        df["parent"] = df["hex_col"].apply(lambda x: h3.cell_to_parent(x, res - 1))
        grouped = df.groupby("parent")[cat_col].nunique()
        unique_parents = grouped[grouped == 1].index
        if len(unique_parents) == 0:
            return df
        parent_rows = (
            df[df["parent"].isin(unique_parents)]
            .groupby("parent")
            .first()
            .reset_index()
        )
        parent_rows["hex_col"] = parent_rows["parent"]
        parent_rows["geometry"] = parent_rows["hex_col"].apply(get_h3_geom)
        parent_rows = parent_rows[["hex_col", cat_col, "geometry"]]

        # Exclude children that are aggregated
        df_remaining = df[~df["parent"].isin(unique_parents)]
        df_remaining = df_remaining[["hex_col", cat_col, "geometry"]]

        # Combine and recurse if possible
        df_new = pd.concat([df_remaining, parent_rows], ignore_index=True)
        if len(parent_rows) == 0:
            return df_new
        write_log(f"Aggregated {len(parent_rows)} hexagons at resolution {res-1}.")
        return _aggregate(df_new, res=res)

    df = _aggregate(df)
    return df


def main():
    """Main function to execute the land use retrieval and aggregation."""
    df = get_land_use("3538709", [2016, 2023])
    dft = df[["hex_col", "geometry", "land_use_2016"]]
    dft = aggregate_h3_by_category(dft, "land_use_2016")
    dft2 = df[["hex_col", "geometry", "land_use_2023"]]
    dft2 = aggregate_h3_by_category(dft2, "land_use_2023")
    dft.to_csv("land_use_piracicaba_2016.csv")
    dft2.to_csv("land_use_piracicaba_2023.csv")
    df[
        (df["land_use_2016"] != "area_urbana") & (df["land_use_2023"] == "area_urbana")
    ].to_csv("change_area_urbana.csv")
