"""
This module contains functions related to the processing of census data for municipalities.

Functions:
- get_hex_from_geom(df: pd.DataFrame) -> pd.DataFrame: Get the hexagon ID from the geometry column.
- get_muns(): Get the file from the physical path.
- main(): Main function.

"""

import geopandas as gpd

from src.tools.data_contract.censo_data_contract import get_censo_contracts
from src.tools.utils.common import get_db_path
from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.h3 import add_h3_index_to_large_geom
from src.tools.utils.constants import CRS_GLOBAL

CONTRACT_MUN_CENSO_BRONZE = get_censo_contracts("bronze")["mun_2022"]
CONTRACT_MUN_CENSO_SILVER = get_censo_contracts("silver")["mun_2022"]


@save_parquet_decorator("silver", CONTRACT_MUN_CENSO_SILVER)
def get_muns():
    """
    Get the file from the physical path.
    """
    conn = DBConnection("bronze")
    path = get_db_path(CONTRACT_MUN_CENSO_BRONZE)
    df = conn.query_database(f"SELECT * FROM {path}", geo=False)
    df["geometry"] = gpd.GeoSeries.from_wkb(df["geometry"])
    df = gpd.GeoDataFrame(df, geometry="geometry", crs=CRS_GLOBAL)
    df = add_h3_index_to_large_geom(df, "cd_setor")
    df["geometry"] = df["geometry"].apply(lambda x: x.wkt)
    return df


def main():
    """
    Main function.
    """
    _ = get_muns()
