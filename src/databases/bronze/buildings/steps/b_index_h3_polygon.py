"""
This module contains functions for processing building data and adding H3 indexes to a GeoDataFrame.

Functions:
- flatten_multipolygon: Flattens a MultiPolygon into a Polygon.
- process_data: Process the data from a specific source and return a GeoDataFrame.
- save_partition: Decorator function for saving a partition of a GeoDataFrame.
- main: The main function that processes data for "omf" and "google".
"""

import geopandas as gpd
from tqdm import tqdm

from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.constants import BUILDING_PARTITIONS, CRS_GLOBAL
from src.tools.utils.read import Reader
from src.tools.data_contract.buildings_data_contract import get_buildings_contracts
from src.tools.utils.h3 import add_h3_index_to_small_geom

CONTRACT_DATALAKE = get_buildings_contracts("datalake")
CONTRACT_BRONZE = get_buildings_contracts("bronze")


def set_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Sets the coordinate reference system (CRS) of a GeoDataFrame to CRS_GLOBAL.

    Parameters:
    gdf (gpd.GeoDataFrame): The GeoDataFrame to set the CRS for.

    Returns:
    gpd.GeoDataFrame: The GeoDataFrame with the CRS set to CRS_GLOBAL.
    """
    if gdf.crs is None:
        gdf = gdf.set_crs(CRS_GLOBAL)
    else:
        gdf = gdf.to_crs(CRS_GLOBAL)
    return gdf


def process_data(source: str) -> None:
    """
    Process the data from a specific source and return a GeoDataFrame.

    Args:
        source (str): The source of the data.
    """

    @save_parquet_decorator("bronze", CONTRACT_BRONZE[f"buildings_{source}"])
    def save_partition(gdf: gpd.GeoDataFrame, **kwargs) -> None:
        return gdf

    cols = {
        "google": ["hex_col", "confidence", "area_in_meters", "geometry"],
        "omf": ["hex_col", "height", "numfloors", "class", "geometry"],
    }
    for i, file in tqdm(
        enumerate(BUILDING_PARTITIONS[source]), desc="Processing partitions"
    ):
        reader = Reader()
        path = CONTRACT_DATALAKE[f"buildings_{source}"]["physicalPath"]
        gdf = reader.read_geoparquet("".join([path, file])).pipe(set_crs)
        if source == "google":
            gdf = gdf.query("confidence >= 0.75").reset_index(drop=True)
        gdf = add_h3_index_to_small_geom(gdf)
        gdf = gdf[cols[source]]
        kwargs = {"filename": f"partition_{i}"}
        save_partition(gdf, **kwargs)


def main() -> None:
    """
    This is the main function that processes data for "omf" and "google".
    """
    process_data("google")
    process_data("omf")
