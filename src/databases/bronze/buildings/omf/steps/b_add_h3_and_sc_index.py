"""
This module contains functions for processing building data and adding H3 indexes to a GeoDataFrame.

Functions:
- flatten_multipolygon: Flattens a MultiPolygon into a Polygon.
- process_data: Process the data from a specific source and return a GeoDataFrame.
- save_partition: Decorator function for saving a partition of a GeoDataFrame.
- main: The main function that processes data for "omf" and "google".
"""

import os
import geopandas as gpd
from tqdm import tqdm

from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.constants import BUILDING_PARTITIONS, CRS_GLOBAL
from src.tools.utils.reader import Reader
from src.tools.utils.loader import Loader
from src.tools.utils.h3 import add_h3_index_to_small_geom
from src.tools.managers.execution_manager import ExecutionManager
from src.databases.bronze.buildings.omf.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)

CONTRACT_RAW_DATA = execution_parameters["data_contracts"]["raw_data"]
CONTRACT_BRONZE = execution_parameters["data_contracts"]["bronze"]

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


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


def add_sc_index_to_small_geom(
    gdf: gpd.GeoDataFrame, df_sc: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Adds a spatial index to a GeoDataFrame.

    Parameters:
    gdf (gpd.GeoDataFrame): The GeoDataFrame to add the spatial index to.
    df_sc (gpd.GeoDataFrame): The GeoDataFrame containing the spatial index.

    Returns:
    gpd.GeoDataFrame: The GeoDataFrame with the spatial index added.
    """
    return gpd.sjoin(gdf, df_sc, how="inner", op="intersects")


def process_data(source: str, df_sc: gpd.GeoDataFrame) -> None:
    """
    Process the data from a specific source and return a GeoDataFrame.

    Args:
        source (str): The source of the data.
        df_sc (gpd.GeoDataFrame): The GeoDataFrame containing the spatial index.
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
        path = CONTRACT_RAW_DATA[f"buildings_{source}"]["physicalPath"]
        gdf = reader.read_geoparquet("".join([path, file])).pipe(set_crs)
        if source == "google":
            gdf = gdf.query("confidence >= 0.75").reset_index(drop=True)
        gdf = add_h3_index_to_small_geom(gdf)
        gdf = add_sc_index_to_small_geom(gdf, df_sc)
        gdf = gdf[cols[source]]
        kwargs = {"filename": f"partition_{i}"}
        save_partition(gdf, **kwargs)


def main() -> None:
    """
    This is the main function that processes data for "omf" and "google".
    """
    df_sc = Loader().get_sc_2022()
    process_data("google", df_sc)
    process_data("omf", df_sc)
    manager.update_last_run()
