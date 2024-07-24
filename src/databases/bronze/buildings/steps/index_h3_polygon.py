"""
This module contains functions for processing building data and adding H3 indexes to a GeoDataFrame.

Functions:
- flatten_multipolygon: Flattens a MultiPolygon into a Polygon.
- add_h3_index: Adds H3 index to a GeoDataFrame.
- process_data: Process the data from a specific source and return a GeoDataFrame.
- save_partition: Decorator function for saving a partition of a GeoDataFrame.
- main: The main function that processes data for "omf" and "google".
"""

import random
import geopandas as gpd
import pandas as pd
from tqdm import tqdm
from shapely.geometry import mapping
import h3

from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.constants import BUILDING_PARTITIONS, HEX_RESOLUTION, CRS_GLOBAL
from src.tools.utils.read import Reader
from src.tools.data_contract.buildings_data_contract import get_buildings_contracts

CONTRACT_DATALAKE = get_buildings_contracts("datalake")
CONTRACT_BRONZE = get_buildings_contracts("bronze")


def flatten_multipolygon(multipolygon: dict) -> dict:
    """
    Flattens a MultiPolygon into a Polygon.

    Args:
        multipolygon (dict): The MultiPolygon to be flattened.

    Returns:
        dict: The flattened Polygon.
    """
    if multipolygon["type"] == "MultiPolygon":
        return {
            "type": "Polygon",
            "coordinates": [
                coord for poly in multipolygon["coordinates"] for coord in poly
            ],
        }
    return multipolygon


def add_h3_index(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Adds H3 index to a GeoDataFrame.

    Parameters:
    gdf (gpd.GeoDataFrame): The input GeoDataFrame.

    Returns:
    gpd.GeoDataFrame: The GeoDataFrame with H3 index added.
    """
    hex_list = []
    for g in tqdm(gdf.geometry, desc="Adding H3 index"):
        temp = mapping(g)
        temp["coordinates"] = [[[j[1], j[0]] for j in i] for i in temp["coordinates"]]
        temp = flatten_multipolygon(temp)
        h3_index = list(h3.polyfill(temp, HEX_RESOLUTION))
        if not h3_index:
            centroid = g.centroid
            h3_index = [h3.geo_to_h3(centroid.y, centroid.x, HEX_RESOLUTION)]
        if len(h3_index) > 1:
            idx = random.randint(0, len(h3_index) - 1)
            h3_index = [list(h3_index)[idx]]
        hex_list.append(h3_index[0])
    dfh = pd.DataFrame(hex_list, columns=["hex_col"])
    assert (
        dfh.shape[0] == gdf.shape[0]
    ), f"The number of h3 indexes ({dfh.shape[0]}) is diff from the GeoDataFrame ({gdf.shape[0]})"
    gdf = gdf.join(dfh)
    return gdf


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
        gdf = add_h3_index(gdf)
        gdf = gdf[cols[source]]
        kwargs = {"filename": f"partition_{i}"}
        save_partition(gdf, **kwargs)


def main() -> None:
    """
    This is the main function that processes data for "omf" and "google".
    """
    process_data("google")
    # process_data("omf")
