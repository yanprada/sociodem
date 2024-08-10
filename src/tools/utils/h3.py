"""
This module provides utility functions for working with H3 indexes and GeoDataFrames.

Functions:
- flatten_multipolygon(multipolygon: dict) -> dict: Flattens a MultiPolygon into a Polygon.
- add_h3_index_to_geopandas(gdf: gpd.GeoDataFrame) -> 
    gpd.GeoDataFrame: Adds H3 index to a GeoDataFrame.
"""

import math
import random
from typing import List, Tuple
import geopandas as gpd
import pandas as pd
from dask.distributed import Client, LocalCluster, as_completed
from tqdm import tqdm
from shapely.geometry import mapping, Polygon
import h3

from src.tools.utils.constants import HEX_RESOLUTION


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


def add_h3_index_to_small_geom(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
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


def process_in_batches(gdf: gpd.GeoDataFrame, key: str) -> Tuple[List[str], List[str]]:
    """
    Process the GeoDataFrame in parallel and add H3 index to each geometry.
    Args:
        gdf (gpd.GeoDataFrame): The input GeoDataFrame.
        key (str): The column name in the GeoDataFrame to use as the key.
    Returns:
        Tuple[List[str], List[str]]: A tuple containing two lists. The first list
            contains the H3 index values for each geometry, and the second list contains
            the corresponding key values.
    """
    hex_list = []
    cod_list = []
    for k, g in tqdm(zip(gdf[key], gdf.geometry), desc="Adding H3 index"):
        temp = mapping(g)
        temp["coordinates"] = [[[j[1], j[0]] for j in i] for i in temp["coordinates"]]
        temp = flatten_multipolygon(temp)
        hex_ids = list(h3.polyfill(temp, HEX_RESOLUTION))
        if not hex_ids:
            centroid = g.centroid
            hex_ids = [h3.geo_to_h3(centroid.y, centroid.x, HEX_RESOLUTION)]
        cod_sc = [k] * len(hex_ids)
        hex_list.extend(hex_ids)
        cod_list.extend(cod_sc)

    assert len(set(cod_list)) == len(
        gdf
    ), f"The key column {key} has different values. {key}: {len(set(cod_list))}, gdf: {len(gdf)}"
    return hex_list, cod_list


def add_h3_index_to_large_geom(
    gdf: gpd.GeoDataFrame, key: str, paralel: bool = False
) -> gpd.GeoDataFrame:
    """
    Adds H3 index to a GeoDataFrame.

    Parameters:
    gdf (gpd.GeoDataFrame): The input GeoDataFrame.

    Returns:
    gpd.GeoDataFrame: The GeoDataFrame with H3 index added.
    """
    if paralel:
        hex_list = []
        cod_list = []
        num_workers = 10
        cluster = LocalCluster(n_workers=num_workers)
        client = Client(cluster)
        steps = math.ceil(len(gdf) / num_workers)
        futures = [
            client.submit(process_in_batches, gdf[i * steps : i * steps + steps], key)
            for i in range(num_workers)
        ]
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Adding H3 index"
        ):
            result_hex, result_cod = future.result()
            hex_list.extend(result_hex)
            cod_list.extend(result_cod)
        client.close()
    else:
        hex_list, cod_list = process_in_batches(gdf, key)
    dfh = pd.DataFrame({"hex_col": hex_list, key: cod_list})
    gdf = pd.DataFrame(gdf.drop(columns="geometry"))
    return pd.merge(dfh, gdf, on=key)


def get_h3_geom(h3_index: str) -> Polygon:
    """Convert an H3 index to a Shapely Polygon."""
    boundary = h3.h3_to_geo_boundary(h3_index, geo_json=True)
    polygon = Polygon(boundary)
    return polygon


def create_hex_col_from_dot(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Creates a new column in the DataFrame containing the hexagon ID of each point.

    Args:
        df (gpd.GeoDataFrame): The DataFrame containing the points.

    Returns:
        DataFrame: The DataFrame with the new column.
    """
    df["hex_col"] = df.apply(
        lambda row: h3.geo_to_h3(row["latitude"], row["longitude"], HEX_RESOLUTION),
        axis=1,
    )
    return df
