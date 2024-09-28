"""
This module processes a GeoTIFF image to aggregate building data into
hexagonal grids using the H3 library.
The resulting data is saved as a CSV file with hexagon IDs, building counts, and geometries.

Steps:
1. Read the GeoTIFF image and extract building data.
2. Convert coordinates to latitude and longitude if necessary.
3. Aggregate building data into hexagonal grids.
4. Save the aggregated data to a CSV file.
"""

import os
import rasterio
import numpy as np
import mlflow
import h3
from dask.distributed import Client, LocalCluster
from tqdm import tqdm
import geopandas as gpd
import pandas as pd

from shapely.geometry import Polygon
from pyproj import Transformer
from src.tools.utils.constants import HEX_RESOLUTION, CRS_GLOBAL
from src.tools.utils.common import write_log
from src.tools.utils.save import save_parquet_decorator

from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.buildings.google.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG


manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
BUILDING_CONTRACTS_RAW = execution_parameters["data_contracts"]["raw_data"]
BUILDING_CONTRACTS_BRONZE = execution_parameters["data_contracts"]["bronze"]
manager.update_status("running_step_2")

EXPERIMENT_ID = execution_parameters["mlflow_experiment"]
mlflow.set_experiment(EXPERIMENT_ID)


def process_image(file_path):
    """
    Processes an image file to extract building locations and their fractional counts.

    Args:
        file_path (str): The path to the image file to be processed.

    Returns:
        tuple: A tuple containing:
            - lats_lons (numpy.ndarray): An array of latitude and longitude
                coordinates of buildings.
            - building_counts (numpy.ndarray): An array of fractional building counts
                corresponding to the coordinates.
    """
    with rasterio.open(file_path) as src:
        crs = src.crs
        building_fractional_count = src.read(1)
        rows, cols = np.nonzero(building_fractional_count)
        proj_coords = np.array(src.xy(rows, cols)).T
        if crs.is_projected:
            transformer = Transformer.from_crs(crs, CRS_GLOBAL, always_xy=True)
            lats_lons = np.array(
                transformer.transform(proj_coords[:, 0], proj_coords[:, 1])
            ).T
        else:
            lats_lons = proj_coords
        building_counts = building_fractional_count[rows, cols]
    return lats_lons, building_counts


def add_hexagons(lats_lons, building_counts):
    """
    Adds hexagon indices to latitude and longitude coordinates and aggregates building counts.
    Parameters:
        lats_lons (numpy.ndarray): A 2D array of latitude and longitude coordinates.
        building_counts (numpy.ndarray): An array of building counts corresponding
            to the coordinates.
    Returns:
        geopandas.GeoDataFrame: A GeoDataFrame with hexagon indices, aggregated building counts,
            and geometries in WKT format.
    """

    vfunc = np.vectorize(h3.geo_to_h3)
    h3_indices = vfunc(lats_lons[:, 1], lats_lons[:, 0], HEX_RESOLUTION)
    df = pd.DataFrame({"h3_index": h3_indices, "building_count": building_counts})
    grouped = df.groupby("h3_index")["building_count"].sum().reset_index()
    geometries = grouped["h3_index"].apply(
        lambda h: Polygon(h3.h3_to_geo_boundary(h, geo_json=True))
    )
    gdf = gpd.GeoDataFrame(grouped, geometry=geometries)
    return gdf


@save_parquet_decorator(
    "bronze", BUILDING_CONTRACTS_BRONZE["buildings_google"], save_db=True
)
def process_file(file_path, **kwargs):
    """
    Processes an image file and adds hexagons based on the extracted data.
    Args:
        file_path (str): The path to the image file to be processed.
        **kwargs: Additional keyword arguments.
    Returns:
        Any: The result of adding hexagons to the processed image data.
    """
    lats_lons, building_counts = process_image(file_path)
    return add_hexagons(lats_lons, building_counts)


@save_parquet_decorator(
    "bronze", BUILDING_CONTRACTS_BRONZE["buildings_google"], save_db=True, save_pq=True
)
def save_batch_results(batch_df, **kwargs):
    """
    Saves the batch results.
    Parameters:
        batch_df (DataFrame): The DataFrame containing the batch results.
        **kwargs: Additional keyword arguments.
    Returns:
        DataFrame: The same DataFrame that was passed in.
    """

    return batch_df


def process_and_save_batch(file_paths, batch_number):
    """
    Processes a batch of files and saves the results.
    Args:
        file_paths (list of str): List of file paths to be processed.
        batch_number (int): The batch number for identification.
    Raises:
        Exception: If an error occurs during file processing, it logs the error.
    The function processes each file in the provided list of file paths.
    If processing is successful, the results are appended to a list.
    If an error occurs during processing, it logs the error.
    After processing all files, it combines the results into a single DataFrame and saves it with
    a filename that includes the batch number. If no results are available to save,
    it logs a message indicating that there are no results for the given batch.
    """

    results = []
    for file in file_paths:
        try:
            result = process_file(file)
            results.append(result)
        except Exception as e:
            write_log(f"An error occurred processing {file}: {e}")

    if results:
        combined_df = pd.concat(results, ignore_index=True)
        save_batch_results(combined_df, filename=f"batch_{batch_number}")
    else:
        write_log(f"No results to save for batch {batch_number}")


def main():
    """
    Main function to process image files into hex format.
    This function initializes a Dask client with specified resources, processes
    image files in batches, and saves the results. It updates the status upon
    completion.
    """
    parallel = False
    state = "SC"
    path = os.path.join(
        BUILDING_CONTRACTS_RAW["buildings_google"]["physicalPath"], state
    )
    files = [
        os.path.join(path, file) for file in os.listdir(path) if file.endswith(".tif")
    ]
    if parallel:
        num_workers = 5
        cluster = LocalCluster(n_workers=num_workers)
        client = Client(cluster)
        batch_size = 5
        for i in tqdm(range(0, len(files), batch_size)):
            batch_files = files[i : i + batch_size]
            batch_number = i // batch_size + 1
            future = client.submit(process_and_save_batch, batch_files, batch_number)
            try:
                future.result()
            except Exception as e:
                write_log(f"An error occurred processing batch {batch_number}: {e}")
        client.close()
    else:
        for file in tqdm(files):
            kwargs = {"filename": file.replace(path, "").replace(".tif", "")}
            process_file(file, **kwargs)
    manager.update_status("finished_step_2")


if __name__ == "__main__":
    main()
