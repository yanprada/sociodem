"""
This module processes GeoTIFF images to aggregate building data into
hexagonal grids using the H3 library and Dask for parallel processing.
The resulting data is saved as Parquet files with hexagon IDs, building counts, and geometries.

Steps:
1. Read the GeoTIFF images and extract building data.
2. Convert coordinates to latitude and longitude if necessary.
3. Aggregate building data into hexagonal grids.
4. Save the aggregated data to Parquet files.
"""

import os
import time
import gc
import logging
from typing import List, Tuple
import rasterio
import numpy as np
import mlflow
import h3
from dask.distributed import Client, LocalCluster, progress
from dask import dataframe as dd
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
manager.update_status("running_step_2")

BUILDING_CONTRACTS_RAW = execution_parameters["data_contracts"]["raw_data"]
BUILDING_CONTRACTS_BRONZE = execution_parameters["data_contracts"]["bronze"]
EXPERIMENT_ID = execution_parameters["mlflow_experiment"]

mlflow.set_experiment(EXPERIMENT_ID)

LOG_FILE_PATH = os.path.join(os.path.dirname(__file__), "failed_files.log")
logging.basicConfig(filename=LOG_FILE_PATH, level=logging.ERROR)

RUN_TIME = time.strftime("%Y-%m-%d %H:%M:%S")
STATE = "SP"
YEAR = BUILDING_CONTRACTS_RAW["buildings_google"]["physicalPath"].split("/")[-2]


def log_failed_file(file_path: str) -> None:
    """Log a file as failed."""
    with open(LOG_FILE_PATH, "a", encoding="utf-8") as log_file:
        log_file.write(f"{file_path}\n")


def process_image(file_path: str) -> gpd.GeoDataFrame:
    """
    Processes a GeoTIFF image file to extract building locations and their fractional counts.
    Reads the image in window chunks and skips windows without data.
    """
    result = []
    with rasterio.open(file_path) as src:
        crs = src.crs
        transformer = (
            Transformer.from_crs(crs, CRS_GLOBAL, always_xy=True)
            if crs.is_projected
            else None
        )

        for _, window in src.block_windows(1):
            building_fractional_count = src.read(1, window=window)

            if np.all(building_fractional_count <= 0):
                continue

            rows, cols = np.nonzero(building_fractional_count)

            if len(rows) == 0:
                continue
            rows += window.row_off
            cols += window.col_off

            proj_coords = np.array(src.xy(rows, cols)).T

            if crs.is_projected:
                lats_lons = np.array(
                    transformer.transform(proj_coords[:, 0], proj_coords[:, 1])
                ).T
            else:
                lats_lons = proj_coords
            building_counts = building_fractional_count[
                rows - window.row_off, cols - window.col_off
            ]
            if lats_lons is not None or lats_lons.size != 0:
                result.append(add_hexagons(lats_lons, building_counts))
    return gpd.GeoDataFrame(pd.concat(result, ignore_index=True))


def add_hexagons(lats_lons, building_counts):
    """
    Adds hexagon indices to latitude and longitude coordinates and aggregates building counts.
    Ensures consistent CRS handling.
    """
    h3_indices = np.vectorize(h3.geo_to_h3)(
        lats_lons[:, 1], lats_lons[:, 0], HEX_RESOLUTION
    )
    df = pd.DataFrame({"h3_index": h3_indices, "building_count": building_counts})
    df = df[df["building_count"] > 0]
    grouped = df.groupby("h3_index")["building_count"].sum().reset_index()

    geometries = grouped["h3_index"].apply(
        lambda h: Polygon(h3.h3_to_geo_boundary(h, geo_json=True))
    )
    gdf = gpd.GeoDataFrame(grouped, geometry=geometries, crs=CRS_GLOBAL)

    return gdf


@save_parquet_decorator(
    "bronze", BUILDING_CONTRACTS_BRONZE["buildings_google"], save_db=True, save_pq=True
)
def save_batch_results(batch_df, **kwargs):
    """
    Saves the batch results and logs metrics to MLflow.
    """
    return batch_df


def process_and_save(file_path, batch_number):
    """
    Processes a single file and saves the results.
    """
    try:

        result = process_image(file_path)
        if result.empty:
            log_failed_file(file_path)
            return 0, 0
        result["year"] = int(file_path.split("_")[3])
        kwargs = {"filename": f"batch_{batch_number}_{os.path.basename(file_path)}"}
        save_batch_results(result, **kwargs)
        result_len = len(result)
        result_dompp = result["building_count"].sum()
        del result
        gc.collect()
        return result_len, result_dompp
    except Exception as e:
        write_log(f"An error occurred processing {file_path}: {e}")
        log_failed_file(file_path)
        return 0, 0


def get_remaining_files(path: str) -> List[str]:
    """
    Retrieves a list of .tif files in the specified directory that
        have not yet been processed into .parquet files.
    Args:
        path (str): The directory path to search for .tif files.
    Returns:
        List[str]: A list of .tif file paths that have not been processed into .parquet files.
    """

    files = [
        os.path.join(path, file) for file in os.listdir(path) if file.endswith(".tif")
    ]
    processed_files = [
        os.path.join(path, os.path.splitext(file)[0].split("_", 2)[-1])
        for file in os.listdir(
            BUILDING_CONTRACTS_BRONZE["buildings_google"]["physicalPath"]
        )
        if file.endswith(".parquet")
    ]
    return [file for file in files if file not in processed_files]


def get_failed_files():
    """Reads failed file paths from the log file."""
    if os.path.exists(LOG_FILE_PATH):
        with open(LOG_FILE_PATH, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f.readlines())
    return set()


def categorize_files_by_size(
    file_paths: List[str],
) -> Tuple[List[str], List[str], List[str]]:
    """
    Categorizes files into small, medium, and large based on their sizes.

    Args:
        file_paths (List[str]): List of file paths to categorize.

    Returns:
        Tuple[List[str], List[str], List[str]]: Three lists containing
            small, medium, and large files respectively.
    """
    nano_files = []
    xsm_files = []
    small_files = []
    medium_files = []
    large_files = []
    xl_files = []
    for file_path in file_paths:
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Convert bytes to MB

        if file_size_mb > 1000:
            xl_files.append(file_path)
        elif file_size_mb > 400:
            large_files.append(file_path)
        elif file_size_mb > 80:
            medium_files.append(file_path)
        elif file_size_mb > 40:
            small_files.append(file_path)
        elif file_size_mb > 20:
            xsm_files.append(file_path)
        else:
            nano_files.append(file_path)

    return nano_files, xsm_files, small_files, medium_files, large_files, xl_files


def process_files_in_parallel(
    file_paths: List[str],
    n_workers: int,
    memory_limit: str,
):
    """
    Processes a list of files in parallel using Dask.
    Args:
        file_paths (List[str]): List of file paths to be processed.
        n_workers (int): Number of worker processes to use.
        memory_limit (str): Memory limit for each worker process.
    Returns:
        int: Total number of processed rows.
    """

    with LocalCluster(
        n_workers=n_workers,
        threads_per_worker=1,
        memory_limit=memory_limit,
        processes=True,
    ) as cluster, Client(cluster):
        ddf = dd.from_pandas(
            pd.DataFrame({"file_path": file_paths}), npartitions=n_workers
        )
        futures = ddf.apply(
            lambda row: process_and_save(
                row["file_path"],
                row.name // (len(file_paths) // n_workers),
            ),
            axis=1,
            meta=("processed_rows", "int64"),
        ).persist()

        progress(futures)

        results = futures.compute()
        results_len = results["processed_rows"]
        results_dompp = results["building_count"]
        total_processed = results_len.sum()
        total_dompp = results_dompp.sum()
    return total_processed, total_dompp


def main():
    """
    Main function to process image files into hex format using Dask for parallelization.
    """
    col_sum = 0
    dompp_sum = 0
    with mlflow.start_run(run_name=f"{STATE}_{YEAR}_{RUN_TIME}"):
        path = os.path.join(
            BUILDING_CONTRACTS_RAW["buildings_google"]["physicalPath"], STATE
        )
        files = get_remaining_files(path)
        nano_files, xsm_files, small_files, medium_files, large_files, xl_files = (
            categorize_files_by_size(files)
        )
        for file_group, n_workers, memory_limit in [
            (xl_files, 10, "10GB"),
            (large_files, 20, "9GB"),
            (medium_files, 30, "8GB"),
            (small_files, 40, "7GB"),
            (xsm_files, 50, "6GB"),
            (nano_files, 60, "5GB"),
        ]:
            if file_group:
                write_log(
                    f"Processing {file_group} {len(file_group)} files with {n_workers} workers"
                )
                n_workers = min(n_workers, len(file_group))
                col_sum_batch, dompp_sum_batch = process_files_in_parallel(
                    file_group, n_workers, memory_limit
                )
                col_sum += col_sum_batch
                dompp_sum += dompp_sum_batch
            mlflow.log_metric("total_rows", col_sum)
            mlflow.log_metric("total_dompp", dompp_sum)
    manager.update_status("finished_step_2")


if __name__ == "__main__":
    main()
