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
from tqdm import tqdm
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


def log_failed_file(file_path: str) -> None:
    """Log a file as failed."""
    with open(LOG_FILE_PATH, "a", encoding="utf-8") as log_file:
        log_file.write(f"{file_path}\n")


def process_image(file_path: str) -> Tuple[np.ndarray, np.ndarray]:
    """
    Processes a GeoTIFF image file to extract building locations and their fractional counts.
    Reads the image in window chunks and skips windows without data.
    """
    lats_lons_list = []
    building_counts_list = []
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

            lats_lons_list.append(lats_lons)
            building_counts_list.append(building_counts)
    lats_lons = np.concatenate(lats_lons_list) if lats_lons_list else np.array([])
    building_counts = (
        np.concatenate(building_counts_list) if building_counts_list else np.array([])
    )

    return lats_lons, building_counts


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


def process_file(file_path: str) -> pd.DataFrame:
    """
    Processes an image file and adds hexagons based on the extracted data.
    """
    failed_files = get_failed_files()
    if file_path in failed_files:
        return pd.DataFrame()
    try:
        lats_lons, building_counts = process_image(file_path)
        if lats_lons is None or lats_lons.size == 0:
            return pd.DataFrame()
        return add_hexagons(lats_lons, building_counts)
    except Exception as e:
        logging.error("Error processing %s: %s", file_path, str(e))
        log_failed_file(file_path)
        return pd.DataFrame()


@save_parquet_decorator(
    "bronze", BUILDING_CONTRACTS_BRONZE["buildings_google"], save_db=True, save_pq=True
)
def save_batch_results(batch_df, **kwargs):
    """
    Saves the batch results and logs metrics to MLflow.
    """
    filename = kwargs.get("filename", "default_filename")
    with mlflow.start_run(run_name=filename, nested=True):
        mlflow.log_metric("num_rows", batch_df.shape[0])
        mlflow.log_metric("building_count", batch_df["building_count"].sum())
        return batch_df


def process_and_save(file_path, batch_number, parent_run_id):
    """
    Processes a single file and saves the results.
    """
    try:
        with mlflow.start_run(run_id=parent_run_id):
            result = process_file(file_path)
            if result.empty:
                return 0
            kwargs = {"filename": f"batch_{batch_number}_{os.path.basename(file_path)}"}
            save_batch_results(result, **kwargs)
            result_len = len(result)
            del result
            gc.collect()
            return result_len
    except Exception as e:
        write_log(f"An error occurred processing {file_path}: {e}")
        log_failed_file(file_path)
        return 0


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
    small_files = []
    medium_files = []
    large_files = []

    for file_path in file_paths:
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Convert bytes to MB
        if file_size_mb > 600:
            large_files.append(file_path)
        elif file_size_mb > 200:
            medium_files.append(file_path)
        else:
            small_files.append(file_path)

    return small_files, medium_files, large_files


def process_files_in_parallel(
    file_paths: List[str], n_workers: int, parent_run: mlflow.entities.Run
):
    """
    Processes a list of files in parallel using Dask.
    Args:
        file_paths (List[str]): List of file paths to be processed.
        n_workers (int): Number of worker processes to use.
        parent_run (mlflow.entities.Run): The parent run object containing run information.
    Returns:
        int: Total number of processed rows.
    """

    with LocalCluster(
        n_workers=n_workers,
        threads_per_worker=1,
        memory_limit="auto",
        processes=True,
    ) as cluster, Client(cluster):
        ddf = dd.from_pandas(
            pd.DataFrame({"file_path": file_paths}), npartitions=n_workers
        )
        futures = ddf.apply(
            lambda row: process_and_save(
                row["file_path"],
                row.name // (len(file_paths) // n_workers),
                parent_run.info.run_id,
            ),
            axis=1,
            meta=("processed_rows", "int64"),
        ).persist()

        progress(futures)

        results = futures.compute()
        total_processed = results.sum()
    return total_processed


def main():
    """
    Main function to process image files into hex format using Dask for parallelization.
    """
    parallel = True
    run_time = time.strftime("%Y-%m-%d %H:%M:%S")
    state = "MG"
    if parallel:
        with mlflow.start_run(run_name=f"{state}_{run_time}") as parent_run:
            path = os.path.join(
                BUILDING_CONTRACTS_RAW["buildings_google"]["physicalPath"], state
            )
            files = get_remaining_files(path)
            small_files, medium_files, large_files = categorize_files_by_size(files)
            n_workers = 12
            total_processed_small = process_files_in_parallel(
                small_files, n_workers, parent_run
            )
            n_workers = 6
            total_processed_medium = process_files_in_parallel(
                medium_files, n_workers, parent_run
            )
            n_workers = 2
            total_processed_large = process_files_in_parallel(
                large_files, n_workers, parent_run
            )
            mlflow.log_metric(
                "total_processed_rows",
                total_processed_small + total_processed_medium + total_processed_large,
            )
    else:
        path = os.path.join(
            BUILDING_CONTRACTS_RAW["buildings_google"]["physicalPath"], state
        )
        files = ["/mnt/files/google_buildings/2016/MG/00a5c_2016_06_30_RTk0wGE47-Q.tif"]
        for i, file in tqdm(enumerate(files)):
            process_and_save(file, i, None)
    manager.update_status("finished_step_2")


if __name__ == "__main__":
    main()
