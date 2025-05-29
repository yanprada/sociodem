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
from typing import List, Tuple
import rasterio
import numpy as np
import mlflow
import h3
from dask.distributed import Client, LocalCluster, progress
from dask import dataframe as dd
import geopandas as gpd
import pandas as pd
from pyproj import Transformer


from src.tools.utils.constants import HEX_RESOLUTION, CRS_GLOBAL
from src.tools.utils.common import write_log
from src.tools.utils.h3 import get_h3_geom
from src.tools.managers.saver import save_parquet_decorator
from src.databases.bronze.buildings.google.config import (
    manager,
    BUILDING_CONTRACTS_BRONZE,
    BUILDING_CONTRACTS_RAW,
    EXPERIMENT_NAME,
)


mlflow.set_experiment(EXPERIMENT_NAME)

RUN_TIME = time.strftime("%Y-%m-%d %H:%M:%S")
YEAR = 2019
STATE = "MS"


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
                if transformer is None:
                    raise ValueError("Failed to create transformer")
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
    # Filter out zero counts upfront to reduce processing
    valid_mask = building_counts > 0
    if not valid_mask.any():
        return gpd.GeoDataFrame(
            columns=["h3_index", "building_count", "geometry"], crs=CRS_GLOBAL
        )

    valid_coords = lats_lons[valid_mask]
    valid_counts = building_counts[valid_mask]

    # Vectorized H3 index generation
    h3_indices = np.vectorize(h3.latlng_to_cell, otypes=[object])(
        valid_coords[:, 1], valid_coords[:, 0], HEX_RESOLUTION
    )

    # Direct aggregation using pandas groupby
    df = pd.DataFrame({"h3_index": h3_indices, "building_count": valid_counts})
    grouped = df.groupby("h3_index", as_index=False)["building_count"].sum()

    # Batch geometry creation
    geometries = grouped["h3_index"].apply(get_h3_geom).tolist()

    return gpd.GeoDataFrame(grouped, geometry=geometries, crs=CRS_GLOBAL)


@save_parquet_decorator(medallon="bronze")
def save_results(batch_df, **kwargs):
    """
    Saves the batch results
    """
    return batch_df


def process_and_save(file_path, batch_number):
    """
    Processes a single file and saves the results.
    """
    try:

        result = process_image(file_path)
        if result.empty:
            return 0, 0
        result["year"] = int(YEAR)
        result["state"] = STATE
        kwargs = {
            "filename": f"batch_{batch_number}_{os.path.basename(file_path)}",
            "contract": BUILDING_CONTRACTS_BRONZE[f"google_{YEAR}"],
        }
        result = save_results(result, **kwargs)
        result_len = len(result)
        result_dompp = result["building_count"].sum()
        del result
        gc.collect()
        return result_len, result_dompp
    except Exception as e:
        write_log(f"An error occurred processing {file_path}: {e}")
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
            BUILDING_CONTRACTS_BRONZE[f"google_{YEAR}"]["physicalPath"]
        )
        if file.endswith(".parquet")
    ]
    return [file for file in files if file not in processed_files]


def categorize_files_by_size(
    file_paths: List[str],
) -> Tuple[List[str], List[str], List[str], List[str], List[str], List[str]]:
    """
    Categorizes files into small, medium, and large based on their sizes.

    Args:
        file_paths (List[str]): List of file paths to categorize.

    Returns:
        Tuple[List[str], List[str], List[str],
            List[str], List[str], List[str]]: Six lists containing
            nano, extra small, small, medium, large and extra large files respectively.
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
    name: str,
    file_paths: List[str],
    n_workers: int,
    memory_limit: str,
):
    """
    Processes a list of files in parallel using Dask.
    Args:
        name (str): Name of the file group.
        file_paths (List[str]): List of file paths to be processed.
        n_workers (int): Number of worker processes to use.
        memory_limit (str): Memory limit for each worker process.
    Returns:
        int: Total number of processed rows.
    """
    with mlflow.start_run(run_name=name, nested=True):
        with LocalCluster(
            n_workers=n_workers,
            threads_per_worker=1,
            memory_limit=memory_limit,
            processes=True,
        ) as cluster, Client(cluster):
            ddf = dd.from_pandas(  # type: ignore
                pd.DataFrame({"file_path": file_paths}), npartitions=n_workers
            )
            futures = ddf.apply(
                lambda row: process_and_save(
                    row["file_path"],
                    row.name // (len(file_paths) // n_workers),
                ),
                axis=1,
                meta=[("processed_rows", "int64"), ("building_count", "int64")],
            ).persist()

            progress(futures)
            futures_dict = futures.compute()
    log_mlflow_metrics(futures_dict)
    return futures_dict


def log_mlflow_metrics(results):
    """
    Logs various metrics to MLflow based on the provided results.
    Parameters:
    results (list of tuples): A list where each tuple contains two elements:
        - The first element is used to calculate 'results_len' metrics.
        - The second element is used to calculate 'dompp_sum' metrics.
    Metrics Logged:
    - results_len: Sum of the first elements in the results tuples.
    - results_len_median: Median of the first elements in the results tuples.
    - results_len_mean: Mean of the first elements in the results tuples.
    - dompp_sum: Sum of the second elements in the results tuples.
    - dompp_sum_median: Median of the second elements in the results tuples.
    - dompp_sum_mean: Mean of the second elements in the results tuples.
    """

    mlflow.log_metric("results_len", np.sum(r_tuple[0] for r_tuple in results))  # type: ignore
    mlflow.log_metric(
        "results_len_median", np.median([r_tuple[0] for r_tuple in results])  # type: ignore
    )
    mlflow.log_metric(
        "results_len_mean", np.mean([r_tuple[0] for r_tuple in results])  # type: ignore
    )  # type: ignore
    mlflow.log_metric("dompp_sum", np.sum(r_tuple[1] for r_tuple in results))  # type: ignore
    mlflow.log_metric(
        "dompp_sum_median", np.median([r_tuple[1] for r_tuple in results])  # type: ignore
    )
    mlflow.log_metric(
        "dompp_sum_mean", np.mean([r_tuple[1] for r_tuple in results])  # type: ignore
    )  # type: ignore


def move_files_location():
    """
    Updates the folder structure for building contracts by renaming and moving
    the existing folder and creating a new folder at the old path.
    The function performs the following steps:
    1. Retrieves the current path of the building contracts folder.
    2. Constructs a new path by replacing the year in the old path and appending the state.
    3. If the old path exists, renames and moves the folder to the new path.
    4. Creates a new folder at the old path.
    5. Logs the actions performed.
    Note: The function assumes that `BUILDING_CONTRACTS_RAW`, `STATE`, `os`, and `write_log`
    are defined elsewhere in the code.
    Raises:
        OSError: If an error occurs while renaming or creating directories.
    """
    old_path = BUILDING_CONTRACTS_BRONZE[f"google_{YEAR}"]["physicalPath"]
    new_path = old_path.replace(str(YEAR), f"process/{YEAR}")
    new_path = os.path.join(new_path, STATE)
    if os.path.exists(old_path):
        os.rename(old_path, new_path)
        write_log(f"Renamed and moved folder from {old_path} to {new_path}")
    os.makedirs(old_path, exist_ok=True)
    write_log(f"Created new folder at {old_path}")


def main():
    """
    Main function to process image files into hex format using Dask for parallelization.
    """
    module_name = os.path.basename(__file__).replace(".py", "")
    manager.update_status(module_name)
    results_total = pd.Series()
    with mlflow.start_run(run_name=f"{STATE}_{YEAR}_{RUN_TIME}"):
        path = os.path.join(
            BUILDING_CONTRACTS_RAW[f"google_{YEAR}"]["physicalPath"], STATE
        )
        files = get_remaining_files(path)
        nano_files, xsm_files, small_files, medium_files, large_files, xl_files = (
            categorize_files_by_size(files)
        )
        for name, file_group, n_workers, memory_limit in [
            ("xlarge", xl_files, 8, "6GB"),
            ("large", large_files, 15, "5GB"),
            ("medium", medium_files, 18, "5GB"),
            ("small", small_files, 22, "5GB"),
            ("xsmall", xsm_files, 22, "5GB"),
            ("nano", nano_files, 22, "5GB"),
        ]:
            if file_group:
                n_workers = min(n_workers, len(file_group))
                write_log(
                    f"Processing {name} {len(file_group)} files with {n_workers} workers"
                )
                results = process_files_in_parallel(
                    name, file_group, n_workers, memory_limit
                )
                results_total = pd.concat([results_total, results], ignore_index=True)
        log_mlflow_metrics(results_total)

    # Rename the folder to the state and move to another path
    move_files_location()


if __name__ == "__main__":
    main()
