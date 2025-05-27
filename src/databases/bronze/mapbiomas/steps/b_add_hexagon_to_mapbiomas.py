"""
This module contains functions to process and save MapBiomas data as a DataFrame.

The main function `main()` processes the MapBiomas image and saves the data
in a Parquet file.
The `save_mapbiomas()` function is a decorator that saves the MapBiomas
DataFrame to a file or database.
The `process_block()` function processes a block of the image and returns a DataFrame
with the coordinates and values of the pixels.
"""

import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import gc
from typing import List
import h3
import mlflow
import rasterio
import rasterio.windows
import geopandas as gpd
import pandas as pd
import numpy as np
from tqdm import tqdm

from src.tools.utils.constants import CRS_GLOBAL, HEX_RESOLUTION

from src.tools.managers.saver_manager import save_parquet_decorator

from src.databases.bronze.mapbiomas.config import (
    manager,
    EXPERIMENT_NAME,
    CONTRACTS_BRONZE,
    CONTRACTS_RAW,
    YEARS,
)


module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)

mlflow.set_experiment(EXPERIMENT_NAME)


def df_to_gdf(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Convert a pandas DataFrame with latitude and longitude columns to a GeoDataFrame
    and transform it to a global CRS (EPSG:4326).

    Args:
        df (pd.DataFrame): The input DataFrame with 'lat' and 'lng' columns.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame with Point geometries in global CRS.
    """
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lng, df.lat))
    gdf.set_crs({"init": CRS_GLOBAL}, inplace=True)
    return gdf


def process_block(
    window: rasterio.windows.Window, transform, file_path: str
) -> pd.DataFrame:
    """
    Process a block of data from a raster file and transform it into a pandas DataFrame.

    Args:
        window (rasterio.windows.Window): The window defining the block of data to be processed.
        transform (affine.Affine): The affine transformation matrix for converting
        pixel coordinates to world coordinates.
        file_path (str): The path to the raster file.

    Returns:
        pandas.DataFrame: A DataFrame containing the transformed
        coordinates and values of the block.

    """
    with rasterio.open(file_path) as src:
        block = src.read(1, window=window)
        rows, cols = block.shape
        indices = np.indices((rows, cols)).reshape(2, -1)
        indices[0] += window.row_off
        indices[1] += window.col_off
        x_coords, y_coords = rasterio.transform.xy(transform, indices[0], indices[1])
        values = block.flatten()
        return pd.DataFrame({"lng": x_coords, "lat": y_coords, "value": values})


def generate_windows(height: int, width: int, block_size: int):
    """
    Generate windows of a specified block size over a given height and width.

    Parameters:
        height (int): The total height of the image.
        width (int): The total width of the image.
        block_size (int): The size of each window block.

    Yields:
        rasterio.windows.Window: A window object representing a block of the image.
    """
    for i in range(0, height, block_size):
        for j in range(0, width, block_size):
            yield rasterio.windows.Window(j, i, block_size, block_size)  # type: ignore


@save_parquet_decorator("bronze")
def save_partitions(
    all_dfs: List[pd.DataFrame], partition: int, year: int, **kwargs
) -> int:
    """
    Save the partitions of dataframes into separate files.

    Args:
        all_dfs (List[pd.DataFrame]): A list of dataframes to be concatenated and saved.
        partition (int): The current partition number.
        year (int) : The Mapbiomas year being processed.
        **kwargs: Additional keyword arguments to be passed to the save function.

    Returns:
        int: The updated partition number.

    """
    df = pd.concat(all_dfs, ignore_index=True)
    df["year"] = year
    df = df_to_gdf(df)
    df["hex_col"] = df.apply(lambda x: h3.geo_to_h3(x.lat, x.lng, HEX_RESOLUTION), 1)
    df = df.groupby(["hex_col", "value"], as_index=False).size()
    add_to_mlflow(df, partition)
    return df


def process_batch(
    file_path: str,
    size_list: List[int],
    batch: int,
    partition: int,
    year: int,
) -> int:
    """
    Process a batch of raster blocks and transform them into dataframes.

    Args:
        file_path (str): The path to the raster file.
        size_list (List[int]): A list containing the block size and batch size.
        batch (int): The index of the current batch.
        partition (int): The current partition number.
        year (int) : The Mapbiomas year being processed.

    Returns:
        int: The updated partition number.

    Raises:
        None

    This function takes a batch of raster blocks from a given raster file and
    processes them one by one.
    Each block is transformed into a dataframe, and the resulting dataframes
    are concatenated and saved
    as a MapBiomas dataframe. The function returns the updated partition number.

    Note:
        The function uses the `rasterio` library to read the raster file and
        generate windows for processing.
        It also utilizes the `ProcessPoolExecutor` from the `concurrent.futures`
        module to parallelize the
        processing of blocks.

    Example:
        >>> file_path = "/path/to/raster/file.tif"
        >>> size_list = [256, 10]
        >>> batch = 0
        >>> partition = 1
        >>> year = 2022
        >>> new_partition = process_batch(file_path, size_list, batch, partition, year)
    """
    all_dfs = []
    contract = CONTRACTS_BRONZE[f"brasil_coverage_{year}"].copy()
    kwargs = {"filename": f"brasil_coverage_{year}_{partition}", "contract": contract}
    with rasterio.open(file_path) as src:
        transform = src.transform
        windows = list(generate_windows(src.height, src.width, size_list[0]))[
            batch : batch + size_list[1]
        ]
        with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            future_to_window = {
                executor.submit(process_block, window, transform, file_path): window
                for window in windows
            }

            for future in tqdm(
                as_completed(future_to_window),
                total=len(windows),
                desc="Processing MapBiomas",
            ):
                try:
                    block_df = future.result()
                    if (
                        block_df.value.nunique() == 1
                        and block_df.value.unique().squeeze() == 0
                    ):
                        del block_df
                        gc.collect()
                        continue
                    all_dfs.append(block_df.query("value != 0"))
                    if sum(len(df) for df in all_dfs) > 1e5:
                        df = save_partitions(all_dfs, partition, year, **kwargs)
                        del df
                        gc.collect()
                        partition += 1
                        all_dfs = []
                except Exception as e:
                    print(f"Error processing window: {e}")

    if all_dfs:
        partition = save_partitions(all_dfs, partition, year, **kwargs)
    return partition


def add_to_mlflow(df: pd.DataFrame, partition: int) -> None:
    """
    Process a partition of data.

    Args:
        df (pd.DataFrame): The data to be processed.
        partition (int): The partition number.
    """
    temp = df.groupby(["hex_col"])["size"].sum()
    with mlflow.start_run(run_name=str(partition), nested=True):
        mlflow.log_metric("num_hex", df["hex_col"].nunique())
        mlflow.log_metric("num_points", df["size"].sum())
        mlflow.log_metric("mean_points_per_hex", temp.mean())
        mlflow.log_metric("std_point_per_hex", temp.std())


def main() -> None:
    """
    Main function that processes a raster file and calls the process_batch function.

    This function opens a raster file specified by the file_path,
    retrieves the transform information,
    and then calls the process_batch function to process the file in batches.
    """
    date = pd.Timestamp.now().strftime("%d/%m/%Y %H:%M")
    manager.update_mlflow_runs(date)
    for year in tqdm(YEARS, desc="Processing Years"):
        contract_key = f"brasil_coverage_{year}"
        with mlflow.start_run(run_name=str(year)):
            filename = "".join([CONTRACTS_RAW[contract_key]["tableName"], ".tif"])
            file_path = os.path.join(
                CONTRACTS_RAW[contract_key]["physicalPath"], filename
            )
            block_size = 2048
            batch_size = 100  # Limit to a small number for quick profiling
            with rasterio.open(file_path) as src:
                windows = list(generate_windows(src.height, src.width, block_size))
            partition = 0
            for batch in tqdm(
                range(0, len(windows), batch_size), desc="Processing Batches"
            ):
                partition = process_batch(
                    file_path, [block_size, batch_size], batch, partition, year
                )
                gc.collect()
    manager.update_last_run()


if __name__ == "__main__":
    main()
