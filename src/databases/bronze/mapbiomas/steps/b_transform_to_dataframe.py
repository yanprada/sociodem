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
import rasterio
import rasterio.windows
import pandas as pd
import numpy as np
from tqdm import tqdm

from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.mapbiomas.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
manager.update_status("running_step_2")

CONTRACTS = execution_parameters["data_contracts"][0]


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


@save_parquet_decorator("bronze", CONTRACTS["mapbiomas_2022"], save_db=False)
def save_mapbiomas(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Save the MapBiomas dataframe to a file or database.

    Args:
        df (pd.DataFrame): The MapBiomas dataframe to be saved.
        **kwargs: Additional keyword arguments for saving options.

    Returns:
        pd.DataFrame: The saved MapBiomas dataframe.
    """
    return df


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
            yield rasterio.windows.Window(j, i, block_size, block_size)


def save_partitions(all_dfs: List[pd.DataFrame], partition: int) -> int:
    """
    Save the partitions of dataframes into separate files.

    Args:
        all_dfs (List[pd.DataFrame]): A list of dataframes to be concatenated and saved.
        partition (int): The current partition number.

    Returns:
        int: The updated partition number.

    """
    df = pd.concat(all_dfs, ignore_index=True)
    kwargs = {"filename": f"mapbiomas_2022_{partition}"}
    df = save_mapbiomas(df, **kwargs)
    del df
    gc.collect()
    partition += 1
    return partition


def process_batch(
    file_path: str, block_size: int, batch: int, batch_size: int, partition: int
) -> int:
    """
    Process a batch of raster blocks and transform them into dataframes.

    Args:
        file_path (str): The path to the raster file.
        block_size (int): The size of each block.
        batch (int): The index of the current batch.
        batch_size (int): The number of blocks to process in each batch.
        partition (int): The current partition number.

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
        >>> block_size = 256
        >>> batch = 0
        >>> batch_size = 10
        >>> partition = 1
        >>> new_partition = process_batch(file_path, block_size, batch, batch_size, partition)
    """
    all_dfs = []

    with rasterio.open(file_path) as src:
        transform = src.transform
        windows = list(generate_windows(src.height, src.width, block_size))[
            batch : batch + batch_size
        ]
        with ProcessPoolExecutor() as executor:
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
                        partition = save_partitions(all_dfs, partition)
                        all_dfs = []
                except Exception as e:
                    print(f"Error processing window: {e}")

    if all_dfs:
        partition = save_partitions(all_dfs, partition)
    return partition


def main() -> None:
    """
    Main function that processes a raster file and calls the process_batch function.

    This function opens a raster file specified by the file_path,
    retrieves the transform information,
    and then calls the process_batch function to process the file in batches.
    """
    for year in range(2018, 2023):
        filename = "".join([CONTRACTS["raw_data"]["tableName"], ".tif"]).replace(
            "2022", str(year)
        )
        file_path = os.path.join(
            CONTRACTS["raw_data"]["physicalPath"], filename
        ).replace("2022", str(year))
        block_size = 2048
        batch_size = 100  # Limit to a small number for quick profiling

        with rasterio.open(file_path) as src:
            windows = list(generate_windows(src.height, src.width, block_size))
        partition = 0
        for batch in tqdm(
            range(0, len(windows), batch_size), desc="Processing Batches"
        ):
            partition = process_batch(
                file_path, block_size, batch, batch_size, partition
            )
            gc.collect()
    manager.update_status("finished_step_2")
    manager.update_last_run()


if __name__ == "__main__":
    main()
