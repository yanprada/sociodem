"""
This script reads parquet files from a specified path,
filters the data based on a bounding box, and saves the filtered
data as parquet files.

The script contains the following functions:
- read_files(file): Reads the parquet files from the specified
    path and returns the data.
- save_file(df, **kwargs): Saves the DataFrame in the specified path.
- main(): Reads files from a specified bucket and prefix, filters
    the data, and saves the filtered files.
"""

import os
import gc
from tqdm import tqdm
import pandas as pd
from shapely import wkb


from src.tools.managers.http_requester import (
    HttpRequesterOvertureMaps,
)
from src.tools.managers.saver import save_parquet_decorator


from src.tools.managers.execution import ExecutionManager
from src.databases.bronze.transportation.config import EXECUTION_ID, BASE_PARAMS


manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, overwrite=True)
module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)

TRANSPORT_CONTRACTS = execution_parameters["data_contracts"]["transportation_bronze"]


@save_parquet_decorator("bronze", TRANSPORT_CONTRACTS["pois"], save_db=False)
def save_file(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Saves the DataFrame in the specified path.

    Args:
        df (DataFrame): The DataFrame to be saved.
        **kwargs: Additional keyword arguments.

    Returns:
        DataFrame: The saved DataFrame.
    """
    df["geometry"] = df["geometry"].apply(wkb.loads)
    return df


def main():
    """
    Reads files from a specified bucket and prefix, filters
    the data based on a bounding box, and saves the filtered files.
    """
    download_path = TRANSPORT_CONTRACTS["raw_data"]["physicalPath"]
    prefix = "release/2024-07-22.0/theme=transportation/"
    request = HttpRequesterOvertureMaps(prefix, download_path)
    for i, file in tqdm(enumerate(download_path), desc="Reading files"):
        path = os.path.join(TRANSPORT_CONTRACTS["raw_data"]["physicalPath"], file)
        df = request.read_files_bbox_brazil(path)
        kwargs = {"filename": f"partition_{i}"}
        save_file(df, **kwargs)
        del df
        gc.collect()
    manager.update_last_run()
