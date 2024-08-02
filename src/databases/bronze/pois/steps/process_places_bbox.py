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
from src.tools.utils.read import Reader
from src.tools.data_contract.pois_data_contract import get_pois_contracts
from src.tools.utils.constants import BBOX_BRAZIL
from src.tools.utils.save import save_parquet_decorator

tqdm.pandas()
POIS_CONTRACTS = get_pois_contracts("bronze")


def read_files(file: str) -> pd.DataFrame:
    """
    Reads the parquet files from the specified path and returns the data.

    Args:
        file (str): The name of the parquet file.

    Returns:
        DataFrame: The data read from the parquet file.
    """
    reader = Reader()
    path = os.path.join(POIS_CONTRACTS["datalake"]["physicalPath"], file)
    return reader.read_parquet(path)


@save_parquet_decorator("bronze", POIS_CONTRACTS["pois"], save_db=False)
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
    path = POIS_CONTRACTS["datalake"]["physicalPath"]
    files = os.listdir(path)
    for i, file in tqdm(enumerate(files), desc="Reading files"):
        df = read_files(file)
        df["in_brazil"] = df["bbox"].progress_apply(
            lambda bb: bb["xmin"] > BBOX_BRAZIL["xmin"]
            and bb["xmax"] < BBOX_BRAZIL["xmax"]
            and bb["ymin"] > BBOX_BRAZIL["ymin"]
            and bb["ymax"] < BBOX_BRAZIL["ymax"]
        )
        df = df[df["in_brazil"]]
        kwargs = {"filename": f"partition_{i}"}
        save_file(df, **kwargs)
        del df
        gc.collect()
