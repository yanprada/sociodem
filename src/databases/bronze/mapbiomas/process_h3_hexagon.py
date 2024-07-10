"""
This module contains functions to process H3 hexagon data.

The main function, `main()`, retrieves the folder path from the CONTRACT dictionary,
counts the number of files in the folder, and then loads the data for each partition.

The `load_data()` function loads data from a specific partition and returns a pandas DataFrame
grouped by "hex_col", "value", and "size".

"""

from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from tqdm import tqdm
import h3
import mlflow

from src.tools.utils.constants import HEX_RESOLUTION
from src.tools.utils.read import Reader
from src.tools.utils.save import save_parquet_decorator
from src.tools.data_contract.mapbiomas_data_contract import get_mapbiomas_contracts
from src.tools.utils.common import count_files

CONTRACT = get_mapbiomas_contracts("bronze")
mlflow.set_experiment("mapbiomas bronze")


@save_parquet_decorator("bronze", CONTRACT["mapbiomas_2022"], save_pq=False)
def load_data(partition: int, folder_path: str):
    """
    Load data from a specific partition.

    Args:
        partition (int): The partition number.
        folder_path (str): The path to the folder containing the data.

    Returns:
        pandas.DataFrame: The loaded data grouped by "hex_col", "value", and "size".
    """
    path = os.path.join(folder_path, f"mapbiomas_2022_{partition}.parquet")
    reader = Reader(CONTRACT["mapbiomas_2022"])
    df = reader.read_parquet(path)
    df["hex_col"] = df.apply(lambda x: h3.geo_to_h3(x.lat, x.lng, HEX_RESOLUTION), 1)
    return df.groupby(["hex_col", "value"], as_index=False).size()


def process_partition(partition: int, folder_path: str):
    """
    Process a partition of data.

    Args:
        partition (int): The partition number.
        folder_path (str): The path to the folder containing the data.

    Returns:
        The processed data.

    """
    df = load_data(partition, folder_path)
    temp = df.groupby(["hex_col"])["size"].sum()
    with mlflow.start_run(run_name=str(partition)):
        mlflow.log_metric("num_hex", df["hex_col"].nunique())
        mlflow.log_metric("num_points", df["size"].sum())
        mlflow.log_metric("mean_points_per_hex", temp.mean())
        mlflow.log_metric("std_point_per_hex", temp.std())
    return df


def run_process(folder_path: str, start: int, end: int):
    """
    Run the process to process H3 hexagon data in parallel.

    Args:
        folder_path (str): The path to the folder containing the data files.
        start (int): The starting index of the data files to process.
        end (int): The ending index of the data files to process.
    """
    num_files = end - start
    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_partition, partition, folder_path)
            for partition in range(start, end)
        ]
        for future in tqdm(
            as_completed(futures),
            total=num_files,
            desc="Processing H3 hexagon data batch",
        ):
            try:
                _ = future.result()

            except Exception as e:
                print(e)


def main():
    """
    This is the main function that processes the H3 hexagon data.

    It retrieves the folder path from the CONTRACT dictionary,
    counts the number of files in the folder,
    and then loads the data for each partition in parallel using ProcessPoolExecutor.
    """
    folder_path = CONTRACT["mapbiomas_2022"]["physicalPath"]
    num_files = count_files(folder_path)
    start = 0
    batch = 100
    for i in tqdm(range(start, num_files, batch), desc="Processing h3 data"):
        end = i + batch if i + batch < num_files else num_files
        run_process(folder_path, i, end)


if __name__ == "__main__":
    main()
