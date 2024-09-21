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
import pandas as pd

from src.tools.utils.constants import HEX_RESOLUTION
from src.tools.utils.reader import Reader
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.common import count_files
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.mapbiomas.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
CONTRACTS = execution_parameters["data_contracts"][0]
manager.update_status("running_step_3")


EXPERIMENT_ID = execution_parameters["mlflow_experiment"]
mlflow.set_experiment(EXPERIMENT_ID)


@save_parquet_decorator("bronze", CONTRACTS["mapbiomas_2022"])
def load_data(partition: int, folder_path: str) -> pd.DataFrame:
    """
    Load data from a specific partition.

    Args:
        partition (int): The partition number.
        folder_path (str): The path to the folder containing the data.

    Returns:
        pandas.DataFrame: The loaded data grouped by "hex_col", "value", and "size".
    """
    path = os.path.join(folder_path, f"mapbiomas_2022_{partition}.parquet")
    reader = Reader()
    df = reader.read_parquet(path)
    df["hex_col"] = df.apply(lambda x: h3.geo_to_h3(x.lat, x.lng, HEX_RESOLUTION), 1)
    return df.groupby(["hex_col", "value"], as_index=False).size()


def process_partition(partition: int, folder_path: str) -> pd.DataFrame:
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
    with mlflow.start_run(run_name=str(partition), nested=True):
        mlflow.log_metric("num_hex", df["hex_col"].nunique())
        mlflow.log_metric("num_points", df["size"].sum())
        mlflow.log_metric("mean_points_per_hex", temp.mean())
        mlflow.log_metric("std_point_per_hex", temp.std())
    return df


def run_process(folder_path: str, start: int, end: int) -> None:
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


def main() -> None:
    """
    This is the main function that processes the H3 hexagon data.

    It retrieves the folder path from the CONTRACT dictionary,
    counts the number of files in the folder,
    and then loads the data for each partition in parallel using ProcessPoolExecutor.
    """
    date = pd.Timestamp.now().strftime("%d/%m/%Y %H:%M")
    manager.update_mlflow_runs(date)
    with mlflow.start_run(run_name=str(date)):
        folder_path = CONTRACTS["mapbiomas_2022"]["physicalPath"]
        num_files = count_files(folder_path)
        start = 0
        batch = 100
        for i in tqdm(range(start, num_files, batch), desc="Processing h3 data"):
            end = i + batch if i + batch < num_files else num_files
            run_process(folder_path, i, end)
    manager.update_status("finished_step_3")
    manager.update_last_run()


if __name__ == "__main__":
    main()
