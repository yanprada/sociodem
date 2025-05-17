"""
This module contains utility functions for common tasks.

Functions:
- write_log(message, level="info"): Write a log message with the specified level and timestamp.
- get_column_memory_usage(df): Calculate the memory usage of each column in a DataFrame.
"""

import os
import random
import string
import logging
import ctypes
from functools import lru_cache
import yaml
import pandas as pd
from mlflow.tracking import MlflowClient

from src.tools.data_contract.validation_data_contract import get_validation_partitions
from src.tools.utils.save import add_partition_size_to_yaml
from src.tools.databases.data_connection.connection import DBConnection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

CONTRACT_PARTITIONS = get_validation_partitions()


def write_log(message, level="info"):
    """
    Write a log message with the specified level and timestamp.

    Parameters:
    message (str): The log message to be written.
    level (str): The log level, either 'info', 'warning', or 'error'. Default is 'info'.
    """
    logger = logging.getLogger()
    if level == "info":
        logger.info("%s", message)
    elif level == "warning":
        logger.warning("%s", message)
    else:
        logger.error("%s", message)


def count_files(folder_path):
    """
    Count the number of files in a given folder.

    Args:
        folder_path (str): The path to the folder.

    Returns:
        int: The number of files in the folder.
    """
    file_count = 0
    for _, _, files in os.walk(folder_path):
        file_count += len(files)
    return file_count


def get_column_memory_usage(df: pd.DataFrame) -> dict:
    """
    Calculate the memory usage of each column in a DataFrame.

    Parameters:
    df (pandas.DataFrame): The DataFrame for which to calculate the memory usage.

    Returns:
    dict: A dictionary where the keys are column names and the values are
    the memory usage of each column.
    """
    column_memory_usage = {}
    for column in df.columns:
        column_memory_usage[column] = round(
            df[column].memory_usage(deep=True) / 1024, 2
        )
    column_memory_usage = dict(
        sorted(column_memory_usage.items(), key=lambda x: x[1], reverse=True)
    )
    for column, memory_usage in column_memory_usage.items():
        column_memory_usage[column] = str(memory_usage) + " Kb"
    return column_memory_usage


def check_data_consistency(path: str) -> dict:
    """
    Check the consistency of data in the given path.

    Args:
        path (str): The path to check for data consistency.

    Returns:
        dict: A dictionary containing subfolders as keys and a boolean value indicating
              whether the number of files in the subfolder matches the expected number
              of files.

    """
    subfolders = [
        subfolder for subfolder in CONTRACT_PARTITIONS.keys() if path in subfolder
    ]
    data_wrong = {}
    for subfolder in subfolders:
        expected_num_files = CONTRACT_PARTITIONS[subfolder]
        file_count = len(os.listdir(subfolder))
        data_wrong[subfolder] = expected_num_files > file_count
    return data_wrong


def add_test_to_yaml(yaml_path: str, key: str, value: str) -> None:
    """
    Add key-value pair to a YAML file.

    This function reads the existing YAML file, adds the specified key-value pair,
    and writes the updated data back to the file.

    Args:
        yaml_path (str): The path to the YAML file.
        key (str): The key to be added.
        value (str): The value associated with the key.
    """
    with open(yaml_path, "r", encoding="utf-8") as file:
        existing_data = yaml.safe_load(file)
        if existing_data is None:
            existing_data = {}
        existing_data[key] = value
    with open(yaml_path, "w", encoding="utf-8") as file:
        yaml.dump(existing_data, file)


def get_test_yaml(yaml_path: str) -> dict:
    """
    Get the content of a YAML file.

    Args:
        yaml_path (str): The path to the YAML file.

    Returns:
        dict: The content of the YAML file.
    """
    with open(yaml_path, "r", encoding="utf-8") as file:
        existing_data = yaml.safe_load(file)
    return existing_data


def check_file_exists_in_disk(
    filename: str, filepath: str, extension: str = ".parquet"
) -> bool:
    """
    Check if a file exists in the given filepath.

    Args:
        filename (str): The name of the file.
        filepath (str): The path to the directory where the file should be located.
        extension (str, optional): The file extension. Defaults to ".parquet".

    Returns:
        bool: True if the file exists, False otherwise.
    """
    path_large_file = os.path.join(filepath, filename)
    path_small_file = os.path.join(filepath, "".join([filename, extension]))
    exist_small_file = os.path.exists(path_small_file)
    exist_large_file = os.path.exists(path_large_file)
    exist_file = exist_small_file or exist_large_file
    if exist_large_file:
        try:
            num_partitions = CONTRACT_PARTITIONS["/".join([path_large_file, ""])]
            num_files = len(os.listdir(path_large_file))
        except KeyError:
            num_files = len(os.listdir(path_large_file))
            add_partition_size_to_yaml("/".join([path_large_file, ""]), num_files)
            num_partitions = num_files
        exist_file = num_partitions == num_files
        if num_partitions < num_files:
            raise ValueError(
                f"Number of partitions is greater than number of files in {path_large_file}"
            )
    return exist_file


@lru_cache(maxsize=10)
def check_file_exists_in_db(
    conn: DBConnection, path_saved: str, condition: str = "LIMIT 1"
):
    """
    Check if a file exists in the database.

    Args:
        conn (DBConnection): The database connection object.
        path_saved (str): The path of the file to check.
        condition (str, optional): The condition to apply to the query. Defaults to "LIMIT 1".

    Returns:
        bool: True if the file exists in the database, False otherwise.
    """
    try:
        file_exists = conn.query_database(f"SELECT * FROM {path_saved} {condition}")
        return len(file_exists) > 0
    except:
        return False


def get_all_mlflow_runs(client, experiment_id):
    """
    Retrieves all MLflow runs for a given experiment ID.

    Args:
        client (mlflow.tracking.MlflowClient): The MLflow client object.
        experiment_id (str): The ID of the experiment.

    Returns:
        list: A list of MLflow runs.

    """
    runs = []
    page_token = None

    while True:
        result = client.search_runs(
            experiment_ids=[experiment_id],
            order_by=["attributes.start_time desc"],
            max_results=1000,
            page_token=page_token,
        )
        runs.extend(result)
        if result.token is None:
            break
        page_token = result.token

    return runs


def get_ml_flow_data(experiment_name: str) -> pd.DataFrame:
    """
    Retrieves data from MLflow for a given experiment.

    Args:
        experiment_name (str): The name of the MLflow experiment.

    Returns:
        pd.DataFrame: A DataFrame containing the retrieved data.
    """

    client = MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    experiment_id = experiment.experiment_id
    runs = get_all_mlflow_runs(client, experiment_id)
    data = []
    for run in runs:
        run_data = run.data.to_dictionary()
        row = {
            "run_id": run.info.run_id,
            "start_time": run.info.start_time,
            "end_time": run.info.end_time,
            "status": run.info.status,
            **run_data["params"],
            **run_data["metrics"],
            **run_data["tags"],
        }
        data.append(row)
    return pd.DataFrame(data)


def get_db_path(contract: dict) -> str:
    """
    Get the path of a database table based on the contract.

    Args:
        contract (dict): The contract containing the schema and table name.

    Returns:
        str: The path of the database table.
    """
    return ".".join([contract["schema"], contract["tableName"]])


def add_year_to_contract(contract: dict, year: int) -> dict:
    """
    Add the year to the contract.

    Args:
        contract (dict): The contract containing the schema and table name.
        year (int): The year to be added to the contract.

    Returns:
        dict: The updated contract.
    """
    contract["tableName"] = contract["tableName"].format(year=year)
    contract["physicalPath"] = contract["physicalPath"].format(year=year)
    return contract


def get_column_name(contract: dict, column_name: str) -> str:
    """
    Get the column name based on the contract.

    Args:
        contract (dict): The contract containing the schema and table name.
        column_name (str): The column name.

    Returns:
        str: The column name.
    """
    column_list = contract["columns"]
    for column in column_list:
        if column["column"] == column_name:
            return column["column_name_raw"]
    raise ValueError(f"Column {column_name} not found in contract")


def generate_random_string(length):
    """
    Generates a random string of specified length.

    Args:
        length (int): The length of the random string.

    Returns:
        str: The random string.
    """
    letters = string.ascii_letters + string.digits
    return "".join(random.choice(letters) for _ in range(length))


def trim_memory() -> int:
    """
    Trims the memory used by the process.
    This function is used to free up memory that is no longer needed.
    Returns:
        int: The result of the malloc_trim function.
    """
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)
