"""
This module contains utility functions for common tasks.

Functions:
- write_log(message, level="info"): Write a log message with the specified level and timestamp.
- get_column_memory_usage(df): Calculate the memory usage of each column in a DataFrame.
"""

import os
import logging
from src.tools.utils.config import get_contract


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

CONTRACT_PARTITIONS = get_contract("contract_partitions.yaml")


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


def get_column_memory_usage(df):
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


def check_data_consistency(path) -> dict:
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
