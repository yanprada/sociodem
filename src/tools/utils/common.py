"""
This module contains utility functions for common tasks.

Functions:
- write_log(message, level="info"): Write a log message with the specified level and timestamp.
- get_column_memory_usage(df): Calculate the memory usage of each column in a DataFrame.
"""

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def write_log(message, level="info"):
    """
    Write a log message with the specified level and timestamp.

    Parameters:
    message (str): The log message to be written.
    level (str): The log level, either 'info' or 'error'. Default is 'info'.

    Returns:
    None
    """
    # Get logger instance
    logger = logging.getLogger()

    # Write log message
    if level == "info":
        logger.info("%s", message)
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
