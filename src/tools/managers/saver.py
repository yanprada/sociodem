"""
This module provides functions for saving data as Parquet files and storing them in a database.

Functions:
- save_parquet_decorator: Decorator function that saves the
    output of a decorated function as a Parquet file.
- save_in_db: Save the data to the database.
- save_parquet: Save a DataFrame or Series as a Parquet file.
- save_particionado: Save the DataFrame to a file, either as a
    Parquet file or using Dask for partitioning.
- converte_geometria: Convert geometries of type 'object' to string in a DataFrame.
- save_as_dask: Partition a file with more than 100Mb into smaller partitions using Dask.
"""

from typing import Union, Callable
import os
import gc
import yaml
import pandas as pd
import dask.dataframe as dd

from src.tools.databases.data_connection.connection import DBConnection


def save_parquet_decorator(
    medallon: str,
    save_pq: bool = True,
    save_db: bool = True,
) -> Callable:
    """
    Decorator function that saves the output of a decorated function as a Parquet file.

    Args:
        medallon (str): The medallon string.
        save_pq (bool, optional): Flag indicating whether to save the result
                    as a Parquet file. Defaults to True.
        save_db (bool, optional): Flag indicating whether to save the result
                    in the database. Defaults to True.

    Returns:
        None
    """

    def wrap_outer(funcao):
        def wrapper(*args, **kwargs):
            result = funcao(*args, **kwargs)
            contract = kwargs.get("contract", None)
            if contract is None:
                raise ValueError("Contract not provided in kwargs.")
            path = contract["physicalPath"].split(".")[0]
            if isinstance(result, (pd.DataFrame, pd.Series)):
                if len(result) == 0:
                    return result
                if save_db:
                    save_in_db(result, medallon, contract)
                if save_pq:
                    save_parquet(result, path, **kwargs)
            elif isinstance(result, tuple):
                for i, obj in enumerate(result):
                    if isinstance(obj, pd.DataFrame):
                        path = "_".join([path, str(i)])
                        database_contract_single = contract[i]
                        if len(result) == 0:
                            return result
                        if save_pq:
                            save_parquet(obj, path, **kwargs)
                        if save_db:
                            save_in_db(obj, medallon, database_contract_single)
            return result

        return wrapper

    return wrap_outer


def save_in_db(
    df_data: Union[pd.DataFrame, pd.Series], medallon: str, database_contract: dict
) -> None:
    """
    Saves the given DataFrame to a database table.

    Args:
        df_data (pd.DataFrame): The DataFrame to be saved.
        medallon (str): The medallon identifier.
        database_contract (dict): The contract specifying the database table structure.
    """
    with DBConnection(medallon) as conn:
        conn.add_table(df_data, database_contract)


def save_parquet(df_data: Union[pd.Series, pd.DataFrame], path: str, **kwargs) -> None:
    """
    Save a DataFrame as a Parquet file.

    Parameters:
        df_data (pd.DataFrame): The DataFrame to be saved.
        path (str): The path where the Parquet file will be saved.
        **kwargs: Additional keyword arguments.
    """
    filename = kwargs.get("filename", None)
    if filename is not None:
        path = "".join([path, filename, ".parquet"])
    else:
        path = "".join([path, ".parquet"])
    df_data = converte_geometria(df_data)
    if isinstance(df_data, pd.DataFrame):
        df_data.columns = [str(i).lower() for i in df_data.columns]
    elif isinstance(df_data, pd.Series):
        df_data.name = str(df_data.name).lower()
        df_data = df_data.to_frame()
    save_particionado(df_data, path)


def save_particionado(df_data: pd.DataFrame, path: str):
    """
    Save the DataFrame `df_data` to a file specified by `path`.
    If the total memory usage of `df_data` is less than or equal to
    `limit_partition`, the DataFrame is saved as a parquet file.
    Otherwise, it is saved using the `save_as_dask` function.

    Args:
        df_data: The DataFrame to be saved.
        path: The path and filename to save the DataFrame.
    """
    folder_path = "/".join(path.split("/")[:-1])
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    total_size = df_data.memory_usage(deep=True).sum()
    limit_partition = 100 * (2**20)
    if total_size <= limit_partition:
        df_data.to_parquet(path)
    else:
        save_as_dask(df_data, path, total_size, limit_partition)


def converte_geometria(
    df_data: Union[pd.DataFrame, pd.Series],
) -> Union[pd.DataFrame, pd.Series]:
    """
    Convert the geometries that are of type 'object' to string.

    Args:
        df_data (pd.DataFrame): The DataFrame to be converted.

    Returns:
        pd.DataFrame: The converted DataFrame.
    """

    cols_object = list(df_data.select_dtypes("O").columns)
    if not any("geom" in i.lower() for i in cols_object):
        return df_data
    for col in cols_object:
        if "geom" in col.lower() and not isinstance(df_data[col].iloc[0], str):
            df_data[col] = df_data[col].apply(
                lambda geom: geom.wkt if geom is not None else "-1"
            )
    return df_data


def add_partition_size_to_yaml(filename: str, n_particoes: int) -> None:
    """
    Add partition size information to a YAML file.

    Args:
        filename (str): The name of the file.
        n_particoes (int): The number of partitions.
    """
    contracts_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        "data_contract",
        "validation",
        "contract_partitions.yaml",
    )
    with open(contracts_path, "r", encoding="utf-8") as file:
        existing_data = yaml.safe_load(file)
        if existing_data is None:
            existing_data = {}
        existing_data[filename] = int(n_particoes)
    with open(contracts_path, "w", encoding="utf-8") as file:
        yaml.dump(existing_data, file)


def save_as_dask(
    df_data: pd.DataFrame, filename: str, total_size: int, limit_partition: int
) -> None:
    """
    Partition a file with more than 100Mb into smaller partitions using dask.

    Args:
        df_data (pd.DataFrame): The DataFrame to be partitioned.
        filename (str): The path and filename to save the DataFrame.
        total_size (int): The total memory usage of the DataFrame.
        limit_partition (int): The maximum memory size for each partition.

    """
    filename = filename.replace(".parquet", "/")
    n_particoes = total_size // limit_partition + 1
    # add_partition_size_to_yaml(filename, n_particoes)
    for col in df_data.filter(like="geom").columns:
        if df_data[col].dtype != "O":
            df_data[col] = df_data[col].apply(str)
    ddf_data = dd.from_pandas(df_data, npartitions=int(n_particoes))  # type: ignore
    del df_data
    gc.collect()
    ddf_data.to_parquet(filename)
