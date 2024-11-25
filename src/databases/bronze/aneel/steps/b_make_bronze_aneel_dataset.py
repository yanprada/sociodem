"""
This script is used to download files from the ANEEL website based on ANEEL IDs.

It connects to a database, retrieves ANEEL IDs for a specific year, and then uses 
those IDs to download files from the ANEEL website.

The downloaded files are saved to a specified output path.

Usage:
    - Ensure that the necessary database connection and contract 
        configurations are set up correctly.
    - Run the script to download the files.

"""

import os
import gc
import glob
import multiprocessing
import concurrent.futures
from collections import defaultdict
from typing import List, Tuple
from functools import lru_cache
from tqdm import tqdm
import fiona
import mlflow
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.reader import Reader
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.common import (
    check_file_exists_in_db,
    get_ml_flow_data,
    write_log,
    get_db_path,
)
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.aneel.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
manager.update_status("running_step_2")

CONTRACTS_BRONZE = execution_parameters["data_contracts"]["aneel_bronze"]


EXPERIMENT_NAME = execution_parameters["mlflow_experiment"]
mlflow.set_experiment(EXPERIMENT_NAME)


def add_to_mlflow(df: gpd.GeoDataFrame, database: str, company_id: str) -> None:
    """
    Logs the parameters and metrics to MLflow for the given GeoDataFrame.

    Args:
        df (gpd.GeoDataFrame): The GeoDataFrame to log.
        database (str): The name of the database.
        company_id (str): The ID of the company.
    """
    mlflow.log_param("database", database)
    mlflow.log_param("company", df.dist.unique()[0])
    mlflow.log_metric("num_rows", len(df))
    if database == "ucbt":
        mlflow.log_metric("sum_energy", df.filter(regex="ene_").sum().sum())
        mlflow.log_metric("mean_energy", df.filter(regex="ene_").sum(axis=1).mean())
        mlflow.log_metric("std_energy", df.filter(regex="ene_").sum(axis=1).std())
    else:
        mlflow.log_metric("sum_energy", 0)
        mlflow.log_metric("mean_energy", 0)
        mlflow.log_metric("std_energy", 0)


def read_aneel_wraper_large_file(database: str, company_id: str, **kwargs):
    """
    Wrapper function to read ANEEL data from a specific database and company.

    Args:
        database (str): The name of the database.
        company_id (str): The ID of the company.
        **kwargs: Additional keyword arguments.
    """

    @save_parquet_decorator(medallon="bronze", contract=CONTRACTS_BRONZE[database])
    def read_aneel_in_chunks(layer_src, start, chunk_size, company_id, **kwargs):
        """
        Read a chunk of features from a layer source into a GeoDataFrame.

        Args:
            layer_src (Layer): The source layer containing the features.
            start (int): The starting index of the chunk.
            chunk_size (int): The size of the chunk to be saved.
            company_id (str): The ID of the company.
            **kwargs: Additional keyword arguments to be passed to the reader.

        Returns:
            GeoDataFrame: The GeoDataFrame containing the saved features.
        """
        reader = Reader()
        features = []
        for feature in layer_src[start : start + chunk_size]:
            features.append(feature)
        geometries = [
            shape(feature["geometry"])
            for feature in features
            if feature["geometry"] is not None
        ]
        properties = [feature["properties"] for feature in features]
        if geometries == []:
            df = reader.read_geopandas(properties)
        else:
            df = reader.read_geopandas(
                properties, geometry=geometries, crs=layer_src.crs
            )
        df = df.drop_duplicates()
        return columns_engineering(df, database, company_id)

    def parallel_process():
        with fiona.Env():
            with fiona.open(path, layer=layers_dict[database]) as layer_src:
                chunk_size = int(5e4)
                num_features = len(layer_src)
                tasks = []
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    for i, start in tqdm(enumerate(range(0, num_features, chunk_size))):
                        comp_file = company_id.split(".")[0]
                        file = "_".join([comp_file, str(i)])
                        task_kwargs = {"filename": "/".join([comp_file, file])}
                        future = executor.submit(
                            read_aneel_in_chunks,
                            layer_src,
                            start,
                            chunk_size,
                            company_id,
                            **task_kwargs,
                        )
                        tasks.append(future)

                    for future in tqdm(
                        concurrent.futures.as_completed(tasks), total=len(tasks)
                    ):
                        with mlflow.start_run(run_name=company_id):
                            df = future.result()
                            add_to_mlflow(df, database, company_id)

    layers_dict = {
        "ramlig": "RAMLIG",
        "ucbt": "UCBT_tab",
        "ponnot": "PONNOT",
        "conj": "CONJ",
    }
    path = os.path.join(
        CONTRACTS_BRONZE["raw_data"]["physicalPath"],
        company_id,
    )
    layers = fiona.listlayers(path)
    if layers_dict[database] not in layers:
        layers_dict = {
            "ramlig": "RAM_LIG",
            "ucbt": "UC_BT_tab",
            "ponnot": "PON_NOT",
            "conj": "CONJ",
        }
    parallel_process()


def add_missing_columns(df: pd.DataFrame, saved_columns: List[str]) -> pd.DataFrame:
    """
    Adds missing columns to a DataFrame and fills them with NaN values.

    Args:
        df (pd.DataFrame): The DataFrame to add missing columns to.
        saved_columns (List[str]): The list of columns that should be present in the DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with missing columns added and filled with NaN values.
    """
    cols_not_in_saved_cols = list(set(saved_columns).difference(set(df.columns)))
    df[cols_not_in_saved_cols] = [np.nan] * len(cols_not_in_saved_cols)
    return df[saved_columns]


def columns_engineering(
    df: gpd.GeoDataFrame, database: str, company_id: str
) -> gpd.GeoDataFrame:
    """
    Function to engineer columns in a GeoDataFrame.

    Args:
        df (gpd.GeoDataFrame): The input GeoDataFrame.
        database (str): The name of the database.
        company_id (str): The ID of the company.

    Returns:
        gpd.GeoDataFrame: The GeoDataFrame with engineered columns.
    """
    exclude_cols = [
        "ceq",
        "uni_tr_d",
        "ctmt",
        "uni_tr_s",
        "car_inst",
        "liv",
        "fic",
        "semred",
        "descr",
        "cod_id",
        "odi",
        "cm",
        "tuc",
        "a1",
        "a2",
        "a3",
        "a4",
        "a5",
        "a6",
    ]
    df = df.drop(columns=exclude_cols, errors="ignore")
    conn = DBConnection("bronze")
    contract = CONTRACTS_BRONZE[database]
    path = get_db_path(contract)
    df["year"] = company_id.split(" - ")[1].split("-")[0]
    df["company_file"] = company_id
    if "conj" in df.columns:
        df["conj"] = df["conj"].fillna("0").astype(int)
    if check_file_exists_in_db(conn, path):
        cols = conn.query_database(f"SELECT * FROM {path} LIMIT 1").columns
        df = add_missing_columns(df, cols)
    df = df.drop_duplicates()
    return df


def read_aneel_wraper(database: str, company_id: str, **kwargs):
    """
    Wrapper function to read ANEEL data from a specific database and company.

    Args:
        database (str): The name of the database.
        company_id (str): The ID of the company.
        **kwargs: Additional keyword arguments.
    """

    @save_parquet_decorator(medallon="bronze", contract=CONTRACTS_BRONZE[database])
    def read_aneel(company_id: str, **kwargs) -> gpd.GeoDataFrame:
        layers_dict = {
            "ramlig": "RAMLIG",
            "ucbt": "UCBT_tab",
            "ponnot": "PONNOT",
            "conj": "CONJ",
        }
        reader = Reader()
        path = os.path.join(CONTRACTS_BRONZE["raw_data"]["physicalPath"], company_id)
        layers = fiona.listlayers(path)
        if layers_dict[database] not in layers:
            layers_dict = {
                "ramlig": "RAM_LIG",
                "ucbt": "UC_BT_tab",
                "ponnot": "PON_NOT",
                "conj": "CONJ",
            }
        df = reader.read_geofile(
            file_path=path,
            driver="FileGDB",
            layer=layers_dict[database],
        )
        df = df.drop_duplicates()
        del reader
        del path
        gc.collect()
        return columns_engineering(df, database, company_id)

    with mlflow.start_run(run_name=company_id):
        df = read_aneel(company_id, **kwargs)
        add_to_mlflow(df, database, company_id)


def read_file(database: str, company_id: str, is_large_file: bool = False) -> None:
    """
    Reads a file from a specified database and performs some operations on it.

    Args:
        database (str): The name of the database to read from.
        company_id (str): The ID of the company.
        is_large_file (bool, optional): Indicates whether the file is
                                        large or not. Defaults to False.
    """
    file_name = company_id.split(".")[0]
    manager.update_mlflow_runs(str(database))
    kwargs = {"filename": file_name}
    if is_large_file:
        read_aneel_wraper_large_file(database, company_id, **kwargs)
    else:
        read_aneel_wraper(database, company_id, **kwargs)


def read_aneel_company_files(
    company_id: str, df_processed: pd.DataFrame, is_large_file: bool = False
) -> None:
    """
    Reads ANEEL company files.

    This function reads the downloaded ANEEL company files.
    """
    for database in ["ponnot", "ramlig", "ucbt", "conj"]:
        exist_file = (
            df_processed[
                (df_processed["mlflow.runName"] == company_id)
                & (df_processed["database"] == database)
            ].shape[0]
            > 0
        )
        if not exist_file:
            write_log(f"Reading {database} file {company_id}")
            read_file(database, company_id, is_large_file)


def flat_list(files_dict: dict) -> List[str]:
    """
    Flattens a list of lists.

    Args:
        files_dict (dict): A dictionary containing lists of files.

    Returns:
        list: The flattened list.
    """
    temp_files = []
    if not files_dict:
        temp_files.append([])
    for _, files in files_dict.items():
        temp_files.extend(files)
    return temp_files


def flat_lists(
    extra_large_files: List[str],
    large_files: List[str],
    medium_files: List[str],
    small_files: List[str],
) -> Tuple[List[str], List[str], List[str], List[str]]:
    """
    Flattens a list of lists.

    Args:
        extra_large_files (list): A list of extra large files.
        large_files (list): A list of large files.
        medium_files (list): A list of medium files.
        small_files (list): A list of small

    Returns:
        list: The flattened list.
    """
    extra_large_files = flat_list(extra_large_files)
    large_files = flat_list(large_files)
    medium_files = flat_list(medium_files)
    small_files = flat_list(small_files)
    return extra_large_files, large_files, medium_files, small_files


def split_file_sizes() -> Tuple[List[str], List[str], List[str], List[str]]:
    """
    Splits the file sizes into four lists based on their sizes.

    Returns:
        tuple: A tuple containing four lists - extra_large_files, large_files,
               medium_files, and small_files.
               extra_large_files: List of file ids with sizes greater than or equal to 1.76GB.
               large_files: List of file ids with sizes between 800MB and 1.76GB.
               medium_files: List of file ids with sizes between 100MB and 800MB.
               small_files: List of file ids with sizes less than 100MB.
    """
    extra_large_files = defaultdict(list)
    large_files = defaultdict(list)
    medium_files = defaultdict(list)
    small_files = defaultdict(list)
    split_size = 800 * 1024 * 1024
    df_aneel_ids = pd.DataFrame(
        {
            "company_ids": [
                os.path.basename(f)
                for f in glob.glob(
                    os.path.join(
                        CONTRACTS_BRONZE["raw_data"]["physicalPath"], "*.gdb.zip"
                    )
                )
            ]
        }
    )
    for company_id in df_aneel_ids["company_ids"]:
        if " - " not in company_id:
            continue
        year = company_id.split(" - ")[1].split("-")[0]
        file_path = os.path.join(
            CONTRACTS_BRONZE["raw_data"]["physicalPath"],
            company_id,
        )
        file_size = os.path.getsize(file_path)
        if file_size >= 1.6 * split_size:
            extra_large_files[year].append(company_id)
        elif file_size >= split_size:
            large_files[year].append(company_id)
        elif (split_size / 8) <= file_size < split_size:
            medium_files[year].append(company_id)
        else:
            small_files[year].append(company_id)
    return flat_lists(extra_large_files, large_files, medium_files, small_files)


def process_small_files(small_files: list, df_processed: pd.DataFrame) -> None:
    """
    Process small files using multiprocessing.

    Args:
        small_files (list): A list of small file paths to be processed.
        df_processed (pd.DataFrame): The DataFrame containing processed files.
    """
    write_log("Processing small files")
    num_cores = multiprocessing.cpu_count()
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = [
            executor.submit(read_aneel_company_files, company_id, df_processed)
            for company_id in tqdm(small_files, desc="Processing small files")
        ]
        concurrent.futures.wait(futures)


def proccess_medium_files(medium_files: list, df_processed: pd.DataFrame) -> None:
    """
    Process medium files using concurrent.futures.ProcessPoolExecutor.

    Args:
        medium_files (list): A list of medium files to be processed.
        df_processed (pd.DataFrame): The DataFrame containing processed files.
    """
    write_log("Processing medium files")
    num_cores = min(3, multiprocessing.cpu_count())
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = [
            executor.submit(read_aneel_company_files, company_id, df_processed)
            for company_id in tqdm(medium_files, desc="Processing medium files")
        ]
        concurrent.futures.wait(futures)


def process_large_files(large_files, df_processed: pd.DataFrame) -> None:
    """
    Process a list of large files.

    Args:
        large_files (list): A list of file ids.
        df_processed (pd.DataFrame): The DataFrame containing processed files.
    """
    for company_id in tqdm(large_files, desc="Processing large files"):
        write_log(
            f"Reading large file: {company_id}",
        )
        read_aneel_company_files(company_id, df_processed)


def process_extra_large_files(
    extra_large_files: list, df_processed: pd.DataFrame
) -> None:
    """
    Process a list of extra large files.

    This function iterates over a list of extra large file ids and calls the
    `read_aneel_company_files` function to read each file.

    Args:
        extra_large_files (list): A list of extra large file ids.
        df_processed (pd.DataFrame): The DataFrame containing processed files.
    """
    for company_id in tqdm(extra_large_files, desc="Processing extra large files"):
        write_log(
            f"Reading extra large file: {company_id}",
        )
        read_aneel_company_files(company_id, df_processed, is_large_file=True)


@lru_cache(maxsize=10)
def get_cols_in_db(table_name) -> List[str]:
    """
    Retrieves the columns of the 'infrastructure.ucbt' table from the 'bronze' database.

    Returns:
    list: A list of column names.
    """
    schema = CONTRACTS_BRONZE[table_name]["schema"]
    table = CONTRACTS_BRONZE[table_name]["tableName"]
    conn = DBConnection("bronze")
    return conn.query_database(f"SELECT * FROM {schema}.{table} LIMIT 1").columns


def get_df_processed():
    """
    Get the processed data from MLflow.
    """
    df_processed = get_ml_flow_data(EXPERIMENT_NAME)
    if df_processed.empty:
        return pd.DataFrame({"mlflow.runName": [], "database": []})
    df_processed = df_processed[
        (df_processed["status"] == "FINISHED")
        & (~df_processed["mlflow.runName"].isin(["ponnot", "ucbt", "conj", "ramlig"]))
    ]
    return df_processed


def process_files_aneel():
    """
    Process the files in the ANEEL dataset based on their sizes.
    """
    df_processed = get_df_processed()
    extra_large_files, large_files, medium_files, small_files = split_file_sizes()
    process_small_files(small_files, df_processed)
    proccess_medium_files(medium_files, df_processed)
    process_large_files(large_files, df_processed)
    process_extra_large_files(extra_large_files, df_processed)


def main():
    """
    Main function for making dataset of ANEEL companies.

    This function loads ANEEL IDs, splits file sizes, and reads ANEEL
    company files using multiprocessing.
    It utilizes concurrent.futures.ProcessPoolExecutor to parallelize the
    file reading process.
    """
    process_files_aneel()
    manager.update_status("finished_step_2")
    manager.update_last_run()
