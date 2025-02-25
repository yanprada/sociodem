"""
This script processes files from the ANEEL dataset based on their sizes.

Important: For large and extra-large files, the script uses parallel processing.
If a database is not already saved, multiple workers may attempt to create it simultaneously,
which can cause a deadlock.

The script connects to a database, retrieves ANEEL IDs for specific years, and processes
the corresponding files. The processed files are saved to a specified output path.
"""

import os
import glob
import multiprocessing
import concurrent.futures
from collections import defaultdict
from typing import List, Tuple, Union
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
from src.tools.utils.constants import CRS_GLOBAL
from src.databases.bronze.aneel.config import MANAGER, CONTRACTS_BRONZE, EXPERIMENT_NAME

module_name = os.path.basename(__file__).replace(".py", "")
MANAGER.update_status(module_name)


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
    mlflow.log_param("year", company_id.split(" - ")[1].split("-")[0])
    mlflow.log_metric("num_rows", len(df))
    if database == "ucbt":
        mlflow.log_metric("sum_energy", df.filter(regex=r"ene_\d{2}_sum").sum().sum())
        mlflow.log_metric(
            "mean_energy", df.filter(regex=r"ene_\d{2}_sum").sum(axis=1).mean()
        )
        mlflow.log_metric(
            "std_energy", df.filter(regex=r"ene_\d{2}_sum").sum(axis=1).std()
        )
    else:
        mlflow.log_metric("sum_energy", 0)
        mlflow.log_metric("mean_energy", 0)
        mlflow.log_metric("std_energy", 0)


def read_aneel_in_chunks(layer_src: str, start: int, chunk_size: int):
    """
    Read a chunk of features from a layer source into a GeoDataFrame.

    Args:
        layer_src (str): The source layer containing the features.
        start (int): The starting index of the chunk.
        chunk_size (int): The size of the chunk.

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
        df = reader.read_geopandas(properties, geometry=geometries, crs=layer_src.crs)
    return df


def read_in_parallel(layers_dict, path, database: str):
    """
    Wrapper function to read ANEEL data from a specific database and company.

    Args:
        layers_dict (dict): A dictionary mapping database keys to layer names.
        path (str): The path to the company's data directory.
        database (str): The name of the database.
    """
    with fiona.Env():
        with fiona.open(path, layer=layers_dict[database]) as layer_src:
            chunk_size = int(5e4)
            num_features = len(layer_src)
            tasks = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for start in tqdm(range(0, num_features, chunk_size)):
                    future = executor.submit(
                        read_aneel_in_chunks, layer_src, start, chunk_size
                    )
                    tasks.append(future)
                dfs = []
                for future in tqdm(
                    concurrent.futures.as_completed(tasks), total=len(tasks)
                ):
                    df = future.result()
                    dfs.append(df)
    df = pd.concat(dfs)
    return df


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


@save_parquet_decorator(medallon="bronze")
def columns_engineering(
    df: Union[gpd.GeoDataFrame, pd.DataFrame],
    cols: List[str],
    company_id: str,
    database: str,
    **kwargs,
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Function to engineer columns in a GeoDataFrame.

    Args:
        df (Union[gpd.GeoDataFrame, pd.DataFrame]): The input GeoDataFrame.
        cols (List[str]): The columns that are already present in the databases saved.
        company_id (str): The ID of the company.
        database (str): The name of the database.
        **kwargs: Additional keyword arguments.

    Returns:
        Union[gpd.GeoDataFrame, pd.DataFrame]: The GeoDataFrame or DataFrame
                                                with engineered columns.
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
        "odi",
        "cm",
        "tuc",
        "uar",
        "tip_pn",
        "tip_inst",
        "a1",
        "a2",
        "a3",
        "a4",
        "a5",
        "a6",
        "pos",
        "estr",
        "esf",
        "alt",
        "ti",
    ]
    df = df.drop(columns=exclude_cols, errors="ignore")
    df["year"] = company_id.split(" - ")[1].split("-")[0]
    df["company_file"] = company_id

    if "conj" in df.columns:
        df["conj"] = df["conj"].fillna("0").astype(int)

    # transform crs to global
    if (
        isinstance(df, gpd.GeoDataFrame)
        and "geometry" in df.columns
        and not df["geometry"].isnull().all()
    ):
        df = df.set_geometry("geometry")
        df = df.to_crs(CRS_GLOBAL)
    if database == "ponnot":
        df = df.drop_duplicates()
    if database == "ucbt":
        df = df.drop(columns=["cod_id", "geometry"], errors="ignore")
        # transform negative energy values to 0
        energy_cols = df.filter(regex="ene_").columns
        for col in energy_cols:
            df[col] = np.where(df[col] < 0, 0, df[col])
        # generate grouped col
        not_energy_cols = list(df.columns.difference(energy_cols))
        agg_dict = {col: ["sum", "mean"] for col in energy_cols}
        df = df.groupby(not_energy_cols).agg(agg_dict)
        df.columns = ["_".join(col).strip() for col in df.columns.values]
        df = df.reset_index()

    # add missing cols
    if len(cols) > 0:
        df = add_missing_columns(df, cols)
    return df


def read_single_file(layers_dict, path, database: str):
    """
    Wrapper function to read ANEEL data from a specific database and company.

    Args:
        layers_dict (dict): A dictionary mapping database keys to layer names.
        path (str): The path to the company's data directory.
        database (str): The name of the database.
    """
    reader = Reader()
    df = reader.read_geofile(
        file_path=path,
        driver="FileGDB",
        layer=layers_dict[database],
    )
    return df


def get_layers_and_path(company_id: str, database: str) -> Tuple[dict, str]:
    """
    Retrieves the dictionary of layer names and the path to the specified company's data.
    Args:
        company_id (str): The ID of the company whose data path is to be retrieved.
        database (str): The database key to look up in the layers dictionary.
    Returns:
        tuple: A tuple containing:
            - layers_dict (dict): A dictionary mapping database keys to layer names.
            - path (str): The path to the company's data directory.
    """
    layers_dict = {
        "ucbt": "UCBT_tab",
        "ponnot": "PONNOT",
        "conj": "CONJ",
    }
    path = os.path.join(CONTRACTS_BRONZE["raw_data"]["physicalPath"], company_id)
    layers = fiona.listlayers(path)
    if layers_dict[database] not in layers:
        layers_dict = {
            "ucbt": "UC_BT_tab",
            "ponnot": "PON_NOT",
            "conj": "CONJ",
        }
    return layers_dict, path


def read_and_process_file(
    database: str, company_id: str, cols: List[str], is_large_file: bool = False
) -> None:
    """
    Reads a file from a specified database and performs some operations on it.

    Args:
        database (str): The name of the database to read from.
        company_id (str): The ID of the company.
        cols (List[str]): The columns that are already present in the databases saved.
        is_large_file (bool, optional): Indicates whether the file is
                                        large or not. Defaults to False.
    """
    file_name = company_id.split(".")[0]
    MANAGER.update_mlflow_runs(str(database))
    kwargs = {"filename": file_name, "contract": CONTRACTS_BRONZE[database]}
    with mlflow.start_run(run_name=company_id):
        layers_dict, path = get_layers_and_path(company_id, database)
        if is_large_file:
            df = read_in_parallel(layers_dict, path, database)
            # import ipdb

            # ipdb.set_trace()
        else:
            df = read_single_file(layers_dict, path, database)
        df = columns_engineering(df, cols, company_id, database, **kwargs)
        add_to_mlflow(df, database, company_id)


def get_cols_saved_in_db(database: str) -> List[str]:
    """
    Checks if a file exists in the database and returns the columns if it does.

    Args:
        database (str): The name of the database to check.

    Returns:
        list: A list of column names.
    """
    conn = DBConnection("bronze")
    contract = CONTRACTS_BRONZE[database]
    path = get_db_path(contract)
    if check_file_exists_in_db(conn, path):
        return conn.query_database(f"SELECT * FROM {path} LIMIT 1").columns
    return []


def read_aneel_company_files(
    company_id: str, df_processed: pd.DataFrame, is_large_file: bool = False
) -> None:
    """
    Reads ANEEL company files.

    This function reads the downloaded ANEEL company files.
    """
    for database in ["ponnot", "ucbt", "conj"]:
        if df_processed.empty:
            exist_file = False
        else:
            exist_file = (
                df_processed[
                    (df_processed["mlflow.runName"] == company_id)
                    & (df_processed["database"] == database)
                ].shape[0]
                > 0
            )
        if not exist_file:
            cols = get_cols_saved_in_db(database)
            write_log(f"Reading {database} file {company_id}")
            read_and_process_file(database, company_id, cols, is_large_file)


def flat_list(files_dict: dict) -> Union[List[str], None]:
    """
    Flattens a list of lists.

    Args:
        files_dict (dict): A dictionary containing lists of files.

    Returns:
        list: The flattened list.
    """
    if not files_dict:
        return None
    temp_files = []
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
        & (~df_processed["mlflow.runName"].isin(["ponnot", "ucbt", "conj"]))
    ]
    return df_processed


def process_files_aneel():
    """
    Process the files in the ANEEL dataset based on their sizes.

    This function splits the files into four categories based on their sizes and then processes
    each category using the appropriate functions for small, medium, large, and extra-large files.
    """
    df_processed = get_df_processed()
    extra_large_files, large_files, medium_files, small_files = split_file_sizes()

    if small_files:
        process_small_files(small_files, df_processed)

    if medium_files:
        proccess_medium_files(medium_files, df_processed)

    df_processed = get_df_processed()

    if df_processed.empty:
        raise RuntimeError(
            "Important: For large and extra-large files, the script uses parallel processing. "
            "If a database is not already saved, multiple workers may attempt"
            "to create it simultaneously, which can cause a deadlock."
        )

    if large_files:
        process_large_files(large_files, df_processed)

    if extra_large_files:
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
    MANAGER.update_last_run()
