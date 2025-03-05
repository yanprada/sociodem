"""
This script processes files from the ANEEL dataset based on their sizes.

Important: For large and extra-large files, the script uses parallel processing.
If a database is not already saved, multiple workers may attempt to create it simultaneously,
which can cause a deadlock.

The script connects to a database, retrieves ANEEL IDs for specific years, and processes
the corresponding files. The processed files are saved to a specified output path.
"""

import os
import multiprocessing
import gc
from typing import List, Tuple, Union, Dict
from functools import lru_cache
from dask.distributed import Client, LocalCluster, as_completed
import duckdb
from tqdm import tqdm
import fiona
import mlflow
import numpy as np
import pandas as pd
import geopandas as gpd


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
from src.databases.bronze.aneel.common import split_file_sizes


from src.databases.bronze.aneel.config import (
    manager,
    CONTRACT_BRONZE_ENERGY,
    CONTRACT_RAW_ENERGY,
    EXPERIMENT_NAME,
    YEARS,
)

mlflow.set_experiment(EXPERIMENT_NAME)


def generate_mlflow_results_dict(
    df: Union[pd.DataFrame, gpd.GeoDataFrame], database: str, year: int, company_id: str
) -> Dict[str, Union[str, int]]:
    """
    Logs the parameters and metrics to MLflow for the given DataFrame or GeoDataFrame.
    """
    results = {
        "company_id": company_id,
        "company": df.dist.unique()[0],
        "database": database,
        "year": year,
        "num_rows": len(df),
        "sum_energy": 0,
        "mean_energy": 0,
        "std_energy": 0,
    }
    if database == "ucbt":
        energy_columns = df.filter(regex=r"ene_\d{2}_sum")
        results.update(
            {
                "sum_energy": energy_columns.sum().sum(),
                "mean_energy": energy_columns.sum(axis=1).mean(),
                "std_energy": energy_columns.sum(axis=1).std(),
            }
        )
    return results


def read_aneel_in_chunks(
    layer_src_path: str,
    layer_name: str,
    start: int,
    chunk_size: int,
    is_ucbt: bool,
    queue: multiprocessing.Queue,
):
    """
    Read a chunk of features from a layer source and put the result in a queue.
    """
    reader = Reader()
    with fiona.open(layer_src_path, layer=layer_name) as layer_src:
        features = list(layer_src[start : start + chunk_size])
        if is_ucbt:
            df = reader.read_geopandas(features, as_feature=True)
        else:
            df = reader.read_geopandas(features, as_feature=True, crs=layer_src.crs)
        queue.put(df)


def worker(
    queue_in: multiprocessing.Queue,
    queue_out: multiprocessing.Queue,
    layer_src_path: str,
    layer_name: str,
    is_ucbt: bool,
):
    """
    Worker function to process data in chunks from a source layer.
    """
    while True:
        item = queue_in.get()
        if item is None:
            break
        start, chunk_size = item
        read_aneel_in_chunks(
            layer_src_path, layer_name, start, chunk_size, is_ucbt, queue_out
        )


def read_in_parallel(
    layers_dict: Dict[str, str], path: str, database: str
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Wrapper function to read ANEEL data in parallel.
    """
    chunk_size = int(5e4)
    is_ucbt = database == "ucbt"
    layer_name = layers_dict[database]

    queue_in = multiprocessing.Queue()
    queue_out = multiprocessing.Queue()
    processes = []

    with fiona.open(path, layer=layer_name) as layer_src:
        feature_count = len(layer_src)

    num_workers = multiprocessing.cpu_count()
    for _ in range(num_workers):
        p = multiprocessing.Process(
            target=worker, args=(queue_in, queue_out, path, layer_name, is_ucbt)
        )
        p.start()
        processes.append(p)

    for start in range(0, feature_count, chunk_size):
        queue_in.put((start, chunk_size))

    for _ in range(num_workers):
        queue_in.put(None)

    dfs = []
    for _ in tqdm(range(0, feature_count, chunk_size)):
        df = queue_out.get()
        dfs.append(df)

    for p in processes:
        p.join()
    df = pd.concat(dfs, ignore_index=True)
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
def engineer_columns(
    df: Union[gpd.GeoDataFrame, pd.DataFrame],
    saved_columns: List[str],
    company_id: str,
    database: str,
    **kwargs,
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Function to engineer columns in a GeoDataFrame.
    """
    df = drop_unnecessary_columns(df)
    df = add_basic_columns(df, company_id)

    if database in ["ponnot", "conj"]:
        df = process_geometry_column(df)  # type: ignore
    elif database == "ucbt":
        df = process_ucbt_data(df)

    if saved_columns:
        df = add_missing_columns(df, saved_columns)

    return df


def drop_unnecessary_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops unnecessary columns from the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with unnecessary columns dropped.
    """
    columns_to_exclude = [
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
    return df.drop(columns=columns_to_exclude, errors="ignore")


def add_basic_columns(df: pd.DataFrame, company_id: str) -> pd.DataFrame:
    """
    Adds basic columns to the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.
        company_id (str): The ID of the company.

    Returns:
        pd.DataFrame: The DataFrame with basic columns added.
    """
    df["year"] = company_id.split(" - ")[1].split("-")[0]
    df["company_file"] = company_id
    return df


def process_geometry_column(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Processes data specific to the 'ponnot' database.

    Args:
        df (gpd.GeoDataFrame): The input GeoDataFrame.

    Returns:
        gpd.GeoDataFrame: The processed GeoDataFrame.
    """
    df = df.set_geometry("geometry").to_crs(CRS_GLOBAL).drop_duplicates()  # type: ignore
    return df


def process_ucbt_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes data specific to the 'ucbt' database.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    df = df.drop(columns=["cod_id", "geometry"], errors="ignore")
    df["dat_con"] = pd.to_datetime(df["dat_con"], errors="coerce").dt.date
    df = transform_negative_energy_values(df)
    df = generate_grouped_columns(df)
    return df


def transform_negative_energy_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms negative energy values to its positive couterpart.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with negative energy values
                        transformed to its positive couterpart.
    """
    energy_columns = df.filter(regex="ene_").columns
    for col in energy_columns:
        df[col] = np.where(df[col] < 0, df[col] * (-1), df[col])
    return df


def generate_grouped_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates grouped columns in the DataFrame using DuckDB for high performance.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with grouped columns.
    """
    with duckdb.connect(database=":memory:") as con:
        con.register("input_df", df)
        query = """
        SELECT
            year,
            company_file,
            dist,
            mun,
            conj,
            clas_sub,
            pn_con,
            
            -- Sum & Mean of energy-related columns
            {energy_columns},

            -- Aggregations for 'dat_con'
            MIN(dat_con) AS dat_con_oldest,
            MAX(dat_con) AS dat_con_latest,
            MODE() WITHIN GROUP (ORDER BY dat_con) AS dat_con_most_frequent,

            -- Aggregation for 'brr'
            MODE() WITHIN GROUP (ORDER BY brr) AS brr_most_frequent

        FROM input_df
        GROUP BY 1, 2, 3, 4, 5, 6, 7
        """
        energy_cols = [col for col in df.columns if col.startswith("ene_")]
        energy_agg = ", ".join(
            [
                f"SUM({col}) AS {col}_sum, AVG({col}) AS {col}_mean"
                for col in energy_cols
            ]
        )
        result_df = con.execute(query.format(energy_columns=energy_agg)).df()
    return result_df


def read_single_file(layers_dict, path, database: str) -> gpd.GeoDataFrame:
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
    path = os.path.join(CONTRACT_RAW_ENERGY["raw_data"]["physicalPath"], company_id)
    layers = fiona.listlayers(path)
    if layers_dict[database] not in layers:
        layers_dict = {
            "ucbt": "UC_BT_tab",
            "ponnot": "PON_NOT",
            "conj": "CONJ",
        }
    return layers_dict, path


def start_process(
    database: str,
    company_id: str,
    cols: List[str],
    year: int,
    is_large_file: bool = False,
    **kwargs,
) -> Dict[str, Union[str, int]]:
    """
    Reads a file from a specified database and performs some operations on it.

    Args:
        database (str): The name of the database to read from.
        company_id (str): The ID of the company.
        cols (List[str]): The columns that are already present in the databases saved.
        year (int): The year of the data.
        is_large_file (bool, optional): Indicates whether the file is
                                        large or not. Defaults to False.
        **kwargs: Additional keyword arguments.
    """
    layers_dict, path = get_layers_and_path(company_id, database)
    if is_large_file:
        df = read_in_parallel(layers_dict, path, database)
    else:
        df = read_single_file(layers_dict, path, database)
    df = engineer_columns(df, cols, company_id, database, **kwargs)
    results = generate_mlflow_results_dict(df, database, year, company_id)
    del df
    gc.collect()
    return results


@lru_cache(maxsize=256)
def get_cols_saved_in_db(database: str, year: int) -> List[str]:
    """
    Checks if a file exists in the database and returns the columns if it does.

    Args:
        database (str): The name of the database to check.
        year (int): The year of the data.

    Returns:
        list: A list of column names.
    """
    conn = DBConnection("bronze")
    contract_key = "_".join([database, str(year)])
    contract = CONTRACT_BRONZE_ENERGY[contract_key]
    path = get_db_path(contract)
    columns = []
    if check_file_exists_in_db(conn, path):
        columns = list(conn.query_database(f"SELECT * FROM {path} LIMIT 1").columns)
    conn.close()
    return columns


def check_if_file_exists(df_processed: pd.DataFrame, database: str, company_id: str):
    """
    Check if a file exists in the database.

    Args:
        df_processed (pd.DataFrame): The DataFrame containing processed files.
        database (str): The name of the database.
        company_id (str): The ID of the company.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    if df_processed.empty:
        return False
    return (
        df_processed[
            (df_processed["mlflow.runName"] == company_id)
            & (df_processed["database"] == database)
        ].shape[0]
        > 0
    )


def process_single_file(
    company_id: str, df_processed: pd.DataFrame, is_large_file: bool = False
) -> List[Dict[str, Union[str, int]]]:
    """
    Reads ANEEL company files.

    This function reads the downloaded ANEEL company files.
    It processes the files and saves the results to a specified output path.
    Args:
        company_id (str): The ID of the company.
        df_processed (pd.DataFrame): The DataFrame containing processed files.
        is_large_file (bool, optional): Indicates whether the file is

    Returns:
        List[Dict[str, Union[str, int]]]: A list of dictionaries containing the results.
    """
    results = []
    for database in ["conj"]:
        exist_file = check_if_file_exists(df_processed, database, company_id)
        if not exist_file:
            year = int(company_id.split(" - ")[1].split("-")[0])
            cols = get_cols_saved_in_db(database, year)
            file_name = company_id.split(".")[0]
            contract_key = "_".join([database, str(year)])
            contract = CONTRACT_BRONZE_ENERGY[contract_key]
            kwargs = {"filename": file_name, "contract": contract}
            if is_large_file:
                write_log(f"Reading {database} file {company_id}")
            results.append(
                start_process(database, company_id, cols, year, is_large_file, **kwargs)
            )
    return results


def process_in_parallel(
    files: list, df_processed: pd.DataFrame, max_num_cores: int
) -> None:
    """
    Process files using Dask for parallel processing.
    """
    write_log("Processing files in parallel")

    batch_size = min(max_num_cores, 10)

    cluster = LocalCluster(n_workers=batch_size, threads_per_worker=1, processes=True)
    client = Client(cluster)

    file_batches = [files[i::batch_size] for i in range(batch_size)]
    futures = []
    for batch in file_batches:
        for company_id in batch:
            future = client.submit(process_single_file, company_id, df_processed)
            futures.append(future)

    for future in tqdm(as_completed(futures), desc="Processing batches"):
        try:
            results = future.result()  # type: ignore
            for result in results:
                company_id = result["company_id"]
                with mlflow.start_run(run_name=company_id):
                    mlflow.log_params(result)
        except Exception as e:
            write_log(f"Error processing file: {e}")

    client.close()
    cluster.close()


def process_large_files(large_files: list, df_processed: pd.DataFrame) -> None:
    """
    Process a list of large files.
    """
    for company_id in tqdm(large_files, desc="Processing large files"):
        if company_id in ["NEOENERGIA_COELBA - 2022-12-31.gdb.zip"]:
            continue
        write_log(f"Reading large file: {company_id}")
        try:
            results = process_single_file(company_id, df_processed, is_large_file=True)
        except Exception as e:
            write_log(f"Error processing large file: {e}")
            results = []

        if results:
            for result in results:
                company_id = str(result["company_id"])
                with mlflow.start_run(run_name=company_id):
                    mlflow.log_params(result)


def get_data_processed_from_mlflow():
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


def get_data_processed_from_db():
    """
    Retrieves and processes data from the bronze database for specified years and databases.
    This function connects to the bronze database, queries distinct company files for each
    specified year and database, and concatenates the results into a single DataFrame.
    Returns:
        pd.DataFrame: A DataFrame containing the concatenated results of distinct company files
                      from the specified years and databases.
    """
    conn = DBConnection("bronze")
    company_files = []
    for year in YEARS:
        for database in ["ponnot", "ucbt", "conj"]:
            path = get_db_path(CONTRACT_BRONZE_ENERGY[f"{database}_{year}"])
            company_files.append(
                conn.query_database(f"SELECT DISTINCT company_file FROM {path}")
            )
    return pd.concat(company_files)


def get_df_already_processed():
    """
    Get the processed data from MLflow.
    """
    df_mlflow = get_data_processed_from_mlflow()
    df_db = get_data_processed_from_db()
    assert (
        set(df_mlflow["company_id"]).difference(set(df_db["company_file"])) == set()
    ), "MLflow data not in DB"
    # assert (
    #     set(df_db["company_file"]).difference(set(df_mlflow["company_id"])) == set()
    # ), "DB data not in MLflow"
    return df_mlflow


def process_files_per_size_aneel():
    """
    Process the files in the ANEEL dataset based on their sizes.
    """
    df_processed = get_df_already_processed()
    large_files, small_files = split_file_sizes()

    if small_files:
        process_in_parallel(small_files, df_processed, 10)
    if large_files:
        process_large_files(large_files, df_processed)


def main():
    """
    Main function for making dataset of ANEEL companies.

    This function loads ANEEL IDs, splits file sizes, and reads ANEEL
    company files using multiprocessing.
    It utilizes concurrent.futures.ProcessPoolExecutor to parallelize the
    file reading process.
    """
    module_name = os.path.basename(__file__).replace(".py", "")
    manager.update_status(module_name)
    process_files_per_size_aneel()
    manager.update_last_run()
