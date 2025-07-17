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
from dask.distributed import Client, LocalCluster, as_completed
import polars as pl
from tqdm import tqdm
import fiona
import mlflow
import numpy as np
import pandas as pd
import geopandas as gpd


from src.tools.managers.db_connector import DBConnection
from src.tools.managers.reader import Reader
from src.tools.managers.saver import save_parquet_decorator
from src.tools.utils.common import (
    check_file_exists_in_db,
    write_log,
    get_db_path,
    trim_memory,
)
from src.tools.utils.constants import CRS_GLOBAL
from src.databases.bronze.aneel.common import split_file_sizes, get_df_already_processed


from src.databases.bronze.aneel.config import (
    manager,
    CONTRACT_BRONZE_ENERGY,
    CONTRACT_RAW_ENERGY,
    EXPERIMENT_NAME,
)

mlflow.set_experiment(EXPERIMENT_NAME)


def generate_mlflow_results_dict(
    df: Union[pd.DataFrame, gpd.GeoDataFrame], database: str, year: int, company_id: str
) -> Dict[str, Dict[str, Union[str, int]]]:
    """
    Logs the parameters and metrics to MLflow for the given DataFrame or GeoDataFrame.
    """
    results = {"parameters": {}, "metrics": {}}
    results["parameters"] = {
        "company_id": company_id,
        "company": df.dist.unique()[0],
        "database": database,
    }
    results["metrics"] = {
        "year": year,
        "num_rows": len(df),
        "sum_energy": 0,
        "mean_energy": 0,
        "std_energy": 0,
    }
    if database == "ucbt":
        energy_columns = df.filter(regex=r"ene_\d{2}_sum")
        results["metrics"].update(
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
        df = drop_unnecessary_columns(df, is_ucbt)
        # df = reduce_memory_usage(df)
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

    try:
        with fiona.open(path, layer=layer_name) as layer_src:
            feature_count = len(layer_src)

        num_workers = multiprocessing.cpu_count()
        for _ in range(num_workers):
            p = multiprocessing.Process(
                target=worker, args=(queue_in, queue_out, path, layer_name, is_ucbt)
            )
            p.start()
            processes.append(p)
        num_chunks = 0

        for start in range(0, feature_count, chunk_size):
            queue_in.put((start, chunk_size))
            num_chunks += 1

        for _ in range(num_workers):
            queue_in.put(None)

        dfs = []

        for _ in tqdm(range(num_chunks)):
            df = queue_out.get()
            dfs.append(df)

        # Concatenate results
        if dfs:
            df = pd.concat(dfs, ignore_index=True)
        else:
            df = pd.DataFrame()

    finally:
        for p in processes:
            if p.is_alive():
                p.terminate()
            p.join()
        queue_in.close()
        queue_in.join_thread()
        queue_out.close()
        queue_out.join_thread()
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
def engineer_columns_and_save(
    df: Union[gpd.GeoDataFrame, pd.DataFrame],
    saved_columns: List[str],
    company_id: str,
    database: str,
    **kwargs,
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Function to engineer columns in a GeoDataFrame.
    """
    df = add_basic_columns(df, company_id)
    if "conj" in df.columns:
        df["conj"] = df["conj"].fillna(0).astype(float).astype(int)
    if database in ["ponnot", "conj"]:
        df = process_geometry_column(df)  # type: ignore
    elif database == "ucbt":
        df = process_ucbt_data(df)
    if saved_columns:
        df = add_missing_columns(df, saved_columns)
    trim_memory()
    return df


def drop_unnecessary_columns(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    is_ucbt: bool,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Drops unnecessary columns from the DataFrame.

    Args:
        df (Union[pd.DataFrame, gpd.GeoDataFrame]): The input DataFrame.
        is_ucbt (bool): Whether the database is 'ucbt'.
    Returns:
         Union[pd.DataFrame, gpd.GeoDataFrame]: The DataFrame with unnecessary columns dropped.
    """
    ucbt_extra_cols = ["cod_id", "geometry"] if is_ucbt else []
    num_cols = [str(i).zfill(2) for i in range(1, 13)]
    dic_cols = [f"dic_{i}" for i in num_cols]
    fic_cols = [f"fic_{i}" for i in num_cols]
    other_cols = [
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
    columns_to_exclude = dic_cols + fic_cols + other_cols + ucbt_extra_cols

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
    df["year"] = int(company_id.split(" - ")[1].split("-")[0])
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


def convert_to_float(df):
    """Converts energy columns to float32"""
    for col in df.filter(regex="ene_").columns:
        df[col] = df[col].astype("float32")
    return df


def process_ucbt_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes data specific to the 'ucbt' database.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    df["dat_con"] = pd.to_datetime(df["dat_con"], errors="coerce").dt.date
    df = (
        transform_negative_energy_values(df)
        .pipe(generate_grouped_columns)
        .pipe(convert_to_float)
    )
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
        df[col] = np.absolute(df[col])
        df[col] = df[col].fillna(0).astype(int)
    trim_memory()
    return df


def reduce_memory_usage(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Reduces memory usage of the DataFrame by downcasting numeric columns.

    Args:
        df ( Union[pd.DataFrame, gpd.GeoDataFrame]): The input DataFrame.

    Returns:
         Union[pd.DataFrame, gpd.GeoDataFrame]: The DataFrame with reduced memory usage.
    """
    # Convert to categorical to reduce memory usage
    categorical_cols = ["dist", "mun", "clas_sub", "pn_con", "brr"]
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].astype("category")

    # Process energy columns to int32 if possible to reduce memory
    energy_cols = [col for col in df.columns if col.startswith("ene_")]
    for col in energy_cols:
        if df[col].max() <= 2147483647:  # int32 max
            df[col] = df[col].fillna(0).astype("int32")
    return df


def generate_grouped_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa colunas de energia e gera estatísticas por grupo usando Polars.

    Args:
        df (pd.DataFrame): DataFrame com os dados (deve estar no formato Pandas).

    Returns:
        pd.DataFrame: DataFrame com colunas agregadas por grupo.
    """
    df_polars = pl.from_pandas(df)
    energy_cols = [col for col in df.columns if col.startswith("ene_")]
    group_cols = ["year", "company_file", "dist", "mun", "conj", "clas_sub", "pn_con"]
    agg_exprs = []

    for col in energy_cols:
        agg_exprs.extend(
            [
                pl.col(col).sum().alias(f"{col}_sum"),
                pl.col(col).filter(pl.col(col) != 0).mean().alias(f"{col}_mean"),
                pl.col(col).median().alias(f"{col}_median"),
            ]
        )

    agg_exprs.extend(
        [
            pl.col("dat_con").min().alias("dat_con_oldest"),
            pl.col("dat_con").max().alias("dat_con_latest"),
            pl.col("dat_con").mode().first().alias("dat_con_most_frequent"),
            pl.col("brr").mode().first().alias("brr_most_frequent"),
        ]
    )

    result = df_polars.group_by(group_cols).agg(agg_exprs)
    return result.to_pandas()


def read_single_file(
    layers_dict, path, database: str
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Wrapper function to read ANEEL data from a specific database and company.

    Args:
        layers_dict (dict): A dictionary mapping database keys to layer names.
        path (str): The path to the company's data directory.
        database (str): The name of the database.
    Returns:
        Union[pd.DataFrame, gpd.GeoDataFrame]: The DataFrame or GeoDataFrame containing
                                            the data from the specified energy company.
    """
    is_ucbt = database == "ucbt"
    reader = Reader()
    # Read just a sample of rows instead of the entire file
    df = reader.read_geofile(
        file_path=path,
        driver="FileGDB",
        layer=layers_dict[database],
    )
    df = drop_unnecessary_columns(df, is_ucbt)
    # df = reduce_memory_usage(df)
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
    path = CONTRACT_RAW_ENERGY["raw_data"]["physicalPath"]
    path = os.path.join(path, company_id)
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
) -> Dict[str, Dict[str, Union[str, int]]]:
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
    trim_memory()
    df = engineer_columns_and_save(df, cols, company_id, database, **kwargs)
    results = generate_mlflow_results_dict(df, database, year, company_id)
    del df
    gc.collect()
    trim_memory()
    return results


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
            (df_processed["company_id"] == company_id)
            & (df_processed["database"] == database)
        ].shape[0]
        > 0
    )


def assert_sum_energy_db_and_mlflow_are_equal(conn: DBConnection, results: dict):
    """
    Asserts that the sum_energy values in the database and MLflow are equal.

    Args:
        conn (DBConnection): The database connection object.
        results (dict): The results dictionary.

    Raises:
        AssertionError: If the sum_energy values are not equal.
    """
    year = results["metrics"]["year"]
    company_id = results["parameters"]["company_id"]
    contract = CONTRACT_BRONZE_ENERGY[f"ucbt_{year}"]
    path = get_db_path(contract)
    sum_energy_db = conn.query_database(
        f"""SELECT sum(
                    u.ene_01_sum + u.ene_02_sum+
                    u.ene_03_sum + u.ene_04_sum+
                    u.ene_05_sum + u.ene_06_sum+
                    u.ene_07_sum + u.ene_08_sum+
                    u.ene_09_sum + u.ene_10_sum+
                    u.ene_11_sum + u.ene_12_sum
                ) FROM {path} u where company_file= '{company_id}'
                """
    ).squeeze()
    np.testing.assert_almost_equal(
        sum_energy_db / 1e5, results["metrics"]["sum_energy"] / 1e5, decimal=0  # type: ignore
    )


def process_single_file(
    company_id: str, database: str, is_large_file: bool = False
) -> Dict[str, Dict[str, Union[str, int]]]:
    """
    Reads ANEEL company files.

    This function reads the downloaded ANEEL company files.
    It processes the files and saves the results to a specified output path.
    Args:
        company_id (str): The ID of the company.
        database (str): The name of the database.
        is_large_file (bool, optional): Indicates whether the file is

    Returns:
        Dict[str, Dict[str, Union[str, int]]]: A dictionary containing the results.
    """

    year = int(company_id.split(" - ")[1].split("-")[0])
    cols = get_cols_saved_in_db(database, year)
    file_name = company_id.split(".")[0]
    contract_key = "_".join([database, str(year)])
    contract = CONTRACT_BRONZE_ENERGY[contract_key]
    kwargs = {"filename": file_name, "contract": contract}
    if is_large_file:
        write_log(f"Reading {database} file {company_id}")
    mlflow_results = start_process(
        database, company_id, cols, year, is_large_file, **kwargs
    )
    return mlflow_results


def assert_lenth_are_equal(conn: DBConnection, results: dict):
    """
    Asserts that the length of the database and MLflow results are equal.

    Args:
        conn (DBConnection): The database connection object.
        results (dict): The results dictionary.

    Raises:
        AssertionError: If the lengths are not equal.
    """
    year = results["metrics"]["year"]
    company_id = results["parameters"]["company_id"]
    database = results["parameters"]["database"]
    contract = CONTRACT_BRONZE_ENERGY[f"{database}_{year}"]
    path = get_db_path(contract)
    length_db = conn.query_database(
        f"""SELECT count(*) FROM {path} where company_file= '{company_id}'"""
    ).squeeze()

    np.testing.assert_almost_equal(
        length_db, results["metrics"]["num_rows"], decimal=0  # type: ignore
    )


def process_small_files(files: list, max_num_cores: int, parallel: bool = True) -> None:
    """
    Process files using Dask for parallel processing.
    """
    write_log("Processing files in parallel")
    if parallel:
        batch_size = min(max_num_cores, len(files))
        cluster = LocalCluster(
            n_workers=batch_size, threads_per_worker=1, processes=True
        )
        client = Client(cluster)

        file_batches = [files[i::batch_size] for i in range(batch_size)]
        futures = []
        for batch in file_batches:
            for company_id, database in batch:
                future = client.submit(process_single_file, company_id, database)
                futures.append(future)
        conn = DBConnection("bronze")
        for future in tqdm(as_completed(futures), desc="Processing batches"):
            try:
                results = future.result()  # type: ignore
                if results["parameters"]["database"] == "ucbt":
                    assert_sum_energy_db_and_mlflow_are_equal(conn, results)
                assert_lenth_are_equal(conn, results)
                with mlflow.start_run(run_name=results["parameters"]["company_id"]):
                    mlflow.log_params(results["parameters"])
                    mlflow.log_metrics(results["metrics"])
                trim_memory()
            except Exception as e:
                write_log(f"Error processing file: {e}")
        conn.close()
        client.close()
        cluster.close()
    else:
        conn = DBConnection("bronze")
        for company_id, database in tqdm(files, desc="Processing files"):
            results = process_single_file(company_id, database)
            if results["parameters"]["database"] == "ucbt":
                assert_sum_energy_db_and_mlflow_are_equal(conn, results)
            assert_lenth_are_equal(conn, results)
            with mlflow.start_run(run_name=company_id):
                mlflow.log_params(results["parameters"])
                mlflow.log_metrics(results["metrics"])  # type: ignore
        conn.close()


def process_large_files(large_files: list) -> None:
    """
    Process a list of large files.
    """
    conn = DBConnection("bronze")
    for company_id, database in tqdm(large_files, desc="Processing large files"):
        write_log(f"Reading large file: {company_id}")
        try:
            results = process_single_file(company_id, database, is_large_file=True)
        except Exception as e:
            write_log(f"Error processing large file: {e}")
            results = {}

        if results:
            if results["parameters"]["database"] == "ucbt":
                assert_sum_energy_db_and_mlflow_are_equal(conn, results)
            assert_lenth_are_equal(conn, results)
            with mlflow.start_run(run_name=company_id):
                mlflow.log_params(results["parameters"])
                mlflow.log_metrics(results["metrics"])  # type: ignore
    conn.close()


def process_files_per_size_aneel(refresh_view: bool = False) -> None:
    """
    Process the files in the ANEEL dataset based on their sizes.
    """
    df_processed = get_df_already_processed(refresh_view)
    large_files, small_files = split_file_sizes(df_processed)
    if small_files:
        process_small_files(small_files, 10)
    if large_files:
        process_large_files(large_files)


def main():
    """
    Main function for making dataset of ANEEL companies.

    This function loads ANEEL IDs, splits file sizes, and reads ANEEL
    company files using multiprocessing.
    It utilizes concurrent.futures.ProcessPoolExecutor to parallelize the
    file reading process.
    """
    refresh_view = False
    module_name = os.path.basename(__file__).replace(".py", "")
    manager.update_status(module_name)
    process_files_per_size_aneel(refresh_view)
    manager.update_last_run()
