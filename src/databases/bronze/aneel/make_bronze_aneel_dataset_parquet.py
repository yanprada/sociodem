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
import multiprocessing
import concurrent.futures
from typing import List, Tuple
from functools import lru_cache
from tqdm import tqdm
import fiona
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.read import Reader
from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from src.tools.data_contract.validation_data_contract import get_validation_partitions
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.common import write_log, check_file_exists


CONTRACTS = get_aneel_contracts("bronze")
VALIDATION_PARTITIONS = get_validation_partitions()


@lru_cache(maxsize=10)
def load_aneel_ids() -> pd.DataFrame:
    """Loads ANEEL IDs from a CSV file and returns them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing ANEEL IDs.

    Examples:
        >>> load_aneel_ids()
           id
        0   18729481353
        1   29873689872
        2   39048776183
    """
    conn = DBConnection("bronze")
    path = ".".join(
        [CONTRACTS["company_id"]["schema"], CONTRACTS["company_id"]["tableName"]]
    )
    df = conn.query_database(
        f"""
            SELECT * FROM {path} 
        """
    )
    return df


def read_aneel_wraper_large_file(database: str, company_id: str, **kwargs):
    """
    Wrapper function to read ANEEL data from a specific database and company.

    Args:
        database (str): The name of the database.
        company_id (str): The ID of the company.
        **kwargs: Additional keyword arguments.
    """

    @save_parquet_decorator(
        medallon="bronze", contract=CONTRACTS[database], save_db=False
    )
    def read_aneel_in_chunks(layer_src, start, chunk_size, **kwargs):
        """
        Read a chunk of features from a layer source into a GeoDataFrame.

        Args:
            layer_src (Layer): The source layer containing the features.
            start (int): The starting index of the chunk.
            chunk_size (int): The size of the chunk to be saved.
            **kwargs: Additional keyword arguments to be passed to the reader.

        Returns:
            GeoDataFrame: The GeoDataFrame containing the saved features.
        """
        reader = Reader(CONTRACTS[database])
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
        return df

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
                            **task_kwargs,
                        )
                        tasks.append(future)

                    for future in tqdm(
                        concurrent.futures.as_completed(tasks), total=len(tasks)
                    ):
                        future.result()

    layers_dict = {
        "ramlig": "RAMLIG",
        "ucbt": "UCBT_tab",
        "ponnot": "PONNOT",
        "conj": "CONJ",
    }
    path = os.path.join(
        CONTRACTS["datalake"]["physicalPath"],
        company_id,
    )
    assert layers_dict[database] in fiona.listlayers(
        path
    ), f"{layers_dict[database]} not found in the file {path}"
    parallel_process()


def read_aneel_wraper(database: str, company_id: str, **kwargs):
    """
    Wrapper function to read ANEEL data from a specific database and company.

    Args:
        database (str): The name of the database.
        company_id (str): The ID of the company.
        **kwargs: Additional keyword arguments.
    """

    @save_parquet_decorator(
        medallon="bronze", contract=CONTRACTS[database], save_db=False
    )
    def read_aneel(company_id: str, **kwargs) -> gpd.GeoDataFrame:
        layers_dict = {
            "ramlig": "RAMLIG",
            "ucbt": "UCBT_tab",
            "ponnot": "PONNOT",
            "conj": "CONJ",
        }
        reader = Reader(CONTRACTS[database])
        path = os.path.join(CONTRACTS["datalake"]["physicalPath"], company_id)
        layers = fiona.listlayers(path)
        assert (
            layers_dict[database] in layers
        ), f"{layers_dict[database]} not found in the file {path}"
        df_conj = reader.read_geofile(
            file_path=path,
            driver="FileGDB",
            layer=layers_dict[database],
        )
        del reader
        del path
        gc.collect()
        return df_conj

    _ = read_aneel(company_id, **kwargs)


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
    file_path = CONTRACTS[database]["physicalPath"]
    exist_file = check_file_exists(file_name, file_path)
    if not exist_file:
        kwargs = {"filename": file_name}
        if is_large_file:
            read_aneel_wraper_large_file(database, company_id, **kwargs)
        else:
            read_aneel_wraper(database, company_id, **kwargs)


def read_aneel_company_files(company_id: str, is_large_file: bool = False) -> None:
    """
    Reads ANEEL company files.

    This function reads the downloaded ANEEL company files.
    """
    for database in ["ponnot", "ramlig", "ucbt", "conj"]:
        write_log(f"Reading {database} file")
        read_file(database, company_id, is_large_file)


def split_file_sizes(df_aneel_ids) -> Tuple[List[str], List[str]]:
    """
    Splits the file sizes into two lists based on their sizes.

    Args:
        df_aneel_ids (DataFrame): The DataFrame containing the file ids.

    Returns:
        tuple: A tuple containing two lists - large_files and small_files.
               large_files: List of file ids with sizes greater than 800MB.
               small_files: List of file ids with sizes less than 800KB.
    """
    extra_large_files = []
    large_files = []
    medium_files = []
    small_files = []
    split_size = 800 * 1024 * 1024
    for company_id in df_aneel_ids["company_ids"]:
        company_id = "".join([company_id, ".gdb.zip"])
        file_path = os.path.join(
            CONTRACTS["datalake"]["physicalPath"],
            company_id,
        )
        file_size = os.path.getsize(file_path)
        if file_size >= 2.2 * split_size:
            extra_large_files.append(company_id)
        elif file_size >= split_size:
            large_files.append(company_id)
        elif (split_size / 8) <= file_size < split_size:
            medium_files.append(company_id)
        else:
            small_files.append(company_id)
    return extra_large_files, large_files, medium_files, small_files


def process_small_files(small_files: list) -> None:
    """
    Process small files using multiprocessing.

    Args:
        small_files (list): A list of small file paths to be processed.
    """
    write_log("Processing small files")
    num_cores = multiprocessing.cpu_count()
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = [
            executor.submit(read_aneel_company_files, company_id)
            for company_id in tqdm(small_files, desc="Processing small files")
        ]
        concurrent.futures.wait(futures)


def proccess_medium_files(medium_files: list) -> None:
    """
    Process medium files using concurrent.futures.ProcessPoolExecutor.

    Args:
        medium_files (list): A list of medium files to be processed.
    """
    write_log("Processing medium files")
    num_cores = min(3, multiprocessing.cpu_count())
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = [
            executor.submit(read_aneel_company_files, company_id)
            for company_id in tqdm(medium_files, desc="Processing medium files")
        ]
        concurrent.futures.wait(futures)


def process_large_files(large_files) -> None:
    """
    Process a list of large files.

    Args:
        large_files (list): A list of file ids.
    """
    for company_id in tqdm(large_files, desc="Processing large files"):
        write_log(
            f"Reading large file: {company_id}",
        )
        read_aneel_company_files(company_id)


def process_extra_large_files(extra_large_files: list) -> None:
    """
    Process a list of extra large files.

    This function iterates over a list of extra large file ids and calls the
    `read_aneel_company_files` function to read each file.

    Args:
        extra_large_files (list): A list of extra large file ids.
    """
    for company_id in tqdm(extra_large_files, desc="Processing extra large files"):
        write_log(
            f"Reading extra large file: {company_id}",
        )
        read_aneel_company_files(company_id, is_large_file=True)


def main():
    """
    Main function for making dataset of ANEEL companies.

    This function loads ANEEL IDs, splits file sizes, and reads ANEEL
    company files using multiprocessing.
    It utilizes concurrent.futures.ProcessPoolExecutor to parallelize the
    file reading process.
    """
    df_aneel_ids = load_aneel_ids()
    extra_large_files, large_files, medium_files, small_files = split_file_sizes(
        df_aneel_ids
    )
    process_small_files(small_files)
    proccess_medium_files(medium_files)
    process_large_files(large_files)
    process_extra_large_files(extra_large_files)
