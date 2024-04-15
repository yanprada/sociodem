"""
This script is used to upload ANEEL (Agência Nacional de Energia Elétrica) 
dataset files to the database.
It contains functions to upload different types of files (PONNOT, UCBT, RAMLIG) 
and process each row title from the 'df_aneel_ids' DataFrame.

Functions:
- upload_ponnot(row_title): Uploads ANEEL PONNOT files to the database.
- save_df_problematic(df_ucbt): Filters the given DataFrame to remove rows 
        where the 'pn_con' column is empty.
- upload_ucbt(row_title): Uploads ANEEL UCBT files to the database.
- upload_ramlig(row_title): Uploads ANEEL RAMLIG files to the database.
- main(): Processes each row title from the 'df_aneel_ids' DataFrame, 
        writes a log message, and uploads data to various services.
"""

import os
from shapely import wkt
import geopandas as gpd
from tqdm import tqdm
from src.tools.utils.read import Reader
from src.tools.utils.common import write_log
from src.tools.utils.config import get_contract
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.constants import CRS
from src.databases.bronze.aneel.make_dataset_aneel_companies import load_aneel_ids


CONTRACT_ID = get_contract("aneel/contract_aneel_companies_id.yaml", "silver")
CONTRACT_PONNOT = get_contract("aneel/contract_aneel_companies_ponnot.yaml", "bronze")
CONTRACT_UCBT = get_contract("aneel/contract_aneel_companies_ucbt.yaml", "bronze")
CONTRACT_RAMLIG = get_contract("aneel/contract_aneel_companies_ramlig.yaml", "bronze")


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_PONNOT, save_pq=False)
def upload_ponnot(row_title):
    """
    Uploads ANEEL PONNOT files to the database.
    """
    cols = [col["column"] for col in CONTRACT_PONNOT["columns"]]
    file_name = "_".join([row_title.split(".")[0], "ponnot"])
    file_path_large_files = os.path.join(CONTRACT_PONNOT["physicalPath"], file_name)
    file_path_small_files = os.path.join(
        CONTRACT_PONNOT["physicalPath"], "".join([file_name, ".parquet"])
    )
    exist_large_file = os.path.exists(file_path_large_files)
    reader = Reader(CONTRACT_PONNOT)
    if exist_large_file:
        df_ponnot = reader.read_parquet(file_path=file_path_large_files, columns=cols)
        df_ponnot = gpd.GeoDataFrame(
            df_ponnot,
            geometry=df_ponnot.filter(like="geom").map(wkt.loads).squeeze(),
            crs=CRS,
        )
    else:
        df_ponnot = reader.read_geoparquet(
            file_path=file_path_small_files, columns=cols
        )
    return df_ponnot


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_UCBT, save_pq=False)
def upload_ucbt(row_title):
    """
    Uploads ANEEL UCBT files to the database.
    """
    cols = [col["column"] for col in CONTRACT_UCBT["columns"]]
    file_name = "_".join([row_title.split(".")[0], "ucbt"])
    file_path_large_files = os.path.join(CONTRACT_UCBT["physicalPath"], file_name)
    file_path_small_files = os.path.join(
        CONTRACT_UCBT["physicalPath"], "".join([file_name, ".parquet"])
    )
    exist_large_file = os.path.exists(file_path_large_files)
    file = file_path_large_files if exist_large_file else file_path_small_files
    reader = Reader(CONTRACT_UCBT)
    df_ucbt = reader.read_parquet(file_path=file, columns=cols)
    return df_ucbt


@save_parquet_decorator(medallon="bronze", contract=CONTRACT_RAMLIG, save_pq=False)
def upload_ramlig(row_title):
    """
    Uploads ANEEL RAMLIG files to the database.
    """
    cols = [col["column"] for col in CONTRACT_RAMLIG["columns"]]
    file_name = "_".join([row_title.split(".")[0], "ramlig"])
    file_path_large_files = os.path.join(CONTRACT_RAMLIG["physicalPath"], file_name)
    file_path_small_files = os.path.join(
        CONTRACT_RAMLIG["physicalPath"], "".join([file_name, ".parquet"])
    )
    exist_large_file = os.path.exists(file_path_large_files)
    file = file_path_large_files if exist_large_file else file_path_small_files
    reader = Reader(CONTRACT_RAMLIG)
    df_ramlig = reader.read_parquet(file_path=file, columns=cols)
    return df_ramlig


def main():
    """
    This function processes each row title from the 'df_aneel_ids' DataFrame,
    writes a log message, and uploads data to various services.
    """
    df_aneel_ids = load_aneel_ids()
    # retirar arquivo que não tem camada ucbt
    df_aneel_ids = df_aneel_ids.query(
        "title != 'EAC_26_2022-12-31_V11_20230725-1759.gdb.zip'"
    )
    for row_title in tqdm(df_aneel_ids["title"]):
        write_log(f"Processing row title: {row_title}")
        _ = upload_ponnot(row_title)
        _ = upload_ramlig(row_title)
        _ = upload_ucbt(row_title)
