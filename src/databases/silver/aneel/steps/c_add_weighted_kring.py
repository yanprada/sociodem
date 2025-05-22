"""
This script processes municipality data from the ANEEL database and applies
a weighted kring smoothing algorithm to the data.
It saves the processed data to a parquet file and creates an index for the saved data.
It uses multiprocessing to speed up the processing of multiple municipalities.
"""

import os
from typing import List
import multiprocessing
import concurrent.futures
from functools import reduce
import pandas as pd
import geopandas as gpd
from tqdm import tqdm


from src.tools.utils.h3 import get_h3_geom, weighted_kring_smoothing
from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.common import get_db_path, write_log
from src.tools.utils.constants import CRS_GLOBAL
from src.tools.utils.save import save_parquet_decorator
from src.databases.silver.aneel.config import manager, CONTRACT_SILVER_ENERGY, YEARS


from src.databases.silver.censo.config import (
    PATHS_MV as PATHS_MV_CENSO,
    CONTRACTS_SILVER as CONTRACTS_SILVER_CENSO,
)

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)

COEF = [1, 0.8, 0.4, 0.15, 0.05]

COLS = [
    "energy_consumption_residencial_urbano",
    "energy_consumption_residencial_rural",
    "energy_consumption_comercial",
    "energy_consumption_industrial",
    "energy_consumption_agropecuaria",
    "energy_consumption_poder_publico",
    "energy_consumption_rodovias",
    "energy_consumption_servicos_publicos",
    "mean_energy_consumption_residencial_urbano",
]


@save_parquet_decorator("silver")
def save_data(df, **kwargs):
    """
    Save the DataFrame to a parquet file.
    Args:
        df (pd.DataFrame): The DataFrame to save.
        **kwargs: Additional arguments for saving the DataFrame.
    """
    return df


def create_mun_data(
    cd_mun: str,
    path_aneel: str,
    path_hex_sc: str,
    year: int,
) -> None:
    """
    Create a DataFrame for a specific municipality and save it to a parquet file.
    Args:
        cd_mun (str): The municipality code.
        path_aneel (str): The path to the ANEEL data.
        path_hex_sc (str): The path to the hexagon data.
        year (int): The year of the data.
    """
    write_log(f"Processing municipality {cd_mun}.")
    conn = DBConnection("silver")
    query = f"""
            with hex_city as (
                select hsc.hex_col 
                from {path_hex_sc} hsc
                where hsc.cd_mun = '{cd_mun}'
            )
            select ah.*
            from {path_aneel} ah 
            join hex_city
            on hex_city.hex_col = ah.hex_col
        """
    df = conn.query_database(query)
    conn.close()
    if df.empty:
        df = pd.DataFrame({"cd_mun": [cd_mun]})
        kwargs = {
            "filename": cd_mun,
            "contract": CONTRACT_SILVER_ENERGY[f"aneel_no_mun_{year}"],
        }
        save_data(df, **kwargs)
        print(f"Municipality {cd_mun} not found in the database.")
        return None
    dfs = []
    write_log("Applying weighted kring smoothing.")
    for col in COLS:
        dfs.append(weighted_kring_smoothing(df, "hex_col", col, COEF))
    merged_df = reduce(
        lambda left, right: pd.merge(left, right, on="hex_col", how="inner"), dfs
    )
    merged_df.columns = [
        f"kring_{col}" if col in COLS else col for col in merged_df.columns
    ]
    df = pd.merge(df, merged_df, on="hex_col", how="inner")
    df["geometry"] = df["hex_col"].apply(get_h3_geom)  # type: ignore
    df["cd_mun"] = cd_mun
    df = gpd.GeoDataFrame(df, geometry="geometry", crs=CRS_GLOBAL)
    kwargs = {
        "filename": cd_mun,
        "contract": CONTRACT_SILVER_ENERGY[f"aneel_kring_hexagon_grp_{year}"],
    }
    save_data(df, **kwargs)
    return None


def process_in_parallel_executor(
    cd_mun_list: List[str], path_aneel: str, path_hex_sc: str, year: int
) -> None:
    """
    Process the municipalities in parallel using a process pool executor.
    Args:
        cd_mun_list (List[str]): The list of municipality codes.
        path_aneel (str): The path to the ANEEL data.
        path_hex_sc (str): The path to the hexagon data.
        year (int): The year of the data.
    """
    num_workers = multiprocessing.cpu_count()
    batch_size = 10
    write_log(
        f"Processing {len(cd_mun_list)} municipalities in batches of {batch_size}."
    )
    batches = [
        cd_mun_list[i : i + batch_size] for i in range(0, len(cd_mun_list), batch_size)
    ]

    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for batch in batches:
            for cd_mun in batch:
                futures.append(
                    executor.submit(
                        create_mun_data, cd_mun, path_aneel, path_hex_sc, year
                    )
                )

        for future in tqdm(
            concurrent.futures.as_completed(futures),
            total=len(futures),
            desc="Processing municipalities",
        ):
            try:
                _ = future.result()
            except Exception as e:
                write_log(f"Error processing municipality: {e}", level="error")


def main():
    """
    Main function to process the municipalities and save the data.
    """
    for year in tqdm(YEARS, desc="Processing years"):
        path_aneel = get_db_path(CONTRACT_SILVER_ENERGY[f"aneel_hexagon_grp_{year}"])
        path_already_processed = get_db_path(
            CONTRACT_SILVER_ENERGY[f"aneel_kring_hexagon_grp_{year}"]
        )
        schema_sc = CONTRACTS_SILVER_CENSO["sectors_2022"]["schema"]
        path_hex_sc = f"{schema_sc}.{PATHS_MV_CENSO['hex_unique_sc_2022_sc_info']}"
        conn = DBConnection("silver")
        write_log(f"Reading municipalities from {path_hex_sc} table.")
        df_cd_mun = conn.query_database(f"SELECT DISTINCT cd_mun FROM {path_hex_sc}")
        df_cd_mun_already_processed = conn.query_database(
            f"SELECT DISTINCT cd_mun FROM {path_already_processed}"
        )

        cd_mun_list = df_cd_mun["cd_mun"].tolist()
        if not df_cd_mun_already_processed.empty:
            list_already_processed = df_cd_mun_already_processed["cd_mun"].tolist()
            cd_mun_list = list(set(cd_mun_list) - set(list_already_processed))
        process_in_parallel_executor(cd_mun_list, path_aneel, path_hex_sc, year)
        write_log(f"Creating index for {path_already_processed}.")
        schema = path_already_processed.split(".", maxsplit=1)[0]
        table_name = path_already_processed.split(".", maxsplit=1)[1]
        conn.create_index(schema, table_name, ["cd_mun"])
        conn.close()
