"""
This script processes and saves census data to Glacier storage. 
It uses a multi-threaded approach to handle small municipalities and a 
single-threaded approach for large municipalities.
"""

import os
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import pandas as pd

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.execution_manager import ExecutionManager

from src.tools.utils.common import get_db_path
from src.tools.utils.save import save_parquet_decorator

from src.databases.silver.censo.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)

manager.update_status("running_step_4")

CONTRACT_SCS_CENSO_SILVER = execution_parameters["data_contracts"]["censo_silver"]


@save_parquet_decorator(
    "silver", CONTRACT_SCS_CENSO_SILVER["sectors_2022_hex"], save_db=False, save_pq=True
)
def save_data(df: pd.DataFrame, **kwargs):
    """
    Save the data to Glacier.
    """
    return df


def process_municipality(mun, df_mun_sc):
    """
    Processes data for a specific municipality and saves it to a specified location.
    Args:
        mun (int or str): The municipality code to process.
        df_mun_sc (pandas.DataFrame): DataFrame containing sector codes for municipalities.
    """
    conn = DBConnection("silver")
    scs_list = tuple(df_mun_sc[df_mun_sc["cd_mun"] == mun]["cd_setor"].unique())
    path = get_db_path(CONTRACT_SCS_CENSO_SILVER["sectors_2022_hex"])
    df = conn.query_database(f"SELECT * FROM {path} WHERE cd_setor IN {scs_list}")
    kwargs = {"filename": mun}
    save_data(df, **kwargs)
    conn.close()


def main():
    """
    Main function to process and save municipality data to Glacier.
    This function performs the following steps:
    1. Establishes a connection to the "silver" database.
    2. Retrieves the path to the database table for sectors in 2022.
    3. Queries the database to get a DataFrame containing sector and municipality codes.
    4. Groups the data by municipality and counts the number of sectors per municipality.
    5. Filters municipalities with fewer than 1000 sectors and checks if their data already exists.
    6. Processes small municipalities concurrently using a ThreadPoolExecutor.
    7. Processes large municipalities sequentially.
    """
    conn = DBConnection("silver")
    path = get_db_path(CONTRACT_SCS_CENSO_SILVER["sectors_2022"])
    df_mun_sc = conn.query_database(f"SELECT cd_setor, cd_mun FROM {path}")
    mun_count = df_mun_sc.groupby("cd_mun").count()
    municipalities = mun_count[mun_count["cd_setor"] < 1000].index
    output_dir = CONTRACT_SCS_CENSO_SILVER["sectors_2022_hex"]["physicalPath"]
    municipalities = [
        mun
        for mun in municipalities
        if not os.path.exists(os.path.join(output_dir, f"{mun}.parquet"))
        and not os.path.isdir(os.path.join(output_dir, f"{mun}"))
    ]
    with ThreadPoolExecutor() as executor:
        list(
            tqdm(
                executor.map(
                    lambda mun: process_municipality(mun, df_mun_sc),
                    municipalities,
                ),
                desc="Small Municipalities",
                total=len(municipalities),
            )
        )
    municipalities = mun_count[mun_count["cd_setor"] >= 1000].index
    for mun in tqdm(municipalities, desc="Large Municipalities"):
        process_municipality(mun, df_mun_sc)
