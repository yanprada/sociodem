"""
This script retrieves data from the Censo 2010 and 
Censo 2022 datasets and saves it as parquet files and to database.

The script contains the following functions:
- get_mun_2010: Retrieves the municipalities data from the Censo 2010 dataset.
- get_mun_2022: Retrieves the municipalities data from the Censo 2022 dataset.
- get_sectors_2010: Retrieves the census sectors data from the Censo 2010 dataset.
- main: The main function that executes the script.

Note: The script assumes the existence of certain contract files and directories.
"""

import os
import zipfile
import time
import mlflow
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm

from src.tools.utils.reader import Reader
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.constants import STATES, CRS_GLOBAL, CRS_IBGE
from src.tools.utils.common import write_log, get_ml_flow_data
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.censo.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG


manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
CONTRACTS_BRONZE = execution_parameters["data_contracts"]["bronze"]
CONTRACTS_RAW_DATA = execution_parameters["data_contracts"]["raw_data"]

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(execution_parameters, module_name)

EXPERIMENT_ID = execution_parameters["mlflow_experiment"]
mlflow.set_experiment(EXPERIMENT_ID)


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS_BRONZE["dompp_2022"])
def get_dompp_per_state_2022(state, **kwargs):
    """
    Retrieves the DOMPP data for a specific state in 2022.

    Args:
        state (str): The abbreviation of the state for which the data is requested.
        **kwargs: Additional keyword arguments.

    Returns:
        pandas.DataFrame: The DOMPP data for the specified state in 2022.

    Raises:
        FileNotFoundError: If the file for the specified state is not found.

    """
    reader = Reader()
    filepath = os.path.join(
        CONTRACTS_RAW_DATA["dompp_2022"]["physicalPath"],
        "".join([state, ".zip"]),
    )
    with zipfile.ZipFile(filepath, "r") as zip_ref:
        csv_filename = zip_ref.namelist()[0]
        with zip_ref.open(csv_filename) as csv_file:
            df = reader.read_csv(csv_file, sep=";")
            df = (
                df.groupby(df.columns.tolist(), as_index=False)
                .size()
                .rename(columns={"size": "count"})
                .astype({"cod_uf": "category", "cod_mun": "category"})
            )
            if "latitude" in df.columns and "longitude" in df.columns:
                df = gpd.GeoDataFrame(
                    df,
                    geometry=[Point(xy) for xy in zip(df["longitude"], df["latitude"])],
                    crs=CRS_IBGE,
                ).to_crs(CRS_GLOBAL)
            return df


def get_dompp_2022():
    """
    Retrieves the DOMPP data from the Censo 2022 dataset.
    """
    for state in tqdm(STATES):
        with mlflow.start_run(run_name=state, nested=True):
            df = get_dompp_per_state_2022(state)
            add_mlflow_metrics(df)


def upload_dompp_2022(run_name_id: str):
    """
    Uploads the DOMPP 2022 data if the specified run name ID is not present in the MLflow runs.
    Args:
        run_name_id (str): The run name ID to check in the MLflow runs.
    """
    write_log("Processing dompp data...")
    mlflow_runs_df = get_ml_flow_data(EXPERIMENT_ID)
    if run_name_id not in mlflow_runs_df["mlflow.runName"]:
        get_dompp_2022()


def add_mlflow_metrics(df: pd.DataFrame):
    """
    Adds metrics to the mlflow run.

    Args:
        df (pd.DataFrame): The DataFrame to add metrics to.
    """
    mlflow.log_metric("num_rows", df.shape[0])


def upload_censo_data(layer_key: str, run_name_id: str):
    """
    Uploads municipalities data for the year 2010.

    This function processes the municipalities data and checks if the data already exists.
    If the data does not exist, it calls the `get_mun_2010` function to retrieve it.
    """

    @save_parquet_decorator(medallon="bronze", contract=CONTRACTS_BRONZE[layer_key])
    def get_censo_data(layer_key, **kwargs):
        """
        Retrieves the data from the Censo dataset.

        Args:
            layer_key (str): The key of the layer to use.
            **kwargs: Additional keyword arguments.

        Returns:
            pandas.DataFrame: A DataFrame containing the data.
        """
        reader = Reader()
        dfs = []
        for state in tqdm(STATES):
            with mlflow.start_run(run_name=state, nested=True):
                filepath = os.path.join(
                    CONTRACTS_RAW_DATA[layer_key]["physicalPath"],
                    "".join([state, ".zip"]),
                )
                df = reader.read_geofile(filepath)
                df = df.to_crs(CRS_GLOBAL)
                add_mlflow_metrics(df)
                dfs.append(df)
        dfs = pd.concat(dfs)
        return dfs

    write_log(f"Processing {layer_key} data...")
    mlflow_runs_df = get_ml_flow_data(EXPERIMENT_ID)
    if run_name_id not in mlflow_runs_df["mlflow.runName"]:
        _ = get_censo_data(layer_key)
    else:
        write_log(f"{layer_key} data already exists.")


@save_parquet_decorator(medallon="bronze", contract=CONTRACTS_BRONZE["states_2022"])
def get_states_data():
    """
    Retrieves the geographical data for all states.
    """
    reader = Reader()
    dfs = []
    for state in tqdm(STATES):
        filepath = os.path.join(
            CONTRACTS_RAW_DATA["states_2022"]["physicalPath"],
            "".join([state, ".geojson"]),
        )
        df = reader.read_geofile(filepath)
        df = df.to_crs(CRS_GLOBAL)
        dfs.append(df)
    dfs = pd.concat(dfs)
    return dfs


def upload_states_2022(run_name_id: str):
    """
    Processes and uploads state data for the year 2022.
    This function reads geographical data files for each state,
    concatenates them into a single DataFrame,
    and returns the combined DataFrame. It logs the processing
    steps and uses a Reader object to read the geojson files.
    """
    write_log("Processing states data...")
    mlflow_runs_df = get_ml_flow_data(EXPERIMENT_ID)
    if run_name_id not in mlflow_runs_df["mlflow.runName"]:
        get_states_data()


def main():
    """
    The main function that executes the script.
    """
    run_date = time.strftime("%Y-%m")
    for layer_key in [
        "mun_2010",
        "mun_2022",
        "sectors_2010",
        "sectors_2022",
        "districts_2010",
        "districts_2022",
        "subdistricts_2010",
        "subdistricts_2022",
    ]:
        run_name_id = "-".join([layer_key, run_date])
        with mlflow.start_run(run_name=run_name_id):
            upload_censo_data(layer_key, run_name_id)

    run_name_id = "-".join(["dompp", run_date])
    with mlflow.start_run(run_name=run_name_id):
        upload_dompp_2022(run_name_id)

    run_name_id = "-".join(["states", run_date])
    with mlflow.start_run(run_name=run_name_id):
        upload_states_2022(run_name_id)
    manager.update_last_run()
