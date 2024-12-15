"""
This module processes census data from the bronze database and saves it to the silver database.
It includes functions to retrieve and transform data, adding a hex index column to the GeoDataFrame
based on the geometry column, and saving the transformed data.

Functions:
- save_data: Adds a hex index column to the GeoDataFrame and saves it.
- main: Main function to execute the data processing steps.

Modules:
- logging: For logging messages.
- pandas: For data manipulation.
- geopandas: For geospatial data manipulation.
- src.tools.databases.data_connection.connection: For database connection.
- src.tools.utils.constants: For coordinate reference system constants.
- src.tools.utils.execution_manager: For managing execution details.
- src.tools.utils.common: For common utility functions.
- src.tools.utils.save: For saving data with a decorator.
- src.databases.silver.censo.config: For configuration parameters.
- config.run_mode: For run mode configuration.

"""

import logging
import pandas as pd
import geopandas as gpd

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.constants import CRS_GLOBAL, CRS_IBGE
from src.tools.utils.execution_manager import ExecutionManager

from src.tools.utils.common import get_db_path
from src.tools.utils.save import save_parquet_decorator

from src.databases.silver.censo.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

logging.getLogger("distributed").setLevel(logging.WARNING)

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)

manager.update_status("running_step_4")

CONTRACT_SCS_CENSO_BRONZE = execution_parameters["data_contracts"]["censo_bronze"]
CONTRACT_SCS_CENSO_SILVER = execution_parameters["data_contracts"]["censo_silver"]


@save_parquet_decorator("silver", CONTRACT_SCS_CENSO_SILVER["sectors_2022"])
def save_data(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Adds a hex index column to the GeoDataFrame based on the geometry column.
    Parameters:
    - df (gpd.GeoDataFrame): The input GeoDataFrame.
    - **kwargs: Additional arguments.
    Returns:
    - gpd.GeoDataFrame: The modified GeoDataFrame with the hex index column added.
    """
    df["geometry"] = gpd.GeoSeries.from_wkb(df["geometry"])
    df = gpd.GeoDataFrame(df, geometry="geometry", crs=CRS_IBGE).to_crs(CRS_GLOBAL)
    return df


def main():
    """
    Main function.
    """
    conn = DBConnection("bronze")
    path = get_db_path(CONTRACT_SCS_CENSO_BRONZE["sectors_2022"])
    df = conn.query_database(f"SELECT * FROM {path}", geo=False)
    df = save_data(df)
    manager.update_status("finished_step_4")
    manager.update_last_run()
