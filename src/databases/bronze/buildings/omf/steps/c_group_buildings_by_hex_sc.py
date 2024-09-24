"""
This module contains functions to create grouped tables based on 
hex_col and value columns from an existing table.
"""

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.common import write_log, get_db_path
from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.buildings.omf.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)

CONTRACT_BRONZE = execution_parameters["data_contracts"][1]
manager.update_status("running_step_3")


def create_grouped_by_hex_google_buildings(source: str, query: str) -> None:
    """
    Creates a new table with grouped data based on hex_col and value columns from an existing table.

    Args:
        source (str): The source of the data (e.g., "google", "omf").
        query (str): The SQL query to calculate the aggregated values.
    """
    conn = DBConnection("bronze")
    contract_buildings = CONTRACT_BRONZE[f"buildings_{source}"]
    contract_building_hex = CONTRACT_BRONZE[f"buildings_{source}_grouped_by_hex"]
    old_path = get_db_path(contract_buildings)
    new_path = get_db_path(contract_building_hex)
    schema = contract_building_hex["schema"]
    table = contract_building_hex["tableName"]
    creation_query = f"""
    SELECT 
    hex_col,
    {query}
    FROM {old_path}
    GROUP BY hex_col
    """
    conn.create_table_from_sql(creation_query, new_path)
    conn.add_pk_to_table(schema, table, "hex_col")


def create_grouped_google() -> None:
    """
    Creates a new table with grouped data based on hex_col and value columns from the Google source.
    """
    write_log("Creating grouped table for Google buildings")
    query = """
    COUNT(*) AS buildings_count,
    AVG(area_in_meters) AS buildings_mean_area,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY area_in_meters) AS buildings_median_area,
    STDDEV(area_in_meters) AS buildings_std_area,
    SUM(area_in_meters) AS buildings_total_area
    """
    create_grouped_by_hex_google_buildings("google", query)


def create_grouped_omf() -> None:
    """
    Creates a new table with grouped data based on hex_col and value columns from the OMF source.
    """
    write_log("Creating grouped table for OMF buildings")
    query = """
    COUNT(*) AS buildings_count,
    AVG(height) AS buildings_mean_height,
    STDDEV(height) AS buildings_std_height,
    AVG(numfloors) AS buildings_mean_num_floors,
    STDDEV(numfloors) AS buildings_std_num_floors
    """
    create_grouped_by_hex_google_buildings("omf", query)


def create_joined_table() -> None:
    """
    Creates a new table with joined data based on hex_col and value
    columns from the Google and OMF sources.
    """
    write_log("Creating joined table for Google and OMF buildings")
    conn = DBConnection("bronze")
    contract_building_google = CONTRACT_BRONZE["buildings_google_grouped_by_hex"]
    contract_building_omf = CONTRACT_BRONZE["buildings_omf_grouped_by_hex"]
    contract_joined_buildings = CONTRACT_BRONZE["joined_building"]
    google_path = get_db_path(contract_building_google)
    omf_path = get_db_path(contract_building_omf)
    new_path = get_db_path(contract_joined_buildings)
    schema = contract_joined_buildings["schema"]
    table = contract_joined_buildings["tableName"]
    creation_query = f"""
    SELECT 
    google.hex_col,
    google.buildings_count AS google_buildings_count,
    google.buildings_mean_area AS buildings_mean_area,
    google.buildings_median_area AS buildings_median_area,
    google.buildings_std_area AS buildings_std_area,
    google.buildings_total_area AS buildings_total_area,
    omf.buildings_count AS omf_buildings_count,
    omf.buildings_mean_height AS buildings_mean_height,
    omf.buildings_std_height AS buildings_std_height,
    omf.buildings_mean_num_floors AS buildings_mean_num_floors,
    omf.buildings_std_num_floors AS buildings_std_num_floors
    FROM {google_path} AS google
    FULL OUTER JOIN {omf_path} AS omf
    ON google.hex_col = omf.hex_col
    """
    conn.create_table_from_sql(creation_query, new_path)
    conn.add_pk_to_table(schema, table, "hex_col")


def main():
    """
    This is the main function of the pipeline module.
    It executes the necessary steps to process the buildings data.
    """
    create_grouped_google()
    create_grouped_omf()
    create_joined_table()
    manager.update_status("finished_step_3")
    manager.update_last_run()
