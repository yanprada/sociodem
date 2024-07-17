"""
This module contains functions to create grouped tables based on 
hex_col and value columns from an existing table.
"""

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.data_contract.buildings_data_contract import get_buildings_contracts
from src.tools.utils.common import write_log

CONTRACT_BRONZE = get_buildings_contracts("bronze")


def create_grouped_by_hex_google_buildings(source: str, query: str) -> None:
    """
    Creates a new table with grouped data based on hex_col and value columns from an existing table.

    Args:
        source (str): The source of the data (e.g., "google", "omf").
        query (str): The SQL query to calculate the aggregated values.
    """
    conn = DBConnection("bronze")
    old_path = ".".join(
        [
            CONTRACT_BRONZE[f"buildings_{source}"]["schema"],
            CONTRACT_BRONZE[f"buildings_{source}"]["tableName"],
        ]
    )
    schema = CONTRACT_BRONZE[f"buildings_{source}_grouped_by_hex"]["schema"]
    table = CONTRACT_BRONZE[f"buildings_{source}_grouped_by_hex"]["tableName"]
    new_path = ".".join([schema, table])
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
    google_path = ".".join(
        [
            CONTRACT_BRONZE["buildings_google_grouped_by_hex"]["schema"],
            CONTRACT_BRONZE["buildings_google_grouped_by_hex"]["tableName"],
        ]
    )
    omf_path = ".".join(
        [
            CONTRACT_BRONZE["buildings_omf_grouped_by_hex"]["schema"],
            CONTRACT_BRONZE["buildings_omf_grouped_by_hex"]["tableName"],
        ]
    )
    schema = CONTRACT_BRONZE["joined_building"]["schema"]
    table = CONTRACT_BRONZE["joined_building"]["tableName"]
    new_path = ".".join([schema, table])
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
    # create_grouped_google()
    # create_grouped_omf()
    create_joined_table()
