"""
This script creates a new table in the 'silver' database by aggregating data 
from the 'bronze' database.
It calculates the total size for each combination of 'hex_col' and 'value' 
columns in the 'bronze' table,
and stores the result in the 'silver' table.
"""

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.data_contract.mapbiomas_data_contract import get_mapbiomas_contracts

CONTRACT_BRONZE = get_mapbiomas_contracts("bronze")


def main():
    """
    This function creates a new table in the 'silver' database by aggregating data
    from the 'bronze' database.
    It calculates the total size for each combination of 'hex_col' and 'value' columns
    in the 'bronze' table,
    and stores the result in the 'silver' table.
    """
    conn = DBConnection("bronze")
    old_path = ".".join(
        [
            CONTRACT_BRONZE["mapbiomas_2022"]["schema"],
            CONTRACT_BRONZE["mapbiomas_2022"]["tableName"],
        ]
    )
    new_path = ".".join(
        [
            CONTRACT_BRONZE["grouped_by_hex_mapbiomas_2022"]["schema"],
            CONTRACT_BRONZE["grouped_by_hex_mapbiomas_2022"]["tableName"],
        ]
    )
    query = f"""
    SELECT hex_col, value, SUM(size) AS total_count
    FROM {old_path}
    GROUP BY hex_col, value;
    """
    conn.create_table_from_sql(query, new_path)
