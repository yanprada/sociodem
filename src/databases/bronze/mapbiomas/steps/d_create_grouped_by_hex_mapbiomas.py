"""
This script creates a new table in the 'silver' database by aggregating data 
from the 'bronze' database.
It calculates the total size for each combination of 'hex_col' and 'value' 
columns in the 'bronze' table,
and stores the result in the 'silver' table.
"""

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.data_contract.mapbiomas_data_contract import get_mapbiomas_contracts
from src.tools.utils.common import get_db_path

CONTRACT_BRONZE = get_mapbiomas_contracts("bronze")


def create_grouped_by_hex_mapbiomas(conn: DBConnection) -> None:
    """
    Creates a new table with grouped data based on hex_col and value columns from an existing table.

    Args:
        conn (DBConnection): The database connection object.
    """
    contract_mapbiomas = CONTRACT_BRONZE["mapbiomas_2022"]
    contract_mapbiomas_hex = CONTRACT_BRONZE["grouped_by_hex_mapbiomas_2022"]
    old_path = get_db_path(contract_mapbiomas)
    new_path = get_db_path(contract_mapbiomas_hex)
    query = f"""
    SELECT hex_col, value, SUM(size) AS total_count
    FROM {old_path}
    GROUP BY hex_col, value;
    """
    conn.create_table_from_sql(query, new_path)


def create_unique_hex_ids_mapbiomas(conn: DBConnection) -> None:
    """
    Create a table with unique hex IDs from the existing mapbiomas table.

    Args:
        conn (DBConnection): The database connection object.
    """
    contract_mapbiomas_hex = CONTRACT_BRONZE["grouped_by_hex_mapbiomas_2022"]
    contract_mapbiomas_unique_hex = CONTRACT_BRONZE["unique_hex_ids"]
    old_path = get_db_path(contract_mapbiomas_hex)
    new_path = get_db_path(contract_mapbiomas_unique_hex)
    query = f"""
    SELECT DISTINCT hex_col
    FROM {old_path}
    """

    schema = contract_mapbiomas_unique_hex["schema"]
    table = contract_mapbiomas_unique_hex["tableName"]
    conn.create_table_from_sql(query, new_path)
    conn.create_pk(schema, table, "col_id")


def main() -> None:
    """
    This function creates a new table in the 'silver' database by aggregating data
    from the 'bronze' database.
    It calculates the total size for each combination of 'hex_col' and 'value' columns
    in the 'bronze' table,
    and stores the result in the 'silver' table.
    """
    conn = DBConnection("bronze")
    create_grouped_by_hex_mapbiomas(conn)
    create_unique_hex_ids_mapbiomas(conn)
