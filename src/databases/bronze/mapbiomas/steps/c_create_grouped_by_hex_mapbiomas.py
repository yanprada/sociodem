"""
This script creates a new table in the 'bronze' database by aggregating data 
from the 'bronze' database.
It calculates the total size for each combination of 'hex_col' and 'value' 
columns in the 'bronze' table,
and stores the result in the 'bronze' table.
"""

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.common import get_db_path

from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.mapbiomas.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
CONTRACTS_BRONZE = execution_parameters["data_contracts"]["mapbiomas_bronze"]
manager.update_status("running_step_4")


def create_grouped_by_hex_mapbiomas(conn: DBConnection) -> None:
    """
    Creates a new table with grouped data based on hex_col and value columns from an existing table.

    Args:
        conn (DBConnection): The database connection object.
    """
    contract_mapbiomas = CONTRACTS_BRONZE["mapbiomas"]
    contract_mapbiomas_hex = CONTRACTS_BRONZE["grouped_by_hex_mapbiomas"]
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
    contract_mapbiomas_hex = CONTRACTS_BRONZE["grouped_by_hex_mapbiomas"]
    contract_mapbiomas_unique_hex = CONTRACTS_BRONZE["unique_hex_ids"]
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
    This function creates a new table in the 'bronze' database by aggregating data
    from the 'bronze' database.
    It calculates the total size for each combination of 'hex_col' and 'value' columns
    in the 'bronze' table,
    and stores the result in the 'bronze' table.
    """
    conn = DBConnection("bronze")
    create_grouped_by_hex_mapbiomas(conn)
    create_unique_hex_ids_mapbiomas(conn)
    manager.update_status("finished_step_4")
    manager.update_last_run()


if __name__ == "__main__":
    main()
