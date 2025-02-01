"""
This script creates a new table in the 'bronze' database by aggregating data 
from the 'bronze' database.
It calculates the total size for each combination of 'hex_col' and 'value' 
columns in the 'bronze' table,
and stores the result in the 'bronze' table.
"""

import os
from tqdm import tqdm

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.common import get_db_path

from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.mapbiomas.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(execution_parameters, module_name)

CONTRACTS_BRONZE = execution_parameters["data_contracts"]["mapbiomas_bronze"]


def create_indexes(year: int) -> None:
    """
    Creates indexes on the 'mapbiomas' table.

    Args:
        year (int): The year to be processed.
    """
    conn = DBConnection("bronze")
    schema = CONTRACTS_BRONZE["mapbiomas"]["schema"]
    table_name = CONTRACTS_BRONZE["mapbiomas"]["tableName"].format(year=year)
    conn.create_index(schema, table_name, ["hex_col", "value"])
    conn.close()


def create_grouped_by_hex_mapbiomas(year: int) -> None:
    """
    Creates a new table with grouped data based on hex_col and value columns from an existing table.

    Args:
        year (int): The year to be processed.
    """
    conn = DBConnection("bronze")
    contract_mapbiomas = CONTRACTS_BRONZE["mapbiomas"]
    contract_mapbiomas_hex = CONTRACTS_BRONZE["grouped_by_hex_mapbiomas"]
    old_path = get_db_path(contract_mapbiomas).format(year=year)
    new_path = get_db_path(contract_mapbiomas_hex).format(year=year)
    query = f"""
    SELECT hex_col, value, SUM(size) AS total_count
    FROM {old_path}
    GROUP BY hex_col, value;
    """
    conn.create_table_from_sql(query, new_path)
    conn.close()


def main() -> None:
    """
    This function creates a new table in the 'bronze' database by aggregating data
    from the 'bronze' database.
    It calculates the total size for each combination of 'hex_col' and 'value' columns
    in the 'bronze' table,
    and stores the result in the 'bronze' table.
    """
    year_init, year_end = CONTRACTS_BRONZE["mapbiomas"]["queryYears"]
    for year in tqdm(
        range(year_init, year_end), desc="Creating grouped by hex mapbiomas"
    ):
        create_indexes(year)
        create_grouped_by_hex_mapbiomas(year)
    manager.update_last_run()


if __name__ == "__main__":
    main()
