"""
This script creates a new table in the 'bronze' database by aggregating data
from the 'bronze' database.
It calculates the total size for each combination of 'hex_col' and 'value'
columns in the 'bronze' table,
and stores the result in the 'bronze' table.
"""

import os
from tqdm import tqdm

from src.tools.managers.db_connector import DBConnection
from src.tools.utils.common import get_db_path

from src.databases.bronze.mapbiomas.config import manager, CONTRACTS_BRONZE, YEARS

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


def create_indexes(year: int) -> None:
    """
    Creates indexes on the 'mapbiomas' table.

    Args:
        year (int): The year to be processed.
    """
    conn = DBConnection("bronze")
    schema = CONTRACTS_BRONZE[f"brasil_coverage_{year}"]["schema"]
    table_name = CONTRACTS_BRONZE[f"brasil_coverage_{year}"]["tableName"]
    conn.create_index(schema, table_name, ["hex_col", "value"])
    conn.close()


def create_grouped_by_hex_mapbiomas(year: int) -> None:
    """
    Creates a new table with grouped data based on hex_col and value columns from an existing table.

    Args:
        year (int): The year to be processed.
    """
    conn = DBConnection("bronze")
    contract_mapbiomas = CONTRACTS_BRONZE[f"brasil_coverage_{year}"]
    contract_mapbiomas_hex = CONTRACTS_BRONZE[f"grouped_by_hex_brasil_coverage_{year}"]
    old_path = get_db_path(contract_mapbiomas)
    new_path = get_db_path(contract_mapbiomas_hex)
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
    for year in tqdm(YEARS, desc="Creating grouped by hex mapbiomas"):
        create_indexes(year)
        create_grouped_by_hex_mapbiomas(year)
    manager.update_last_run()


if __name__ == "__main__":
    main()
