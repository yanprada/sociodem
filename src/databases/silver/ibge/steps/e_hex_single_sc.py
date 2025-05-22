"""Move the table from bronze to silver database."""

import os
import pandas as pd

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.common import get_db_path, write_log
from src.tools.utils.save import save_parquet_decorator

from src.databases.silver.ibge.config import (
    manager,
    CONTRACTS_SILVER,
    CONTRACTS_BRONZE,
    PATHS_MV,
)


module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


@save_parquet_decorator("silver", save_pq=False)
def move_sc_table_to_silver_db(**kwargs) -> pd.DataFrame:
    """
    Moves the table from bronze to silver database.
    Args:
        **kwargs: Additional arguments.
    Returns:
        pd.DataFrame: The DataFrame containing the data from the bronze database.
    """
    table_name = CONTRACTS_BRONZE["sectors_2022"]["table_name"]
    write_log(f"Moving table {table_name} from bronze to silver database.")
    conn = DBConnection("bronze")
    path = get_db_path(CONTRACTS_BRONZE["sectors_2022"])
    df = conn.query_database(f"SELECT * FROM {path}")
    return df


def create_materialized_view_sc_hex() -> None:
    """
    Creates a materialized view in the silver database.
    """
    write_log(
        f"Creating materialized view {PATHS_MV['hex_unique_sc_2022']} in silver database."
    )
    conn = DBConnection("silver")
    new_path = PATHS_MV["hex_unique_sc_2022"]
    schema = CONTRACTS_SILVER["sectors_2022"]["schema"]
    new_path = f"{schema}.{new_path}"
    path_pct_hex_in_hex = get_db_path(CONTRACTS_SILVER["hex_participation_sc_2022"])
    query = f"""
            WITH rank_table AS (
                SELECT  hex_col,
                        cd_setor,
                        ROW_NUMBER() OVER (
                        PARTITION BY hex_col 
                        ORDER BY pct_dompp_total_domicilio_particular DESC
                        ) AS rn
                FROM {path_pct_hex_in_hex}
                )
                SELECT *
                FROM rank_table
                WHERE rn = 1
    """
    conn.create_materialized_view(query, new_path)


def add_sc_info() -> str:
    """
    Adds the SC information to the table.

    Returns:
        str: The path of the new table with SC information.
    """
    write_log(
        f"Adding SC information to the table {PATHS_MV['hex_unique_sc_2022']}"
        "_sc_info in silver database."
    )
    conn = DBConnection("silver")
    schema = CONTRACTS_SILVER["sectors_2022"]["schema"]
    path_mv = f"{schema}.{PATHS_MV['hex_unique_sc_2022']}"
    path_sc = get_db_path(CONTRACTS_SILVER["sectors_2022"])
    new_path = f"{schema}.{PATHS_MV['hex_unique_sc_2022_sc_info']}"
    query = f"""
            SELECT h.cd_setor,
                h.hex_col,
                s.cd_regiao,
                s.nm_regiao,
                s.cd_uf,
                s.nm_uf,
                s.cd_mun,
                s.nm_mun,
                s.cd_dist,
                s.nm_dist,
                s.cd_subdist,
                s.nm_subdist,
                cd_micro,
                nm_micro,
                cd_meso,
                nm_meso
            FROM {path_mv} as h
            LEFT JOIN {path_sc} as s
            ON s.cd_setor = h.cd_setor
            """
    conn.create_materialized_view(query, new_path)
    return new_path


def main() -> None:
    """
    Main function to execute the script.
    """
    kwargs = {
        "contract": CONTRACTS_SILVER["sectors_2022"],
    }
    conn = DBConnection("silver")
    path = get_db_path(CONTRACTS_SILVER["sectors_2022"])
    df = conn.query_database(f"SELECT * FROM {path} LIMIT 1")
    if df.empty:
        _ = move_sc_table_to_silver_db(**kwargs)
    create_materialized_view_sc_hex()
    new_path = add_sc_info()

    schema = new_path.split(".", maxsplit=1)[0]
    table_name = new_path.split(".", maxsplit=1)[1]
    write_log(f"Creating indexes to {table_name} database.")
    # conn.create_index(schema, table_name, ["cd_setor"])
    # conn.create_index(schema, table_name, ["hex_col"])
    conn.create_index(schema, table_name, ["hex_col", "nm_mun"])
    conn.create_index(schema, table_name, ["hex_col", "cd_setor"])
    # conn.create_index(schema, table_name, ["nm_mun"])
    # conn.create_index(schema, table_name, ["cd_mun"])
    conn.close()
