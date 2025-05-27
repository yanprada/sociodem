"""Move the table from bronze to silver database."""

import os
import pandas as pd

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.common import get_db_path, write_log
from src.tools.managers.saver import save_parquet_decorator

from src.databases.silver.ibge.config import manager, CONTRACTS_SILVER, CONTRACTS_BRONZE


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
    table_name = CONTRACTS_BRONZE["sectors_2022"]["tableName"]
    write_log(f"Moving table {table_name} from bronze to silver database.")
    conn = DBConnection("bronze")
    path = get_db_path(CONTRACTS_BRONZE["sectors_2022"])
    df = conn.query_database(f"SELECT * FROM {path}")
    return df


def create_table_sc_hex() -> None:
    """
    Creates a table in the silver database.
    """
    write_log("Creating table 'hex_unique_sc_2022' in silver database.")
    conn = DBConnection("silver")
    new_path = get_db_path(CONTRACTS_SILVER["hex_unique_sc_2022"])
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
    conn.create_table_from_sql(query, new_path)


def add_sc_info() -> None:
    """
    Adds the SC information to the table.
    """
    conn = DBConnection("silver")
    path_hex_sc_unique = get_db_path(CONTRACTS_SILVER["hex_unique_sc_2022"])
    path_sc = get_db_path(CONTRACTS_SILVER["sectors_2022"])
    new_path = get_db_path(CONTRACTS_SILVER["hex_unique_sc_2022_sc_info"])
    query = f"""
            SELECT h.cd_setor,
                h.hex_col,
                s.cd_regiao,
                s.nm_regiao,
                s.situacao,
                s.cd_uf,
                s.nm_uf,
                s.cd_mun,
                s.nm_mun,
                s.cd_dist,
                s.nm_dist,
                s.cd_subdist,
                s.nm_subdist
            FROM {path_hex_sc_unique} as h
            LEFT JOIN {path_sc} as s
            ON s.cd_setor = h.cd_setor
            """
    conn.create_table_from_sql(query, new_path)
    conn.close()


def move_sc_table() -> None:
    """
    Moves the table from bronze to silver database.
    """
    write_log("Moving table 'sectors_2022' from bronze to silver database.")
    kwargs = {
        "contract": CONTRACTS_SILVER["sectors_2022"],
    }
    conn = DBConnection("silver")
    path = get_db_path(CONTRACTS_SILVER["sectors_2022"])
    df = conn.query_database(f"SELECT * FROM {path} LIMIT 1")
    conn.close()
    if df.empty:
        _ = move_sc_table_to_silver_db(**kwargs)


def create_table_hex_unique_sc() -> None:
    """
    Creates the table 'hex_unique_sc_2022' in the silver database.
    """
    write_log("Creating table 'hex_unique_sc_2022' in silver database.")
    conn = DBConnection("silver")
    path = get_db_path(CONTRACTS_SILVER["hex_unique_sc_2022"])
    df = conn.query_database(f"SELECT * FROM {path} LIMIT 1")
    conn.close()
    if df.empty:
        create_table_sc_hex()


def add_sc_info_to_hex_sc_unique() -> None:
    """
    Adds the SC information to the table 'hex_unique_sc_2022_sc_info' in the silver database.
    """
    write_log(
        "Adding SC information to the table 'hex_unique_sc_2022_sc_info' in silver database."
    )
    conn = DBConnection("silver")
    path = get_db_path(CONTRACTS_SILVER["hex_unique_sc_2022_sc_info"])
    df = conn.query_database(f"SELECT * FROM {path} LIMIT 1")
    conn.close()
    if df.empty:
        add_sc_info()


def add_indexes() -> None:
    """
    Adds indexes to the table 'hex_unique_sc_2022_sc_info' in the silver database.
    """
    new_path = get_db_path(CONTRACTS_SILVER["hex_unique_sc_2022_sc_info"])
    schema = new_path.split(".", maxsplit=1)[0]
    table_name = new_path.split(".", maxsplit=1)[1]
    write_log(f"Creating indexes to {table_name} database.")
    conn = DBConnection("silver")
    write_log("Creating index 'cd_setor' to database.")
    conn.create_index(schema, table_name, ["cd_setor"])
    write_log("Creating index 'cd_mun' to database.")
    conn.create_index(schema, table_name, ["cd_mun"])
    write_log("Creating index 'hex_col' to database.")
    conn.create_index(schema, table_name, ["hex_col"])
    write_log("Creating index 'hex_col, nm_mun' to database.")
    conn.create_index(schema, table_name, ["hex_col", "nm_mun"])
    write_log("Creating index 'hex_col, cd_setor' to database.")
    conn.create_index(schema, table_name, ["hex_col", "cd_setor"])
    write_log("Creating index 'nm_mun' to database.")
    conn.create_index(schema, table_name, ["nm_mun"])
    conn.close()


def main() -> None:
    """
    Main function to execute the script.
    """
    move_sc_table()
    create_table_hex_unique_sc()
    add_sc_info_to_hex_sc_unique()
    add_indexes()
