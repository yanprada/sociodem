"""
This module contains functions to clean and preprocess the 'ponnot' and 'ucbt' tables
from the ANEEL bronze database. It removes unnecessary columns and creates primary keys
for the cleaned tables.

Functions:
    clean_ponnot(): Cleans the 'ponnot' table by removing unnecessary columns.
    clean_ucbt(): Cleans the 'ucbt' table by removing unnecessary columns.
    create_primary_key(path_table: str, pk_key: str): Creates a primary key on the 
        specified column of a table.
    main(): Executes the cleaning of the 'ucbt' and 'ponnot' tables and updates the 
        execution status.

"""

from src.tools.utils.common import get_db_path, write_log
from src.tools.databases.data_connection.connection import DBConnection

from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.aneel.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
manager.update_status("running_step_1")


ANEEL_BRONZE_CONTRACTS = execution_parameters["data_contracts"]["aneel_bronze"]


def clean_ponnot():
    """
    Cleans the 'ponnot' table by removing unnecessary columns.
    """
    write_log("Cleaning the 'ponnot' table.")
    conn = DBConnection("bronze")
    path_clean = get_db_path(ANEEL_BRONZE_CONTRACTS["ponnot_clean"])
    path = get_db_path(ANEEL_BRONZE_CONTRACTS["ponnot"])
    query = f"""
        CREATE TABLE {path_clean} AS
        SELECT DISTINCT *
        FROM {path};

        ALTER TABLE {path_clean}
        DROP COLUMN pos,
        DROP COLUMN estr,
        DROP COLUMN esf,
        DROP COLUMN alt,
        DROP COLUMN odi,
        DROP COLUMN ti,
        DROP COLUMN cm,
        DROP COLUMN tuc,
        DROP COLUMN a1,
        DROP COLUMN a2,
        DROP COLUMN a3,
        DROP COLUMN a4,
        DROP COLUMN a5,
        DROP COLUMN a6,
        DROP COLUMN descr;
    """
    conn.execute_query(query)


def clean_ucbt():
    """
    Cleans the 'ucbt' table by removing unnecessary columns.
    """
    write_log("Cleaning the 'ucbt' table.")
    conn = DBConnection("bronze")
    path_clean = get_db_path(ANEEL_BRONZE_CONTRACTS["ucbt_clean"])
    path = get_db_path(ANEEL_BRONZE_CONTRACTS["ucbt"])
    query = f"""
        CREATE TABLE {path_clean} AS
        SELECT DISTINCT *
        FROM {path};

        ALTER TABLE {path_clean}
        DROP COLUMN ceg,
        DROP COLUMN uni_tr_d,
        DROP COLUMN ctmt,
        DROP COLUMN uni_tr_s,
        DROP COLUMN ten_forn,
        DROP COLUMN car_inst,
        DROP COLUMN liv,
        DROP COLUMN dic,
        DROP COLUMN fic,
        DROP COLUMN semred,
        DROP COLUMN descr,
        DROP COLUMN cod_id;
    """
    conn.execute_query(query)


def create_primary_key(path_table: str, pk_key: str) -> None:
    """
    Creates a primary key on the ID column of ucbt table.
    """
    conn = DBConnection("bronze")
    schema, table_name = path_table.split(".")

    df = conn.query_database(f"SELECT * FROM {path_table} LIMIT 1")
    if pk_key not in df.columns:
        conn.create_pk(schema, table_name, pk_key)


def main():
    """
    Executes the cleaning of the 'ucbt' and 'ponnot' tables.
    """
    path_ponnot = get_db_path(ANEEL_BRONZE_CONTRACTS["ponnot_clean"])
    clean_ponnot()
    create_primary_key(path_ponnot, "row_id")
    clean_ucbt()
    manager.update_status("step_1_done")
