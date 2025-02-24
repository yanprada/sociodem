"""
This script performs the following operations on the 'ucbt' table in the database:
1. Creates indexes on the 'ucbt' table to optimize query performance.
2. Groups data from the 'ucbt' table and creates a materialized view named 'aggregated_ucbt' 
    with aggregated columns for year, dist, mun, conj, pn_con, clas_sub, and various 
    energy consumption metrics.

Functions:
- group_ucbt(conn: DBConnection, path_ucbt: str) -> None:
     from the specified table. The view includes aggregated columns for grouping, first occurrence, 
     sum, average, and standard deviation of monthly energy consumption.

- create_indexes(conn: DBConnection) -> None:
     Creates indexes on the 'ucbt' table to optimize query performance.

- main() -> None:
     The main function that executes the operations of creating indexes and grouping data 
     from the 'ucbt' table.

"""

import os

from src.tools.utils.common import get_db_path, write_log
from src.tools.databases.data_connection.connection import DBConnection

from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.aneel.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(execution_parameters, module_name)


ANEEL_BRONZE_CONTRACTS = execution_parameters["data_contracts"]["aneel_bronze"]


def group_ucbt(conn: DBConnection, path_ucbt: str, path_ucbt_agg: str) -> None:
    """
    Creates a materialized view named 'aggregated_ucbt' in the database by aggregating data
    from the specified table.
    The view includes the following aggregated columns:
    - year, dist, mun, conj, pn_con, clas_sub: Grouping columns.
    - FIRST(dat_con), FIRST(company_file), FIRST(brr): First occurrence of these columns.
    - SUM(ene_01) to SUM(ene_12): Sum of monthly energy consumption.
    - AVG(ene_01) to AVG(ene_12): Average of monthly energy consumption.
    - STDDEV(ene_01) to STDDEV(ene_12): Standard deviation of monthly energy consumption.

    Args:
        conn (DBConnection): The database connection object.
        path_ucbt (str): The name of the table to aggregate data from.
        path_ucbt_agg (str): The name of the materialized view to create.
    """
    query = f"""
            CREATE TABLE {path_ucbt_agg} AS
            SELECT
                year,
                dist,
                mun, 
                conj,
                pn_con,
                clas_sub,
                MIN(dat_con) as dat_con,
                MIN(company_file) as company_file,
                MIN(brr) as brr, 
                SUM(ene_01) as ene_01_sum,
                SUM(ene_02) as ene_02_sum,
                SUM(ene_03) as ene_03_sum,
                SUM(ene_04) as ene_04_sum,
                SUM(ene_05) as ene_05_sum,
                SUM(ene_06) as ene_06_sum,
                SUM(ene_07) as ene_07_sum,
                SUM(ene_08) as ene_08_sum,
                SUM(ene_09) as ene_09_sum,
                SUM(ene_10) as ene_10_sum,
                SUM(ene_11) as ene_11_sum,
                SUM(ene_12) as ene_12_sum,
                AVG(ene_01) as ene_01_mean,
                AVG(ene_02) as ene_02_mean,
                AVG(ene_03) as ene_03_mean,
                AVG(ene_04) as ene_04_mean,
                AVG(ene_05) as ene_05_mean,
                AVG(ene_06) as ene_06_mean,
                AVG(ene_07) as ene_07_mean,
                AVG(ene_08) as ene_08_mean,
                AVG(ene_09) as ene_09_mean,
                AVG(ene_10) as ene_10_mean,
                AVG(ene_11) as ene_11_mean,
                AVG(ene_12) as ene_12_mean
            FROM {path_ucbt}
            GROUP BY
                year,
                dist,
                mun,
                conj,
                clas_sub,
                pn_con
            """
    conn.execute_query(query)


def create_indexes(conn: DBConnection) -> None:
    """
    Creates indexes on the 'ucbt' table.

    Args:
        conn (DBConnection): The connection object to the database.
    """
    schema = ANEEL_BRONZE_CONTRACTS["ucbt"]["schema"]
    table_name = ANEEL_BRONZE_CONTRACTS["ucbt"]["tableName"]
    conn.create_index(
        schema, table_name, ["year", "dist", "mun", "conj", "clas_sub", "pn_con"]
    )
    conn.create_index(schema, table_name, ["year"])


def main() -> None:
    """
    This is the main function that executes the join_ucbt_and_ponnot operation.
    """
    path_ucbt = get_db_path(ANEEL_BRONZE_CONTRACTS["ucbt"])
    path_ucbt_agg = get_db_path(ANEEL_BRONZE_CONTRACTS["ucbt_agg"])
    conn = DBConnection("bronze")
    write_log("Creating indexes on the 'ucbt' table.")
    create_indexes(conn)
    write_log("Grouping 'ucbt' table.")
    group_ucbt(conn, path_ucbt, path_ucbt_agg)
    write_log("Creating pk on 'ucbt' table.")
    conn.close()
