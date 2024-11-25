"""
This script performs a join operation between the 'ucbt' and 'ponnot' tables 
to update the 'pn_con' column in the 'ucbt' table.
It retrieves batches of data from two paths and joins them based on specific conditions.
The resulting DataFrame is then saved in parquet format, with dropped columns, 
for each municipality.
Additionally, the 'pn_con' values that did not match between the 'ucbt' and 
'ponnot' tables are saved separately.

The script consists of the following functions:
- try_join: Executes a database query and appends the resulting DataFrame to the 
    list of good match DataFrames.
- save_no_join_ucbt_ponnot: Saves the 'pn_con' values that did not match between the 
    'ucbt' and 'ponnot' tables.
- save_partitioned_mun: Saves the concatenated DataFrame with dropped columns.
- save_mun: Saves the concatenated DataFrame with dropped columns, filtered by a 
    specific municipality.
- join_batches: Joins batches of data from two paths based on specific conditions.
- join_ucbt_and_ponnot: Joins the 'ucbt' and 'ponnot' tables to update the 'pn_con'
     column in the 'ucbt' table.
- main: The main function that executes the join_ucbt_and_ponnot operation.
"""

from typing import List
from functools import lru_cache
from tqdm import tqdm
import pandas as pd

from src.tools.databases.data_connection.connection import DBConnection

from src.tools.utils.common import get_db_path, check_file_exists_in_db

from src.tools.utils.execution_manager import ExecutionManager
from src.databases.bronze.aneel.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
manager.update_status("running_step_1")


ANEEL_BRONZE_CONTRACTS = execution_parameters["data_contracts"]["aneel_bronze"]


def create_primary_key(path_table: str, pk_key: str) -> None:
    """
    Creates a primary key on the ID column of ucbt table.
    """
    conn = DBConnection("bronze")
    schema, table_name = path_table.split(".")

    df = conn.query_database(f"SELECT * FROM {path_table} LIMIT 1")
    if pk_key not in df.columns:
        conn.create_pk(schema, table_name, pk_key)


def get_paths(year):
    """
    Generates and returns a dictionary of various database paths and table names
    based on the provided year.
    Args:
        year (str): The year to append to certain paths and table names.
    Returns:
        dict: A dictionary containing the following keys and their corresponding
            paths or table names:
            - "path_ucbt": Path to the UCBT aggregated database.
            - "path_ponnot": Path to the Ponnot database.
            - "path_join": Path to the joined database with the year appended.
            - "table_name_join": Table name for the joined database with the year appended.
            - "path_no_join": Path to the UCBT no join database with the year appended.
            - "table_name_no_join": Table name for the UCBT no join database with the year appended.
            - "schema": The schema extracted from the UCBT path.
            - "path_temp_ucbt": Temporary path for UCBT.
            - "path_temp_ponnot": Temporary path for Ponnot.
            - "path_first_join": Path for the first join operation.
            - "path_second_join": Path for the second join operation.
    """

    path_ucbt = get_db_path(ANEEL_BRONZE_CONTRACTS["ucbt_agg"])
    path_ponnot = get_db_path(ANEEL_BRONZE_CONTRACTS["ponnot_clean"])
    path_join = get_db_path(ANEEL_BRONZE_CONTRACTS["aneel_join"])
    path_join = "_".join([path_join, year])
    table_name_join = path_join.split(".", maxsplit=1)[1]
    path_no_join = get_db_path(ANEEL_BRONZE_CONTRACTS["ucbt_no_join"])
    path_no_join = "_".join([path_no_join, year])
    table_name_no_join = path_no_join.split(".", maxsplit=1)[1]
    schema = path_ucbt.split(".", maxsplit=1)[0]
    path_temp_ucbt = ".".join([schema, "z_temp_ucbt"])
    path_temp_ponnot = ".".join([schema, "z_temp_ponnot"])
    path_first_join = ".".join([schema, "z_first_join"])
    path_second_join = ".".join([schema, "z_second_join"])
    path_third_join = ".".join([schema, "z_third_join"])
    path_fourth_join = ".".join([schema, "z_fourth_join"])
    return {
        "path_ucbt": path_ucbt,
        "path_ponnot": path_ponnot,
        "path_join": path_join,
        "table_name_join": table_name_join,
        "path_no_join": path_no_join,
        "table_name_no_join": table_name_no_join,
        "schema": schema,
        "path_temp_ucbt": path_temp_ucbt,
        "path_temp_ponnot": path_temp_ponnot,
        "path_first_join": path_first_join,
        "path_second_join": path_second_join,
        "path_third_join": path_third_join,
        "path_fourth_join": path_fourth_join,
    }


def query_temp_ucbt(conn: DBConnection, mun_batch: str, year: str, pk_key: str) -> None:
    """
    Creates a temporary table with filtered data from the UCBT table based on the given
    municipality batch and year.
    Args:
        conn: Database connection object.
        mun_batch (str): A string containing a batch of municipalities to filter by.
        year (str): The year to filter the data by.
        pk_key (str): The primary key column to create in the temporary table.
    """
    path_ucbt = get_paths(year)["path_ucbt"]
    path_temp_ucbt = get_paths(year)["path_temp_ucbt"]
    sql_query = f"""
    -- Create filtered_ucbt table
        DROP TABLE IF EXISTS {path_temp_ucbt};
        CREATE TABLE {path_temp_ucbt} AS
        SELECT *
        FROM {path_ucbt}
        WHERE mun IN ({mun_batch}) AND year = '{year}';
    """
    conn.execute_query(sql_query)
    create_primary_key(path_temp_ucbt, pk_key)


def query_temp_ponnot(
    conn: DBConnection, mun_batch: str, year: str, pk_key: str
) -> None:
    """
    Creates a temporary table with filtered data from the 'ponnot' table based on the
    specified municipality batch and year.
    Args:
        conn: Database connection object used to execute the query.
        mun_batch (str): A string containing a batch of municipalities to filter the data.
        year (str): The year to filter the data.
        pk_key (str): The primary key column to create in the temporary table.
    """
    path_ponnot = get_paths(year)["path_ponnot"]
    path_temp_ponnot = get_paths(year)["path_temp_ponnot"]
    sql_query = f"""
    -- Create filtered_ponnot table
        DROP TABLE IF EXISTS {path_temp_ponnot};
        CREATE TABLE {path_temp_ponnot} AS
        SELECT *
        FROM {path_ponnot}
        WHERE mun IN ({mun_batch}) AND year = '{year}';
    """
    conn.execute_query(sql_query)
    create_primary_key(path_temp_ponnot, pk_key)


def delete_rows_in_table(
    conn: DBConnection, path_table: str, path_index_to_delete: str, pk_key: str
):
    """
    Deletes rows from two tables based on the IDs present in an index table.

    Args:
        conn: A database connection object that has an execute_query method.
        path_table (str): The table from which to delete rows.
        path_index_to_delete (str): The table containing the IDs to delete.
        pk_key (str): The primary key column to use for the deletion.
    """
    sql_query = f"""
        DELETE FROM {path_table}
        WHERE {pk_key} IN (SELECT {pk_key} FROM {path_index_to_delete});
        """
    conn.execute_query(sql_query)


def query_first_join(conn: DBConnection, year: str, pk_key: str) -> None:
    """
    Executes a SQL query to create a new table by joining two temporary tables based
    on specific conditions.
    Args:
        conn (DBConnection): The database connection object used to execute the query.
        year (str): The year used to retrieve the paths for the temporary tables and the
        resulting joined table.
        pk_key (str): The primary key column to create in the temporary table.

    The function performs the following steps:
        1. Retrieves the paths for the temporary tables and the resulting joined table
        based on the provided year.
        2. Constructs a SQL query to drop the existing joined table (if it exists) and
        create a new one by joining the two temporary tables.
        3. Executes the constructed SQL query using the provided database connection.
    The resulting joined table includes all columns from the first temporary table (ucbt) and
    selected columns (geometry, mat, are_loc, cod_id) from the second temporary table (ponnot),
    joined on matching values of pn_con, dist, conj, and mun.
    """
    path_first_join = get_paths(year)["path_first_join"]
    path_temp_ponnot = get_paths(year)["path_temp_ponnot"]
    path_temp_ucbt = get_paths(year)["path_temp_ucbt"]
    sql_query = f"""
    -- Create first_join table
        DROP TABLE IF EXISTS {path_first_join};
        CREATE TABLE {path_first_join} AS
        SELECT u.*, p.geometry, p.mat, p.are_loc, p.cod_id, p.{pk_key}
        FROM {path_temp_ucbt} u
        INNER JOIN {path_temp_ponnot} p
        ON u.pn_con = p.cod_id 
        AND u.dist = p.dist
        AND u.conj = p.conj
        AND u.mun = p.mun;
    """
    conn.execute_query(sql_query)
    delete_rows_in_table(
        conn, path_temp_ucbt, path_first_join, "_".join([pk_key, "ucbt"])
    )
    delete_rows_in_table(conn, path_temp_ponnot, path_first_join, pk_key)


def query_second_join(conn: DBConnection, year: str, pk_key: str) -> None:
    """
    Executes a SQL query to create a new table by performing a right join between two
    existing tables.
    Args:
        conn (DBConnection): The database connection object used to execute the query.
        year (str): The year used to determine the file paths for the tables involved in the join.
        pk_key (str): The primary key column to create in the temporary table.

    The function performs the following steps:
        1. Retrieves the file paths for the second join table, temporary UCBT table,
            and first join table based on the provided year.
        2. Constructs a SQL query to:
            a. Drop the existing second join table if it exists.
            b. Create a new second join table by performing a right join between the
                temporary UCBT table and the first join table.
            c. Select specific columns from the joined tables.
            d. Filter the results where the 'pn_con' column in the first join table is NULL.
        3. Executes the constructed SQL query using the provided database connection.
    Note:
        The function assumes that the `get_paths` function is defined elsewhere and
        returns a dictionary with the required file paths.
    """
    path_second_join = get_paths(year)["path_second_join"]
    path_temp_ucbt = get_paths(year)["path_temp_ucbt"]
    path_temp_ponnot = get_paths(year)["path_temp_ponnot"]
    sql_query = f"""
    -- Create second_join table
        DROP TABLE IF EXISTS {path_second_join};
        CREATE TABLE {path_second_join} AS
        SELECT u.*, p.geometry, p.mat, p.are_loc, p.cod_id, p.{pk_key}
        FROM {path_temp_ucbt} u
        INNER JOIN {path_temp_ponnot} p
        ON u.pn_con = p.cod_id
        AND u.dist = p.dist
        AND u.conj = p.conj;
    """
    conn.execute_query(sql_query)
    delete_rows_in_table(
        conn, path_temp_ucbt, path_second_join, "_".join([pk_key, "ucbt"])
    )
    delete_rows_in_table(conn, path_temp_ponnot, path_second_join, pk_key)


def query_third_join(conn: DBConnection, year: str, pk_key: str) -> None:
    """
    Executes a SQL query to create a new table by performing a right join between two
    existing tables.
    Args:
        conn (DBConnection): The database connection object used to execute the query.
        year (str): The year used to determine the file paths for the tables involved in the join.
        pk_key (str): The primary key column to create in the temporary table.

    The function performs the following steps:
        1. Retrieves the file paths for the second join table, temporary UCBT table,
            and first join table based on the provided year.
        2. Constructs a SQL query to:
            a. Drop the existing second join table if it exists.
            b. Create a new second join table by performing a right join between the
                temporary UCBT table and the first join table.
            c. Select specific columns from the joined tables.
            d. Filter the results where the 'pn_con' column in the first join table is NULL.
        3. Executes the constructed SQL query using the provided database connection.
    Note:
        The function assumes that the `get_paths` function is defined elsewhere and
        returns a dictionary with the required file paths.
    """
    path_third_join = get_paths(year)["path_third_join"]
    path_temp_ucbt = get_paths(year)["path_temp_ucbt"]
    path_temp_ponnot = get_paths(year)["path_temp_ponnot"]
    sql_query = f"""
    -- Create third_join table
        DROP TABLE IF EXISTS {path_third_join};
        CREATE TABLE {path_third_join} AS
        SELECT u.*, p.geometry, p.mat, p.are_loc, p.cod_id, p.{pk_key}
        FROM {path_temp_ucbt} u
        INNER JOIN {path_temp_ponnot} p
        ON u.pn_con = p.cod_id
        AND u.dist = p.dist
        AND u.mun = p.mun;
    """
    conn.execute_query(sql_query)
    delete_rows_in_table(
        conn, path_temp_ucbt, path_third_join, "_".join([pk_key, "ucbt"])
    )
    delete_rows_in_table(conn, path_temp_ponnot, path_third_join, pk_key)


def query_fourth_join(conn: DBConnection, year: str, pk_key: str) -> None:
    """
    Executes a SQL query to join two temporary tables and create a new table with the results.
    This function performs the following steps:
    1. Drops the existing table if it exists.
    2. Creates a new table by joining two temporary tables on specified columns.
    3. Deletes rows from the temporary tables that are present in the new table.
    Args:
        conn (DBConnection): The database connection object used to execute the query.
        year (str): The year used to determine the paths for the temporary tables and the new table.
        pk_key (str): The primary key column to create in the temporary table.

    """
    path_temp_ucbt = get_paths(year)["path_temp_ucbt"]
    path_temp_ponnot = get_paths(year)["path_temp_ponnot"]
    path_fourth_join = get_paths(year)["path_fourth_join"]
    sql_query = f"""
    -- Create third_join table
        DROP TABLE IF EXISTS {path_fourth_join};
        CREATE TABLE {path_fourth_join} AS
        SELECT u.*, p.geometry, p.mat, p.are_loc, p.cod_id, p.{pk_key}
        FROM {path_temp_ucbt} u
        RIGHT JOIN {path_temp_ponnot} p
        ON u.pn_con = p.cod_id 
        AND u.dist = p.dist
    """
    conn.execute_query(sql_query)
    delete_rows_in_table(
        conn, path_temp_ucbt, path_fourth_join, "_".join([pk_key, "ucbt"])
    )
    delete_rows_in_table(conn, path_temp_ponnot, path_fourth_join, pk_key)


def create_indexes(conn: DBConnection, year: str) -> None:
    """
    Creates indexes on temporary and join tables for the specified year.
    This function generates and executes SQL queries to create indexes on the
    specified tables to optimize query performance. The indexes are created
    on the columns 'mun', 'year', and 'pn_con' for the respective tables.
    Args:
        conn (DBConnection): The database connection object used to execute the queries.
        year (str): The year for which the indexes are to be created.
    """
    path_temp_ucbt = get_paths(year)["path_temp_ucbt"]
    path_temp_ponnot = get_paths(year)["path_temp_ponnot"]
    query_create_indexes = f"""
        CREATE INDEX idx_filtered_ucbt_mun_year ON {path_temp_ucbt} (mun, year);
        CREATE INDEX idx_filtered_ponnot_mun_year ON {path_temp_ponnot} (mun, year);
        CREATE INDEX idx_filtered_ucbt_pncon_dist ON {path_temp_ucbt} (pn_con, dist);
        CREATE INDEX idx_filtered_ponnot_codid_dist ON {path_temp_ponnot} (cod_id, dist);
    """
    conn.execute_query(query_create_indexes)


def create_final_join(conn: DBConnection, year: str) -> None:
    """
    Creates a final join of tables and inserts the result into the database.
    This function constructs and executes a SQL query to join two tables based on
    specific conditions and either appends the result to an existing table or
    creates a new table if it does not exist. The paths and table names are
    dynamically determined based on the provided year.
    Args:
        conn (DBConnection): The database connection object used to execute the query.
        year (str): The year used to determine the paths and table names for the join.
    """
    path_join = get_paths(year)["path_join"]
    table_name_join = get_paths(year)["table_name_join"]
    schema = get_paths(year)["schema"]

    path_second_join = get_paths(year)["path_second_join"]
    path_first_join = get_paths(year)["path_first_join"]
    table_name_fj = path_first_join.split(".", maxsplit=1)[1]
    path_third_join = get_paths(year)["path_third_join"]
    path_fourth_join = get_paths(year)["path_fourth_join"]
    path_temp_ponnot = get_paths(year)["path_temp_ponnot"]
    table_name_temp_ponnot = path_temp_ponnot.split(".", maxsplit=1)[1]
    query_rearrange_ponnot_cols = f"""
    DO $$
    DECLARE
        col_name TEXT;
        col_type TEXT;
    BEGIN
        FOR col_name, col_type IN
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{table_name_fj}'
            AND column_name NOT IN (
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                    AND table_name = '{table_name_temp_ponnot}'
            )
        LOOP
            EXECUTE format(
                'ALTER TABLE {path_temp_ponnot} ADD COLUMN %I %s DEFAULT NULL;',
                col_name, col_type
            );
        END LOOP;

            -- Remove columns from path_temp_ponnot that are not in path_first_join
        FOR col_name IN
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{table_name_temp_ponnot}'
            AND column_name NOT IN (
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name_fj}'
            )
        LOOP
            EXECUTE format(
                'ALTER TABLE %I.%I DROP COLUMN %I;',
                '{schema}', '{table_name_temp_ponnot}', col_name
            );
        END LOOP;
    END $$; 
    """
    conn.execute_query(query_rearrange_ponnot_cols)

    query_join = f"""
    SELECT *
    FROM {path_first_join} f
    UNION ALL
    SELECT *
    FROM {path_second_join} s
    UNION ALL
    SELECT *
    FROM {path_third_join} t
    UNION ALL
    SELECT *
    FROM {path_fourth_join} fo
    UNION ALL
    SELECT year, dist, mun, conj, pn_con, clas_sub, dat_con,
       company_file, brr, ene_01_sum, ene_02_sum, ene_03_sum,
       ene_04_sum, ene_05_sum, ene_06_sum, ene_07_sum, ene_08_sum,
       ene_09_sum, ene_10_sum, ene_11_sum, ene_12_sum, ene_01_mean,
       ene_02_mean, ene_03_mean, ene_04_mean, ene_05_mean,
       ene_06_mean, ene_07_mean, ene_08_mean, ene_09_mean,
       ene_10_mean, ene_11_mean, ene_12_mean, ene_01_std, ene_02_std,
       ene_03_std, ene_04_std, ene_05_std, ene_06_std, ene_07_std,
       ene_08_std, ene_09_std, ene_10_std, ene_11_std, ene_12_std,
       row_id_ucbt, geometry, mat, are_loc, cod_id, row_id
    FROM {path_temp_ponnot} p
    """

    query_final_join = f"""
    -- Check if the table exists and append or create
        DO $$
        BEGIN
            -- If the table exists, insert new rows
            IF EXISTS (SELECT 1 FROM information_schema.tables 
                    WHERE table_name = '{table_name_join}' AND table_schema = '{schema}') THEN
                INSERT INTO {path_join} ({query_join});
            ELSE
                -- If the table does not exist, create it and insert rows
               CREATE TABLE {path_join} AS {query_join};
            END IF;
        END $$;
    """
    conn.execute_query(query_final_join)


def create_no_join(conn: DBConnection, year: str) -> None:
    """
    Creates or inserts data into a table that contains records from a temporary table
    that do not have matching entries in a second table.
    Args:
        conn (DBConnection): The database connection object used to execute queries.
        year (str): The year used to retrieve paths and table names from the configuration.
    """
    path_no_join = get_paths(year)["path_no_join"]
    table_name_no_join = get_paths(year)["table_name_no_join"]
    schema = get_paths(year)["schema"]
    path_temp_ucbt = get_paths(year)["path_temp_ucbt"]

    query_no_join = f"""
    SELECT u.*
    FROM {path_temp_ucbt} u
    """

    query_no_join_final = f"""
    DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM information_schema.tables 
                    WHERE table_name = '{table_name_no_join}' AND table_schema = '{schema}') THEN
                INSERT INTO {path_no_join} ({query_no_join});
            ELSE
                CREATE TABLE {path_no_join} AS
                {query_no_join};
            END IF;
        END $$;
    """
    conn.execute_query(query_no_join_final)


def drop_temp_tables(conn: DBConnection, year: str) -> None:
    """
    Drops temporary tables used in the data processing pipeline for a given year.
    Args:
        conn (DBConnection): The database connection object used to execute queries.
        year (str): The year for which the temporary tables should be dropped.
    """

    path_temp_ucbt = get_paths(year)["path_temp_ucbt"]
    path_temp_ponnot = get_paths(year)["path_temp_ponnot"]
    path_first_join = get_paths(year)["path_first_join"]
    path_second_join = get_paths(year)["path_second_join"]
    path_third_join = get_paths(year)["path_third_join"]
    path_fourth_join = get_paths(year)["path_fourth_join"]
    query_drop_tables = f"""
        DROP TABLE IF EXISTS {path_temp_ucbt};
        DROP TABLE IF EXISTS {path_temp_ponnot};
        DROP TABLE IF EXISTS {path_first_join};
        DROP TABLE IF EXISTS {path_second_join};
        DROP TABLE IF EXISTS {path_third_join};
        DROP TABLE IF EXISTS {path_fourth_join};
    """
    conn.execute_query(query_drop_tables)


def join_batches(conn: DBConnection, mun_batch: str, year: str) -> None:
    """
    Joins batches of data from two paths based on specific conditions.

    Args:
        conn (DBConnection): The database connection object.
        mun_batch (str): A string containing a batch of municipalities to filter by.
        year (int): The year to include in the join.
    """
    pk_key = "row_id"
    query_temp_ucbt(conn, mun_batch, year, "_".join([pk_key, "ucbt"]))
    query_temp_ponnot(conn, mun_batch, year, pk_key)
    query_first_join(conn, year, pk_key)
    query_second_join(conn, year, pk_key)
    query_third_join(conn, year, pk_key)
    query_fourth_join(conn, year, pk_key)
    create_indexes(conn, year)
    create_final_join(conn, year)
    create_no_join(conn, year)
    drop_temp_tables(conn, year)


@lru_cache(maxsize=1)
def get_mun_batches() -> List[str]:
    """
    Retrieves batches of municipalities (mun) from the database,
    excluding a predefined list of large municipalities.
    Returns:
        List[str]: A list of DataFrames, each containing a batch of municipalities.
        Large municipalities are returned as individual DataFrames.
    """
    conn = DBConnection("bronze")
    muns = conn.query_database(
        "select distinct (cd_mun) as mun from layers.mun_censo_2022 ORDER BY mun ASC"
    )

    large_mun_cods = [
        "3550308",
        "3304557",
        "3106200",
        "5300108",
        "2304400",
        "2927408",
        "1302603",
        "4106902",
        "2611606",
        "5208707",
        "4314902",
        "3518800",
        "3509502",
        "2111300",
    ]
    batch_size = 150
    muns = muns[~muns["mun"].isin(large_mun_cods)]
    mun_batches = [muns[i : i + batch_size] for i in range(0, len(muns), batch_size)]
    mun_batches.extend([pd.DataFrame({"mun": [mun]}) for mun in large_mun_cods])
    return mun_batches


def main() -> None:
    """
    This is the main function that executes the join_ucbt_and_ponnot operation.
    """
    year_init, year_end = ANEEL_BRONZE_CONTRACTS["ucbt_agg"]["queryYears"]
    years = [str(year) for year in range(year_init, year_end + 1)]
    conn = DBConnection("bronze")
    path_join = get_db_path(ANEEL_BRONZE_CONTRACTS["aneel_join"])
    mun_batches = get_mun_batches()
    for year in tqdm(years, desc="Processing years"):
        for mun_batch in tqdm(mun_batches, desc="Processing mun batches"):
            if all(
                check_file_exists_in_db(
                    conn,
                    path_join,
                    condition=f"mun = {mun} AND year = {year} LIMIT 1",
                )
                for mun in mun_batch["mun"].to_list()
            ):
                continue
            mun_batch = ",".join([f"'{mun}'" for mun in mun_batch["mun"].to_list()])
            join_batches(conn, mun_batch, year)
    manager.update_status("finished_step_1")
    manager.update_last_run()
