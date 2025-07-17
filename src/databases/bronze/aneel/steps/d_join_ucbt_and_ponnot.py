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
- join_ucbt_ponnot_tables: Joins batches of data from two paths based on specific conditions.
- join_ucbt_and_ponnot: Joins the 'ucbt' and 'ponnot' tables to update the 'pn_con'
     column in the 'ucbt' table.
- main: The main function that executes the join_ucbt_and_ponnot operation.
"""

from typing import List
from tqdm import tqdm
import pandas as pd

from src.tools.managers.db_connector import DBConnection

from src.tools.utils.common import write_log, check_file_exists_in_db, get_db_path
from src.databases.bronze.aneel.common import split_file_sizes
from src.databases.bronze.aneel.config import (
    manager,
    CONTRACT_BRONZE_ENERGY,
    YEARS,
    PATHS_MV,
)


def create_primary_key(conn: DBConnection, path_table: str, pk_key: str) -> None:
    """
    Creates a primary key on the ID column of ucbt table.
    """
    write_log(f"Creating primary key for {path_table}")
    schema, table_name = path_table.split(".")
    df = conn.query_database(f"SELECT * FROM {path_table} LIMIT 1")
    if pk_key not in df.columns:
        conn.create_pk(schema, table_name, pk_key)


def get_paths(year: str, table_name: str):
    """
    Generates and returns a dictionary of various database paths and table names
    based on the provided year.
    Args:
        year (str): The year to append to certain paths and table names.
        table_name (str): The name of the table to retrieve the paths for.
    Returns:
        dict: A dictionary containing the following keys and their corresponding
            paths of the request table name from:
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

    path_ucbt = get_db_path(CONTRACT_BRONZE_ENERGY[f"ucbt_{year}"])
    path_ponnot = get_db_path(CONTRACT_BRONZE_ENERGY[f"ponnot_{year}"])
    path_join = get_db_path(CONTRACT_BRONZE_ENERGY[f"aneel_join_{year}"])
    table_name_join = path_join.split(".", maxsplit=1)[1]
    path_no_join = get_db_path(CONTRACT_BRONZE_ENERGY[f"ucbt_no_join_{year}"])
    table_name_no_join = path_no_join.split(".", maxsplit=1)[1]
    schema = path_ucbt.split(".", maxsplit=1)[0]
    path_temp_ucbt = ".".join([schema, "z_temp_ucbt"])
    path_temp_ponnot = ".".join([schema, "z_temp_ponnot"])
    path_first_join = ".".join([schema, "z_first_join"])
    path_second_join = ".".join([schema, "z_second_join"])
    path_third_join = ".".join([schema, "z_third_join"])
    path_fourth_join = ".".join([schema, "z_fourth_join"])
    dict_all_paths = {
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
    return dict_all_paths[table_name]


def create_temp_table(
    conn: DBConnection,
    company_file: str,
    path_original_table: str,
    path_temp_table: str,
) -> None:
    """
    Creates a temporary table by filtering rows from the original table based on the company_file.
    Args:
        conn (DBConnection): The database connection object.
        company_file (str): The ID of the company to filter the rows.
        path_original_table (str): The path to the original table.
        path_temp_table (str): The path where the temporary table will be created.
    """
    sql_query = f"""
    -- Create filtered_ucbt table
        DROP TABLE IF EXISTS {path_temp_table};
        CREATE TABLE {path_temp_table} AS
        SELECT *
        FROM {path_original_table}
        WHERE company_file = '{company_file}'
    """
    conn.execute_query(sql_query)


def create_temp_ucbt(conn: DBConnection, company_file: str, year: str) -> None:
    """
    Creates a temporary table with filtered data from the UCBT table based on the given
    municipality batch and year.
    Args:
        conn: Database connection object.
        company_file (str): The company_file name to filter the data by.
        year (str): The year to filter the data by.
    """
    write_log(f"Creating temp UCBT table for {company_file}")
    path_ucbt = get_paths(year, "path_ucbt")
    path_temp_ucbt = get_paths(year, "path_temp_ucbt")
    create_temp_table(conn, company_file, path_ucbt, path_temp_ucbt)


def create_temp_ponnot(conn: DBConnection, company_file: str, year: str) -> None:
    """
    Creates a temporary table with filtered data from the 'ponnot' table based on the
    specified municipality batch and year.
    Args:
        conn: Database connection object used to execute the query.
        company_file (str): A string containing the company_file name.
        year (str): The year to filter the data.
    """
    write_log(f"Creating temp Ponnot table for {company_file}")
    path_ponnot = get_paths(year, "path_ponnot")
    path_temp_ponnot = get_paths(year, "path_temp_ponnot")
    create_temp_table(conn, company_file, path_ponnot, path_temp_ponnot)


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
    write_log("Creating first join table")
    path_first_join = get_paths(year, "path_first_join")
    path_temp_ponnot = get_paths(year, "path_temp_ponnot")
    path_temp_ucbt = get_paths(year, "path_temp_ucbt")
    sql_query = f"""
    -- Create first_join table
        DROP TABLE IF EXISTS {path_first_join};
        CREATE TABLE {path_first_join} AS
        SELECT u.*, p.geometry, p.mat, p.are_loc, p.cod_id, p.{pk_key}, 1 AS join_type, p.company_file as company_file_ponnot
        FROM {path_temp_ucbt} u
        INNER JOIN {path_temp_ponnot} p
        ON u.pn_con = p.cod_id 
        AND u.dist::text = p.dist
        AND u.conj = p.conj
        AND u.mun = p.mun;
    """
    conn.execute_query(sql_query)
    delete_rows_in_table(
        conn, path_temp_ucbt, path_first_join, "_".join([pk_key, "ucbt"])
    )
    delete_rows_in_table(conn, path_temp_ponnot, path_first_join, pk_key)
    create_primary_key(conn, path_first_join, pk_key)


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
    write_log("Creating second join table")
    path_second_join = get_paths(year, "path_second_join")
    path_temp_ucbt = get_paths(year, "path_temp_ucbt")
    path_temp_ponnot = get_paths(year, "path_temp_ponnot")
    sql_query = f"""
    -- Create second_join table
        DROP TABLE IF EXISTS {path_second_join};
        CREATE TABLE {path_second_join} AS
        SELECT u.*, p.geometry, p.mat, p.are_loc, p.cod_id, p.{pk_key}, 2 AS join_type, p.company_file as company_file_ponnot
        FROM {path_temp_ucbt} u
        INNER JOIN {path_temp_ponnot} p
        ON u.pn_con = p.cod_id
        AND u.dist::text = p.dist
        AND u.conj = p.conj;
    """
    conn.execute_query(sql_query)
    delete_rows_in_table(
        conn, path_temp_ucbt, path_second_join, "_".join([pk_key, "ucbt"])
    )
    delete_rows_in_table(conn, path_temp_ponnot, path_second_join, pk_key)
    create_primary_key(conn, path_second_join, pk_key)


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
    write_log("Creating third join table")
    path_third_join = get_paths(year, "path_third_join")
    path_temp_ucbt = get_paths(year, "path_temp_ucbt")
    path_temp_ponnot = get_paths(year, "path_temp_ponnot")
    sql_query = f"""
    -- Create third_join table
        DROP TABLE IF EXISTS {path_third_join};
        CREATE TABLE {path_third_join} AS
        SELECT u.*, p.geometry, p.mat, p.are_loc, p.cod_id, p.{pk_key}, 3 AS join_type, p.company_file as company_file_ponnot
        FROM {path_temp_ucbt} u
        INNER JOIN {path_temp_ponnot} p
        ON u.pn_con = p.cod_id
        AND u.dist::text = p.dist
        AND u.mun = p.mun;
    """
    conn.execute_query(sql_query)
    delete_rows_in_table(
        conn, path_temp_ucbt, path_third_join, "_".join([pk_key, "ucbt"])
    )
    delete_rows_in_table(conn, path_temp_ponnot, path_third_join, pk_key)
    create_primary_key(conn, path_third_join, pk_key)


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
    write_log("Creating fourth join table")
    path_temp_ucbt = get_paths(year, "path_temp_ucbt")
    path_temp_ponnot = get_paths(year, "path_temp_ponnot")
    path_fourth_join = get_paths(year, "path_fourth_join")
    sql_query = f"""
    -- Create third_join table
        DROP TABLE IF EXISTS {path_fourth_join};
        CREATE TABLE {path_fourth_join} AS
        SELECT u.*, p.geometry, p.mat, p.are_loc, p.cod_id, p.{pk_key}, 4 AS join_type, p.company_file as company_file_ponnot
        FROM {path_temp_ucbt} u
        RIGHT JOIN {path_temp_ponnot} p
        ON u.pn_con = p.cod_id 
        AND u.dist::text = p.dist
    """
    conn.execute_query(sql_query)
    delete_rows_in_table(
        conn, path_temp_ucbt, path_fourth_join, "_".join([pk_key, "ucbt"])
    )
    delete_rows_in_table(conn, path_temp_ponnot, path_fourth_join, pk_key)
    create_primary_key(conn, path_fourth_join, pk_key)


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
    write_log("Creating indexes")
    path_temp_ucbt = get_paths(year, "path_temp_ucbt")
    path_temp_ponnot = get_paths(year, "path_temp_ponnot")
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
    write_log("Creating final join")
    path_join = get_paths(year, "path_join")
    table_name_join = get_paths(year, "table_name_join")
    schema = get_paths(year, "schema")

    path_second_join = get_paths(year, "path_second_join")
    path_first_join = get_paths(year, "path_first_join")
    table_name_fj = path_first_join.split(".", maxsplit=1)[1]
    path_third_join = get_paths(year, "path_third_join")
    path_fourth_join = get_paths(year, "path_fourth_join")
    path_temp_ponnot = get_paths(year, "path_temp_ponnot")
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

    query_join_table = f"""
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
    """

    query_join = f"""
    -- Check if the table exists and append or create
        DO $$
        BEGIN
            -- If the table exists, insert new rows
            IF EXISTS (SELECT 1 FROM information_schema.tables 
                    WHERE table_name = '{table_name_join}' AND table_schema = '{schema}') THEN
                INSERT INTO {path_join} ({query_join_table});
            ELSE
                -- If the table does not exist, create it and insert rows
               CREATE TABLE {path_join} AS {query_join_table};
            END IF;
        END $$;
    """
    conn.execute_query(query_join)
    temp_ponnot_lenth = conn.query_database(
        f"SELECT COUNT(*) FROM {path_temp_ponnot}"
    ).squeeze()
    if temp_ponnot_lenth > 0:
        query_append_ponnot = f"SELECT * FROM {path_temp_ponnot}"
        final_query = f"""
            INSERT INTO {path_join} ({query_append_ponnot})"""
        conn.execute_query(final_query)


def create_no_join(conn: DBConnection, year: str) -> None:
    """
    Creates or inserts data into a table that contains records from a temporary table
    that do not have matching entries in a second table.
    Args:
        conn (DBConnection): The database connection object used to execute queries.
        year (str): The year used to retrieve paths and table names from the configuration.
    """
    write_log("Creating no join table")
    path_no_join = get_paths(year, "path_no_join")
    table_name_no_join = get_paths(year, "table_name_no_join")
    schema = get_paths(year, "schema")
    path_temp_ucbt = get_paths(year, "path_temp_ucbt")

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
    write_log("Dropping temporary tables")
    path_temp_ucbt = get_paths(year, "path_temp_ucbt")
    path_temp_ponnot = get_paths(year, "path_temp_ponnot")
    path_first_join = get_paths(year, "path_first_join")
    path_second_join = get_paths(year, "path_second_join")
    path_third_join = get_paths(year, "path_third_join")
    path_fourth_join = get_paths(year, "path_fourth_join")
    query_drop_tables = f"""
        DROP TABLE IF EXISTS {path_temp_ucbt};
        DROP TABLE IF EXISTS {path_temp_ponnot};
        DROP TABLE IF EXISTS {path_first_join};
        DROP TABLE IF EXISTS {path_second_join};
        DROP TABLE IF EXISTS {path_third_join};
        DROP TABLE IF EXISTS {path_fourth_join};
    """
    conn.execute_query(query_drop_tables)


def join_ucbt_ponnot_tables(
    conn: DBConnection, company_file: str, year: str, pk_key: str
) -> None:
    """
    Joins batches of data from two paths based on specific conditions.

    Args:
        conn (DBConnection): The database connection object.
        company_file (str): A string containing a batch of municipalities to filter by.
        year (int): The year to include in the join.
        pk_key (str): The primary key column to create in the temporary table.
    """

    create_temp_ucbt(conn, company_file, year)
    create_temp_ponnot(conn, company_file, year)
    query_first_join(conn, year, pk_key)
    query_second_join(conn, year, pk_key)
    query_third_join(conn, year, pk_key)
    query_fourth_join(conn, year, pk_key)
    create_indexes(conn, year)
    create_final_join(conn, year)
    create_no_join(conn, year)
    drop_temp_tables(conn, year)


def get_data_already_processed(
    year: int,
    refresh_view: bool = False,
) -> pd.DataFrame:
    """
    Retrieves the processed data from Aneel join.
    Args:
        year (int): The year for which to retrieve the processed data.
        refresh_view (bool): A flag indicating whether to refresh the view.

    Returns:
        pd.DataFrame: A DataFrame containing the processed data.
    """
    write_log("Getting processed data from MLflow")
    conn = DBConnection("bronze")
    df = pd.DataFrame()

    path_join = get_db_path(CONTRACT_BRONZE_ENERGY[f"aneel_join_{year}"])
    data_exists = check_file_exists_in_db(conn, path_join)
    if data_exists:
        path_mv = PATHS_MV["common"].format(path=path_join)
        df = conn.query_database(f"SELECT * FROM {path_mv}")
        if df.empty:
            query = f"""
            CREATE MATERIALIZED VIEW {path_mv} AS
            SELECT DISTINCT(company_file) as company_id
            FROM {path_join}
            """
            conn.execute_query(query)
            df = conn.query_database(f"SELECT * FROM {path_mv}")
        if refresh_view:
            conn.execute_query(f"REFRESH MATERIALIZED VIEW {path_mv}")
            df = conn.query_database(f"SELECT * FROM {path_mv}")
        conn.close()
    return df


def get_all_companies() -> List[str]:
    """
    Retrieves a list of all unique company names from a collection of files.

    This function splits the files into large and small categories, combines them,
    and extracts the unique company names from the combined list.

    Returns:
        List[str]: A list of unique company names.
    """
    large_files, small_files = split_file_sizes()
    company_files = large_files + small_files
    company_files = list(set(company_file for company_file, _ in company_files))
    company_files.sort(key=lambda x: int(x.split(" - ")[1].split("-")[0]))
    return company_files


def get_company_files_to_process(year: int, refresh_view: bool = False) -> List[str]:
    """
    Retrieves a list of company files that need to be processed.

    This function compares the list of all company files with the list of
    already processed company files and returns the difference.
    Args:
        year (int): The year for which to retrieve the company files.
        refresh_view (bool): A flag indicating whether to refresh the view.

    Returns:
        List[str]: A list of company file identifiers that have not yet been processed.
    """
    df_processed = get_data_already_processed(year, refresh_view)
    all_company_files = get_all_companies()
    all_company_files = [
        company_file
        for company_file in all_company_files
        if company_file.split(" - ")[1].split("-")[0] == str(year)
    ]
    if df_processed.empty:
        return all_company_files
    files_to_process = list(
        set(all_company_files).difference(set(df_processed["company_id"]))
    )
    return files_to_process


def main() -> None:
    """
    This is the main function that executes the join_ucbt_and_ponnot operation.
    """
    for year in YEARS:
        conn = DBConnection("bronze")
        path_ucbt = get_paths(year, "path_ucbt")
        path_ponnot = get_paths(year, "path_ponnot")
        pk_key = "row_id"
        create_primary_key(conn, path_ucbt, f"{pk_key}_ucbt")
        create_primary_key(conn, path_ponnot, pk_key)
        company_files = get_company_files_to_process(year, refresh_view=True)
        for company_file in tqdm(company_files, desc="Processing companies"):
            join_ucbt_ponnot_tables(conn, company_file, year, pk_key)
        manager.update_last_run()
        conn.close()
