"""
This module contains functions to create and update the 'aneel_silver' database.
"""

from typing import List, Tuple
from functools import lru_cache
from tqdm import tqdm
import pandas as pd
from src.tools.databases.data_connection.connection import DBConnection
from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from src.tools.data_contract.validation_data_contract import (
    get_validation_contracts,
    get_validation_partitions,
)
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.common import check_file_exists, write_log

ANEEL_BRONZE_CONTRACTS = get_aneel_contracts("bronze")
ANEEL_SILVER_CONTRACTS = get_aneel_contracts("silver")
VALIDATION_PARTITIONS = get_validation_partitions()
VALIDATION_CONTRACT_EMPTY_STR_UCBT_PONNOT = get_validation_contracts("bronze", 0)
VALIDATION_CONTRACT_NO_JOIN_UCBT_PONNOT = get_validation_contracts("silver", 0)


@lru_cache(maxsize=10)
def get_cols_in_db(table_name) -> List[str]:
    """
    Retrieves the columns of the 'infrastructure.ucbt' table from the 'bronze' database.

    Returns:
    list: A list of column names.
    """
    schema = ANEEL_BRONZE_CONTRACTS[table_name]["schema"]
    table = ANEEL_BRONZE_CONTRACTS[table_name]["tableName"]
    conn = DBConnection("bronze")
    return conn.query_database(f"SELECT * FROM {schema}.{table} LIMIT 1").columns


def update_ponnot_id_in_ucbt_table():
    """
    Retrieves data from the bronze database and performs some transformations.
    Updates the 'pn_con' column in the 'ucbt' table by joining it with the 'ramlig' table.
    """
    conn = DBConnection("bronze")
    df = conn.query_database(
        """ 
        SELECT * 
        FROM infrastructure.ucbt u 
        LEFT JOIN infrastructure.ramlig r 
        ON u.ramal = r.cod_id 
        WHERE u.pn_con = ' ' 
        AND r.pn_con_1 != ' ' 
        AND u.dist = r.dist
        AND u.conj = r.conj
        """
    )
    saved_columns_ramlig = get_cols_in_db("ramlig")
    df["pn_con"] = df["pn_con_1"]  # update pn_con column with ramlig pn_con value
    df = df.iloc[:, : -len(saved_columns_ramlig)]
    match_cols = [col for col in df.columns if col != "pn_con"]
    conn.update_table(df, match_cols, ("infrastructure", "ucbt"))


def create_primary_key(
    conn: DBConnection, schema: str, table_name: str, column_name: str
):
    """
    Creates a primary key on a specified column in a table.

    Args:
        conn (DBConnection): The connection object to the database.
        schema (str): The name of the schema where the table is located.
        table_name (str): The name of the table.
        column_name (str): The name of the column to create the primary key on.
    """
    df = conn.query_database(f"SELECT * FROM {schema}.{table_name} LIMIT 1")
    if column_name not in df.columns:
        conn.create_pk(schema, table_name, column_name)


def test_integrity_of_join(joined_cols, df_keys, df_values):
    """
    Test the integrity of the join operation by comparing the unique values of
    specified columns in two dataframes.

    Parameters:
    joined_cols (list): A list of column names to be compared.
    df_keys (DataFrame): The first dataframe to be compared.
    df_values (DataFrame): The second dataframe to be compared.

    Raises:
    AssertionError: If the unique values of any of the specified columns in
                    df_keys and df_values are not equal.

    """
    for col in joined_cols:
        assert df_keys[col].unique() == df_values[col].unique()


def test_integrity_energy_addition(df_values, df_keys, df_result):
    """
    Test the integrity of energy addition in the given dataframes.

    Parameters:
    - df_values (DataFrame): The dataframe containing the values.
    - df_keys (DataFrame): The dataframe containing the keys.
    - df_result (DataFrame): The dataframe containing the result.

    Raises:
    - AssertionError: If the sum of the result column minus the sum of the values column
                      and the corresponding key value is greater than or equal to 1.
    """
    for col in df_values.filter(regex="ene_|fic_|dic_").columns:
        assert (
            df_result[col].sum() - (df_values[col].sum() + df_keys.loc[col].squeeze())
            < 1
        )


def distribute_energy(df_values, df_keys):
    """
    Distributes energy values based on the given keys.

    Args:
        df_values (DataFrame): The DataFrame containing energy values.
        df_keys (DataFrame): The DataFrame containing keys.

    Returns:
        DataFrame: The resulting DataFrame after distributing energy values.

    """
    df_pct = df_values.filter(regex="ene_|fic_|dic_") / df_values.filter(
        regex="ene_|fic_|dic_"
    ).sum().fillna(0)
    df_result = df_pct.mul(df_keys[0]).add(
        df_values.filter(regex="ene_|fic_|dic_"), fill_value=0
    )
    return df_result


def query_keys(conn: DBConnection, pk_key: str, keys: set) -> pd.DataFrame:
    """
    Queries the database for records with specified primary key values.

    Args:
        conn (DBConnection): The database connection object.
        pk_key (str): The name of the primary key column.
        keys (set): A set of primary key values to query.

    Returns:
        pd.DataFrame: A dataframe of rows matching the specific primary key values.
    """
    return conn.query_database(
        f""" 
            SELECT * 
            FROM infrastructure.ucbt u
            WHERE u.{pk_key} in ({",".join(list(keys))})
            """
    )


def query_values(conn: DBConnection, pk_key: str, values: set) -> pd.DataFrame:
    """
    Executes a database query to retrieve rows from the 'infrastructure.ucbt' table
    based on the provided primary key values.

    Args:
        conn (DBConnection): The database connection object.
        pk_key (str): The name of the primary key column.
        values (set): A set of primary key values to filter the query.

    Returns:
        pd.DataFrame: A dataframe of rows matching the provided primary key values.
    """
    return conn.query_database(
        f""" 
                SELECT * 
                FROM infrastructure.ucbt u
                WHERE u.{pk_key} in ({",".join(list(values))})
                """
    )


@save_parquet_decorator("bronze", VALIDATION_CONTRACT_EMPTY_STR_UCBT_PONNOT)
def query_empty_ucbt_pncon(
    conn: DBConnection, pk_key: str, joined_cols: list, schema: str, table_name: str
):
    """
    Query the database to retrieve empty ucbt_ponnot records.

    This function executes a SQL query to retrieve empty ucbt_ponnot records from the database.
    It joins the specified columns from the given schema and table name with the ucbt_ponnot table.
    The primary key column name is used for grouping the results.

    Args:
        conn (DBConnection): The database connection object.
        pk_key (str): The primary key column name.
        joined_cols (list): The list of column names used for joining.
        schema (str): The name of the database schema.
        table_name (str): The name of the table.

    Returns:
        ResultSet: The result set containing the group keys and corresponding values.

    """
    cols_text = " AND ".join([f"u.{col} = u2.{col}" for col in joined_cols])

    return conn.query_database(
        f"""
        SELECT u.group_keys AS chave,
            STRING_AGG(u2.{pk_key}::text, ',') AS valores
        FROM (
            SELECT 
                {", ".join(joined_cols)},
                STRING_AGG({pk_key}::text, ',') as group_keys
            FROM {schema}.{table_name}
            WHERE pn_con = ' '
            GROUP BY 
                {", ".join(joined_cols)}
        ) AS u
        LEFT JOIN {schema}.{table_name} u2 
            ON {cols_text}
        GROUP BY u.group_keys

    """
    )


def replace_energy_from_empty_ucbt_to_nearest_pncons(
    conn: DBConnection, df_empty_ucbt: pd.DataFrame, pk_key: str, joined_cols: list
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Replaces energy from empty UCBT (Unidade Consumidora com Baixa Tensão) to
    the nearests PONNOTs (Pontos Notaveis).

    Args:
        conn (DBConnection): The database connection object.
        df_empty_ucbt (pd.DataFrame): The DataFrame containing empty UCBT data.
        pk_key (str): The primary key column name.
        joined_cols (list): The list of columns used for joining the data.

    Returns:
        Tuple[pd.DataFrame, List[str]]: A tuple containing the DataFrame with overwritten
                                        energy values and a list of excluded keys.
    """

    dfs_overwrite = []
    exclude_keys = []
    for row in tqdm(df_empty_ucbt.itertuples()):
        keys = set(row[1].split(", "))
        values = set(row[2].split(", ")).difference(keys)
        if values == set():
            continue
        df_keys = query_keys(conn, pk_key, keys)
        df_values = query_values(conn, pk_key, values)
        test_integrity_of_join(joined_cols, df_keys, df_values)
        df_keys = df_keys.filter(regex="ene_|dic_|fic_").sum().to_frame()
        df_result = distribute_energy(df_values, df_keys)
        test_integrity_energy_addition(df_values, df_keys, df_result)
        df_values[df_result.columns] = df_result
        dfs_overwrite.append(df_values)
        exclude_keys.append(list(keys))
    df_overwrite = pd.concat(dfs_overwrite)
    return df_overwrite, exclude_keys


def fix_empty_ucbt_pncon(
    joined_cols: list, update_ponnot: bool = True, delete_excluded_keys: bool = True
):
    """
    Fixes empty ucbt ponnot values by querying the database and updating the records.

    Args:
        joined_cols (list): List of columns to join in the query.
        update_ponnot (bool, optional): Flag indicating whether to update the ponnot
                                        values. Defaults to True.
        delete_excluded_keys (bool, optional): Flag indicating whether to delete the
                                        excluded keys. Defaults to True.
    """
    conn = DBConnection("bronze")
    schema = ANEEL_BRONZE_CONTRACTS["ucbt"]["schema"]
    table_name = ANEEL_BRONZE_CONTRACTS["ucbt"]["tableName"]
    pk_key = "id_coluna"
    create_primary_key(conn, schema, table_name, pk_key)
    kwargs = {"filename": "empty_ucbt_ponnot"}
    df_empty_ucbt = query_empty_ucbt_pncon(
        conn, pk_key, joined_cols, schema, table_name, **kwargs
    )
    df_overwrite, exclude_keys = replace_energy_from_empty_ucbt_to_nearest_pncons(
        conn, df_empty_ucbt, pk_key, joined_cols
    )
    if update_ponnot:
        conn.update_table(df_overwrite, [pk_key], (schema, table_name))
    if delete_excluded_keys:
        conn.delete_rows_table(
            (schema, table_name), f"{pk_key} in ({','.join(exclude_keys)})"
        )


def try_join(
    query: str,
    dfs_good_match: List[pd.DataFrame],
    id_col_no_match: str,
) -> Tuple[List[pd.DataFrame], str]:
    """
    Executes a database query using the provided connection object
    and appends the resulting DataFrame
    to the list of good match DataFrames. Also returns a string representation
    of the unique values in the 'pn_con' column for rows with missing geometry.

    Parameters:
        query (str): The SQL query to execute.
        dfs_good_match (list): The list of DataFrames to append the query result to.
        id_col_no_match (str): The string representation of the unique values in the 'pn_con' column
            for rows with missing geometry.

    Returns:
        tuple: A tuple containing the updated list of good match DataFrames
        and the string representation of the unique values in the 'pn_con' column
        for rows with missing geometry.
    """
    conn = DBConnection("bronze")
    if id_col_no_match == "":
        return dfs_good_match, id_col_no_match
    df = conn.query_database(query)
    df_good_match = df[df.geometry.notna()]
    dfs_good_match.append(df_good_match.loc[:, ~df_good_match.columns.duplicated()])

    id_col_no_match = ", ".join(
        [str(id_col) for id_col in df[df.geometry.isna()].id_coluna.unique()]
    )
    return dfs_good_match, id_col_no_match


@save_parquet_decorator("silver", VALIDATION_CONTRACT_NO_JOIN_UCBT_PONNOT)
def save_no_join_ucbt_ponnot(mun_batch: str, id_col_no_match: str, **kwargs):
    """
    Saves the 'pn_con' values that did not match between the 'ucbt' and 'ponnot' tables.

    Args:
        mun_batch (str): The municipality to filter the data.
        id_col_no_match (str): The 'pn_con' values that did not match
                                between the 'ucbt' and 'ponnot' tables.
        **kwargs: Additional keyword arguments.

    Returns:
        DataFrame: A DataFrame containing the 'mun' and 'pn_con' values.

    Example:
        >>> save_no_join_ucbt_ponnot('example_municipality', 'example_pn_con')
        example_municipality  example_pn_con
    """
    df = pd.DataFrame({"mun_batch": [mun_batch], "pn_con": [id_col_no_match]})
    return df


@save_parquet_decorator("silver", ANEEL_SILVER_CONTRACTS["aneel"])
def save_partitioned_mun(df: pd.DataFrame, **kwargs):
    """
    Save the concatenated DataFrame with dropped columns.

    Args:
        df (pd.DataFrame): The DataFrame to filter.
        **kwargs: Additional keyword arguments.

    Returns:
        pd.DataFrame: The filtered DataFrame containing only the specified municipality.
    """
    return df


@save_parquet_decorator("silver", ANEEL_SILVER_CONTRACTS["aneel"])
def save_mun(df: pd.DataFrame, mun: str, **kwargs):
    """
    Save the concatenated DataFrame with dropped columns.

    Args:
        df (pd.DataFrame): The DataFrame to filter.
        mun (str): The municipality to filter by.
        **kwargs: Additional keyword arguments.

    Returns:
        pd.DataFrame: The filtered DataFrame containing only the specified municipality.
    """
    df_mun = df[df["mun"] == mun]
    if len(df_mun) < 1e6:
        return df_mun
    batch_size = int(1e6)
    df_batches = [df_mun[i : i + batch_size] for i in range(0, len(df_mun), batch_size)]
    for batch in df_batches:
        _ = save_partitioned_mun(batch, **kwargs)
    return pd.DataFrame()


def join_batches(path_ucbt: str, path_ponnot: str, mun_batch: List[str]):
    """
    Joins batches of data from two paths based on specific conditions.

    Args:
        path_ucbt (str): The path to the first batch of data.
        path_ponnot (str): The path to the second batch of data.
        mun_batch (List[str]): A list of municipalities to include in the join.
    """
    dfs_good_match = []
    id_col_no_match = "()"
    query = f"""
            SELECT *
            FROM {path_ucbt} u
            FULL JOIN {path_ponnot} p
            on u.pn_con = p.cod_id 
            and u.dist = p.dist
            and u.conj = p.conj
            and u.mun = p.mun
            where u.mun in ({mun_batch})
            """
    dfs_good_match, id_col_no_match = try_join(query, dfs_good_match, id_col_no_match)
    query = f"""
            SELECT *
            FROM {path_ucbt} u
            FULL JOIN {path_ponnot} p
            on u.pn_con = p.cod_id 
            and u.dist = p.dist
            and u.mun = p.mun 
            where u.id_coluna in ({id_col_no_match})
            and u.mun in ({mun_batch})
            """
    dfs_good_match, id_col_no_match = try_join(query, dfs_good_match, id_col_no_match)
    query = f"""
            SELECT *
            FROM {path_ucbt} u
            FULL JOIN {path_ponnot} p
            on u.pn_con = p.cod_id 
            and u.dist = p.dist
            and u.conj = p.conj 
            where u.id_coluna in ({id_col_no_match})
            and u.mun in ({mun_batch})
            """
    dfs_good_match, id_col_no_match = try_join(query, dfs_good_match, id_col_no_match)
    df = pd.concat(dfs_good_match).drop(
        columns=["ti", "cm", "tuc", "a1", "a2", "a3", "a4", "a5", "a6"]
    )
    kwargs = {"filename": "no_join_ucbt_ponnot"}
    _ = save_no_join_ucbt_ponnot(mun_batch, id_col_no_match, **kwargs)
    mun_batch = mun_batch.replace("'", "").split(",")
    for mun in tqdm(mun_batch, desc="Saving municipalities"):
        kwargs = {"filename": mun}
        _ = save_mun(df, mun, **kwargs)


def join_ucbt_and_ponnot():
    """
    Joins the 'ucbt' and 'ponnot' tables to update the 'pn_con' column in the 'ucbt' table.
    """
    path_ucbt = ".".join(
        [
            ANEEL_BRONZE_CONTRACTS["ucbt"]["schema"],
            ANEEL_BRONZE_CONTRACTS["ucbt"]["tableName"],
        ]
    )
    path_ponnot = ".".join(
        [
            ANEEL_BRONZE_CONTRACTS["ponnot"]["schema"],
            ANEEL_BRONZE_CONTRACTS["ponnot"]["tableName"],
        ]
    )
    conn = DBConnection("bronze")
    muns = conn.query_database(f"select distinct(mun) from {path_ucbt}")
    # list of capital cities
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
    for mun_batch in tqdm(mun_batches, desc="Processing mun batches"):
        if all(
            check_file_exists(mun, ANEEL_SILVER_CONTRACTS["aneel"]["physicalPath"])
            for mun in mun_batch["mun"].to_list()
        ):
            continue
        mun_batch = ",".join([f"'{mun}'" for mun in mun_batch["mun"].to_list()])
        join_batches(path_ucbt, path_ponnot, mun_batch)
    for mun in tqdm(large_mun_cods, desc="Processing large muns"):
        if check_file_exists(mun, ANEEL_SILVER_CONTRACTS["aneel"]["physicalPath"]):
            write_log(f"Skipping {mun}")
            continue
        join_batches(path_ucbt, path_ponnot, f"'{mun}'")


def get_neighboors(conn: DBConnection, df_col_id: pd.DataFrame):
    """
    Retrieves the neighboring information from the database based on the given column IDs.

    Args:
        conn (DBConnection): The database connection object.
        df_col_id (pd.DataFrame): The DataFrame containing the column IDs.

    Returns:
        pd.DataFrame: The DataFrame containing the neighboring information.
    """
    schema = ANEEL_SILVER_CONTRACTS["aneel"]["schema"]
    table_name = ANEEL_SILVER_CONTRACTS["aneel"]["tableName"]
    neighboors_info = dict(zip(df_col_id.columns, df_col_id.values[0]))
    cols_text = " AND ".join(
        [
            f"u.{col} = '{value}'" if isinstance(value, str) else f"{col} = {value}"
            for col, value in neighboors_info.items()
        ]
    )
    cols_text = cols_text.replace("None", "NULL")
    return set(
        conn.query_database(
            f"""
        SELECT id_coluna
        FROM {schema}.{table_name} u
        WHERE {cols_text}
        """
        ).id_coluna.to_list()
    )


def index_table(conn: DBConnection, joined_cols: list):
    """
    Create an index on a table in the ANEEL bronze database.

    Args:
        conn (DBConnection): The database connection object.
        joined_cols (list): The list of columns to be joined.
    """
    schema = ANEEL_SILVER_CONTRACTS["aneel"]["schema"]
    table_name = ANEEL_SILVER_CONTRACTS["aneel"]["tableName"]
    conn.create_index(schema, table_name, joined_cols)


def select_ids_without_match(conn: DBConnection):
    """
    Retrieves a set of IDs without a match from the specified database connection.

    Args:
        conn (DBConnection): The database connection object.

    Returns:
        set: A set of IDs without a match.
    """
    schema_val = VALIDATION_CONTRACT_NO_JOIN_UCBT_PONNOT["schema"]
    table_name_val = VALIDATION_CONTRACT_NO_JOIN_UCBT_PONNOT["tableName"]
    df_val = conn.query_database(
        f"""
        SELECT * 
        FROM {schema_val}.{table_name_val}
        """
    )
    col_ids = set(
        int(col_id.replace(" ", "")) if col_id != "" else ""
        for list_cols_id in df_val.pn_con.values
        for col_id in list_cols_id.split(",")
    )
    col_ids.remove("")
    col_ids = set(int(col_id) for col_id in col_ids)
    return col_ids


def fix_ponnot_without_match(joined_cols: list):
    """
    Fixes the 'ponnot' without a match in the joined columns.

    Args:
        joined_cols (list): A list of joined columns.
    """
    conn_silver = DBConnection("silver")
    index_table(conn_silver, joined_cols)
    col_ids = select_ids_without_match(conn_silver)
    schema = ANEEL_BRONZE_CONTRACTS["ucbt"]["schema"]
    table_name = ANEEL_BRONZE_CONTRACTS["ucbt"]["tableName"]
    conn_bronze = DBConnection("bronze")
    counter = 0
    for col_id in tqdm(col_ids):
        df_col_id = conn_bronze.query_database(
            f"""
            select {', '.join(joined_cols)} 
            from {schema}.{table_name} 
            where id_coluna = {int(col_id)}"""
        )
        neighboors_set = get_neighboors(conn_silver, df_col_id)
        if len(neighboors_set) == 1:
            counter += 1
            write_log(f"Counter: {counter}")
        # diff_set = neighboors_set.difference(col_ids)


def main():
    """
    This is the main function that performs the fix_empty_ucbt_ponnot operation.
    """
    joined_cols = [
        "dist",
        "mun",
        "conj",
        "brr",
        "uni_tr_at",
        "ctmt",
        "clas_sub",
        "fas_con",
        "gru_ten",
        "gru_tar",
        "are_loc",
    ]
    # update_ponnot_id_in_ucbt_table()
    # fix_empty_ucbt_pncon(joined_cols, True, True)
    # join_ucbt_and_ponnot()
    fix_ponnot_without_match(joined_cols)
