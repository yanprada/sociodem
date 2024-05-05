"""
This module contains functions to create and update the 'aneel_silver' database.
"""

from typing import List, Tuple
from tqdm import tqdm
import pandas as pd
from src.tools.databases.data_connection.connection import DBConnection
from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from src.tools.data_contract.validation_data_contract import get_validation_contracts
from src.tools.utils.save import save_parquet_decorator

ANEEL_BRONZE_CONTRACTS = get_aneel_contracts("bronze")
ANEEL_SILVER_CONTRACTS = get_aneel_contracts("silver")
VALIDATION_CONTRACT = get_validation_contracts("bronze", 0)


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
    df["pn_con"] = df["pn_con_1"]  # update pn_con column with ramlig pn_con value
    df = df.iloc[:, :-22]  # remove columns from ramlig
    conn.update_table(df, (df.columns[1:], df.columns), ("infrastructure", "ucbt"))


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


@save_parquet_decorator("bronze", VALIDATION_CONTRACT)
def query_empty_ucbt_ponnot(conn: DBConnection, pk_key: str, joined_cols: list):
    """
    Query the database to retrieve empty ucbt_ponnot records.

    Args:
        conn (DBConnection): The database connection object.
        pk_key (str): The primary key column name.
        joined_cols (list): The list of column names used for joining.

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
            FROM infrastructure.ucbt
            WHERE pn_con = ' '
            GROUP BY 
                {", ".join(joined_cols)}
        ) AS u
        LEFT JOIN infrastructure.ucbt u2 
            ON {cols_text}
        GROUP BY u.group_keys

    """
    )


def replace_energy_from_empty_ucbt_to_nearest_ponnots(
    conn: DBConnection, df_empty_ucbt: pd.DataFrame, pk_key: str, joined_cols: list
) -> Tuple[pd.DataFrame, List[str]]:
    """
    Replaces energy from empty UCBT (Unidade Consumidora com Baixa Tensão) to
    the nearest PONNOTs (Pontos Notaveis).

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


def fix_empty_ucbt_ponnot(
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
    df_empty_ucbt = query_empty_ucbt_ponnot(conn, pk_key, joined_cols)
    df_overwrite, exclude_keys = replace_energy_from_empty_ucbt_to_nearest_ponnots(
        conn, df_empty_ucbt, pk_key, joined_cols
    )
    if update_ponnot:
        conn.update_table(df_overwrite, [pk_key], (schema, table_name))
    if delete_excluded_keys:
        conn.delete_rows_table(
            (schema, table_name), f"{pk_key} in ({','.join(exclude_keys)})"
        )


@save_parquet_decorator("bronze", VALIDATION_CONTRACT)
def join_data_per_mun(conn, mun, path_ucbt, path_ponnot):
    """
    Joins data per municipality.

    Args:
        conn (Connection): The database connection object.
        mun (str): The municipality to filter the data.
        path_ucbt (str): The path to the UCBT table.
        path_ponnot (str): The path to the PONNOT table.

    Returns:
        ResultSet: The result of the SQL query joining the UCBT and PONNOT
                    tables for the specified municipality.
    """
    return conn.query_database(
        f"""
        SELECT *
        FROM {path_ucbt} u
        LEFT JOIN {path_ponnot} p
        on u.pn_con = p.cod_id 
        and u.dist = p.dist
		and u.conj = p.conj
		and u.mun = p.mun 
        where u.mun = '{mun}'
        """
    )


def join_ucbt_and_ponnot():
    """
    Joins the 'ucbt' and 'ponnot' tables to update the 'pn_con' column in the 'ucbt' table.
    """
    conn = DBConnection("bronze")
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
    muns = conn.query_database(f"select distinct(mun) from {path_ucbt}")
    for mun in muns["mun"]:
        _ = join_data_per_mun(conn, mun, path_ucbt, path_ponnot)


def main():
    """
    This is the main function that performs the fix_empty_ucbt_ponnot operation.
    """
    joined_cols = [
        "dist",
        "uni_tr_at",
        "ctmt",
        "conj",
        "mun",
        "clas_sub",
        "fas_con",
        "gru_ten",
        "gru_tar",
        "are_loc",
        "brr",
    ]
    update_ponnot_id_in_ucbt_table()
    fix_empty_ucbt_ponnot(joined_cols, False, False)
    join_ucbt_and_ponnot()
