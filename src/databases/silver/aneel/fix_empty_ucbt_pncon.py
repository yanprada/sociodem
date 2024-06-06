"""
This script contains functions to fix empty ucbt_ponnot values in the ANEEL database.

The functions in this script perform the following tasks:
- Create a primary key on a specified column in a table.
- Query the database to retrieve empty ucbt_ponnot records.
- Query the database for records with specified primary key values.
- Test the integrity of the join operation by comparing the unique values of specified 
    columns in two dataframes.
- Test the integrity of energy addition in the given dataframes.
- Distribute energy values based on the given keys.
- Replace energy from empty UCBT (Unidade Consumidora com Baixa Tensão) to the 
    nearests PONNOTs (Pontos Notaveis).
- Fix empty ucbt ponnot values by querying the database and updating the records.
- Generate aggregation IDs by columns.
- Get a list of columns to join in a dataset.
"""

from typing import List, Tuple
from tqdm import tqdm
import pandas as pd

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from src.tools.data_contract.validation_data_contract import (
    get_validation_contracts,
    get_validation_partitions,
)
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.common import write_log

ANEEL_BRONZE_CONTRACTS = get_aneel_contracts("bronze")
ANEEL_SILVER_CONTRACTS = get_aneel_contracts("silver")
VALIDATION_PARTITIONS = get_validation_partitions()
VALIDATION_CONTRACT_EMPTY_STR_UCBT_PONNOT = get_validation_contracts("bronze", 0)


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


@save_parquet_decorator("bronze", VALIDATION_CONTRACT_EMPTY_STR_UCBT_PONNOT)
def query_empty_ucbt_pncon(
    conn: DBConnection, pk_key: str, joined_cols: list, schema: str, table_name: str
):
    """
    Query the database to retrieve empty ucbt_ponnot records.

    This function executes a SQL query to retrieve empty ucbt_ponnot records from
    the database.
    It joins the specified columns from the given schema and table name with the
    ucbt_ponnot table.
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


def query_db(conn: DBConnection, path: str, pk_key: str, keys: set) -> pd.DataFrame:
    """
    Queries the database for records with specified primary key values.

    Args:
        conn (DBConnection): The database connection object.
        path (str): The path of the database table.
        pk_key (str): The name of the primary key column.
        keys (set): A set of primary key values to query.

    Returns:
        pd.DataFrame: A dataframe of rows matching the specific primary key values.
    """
    return conn.query_database(
        f"""
            SELECT *
            FROM {path} 
            WHERE {pk_key} in ({", ".join([str(key) for key in keys])})
            """
    )


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
    df_pct = (
        df_values.filter(regex="ene_|fic_|dic_")
        / df_values.filter(regex="ene_|fic_|dic_").sum()
    ).fillna(0)
    df_result = df_pct.mul(df_keys[0]).add(
        df_values.filter(regex="ene_|fic_|dic_").fillna(0), fill_value=0
    )
    return df_result


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
    path_table = ".".join(
        [
            ANEEL_BRONZE_CONTRACTS["ucbt"]["schema"],
            ANEEL_BRONZE_CONTRACTS["ucbt"]["tableName"],
        ]
    )
    dfs_overwrite = []
    exclude_keys = []
    for row in tqdm(df_empty_ucbt.itertuples()):
        keys = set(row[1].split(", "))
        values = set(row[2].split(", ")).difference(keys)
        if values == set():
            continue
        df_keys = query_db(conn, path_table, pk_key, keys)
        df_values = query_db(conn, path_table, pk_key, values)
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

    df_empty_ucbt = query_empty_ucbt_pncon(
        conn,
        pk_key,
        joined_cols,
        schema,
        table_name,
        **{"filename": "empty_ucbt_ponnot"},
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


@save_parquet_decorator("silver", ANEEL_SILVER_CONTRACTS["aneel"], save_pq=False)
def make_aggregation_ids_by_cols(cols: list, path: str, **kwargs) -> pd.DataFrame:
    """
    Generate aggregation IDs by columns.

    Args:
        cols (list): List of columns to be selected.
        path (str): Path of the table to query.
        **kwargs: Additional keyword arguments.

    Returns:
        pd.DataFrame: List of aggregation IDs.

    """
    conn = DBConnection("bronze")
    query = f"""
    SELECT {", ".join(cols)},
    STRING_AGG(id_coluna::text, ',') AS ids_agrupados
    FROM
    {path} u
    GROUP BY
    {",".join(cols)}
    """
    return conn.query_database(query)


def get_cols_to_join(joined_cols: List[str]):
    """
    Returns a list of columns to join in a dataset.

    Parameters:
        joined_cols (List[str]): The list of columns already joined.

    Returns:
        List[List[str]]: A list of lists, where each inner list represents a set of columns to join.

    Example:
        >>> joined_cols = ["col1", "col2"]
        >>> get_cols_to_join(joined_cols)
        [['col1', 'col2'], ['dist', 'mun', 'conj', 'brr', 'cep', 'clas_sub',
            'fas_con', 'gru_ten', 'gru_tar', 'are_loc'],
        ['dist', 'mun', 'conj', 'clas_sub', 'fas_con', 'gru_ten', 'gru_tar', 'are_loc'],
        ['dist', 'mun', 'conj', 'clas_sub', 'gru_tar', 'are_loc']]
    """

    join1 = [
        "dist",  # Aneel company
        "mun",  # Municipality
        "conj",  # Ponnot group
        "brr",  # Neighborhood
        "cep",  # Region identifier
        "clas_sub",  # Subclass - ex: residential, commercial, etc
        "fas_con",  # Connection phase reference code
        "gru_ten",  # Voltage group reference code
        "gru_tar",  # Tariff group reference code
        "are_loc",  # Reference code of the area - ex: urban, rural
    ]
    join2 = [
        "dist",  # Aneel company
        "mun",  # Municipality
        "conj",  # Ponnot group
        "clas_sub",  # Subclass - ex: residential, commercial, etc
        "fas_con",  # Connection phase reference code
        "gru_ten",  # Voltage group reference code
        "gru_tar",  # Tariff group reference code
        "are_loc",  # Reference code of the area - ex: urban, rural
    ]
    join3 = [
        "dist",  # Aneel company
        "mun",  # Municipality
        "conj",  # Ponnot group
        "clas_sub",  # Subclass - ex: residential, commercial, etc
        "gru_tar",  # Tariff group reference code
        "are_loc",  # Reference code of the area - ex: urban, rural
    ]
    return [joined_cols, join1, join2, join3]


def check_db_exists(filename: str) -> bool:
    """
    Check if a database exists in the silver schema.

    Args:
        filename (str): The name of the file representing the database.

    Returns:
        bool: True if the database exists, False otherwise.
    """
    path = ".".join([ANEEL_SILVER_CONTRACTS["aneel"]["schema"], filename])
    conn = DBConnection("silver")
    try:
        _ = conn.query_database(f"SELECT * FROM {path} LIMIT 1")
        return True
    except Exception as e:
        # Catch any other exception and print its type and message
        write_log(f"Unexpected error: {type(e).__name__}, {e}")
        return False


def create_grouped_tables(joined_cols):
    """
    Create grouped tables based on the joined columns.

    Args:
        joined_cols (list): A list of columns to join.
    """
    path_bronze = ".".join(
        [
            ANEEL_BRONZE_CONTRACTS["ucbt"]["schema"],
            ANEEL_BRONZE_CONTRACTS["ucbt"]["tableName"],
        ]
    )
    for i, cols in enumerate(get_cols_to_join(joined_cols)):
        filename = f"aggregation_ids_level_{i}"
        db_exists = check_db_exists(filename)
        if not db_exists:
            kwargs = {"filename": f"aggregation_ids_level_{i}"}
            _ = make_aggregation_ids_by_cols(cols, path_bronze, **kwargs)


def main():
    """
    This is the main function that performs the fix_empty_ucbt_pncon operation.
    """

    joined_cols = [
        "dist",  # Aneel company
        "mun",  # Municipality
        "conj",  # Ponnot group
        "brr",  # Neighborhood
        "cep",  # Region identifier
        "uni_tr_mt",  # Medium voltage transformer unit code
        "uni_tr_at",  # High voltage transformer unit code
        "ctmt",  # Medium Voltage Circuit Code
        "clas_sub",  # Subclass - ex: residential, commercial, etc
        "fas_con",  # Connection phase reference code
        "gru_ten",  # Voltage group reference code
        "gru_tar",  # Tariff group reference code
        "are_loc",  # Reference code of the area - ex: urban, rural
    ]
    fix_empty_ucbt_pncon(joined_cols, True, True)
    create_grouped_tables(joined_cols)
