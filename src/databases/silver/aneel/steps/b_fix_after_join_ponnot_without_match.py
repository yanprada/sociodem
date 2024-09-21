"""
This module contains functions to create and update the 'aneel_silver' database.
"""

import gc
import os
from functools import lru_cache
from typing import Tuple, Set, List
import ipdb
from tqdm import tqdm
import pandas as pd

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.common import write_log, check_file_exists_in_db, get_db_path
from src.tools.utils.save import save_parquet_decorator

from src.tools.utils.execution_manager import ExecutionManager
from src.databases.silver.aneel.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
manager.update_status("running_step_2")


ANEEL_BRONZE_CONTRACTS = execution_parameters["data_contracts"][0]
ANEEL_SILVER_CONTRACTS = execution_parameters["data_contracts"][1]


def index_table(conn: DBConnection, joined_cols: list, filename: str):
    """
    Create an index on a table in the ANEEL bronze database.

    Args:
        conn (DBConnection): The database connection object.
        joined_cols (list): The list of columns to be joined.
        filename (str): The name of the file.
    """
    schema = ANEEL_SILVER_CONTRACTS[filename]["schema"]
    table_name = ANEEL_SILVER_CONTRACTS[filename]["tableName"]
    conn.create_index(schema, table_name, joined_cols)


@lru_cache(1)
def select_ids_without_match(
    conn_bronze: DBConnection,
    conn_silver: DBConnection,
    path_bronze: str,
    path_silver: str,
) -> Set[int]:
    """
    Retrieves a set of IDs without a match from the specified database connection.

    Args:
        conn_bronze (DBConnection): The database connection object for the bronze database.
        conn_silver (DBConnection): The database connection object for the silver database.
        path_bronze (str): The path to the bronze table.
        path_silver (str): The path to the silver table.

    Returns:
        set: A set of IDs without a match.
    """
    dfb = conn_bronze.query_database(f"""SELECT row_id FROM {path_bronze}""")
    dfs = conn_silver.query_database(f"""SELECT row_id FROM {path_silver}""")
    col_ids = set(dfb.row_id).difference(set(dfs.row_id))
    return col_ids


def query_db(conn: DBConnection, path: str, pk_key: str, keys: str) -> pd.DataFrame:
    """
    Queries the database for records with specified primary key values.

    Args:
        conn (DBConnection): The database connection object.
        path (str): The path of the database table.
        pk_key (str): The name of the primary key column.
        keys (str): A string of primary key values to query.

    Returns:
        pd.DataFrame: A dataframe of rows matching the specific primary key values.
    """
    return conn.query_database(
        f"""
            SELECT *
            FROM {path} 
            WHERE {pk_key} in ({keys})
            """
    )


def test_integrity_of_join(
    joined_cols: List[str], df_keys: pd.DataFrame, df_values: pd.DataFrame
) -> None:
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
        try:
            assert df_keys[col].unique() == df_values[col].unique()
        except:
            ipdb.set_trace()


def test_integrity_energy_addition(
    df_values: pd.DataFrame, df_keys: pd.DataFrame, df_result: pd.DataFrame
) -> None:
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
        try:
            assert (
                df_result[col].sum()
                - (df_values[col].sum() + df_keys.loc[col].squeeze())
                < 1
            )
        except:
            ipdb.set_trace()


def distribute_energy(df_values: pd.DataFrame, df_keys: pd.DataFrame) -> pd.DataFrame:
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


def find_problematic_neighbors(
    df: pd.DataFrame,
    new_keys: Set[int],
    all_keys: Set[int],
    grouped_lvl: int,
) -> pd.DataFrame:
    """
    Find problematic neighbors in the given DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to search for problematic neighbors.
        new_keys (Set[int]): A set of new keys to search for in the DataFrame.
        all_keys (Set[int]): A set of all keys in the DataFrame.
        grouped_lvl (int): The level of grouping.

    Returns:
        pd.DataFrame: A DataFrame containing the problematic neighbors and their counts.
    """
    data_dict = {
        "ids_problematicos": [],
        "ids_normais": [],
        "count_ids_problematicos": [],
        "count_ids_normais": [],
        "count_ids_totais": [],
    }

    for ids_agrupado in tqdm(
        df.ids_agrupados, desc=f"Finding neighbors level {grouped_lvl}"
    ):
        elements = ids_agrupado.split(",")
        int_elements = set(map(int, elements))
        problematic_elements = int_elements & new_keys
        count_problematic = len(problematic_elements)
        if count_problematic > 0:
            ids_normais = int_elements - all_keys
            count_normais = len(ids_normais)
            data_dict["ids_problematicos"].append(
                ",".join(map(str, problematic_elements))
            )
            data_dict["count_ids_problematicos"].append(count_problematic)
            data_dict["ids_normais"].append(",".join(map(str, ids_normais)))
            data_dict["count_ids_normais"].append(count_normais)
            data_dict["count_ids_totais"].append(len(elements))
    return pd.DataFrame.from_dict(data_dict).sort_values("count_ids_problematicos")


def get_neighbors(
    conn: DBConnection, grouped_lvl: str, keys: Set[int], all_keys: Set[int]
) -> Tuple[pd.DataFrame, Set[int]]:
    """
    Retrieves the neighbors of the given keys from the ANEEL Silver Contracts table.

    Args:
        conn (Connection): The database connection object.
        grouped_lvl (str): The grouped level of the ANEEL Silver Contracts table.
        keys (Set[int]): The set of keys for which to retrieve the neighbors.
        all_keys (Set[int]): The set of all keys in the ANEEL Silver Contracts table.

    Returns:
        Tuple[pd.DataFrame, Set[int]]: A tuple containing a DataFrame with the new rows
        with the neighbors and a set of remaining keys.

    """
    new_keys = keys.copy()
    contract_neighboors = ANEEL_SILVER_CONTRACTS[grouped_lvl]
    path = get_db_path(contract_neighboors)
    query = f"""
    SELECT ids_agrupados
    FROM {path}
    """
    df = conn.query_database(query)
    df = find_problematic_neighbors(df, new_keys, all_keys, grouped_lvl).copy()
    div_col = df["count_ids_problematicos"] / (
        df["count_ids_normais"] + df["count_ids_problematicos"]
    )
    fit_ids = df[(div_col) <= 0.7]["ids_problematicos"]
    new_keys.difference_update(
        set(int(x) for sublist in fit_ids for x in sublist.split(","))
    )
    return (
        df[df.ids_problematicos.isin(fit_ids)].assign(level=grouped_lvl),
        new_keys,
    )


def get_col_id_neighboors(conn_silver: DBConnection, cols_id: Set[int]) -> pd.DataFrame:
    """
    Retrieves the neighbors of the given column IDs from the silver database.

    Args:
        conn_silver (connection): The connection to the silver database.
        cols_id (list): A list of column IDs.

    Returns:
        DataFrame: A DataFrame containing the neighbors of the given column IDs.

    """
    dfs = []
    ids_for_loop = cols_id
    for i in tqdm(range(7), desc="Finding neighbors in different level aggregations"):
        temp_df, ids_for_loop = get_neighbors(
            conn_silver, f"neighbors_lvl{i}", ids_for_loop, cols_id
        )
        dfs.append(temp_df)
        if ids_for_loop == set():
            write_log(f"All neighbors were found in lvl {i}.")
            break
    df = pd.concat(dfs)
    return df.sort_values("count_ids_totais", ascending=False)


def get_cols_to_join(lvl: int):
    """
    Returns a list of columns to join in a dataset.

    Parameters:
        lvl (int): The level of columns to join. It determines which set of columns to return.

    Returns:
        List[str]: A list of columns to join.

    Example:
        >>> get_cols_to_join(0)
        ['dist', 'mun', 'conj', 'brr', 'cep', 'uni_tr_mt', 'uni_tr_at', 'ctmt',
            'clas_sub', 'fas_con', 'gru_ten', 'gru_tar', 'are_loc']
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
    join4 = [
        "dist",  # Aneel company
        "conj",  # Ponnot group
        "clas_sub",  # Subclass - ex: residential, commercial, etc
        "gru_tar",  # Tariff group reference code
        "are_loc",  # Reference code of the area - ex: urban, rural
    ]
    join5 = [
        "dist",  # Aneel company
        "mun",  # Municipality
        "clas_sub",  # Subclass - ex: residential, commercial, etc
        "are_loc",  # Reference code of the area - ex: urban, rural
    ]
    join6 = [
        "dist",  # Aneel company
        "conj",  # Ponnot group
        "clas_sub",  # Subclass - ex: residential, commercial, etc
        "are_loc",  # Reference code of the area - ex: urban, rural
    ]
    col_list = [joined_cols, join1, join2, join3, join4, join5, join6]
    return col_list[lvl]


@save_parquet_decorator("silver", ANEEL_SILVER_CONTRACTS["temp_join"])
def process_rows_distribute_energy(
    df_values_ids: pd.DataFrame,
    i: int,
    batch: int,
    conns: Tuple[DBConnection],
    paths: List[str],
) -> pd.DataFrame:
    """
    Process rows to distribute energy.

    Args:
        df_values_ids (pd.DataFrame): DataFrame containing values and ids.
        i (int): Starting index of the batch.
        batch (int): Number of rows to process in each batch.
        conns (Tuple[DBConnection]): Tuple of database connections (bronze and silver).
        paths (List[str]): List of file paths (bronze and silver).

    Returns:
        pd.DataFrame: DataFrame with the distributed energy values.

    """
    df_overwrite = []
    missing_keys = []
    for row in tqdm(
        df_values_ids.iloc[i : i + batch].itertuples(),
        desc=f"Processing rows from {i} to {i+batch}",
    ):
        df_keys = query_db(conns[0], paths[0], "row_id", row.ids_problematicos)
        df_values = query_db(conns[1], paths[1], "row_id", row.ids_normais)
        assert set(int(x) for x in row.ids_normais.split(",")) == set(
            df_values.row_id
        ), "Missing keys in df_values"
        if df_values.empty:
            missing_keys.append(row.ids_normais)
        joined_cols = get_cols_to_join(int(row.level.split("lvl")[-1]))
        test_integrity_of_join(joined_cols, df_keys, df_values)
        df_keys = df_keys.filter(regex="ene_|dic_|fic_").sum().to_frame()
        df_result = distribute_energy(df_values, df_keys)
        test_integrity_energy_addition(df_values, df_keys, df_result)
        df_values[df_result.columns] = df_result
        df_overwrite.append(df_values)
    df_overwrite = pd.concat(df_overwrite)
    missing_keys_dir = os.path.join(
        ANEEL_SILVER_CONTRACTS["temp_join"]["physicalPath"], "missing_keys"
    )
    os.makedirs(missing_keys_dir, exist_ok=True)
    if len(missing_keys) > 0:
        pd.DataFrame(data=missing_keys, columns=["missing_keys"]).to_parquet(
            os.path.join(
                missing_keys_dir,
                f"{i}.parquet",
            )
        )
    return df_overwrite


def check_file_exists(conns: Tuple[DBConnection], df: pd.DataFrame) -> bool:
    """
    Checks if a file exists in the database based on the last ID in the given DataFrame.

    Args:
        conns (Tuple[DBConnection]): A tuple of database connections.
        df (pd.DataFrame): The DataFrame containing the data.

    Returns:
        bool: True if the file exists in the database, False otherwise.
    """
    contract_aneel_temp_join = ANEEL_SILVER_CONTRACTS["temp_join"]
    path_saved = get_db_path(contract_aneel_temp_join)
    last_id = df.ids_normais.iloc[-1]
    condition = f"WHERE row_id IN ({last_id})"
    return check_file_exists_in_db(conns[1], path_saved, condition)


def process_batch(
    df: pd.DataFrame,
    conns: Tuple[DBConnection],
    paths: Tuple[str],
    batch: int,
    lvl: str,
) -> None:
    """
    Process files in batches and distribute energy data.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to be processed.
        conns (Tuple[DBConnection]): A tuple of database connections.
        paths (Tuple[str]): A tuple of file paths.
        batch (int): The batch size for processing the files.
        lvl (str): The level of the files being processed.
    """
    for i in tqdm(
        range(0, len(df), batch),
        desc=f"Processing {lvl} files",
    ):
        if check_file_exists(conns, df):
            continue
        _ = process_rows_distribute_energy(
            df,
            i,
            batch,
            conns,
            paths,
        )


def distribute_energy_ponnot_without_match(
    conn_bronze: DBConnection,
    conn_silver: DBConnection,
    path_bronze: str,
    path_silver: str,
) -> None:
    """
    Distributes energy without matching for the given columns.

    Args:
        conn_bronze (connection): Connection to the bronze database.
        conn_silver (connection): Connection to the silver database.
        path_bronze (str): Path to the bronze database.
        path_silver (str): Path to the silver database.
    """
    cols_id = select_ids_without_match(
        conn_bronze, conn_silver, path_bronze, path_silver
    )
    df_values_ids = get_col_id_neighboors(conn_silver, cols_id)
    process_list = [
        (1, (df_values_ids.count_ids_totais >= 1e6), "large"),
        (
            10,
            (
                (df_values_ids.count_ids_totais >= 7e4)
                & (df_values_ids.count_ids_totais < 1e6)
            ),
            "medium",
        ),
        (
            200,
            (
                (df_values_ids.count_ids_totais >= 1e4)
                & (df_values_ids.count_ids_totais < 7e4)
            ),
            "small",
        ),
        (1000, (df_values_ids.count_ids_totais < 1e4), "extra small"),
    ]
    for batch, condition, lvl in process_list:
        df_values_ids_lvl = df_values_ids[condition]
        process_batch(
            df_values_ids_lvl,
            (conn_bronze, conn_silver),
            (path_bronze, path_silver),
            batch,
            lvl,
        )


def make_aggregation_ids(cols: list, path: str, filename: str):
    """
    Generate aggregation IDs by columns.

    Args:
        cols (list): List of columns to be selected.
        path (str): Path of the table to query.
        filename (int): filename of Level of aggregation.
    """

    @save_parquet_decorator("silver", ANEEL_SILVER_CONTRACTS[filename])
    def make_aggregation_by_cols(cols: list, path: str) -> pd.DataFrame:
        conn = DBConnection("bronze")
        query = f"""
        SELECT {", ".join(cols)},
        STRING_AGG(row_id::text, ',') AS ids_agrupados
        FROM
        {path} u
        GROUP BY
        {", ".join(cols)}
        """
        return conn.query_database(query)

    _ = make_aggregation_by_cols(cols, path)
    del _
    gc.collect()


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


def create_grouped_tables(conn: DBConnection) -> None:
    """
    Create grouped tables based on the joined columns.
    """
    contract_ucbt = ANEEL_BRONZE_CONTRACTS["ucbt"]
    path_bronze = get_db_path(contract_ucbt)
    for i in tqdm(range(0, 7), desc="Creating grouped tables"):
        cols = get_cols_to_join(i)
        filename = f"neighbors_lvl{i}"
        db_exists = check_db_exists(filename)
        if not db_exists:
            make_aggregation_ids(cols, path_bronze, filename)
        index_table(conn, cols, filename)
        gc.collect()


def main():
    """
    Fixes the 'ponnot' without a match in the joined columns.
    """
    conn_silver = DBConnection("silver")
    create_grouped_tables(conn_silver)
    contract_ucbt = ANEEL_BRONZE_CONTRACTS["ucbt"]
    contract_aneel = ANEEL_BRONZE_CONTRACTS["aneel"]
    path_bronze = get_db_path(contract_ucbt)
    path_silver = get_db_path(contract_aneel)
    conn_silver.add_pk_to_table(
        path_silver.split(".", maxsplit=1)[0], path_silver.split(".")[1], "row_id"
    )
    conn_bronze = DBConnection("bronze")
    distribute_energy_ponnot_without_match(
        conn_bronze, conn_silver, path_bronze, path_silver
    )
    manager.update_status("finished_step_2")
    manager.update_last_run()
