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

from typing import List, Tuple
from tqdm import tqdm
import mlflow
import pandas as pd

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from src.tools.data_contract.validation_data_contract import (
    get_validation_contracts,
    get_validation_partitions,
)
from src.tools.utils.save import save_parquet_decorator
from src.tools.utils.common import check_file_exists_in_disk, write_log, get_db_path

mlflow.set_experiment("aneel silver")

ANEEL_BRONZE_CONTRACTS = get_aneel_contracts("bronze")
ANEEL_SILVER_CONTRACTS = get_aneel_contracts("silver")
VALIDATION_PARTITIONS = get_validation_partitions()
VALIDATION_CONTRACT_NO_JOIN_UCBT_PONNOT = get_validation_contracts("silver", 0)


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
        [str(id_col) for id_col in df[df.geometry.isna()].row_id.unique()]
    )
    return dfs_good_match, id_col_no_match


@save_parquet_decorator("silver", VALIDATION_CONTRACT_NO_JOIN_UCBT_PONNOT)
def save_no_join_ucbt_ponnot(
    mun_batch: str, id_col_no_match: str, **kwargs
) -> pd.DataFrame:
    """
    Saves the 'ids_no_match' values that did not match between the 'ucbt' and 'ponnot' tables.

    Args:
        mun_batch (str): The municipality to filter the data.
        id_col_no_match (str): The 'ids_no_match' values that did not match
                                between the 'ucbt' and 'ponnot' tables.
        **kwargs: Additional keyword arguments.

    Returns:
        DataFrame: A DataFrame containing the 'mun' and 'ids_no_match' values.

    Example:
        >>> save_no_join_ucbt_ponnot('example_municipality', 'example_ids_no_match')
        example_municipality  example_ids_no_match
    """
    df = pd.DataFrame({"mun_batch": [mun_batch], "ids_no_match": [id_col_no_match]})
    return df


@save_parquet_decorator("silver", ANEEL_SILVER_CONTRACTS["aneel"])
def save_partitioned_mun(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
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
def save_mun(df: pd.DataFrame, mun: str, **kwargs) -> pd.DataFrame:
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
    add_to_mlflow(df_mun)
    if len(df_mun) < 1e6:
        return df_mun
    batch_size = int(1e6)
    df_batches = [df_mun[i : i + batch_size] for i in range(0, len(df_mun), batch_size)]
    for i, batch in enumerate(df_batches):
        kwargs = {"filename": "/part_".join([mun, str(i)])}
        _ = save_partitioned_mun(batch, **kwargs)
    return pd.DataFrame()


def add_to_mlflow(df: pd.DataFrame) -> None:
    """
    Logs the parameters and metrics to MLflow for the given GeoDataFrame.

    Args:
        df (pd.DataFrame): The dataframe to log.
    """
    with mlflow.start_run():
        mlflow.log_param("municipality", df.mun.unique()[0])
        mlflow.log_param("company", df.dist.unique()[0])
        mlflow.log_metric("num_rows", len(df))
        mlflow.log_metric("energy", df.filter(regex="ene_").sum().sum())


def join_batches(path_ucbt: str, path_ponnot: str, mun_batch: List[str]) -> None:
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
            SELECT DISTINCT *
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
            SELECT DISTINCT *
            FROM {path_ucbt} u
            FULL JOIN {path_ponnot} p
            on u.pn_con = p.cod_id 
            and u.dist = p.dist
            and u.mun = p.mun 
            where u.row_id in ({id_col_no_match})
            and u.mun in ({mun_batch})
            """
    dfs_good_match, id_col_no_match = try_join(query, dfs_good_match, id_col_no_match)
    query = f"""
            SELECT DISTINCT *
            FROM {path_ucbt} u
            FULL JOIN {path_ponnot} p
            on u.pn_con = p.cod_id 
            and u.dist = p.dist
            and u.conj = p.conj 
            where u.row_id in ({id_col_no_match})
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


def main() -> None:
    """
    This is the main function that executes the join_ucbt_and_ponnot operation.
    """
    contract_ucbt = ANEEL_BRONZE_CONTRACTS["ucbt"]
    contract_ponnot = ANEEL_BRONZE_CONTRACTS["ponnot"]
    path_ucbt = get_db_path(contract_ucbt)
    path_ponnot = get_db_path(contract_ponnot)
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
            check_file_exists_in_disk(
                mun, ANEEL_SILVER_CONTRACTS["aneel"]["physicalPath"]
            )
            for mun in mun_batch["mun"].to_list()
        ):
            continue
        mun_batch = ",".join([f"'{mun}'" for mun in mun_batch["mun"].to_list()])
        join_batches(path_ucbt, path_ponnot, mun_batch)
    for mun in tqdm(large_mun_cods, desc="Processing large muns"):
        if check_file_exists_in_disk(
            mun, ANEEL_SILVER_CONTRACTS["aneel"]["physicalPath"]
        ):
            write_log(f"Skipping {mun}")
            continue
        join_batches(path_ucbt, path_ponnot, f"'{mun}'")
