"""
This module contains functions to process hex IDs for ANEEL data.

The functions in this module retrieve data from the ANEEL database, 
perform calculations on the data,
and group the data by hexagon.
"""

import concurrent.futures
from functools import lru_cache
from itertools import product
from typing import List
import h3
import pandas as pd
from shapely import wkb
from tqdm import tqdm
from src.tools.databases.data_connection.connection import DBConnection
from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from src.tools.utils.constants import HEX_RESOLUTION, ANEEL_CLASSES
from src.tools.utils.save import save_parquet_decorator

ANEEL_SILVER_CONTRACTS = get_aneel_contracts("silver")


@lru_cache(maxsize=1)
def get_table_lenth(conn: DBConnection, path: str) -> int:
    """
    Retrieves the length of a table in the database.

    Args:
        conn (DBConnection): The database connection object.
        path (str): The path to the table.
    Returns:
        int: The number of rows in the table.
    """
    query = f"""
    SELECT COUNT(*) as count FROM {path}
    """
    result = conn.query_database(query)
    return result["count"].squeeze()


def calculate_h3_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the H3 index for each point in the given DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame containing the points.

    Returns:
        pandas.DataFrame: The DataFrame with the H3 index calculated
            for each point.
    """
    df["geometry"] = df["geometry"].apply(wkb.loads)
    df["lat"] = df["geometry"].apply(lambda point: point.y)
    df["lng"] = df["geometry"].apply(lambda point: point.x)
    df["hex_col"] = df.apply(lambda x: h3.geo_to_h3(x.lat, x.lng, HEX_RESOLUTION), 1)
    return df


def calculate_energy_consumption(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the energy consumption metrics for the given DataFrame.

    Parameters:
    df (pandas.DataFrame): The input DataFrame containing energy consumption data.

    Returns:
    pandas.DataFrame: The input DataFrame with additional columns for
        energy consumption metrics.
    """
    summer = ["ene_12", "ene_01", "ene_02"]
    winter = ["ene_05", "ene_06", "ene_07"]
    df_energy = df.filter(regex="ene")
    df["energy_consumption_summer"] = df[summer].sum(axis=1)
    df["mean_energy_consumption_summer"] = df[summer].mean(axis=1)
    df["energy_consumption_winter"] = df[winter].sum(axis=1)
    df["mean_energy_consumption_winter"] = df[winter].sum(axis=1)
    df["energy_consumption"] = df_energy.sum(axis=1)
    df["mean_energy_consumption"] = df_energy.mean(axis=1)
    df["std_energy_consumption"] = df_energy.std(axis=1)
    return df


def calculate_energy_interuption_hours(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the mean energy interruption hours for a given DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame containing the interruption duration data.

    Returns:
        pandas.DataFrame: The DataFrame with an additional column for the mean
            energy interruption hours.
    """
    df_interupt_duration = df.filter(regex="dic")
    df["mean_energy_interuption_hours"] = df_interupt_duration.mean(axis=1)
    return df


def calculate_energy_interuption_frequency(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the mean energy interruption frequency for a given DataFrame.

    Parameters:
    df (pandas.DataFrame): The input DataFrame containing energy interruption data.

    Returns:
    pandas.DataFrame: The input DataFrame with an additional column for the mean
        energy interruption frequency.

    """
    df_interupt_frequency = df.filter(regex="dic")
    df["mean_energy_interuption_frequency"] = df_interupt_frequency.mean(axis=1)
    return df


def group_columns(df: pd.DataFrame, grouped_cols: List[str]) -> pd.DataFrame:
    """
    Group columns in the DataFrame based on specified columns,
    and perform aggregation operations on other columns.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        grouped_cols (List[str]): The columns to group by.

    Returns:
        pandas.DataFrame: The resulting DataFrame with grouped and aggregated columns.
    """
    return (
        df.groupby(grouped_cols, as_index=False)
        .agg(
            {
                "energy_consumption": "sum",
                "energy_consumption_summer": "sum",
                "energy_consumption_winter": "sum",
                "mean_energy_consumption": "mean",
                "mean_energy_consumption_winter": "mean",
                "mean_energy_consumption_summer": "mean",
                "std_energy_consumption": "std",
                "mean_energy_interuption_hours": "mean",
                "mean_energy_interuption_frequency": "mean",
                "mun": lambda x: x.value_counts().idxmax() if not x.empty else None,
                "brr": lambda x: x.value_counts().idxmax() if not x.empty else None,
                "cep": lambda x: x.value_counts().idxmax() if not x.empty else None,
                "mat": lambda x: x.value_counts().idxmax() if not x.empty else None,
                "are_loc": lambda x: x.value_counts().idxmax() if not x.empty else None,
                "sit_ativ": lambda x: (
                    x.value_counts().idxmax() if not x.empty else None
                ),
                "cnae": lambda x: (
                    ",".join([str(i) for i in x if i is not None])
                    if not x.empty
                    else ""
                ),
                "dat_con": lambda x: (
                    ",".join([str(i) for i in x if i is not None])
                    if not x.empty
                    else ""
                ),
            }
        )
        .set_index("hex_col")
    )


def pivot_energy_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Pivot energy columns in the given DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame containing the energy columns.
        cols (List[str]): The list of energy columns to pivot.

    Returns:
        pandas.DataFrame: The DataFrame with energy columns pivoted.

    """
    pivot_df = df.pivot(columns="clas_sub", values=cols)
    pivot_df.columns = [
        f"{col}" if isinstance(col, str) else "_".join(list(col))
        for col in pivot_df.columns
    ]
    df = group_columns(df.reset_index(), ["hex_col"])
    df = df.drop(columns=cols).join(pivot_df).fillna(0)
    return df


def add_missing_columns(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Adds missing columns to the given DataFrame.

    Parameters:
    df (pandas.DataFrame): The DataFrame to add missing columns to.

    Returns:
    pandas.DataFrame: The DataFrame with missing columns added.
    """
    set_values = list(set(ANEEL_CLASSES.values()))
    for col in product(cols, set_values):
        col = "_".join(col)
        if col not in df.columns:
            df[col] = 0
    return df


def group_by_hexagon(df: pd.DataFrame) -> pd.DataFrame:
    """
    Groups the given DataFrame by hexagon.

    Args:
        df (pandas.DataFrame): The DataFrame to be grouped.

    Returns:
        pandas.DataFrame: The grouped DataFrame.
    """
    df["clas_sub"] = df["clas_sub"].apply(lambda x: ANEEL_CLASSES.get(x, "outros"))
    cols = [
        "energy_consumption",
        "energy_consumption_summer",
        "energy_consumption_winter",
        "mean_energy_consumption",
        "mean_energy_consumption_summer",
        "mean_energy_consumption_winter",
        "std_energy_consumption",
        "mean_energy_interuption_hours",
        "mean_energy_interuption_frequency",
    ]
    df = (
        group_columns(df, ["hex_col", "clas_sub"])
        .pipe(pivot_energy_columns, cols)
        .pipe(add_missing_columns, cols)
    )
    return df


def get_data(conn: DBConnection, path: str, batch: int, i: int) -> pd.DataFrame:
    """
    Retrieves data from the specified database connection based on the given parameters.

    Args:
        conn (DBConnection): The database connection object.
        path (str): The path of the table in the database.
        batch (int): The number of rows to retrieve.
        i (int): The starting row index.

    Returns:
        pd.DataFrame: The retrieved data as a pandas DataFrame.
    """
    rows_id = range(i, i + batch)
    query = f"""
    SELECT * FROM {path} WHERE row_id IN {tuple(rows_id)}
    """
    return conn.query_database(query)


@save_parquet_decorator(
    "silver", ANEEL_SILVER_CONTRACTS["aneel_hexagon"], save_pq=False
)
def save_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Save the DataFrame to the database.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
    """
    return df.reset_index()


def process_batch(start: int, batch: int, path: str) -> pd.DataFrame:
    """
    Process a single batch of data.

    Args:
        start (int): The starting index for the batch.
        batch (int): The size of the batch.
        path (str): The database table path.

    Returns:
        pd.DataFrame: The processed DataFrame.
    """
    conn = DBConnection("silver")
    df = (
        get_data(conn, path, batch, start)
        .pipe(calculate_h3_index)
        .pipe(calculate_energy_consumption)
        .pipe(calculate_energy_interuption_hours)
        .pipe(calculate_energy_interuption_frequency)
        .pipe(group_by_hexagon)
    )
    assert df.index.is_unique, "Duplicated hex cols"
    return df


def main() -> None:
    """
    Process the hex IDs for ANEEL data.

    This function retrieves data from the ANEEL database, performs calculations on the data,
    and groups the data by hexagon.
    """
    conn = DBConnection("silver")
    path = ".".join(
        [
            ANEEL_SILVER_CONTRACTS["final_aneel"]["schema"],
            ANEEL_SILVER_CONTRACTS["final_aneel"]["tableName"],
        ]
    )
    table_length = get_table_lenth(conn, path)
    batch = int(7e5)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_batch, i, batch, path)
            for i in range(0, table_length, batch)
        ]

        for future in tqdm(
            concurrent.futures.as_completed(futures),
            total=len(futures),
            desc="Processing batches",
        ):

            try:
                df = future.result()
                _ = save_data(df)
            except Exception as e:
                print(f"Error processing batch: {e}")


if __name__ == "__main__":
    main()
