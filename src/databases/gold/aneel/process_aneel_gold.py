"""
This module contains functions to group a DataFrame by 'hex_col' 
and perform aggregations on other columns.

The main function, `group_by_hex_ids`, takes a DataFrame as input, 
groups it by the 'hex_col' column, and performs aggregations on other columns. 
The resulting grouped and aggregated DataFrame is returned.

The `save_data` function saves the given DataFrame to a file or database.

The `main` function is the entry point of the program. It connects to the 
'silver' database, retrieves data from a specific table, groups the data 
by hex IDs, and performs further processing.
"""

import pandas as pd
import dask.dataframe as dd
from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.save import save_parquet_decorator

ANEEL_SILVER_CONTRACTS = get_aneel_contracts("silver")

ANEEL_GOLD_CONTRACTS = get_aneel_contracts("gold")


def most_common_value(x):
    """Return the most common value in the Series."""
    return x.mode().iloc[0] if not x.mode().empty else None


def join_non_null_values(x):
    """Join non-null values in the Series into a comma-separated string."""
    return ",".join(x.dropna().astype(str))


def group_by_hex_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group the DataFrame by 'hex_col' and perform aggregations on other columns using
    Dask for parallel processing.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The grouped and aggregated DataFrame.
    """

    agg_funcs = {
        "energy_consumption_agropecuaria": "sum",
        "energy_consumption_comercial": "sum",
        "energy_consumption_condominios": "sum",
        "energy_consumption_industrial": "sum",
        "energy_consumption_outros": "sum",
        "energy_consumption_poder_publico": "sum",
        "energy_consumption_residencial_rural": "sum",
        "energy_consumption_residencial_urbano": "sum",
        "energy_consumption_residencial_urbano_baixa_renda": "sum",
        "energy_consumption_rodovias": "sum",
        "energy_consumption_servicos_publicos": "sum",
        "energy_consumption_summer_agropecuaria": "sum",
        "energy_consumption_summer_comercial": "sum",
        "energy_consumption_summer_condominios": "sum",
        "energy_consumption_summer_industrial": "sum",
        "energy_consumption_summer_outros": "sum",
        "energy_consumption_summer_poder_publico": "sum",
        "energy_consumption_summer_residencial_rural": "sum",
        "energy_consumption_summer_residencial_urbano": "sum",
        "energy_consumption_summer_residencial_urbano_baixa_renda": "sum",
        "energy_consumption_summer_rodovias": "sum",
        "energy_consumption_summer_servicos_publicos": "sum",
        "energy_consumption_winter_agropecuaria": "sum",
        "energy_consumption_winter_comercial": "sum",
        "energy_consumption_winter_condominios": "sum",
        "energy_consumption_winter_industrial": "sum",
        "energy_consumption_winter_outros": "sum",
        "energy_consumption_winter_poder_publico": "sum",
        "energy_consumption_winter_residencial_rural": "sum",
        "energy_consumption_winter_residencial_urbano": "sum",
        "energy_consumption_winter_residencial_urbano_baixa_renda": "sum",
        "energy_consumption_winter_rodovias": "sum",
        "energy_consumption_winter_servicos_publicos": "sum",
        "mean_energy_consumption_agropecuaria": "mean",
        "mean_energy_consumption_comercial": "mean",
        "mean_energy_consumption_condominios": "mean",
        "mean_energy_consumption_industrial": "mean",
        "mean_energy_consumption_outros": "mean",
        "mean_energy_consumption_poder_publico": "mean",
        "mean_energy_consumption_residencial_rural": "mean",
        "mean_energy_consumption_residencial_urbano": "mean",
        "mean_energy_consumption_residencial_urbano_baixa_renda": "mean",
        "mean_energy_consumption_rodovias": "mean",
        "mean_energy_consumption_servicos_publicos": "mean",
        "mean_energy_consumption_summer_agropecuaria": "mean",
        "mean_energy_consumption_summer_comercial": "mean",
        "mean_energy_consumption_summer_condominios": "mean",
        "mean_energy_consumption_summer_industrial": "mean",
        "mean_energy_consumption_summer_outros": "mean",
        "mean_energy_consumption_summer_poder_publico": "mean",
        "mean_energy_consumption_summer_residencial_rural": "mean",
        "mean_energy_consumption_summer_residencial_urbano": "mean",
        "mean_energy_consumption_summer_residencial_urbano_baixa_renda": "mean",
        "mean_energy_consumption_summer_rodovias": "mean",
        "mean_energy_consumption_summer_servicos_publicos": "mean",
        "mean_energy_consumption_winter_agropecuaria": "mean",
        "mean_energy_consumption_winter_comercial": "mean",
        "mean_energy_consumption_winter_condominios": "mean",
        "mean_energy_consumption_winter_industrial": "mean",
        "mean_energy_consumption_winter_outros": "mean",
        "mean_energy_consumption_winter_poder_publico": "mean",
        "mean_energy_consumption_winter_residencial_rural": "mean",
        "mean_energy_consumption_winter_residencial_urbano": "mean",
        "mean_energy_consumption_winter_residencial_urbano_baixa_renda": "mean",
        "mean_energy_consumption_winter_rodovias": "mean",
        "mean_energy_consumption_winter_servicos_publicos": "mean",
        "std_energy_consumption_agropecuaria": "mean",
        "std_energy_consumption_comercial": "mean",
        "std_energy_consumption_condominios": "mean",
        "std_energy_consumption_industrial": "mean",
        "std_energy_consumption_outros": "mean",
        "std_energy_consumption_poder_publico": "mean",
        "std_energy_consumption_residencial_rural": "mean",
        "std_energy_consumption_residencial_urbano": "mean",
        "std_energy_consumption_residencial_urbano_baixa_renda": "mean",
        "std_energy_consumption_rodovias": "mean",
        "std_energy_consumption_servicos_publicos": "mean",
        "mean_energy_interuption_hours_agropecuaria": "mean",
        "mean_energy_interuption_hours_comercial": "mean",
        "mean_energy_interuption_hours_condominios": "mean",
        "mean_energy_interuption_hours_industrial": "mean",
        "mean_energy_interuption_hours_outros": "mean",
        "mean_energy_interuption_hours_poder_publico": "mean",
        "mean_energy_interuption_hours_residencial_rural": "mean",
        "mean_energy_interuption_hours_residencial_urbano": "mean",
        "mean_energy_interuption_hours_residencial_urbano_baixa_renda": "mean",
        "mean_energy_interuption_hours_rodovias": "mean",
        "mean_energy_interuption_hours_servicos_publicos": "mean",
        "mean_energy_interuption_frequency_agropecuaria": "mean",
        "mean_energy_interuption_frequency_comercial": "mean",
        "mean_energy_interuption_frequency_condominios": "mean",
        "mean_energy_interuption_frequency_industrial": "mean",
        "mean_energy_interuption_frequency_outros": "mean",
        "mean_energy_interuption_frequency_poder_publico": "mean",
        "mean_energy_interuption_frequency_residencial_rural": "mean",
        "mean_energy_interuption_frequency_residencial_urbano": "mean",
        "mean_energy_interuption_frequency_residencial_urbano_baixa_rend": "mean",
        "mean_energy_interuption_frequency_rodovias": "mean",
        "mean_energy_interuption_frequency_servicos_publicos": "mean",
    }

    mode_cols = ["mun", "brr", "cep", "mat", "are_loc", "sit_ativ"]
    for col in mode_cols:
        agg_funcs[col] = lambda x: x.mode().iloc[0] if not x.mode().empty else None

    str_join_cols = ["cnae", "dat_con"]
    for col in str_join_cols:
        agg_funcs[col] = lambda x: ",".join(x.dropna().astype(str))

    value_counts_dict = {}
    for col in mode_cols:
        value_counts_dict[col] = df[col].value_counts()

    df = dd.from_pandas(df, npartitions=4)
    df = df.groupby("hex_col").agg(agg_funcs).compute()
    for col in mode_cols:
        df[col] = df[col].map(value_counts_dict[col].idxmax)

    return df.reset_index()


@save_parquet_decorator("gold", ANEEL_GOLD_CONTRACTS["aneel"])
def save_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Saves the given DataFrame to a file or database.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.

    Returns:
        pd.DataFrame: The input DataFrame.
    """
    return df


def main():
    """
    Entry point of the program.

    This function connects to the 'silver' database, retrieves data from a specific table,
    groups the data by hex IDs, and performs further processing.
    """

    conn = DBConnection("silver")
    path = ".".join(
        [
            ANEEL_SILVER_CONTRACTS["aneel_hexagon"]["schema"],
            ANEEL_SILVER_CONTRACTS["aneel_hexagon"]["tableName"],
        ]
    )
    query = f"""
    SELECT * FROM {path}
    """
    df = conn.query_database(query).pipe(group_by_hex_ids).pipe(save_data)
    del df
