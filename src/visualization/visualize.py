"""
Module: visualize.py

This module provides functions for visualizing cities. It retrieves data from a database, 
performs weighted kring smoothing on the data, merges the smoothed dataframes, 
adds a geometry column, and saves the resulting dataframe as a CSV file.

Functions:
- main(): Main function for visualizing cities. It iterates over the UFMUNCODS dictionary, 
retrieves data from the database, performs weighted kring smoothing, merges the smoothed dataframes,
 adds a geometry column, and saves the resulting dataframe as a CSV file.


"""

import os
from functools import reduce
import pandas as pd
from tqdm import tqdm

from src.tools.utils.h3 import get_h3_geom, weighted_kring_smoothing
from src.tools.databases.data_connection.connection import DBConnection

UFMUNCODS = {
    "campinas": 3509502,
    "presidente_prudente": 3541406,
    "piracicaba": 3538709,
    "ribeirao_preto": 3543402,
    "sao_paulo": 3550305,
    "rio_de_janeiro": 3304557,
    "belo_horizonte": 3106200,
    "brasilia": 5300108,
    "curitiba": 4106902,
    "florianopolis": 4205407,
    "porto_alegre": 4314902,
    "barra_do_turvo": 3505401,
    "dourados": 5003702,
    "altamira": 1500602,
    "sao_gabriel_da_cachoeira": 1303809,
    "sao_felix_do_xingu": 1507300,
    "porto_velho": 1100205,
}

COEF = [1, 0.8, 0.4, 0.15, 0.05]

COLS = [
    "energy_consumption_residencial_urbano",
    "energy_consumption_residencial_rural",
    "energy_consumption_comercial",
    "energy_consumption_industrial",
    "energy_consumption_agropecuaria",
    "mean_energy_consumption_residencial_urbano",
    "mean_energy_consumption_residencial_rural",
    "mean_energy_consumption_comercial",
    "mean_energy_consumption_industrial",
    "mean_energy_consumption_agropecuaria",
]

COLS_STR = ", ".join(COLS)

BASE_PATH = "src/visualization/aneel"
os.makedirs(BASE_PATH, exist_ok=True)


def main():
    """
    Main function for visualizing cities.
    This function iterates over the UFMUNCODS dictionary,
    retrieves data from the database using a SQL query,
    performs weighted kring smoothing on the retrieved data, merges the smoothed dataframes,
    adds a geometry column to the merged dataframe, and saves the resulting dataframe as a CSV file.
    Parameters:
    None
    Returns:
    None
    """
    for city, code in tqdm(UFMUNCODS.items(), desc="Visualizing cities"):
        query = f"""select hex_col, {COLS_STR}
                from infrastructure.aneel_hexagon_grp ahg  
                where mun = '{code}'"""
        conn = DBConnection("silver")
        df = conn.query_database(query)
        dfs = []
        for col in COLS:
            dfs.append(weighted_kring_smoothing(df, "hex_col", col, COEF))
        merged_df = reduce(
            lambda left, right: pd.merge(left, right, on="hex_col", how="inner"), dfs
        )
        merged_df["geometry"] = merged_df["hex_col"].apply(get_h3_geom)
        merged_df.to_csv(f"{BASE_PATH}/{city}.csv", index=False)
