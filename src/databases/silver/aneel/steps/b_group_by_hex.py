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

import os
from tqdm import tqdm

from src.tools.managers.db_connector import DBConnection
from src.tools.utils.common import get_db_path, write_log

from src.databases.silver.aneel.config import manager, CONTRACT_SILVER_ENERGY, YEARS

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


def create_grouped_table(year: int) -> None:
    """
    Creates a new table with joined data based on hex_col and value
    columns from the Google and OMF sources.
    The new table is saved in the 'silver' database.

    Args:
        year (int): The year for which the data is being processed.
    """
    conn = DBConnection("silver")
    contract_aneel = CONTRACT_SILVER_ENERGY[f"aneel_hexagon_{year}"]
    path = get_db_path(contract_aneel)
    contract_aneel_hex = CONTRACT_SILVER_ENERGY[f"aneel_hexagon_grp_{year}"]
    new_path = get_db_path(contract_aneel_hex)
    creation_query = f"""
    WITH mode_cols AS (
    SELECT 
        hex_col,
        company_file,
        mun,
        brr_most_frequent,
        mat,
        are_loc,
        dat_con_oldest,
        dat_con_latest,
        dat_con_most_frequent,
        ROW_NUMBER() OVER(PARTITION BY hex_col ORDER BY (
            SELECT NULL)) AS rn
    FROM {path}
    ),
    mode_agg AS (
        SELECT 
            hex_col,
            company_file,
            mun,
            brr_most_frequent,
            mat,
            are_loc,
            dat_con_oldest,
            dat_con_latest,
            dat_con_most_frequent
        FROM mode_cols
        WHERE rn = 1
    )
    SELECT 
        t.hex_col,
        SUM(t.energy_consumption_agropecuaria) AS energy_consumption_agropecuaria,
        SUM(t.energy_consumption_comercial) AS energy_consumption_comercial,
        SUM(t.energy_consumption_condominios) AS energy_consumption_condominios,
        SUM(t.energy_consumption_industrial) AS energy_consumption_industrial,
        SUM(t.energy_consumption_outros) AS energy_consumption_outros,
        SUM(t.energy_consumption_poder_publico) AS energy_consumption_poder_publico,
        SUM(t.energy_consumption_residencial_rural) AS energy_consumption_residencial_rural,
        SUM(t.energy_consumption_residencial_urbano) AS energy_consumption_residencial_urbano,
        SUM(t.energy_consumption_residencial_urbano_baixa_renda) AS energy_consumption_residencial_urbano_baixa_renda,
        SUM(t.energy_consumption_rodovias) AS energy_consumption_rodovias,
        SUM(t.energy_consumption_servicos_publicos) AS energy_consumption_servicos_publicos,
        AVG(t.mean_energy_consumption_residencial_rural) AS mean_energy_consumption_residencial_rural,
        AVG(t.mean_energy_consumption_residencial_urbano) AS mean_energy_consumption_residencial_urbano,
        AVG(t.mean_energy_consumption_residencial_urbano_baixa_renda) AS mean_energy_consumption_residencial_urbano_baixa_renda,
        AVG(t.median_energy_consumption_residencial_rural) AS median_energy_consumption_residencial_rural,
        AVG(t.median_energy_consumption_residencial_urbano) AS median_energy_consumption_residencial_urbano,
        AVG(t.median_energy_consumption_residencial_urbano_baixa_renda) AS median_energy_consumption_residencial_urbano_baixa_renda,
        m.company_file,
        m.mun,
        m.brr_most_frequent,
        m.mat,
        m.are_loc,
        m.dat_con_oldest,
        m.dat_con_latest,
        m.dat_con_most_frequent
    FROM {path} t
    JOIN mode_agg m ON t.hex_col = m.hex_col
    GROUP BY t.hex_col, m.company_file,m.mun, m.brr_most_frequent, m.mat, m.are_loc, m.dat_con_oldest, m.dat_con_latest, m.dat_con_most_frequent;

    """
    conn.create_table_from_sql(creation_query, new_path)

    write_log("Creating index for the new table.")
    schema = new_path.split(".", maxsplit=1)[0]
    table_name = new_path.split(".", maxsplit=1)[1]
    conn.create_index(schema, table_name, ["hex_col"])


def main():
    """
    Entry point of the program.

    This function connects to the 'silver' database, retrieves data from a specific table,
    groups the data by hex IDs, and performs further processing.
    """
    for year in tqdm(YEARS, desc="Processing years", unit="year"):
        create_grouped_table(year)
    manager.update_last_run()
