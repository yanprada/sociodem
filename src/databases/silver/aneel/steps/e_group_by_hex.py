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

from src.tools.data_contract.aneel_data_contract import get_aneel_contracts
from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.common import get_db_path

ANEEL_SILVER_CONTRACTS = get_aneel_contracts("silver")


def create_grouped_table() -> None:
    """
    Creates a new table with joined data based on hex_col and value
    columns from the Google and OMF sources.
    """
    conn = DBConnection("silver")
    contract_aneel = ANEEL_SILVER_CONTRACTS["aneel_hexagon"]
    path = get_db_path(contract_aneel)
    contract_aneel_hex = ANEEL_SILVER_CONTRACTS["aneel_hexagon_grouped"]
    new_path = get_db_path(contract_aneel_hex)
    creation_query = f"""
    WITH mode_cols AS (
    SELECT 
        hex_col,
        mun,
        brr,
        cep,
        mat,
        are_loc,
        sit_ativ,
        cnae,
        dat_con,
        ROW_NUMBER() OVER(PARTITION BY hex_col ORDER BY (
            SELECT NULL)) AS rn
    FROM {path}
    ),
    mode_agg AS (
        SELECT 
            hex_col,
            mun,
            brr,
            cep,
            mat,
            are_loc,
            sit_ativ,
            cnae,
            dat_con
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
        SUM(t.energy_consumption_summer_agropecuaria) AS energy_consumption_summer_agropecuaria,
        SUM(t.energy_consumption_summer_comercial) AS energy_consumption_summer_comercial,
        SUM(t.energy_consumption_summer_condominios) AS energy_consumption_summer_condominios,
        SUM(t.energy_consumption_summer_industrial) AS energy_consumption_summer_industrial,
        SUM(t.energy_consumption_summer_outros) AS energy_consumption_summer_outros,
        SUM(t.energy_consumption_summer_poder_publico) AS energy_consumption_summer_poder_publico,
        SUM(t.energy_consumption_summer_residencial_rural) AS energy_consumption_summer_residencial_rural,
        SUM(t.energy_consumption_summer_residencial_urbano) AS energy_consumption_summer_residencial_urbano,
        SUM(t.energy_consumption_summer_residencial_urbano_baixa_renda) AS energy_consumption_summer_residencial_urbano_baixa_renda,
        SUM(t.energy_consumption_summer_rodovias) AS energy_consumption_summer_rodovias,
        SUM(t.energy_consumption_summer_servicos_publicos) AS energy_consumption_summer_servicos_publicos,
        SUM(t.energy_consumption_winter_agropecuaria) AS energy_consumption_winter_agropecuaria,
        SUM(t.energy_consumption_winter_comercial) AS energy_consumption_winter_comercial,
        SUM(t.energy_consumption_winter_condominios) AS energy_consumption_winter_condominios,
        SUM(t.energy_consumption_winter_industrial) AS energy_consumption_winter_industrial,
        SUM(t.energy_consumption_winter_outros) AS energy_consumption_winter_outros,
        SUM(t.energy_consumption_winter_poder_publico) AS energy_consumption_winter_poder_publico,
        SUM(t.energy_consumption_winter_residencial_rural) AS energy_consumption_winter_residencial_rural,
        SUM(t.energy_consumption_winter_residencial_urbano) AS energy_consumption_winter_residencial_urbano,
        SUM(t.energy_consumption_winter_residencial_urbano_baixa_renda) AS energy_consumption_winter_residencial_urbano_baixa_renda,
        SUM(t.energy_consumption_winter_rodovias) AS energy_consumption_winter_rodovias,
        SUM(t.energy_consumption_winter_servicos_publicos) AS energy_consumption_winter_servicos_publicos,
        AVG(t.mean_energy_consumption_agropecuaria) AS mean_energy_consumption_agropecuaria,
        AVG(t.mean_energy_consumption_comercial) AS mean_energy_consumption_comercial,
        AVG(t.mean_energy_consumption_condominios) AS mean_energy_consumption_condominios,
        AVG(t.mean_energy_consumption_industrial) AS mean_energy_consumption_industrial,
        AVG(t.mean_energy_consumption_outros) AS mean_energy_consumption_outros,
        AVG(t.mean_energy_consumption_poder_publico) AS mean_energy_consumption_poder_publico,
        AVG(t.mean_energy_consumption_residencial_rural) AS mean_energy_consumption_residencial_rural,
        AVG(t.mean_energy_consumption_residencial_urbano) AS mean_energy_consumption_residencial_urbano,
        AVG(t.mean_energy_consumption_residencial_urbano_baixa_renda) AS mean_energy_consumption_residencial_urbano_baixa_renda,
        AVG(t.mean_energy_consumption_rodovias) AS mean_energy_consumption_rodovias,
        AVG(t.mean_energy_consumption_servicos_publicos) AS mean_energy_consumption_servicos_publicos,
        AVG(t.mean_energy_consumption_summer_agropecuaria) AS mean_energy_consumption_summer_agropecuaria,
        AVG(t.mean_energy_consumption_summer_comercial) AS mean_energy_consumption_summer_comercial,
        AVG(t.mean_energy_consumption_summer_condominios) AS mean_energy_consumption_summer_condominios,
        AVG(t.mean_energy_consumption_summer_industrial) AS mean_energy_consumption_summer_industrial,
        AVG(t.mean_energy_consumption_summer_outros) AS mean_energy_consumption_summer_outros,
        AVG(t.mean_energy_consumption_summer_poder_publico) AS mean_energy_consumption_summer_poder_publico,
        AVG(t.mean_energy_consumption_summer_residencial_rural) AS mean_energy_consumption_summer_residencial_rural,
        AVG(t.mean_energy_consumption_summer_residencial_urbano) AS mean_energy_consumption_summer_residencial_urbano,
        AVG(t.mean_energy_consumption_summer_residencial_urbano_baixa_renda) AS mean_energy_consumption_summer_residencial_urbano_baixa_renda,
        AVG(t.mean_energy_consumption_summer_rodovias) AS mean_energy_consumption_summer_rodovias,
        AVG(t.mean_energy_consumption_summer_servicos_publicos) AS mean_energy_consumption_summer_servicos_publicos,
        AVG(t.mean_energy_consumption_winter_agropecuaria) AS mean_energy_consumption_winter_agropecuaria,
        AVG(t.mean_energy_consumption_winter_comercial) AS mean_energy_consumption_winter_comercial,
        AVG(t.mean_energy_consumption_winter_condominios) AS mean_energy_consumption_winter_condominios,
        AVG(t.mean_energy_consumption_winter_industrial) AS mean_energy_consumption_winter_industrial,
        AVG(t.mean_energy_consumption_winter_outros) AS mean_energy_consumption_winter_outros,
        AVG(t.mean_energy_consumption_winter_poder_publico) AS mean_energy_consumption_winter_poder_publico,
        AVG(t.mean_energy_consumption_winter_residencial_rural) AS mean_energy_consumption_winter_residencial_rural,
        AVG(t.mean_energy_consumption_winter_residencial_urbano) AS mean_energy_consumption_winter_residencial_urbano,
        AVG(t.mean_energy_consumption_winter_residencial_urbano_baixa_renda) AS mean_energy_consumption_winter_residencial_urbano_baixa_renda,
        AVG(t.mean_energy_consumption_winter_rodovias) AS mean_energy_consumption_winter_rodovias,
        AVG(t.mean_energy_consumption_winter_servicos_publicos) AS mean_energy_consumption_winter_servicos_publicos,
        AVG(t.std_energy_consumption_agropecuaria) AS std_energy_consumption_agropecuaria,
        AVG(t.std_energy_consumption_comercial) AS std_energy_consumption_comercial,
        AVG(t.std_energy_consumption_condominios) AS std_energy_consumption_condominios,
        AVG(t.std_energy_consumption_industrial) AS std_energy_consumption_industrial,
        AVG(t.std_energy_consumption_outros) AS std_energy_consumption_outros,
        AVG(t.std_energy_consumption_poder_publico) AS std_energy_consumption_poder_publico,
        AVG(t.std_energy_consumption_residencial_rural) AS std_energy_consumption_residencial_rural,
        AVG(t.std_energy_consumption_residencial_urbano) AS std_energy_consumption_residencial_urbano,
        AVG(t.std_energy_consumption_residencial_urbano_baixa_renda) AS std_energy_consumption_residencial_urbano_baixa_renda,
        AVG(t.std_energy_consumption_rodovias) AS std_energy_consumption_rodovias,
        AVG(t.std_energy_consumption_servicos_publicos) AS std_energy_consumption_servicos_publicos,
        AVG(t.mean_energy_interuption_hours_agropecuaria) AS mean_energy_interuption_hours_agropecuaria,
        AVG(t.mean_energy_interuption_hours_comercial) AS mean_energy_interuption_hours_comercial,
        AVG(t.mean_energy_interuption_hours_condominios) AS mean_energy_interuption_hours_condominios,
        AVG(t.mean_energy_interuption_hours_industrial) AS mean_energy_interuption_hours_industrial,
        AVG(t.mean_energy_interuption_hours_outros) AS mean_energy_interuption_hours_outros,
        AVG(t.mean_energy_interuption_hours_poder_publico) AS mean_energy_interuption_hours_poder_publico,
        AVG(t.mean_energy_interuption_hours_residencial_rural) AS mean_energy_interuption_hours_residencial_rural,
        AVG(t.mean_energy_interuption_hours_residencial_urbano) AS mean_energy_interuption_hours_residencial_urbano,
        AVG(t.mean_energy_interuption_hours_residencial_urbano_baixa_renda) AS mean_energy_interuption_hours_residencial_urbano_baixa_renda,
        AVG(t.mean_energy_interuption_hours_rodovias) AS mean_energy_interuption_hours_rodovias,
        AVG(t.mean_energy_interuption_hours_servicos_publicos) AS mean_energy_interuption_hours_servicos_publicos,
        AVG(t.mean_energy_interuption_frequency_agropecuaria) AS mean_energy_interuption_frequency_agropecuaria,
        AVG(t.mean_energy_interuption_frequency_comercial) AS mean_energy_interuption_frequency_comercial,
        AVG(t.mean_energy_interuption_frequency_condominios) AS mean_energy_interuption_frequency_condominios,
        AVG(t.mean_energy_interuption_frequency_industrial) AS mean_energy_interuption_frequency_industrial,
        AVG(t.mean_energy_interuption_frequency_outros) AS mean_energy_interuption_frequency_outros,
        AVG(t.mean_energy_interuption_frequency_poder_publico) AS mean_energy_interuption_frequency_poder_publico,
        AVG(t.mean_energy_interuption_frequency_residencial_rural) AS mean_energy_interuption_frequency_residencial_rural,
        AVG(t.mean_energy_interuption_frequency_residencial_urbano) AS mean_energy_interuption_frequency_residencial_urbano,
        AVG(t.mean_energy_interuption_frequency_residencial_urbano_baixa_renda) AS mean_energy_interuption_frequency_residencial_urbano_baixa_renda,
        AVG(t.mean_energy_interuption_frequency_rodovias) AS mean_energy_interuption_frequency_rodovias,
        AVG(t.mean_energy_interuption_frequency_servicos_publicos) AS mean_energy_interuption_frequency_servicos_publicos,
        m.mun,
        m.brr,
        m.cep,
        m.mat,
        m.are_loc,
        m.sit_ativ,
        STRING_AGG(DISTINCT m.cnae, ',') AS cnae,
        STRING_AGG(DISTINCT m.dat_con, ',') AS dat_con
    FROM {path} t
    JOIN mode_agg m ON t.hex_col = m.hex_col
    GROUP BY t.hex_col, m.mun, m.brr, m.cep, m.mat, m.are_loc, m.sit_ativ;

    """
    conn.create_table_from_sql(creation_query, new_path)


def main():
    """
    Entry point of the program.

    This function connects to the 'silver' database, retrieves data from a specific table,
    groups the data by hex IDs, and performs further processing.
    """
    create_grouped_table()
