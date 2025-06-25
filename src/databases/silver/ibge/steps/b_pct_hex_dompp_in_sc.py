"""
Creates a table with the percentage of different types of domiciles per hexagon per sector.
"""

import os

from src.tools.managers.db_connector import DBConnection
from src.tools.utils.common import get_db_path

from src.databases.silver.ibge.config import manager, CONTRACTS_SILVER

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


def create_pct_dompp_hex_sc() -> None:
    """
    Creates a table with the percentage of different types of domiciles per hexagon per sector.
    """

    conn = DBConnection("silver")
    path = get_db_path(CONTRACTS_SILVER["dompp_per_hex_sc_2022"])
    new_path = get_db_path(CONTRACTS_SILVER["pct_hex_dompp_in_sc_2022"])
    query = f"""
    WITH dompp_data AS (
        SELECT 
            hex_col,
            cd_setor,
            SUM(dompp_total_domicilio_coletivo) as dompp_total_domicilio_coletivo,
            SUM(dompp_total_domicilio_particular) as dompp_total_domicilio_particular,
            SUM(dompp_total_edificio_em_construcao) as dompp_total_edificio_em_construcao,
            SUM(dompp_total_estabelecimento_agropecuario) as dompp_total_estabelecimento_agropecuario,
            SUM(dompp_total_estabelecimento_ensino) as dompp_total_estabelecimento_ensino,
            SUM(dompp_total_estabelecimento_religioso) as dompp_total_estabelecimento_religioso,
            SUM(dompp_total_estabelecimento_saude) as dompp_total_estabelecimento_saude,
            SUM(dompp_total_outros_estabelecimentos) as dompp_total_outros_estabelecimentos
        FROM {path}
        GROUP BY hex_col, cd_setor
    ),
    grouped_data AS (
        SELECT 
            cd_setor, 
            SUM(dompp_total_domicilio_coletivo) AS total_domicilio_coletivo,
            SUM(dompp_total_domicilio_particular) AS total_domicilio_particular,
            SUM(dompp_total_edificio_em_construcao) AS total_edificio_em_construcao,
            SUM(dompp_total_estabelecimento_agropecuario) AS total_estabelecimento_agropecuario,
            SUM(dompp_total_estabelecimento_ensino) AS total_estabelecimento_ensino,
            SUM(dompp_total_estabelecimento_religioso) AS total_estabelecimento_religioso,
            SUM(dompp_total_estabelecimento_saude) AS total_estabelecimento_saude,
            SUM(dompp_total_outros_estabelecimentos) AS total_outros_estabelecimentos
        FROM dompp_data
        GROUP BY cd_setor
    ),
    merged_data AS (
        SELECT 
            dompp_data.hex_col,
            dompp_data.cd_setor,
            COALESCE(dompp_data.dompp_total_domicilio_coletivo / NULLIF(grouped_data.total_domicilio_coletivo, 0), 0) AS pct_dompp_total_domicilio_coletivo,
            COALESCE(dompp_data.dompp_total_domicilio_particular / NULLIF(grouped_data.total_domicilio_particular, 0), 0) AS pct_dompp_total_domicilio_particular,
            COALESCE(dompp_data.dompp_total_edificio_em_construcao / NULLIF(grouped_data.total_edificio_em_construcao, 0), 0) AS pct_dompp_total_edificio_em_construcao,
            COALESCE(dompp_data.dompp_total_estabelecimento_agropecuario / NULLIF(grouped_data.total_estabelecimento_agropecuario, 0), 0) AS pct_dompp_total_estabelecimento_agropecuario,
            COALESCE(dompp_data.dompp_total_estabelecimento_ensino / NULLIF(grouped_data.total_estabelecimento_ensino, 0), 0) AS pct_dompp_total_estabelecimento_ensino,
            COALESCE(dompp_data.dompp_total_estabelecimento_religioso / NULLIF(grouped_data.total_estabelecimento_religioso, 0), 0) AS pct_dompp_total_estabelecimento_religioso,
            COALESCE(dompp_data.dompp_total_estabelecimento_saude / NULLIF(grouped_data.total_estabelecimento_saude, 0), 0) AS pct_dompp_total_estabelecimento_saude,
            COALESCE(dompp_data.dompp_total_outros_estabelecimentos / NULLIF(grouped_data.total_outros_estabelecimentos, 0), 0) AS pct_dompp_total_outros_estabelecimentos
        FROM dompp_data 
        LEFT JOIN grouped_data
        ON dompp_data.cd_setor = grouped_data.cd_setor
    )
    SELECT * FROM merged_data;
    """

    conn.create_table_from_sql(query, new_path)


def main():
    """
    Call function create_pct_dompp_hex_sc, that creates a table with
    the percentage of different types of domiciles per hexagon per sector.
    """
    create_pct_dompp_hex_sc()
