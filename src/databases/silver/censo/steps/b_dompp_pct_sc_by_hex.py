"""
Creates a table with the percentage of different types of domiciles per hexagon per sector.
"""

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.common import get_db_path

from src.tools.utils.execution_manager import ExecutionManager
from src.databases.silver.censo.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)

manager.update_status("running_step_2")


CONTRACT_CENSO_SILVER = execution_parameters["data_contracts"][1]


def create_pct_dompp_hex_sc() -> None:
    """
    Creates a table with the percentage of different types of domiciles per hexagon per sector.
    """

    conn = DBConnection("silver")
    path = get_db_path(CONTRACT_CENSO_SILVER["dompp_2022"])
    new_path = get_db_path(CONTRACT_CENSO_SILVER["dompp_pct_2022"])
    query = f"""
    WITH dompp_data AS (
        SELECT 
            hex_id,
            cd_setor,
            dompp_total_domicilio_coletivo,
            dompp_total_domicilio_particular,
            dompp_total_edificio_em_construcao,
            dompp_total_estabelecimento_agropecuario,
            dompp_total_estabelecimento_ensino,
            dompp_total_estabelecimento_religioso,
            dompp_total_estabelecimento_saude,
            dompp_total_outros_estabelecimentos
        FROM {path}
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
            dompp_data.hex_id,
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
    manager.update_status("finished_step_2")
    manager.update_last_run()
