"""
This module contains functions for processing ANEEL data with fixes.

The main function establishes a connection to the 'silver' database and calls 
the 'create_unique_aneel_pncon' function.
"""

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.common import get_db_path

from src.tools.utils.execution_manager import ExecutionManager
from src.databases.silver.aneel.config import EXECUTION_ID, BASE_PARAMS
from config.run_mode import DEBUG

manager = ExecutionManager(BASE_PARAMS)
execution_parameters = manager.get_execution_details(EXECUTION_ID, DEBUG)
manager.update_status("running_step_3")

ANEEL_SILVER_CONTRACTS = execution_parameters["data_contracts"][1]


def create_unique_aneel_pncon(conn: DBConnection) -> None:
    """
    Create a table with unique hex IDs from the existing mapbiomas table.

    Args:
        conn (DBConnection): The database connection object.
    """
    query = f"""
    SELECT 
        a.row_id,    
        atj_counts.repetition_count,
        {', '.join([f"COALESCE(atj_counts.ene_{i:02d}, 0) - ((COALESCE(atj_counts.repetition_count, 0) - 1) * COALESCE(a.ene_{i:02d}, 0)) as ene_{i:02d}" for i in range(1, 13)])},
        {', '.join([f"COALESCE(atj_counts.dic_{i:02d}, 0) - ((COALESCE(atj_counts.repetition_count, 0) - 1) * COALESCE(a.dic_{i:02d}, 0)) as dic_{i:02d}" for i in range(1, 13)])},
        {', '.join([f"COALESCE(atj_counts.fic_{i:02d}, 0) - ((COALESCE(atj_counts.repetition_count, 0) - 1) * COALESCE(a.fic_{i:02d}, 0)) as fic_{i:02d}" for i in range(1, 13)])},
        a.mun,
        a.brr,
        a.cep,
        a.clas_sub,
        a.cnae,
        a.gru_tar,
        a.sit_ativ,
        a.dat_con,
        a.are_loc,
        a.mat,
        a.geometry
    FROM 
        (SELECT 
            row_id,    
            COUNT(*) AS repetition_count,
            {', '.join([f"SUM(ene_{i:02d}) as ene_{i:02d}" for i in range(1, 13)])},
            {', '.join([f"SUM(dic_{i:02d}) as dic_{i:02d}" for i in range(1, 13)])},
            {', '.join([f"SUM(fic_{i:02d}) as fic_{i:02d}" for i in range(1, 13)])}
        FROM 
            infrastructure.aneel_temp_join
        GROUP BY 
            row_id
        ) as atj_counts
        FULL OUTER JOIN 
        infrastructure.aneel a ON atj_counts.row_id = a.row_id
    """
    contract_aneel = ANEEL_SILVER_CONTRACTS["final_aneel"]
    new_path = get_db_path(contract_aneel)
    schema = contract_aneel["schema"]
    table = contract_aneel["tableName"]
    conn.create_table_from_sql(query, new_path)
    conn.create_pk(schema, table, "row_id")


def main():
    """
    This is the main function that performs the processing of ANEEL data with fixes.

    It establishes a connection to the 'silver' database and calls the
    'create_unique_aneel_pncon' function.
    """
    conn = DBConnection("silver")
    create_unique_aneel_pncon(conn)
    manager.update_status("finished_step_3")
    manager.update_last_run()
