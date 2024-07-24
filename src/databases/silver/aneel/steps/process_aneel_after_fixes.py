"""
This module contains functions for processing ANEEL data with fixes.

The main function establishes a connection to the 'silver' database and calls 
the 'create_unique_aneel_pncon' function.
"""

from src.tools.databases.data_connection.connection import DBConnection
from src.tools.data_contract.aneel_data_contract import get_aneel_contracts

ANEEL_SILVER_CONTRACTS = get_aneel_contracts("silver")


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
    schema = ANEEL_SILVER_CONTRACTS["final_aneel"]["schema"]
    table = ANEEL_SILVER_CONTRACTS["final_aneel"]["tableName"]
    new_path = ".".join([schema, table])
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
