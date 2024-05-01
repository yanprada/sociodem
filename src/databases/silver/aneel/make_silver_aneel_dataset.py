"""
This module contains functions to create and update the 'aneel_silver' database.
"""

from src.tools.databases.data_connection.connection import DBConnection


def update_ponnot_id_in_ucbt_table():
    """
    Retrieves data from the bronze database and performs some transformations.
    Updates the 'pn_con' column in the 'ucbt' table by joining it with the 'ramlig' table.
    """
    conn = DBConnection("bronze")
    df = conn.query_database(
        """ 
        SELECT * 
        FROM bronze.infrastructure.ucbt u 
        LEFT JOIN bronze.infrastructure.ramlig r 
        ON u.ramal = r.cod_id 
        WHERE u.pn_con = ' ' 
        AND r.pn_con_1 != ' ' 
        AND u.dist = r.dist
        AND u.conj = r.conj
        """
    )
    df["pn_con"] = df["pn_con_1"]
    df = df.iloc[:, :-22]
    conn.update_table(df, (df.columns[1:], df.columns), ("infrastructure", "ucbt"))


def create_aneel_silver_database():
    """
    Retrieves data from the bronze database and performs some transformations.
    Creates the 'aneel_silver' database by joining the 'ponnot' and 'ucbt' tables.
    """
    conn = DBConnection("bronze")
    df = conn.query_database(
        """
        SELECT * 
        FROM bronze.infrastructure.ponnot p 
        LEFT JOIN bronze.infrastructure.ucbt u
        ON p.cod_id = u.pn_con 
        """
    )
    df["pn_con"] = df["pn_con_1"]
    df = df.iloc[:, :-5]
    conn.update_table(df, (df.columns[1:], df.columns), ("infrastructure", "ucbt"))
