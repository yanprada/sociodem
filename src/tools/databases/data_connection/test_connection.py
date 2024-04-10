"""
This module contains a test case for creating a database engine.

The test case checks if a database engine can be successfully created 
using the DBConnectionHandler class.

Usage:
    To run this test case, use the command: pytest src/tools/databases/test_connection.py

"""

import pytest
from src.tools.databases.data_connection.connection import DBConnectionHandler


@pytest.mark.skip(reason="Sensitive test")
def test_create_database_engine(database: str = "bronze"):
    """
    Test the creation of a database engine.

    Args:
        database (str): The name of the database.
    """

    db_connection_handle = DBConnectionHandler(database)
    engine = db_connection_handle.get_engine()

    assert engine is not None
