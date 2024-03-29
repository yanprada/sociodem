"""
This module provides a class for handling database connections.

The DBConnectionHandler class encapsulates the logic for creating 
and managing a database connection using SQLAlchemy.
"""

from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Database credentials
DB_USER = config("DB_USER")
DB_PASSWORD = config("DB_PASSWORD")


class DBConnectionHandler:
    """
    Class for handling database connections.

    This class provides methods for creating a database engine and managing a session.
    """

    def __init__(self, database: str) -> None:
        self.__connection_string = (
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@localhost:5432/{database}"
        )
        self.__engine = self.__create_database_engine()
        self.session = None

    def __create_database_engine(self):
        """
        Create a database engine.

        Returns:
            sqlalchemy.engine.Engine: The created database engine.
        """
        engine = create_engine(self.__connection_string)
        return engine

    def get_engine(self):
        """
        Get the database engine.

        Returns:
            sqlalchemy.engine.Engine: The database engine.
        """
        return self.__engine

    def __enter__(self):
        """
        Enter method for using the class as a context manager.

        Returns:
            DBConnectionHandler: The current instance of the DBConnectionHandler class.
        """
        session_maker = sessionmaker(bind=self.__engine)
        self.session = session_maker()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit method for using the class as a context manager.

        Args:
            exc_type (type): The type of the exception raised, if any.
            exc_val (Exception): The exception raised, if any.
            exc_tb (traceback): The traceback of the exception raised, if any.
        """
        self.session.close()
