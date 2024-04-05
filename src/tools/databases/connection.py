"""
This module provides a class for handling database connections.

The DBConnectionHandler class encapsulates the logic for creating 
and managing a database connection using SQLAlchemy.
"""

from typing import List
from decouple import config
from sqlalchemy import create_engine, text, schema
from sqlalchemy.orm import sessionmaker
import pandas as pd
import sqlalchemy
from psycopg2.errors import UniqueViolation

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


class DBConnection(DBConnectionHandler):
    """
    A class representing a database connection.

    Args:
        database (str): The name of the database.

    Attributes:
        __engine (sqlalchemy.engine.Engine): The database engine.

    Methods:
        __create_schema: Create a schema in the database.
        __add_pk_to_table: Add a primary key to a table in the database.
        __add_fk_to_table: Add foreign keys to a table in the database.
        __save_as_postgis: Save a table as a PostGIS table in the database.
        __save_to_sql: Save a table as a SQL table in the database.
        add_table: Adds a table to the database.
        query_database: Executes a query on the database and returns the result as a DataFrame.
    """

    def __create_schema(
        self, conn: sqlalchemy.engine.Connection, schema_name: str
    ) -> None:
        """
        Create a schema in the database.

        Args:
            conn (sqlalchemy.engine.Connection): The connection to the database.
            schema_name (str): The name of the schema to be created.
        """
        if not conn.dialect.has_schema(conn, schema_name):
            conn.execute(schema.CreateSchema(schema_name))

    def __get_pk(self, contract):
        return next(
            (col["column"] for col in contract["columns"] if col["isPrimaryKey"]),
            None,
        )

    def __add_pk_to_table(
        self,
        conn: sqlalchemy.engine.Connection,
        schema_name: str,
        table_name: str,
        primary_key: str,
    ) -> None:
        """
        Add a primary key to a table in the database.

        Args:
            conn (sqlalchemy.engine.Connection): The connection to the database.
            schema_name (str): The name of the schema containing the table.
            table_name (str): The name of the table to which the primary key will be added.
        """
        result = conn.execute(
            text(
                f"""SELECT constraint_name 
                FROM information_schema.table_constraints
                WHERE table_name = '{table_name}' 
                AND constraint_type = 'PRIMARY KEY'"""
            )
        )
        if not result.fetchone():
            conn.execute(
                text(
                    f"""ALTER TABLE {schema_name}.{table_name} ADD PRIMARY KEY ({primary_key})"""
                )
            )

    def __get_fk(self, contract):
        return [
            (col["column"], col["ForeignKey"])
            for col in contract["columns"]
            if col["ForeignKey"] is not None
        ]

    def __add_fk_to_table(
        self,
        conn: sqlalchemy.engine.Connection,
        schema_name: str,
        table_name: str,
        foreign_keys: List[tuple[str, str]],
    ) -> None:
        """
        Add foreign keys to a table in the database.

        Args:
            conn (sqlalchemy.engine.Connection): The connection to the database.
            schema_name (str): The name of the schema containing the table.
            table_name (str): The name of the table to which the foreign keys will be added.
            foreign_keys (List[tuple[str, str]]): Tuple containing the column name
                                and the path of the foreging key in the database.
        """
        for fk_col, fk_path in foreign_keys:
            result = conn.execute(
                text(
                    f"""SELECT constraint_name 
                        FROM information_schema.table_constraints 
                        WHERE table_name = '{table_name}' 
                        AND constraint_type = 'FOREIGN KEY'"""
                )
            )
            if not result.fetchone():

                conn.execute(
                    text(
                        f"""ALTER TABLE {schema_name}.{table_name} 
                                ADD FOREIGN KEY ({fk_col}) 
                                REFERENCES {fk_path}"""
                    )
                )

    def __get_not_null(self, contract):
        return [col["column"] for col in contract["columns"] if col["isNullable"]]

    def __add_not_null_to_table(
        self,
        conn: sqlalchemy.engine.Connection,
        schema_name: str,
        table_name: str,
        not_null_columns: List[tuple[str, str]],
    ):

        for col in not_null_columns:
            result = conn.execute(
                text(
                    f"""SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' 
                    AND column_name = '{col}' 
                    AND is_nullable = 'NO'"""
                )
            )
            if not result.fetchone():
                conn.execute(
                    text(
                        f"""ALTER TABLE {schema_name}.{table_name} 
                        ALTER COLUMN {col} SET NOT NULL"""
                    )
                )

    def __save_as_postgis(
        self,
        table: pd.DataFrame,
        names: tuple[str, str],
        conn: sqlalchemy.engine.Connection,
        action_if_exists: str,
    ):
        """
        Save a table as a PostGIS table in the database.

        Args:
            table (pd.DataFrame): The table to be saved.
            names (tuple[str, str]): A tuple containing the schema name and table name.
            conn (sqlalchemy.engine.Connection): The connection to the database.
            action_if_exists (str): The action to take if the table already exists in the database.
        """
        schema_name, table_name = names
        try:
            table.to_postgis(
                table_name,
                conn,
                schema=schema_name,
                if_exists=action_if_exists,
                index=False,
            )
        except UniqueViolation:
            pass

    def __save_to_sql(
        self,
        table: pd.DataFrame,
        names: tuple[str, str],
        conn: sqlalchemy.engine.Connection,
        action_if_exists: str,
    ):
        """
        Save a table as a SQL table in the database.

        Args:
            table (pd.DataFrame): The table to be saved.
            names (tuple[str, str]): A tuple containing the schema name and table name.
            conn (sqlalchemy.engine.Connection): The connection to the database.
            action_if_exists (str): The action to take if the table already exists in the database.
        """
        schema_name, table_name = names
        try:
            table.to_sql(
                table_name,
                conn,
                schema=schema_name,
                if_exists=action_if_exists,
                index=False,
            )
        except UniqueViolation:
            pass

    def add_table(self, table: pd.DataFrame, contract: dict):
        """
        Adds a table to the database.

        Parameters:
        - table (pd.DataFrame): The table to be added.
        - contract (dict): A dictionary containing the database contract.

        Returns:
        None
        """
        table_name = contract["tableName"]

        primary_key = self.__get_pk(contract)
        foreign_keys = self.__get_fk(contract)
        not_null_columns = self.__get_not_null(contract)
        schema_name = contract["schema"]
        action_if_exists = contract["ifExists"]

        with self.__engine.begin() as conn:
            self.__create_schema(conn, schema_name)
            args = (table, (schema_name, table_name), conn, action_if_exists)
            if table.filter(regex="geom").shape[1] > 0:
                self.__save_as_postgis(*args)
            else:
                self.__save_to_sql(*args)

        with self.__engine.begin() as conn:
            if primary_key is not None:
                self.__add_pk_to_table(conn, schema_name, table_name, primary_key)
            if len(foreign_keys) > 0:
                self.__add_fk_to_table(conn, schema_name, table_name, foreign_keys)
            if len(not_null_columns) > 0:
                self.__add_not_null_to_table(
                    conn, schema_name, table_name, not_null_columns
                )

    def query_database(self, query: str) -> pd.DataFrame:
        """
        Executes a query on the database and returns the result as a DataFrame.

        Parameters:
            - query (str): The SQL query to be executed.

            Returns:
            pd.DataFrame: The result of the query as a DataFrame.
        """
        with self.__engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn)
        return df
