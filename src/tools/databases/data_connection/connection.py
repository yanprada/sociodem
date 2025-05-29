"""
This module provides a class for handling database connections.

The DBConnectionHandler class encapsulates the logic for creating
and managing a database connection using SQLAlchemy.
"""

import warnings
import gc
from typing import List, Tuple, Union, Optional
from decouple import config
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient
import pandas as pd
import geopandas as gpd
import numpy as np
import sqlalchemy
from tqdm import tqdm
from pyspark.sql import SparkSession

from src.tools.utils.constants import CRS_GLOBAL


warnings.filterwarnings("ignore")

# Database credentials
DB_USER = config("DB_USER")
DB_PASSWORD = config("DB_PASSWORD")


class MongoDBConnection:
    """
    Represents a MongoDB collection.
    Attributes:
        collection (pymongo.collection.Collection): The MongoDB collection object.
    Methods:
        find(query: dict): Finds documents in the collection that match the specified query.
        Args:
            query (dict): The query to filter documents.
        Returns:
            pymongo.cursor.Cursor: A cursor object containing the matching documents.
        ...
        insert_one(document: dict): Inserts a document into the collection.
        Args:
            document (dict): The document to insert.
        Returns:
            None
        ...
        update_one(filter: dict, update: dict): Updates a single document in the collection.
        Args:
            filter (dict): The filter to select the document to update.
            update (dict): The update operation to perform on the document.
        Returns:
            None
        ...
    """

    def __init__(self, database: str, collection: str):
        self.mongo_url = "mongodb://localhost:27017"
        self.client = MongoClient(self.mongo_url)
        self.database = self.client[database]
        self.collection = self.database[collection]

    def test_connection(self):
        """
        Test the connection to MongoDB server.
        Returns:
            bool: True if the connection is successful, False otherwise.
        """

        try:
            self.client.server_info()
            return True
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
            return False

    def get_database(self):
        """
        Returns the database associated with the current instance.
        Returns:
            The database object.
        """
        return self.database

    def get_collection(self):
        """
        Returns the collection associated with the current instance.
        :return: The collection object.
        """

        return self.collection


class PySparkConnection:
    """
    A class representing a PySpark connection to a PostgreSQL database.

    Attributes:
        jdbc_url (str): The JDBC URL for connecting to the database.
        properties (dict): The properties for the JDBC connection.
        spark (pyspark.sql.SparkSession): The SparkSession object for interacting with the database.

    Methods:
        connect(): Connects to the database using SparkSession.
        close(): Stops the SparkSession.
        query(table: str, table_name: str): Executes a SQL query on the specified
                                            table and creates a temporary view.

    """

    def __init__(self, database: str) -> None:
        self.jdbc_url = f"jdbc:postgresql://localhost:5432/{database}"
        self.properties = {
            "user": DB_USER,
            "password": DB_PASSWORD,
            "driver": "org.postgresql.Driver",
        }
        self.spark = None  # Armazenar a sessão do Spark como atributo da classe

    def connect(self):
        """
        Connects to the database using SparkSession.

        If the SparkSession is already initialized, it will be stopped and
        a new one will be created.
        """
        if self.spark is not None:
            self.spark.stop()
        self.spark = (
            SparkSession.builder.appName("DBConnection")  # type:ignore
            .config(
                "spark.driver.extraClassPath", "/home/yan/.spark/postgresql-42.7.3.jar"
            )
            .getOrCreate()
        )

    def close(self):
        """
        Stop the SparkSession.
        """
        if self.spark:
            self.spark.stop()
            self.spark = None

    def query(self, table: str, table_name: str):
        """
        Executes a SQL query on the specified table and creates a temporary
        view with the given table name.

        Args:
            table (str): The name of the table to query.
            table_name (str): The name to assign to the temporary view created.

        Raises:
            RuntimeError: If the Spark session is not initialized.
        """
        if self.spark is None:
            raise RuntimeError(
                "Spark session not initialized. Call connect() method first."
            )
        df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=table,
            properties=self.properties,
        )
        df.createOrReplaceTempView(table_name)


class DBConnectionHandler:
    """
    Class for handling database connections.

    This class provides methods for creating a database engine and managing a session.
    """

    def __init__(
        self,
        database: str,
        pool_size: int,
        max_overflow: int,
        pool_timeout: int,
    ) -> None:
        self.__connection_string = (
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@localhost:5432/{database}"
        )
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
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
        self.session.close()  # type:ignore
        self.__engine = None

    def __create_database_engine(self):
        """
        Create a database engine.

        Returns:
            sqlalchemy.engine.Engine: The created database engine.
        """
        engine = create_engine(
            self.__connection_string,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_timeout=self.pool_timeout,
        )
        return engine

    def get_engine(self):
        """
        Get the database engine.

        Returns:
            sqlalchemy.engine.Engine: The database engine.
        """
        return self.__engine

    def close(self):
        """
        Close the database engine.
        """
        self.__engine.dispose()  # type:ignore


class DBConnection(DBConnectionHandler):
    """
    A class representing a database connection.

    Args:
        database (str): The name of the database.

    Attributes:
        __engine (sqlalchemy.engine.Engine): The database engine.
        pool_size (int): The size of the connection pool.
        max_overflow (int): The maximum overflow size of the connection pool.
        pool_timeout (int): The timeout for the connection pool.

    Methods:
        __create_schema: Create a schema in the database.
        add_pk_to_table: Add a primary key to a table in the database.
        __add_fk_to_table: Add foreign keys to a table in the database.
        __save_as_postgis: Save a table as a PostGIS table in the database.
        __save_to_sql: Save a table as a SQL table in the database.
        add_table: Adds a table to the database.
        query_database: Executes a query on the database and returns the result as a DataFrame.
    """

    def __init__(
        self,
        database: str,
        pool_size: Optional[int] = 10,
        max_overflow: Optional[int] = 20,
        pool_timeout: Optional[int] = 60,
    ) -> None:
        super().__init__(database, pool_size, max_overflow, pool_timeout)  # type:ignore

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
            conn.execute(sqlalchemy.schema.CreateSchema(schema_name))

    def create_schema(self, schema_name: str) -> None:
        """
        Create a schema in the database.

        Args:
            schema_name (str): The name of the schema to be created.
        """
        with self._DBConnectionHandler__engine.begin() as conn:  # type:ignore
            self.__create_schema(conn, schema_name)

    def __get_pk(self, contract):
        if "columns" not in contract:
            return None
        return next(
            (col["column"] for col in contract["columns"] if col["isPrimaryKey"]),
            None,
        )

    def add_pk_to_table(
        self,
        schema_name: str,
        table_name: str,
        primary_key: str,
    ) -> None:
        """
        Add a primary key to a table in the database.

        Args:
            schema_name (str): The name of the schema containing the table.
            table_name (str): The name of the table to which the primary key will be added.
            primary_key (str): The name of the column that will be set as the primary key.
        """
        with self._DBConnectionHandler__engine.begin() as conn:  # type:ignore
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
                        f"""ALTER TABLE {schema_name}.{table_name} 
                            ADD PRIMARY KEY ({primary_key})"""
                    )
                )

    def __get_fk(self, contract):
        if "columns" not in contract:
            return None
        return [
            (col["column"], col["ForeignKey"])
            for col in contract["columns"]
            if col["ForeignKey"] is not None
        ]

    def __add_fk_to_table(
        self,
        schema_name: str,
        table_name: str,
        foreign_keys: List[tuple[str, str]],
    ) -> None:
        """
        Add foreign keys to a table in the database.

        Args:
            schema_name (str): The name of the schema containing the table.
            table_name (str): The name of the table to which the foreign keys will be added.
            foreign_keys (List[tuple[str, str]]): Tuple containing the column name
                                and the path of the foreging key in the database.
        """
        with self._DBConnectionHandler__engine.begin() as conn:  # type:ignore
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

    def __get_not_null(self, contract: dict):
        if "columns" not in contract:
            return None
        return [col["column"] for col in contract["columns"] if col["isNullable"]]

    def __add_not_null_to_table(
        self,
        schema_name: str,
        table_name: str,
        not_null_columns: List[tuple[str, str]],
    ):
        with self._DBConnectionHandler__engine.begin() as conn:  # type:ignore
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

    def __save_in_sequence(self, partitions, names, table_exists):
        """
        Save partitions of a DataFrame in sequence.

        Args:
            partitions: List of DataFrame partitions to save
            names: Tuple containing schema name and table name
            table_exists: Boolean indicating if the table already exists
        """
        if not partitions:
            return

        # Determine the action for the first partition
        first_action = "replace" if not table_exists else "append"

        # Save the first partition
        self._save_small_table(partitions[0], names, first_action)

        # Save remaining partitions with 'append'
        for partition in tqdm(
            partitions[1:], desc="Saving partitions of the dataframe"
        ):
            self._save_small_table(partition, names, "append")

    def __save_large_table(
        self,
        table: Union[pd.DataFrame, gpd.GeoDataFrame],
        names: tuple[str, str],
        action_if_table_exists: str,
        memory_usage: int,
    ):
        """
        Save a large table to the database by splitting it into smaller partitions.

        Args:
            table: The DataFrame or GeoDataFrame to save
            names: Tuple of (schema_name, table_name)
            action_if_table_exists: What to do if table exists ('replace' or 'append')
            memory_usage: Memory usage in MB to determine number of partitions
        """
        # Check if the table already exists in the database
        schema_name, table_name = names
        table_exists = False

        table_exists = self.__table_exists(schema_name, table_name)

        # If table exists and action is 'replace', drop the table first
        if table_exists and action_if_table_exists == "replace":
            self.__drop_table(*names)

        # Determine number of partitions based on memory usage
        num_partitions = max(1, int(memory_usage / 500))
        partitions = np.array_split(table, num_partitions)

        # Save partitions
        self.__save_in_sequence(partitions, names, table_exists)

    def _save_small_table(
        self,
        table,
        names: tuple[str, str],
        action_if_table_exists: str,
    ):
        """
        Save a small table to the database.

        Args:
            table: The table to be saved. It can be either a GeoDataFrame or a regular DataFrame.
            names: A tuple containing the schema name and table name where the table will be saved.
            action_if_table_exists: The action to take if the table already exists in the database.
        """
        schema_name, table_name = names

        with self._DBConnectionHandler__engine.begin() as conn:  # type:ignore
            self.__create_schema(conn, schema_name)
            try:
                if isinstance(table, gpd.GeoDataFrame):
                    table.to_postgis(
                        table_name,
                        conn,
                        schema=schema_name,
                        if_exists=action_if_table_exists,  # type:ignore
                        index=False,
                    )
                else:
                    table.to_sql(
                        table_name,
                        conn,
                        schema=schema_name,
                        if_exists=action_if_table_exists,
                        index=False,
                    )
            except Exception as e:
                conn.rollback()
                if conn is not None and not conn.closed:
                    conn.close()
                raise e

    def __table_exists(self, schema_name, table_name):
        """
        Check if a table exists in the database.

        Args:
            schema_name: Name of the schema
            table_name: Name of the table

        Returns:
            bool: True if the table exists, False otherwise
        """
        with self._DBConnectionHandler__engine.connect() as conn:  # type:ignore
            result = conn.execute(
                text(
                    f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = '{schema_name}'
                    AND table_name = '{table_name}'
                )
                """
                )
            )
            return result.scalar()

    def __save_table(
        self,
        table: Union[pd.DataFrame, gpd.GeoDataFrame],
        names: tuple[str, str],
        action_if_table_exists: str,
    ) -> None:
        """
        Save a table as a PostGIS table in the database.

        Args:
            table (Union[pd.DataFrame, gpd.GeoDataFrame]): The table to be saved.
            names (tuple[str, str]): A tuple containing the schema name and table name.
            action_if_table_exists (str): The action to take if the table already
            exists in the database.
        """
        # Check for empty table
        if table.empty:
            print("Warning: Empty table provided, nothing to save.")
            return

        # Get memory usage to decide how to save
        memory_usage = table.memory_usage(deep=True).sum() / (1024 * 1024)

        # Check if table already exists
        schema_name, table_name = names
        table_exists = self.__table_exists(schema_name, table_name)

        # If appending to existing table and table doesn't exist, use 'replace' instead
        actual_action = action_if_table_exists
        if action_if_table_exists == "append" and not table_exists:
            actual_action = "replace"

        # Use appropriate method based on table size
        if memory_usage > 500:
            self.__save_large_table(table, names, actual_action, memory_usage)
        else:
            self._save_small_table(table, names, actual_action)

    def __create_temp_table(
        self, table: Union[pd.DataFrame, gpd.GeoDataFrame], schema: str, table_name: str
    ):
        temp_table_name = f"temp_{table_name}"
        self.__save_table(table, (schema, temp_table_name), "replace")

    def __update_table(self, schema: str, table_name: str, match_columns: list):
        with self._DBConnectionHandler__engine.connect() as conn:  # type:ignore
            trans = conn.begin()
            try:
                original_row_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {schema}.{table_name}")
                ).scalar()
                all_columns = conn.execute(
                    text(f"SELECT * FROM {schema}.{table_name} LIMIT 1")
                ).keys()
                # Update the existing rows in the table
                update_query = f"""
                            UPDATE {schema}.{table_name} as t
                            SET
                        """
                update_query += ",\n".join(
                    [f"    {col} = temp.{col}" for col in all_columns]
                )
                update_query += f"""
                            FROM {schema}.temp_{table_name} AS temp
                            WHERE
                        """
                update_query += " AND \n".join(
                    [f"t.{col} = temp.{col}" for col in match_columns]
                )

                conn.execute(text(update_query))
                final_row_count = conn.execute(
                    text(f"SELECT COUNT(*) FROM {schema}.{table_name}")
                ).scalar()
                assert (
                    original_row_count == final_row_count
                ), "Row count mismatch, update failed"
                trans.commit()
            except Exception as e:
                trans.rollback()
                raise e

    def __drop_table(self, schema: str, table_name: str):
        with self._DBConnectionHandler__engine.begin() as conn:  # type:ignore
            try:
                # Drop the temporary table
                drop_query = f"DROP TABLE IF EXISTS {schema}.{table_name}"
                conn.execute(text(drop_query))
            except Exception as e:
                conn.rollback()
                raise e

    def __update_table_keys(self, names, primary_key, foreign_keys, not_null_columns):
        schema_name, table_name = names

        if primary_key is not None:
            self.add_pk_to_table(schema_name, table_name, primary_key)
        if foreign_keys is not None and len(foreign_keys) > 0:
            self.__add_fk_to_table(schema_name, table_name, foreign_keys)
        if not_null_columns is not None and len(not_null_columns) > 0:
            self.__add_not_null_to_table(schema_name, table_name, not_null_columns)

    def execute_query(self, query: str) -> Union[list, None]:
        """
        Executes a given SQL query using the database connection.
        Args:
            query (str): The SQL query to be executed.
        Returns:
            list: A list of rows returned by the query if it returns rows, otherwise None.
        """
        try:
            with self._DBConnectionHandler__engine.begin() as conn:  # type:ignore
                result = conn.execute(text(query))
                if result.returns_rows:
                    return result.fetchall()
                return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def drop_index(self, schema: str, table_name: str):
        """
        Drops an index from the database.

        Args:
            schema (str): The name of the schema containing the table.
            table_name (str): The name of the table.
        """
        query = f"""
                    DROP INDEX IF EXISTS {schema}.{table_name}_idx
                    """
        with self._DBConnectionHandler__engine.begin() as conn:  # type:ignore
            conn.execute(text(query))

    def create_index(self, schema: str, table_name: str, columns: list):
        """
        Creates an index on the specified table and columns in the database.

        Args:
            schema (str): The name of the schema where the table resides.
            table_name (str): The name of the table on which to create the index.
            columns (list): A list of column names on which the index should be created.
        """
        table_name_idx = table_name.replace('"', "").replace(" ", "")
        query = f"""
                    CREATE INDEX IF NOT EXISTS {table_name_idx}_idx_{"_".join(columns)}
                    ON {schema}.{table_name} 
                    ({", ".join(columns)})
                    """
        with self._DBConnectionHandler__engine.begin() as conn:  # type:ignore
            conn.execute(text(query))

    def create_pk(self, schema: str, table: str, column: str):
        """
        Create a primary key in a table.

        Args:
            schema (str): The name of the schema containing the table.
            table (str): The name of the table.
            column (str): The name of the column to be used as the primary key.
        """
        query = f"""ALTER TABLE {schema}.{table}
        ADD COLUMN {column} SERIAL PRIMARY KEY
        """
        with self._DBConnectionHandler__engine.begin() as conn:  # type:ignore
            conn.execute(text(query))

    def add_table(
        self, table: Union[pd.Series, pd.DataFrame, gpd.GeoDataFrame], contract: dict
    ):
        """
        Adds a table to the database.

        Parameters:
        - table (pd.DataFrame): The table to be added.
        - contract (dict): A dictionary containing the database contract.
        """
        # Validate inputs
        if table is None or table.empty:
            print("Warning: Empty table provided to add_table, skipping operation")
            return

        if (
            not isinstance(contract, dict)
            or "schema" not in contract
            or "tableName" not in contract
        ):
            raise ValueError("Invalid contract provided to add_table")

        # Copy the table to prevent modifications to the original
        table_copy = table.copy()

        # Handle geometry column if present
        if table_copy.filter(regex="geom").shape[1] > 0:
            col_geom = table_copy.filter(regex="geom").columns[0]
            if all(table_copy[col_geom].isnull()) or all(
                table_copy[col_geom] == "None"
            ):
                table_copy.drop(columns=col_geom, inplace=True)
                if isinstance(table_copy, gpd.GeoDataFrame):
                    table_copy = pd.DataFrame(table_copy)

        # Extract contract details
        primary_key = self.__get_pk(contract)
        foreign_keys = self.__get_fk(contract)
        not_null_columns = self.__get_not_null(contract)
        action_if_table_exists = contract.get(
            "ifExists", "replace"
        )  # Default to 'replace' if not specified
        names = (contract["schema"], contract["tableName"])

        # Save the table in a single operation
        self.__save_table(table_copy, names, action_if_table_exists)  # type:ignore

        # Update table keys after saving
        self.__update_table_keys(names, primary_key, foreign_keys, not_null_columns)

    def delete_rows_table(self, names: Tuple[str, str], condition: str) -> None:
        """
        Delete rows from a table in the database.

        Args:
            names (Tuple[str, str]): The path to the table in the format (schema, table).
            condition (str): The condition to be used in the DELETE statement.

        Raises:
            Exception: If there's an error during database operation.
        """
        schema, table_name = names
        with self._DBConnectionHandler__engine.begin() as conn:  # type:ignore
            try:
                conn.execute(
                    text(f"DELETE FROM {schema}.{table_name} WHERE {condition}")
                )
            except Exception as e:
                conn.rollback()
                raise e

    def update_table(
        self,
        table: Union[pd.DataFrame, gpd.GeoDataFrame],
        columns: list,
        names: Tuple[str, str],
    ) -> None:
        """
        Update the table in the database with the rows present in the DataFrame.

        Args:
            table (pd.DataFrame): The DataFrame containing the updated rows.
            columns (list): A list of columns to match.
            names (Tuple[str, str]): The path to the table in the format (schema, table).

        Raises:
            Exception: If there's an error during database operation.
        """
        try:
            schema, table_name = names

            self.__create_temp_table(table, schema, table_name)
            self.__update_table(schema, table_name, columns)
            self.__drop_table(schema, f"temp_{table_name}")
        except Exception as e:
            raise e

    def create_table_from_sql(self, query: str, path_new_table: str):
        """
        Create a new table in the database based on the provided SQL query.

        Args:
            query (str): The SQL query used to create the new table.
            path_new_table (str): The name of the new table to be created.

        Raises:
            Exception: If an error occurs during the table creation process.
        """
        creation_query = f"""
        CREATE TABLE IF NOT EXISTS {path_new_table} AS
        {query}
        """
        with self._DBConnectionHandler__engine.begin() as conn:  # type:ignore
            try:
                conn.execute(text(creation_query))
            except Exception as e:
                conn.rollback()
                raise e

    def create_materialized_view(self, query: str, path_new_table: str):
        """
        Create a new materialized view in the database based on the provided SQL query.

        Args:
            query (str): The SQL query used to create the materialized view.
            path_new_table (str): The name of the materialized view to be created.

        Raises:
            Exception: If an error occurs during the materialized view creation process.
        """
        creation_query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {path_new_table} AS
        {query}
        """
        with self._DBConnectionHandler__engine.begin() as conn:  # type:ignore
            try:
                conn.execute(text(creation_query))
            except Exception as e:
                conn.rollback()
                raise e

    def create_view(self, query: str, path_new_table: str):
        """
        Create a new view in the database based on the provided SQL query.

        Args:
            query (str): The SQL query used to create the view.
            path_new_table (str): The name of the view to be created.

        Raises:
            Exception: If an error occurs during the view creation process.
        """
        creation_query = f"""
        CREATE VIEW IF NOT EXISTS {path_new_table} AS
        {query}
        """
        with self._DBConnectionHandler__engine.begin() as conn:  # type:ignore
            try:
                conn.execute(text(creation_query))
            except Exception as e:
                conn.rollback()
                raise e

    def query_database(
        self,
        query: str,
        geo: bool = False,
        display: bool = False,
        max_memory_mb: int = 20_000,  # 20 GB
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """
        Executes a SQL query and returns the result as a DataFrame or GeoDataFrame.
        Args:
            query (str): The SQL query to be executed.
            geo (bool): If True, returns a GeoDataFrame with geometry column.
            display (bool): If True, displays progress messages during execution.
            max_memory_mb (int): Maximum memory usage in MB before consolidating chunks.
        Returns:
            Union[pd.DataFrame, gpd.GeoDataFrame]: The result of the query as a
                                                    DataFrame  or GeoDataFrame.
        Raises:
            Exception: If there's an error during the query execution.
        Note:
            This method handles large queries by processing them in chunks to avoid memory issues.
        """

        with self._DBConnectionHandler__engine.connect() as conn:  # type:ignore
            conn = conn.execution_options(stream_results=True)
            try:
                chunk_iterator = pd.read_sql_query(text(query), conn, chunksize=1000)

                chunks = []
                total_rows = 0
                current_memory_mb = 0

                for chunk in chunk_iterator:
                    chunks.append(chunk)
                    total_rows += len(chunk)

                    # Estimar uso de memória do chunk
                    chunk_memory_mb = chunk.memory_usage(deep=True).sum() / (
                        1024 * 1024
                    )
                    current_memory_mb += chunk_memory_mb

                    # Se atingir limite de memória, concatenar intermediariamente
                    if current_memory_mb > max_memory_mb:
                        if display:
                            print(
                                f"Consolidando {len(chunks)} chunks ({total_rows} registros)..."
                            )

                        # Concatenar e substituir lista por DataFrame único
                        intermediate_df = pd.concat(chunks, ignore_index=True)
                        chunks = [intermediate_df]
                        current_memory_mb = intermediate_df.memory_usage(
                            deep=True
                        ).sum() / (1024 * 1024)

                        # Forçar limpeza de memória

                        gc.collect()

                        if display:
                            print(f"Memória consolidada: {current_memory_mb:.2f} MB")

                # Concatenação final
                if chunks:
                    df = (
                        pd.concat(chunks, ignore_index=True)
                        if len(chunks) > 1
                        else chunks[0]
                    )
                else:
                    df = pd.DataFrame()

                # Limpeza final
                del chunks

                gc.collect()

                if geo and not df.empty:
                    df = gpd.GeoDataFrame(df, geometry="geometry", crs=CRS_GLOBAL)

                return df

            except Exception as e:
                print(f"Erro na query: {e}")
                return pd.DataFrame()
