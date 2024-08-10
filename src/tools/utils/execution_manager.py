"""

This module contains classes for managing MongoDB connections and executing tasks.

Classes:
- MongoDBConnection: Represents a MongoDB collection and provides 
        methods for interacting with the collection.
- ExecutionManager: Manages the execution of a task and provides methods for 
        creating, updating, and retrieving execution details.
    test_connection(): Test the connection to MongoDB server.
    get_database(): Returns the database associated with the current instance.
    get_collection(): Returns the collection associated with the current instance.
"""

import fileinput
from datetime import datetime
from pymongo import MongoClient
from src.tools.utils.common import generate_random_string, write_log


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
            write_log("MongoDB connection successful")
            return True
        except Exception as e:
            write_log(f"MongoDB connection failed: {str(e)}")
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


class ExecutionManager:
    """
    Manages the execution of a task.
    Attributes:
        medallon (str): The medallon value.
        data_name (str): The name of the data.
        execution_details (dict): Details of the execution.
        conn (MongoDBConnection): The MongoDB connection.
        collection (MongoDBCollection): The MongoDB collection.
        execution_id (str): The execution ID.
    Methods:
        create_execution(): Creates a new execution.
        update_status(status: str): Updates the status of the execution.
        start_execution(): Starts the execution.
        get_execution_details(execution_id: str): Retrieves the details of the execution.
        Initializes a new instance of the ExecutionManager class.
        Args:
            params (dict): A dictionary containing the parameters for the execution.
        Creates a new execution.
        Returns:
            None
        ...
        Updates the status of the execution.
        Args:
            status (str): The new status of the execution.
        Returns:
            None
        ...
        Starts the execution.
        Returns:
            None
        ...
        Retrieves the details of the execution.
        Args:
            execution_id (str, optional): The ID of the execution to retrieve details for.
                If not provided, the current execution ID will be used.
        Returns:
            dict: A dictionary containing the details of the execution.
        ...
    """

    def __init__(self, params: dict):
        self.medallon = params["medallon"]
        self.data_name = params["data_name"]
        self.config_path = params["config_path"]
        self.execution_details = params.get("execution_details", None)
        self.conn = MongoDBConnection(f"executions_{self.medallon}", self.data_name)
        self.collection = self.conn.get_collection()
        self.execution_id = None

    def __create_execution_id(self):
        execution_id_str = generate_random_string(15)
        execution_id = f"{self.medallon}-{self.data_name}-{execution_id_str}"
        self.execution_details["execution_id"] = execution_id
        self.execution_id = execution_id
        write_log(f"Execution ID `{execution_id}` successfully created.")

    def __create_datetime(self):
        date = datetime.now().strftime("%d-%m-%Y %H:%M")
        self.execution_details["creation_date"] = date
        write_log(f"Created execution at {date}")

    def __create_mlflow_experiment_name(self):
        experiment_name = f"{self.data_name}_{self.medallon}"
        self.execution_details["mlflow_experiment_name"] = experiment_name
        write_log(f"MLflow experiment name: {experiment_name}")

    def __create_status(self):
        status = "pending"
        self.execution_details["status"] = status
        write_log(f"Status: {status}")

    def create_execution(self):
        """
        Creates an execution with a unique execution ID, current date and time, status,
        and MLflow experiment name.
        The execution details are saved in MongoDB.
        """
        self.execution_details = {}
        self.__create_execution_id()
        self.__create_datetime()
        self.__create_status()
        self.__create_mlflow_experiment_name()
        self.collection.insert_one(self.execution_details)
        write_log(f"Execution ID `{self.execution_id}` saved in MongoDB")
        self.overwrite_execution_id()

    def update_status(self, status: str):
        """
        Update the status of the execution.
        Args:
            status (str): The new status of the execution.
        """

        self.execution_details["status"] = status
        self.collection.update_one(
            {"execution_id": self.execution_id},
            {"$set": {"status": status}},
        )
        write_log(f"Execution ID {self.execution_id} updated to {status}")

    def start_execution(self, execution_id: str = None):
        """
        Starts the execution process.
        This method updates the status to "running" and writes a log message indicating
        that the execution with the given ID has started.
        """
        if execution_id is None:
            self.create_execution()
            execution_id = self.execution_id

        write_log(f"Execution ID {execution_id} started.")
        for i, step in enumerate(self.execution_details["steps"]):
            self.update_status(f"running_step_{i}")
            if step["run"]:
                step["function"]()

    def get_execution_details(self, execution_id: None):
        """
        Retrieves the execution details based on the provided execution ID.
        Args:
            execution_id (optional): The execution ID to retrieve details for.
                If not provided, the method uses the default execution ID.
        Returns:
            dict: A dictionary containing the execution details.
        """
        if execution_id is None and self.execution_id is None:
            self.create_execution()
        query = {"execution_id": execution_id if execution_id else self.execution_id}
        return self.collection.find_one(query)

    def overwrite_execution_id(self):
        """
        Overwrites a constant in a .py file with a new value.
        """
        for line in fileinput.input(self.config_path, inplace=True):
            if line.startswith("EXECUTION_ID"):
                line = f"{'EXECUTION_ID'} = {self.execution_id}\n"
            print(line, end="")
