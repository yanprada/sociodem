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

from typing import Union
import fileinput
from datetime import datetime

from src.tools.utils.common import write_log
from src.tools.databases.data_connection.connection import MongoDBConnection
from src.tools.data_contract.data_contract_class import DataContract


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
        self.params = params
        self.execution_details: dict = params.get("execution_details", {})
        self.data_contracts = self.__load_contracts()
        self.run_mode = params.get("run_mode", None)
        self.conn = MongoDBConnection(
            f"executions_{self.params['medallon']}", self.params["data_name"]
        )
        self.collection = self.conn.get_collection()
        self.execution_id = None

    def __load_contracts(self):
        dc = DataContract()
        contracts = self.params.get("data_contracts", {})
        return {k: self.__process_contract(dc, v) for k, v in contracts.items()}

    def __process_contract(self, dc, contract_params):
        contract_result = dc.get_contract(*contract_params)
        if isinstance(contract_result, list):
            return self.__process_subcontracts(contract_result)
        return {contract_result["tableName"]: contract_result}

    def __process_subcontracts(self, subcontracts):
        processed_subcontracts = {}
        for subc in subcontracts:
            if "{year}" in subc["tableName"]:
                processed_subcontracts.update(self.__expand_yearly_subcontracts(subc))
            else:
                processed_subcontracts[subc["tableName"]] = subc
                year_init, year_end = subc["queryYears"]
                self.params["years"] = list(range(year_init, year_end + 1))
        return processed_subcontracts

    def __expand_yearly_subcontracts(self, subcontract):
        expanded_subcontracts = {}
        year_init, year_end = subcontract["queryYears"]
        self.params["years"] = list(range(year_init, year_end + 1))
        for year in range(year_init, year_end + 1):
            subc_copy = self.__replace_year_in_subcontract(subcontract, year)
            expanded_subcontracts[subc_copy["tableName"]] = subc_copy
        return expanded_subcontracts

    def __replace_year_in_subcontract(self, subcontract, year):
        subc_copy = subcontract.copy()
        subc_copy["tableName"] = subc_copy["tableName"].replace("{year}", str(year))
        for key, value in subc_copy.items():
            if isinstance(value, str) and "{year}" in value:
                subc_copy[key] = value.replace("{year}", str(year))
        return subc_copy

    def __create_execution_id(self, overwrite: bool):
        timestamp_str = datetime.now().strftime("%Y-%m-%d-%Hh%Mm%Ss")
        execution_id = (
            f"{self.params['medallon']}-{self.params['data_name']}-{timestamp_str}"
        )
        self.execution_details["execution_id"] = execution_id
        self.execution_id = execution_id
        if overwrite:
            write_log(f"Execution ID `{execution_id}` successfully created.")

    def __create_datetime(self, overwrite: bool):
        date = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.execution_details["creation_date"] = date
        if overwrite:
            write_log(f"Created execution at {date}")

    def __create_mlflow_experiment_name(self, overwrite: bool):
        experiment_name = f"{self.params['data_name']}_{self.params['medallon']}"
        self.execution_details["mlflow_experiment"] = experiment_name
        self.execution_details["mlflow_runs"] = {}
        if overwrite:
            write_log(f"MLflow experiment name: {experiment_name}")

    def __create_status(self, overwrite: bool):
        self.execution_details["status"] = {}
        if overwrite:
            write_log("Status created!")

    def __create_data_contracts(self, overwrite: bool):
        self.execution_details["data_contracts"] = self.data_contracts
        if overwrite:
            write_log("Data contracts added to execution details")

    def __create_steps(self, overwrite: bool):
        self.execution_details["steps"] = self.execution_details.get("steps", {})
        num_steps = len(self.execution_details["steps"])
        if overwrite:
            write_log(f"{num_steps} steps added to execution details")

    def __create_run_mode(self, overwrite: bool):
        self.execution_details["run_mode"] = self.run_mode
        if overwrite:
            write_log(f"Initialized with mode as {self.run_mode}")

    def __create_last_run(self, overwrite: bool):
        self.execution_details["last_run"] = self.params["last_run"]
        if overwrite:
            write_log(f"Last run set to {self.params['last_run']}")

    def __create_info(self, overwrite: bool):
        self.execution_details["info"] = {"running_years": self.params.get("years", [])}
        if overwrite:
            write_log("Info added to execution details")

    def __create_paths_materialized_views(self, overwrite: bool):
        self.execution_details["materialized_views"] = self.params["materialized_views"]
        if overwrite:
            write_log("Materialized views paths added to execution details")

    def __create_execution(self, overwrite: bool):
        self.execution_details = {}
        self.__create_execution_id(overwrite)
        self.__create_datetime(overwrite)
        self.__create_status(overwrite)
        self.__create_mlflow_experiment_name(overwrite)
        self.__create_data_contracts(overwrite)
        self.__create_steps(overwrite)
        self.__create_run_mode(overwrite)
        self.__create_last_run(overwrite)
        self.__create_info(overwrite)
        self.__create_paths_materialized_views(overwrite)

    def create_execution(self, overwrite: bool = False):
        """
        Creates an execution with a unique execution ID, current date and time, status,
        and MLflow experiment name.
        The execution details are saved in MongoDB.
        """
        self.__create_execution(overwrite)
        if overwrite:
            self.collection.insert_one(self.execution_details)
            write_log(f"Execution ID `{self.execution_id}` saved in MongoDB")
            self.overwrite_execution_id()

    def start_execution(
        self, execution_id: Union[str, None] = None, overwrite: bool = True
    ):
        """
        Starts the execution process.
        This method updates the status to "running" and writes a log message indicating
        that the execution with the given ID has started.
        """
        if execution_id is None:
            self.create_execution(overwrite)
            execution_id = self.execution_id

        write_log(f"Execution ID {execution_id} started.")
        for _, func_step in enumerate(self.params["execution_details"]["steps"]):
            if func_step["run"]:
                func_step["function"]()

    def initialize_execution(
        self, execution_id: Union[str, None] = None, overwrite: bool = False
    ):
        """
        Initializes the execution process.
        This method updates the status to "initialized" and writes a log message indicating
        that the execution with the given ID has been initialized.
        """
        if execution_id is None and self.execution_id is None:
            self.create_execution(overwrite)
        query = {"execution_id": execution_id if execution_id else self.execution_id}
        db_return = self.collection.find_one(query)
        if isinstance(db_return, dict):
            self.execution_details = db_return
        self.execution_id = self.execution_details["execution_id"]

    def overwrite_execution_id(self):
        """
        Overwrites a constant in a .py file with a new value.
        """
        for line in fileinput.input(self.params["config_path"], inplace=True):
            if line.startswith("EXECUTION_ID"):
                line = f"{'EXECUTION_ID'} = '{self.execution_id}'\n"
            print(line, end="")

    def add_status_to_step(self, status: str):
        """
        Add the status to the step of the execution.
        Args:
            status (str): The new status of the execution
        """
        date = datetime.now().strftime("%d-%m-%Y %H:%M")
        self.execution_details["steps"][status] = date
        steps = self.execution_details["steps"]
        self.collection.update_one(
            {"execution_id": self.execution_id},
            {"$set": {"steps": steps}},
        )
        write_log(f"Execution ID {self.execution_id} updated step to {steps}")

    def update_status(self, status: str):
        """
        Update the status of the execution.
        Args:
            status (str): The new status of the execution.
        """
        self.add_status_to_step(status)
        status = "_".join(["running", status])
        self.execution_details["status"] = status
        self.collection.update_one(
            {"execution_id": self.execution_id},
            {"$set": {"status": status}},
        )
        write_log(f"Execution ID {self.execution_id} updated to {status}")

    def update_mlflow_runs(self, run_name: str):
        """
        Update the mlflow runs of the execution.
        Args:
            run_name (str): The new mlflow execution.
        """

        self.execution_details["mlflow_runs"][
            self.execution_details["status"][-1][-1]
        ] = run_name
        self.collection.update_one(
            {"execution_id": self.execution_id},
            {"$set": {"mlflow_runs": self.execution_details["mlflow_runs"]}},
        )
        write_log(f"Execution ID {self.execution_id} updated mlflow runs")

    def update_last_run(self):
        """
        Update the last run of the execution.
        """
        date = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        self.execution_details["last_run"] = date
        self.collection.update_one(
            {"execution_id": self.execution_id},
            {"$set": {"last_run": date}},
        )
        write_log(f"Execution ID {self.execution_id} updated last_run to {date}")


class ExecutionManagerWrapper:
    """Classe wrapper para gerenciar instâncias únicas de ExecutionManager."""

    def __init__(self, base_params, execution_id, debug):
        self._manager = ExecutionManager(base_params)
        self._manager.initialize_execution(execution_id, debug)

    @property
    def manager(self):
        """
        Return instance of ExecutionManager.
        """
        return self._manager

    @property
    def execution_details(self):
        """
        Return execution_details from ExecutionManager.
        """
        return self._manager.execution_details
