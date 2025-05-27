"""
This module provides the DataContract class for managing data contracts.

The DataContract class is responsible for loading and retrieving data contracts
from YAML files stored in a specified directory structure. Each contract is
organized by agent and table name.

Classes:
    DataContract: A class to handle loading and retrieving data contracts.

Usage example:
    data_contract = DataContract()
    contract_data = data_contract.get_contract(meddalon="example_meddalon",
                                                agent="example_agent",
                                                table_name="example_table"
                                                )

"""

import os
import yaml


SOURCE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "contracts")


class DataContract:
    """
    Class for the data contract.
    """

    def __init__(self):
        self.source_path = SOURCE_PATH
        self.contracts = {}
        self._load_contracts()

    def _load_contracts(self):
        tables = os.listdir(self.source_path)
        for table in tables:
            self.contracts[table] = []
            files = os.listdir(os.path.join(self.source_path, table))
            for file in files:
                self.contracts[table].append(file)

    def __get_contract(self, meddalon, path):
        with open(path, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)
        return config[meddalon]

    def get_contract(self, meddalon: str, agent: str, table_name: str) -> dict:
        """
        Retrieves the data from the contract.

        Args:
            meddalon (str): The meddalon value (bronze, silver or gold).
            agent (str): The agent name (ibge, aneel, buildings, etc).
            table_name (str): The table name.

        Returns:
            data (Union[dict, None]): The data from the contract.
        """
        table_name = ".".join([table_name, "yaml"])
        path = os.path.join(self.source_path, agent, table_name)
        for contract_name in self.contracts[agent]:
            if table_name == contract_name:
                return self.__get_contract(meddalon, path)
        raise FileNotFoundError(f"Contract not found: {table_name}")

    def post_contract(self, contract: dict, path: str = None):
        """
        Overrides the contract column datatypes and saves the updated data to a YAML file.

        Args:
            contract (dict): The dictionary containing the contract column datatypes.
            path (str, optional): The path to save the contract.
        """
        with open(path, "w", encoding="utf-8") as file:
            yaml.dump(contract, file)
