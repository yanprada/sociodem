"""
This module provides functions for reading district data from a contract file.

The contract file path is specified in the `CONTRACT_DISTRICTS` variable.

Functions:
- read_districts: Reads the district data from the contract file and returns a DataFrame.
"""

from src.tools.utils.config import get_contract
from src.tools.utils.read import Reader

CONTRACT_DISTRICTS = get_contract("from_to/contract_districts.yaml", "bronze")


def read_districts():
    """
    Reads the district data from the contract file and returns a DataFrame.

    Returns:
    - df: A pandas DataFrame containing the district data.
    """
    reader = Reader(CONTRACT_DISTRICTS)
    df = reader.read_excel(CONTRACT_DISTRICTS["physicalPath"])
    return df
