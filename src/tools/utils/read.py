"""
This module provides a class that provides methods to read different file formats.

It includes methods to read Parquet files, CSV files, Excel files, and shapefiles.
"""

import pandas as pd
import geopandas as gpd


class Reader:
    """
    A class that provides methods to read different file formats.
    """

    def __init__(self, contract: dict) -> None:
        self.contract = contract

    def __read(self, read_fucntion, file_path: str, **kwargs):
        """
        Reads a file and returns a pandas DataFrame.

        Parameters:
        - function (function): The function to read the file.
        - file_path (str): The path to the file.
        - **kwargs: Additional keyword arguments to be passed to the function.

        Returns:
        - DataFrame: The data read from the file.
        """
        dtypes_dict = {
            col["column"]: col["logicalTye"] for col in self.contract["columns"]
        }
        df = read_fucntion(file_path, **kwargs)
        df = df.astype(dtypes_dict)
        return df

    def read_parquet(self, file_path: str, **kwargs):
        """
        Reads a Parquet file and returns a pandas DataFrame.

        Parameters:
        - file_path (str): The path to the Parquet file.
        - **kwargs: Additional keyword arguments to be passed to the `pd.read_parquet` function.

        Returns:
        - DataFrame: The data read from the Parquet file.
        """
        read_function = pd.read_parquet
        df = self.__read(read_function, file_path, **kwargs)
        return df

    def read_csv(self, file_path: str, **kwargs):
        """
        Reads a CSV file and returns a pandas DataFrame.

        Parameters:
        - file_path (str): The path to the CSV file.
        - **kwargs: Additional keyword arguments to be passed to the `pd.read_csv` function.

        Returns:
        - DataFrame: The data read from the CSV file.
        """
        read_function = pd.read_csv
        df = self.__read(read_function, file_path, **kwargs)
        return df

    def read_excel(self, file_path: str, **kwargs):
        """
        Reads an Excel file and returns a pandas DataFrame.

        Parameters:
        - file_path (str): The path to the Excel file.
        - **kwargs: Additional keyword arguments to be passed to the `pd.read_excel` function.

        Returns:
        - DataFrame: The data read from the Excel file.
        """
        read_function = pd.read_excel
        df = self.__read(read_function, file_path, **kwargs)
        return df

    def read_geofile(self, file_path: str, **kwargs):
        """
        Reads a shapefile and returns a GeoDataFrame.

        Parameters:
        - file_path (str): The path to the shapefile.
        - **kwargs: Additional keyword arguments to be passed to the `gpd.read_file` function.

        Returns:
        - GeoDataFrame: The data read from the shapefile.
        """
        read_function = gpd.read_file
        df = self.__read(read_function, file_path, **kwargs)
        return df
