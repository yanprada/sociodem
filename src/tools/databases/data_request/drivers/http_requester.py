"""
Module to make requests to pages.
"""

import os
import logging
import requests
import pandas as pd

from tqdm import tqdm


class HttpRequesterAneel:
    """
    Http request class to download data from Aneel
    """

    def __init__(self) -> None:
        self.__url = "https://www.arcgis.com/sharing/rest/content/items/{id}/data"

    def __save_file(self, response: requests.Response, filename: str) -> None:
        if response.status_code == 200:
            with open(filename, "wb") as f:
                f.write(response.content)
        else:
            raise requests.exceptions.HTTPError(
                f"Error {response.status_code} in request"
            )

    def update(self):
        """
        This method is responsible for updating the data.
        """

    def request_from_page(
        self, id_params: pd.Series, file_names: pd.Series, destination_path: str
    ):
        """Method to request Aneel data from website.

        Args:
            id_params (pd.Series): A pandas Series containing the id
                                    parameters for the Aneel database.
            file_names (pd.Series): A pandas Series containing the names of the files to be saved.
            destination_path (str): The path where the downloaded files will be saved.

        Raises:
            TimeoutError: If the request to the website times out.

        Notes:
            This method iterates over the id_params and file_names Series in
                parallel using the zip function.
            For each id_param and file, it checks if the corresponding file
                already exists in the destination_path.
            If the file does not exist, it sends a request to the Aneel website
                using the id_param and saves the response as a file.
            If the file already exists, a warning message is logged.

        """
        destination_dir = os.path.abspath(destination_path)
        for id_param, file in tqdm(zip(id_params, file_names)):
            filename = os.path.join(destination_dir, file)
            if not os.path.exists(filename):
                response = requests.get(self.__url.format(id=id_param), timeout=10)
                self.__save_file(response, filename)
            else:
                logging.warning("File %s already exists in destination.", filename)


class HttpRequesterGridCenso:
    """
    Http request class to download data from Aneel
    """

    def __init__(self) -> None:
        self.__base_url = (
            "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
            "malhas_territoriais/malhas_de_setores_censitarios__"
            "divisoes_intramunicipais/censo_2010/"
            "setores_censitarios_shp/"
        )

        self.__url = "{base_url}{state_upper}/{state_lower}_{level}.zip"

    def __save_file(self, response: requests.Response, filename: str) -> None:
        if response.status_code == 200:
            with open(filename, "wb") as f:
                f.write(response.content)
        else:
            raise requests.exceptions.HTTPError(
                f"Error {response.status_code} in request"
            )

    def update(self):
        """
        This method is responsible for updating the data.
        """

    def request_from_page(
        self, id_params: pd.Series, file_names: pd.Series, destination_path: str
    ):
        """Method to request Aneel data from website.

        Args:
            id_params (pd.Series): A pandas Series containing the id
                                    parameters for the Aneel database.
            file_names (pd.Series): A pandas Series containing the names of the files to be saved.
            destination_path (str): The path where the downloaded files will be saved.

        Raises:
            TimeoutError: If the request to the website times out.

        Notes:
            This method iterates over the id_params and file_names Series in
                parallel using the zip function.
            For each id_param and file, it checks if the corresponding file
                already exists in the destination_path.
            If the file does not exist, it sends a request to the Aneel website
                using the id_param and saves the response as a file.
            If the file already exists, a warning message is logged.

        """
        destination_dir = os.path.abspath(destination_path)
        for state, level in tqdm(zip(id_params, file_names)):
            filename = os.path.join(destination_dir, state, level)
            if not os.path.exists(filename):
                response = requests.get(
                    self.__url.format(
                        base_url=self.__base_url, state=state, level=level
                    ),
                    timeout=10,
                )
                self.__save_file(response, filename)
            else:
                logging.warning("File %s already exists in destination.", filename)
