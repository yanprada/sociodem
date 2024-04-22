"""
Module to make requests to pages.
"""

import os
from typing import Tuple, Dict
import requests
import pandas as pd
from retry import retry
from tqdm import tqdm
from src.tools.utils.common import write_log


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
                write_log(
                    f"File {filename} already exists in destination.", level="warning"
                )


class HttpRequesterCenso:
    """
    Http request class to download data from Aneel
    """

    def __init__(self, year: int) -> None:
        self.year = int(year)
        if self.year == 2010:
            self.__base_url_layers = (
                "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
                "malhas_territoriais/malhas_de_setores_censitarios__"
                "divisoes_intramunicipais/censo_2010/"
                "setores_censitarios_shp/"
            )
            self.__url_layers = "{base_url}{state}/{state}_{level}.zip"
            self.dompp = None
        elif self.year == 2022:
            self.__base_url_layers = (
                "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
                "malhas_territoriais/malhas_de_setores_censitarios__"
                "divisoes_intramunicipais/censo_2022_preliminar/"
            )
            self.__url_layers = (
                "{base_url}{level}/shp/UF/{state}/"
                "{state}_Malha_Preliminar{level_upper}_2022.zip"
            )
            self.__url_dompp = (
                "https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/"
                "Censo_Demografico_2022/Coordenadas_enderecos/UF/{state_code}_{state}.zip"
            )
        else:
            raise ValueError("Year must be 2010 or 2022")

    def __make_dir(self, filename: str) -> None:
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def __save_response_as_zip(
        self, response: requests.Response, filename: str
    ) -> None:
        zip_filename = ".".join([filename, "zip"])
        self.__make_dir(zip_filename)
        with open(zip_filename, "wb") as f:
            f.write(response.content)

    def __save_file(self, response: requests.Response, filename: str) -> None:
        if response.status_code == 200:
            self.__save_response_as_zip(response, filename)
        else:
            raise requests.exceptions.HTTPError(
                f"Error {response.status_code} in request"
            )

    @retry(tries=5, delay=1, backoff=2)
    def __request_layers_2010(self, state: str, level: str) -> requests.Response:
        """
        This method is responsible for making a request to the website.
        """
        if state == "GO" and level == "setores_censitarios":
            level = "setores _censitarios"
        return requests.get(
            self.__url_layers.format(
                base_url=self.__base_url_layers, state=state.lower(), level=level
            ),
            timeout=10,
        )

    @retry(tries=5, delay=1, backoff=2)
    def __request_layers_2022(self, state: str, level: str) -> requests.Response:
        """
        This method is responsible for making a request to the website.
        """
        if level == "setores":
            level_upper = ""
        else:
            level_upper = "".join(["_", level.capitalize()[:-1]])
        return requests.get(
            self.__url_layers.format(
                base_url=self.__base_url_layers,
                level=level,
                state=state,
                level_upper=level_upper,
            ),
            timeout=10,
        )

    @retry(tries=5, delay=1, backoff=2)
    def __request_dompp_2022(self, state_code: int, state: str) -> requests.Response:
        """
        This method is responsible for making a request to the website.
        """
        return requests.get(
            self.__url_dompp.format(
                base_url=self.__url_dompp, state_code=state_code, state=state
            ),
            timeout=10,
        )

    def request_layers_from_page(
        self, combinations: Tuple[str, str], destination_path: str
    ) -> None:
        """Method to request Aneel data from website.

        Args:
            combinations (Tuple[str, str]): A tuple containing the state and level combinations.
            destination_path (str): The path where the downloaded files will be saved.

        """
        destination_dir = os.path.abspath(destination_path)
        for state, level in tqdm(combinations):
            if level == "setores":
                level_name = "setores_censitarios"
            filename = os.path.join(destination_dir, level_name, state)
            if not os.path.exists(filename):
                write_log(f"Requesting {level} - {state}.")
                if self.year == 2010:
                    response = self.__request_layers_2010(state, level)
                else:
                    response = self.__request_layers_2022(state, level)
                self.__save_file(response, filename)
            else:
                write_log(
                    f"File {filename} already exists in destination.", level="warning"
                )

    def request_dompp_from_page(
        self, states: Dict[str, int], destination_path: str
    ) -> None:
        """
        Requests dompp for each state from a web page and saves the response to a file.

        Args:
            states (Dict[str, int]): A dictionary containing the names and codes of the states.
            destination_path (str): The path where the files will be saved.
        """
        if self.__url_dompp is not None:
            for state, state_code in states.items():
                destination_dir = os.path.abspath(destination_path)
                filename = os.path.join(destination_dir, state)
                if not os.path.exists(filename):
                    write_log(f"Requesting dompp for {state}.")
                    response = self.__request_dompp_2022(state_code, state)
                    self.__save_file(response, filename)
                else:
                    write_log(
                        f"File {filename} already exists in destination.",
                        level="warning",
                    )
        else:
            write_log("Dompp is not available for this year.", level="warning")
