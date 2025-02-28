"""
Module to make requests to pages.
"""

import os
from typing import Tuple, Dict, Union, Optional
import duckdb as db
from tqdm import tqdm
import requests
import pandas as pd
from retry import retry
from src.tools.utils.common import write_log
from src.tools.utils.constants import BBOX_BRAZIL, OVERTURE_RELEASE_VERSION

tqdm.pandas()


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

    @retry(tries=5, delay=1, backoff=2)
    def get_response(self, id_param):
        """
        This method is responsible for updating the data.
        """
        return requests.get(self.__url.format(id=id_param), timeout=10)

    def request_from_page(
        self,
        id_params: pd.Series,
        destination_path: str,
        titles: Optional[Union[pd.Series, None]] = None,
    ):
        """Method to request Aneel data from website.

        Args:
            id_params (pd.Series): A pandas Series containing the id
                                    parameters for the Aneel database.
            destination_path (str): The path where the downloaded files will be saved.
            titles (Union[str, None]): The titles of the files to be saved.

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
        if titles is None:
            titles = id_params
        for id_param, title in tqdm(zip(id_params, titles)):
            write_log(f"Requesting {id_param}")
            file = f"{title}.gdb.zip"
            filename = os.path.join(destination_dir, file)
            if not os.path.exists(filename):
                response = self.get_response(id_param)
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
            self.__url_dompp = None
            self.__url_mun = (
                "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/"
                "malhas_municipais/municipio_2010/{state_lower}/{state_lower}_municipios.zip"
            )
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
            self.__url_mun = (
                "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/"
                "malhas_municipais/municipio_2022/UFs/{state}/{state}_Municipios_2022.zip"
            )
            self.__url_state = (
                "https://servicodados.ibge.gov.br/api/v3/malhas/estados/{state}"
                "?formato=application/vnd.geo+json"
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

    def __save_file(
        self, response: requests.Response, filename: str, as_zip=True
    ) -> None:
        if response.status_code == 200:
            if as_zip:
                self.__save_response_as_zip(response, filename)
            else:
                filename = filename + ".geojson"
                self.__make_dir(filename)
                with open(filename, "wb") as f:
                    f.write(response.content)
        else:
            raise requests.exceptions.HTTPError(
                f"Error {response.status_code} in request"
            )

    @retry(tries=5, delay=1, backoff=2)
    def __request_layers_2010(self, state: str, level: str) -> requests.Response:
        """
        This method is responsible for making a request to the website.
        """
        if level == "municipios":
            return requests.get(
                self.__url_mun.format(state_lower=state.lower()),
                timeout=10,
            )
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
        if level == "municipios":
            return requests.get(
                self.__url_mun.format(state=state),
                timeout=10,
            )
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

    @retry(tries=5, delay=1, backoff=2)
    def request_states_from_page(
        self, states: Dict[str, int], destination_path: str
    ) -> None:
        """
        Requests states from a web page and saves the response to a file.

        Args:
            states (Dict[str, int]): A dictionary containing the names and codes of the states.
            destination_path (str): The path where the files will be saved.
        """
        destination_dir = os.path.abspath(destination_path)
        for state, _ in tqdm(states.items()):
            filename = os.path.join(destination_dir, state)
            if not os.path.exists(f"{filename}.geojson"):
                write_log(f"Requesting {state}.")
                response = requests.get(
                    self.__url_state.format(state=state), timeout=10
                )
                self.__save_file(response, filename, as_zip=False)
            else:
                write_log(
                    f"File {filename} already exists in destination.", level="warning"
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
            filename = os.path.join(destination_dir, level, state)
            if level == "setores":
                filename = os.path.join(destination_dir, "setores_censitarios", state)
            if not os.path.exists(f"{filename}.zip"):
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
            for state, state_code in tqdm(states.items()):
                destination_dir = os.path.abspath(destination_path)
                filename = os.path.join(destination_dir, state)
                if not os.path.exists(f"{filename}.zip"):
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


class HttpRequesterMapbiomas:
    """
    Http request class to download Mapbiomas tiff
    """

    def __init__(self) -> None:
        self.__url = (
            "https://storage.googleapis.com/mapbiomas-public/initiatives/brasil/"
            "collection_9/lclu/coverage/brasil_coverage_{year}.tif"
        )

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

    def request_from_page(self, years: list, destination_path: str):
        """
        Requests files from a page and saves them to the specified destination path.

        Args:
            years (list): A list of years for which files will be requested from the page.
            destination_path (str): The path where the files will be saved.
        """
        destination_dir = os.path.abspath(destination_path)
        for year in tqdm(years):
            filename = f"brasil_coverage_{year}.tif"
            file_path = os.path.join(destination_dir, filename)
            if not os.path.exists(file_path):
                response = requests.get(self.__url.format(year=year), timeout=10)
                self.__save_file(response, file_path)
            else:
                write_log(
                    f"File {file_path} already exists in destination.", level="warning"
                )


class HttpRequesterOvertureMaps:
    """
    Http request class to download Overture Maps data
    """

    def __init__(
        self, theme: str, download_path: str, lines_per_file: int = 1000000
    ) -> None:
        self.path = (
            "s3://overturemaps-us-west-2/release/"
            f"{OVERTURE_RELEASE_VERSION}/theme={theme}/type=*/*"
        )
        self.download_path = download_path
        self.db_con = db.connect()
        self.db_con.install_extension("spatial")
        self.db_con.load_extension("spatial")
        self.db_con.install_extension("httpfs")
        self.db_con.load_extension("httpfs")
        self.db_con.sql("SET s3_region='us-west-2'")
        self.lines_per_file = lines_per_file
        os.makedirs(download_path, exist_ok=True)

    def download_data(self, cols: list):
        """
        Downloads data from a database and saves it into Parquet files.
        Args:
            cols (list): A list of column names to be selected from the database.
        """
        cols = ", ".join(cols)
        offset = 0
        file_count = 0

        while True:
            download_file_path = os.path.join(
                self.download_path, f"{file_count}.parquet"
            )
            self.db_con.sql(
                f"""
                    COPY (
                        SELECT
                            {cols}
                        FROM
                            read_parquet('{self.path}', filename=true, hive_partitioning=1)
                        WHERE
                            bbox.xmin  > {BBOX_BRAZIL["xmin"]}
                            AND bbox.xmax  < {BBOX_BRAZIL["xmax"]}
                            AND bbox.ymin  > {BBOX_BRAZIL["ymin"]}
                            AND bbox.ymax  < {BBOX_BRAZIL["ymax"]}
                        LIMIT {self.lines_per_file}
                        OFFSET {offset}
                        ) TO '{download_file_path}'
                        (FORMAT PARQUET);
                """
            )
            if os.path.getsize(download_file_path) == 0:
                os.remove(download_file_path)
                break
            offset += self.lines_per_file
            file_count += 1

        self.db_con.close()

    def download_buildings_omf(self):
        """
        Requests buildings from a page and saves them to the specified destination path.
        """
        cols = [
            "id",
            "ST_AsText(ST_GeomFromWKB(geometry)) as geometry",
            "subtype",
            "JSON(names) as names",
            "JSON(sources) as sources",
            "class",
            "level",
            "has_parts",
            "height",
            "num_floors",
            "min_height",
            "min_floor",
        ]
        self.download_data(cols)

    def download_places_omf(self):
        """
        Requests places from a page and saves them to the specified destination path.
        """
        cols = [
            "id",
            "ST_AsText(ST_GeomFromWKB(geometry)) as geometry",
            "subtype",
            "JSON(names) as names",
            "JSON(sources) as sources",
            "class",
            "level",
            "has_parts",
            "height",
            "num_floors",
            "min_height",
            "min_floor",
        ]
        self.download_data(cols)
