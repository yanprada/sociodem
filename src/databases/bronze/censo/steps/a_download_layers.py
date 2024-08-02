"""
This module contains functions to download layers from the Censo dataset.
The main functions are `download_info_censo_2010()` and `download_info_censo_2022()`,
which serve as entry points for downloading layers for the years 2010 and 2022, respectively.
The functions use the `HttpRequesterCenso` class to make HTTP requests and retrieve the data.
"""

from itertools import product
from src.tools.data_contract.aneel_data_contract import get_contract
from src.tools.databases.data_request.drivers.http_requester import (
    HttpRequesterCenso,
)
from src.tools.utils.constants import STATES

CONTRACT_LAYERS_CENSO_2010 = get_contract(
    "censo/contract_layers_censo_2010.yaml", "bronze"
)
CONTRACT_LAYERS_CENSO_2022 = get_contract(
    "censo/contract_layers_censo_2022.yaml", "bronze"
)
CONTRACT_DOMPP_CENSO_2022 = get_contract(
    "censo/contract_dompp_censo_2022.yaml", "bronze"
)


def download_info_censo_2010():
    """
    This function is the entry point of the script and is responsible for downloading
    layers from the Censo 2010 dataset.
    It uses the `HttpRequesterCenso` class to make HTTP requests and
    retrieve the data.
    """

    censo_request = HttpRequesterCenso(2010)
    download_layers_censo_2010(censo_request)


def download_layers_censo_2010(censo_request: HttpRequesterCenso):
    """
    Downloads layers for the Censo 2010 dataset.

    Args:
        censo_request (HttpRequesterCenso): An instance of the HttpRequesterCenso class.
    """
    combinations = product(
        STATES.keys(),
        ["setores_censitarios", "subdistritos", "distritos", "municipios"],
    )
    path_to_save = CONTRACT_LAYERS_CENSO_2010["physicalPath"]
    censo_request.request_layers_from_page(combinations, path_to_save)


def download_info_censo_2022():
    """
    This function is the entry point of the script and is responsible for downloading
    layers from the Censo 2022 dataset.
    It uses the `HttpRequesterCenso` class to make HTTP requests and
    retrieve the data.
    """

    censo_request = HttpRequesterCenso(2022)
    download_layers_censo_2022(censo_request)
    download_dompp_censo_2022(censo_request)


def download_layers_censo_2022(censo_request: HttpRequesterCenso):
    """
    Downloads layers for the Censo 2022.

    Args:
        censo_request (HttpRequesterCenso): An instance of the HttpRequesterCenso class.

    """
    combinations = product(
        STATES.keys(),
        ["setores", "subdistritos", "distritos", "municipios"],
    )
    path_to_save = CONTRACT_LAYERS_CENSO_2022["physicalPath"]
    censo_request.request_layers_from_page(combinations, path_to_save)


def download_dompp_censo_2022(censo_request: HttpRequesterCenso):
    """
    Downloads the DOMPP (Documento Oficial do Ministério da Educação) for the Censo 2022.

    This function sends a request to the censo_request object to download the DOMPP from
    the specified page.
    The function takes the following parameters:
    - censo_request: An instance of the HttpRequesterCenso class used to make the request.

    Example usage:
    censo_request = HttpRequesterCenso()
    download_dompp_censo_2022(censo_request)

    :param censo_request: An instance of the HttpRequesterCenso class.
    :type censo_request: HttpRequesterCenso
    """
    path_to_save = (
        CONTRACT_DOMPP_CENSO_2022["physicalPath"]
        .replace("databases", "datalake")
        .replace("bronze/", "")
    )
    censo_request.request_dompp_from_page(STATES, path_to_save)


def main():
    """
    This is the main function that downloads the layers for the Censo dataset.
    It calls the functions to download the layers for the years 2010 and 2022.
    """
    download_info_censo_2010()
    download_info_censo_2022()
