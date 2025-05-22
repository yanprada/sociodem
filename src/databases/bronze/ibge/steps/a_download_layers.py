"""
This module contains functions to download layers from the IBGE dataset.
The main functions are `download_info_ibge_2010()` and `download_info_ibge_2022()`,
which serve as entry points for downloading layers for the years 2010 and 2022, respectively.
The functions use the `HttpRequesterIBGE` class to make HTTP requests and retrieve the data.
"""

import os
from itertools import product


from src.tools.databases.data_request.drivers.http_requester import (
    HttpRequesterIBGE,
)
from src.tools.utils.constants import STATES
from src.databases.bronze.ibge.config import (
    manager,
    CONTRACTS_RAW,
)


module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


def download_info_ibge_2010():
    """
    This function is the entry point of the script and is responsible for downloading
    layers from the IBGE 2010 dataset.
    It uses the `HttpRequesterIBGE` class to make HTTP requests and
    retrieve the data.
    """

    ibge_request = HttpRequesterIBGE(2010)
    download_layers_ibge_2010(ibge_request)


def download_layers_ibge_2010(ibge_request: HttpRequesterIBGE):
    """
    Downloads layers for the IBGE 2010 dataset.

    Args:
        ibge_request (HttpRequesterIBGE): An instance of the HttpRequesterIBGE class.
    """
    combinations = product(
        STATES.keys(),
        ["setores_censitarios", "subdistritos", "distritos", "municipios"],
    )
    path_to_save = CONTRACTS_RAW["mun_2010"]["physicalPath"]
    ibge_request.request_layers_from_page(combinations, path_to_save)


def download_info_ibge_2022():
    """
    This function is the entry point of the script and is responsible for downloading
    layers from the IBGE 2022 dataset.
    It uses the `HttpRequesterIBGE` class to make HTTP requests and
    retrieve the data.
    """

    ibge_request = HttpRequesterIBGE(2022)
    download_layers_ibge_2022(ibge_request)
    download_dompp_ibge_2022(ibge_request)
    download_states_ibge_2022(ibge_request)


def download_layers_ibge_2022(ibge_request: HttpRequesterIBGE):
    """
    Downloads layers for the IBGE 2022.

    Args:
        ibge_request (HttpRequesterIBGE): An instance of the HttpRequesterIBGE class.

    """
    combinations = product(
        STATES.keys(),
        ["setores", "subdistritos", "distritos", "municipios"],
    )
    path_to_save = CONTRACTS_RAW["mun_2022"]["physicalPath"]
    ibge_request.request_layers_from_page(combinations, path_to_save)


def download_dompp_ibge_2022(ibge_request: HttpRequesterIBGE):
    """
    Downloads the DOMPP (Documento Oficial do Ministério da Educação) for the IBGE 2022.

    This function sends a request to the ibge_request object to download the DOMPP from
    the specified page.
    The function takes the following parameters:
    - ibge_request: An instance of the HttpRequesterIBGE class used to make the request.

    Example usage:
    ibge_request = HttpRequesterIBGE()
    download_dompp_ibge_2022(ibge_request)

    :param ibge_request: An instance of the HttpRequesterIBGE class.
    :type ibge_request: HttpRequesterIBGE
    """
    path_to_save = CONTRACTS_RAW["dompp_2022"]["physicalPath"]
    ibge_request.request_dompp_from_page(STATES, path_to_save)


def download_states_ibge_2022(ibge_request: HttpRequesterIBGE):
    """
    Downloads the states for the IBGE 2022 dataset.

    Args:
        ibge_request (HttpRequesterIBGE): An instance of the HttpRequesterIBGE class.
    """
    path_to_save = "".join([CONTRACTS_RAW["mun_2022"]["physicalPath"], "estados/"])
    ibge_request.request_states_from_page(STATES, path_to_save)


def main():
    """
    This is the main function that downloads the layers for the IBGE dataset.
    It calls the functions to download the layers for the years 2010 and 2022.
    """
    download_info_ibge_2010()
    download_info_ibge_2022()
    manager.update_last_run()
