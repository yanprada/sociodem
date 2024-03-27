"""
Module to make requests to pages.
"""

import requests


class HttpRequesterAneel:
    """
    Http request class to download data from Aneel
    """

    def __init__(self) -> None:
        self.__url = "https://www.arcgis.com/sharing/rest/content/items/{id}/data"

    def request_from_page(self, id_param: str):
        """Method to request Aneel data from website.

        Args:
           id_param (str): id of Aneel database. This id comes from a file with all the single
                    ids for companies that Aneel is related.
                    ex: CPFL -> id_param = ''

        Returns:
            _type_: _description_
        """
        response = requests.get(self.__url.format(id=id_param), timeout=10)
        return response

    def update(self):
        """_summary_"""
