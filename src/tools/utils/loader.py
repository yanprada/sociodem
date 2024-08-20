"""
This module contains a utility class for loading data from a database.

Classes:
    Loader: A utility class for loading data from a database.

    get_muns_cod(): Retrieves a list of distinct municipality codes 
        from the specified database table.

"""

from functools import lru_cache
import geopandas as gpd


from src.tools.databases.data_connection.connection import DBConnection
from src.tools.utils.constants import CRS_GLOBAL, CRS_IBGE
from src.tools.utils.common import get_db_path
from src.tools.data_contract.censo_data_contract import get_censo_contracts
from src.tools.data_contract.pois_data_contract import get_pois_contracts

CONTRACT_CENSO_BRONZE = get_censo_contracts("bronze")
CONTRACT_POIS_BRONZE = get_pois_contracts("bronze")


class Loader:
    """
    A utility class for loading data from a database.
    Methods:
        get_sc(): Get the file from the physical path.
        get_muns_cod(): Retrieves a list of distinct municipality
            codes from the specified database table.
    """

    def __init__(self) -> None:
        pass

    @staticmethod
    def get_sc():
        """
        Get the file from the physical path.
        """
        conn = DBConnection("bronze")
        path = get_db_path(CONTRACT_CENSO_BRONZE["sectors_2022"])
        df = conn.query_database(f"SELECT cd_setor, geometry FROM {path}", geo=False)
        df["geometry"] = gpd.GeoSeries.from_wkb(df["geometry"])
        df = gpd.GeoDataFrame(df, geometry="geometry", crs=CRS_IBGE).to_crs(CRS_GLOBAL)
        return df

    @staticmethod
    @lru_cache(maxsize=1)
    def get_muns_cod():
        """
        Retrieves a list of distinct municipality codes from the specified database table.

        Returns:
            list: A list of distinct municipality codes.
        """
        contract_dompp = CONTRACT_CENSO_BRONZE["dompp_2022"]
        path = get_db_path(contract_dompp)
        conn = DBConnection("bronze")
        query = f"SELECT DISTINCT cod_mun FROM {path}"
        return conn.query_database(query)["cod_mun"].tolist()

    @staticmethod
    def get_places():
        """
        Retrieves a list of distinct municipality codes from the specified database table.

        Returns:
            list: A list of distinct municipality codes.
        """
        contract_dompp = CONTRACT_POIS_BRONZE["pois"]
        path = get_db_path(contract_dompp)
        conn = DBConnection("bronze")
        query = f"SELECT cd_mun, nm_mun, general_category, hex_col FROM {path}"
        return conn.query_database(query)
