"""
This script is used to download AlphaEarth data for a specified range of years.

It imports the necessary modules and defines the CONTRACT variable, which contains the contract
information for downloading AlphaEarth data. The main() function is the entry point of the script,
which initializes an instance of HttpRequesterAlphaEarth and makes a request to download data
for the specified range of years using the contract defined in CONTRACT.
"""

import os
import pandas as pd
from tqdm import tqdm
from shapely import wkb
from src.tools.managers.http_requester import (
    HttpRequesterAlphaEarth,
)
from src.tools.managers.db_connector import DBConnection
from src.tools.utils.common import get_db_path

from src.databases.bronze.alphaearth.config import (
    CONTRACTS_RAW,
    CONTRACTS_BRONZE,
    YEARS,
    manager,
    GEE_KEY_FILE,
    GEE_SERVICE_ACCOUNT,
)

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


def get_processed_files(output_path: str) -> list[str]:
    """
    This function retrieves the list of processed files (cities)
    that have data for all specified years in the given output path.
    """
    cities_per_year = []
    for year in YEARS:
        year_path = output_path.replace("{year}", str(year))
        cities = {entry.name for entry in os.scandir(year_path) if entry.is_dir()}
        cities_per_year.append(cities)

    if not cities_per_year:
        return []

    # Find the intersection of cities present in all years
    return list(set.intersection(*cities_per_year))


def get_cities_states_dataframe() -> pd.DataFrame:
    """
    This function retrieves a DataFrame containing the names of
    cities and their corresponding states from the database.
    """
    with DBConnection("bronze") as conn:
        contract_mapbiomas = CONTRACTS_BRONZE["mun_2022"]
        db_path = get_db_path(contract_mapbiomas)
        query = f"""
        select nm_mun, sigla_uf from {db_path}
        """
        df = conn.query_database(query)
    return df


def get_city_geom(city: str, state: str) -> str:
    """
    This function retrieves the geometry of a specified city
    and state from the database.
    """
    with DBConnection("bronze") as conn:
        contract_muns_ibge = CONTRACTS_BRONZE["mun_2022"]
        db_path = get_db_path(contract_muns_ibge)
        query = f"""
        select * from {db_path} where nm_mun = '{city}' and sigla_uf = '{state}';
        """
        df = conn.query_database(query)
    return wkb.loads(df["geometry"].iloc[0])


def main() -> None:
    """
    This function is the entry point of the script for downloading Mapbiomas data.
    It initializes an instance of HttpRequesterMapbiomas and makes a request to download data
    for the specified range of years using the contract defined in CONTRACT.
    """
    requester = HttpRequesterAlphaEarth(GEE_SERVICE_ACCOUNT, GEE_KEY_FILE)
    cities_states = get_cities_states_dataframe()
    random_year = YEARS[0]
    output_path = CONTRACTS_RAW[f"brasil_embeddings_{random_year}"][
        "physicalPath"
    ].replace(str(random_year), "{year}")
    processed_files = get_processed_files(output_path)
    for city, state in tqdm(
        cities_states.itertuples(index=False), desc="Cities", total=len(cities_states)
    ):
        if city in processed_files or state not in ["SP", "RJ", "MG", "ES"]:
            continue
        city_geom = get_city_geom(city, state)
        requester.request_from_page(city_geom, YEARS, city, state, output_path)
    manager.update_last_run()


if __name__ == "__main__":
    main()
