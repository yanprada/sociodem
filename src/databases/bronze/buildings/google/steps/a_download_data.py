"""
Module for downloading and processing geospatial data related to buildings from Google.

This module contains functions and classes to read geospatial files, convert their coordinate
reference system (CRS) to a global CRS, and save the resulting geometries. It uses an execution
manager to handle execution parameters and status updates.

Functions:
    save_states_geom(reader: Reader, file: str) -> GeoDataFrame:

    main():
        Main function to read state contract files and save their geometries. This function
        initializes a Reader object, retrieves the path for state contracts for the year 2022,
        and processes each file in the directory. For each file, it calls the `save_states_geom`
        function to save the geometries.

"""

import os
from tqdm import tqdm

from src.tools.utils.reader import Reader
from src.tools.utils.constants import CRS_GLOBAL
from src.tools.utils.save import save_parquet_decorator


from src.databases.bronze.buildings.google.config import (
    manager,
    STATE_CONTRACTS_RAW,
    BUILDING_CONTRACTS_RAW,
)

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


@save_parquet_decorator("bronze", save_db=False)
def save_states_geom(reader: Reader, file: str, **kwargs):
    """
    Reads a geospatial file, converts its coordinate reference system (CRS) to a global CRS,
    and returns the resulting GeoDataFrame.
    Args:
        reader (Reader): A Reader object to read geospatial files.
        file (str): The path to the geospatial file to be read.
        **kwargs: Additional keyword arguments.
    Returns:
        GeoDataFrame: A GeoDataFrame with the geometries transformed to the global CRS.
    """
    gdf = reader.read_geofile(file)
    gdf = gdf.to_crs(CRS_GLOBAL)
    return gdf


def main():
    """
    Main function to read state contract files and save their geometries.
    This function initializes a Reader object, retrieves the path for state contracts
    for the year 2022, and processes each file in the directory. For each file, it
    calls the `save_states_geom` function to save the geometries.
    Steps:
    1. Initialize a Reader object.
    2. Retrieve the path for state contracts for the year 2022.
    3. List all files in the directory.
    4. For each file, call `save_states_geom` to save the geometries.
    Note:
    - The `STATE_CONTRACTS` dictionary and `save_states_geom`
        function should be defined elsewhere in the code.
    - Ensure the directory path and files are correctly set up before running this function.
    """
    reader = Reader()
    path = STATE_CONTRACTS_RAW["states_2022"]["physicalPath"]
    files = [
        os.path.join(path, file)
        for file in os.listdir(path)
        if os.path.isfile(os.path.join(path, file))
    ]
    for file in tqdm(files, desc="Processing files"):
        kwargs = {
            "filename": file.replace(".geojson", "").replace(path, ""),
            "contract": BUILDING_CONTRACTS_RAW["buildings_google"],
        }
        _ = save_states_geom(reader, file, **kwargs)
    manager.update_last_run()
