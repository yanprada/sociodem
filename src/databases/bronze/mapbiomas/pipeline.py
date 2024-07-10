"""
This module contains the pipeline for processing H3 hexagon data from the MapBiomas database.

The pipeline consists of three main steps:
1. Downloading the MapBiomas data.
2. Transforming the data into a pandas DataFrame.
3. Processing the H3 hexagon data.

To run the pipeline, execute the `main` function.
"""

from src.databases.bronze.mapbiomas import (
    download_mapbiomas,
    transform_to_dataframe,
    process_h3_hexagon,
    create_grouped_by_hex_mapbiomas,
)


def main():
    """
    This is the main function that processes the H3 hexagon data.

    It executes the following steps:
    1. Downloads the MapBiomas data.
    2. Transforms the data into a pandas DataFrame.
    3. Processes the H3 hexagon data.
    """
    download_mapbiomas.main()
    transform_to_dataframe.main()
    process_h3_hexagon.main()
    create_grouped_by_hex_mapbiomas.main()
