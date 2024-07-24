"""
This module contains the pipeline for processing H3 hexagon data using MapBiomas data.

The pipeline consists of the following steps:
1. Downloads the MapBiomas data.
2. Transforms the data into a pandas DataFrame.
3. Processes the H3 hexagon data.
4. Creates a grouped-by-hex mapbiomas.

To run the pipeline, execute the `main` function.
"""

from src.databases.bronze.mapbiomas.steps import (
    transform_to_dataframe,
    create_grouped_by_hex_mapbiomas,
    download_mapbiomas,
    process_h3_hexagon,
)


def main():
    """
    This is the main function that processes the H3 hexagon data.

    It executes the following steps:
    1. Downloads the MapBiomas data.
    2. Transforms the data into a pandas DataFrame.
    3. Processes the H3 hexagon data.
    4. Creates a grouped-by-hex mapbiomas.
    """
    download_mapbiomas.main()
    transform_to_dataframe.main()
    process_h3_hexagon.main()
    create_grouped_by_hex_mapbiomas.main()
