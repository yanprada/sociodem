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
    a_download_mapbiomas,
    b_transform_to_dataframe,
    c_process_h3_hexagon,
    d_create_grouped_by_hex_mapbiomas,
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
    a_download_mapbiomas.main()
    b_transform_to_dataframe.main()
    c_process_h3_hexagon.main()
    d_create_grouped_by_hex_mapbiomas.main()
