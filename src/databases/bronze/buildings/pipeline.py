"""
This module contains the pipeline for processing buildings data.

The pipeline consists of the following steps:
1. Downloading buildings data
2. Indexing H3 polygons
3. Creating grouped by hex buildings

To execute the pipeline, run the main() function.
"""

from src.databases.bronze.buildings.steps import (
    a_download_buildings,
    b_add_h3_and_sc_index,
    c_group_buildings_by_hex_sc,
)


def main():
    """
    This is the main function of the pipeline module.
    It executes the necessary steps to process the buildings data.
    """
    a_download_buildings.main()
    b_add_h3_and_sc_index.main()
    c_group_buildings_by_hex_sc.main()
