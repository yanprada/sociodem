"""
This module contains the pipeline for processing buildings data.

The pipeline consists of the following steps:
1. Downloading buildings data
2. Indexing H3 polygons
3. Creating grouped by hex buildings

To execute the pipeline, run the main() function.
"""

from src.databases.bronze.buildings.steps import (
    download_buildings,
    group_buildings_by_hex,
    index_h3_polygon,
)


def main():
    """
    This is the main function of the pipeline module.
    It executes the necessary steps to process the buildings data.
    """
    download_buildings.main()
    index_h3_polygon.main()
    group_buildings_by_hex.main()
