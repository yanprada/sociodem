"""
This module contains the main function for processing places in Brazil.
"""

from src.databases.bronze.pois.steps import (
    download_places,
    process_places_bbox,
    process_places_brazil,
)


def main():
    """
    This function is the entry point for processing places in Brazil.
    It calls the necessary steps in the pipeline to download and process the places data.
    """
    download_places.main()
    process_places_bbox.main()
    process_places_brazil.main()
