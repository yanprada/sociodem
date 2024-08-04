"""
This module contains the main function for processing places in Brazil.
"""

from src.databases.bronze.pois.steps import (
    a_download_places,
    b_process_places_bbox,
    c_process_places_brazil,
)


def main():
    """
    This function is the entry point for processing places in Brazil.
    It calls the necessary steps in the pipeline to download and process the places data.
    """
    a_download_places.main()
    b_process_places_bbox.main()
    c_process_places_brazil.main()
