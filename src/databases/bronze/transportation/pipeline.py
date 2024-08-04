"""
This module contains the main function for processing places in Brazil.
"""

from src.databases.bronze.transportation.steps import (
    a_download_transportation,
    b_process_transportation_bbox,
    c_process_transportation_brazil,
)


def main():
    """
    This function is the entry point for processing transportation in Brazil.
    It calls the necessary steps in the pipeline to download and process the transportation data.
    """
    a_download_transportation.main()
    b_process_transportation_bbox.main()
    c_process_transportation_brazil.main()
