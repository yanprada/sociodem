"""
This module contains the Censo data processing pipeline.

The pipeline consists of the following steps:
1. Downloads Censo data
2. Creates a bronze Censo dataset in Parquet format and also in PostgresSQL database
"""

from src.databases.bronze.censo.steps import download_layers, make_bronze_dataset_censo


def main():
    """
    Runs the Censo data processing pipeline.

    This function executes the following steps:
    1. Downloads Censo data
    2. Creates a bronze Censo dataset in Parquet format and also in PostgresSQL database
    """
    download_layers.main()
    make_bronze_dataset_censo.main()
