"""
This module contains the pipeline for processing MapBiomas data.

The pipeline consists of the following steps:
1. Creates a silver MapBiomas dataset in Parquet format and also in PostgresSQL database.
"""

from src.databases.silver.mapbiomas.steps import a_create_silver_mapbiomas


def main():
    """
    Runs the MapBiomas data processing pipeline.

    This function executes the following steps:
    1. Creates a silver MapBiomas dataset in Parquet format and also in PostgresSQL database.
    """
    a_create_silver_mapbiomas.main()
