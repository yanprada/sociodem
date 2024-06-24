"""
This module contains the pipeline for processing ANEEL data.

The pipeline consists of three steps:
1. Downloading ANEEL companies data
2. Creating a bronze ANEEL dataset in Parquet format
3. Creating a bronze ANEEL dataset in PostgreSQL database

To run the pipeline, call the `run()` function.
"""

from src.databases.bronze.aneel import (
    download_aneel_companies,
    make_bronze_aneel_dataset,
)


def run():
    """
    Runs the ANEEL data processing pipeline.

    This function executes the following steps:
    1. Downloads ANEEL companies data
    2. Creates a bronze ANEEL dataset in Parquet format
    3. Creates a bronze ANEEL dataset in PostgreSQL database
    """
    download_aneel_companies.main()
    make_bronze_aneel_dataset.main()
