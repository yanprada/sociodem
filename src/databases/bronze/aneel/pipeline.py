"""
This module contains the pipeline for processing ANEEL data.

The pipeline consists of two steps:
1. Downloading ANEEL companies data
2. Creating a bronze ANEEL dataset in Parquet and PostgreSQL format

To run the pipeline, call the `run()` function.
"""

from src.databases.bronze.aneel.steps import (
    a_download_aneel_companies,
    b_make_bronze_aneel_dataset,
)


def run():
    """
    Runs the ANEEL data processing pipeline.

    This function executes the following steps:
    1. Downloads ANEEL companies data
    2. Creates a bronze ANEEL dataset in Parquet format and also in PostgresSQL database
    """
    a_download_aneel_companies.main()
    b_make_bronze_aneel_dataset.main()
