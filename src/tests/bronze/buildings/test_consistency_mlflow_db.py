"""
This module provides functions to retrieve and process data from MLflow and a database,
merge the data, and calculate differences and rates between specific columns.
Functions:
    get_mlflow():
        Returns a pandas DataFrame with columns 'year', 'state', and 'dompp_sum'.
    get_db():
    main():
        Merges data from MLflow and a database, calculates the difference and rate
        between two columns, and returns the resulting DataFrame.

"""

import pandas as pd

from src.databases.bronze.buildings.google.steps.b_process_image_to_hex import (
    EXPERIMENT_ID,
)
from src.tools.utils.common import get_ml_flow_data


def get_mlflow():
    """
    Retrieves and processes MLflow data for a specific experiment.
    This function fetches MLflow data for a given experiment ID, filters out rows
    with missing values in the 'dompp_sum' column, extracts the year and state
    information from the 'mlflow.runName' column, and returns a DataFrame with
    the columns 'year', 'state', and 'dompp_sum'.
    Returns:
        pandas.DataFrame: A DataFrame containing the processed MLflow data with
        columns 'year', 'state', and 'dompp_sum'.
    """
    df = get_ml_flow_data(EXPERIMENT_ID).dropna(subset="dompp_sum")
    df.loc[:, "year"] = df.loc[:, "mlflow.runName"].str.split("_").str[1].astype(int)
    df.loc[:, "state"] = df.loc[:, "mlflow.runName"].str.split("_").str[0]
    return df.loc[:, ["year", "state", "dompp_sum"]]


def get_db():
    """
    Reads the 'buildings_val.csv' file and returns it as a pandas DataFrame.
    Returns:
        pd.DataFrame: The contents of the 'buildings_val.csv' file.
    """
    return pd.read_csv("buildings_val.csv")


def main():
    """
    Merges data from MLflow and a database, calculates the difference and rate between two columns.
    This function retrieves data from MLflow and a database, merges them on the columns
    "year" and "state" using an outer join,
    and then calculates the difference and rate between the columns "dompp_sum" and "sum".
    Returns:
        pd.DataFrame: A DataFrame containing the merged data along with the calculated
        "diff" and "rate" columns.
    """
    df_mlflow = get_mlflow()
    df_db = get_db()
    df = pd.merge(df_mlflow, df_db, on=["year", "state"], how="outer")
    df["diff"] = df["dompp_sum"] - df["sum"]
    df["rate"] = df["sum"] / df["dompp_sum"]
    return df
