"""
Module: video1.py

This module contains functions for processing and analyzing census data related to
the percentage of black and brown residents in the city of São Paulo.

Functions:
- get_sc_geom(): Retrieves the geometries for the SC 2010 dataset.
- get_censo_data(): Reads the census data from a CSV file and returns a DataFrame.
- transform_data(df: pd.DataFrame) -> pd.DataFrame: Transforms the data in the given DataFrame.
- calculate_pct(df: pd.DataFrame) -> pd.DataFrame: Calculate the percentage of black and
     brown residents in a DataFrame.
- make_fake_data(df: pd.DataFrame) -> pd.DataFrame: Function to make fake data by shuffling the
     values in the 'pct_preta_parda' column of a DataFrame.
- plot_hist(df: pd.DataFrame, label: str) -> None: Plot a histogram of the percentage of 
    black and brown people by census sector in the city of São Paulo.
- main(): Performs various steps including data retrieval, transformation, calculation, 
    merging, plotting, and saving.

Note: This module requires the pandas and matplotlib libraries.
"""

import pandas as pd

import matplotlib.pyplot as plt

from src.tools.utils.loader import Loader

cols = {
    "Cod_setor": "cod_setor",
    "V001": "pessoas_residentes",
    "V003": "pessoas_residentes_preta",
    "V005": "pessoas_residentes_parda",
}


def get_sc_geom():
    """
    Retrieves the geometries for the SC 2010 dataset.

    Returns:
        pandas.DataFrame: A DataFrame containing the geometries for the SC 2010 dataset.
    """
    df = Loader().get_sc_2010()
    df["cod_setor"] = df["cod_setor"].astype(str)
    return df


def get_censo_data():
    """
    Reads the census data from a CSV file and returns a DataFrame.
    Returns:
        pandas.DataFrame: The census data as a DataFrame.
    """

    return pd.read_csv("sandbox/video1/input/pessoa03_sp1.csv", sep=";").rename(
        columns=cols
    )[cols.values()]


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the data in the given DataFrame.
    Parameters:
        df (pd.DataFrame): The DataFrame containing the data to be transformed.
    Returns:
        pd.DataFrame: The transformed DataFrame.
    """

    for col in cols.values():
        if col == "cod_setor":
            df[col] = df[col].astype(str)
            continue
        df[col] = df[col].astype(str).str.replace("X", "0").astype(int)
    return df


def calculate_pct(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the percentage of black and brown residents in a DataFrame.
    Parameters:
        df (pd.DataFrame): The input DataFrame containing the following columns:
            - pessoas_residentes_preta (int): Number of black residents.
            - pessoas_residentes_parda (int): Number of brown residents.
            - pessoas_residentes (int): Total number of residents.
    Returns:
        pd.DataFrame: The input DataFrame with the following columns dropped:
            - pessoas_residentes_preta
            - pessoas_residentes_parda
            - pessoas_residentes
    """

    df["pct_preta_parda"] = (
        df["pessoas_residentes_preta"] + df["pessoas_residentes_parda"]
    ) / df["pessoas_residentes"]
    return df.drop(
        columns=[
            "pessoas_residentes_preta",
            "pessoas_residentes_parda",
            "pessoas_residentes",
        ]
    )


def make_fake_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to make fake data by shuffling the values in the 'pct_preta_parda'
    column of a DataFrame.
    Parameters:
    - df (pd.DataFrame): The input DataFrame.
    Returns:
    - pd.DataFrame: The DataFrame with shuffled values in the 'pct_preta_parda' column.
    """

    df["pct_preta_parda"] = df["pct_preta_parda"].sample(frac=1).reset_index(drop=True)
    return df


def plot_hist(df: pd.DataFrame, label: str) -> None:
    """
    Plot a histogram of the percentage of black and brown people
    by census sector in the city of São Paulo.

    Parameters:
    - df (pd.DataFrame): The DataFrame containing the data.
    - label (str): The label to be used in the output file name.
    """
    (100 * df["pct_preta_parda"]).plot.hist()
    plt.title(
        "Porcentagem de pessoas pretas e pardas\npor setor censitário na cidade de São Paulo"
    )
    plt.xlabel("Porcentagem pessoas pretas e pardas (%)")
    plt.ylabel("Frequência")
    plt.tight_layout()
    plt.xticks(range(0, 101, 10))
    plt.savefig(f"sandbox/video1/output/histogram_{label}.png")


def main():
    """
    This function performs the following steps:
    1. Retrieves census data using the `get_censo_data` function.
    2. Transforms the data using the `transform_data` function.
    3. Calculates the percentage using the `calculate_pct` function.
    4. Retrieves the geometry data using the `get_sc_geom` function.
    5. Merges the census data with the geometry data on the "cod_setor" column.
    6. Creates fake data using the `make_fake_data` function.
    7. Plots a histogram of the real data using the `plot_hist` function.
    8. Plots a histogram of the fake data using the `plot_hist` function.
    9. Saves the fake data to a CSV file named "sandbox/video1/output/pop_negra_parda_sp_fake.csv".
    10. Saves the real data to a CSV file named "sandbox/video1/output/pop_negra_parda_sp_real.csv".
    """

    df = get_censo_data().pipe(transform_data).pipe(calculate_pct)
    df_sc = get_sc_geom()
    df = df.merge(df_sc, on="cod_setor", how="left")
    df_fake = make_fake_data(df.copy())
    plot_hist(df, "real")
    plot_hist(df_fake, "fake")
    df_fake.to_csv("sandbox/video1/output/pop_negra_parda_sp_fake.csv")
    df.to_csv("sandbox/video1/output/pop_negra_parda_sp_real.csv")
