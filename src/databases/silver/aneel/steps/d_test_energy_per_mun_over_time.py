"""
This script groups energy consumption data by municipality and year,
saves the results to a database, and generates plots for each municipality.
It connects to a database, retrieves data, processes it, and saves the results.
"""

import os
from tqdm import tqdm
import pandas as pd
import matplotlib.pyplot as plt

from src.tools.databases.data_connection.connection import DBConnection

from src.tools.utils.common import get_db_path, write_log
from src.databases.silver.aneel.config import manager, CONTRACT_SILVER_ENERGY, YEARS

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


def save_plot_each_row(df, output_dir):
    """
    Plots each row of the dataframe and saves the plot as an image.

    Parameters:
    - df: DataFrame containing the data to plot.
    - output_dir: Directory where the plots will be saved.
    """
    os.makedirs(output_dir, exist_ok=True)
    for _, row in tqdm(df.iterrows(), desc="Saving plots", total=len(df)):
        cd_mun = row["cd_mun"]
        years = df.columns[1:]  # Exclude 'cd_mun' column
        values = row[1:]  # Exclude 'cd_mun' value

        plt.figure()
        plt.plot(years, values, marker="o")
        plt.title(f"Energy Consumption for Municipality {cd_mun}")
        plt.xlabel("Year")
        plt.ylabel("Energy Consumption")
        plt.grid(True)

        output_path = os.path.join(output_dir, f"{cd_mun}.png")
        plt.savefig(output_path)
        plt.close()


def main():
    """
    Main function to execute the script.
    """
    conn = DBConnection("silver")
    dfs = []
    cols = [
        "energy_consumption_agropecuaria",
        "energy_consumption_comercial",
        "energy_consumption_condominios",
        "energy_consumption_industrial",
        "energy_consumption_outros",
        "energy_consumption_poder_publico",
        "energy_consumption_residencial_rural",
        "energy_consumption_residencial_urbano",
        "energy_consumption_residencial_urbano_baixa_renda",
        "energy_consumption_rodovias",
        "energy_consumption_servicos_publicos",
    ]
    sum_cols_str = "+".join(cols)
    for year in tqdm(YEARS, desc="Processing years"):
        path = get_db_path(CONTRACT_SILVER_ENERGY[f"aneel_kring_hexagon_grp_{year}"])
        query = f"""
                SELECT cd_mun, SUM({sum_cols_str}) as energy_consumption
                FROM {path}
                GROUP BY cd_mun
            """
        result = conn.execute_query(query)
        df = pd.DataFrame(result, columns=["cd_mun", "energy_consumption"])
        df["year"] = year
        dfs.append(df)
    conn.close()
    df = pd.concat(dfs, ignore_index=True)
    df = (
        df.pivot(index="cd_mun", columns="year", values="energy_consumption")
        .reset_index()
        .fillna(0)
    )
    output_dir = "plots/aneel/energy_data_muns"
    save_plot_each_row(df, output_dir)
    write_log("Plots saved successfully.")
