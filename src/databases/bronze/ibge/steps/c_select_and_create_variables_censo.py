"""
This module is responsible for selecting and creating variables from the Censo dataset.
It defines lists of variables and formulas for different categories such as basic information,
demographics, household characteristics, and family relationships. These variables and formulas
are used to process and analyze census data.
"""

import os

# from src.tools.databases.data_connection.connection import DBConnection
# from src.tools.utils.common import write_log, get_db_path

from src.databases.bronze.ibge.config import (
    manager,
)

module_name = os.path.basename(__file__).replace(".py", "")
manager.update_status(module_name)


basico = [
    "cd_setor",
    "situacao",
    "area_km2",
    "cd_uf",
    "cd_mun",
    "nm_bairro",
    "nm_municipio",
    "nm_uf",
    "nm_regiao",
    "nm_microregiao",
    "nm_rgint",
    "total_pessoas",
]
basico = [f"a.{col}" for col in basico]
basico_formulas = {"densidade_demografica": "total_pessoas / area_km2"}

demografia = [
    "qtd_moradores",
    "v01007_sxM",
    "v01008_sxF",
    "0_4_anos",
    "5_a_9_anos",
    "10_14_anos",
    "v01034_15_19_anos",
    "v01035_20_24_anos",
    "v01036_25_29_anos",
    "30_39_anos",
    "40_49_anos",
    "50_59_anos",
    "60_69_anos",
    "70_anos_ou_mais",
]
demografia = [f"b.{col}" for col in demografia]
demografia_formulas = {
    "pct_criancas_adolescentes": "SUM(b.0_4_anos + b.5_a_9_anos + b.10_14_anos)/b.qtd_moradores",
    "pct_adultos": "SUM(b.v01034_15_19_anos + b.v01035_20_24_anos + b.v01036_25_29_anos +"
    " b.30_39_anos + b.40_49_anos + b.50_59_anos)/b.qtd_moradores",
    "pct_idosos": "SUM(b.60_69_anos + b.70_anos_ou_mais)/b.qtd_moradores",
    "pct_pia": "SUM(b.v01034_15_19_anos + b.v01035_20_24_anos + b.v01036_25_29_anos + "
    "b.30_39_anos + b.40_49_anos + b.50_59_anos)/b.qtd_moradores",
    "razao_masculino_feminino": "b.v01007_sxM/b.v01008_sxF",
    "razao_dependencia_pia": "SUM(b.0_4_anos + b.5_a_9_anos + b.10_14_anos + b.60_69_anos +"
    " b.70_anos_ou_mais)/ (SUM(b.v01034_15_19_anos + b.v01035_20_24_anos + b.v01036_25_29_anos+"
    " b.30_39_anos + b.40_49_anos + b.50_59_anos))",
}
caracteristica_domicilios1 = ["v00001_dompp"]
caracteristica_domicilios1 = [f"c.{col}" for col in caracteristica_domicilios1]
parentesco = [
    "responsavel_dompp",
    "responsavel_dompp_sxM",
    "responsavel_dompp_sxF",
    "unidade_domestica_unipessoal",
    "unidade_domestica_nuclear",
    "dompp_com_responsavel_sem_conjuge_com_filhos_sxF",
    "responsavel_dompp_60_anos_ou_mais",
    "responsavel_dompp_12_17_anos",
    "responsavel_dompp_18_24_anos",
]
parentesco = [f"d.{col}" for col in parentesco]
parentesco_formulas = {
    "pct_dompp_chefe_sxF": "d.responsavel_dompp_sxF / d.responsavel_dompp",
    "pct_dompp_unipessoal": "d.unidade_domestica_unipessoal / c.v00001_dompp",
    "pct_dompp_nuclear": "d.unidade_domestica_nuclear / c.v00001_dompp",
    "pct_dompp_mae_solo": "d.dompp_com_responsavel_sem_conjuge_com_filhos_sxF / c.v00001_dompp",
    "pct_dompp_chefe_idoso": "d.responsavel_dompp_60_anos_ou_mais/d.responsavel_dompp",
    "pct_dompp_chefe_jovem": "SUM(d.responsavel_dompp_60_anos_ou_mais + d.)/d.responsavel_dompp",
}


def select_and_create_variables_censo():
    """
    Select and create variables from the Censo dataset.
    """
